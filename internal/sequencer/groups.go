package sequencer

import (
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
	"github.com/ayushhh1010/sluice/internal/proc"
)

// Classic consumer group coordinator (JoinGroup/SyncGroup). Membership lives
// only in memory: after a failover members get UNKNOWN_MEMBER_ID and
// rejoin. Committed offsets go through the metadata log and survive.

type groupState int

const (
	gsEmpty groupState = iota
	gsPreparing
	gsCompleting
	gsStable
)

func (s groupState) String() string {
	return [...]string{"Empty", "PreparingRebalance", "CompletingRebalance", "Stable"}[s]
}

type group struct {
	name         string
	state        groupState
	protocolType string
	protocol     string
	generation   int32
	leader       string
	members      map[string]*gmember
	pending      map[string]int64 // member IDs handed out with MEMBER_ID_REQUIRED -> expiry
	deadline     int64            // rebalance deadline
	delayUntil   int64            // initial join delay for a group leaving Empty
	seq          uint64
}

type gmember struct {
	id, clientID string
	instanceID   *string
	protocols    []kmsg.JoinGroupRequestProtocol
	sessionMs    int32
	rebalanceMs  int32
	seq          uint64
	hbDeadline   int64
	assignment   []byte

	joinK   *kctx
	joinReq *kmsg.JoinGroupRequest
	syncK   *kctx
	syncReq *kmsg.SyncGroupRequest
}

func (g *group) sortedMembers() []*gmember {
	ms := make([]*gmember, 0, len(g.members))
	for _, m := range g.members {
		ms = append(ms, m)
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].seq < ms[j].seq })
	return ms
}

// candidates is the set of protocols every member supports.
func (g *group) supports(protos []kmsg.JoinGroupRequestProtocol) bool {
	for _, m := range g.members {
		ok := false
		for _, mp := range m.protocols {
			for _, p := range protos {
				if mp.Name == p.Name {
					ok = true
				}
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

func (c *Core) memberID(clientID string) string {
	return fmt.Sprintf("%s-%016x%016x", clientID, c.rnd.Uint64(), c.rnd.Uint64())
}

func (c *Core) joinGroup(k *kctx, req *kmsg.JoinGroupRequest) {
	resp := req.ResponseKind().(*kmsg.JoinGroupResponse)
	fail := func(code int16) {
		resp.ErrorCode, resp.Generation = code, -1
		k.respond(resp)
	}
	switch {
	case req.Group == "":
		fail(kerr.InvalidGroupID.Code)
		return
	case req.SessionTimeoutMillis < c.cfg.GroupMinSessionMs || req.SessionTimeoutMillis > c.cfg.GroupMaxSessionMs:
		fail(kerr.InvalidSessionTimeout.Code)
		return
	case req.ProtocolType == "" || len(req.Protocols) == 0:
		fail(kerr.InconsistentGroupProtocol.Code)
		return
	}
	g := c.groups[req.Group]
	if g == nil {
		if req.MemberID != "" {
			fail(kerr.UnknownMemberID.Code)
			return
		}
		// Membership is not logged, so a new leader starts groups afresh.
		// Seeding the generation from the epoch keeps generation numbers
		// from repeating across failovers, as Kafka guarantees.
		g = &group{name: req.Group, members: map[string]*gmember{}, pending: map[string]int64{}, generation: max(int32(c.st.Epoch&0x7fff)<<16, c.groupGens[req.Group])}
		c.groups[req.Group] = g
	}
	if len(g.members) > 0 && (g.protocolType != req.ProtocolType || !g.supports(req.Protocols)) {
		fail(kerr.InconsistentGroupProtocol.Code)
		return
	}
	id := req.MemberID
	if id == "" {
		id = c.memberID(k.req.ClientId)
		if req.Version >= 4 {
			// KIP-394: the client must rejoin with the assigned ID.
			g.pending[id] = c.now + int64(req.SessionTimeoutMillis)
			resp.MemberID = id
			fail(kerr.MemberIDRequired.Code)
			return
		}
	} else if g.members[id] == nil && g.pending[id] == 0 {
		fail(kerr.UnknownMemberID.Code)
		return
	}
	delete(g.pending, id)
	m := g.members[id]
	if m == nil {
		g.seq++
		m = &gmember{id: id, seq: g.seq}
		g.members[id] = m
	}
	if m.joinK != nil { // a duplicate join replaces the older one
		old := m.joinReq.ResponseKind().(*kmsg.JoinGroupResponse)
		old.ErrorCode, old.Generation = kerr.UnknownMemberID.Code, -1
		m.joinK.respond(old)
	}
	if len(g.members) == 1 {
		g.protocolType = req.ProtocolType
	}
	m.clientID, m.instanceID = k.req.ClientId, req.InstanceID
	m.protocols = req.Protocols
	m.sessionMs, m.rebalanceMs = req.SessionTimeoutMillis, req.RebalanceTimeoutMillis
	if m.rebalanceMs <= 0 { // JoinGroup v0 has no rebalance timeout
		m.rebalanceMs = m.sessionMs
	}
	m.hbDeadline = c.now + int64(m.sessionMs)
	m.joinK, m.joinReq = k, req
	if g.state != gsPreparing {
		c.prepareRebalance(g)
	}
	c.maybeCompleteJoin(g)
}

func (c *Core) prepareRebalance(g *group) {
	if g.state == gsCompleting {
		for _, m := range g.sortedMembers() {
			c.respondSync(m, kerr.RebalanceInProgress.Code)
		}
	}
	wasEmpty := g.state == gsEmpty
	g.state = gsPreparing
	var maxRebalance int32
	for _, m := range g.members {
		maxRebalance = max(maxRebalance, m.rebalanceMs)
	}
	g.deadline = c.now + int64(maxRebalance)
	g.delayUntil = 0
	if wasEmpty {
		g.delayUntil = c.now + min(max(c.cfg.GroupInitialDelayMs, 0), int64(maxRebalance))
	}
	c.Stats.GroupRebalances++
	c.emit(proc.Observe{Name: "group_rebalance", Value: 1})
}

func (c *Core) maybeCompleteJoin(g *group) {
	if g.state != gsPreparing {
		return
	}
	all := true
	for _, m := range g.members {
		if m.joinK == nil {
			all = false
		}
	}
	if (all && c.now >= g.delayUntil) || c.now >= g.deadline {
		c.completeJoin(g)
	}
}

func (c *Core) completeJoin(g *group) {
	for _, m := range g.sortedMembers() {
		if m.joinK == nil { // did not rejoin within the rebalance timeout
			c.respondSync(m, kerr.RebalanceInProgress.Code)
			delete(g.members, m.id)
		}
	}
	g.generation++
	if len(g.members) == 0 {
		g.state, g.protocol, g.leader = gsEmpty, "", ""
		return
	}
	members := g.sortedMembers()
	g.protocol = choose(members)
	if g.members[g.leader] == nil {
		g.leader = members[0].id
	}
	g.state = gsCompleting
	for _, m := range members {
		resp := m.joinReq.ResponseKind().(*kmsg.JoinGroupResponse)
		resp.Generation = g.generation
		resp.ProtocolType, resp.Protocol = strp(g.protocolType), strp(g.protocol)
		resp.LeaderID, resp.MemberID = g.leader, m.id
		if m.id == g.leader {
			for _, o := range members {
				jm := kmsg.NewJoinGroupResponseMember()
				jm.MemberID, jm.InstanceID = o.id, o.instanceID
				for _, p := range o.protocols {
					if p.Name == g.protocol {
						jm.ProtocolMetadata = p.Metadata
					}
				}
				resp.Members = append(resp.Members, jm)
			}
		}
		m.joinK.respond(resp)
		m.joinK, m.joinReq = nil, nil
		m.hbDeadline = c.now + int64(m.sessionMs)
	}
}

// choose picks the protocol by vote: each member votes for its most
// preferred protocol that every member supports.
func choose(members []*gmember) string {
	supportedByAll := func(name string) bool {
		for _, m := range members {
			found := false
			for _, p := range m.protocols {
				if p.Name == name {
					found = true
				}
			}
			if !found {
				return false
			}
		}
		return true
	}
	votes := map[string]int{}
	var order []string
	for _, m := range members {
		for _, p := range m.protocols {
			if supportedByAll(p.Name) {
				if votes[p.Name] == 0 {
					order = append(order, p.Name)
				}
				votes[p.Name]++
				break
			}
		}
	}
	best := ""
	for _, name := range order {
		if best == "" || votes[name] > votes[best] {
			best = name
		}
	}
	return best
}

func (c *Core) respondSync(m *gmember, code int16) {
	if m.syncK == nil {
		return
	}
	resp := m.syncReq.ResponseKind().(*kmsg.SyncGroupResponse)
	resp.ErrorCode = code
	m.syncK.respond(resp)
	m.syncK, m.syncReq = nil, nil
}

func (c *Core) syncGroup(k *kctx, req *kmsg.SyncGroupRequest) {
	resp := req.ResponseKind().(*kmsg.SyncGroupResponse)
	fail := func(code int16) {
		resp.ErrorCode = code
		k.respond(resp)
	}
	g := c.groups[req.Group]
	var m *gmember
	if g != nil {
		m = g.members[req.MemberID]
	}
	switch {
	case m == nil:
		fail(kerr.UnknownMemberID.Code)
		return
	case req.Generation != g.generation:
		fail(kerr.IllegalGeneration.Code)
		return
	case req.ProtocolType != nil && *req.ProtocolType != g.protocolType,
		req.Protocol != nil && g.protocol != "" && *req.Protocol != g.protocol:
		fail(kerr.InconsistentGroupProtocol.Code)
		return
	}
	m.hbDeadline = c.now + int64(m.sessionMs)
	switch g.state {
	case gsPreparing:
		fail(kerr.RebalanceInProgress.Code)
	case gsStable:
		resp.ProtocolType, resp.Protocol = strp(g.protocolType), strp(g.protocol)
		resp.MemberAssignment = m.assignment
		k.respond(resp)
	case gsCompleting:
		c.respondSync(m, kerr.RebalanceInProgress.Code) // duplicate sync
		m.syncK, m.syncReq = k, req
		if m.id != g.leader {
			return
		}
		assigned := map[string][]byte{}
		for _, a := range req.GroupAssignment {
			assigned[a.MemberID] = a.MemberAssignment
		}
		g.state = gsStable
		for _, o := range g.sortedMembers() {
			o.assignment = assigned[o.id]
			if o.syncK != nil {
				r := o.syncReq.ResponseKind().(*kmsg.SyncGroupResponse)
				r.ProtocolType, r.Protocol = strp(g.protocolType), strp(g.protocol)
				r.MemberAssignment = o.assignment
				o.syncK.respond(r)
				o.syncK, o.syncReq = nil, nil
			}
		}
	default:
		fail(kerr.UnknownMemberID.Code)
	}
}

func (c *Core) heartbeatGroup(req *kmsg.HeartbeatRequest) *kmsg.HeartbeatResponse {
	resp := req.ResponseKind().(*kmsg.HeartbeatResponse)
	g := c.groups[req.Group]
	var m *gmember
	if g != nil {
		m = g.members[req.MemberID]
	}
	switch {
	case m == nil:
		resp.ErrorCode = kerr.UnknownMemberID.Code
	case req.Generation != g.generation:
		resp.ErrorCode = kerr.IllegalGeneration.Code
	default:
		m.hbDeadline = c.now + int64(m.sessionMs)
		if g.state == gsPreparing {
			resp.ErrorCode = kerr.RebalanceInProgress.Code
		}
	}
	return resp
}

func (c *Core) leaveGroup(req *kmsg.LeaveGroupRequest) *kmsg.LeaveGroupResponse {
	resp := req.ResponseKind().(*kmsg.LeaveGroupResponse)
	g := c.groups[req.Group]
	ids := []string{req.MemberID}
	if req.Version >= 3 {
		ids = nil
		for _, m := range req.Members {
			ids = append(ids, m.MemberID)
		}
	}
	left := false
	for _, id := range ids {
		code := int16(0)
		if g == nil || g.members[id] == nil {
			code = kerr.UnknownMemberID.Code
		} else {
			c.removeMember(g, g.members[id])
			left = true
		}
		if req.Version >= 3 {
			rm := kmsg.NewLeaveGroupResponseMember()
			rm.MemberID, rm.ErrorCode = id, code
			resp.Members = append(resp.Members, rm)
		} else {
			resp.ErrorCode = code
		}
	}
	if left {
		c.afterMembershipChange(g)
	}
	return resp
}

func (c *Core) removeMember(g *group, m *gmember) {
	if m.joinK != nil {
		r := m.joinReq.ResponseKind().(*kmsg.JoinGroupResponse)
		r.ErrorCode, r.Generation = kerr.UnknownMemberID.Code, -1
		m.joinK.respond(r)
		m.joinK = nil
	}
	c.respondSync(m, kerr.UnknownMemberID.Code)
	delete(g.members, m.id)
}

func (c *Core) afterMembershipChange(g *group) {
	switch {
	case len(g.members) == 0 && g.state != gsPreparing:
		g.state, g.protocol, g.leader = gsEmpty, "", ""
		g.generation++
	case g.state == gsPreparing:
		c.maybeCompleteJoin(g)
	default:
		c.prepareRebalance(g)
		c.maybeCompleteJoin(g)
	}
}

func (c *Core) groupHousekeeping() {
	names := make([]string, 0, len(c.groups))
	for n := range c.groups {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		g := c.groups[n]
		for id, at := range g.pending {
			if c.now > at {
				delete(g.pending, id)
			}
		}
		expired := false
		for _, m := range g.sortedMembers() {
			if m.joinK == nil && c.now > m.hbDeadline && g.state != gsPreparing {
				c.removeMember(g, m)
				expired = true
			}
		}
		if expired {
			c.afterMembershipChange(g)
		}
		c.maybeCompleteJoin(g)
		if g.state == gsEmpty && len(g.members) == 0 && len(g.pending) == 0 {
			c.groupGens[n] = g.generation // never reuse a generation
			delete(c.groups, n)
		}
	}
}

func (g *group) failPending(c *Core) {
	for _, m := range g.sortedMembers() {
		if m.joinK != nil {
			r := m.joinReq.ResponseKind().(*kmsg.JoinGroupResponse)
			r.ErrorCode, r.Generation = kerr.NotCoordinator.Code, -1
			m.joinK.respond(r)
			m.joinK = nil
		}
		c.respondSync(m, kerr.NotCoordinator.Code)
	}
}

const maxOffsetMetadata = 4096

func (c *Core) offsetCommit(k *kctx, req *kmsg.OffsetCommitRequest) {
	resp := req.ResponseKind().(*kmsg.OffsetCommitResponse)
	setAll := func(code int16) {
		for i := range resp.Topics {
			for j := range resp.Topics[i].Partitions {
				if resp.Topics[i].Partitions[j].ErrorCode == 0 {
					resp.Topics[i].Partitions[j].ErrorCode = code
				}
			}
		}
	}
	for _, rt := range req.Topics {
		t := kmsg.NewOffsetCommitResponseTopic()
		t.Topic = rt.Topic
		for _, rp := range rt.Partitions {
			p := kmsg.NewOffsetCommitResponseTopicPartition()
			p.Partition = rp.Partition
			t.Partitions = append(t.Partitions, p)
		}
		resp.Topics = append(resp.Topics, t)
	}
	g := c.groups[req.Group]
	var code int16
	switch {
	case req.Group == "":
		code = kerr.InvalidGroupID.Code
	case req.Generation < 0 && req.MemberID == "":
		// A commit from outside the group protocol (admin tools, manual
		// assignment) is allowed only while the group has no members.
		if g != nil && len(g.members) > 0 {
			code = kerr.UnknownMemberID.Code
		}
	case g == nil || g.members[req.MemberID] == nil:
		code = kerr.UnknownMemberID.Code
	case req.Generation != g.generation:
		code = kerr.IllegalGeneration.Code
	case g.state == gsPreparing:
		code = kerr.RebalanceInProgress.Code
	}
	if code != 0 {
		setAll(code)
		k.respond(resp)
		return
	}
	if g != nil {
		if m := g.members[req.MemberID]; m != nil {
			m.hbDeadline = c.now + int64(m.sessionMs)
		}
	}
	op := &pb.CommitOffsets{Group: req.Group}
	for i, rt := range req.Topics {
		tid, known := c.st.ByName[rt.Topic]
		for j, rp := range rt.Partitions {
			p := &resp.Topics[i].Partitions[j]
			switch {
			case !known || c.st.Partition(tid, uint32(rp.Partition)) == nil || rp.Partition < 0:
				p.ErrorCode = kerr.UnknownTopicOrPartition.Code
			case rp.Metadata != nil && len(*rp.Metadata) > maxOffsetMetadata:
				p.ErrorCode = kerr.OffsetMetadataTooLarge.Code
			default:
				oc := &pb.OffsetCommit{Topic: rt.Topic, Partition: uint32(rp.Partition), Offset: rp.Offset, LeaderEpoch: rp.LeaderEpoch}
				if rp.Metadata != nil {
					oc.Metadata = *rp.Metadata
				}
				op.Offsets = append(op.Offsets, oc)
			}
		}
	}
	if len(op.Offsets) == 0 {
		k.respond(resp)
		return
	}
	// Durable before acked: the reply waits for the metadata append.
	c.enqueue(&queued{
		plan: func(pl *planner) { pl.add(kindOffsets, &pb.Op{Body: &pb.Op_CommitOffsets{CommitOffsets: op}}) },
		done: func(ok bool) {
			if !ok {
				setAll(kerr.NotCoordinator.Code)
			}
			k.respond(resp)
		},
	})
}

func (c *Core) offsetFetch(req *kmsg.OffsetFetchRequest) *kmsg.OffsetFetchResponse {
	resp := req.ResponseKind().(*kmsg.OffsetFetchResponse)
	offs := c.st.Offsets[req.Group]
	topics := req.Topics
	if topics == nil { // all committed offsets for the group
		byTopic := map[string][]int32{}
		var names []string
		for k := range offs {
			if byTopic[k.Topic] == nil {
				names = append(names, k.Topic)
			}
			byTopic[k.Topic] = append(byTopic[k.Topic], int32(k.Partition))
		}
		sort.Strings(names)
		for _, n := range names {
			ps := byTopic[n]
			sort.Slice(ps, func(i, j int) bool { return ps[i] < ps[j] })
			topics = append(topics, kmsg.OffsetFetchRequestTopic{Topic: n, Partitions: ps})
		}
	}
	for _, rt := range topics {
		t := kmsg.NewOffsetFetchResponseTopic()
		t.Topic = rt.Topic
		for _, p := range rt.Partitions {
			rp := kmsg.NewOffsetFetchResponseTopicPartition()
			rp.Partition = p
			rp.Offset, rp.LeaderEpoch, rp.Metadata = -1, -1, strp("")
			if oc := offs[groupTP{rt.Topic, uint32(p)}]; oc != nil {
				rp.Offset, rp.LeaderEpoch, rp.Metadata = oc.Offset, oc.LeaderEpoch, strp(oc.Metadata)
			}
			t.Partitions = append(t.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, t)
	}
	return resp
}

func (c *Core) describeGroups(req *kmsg.DescribeGroupsRequest) *kmsg.DescribeGroupsResponse {
	resp := req.ResponseKind().(*kmsg.DescribeGroupsResponse)
	for _, name := range req.Groups {
		dg := kmsg.NewDescribeGroupsResponseGroup()
		dg.Group = name
		g := c.groups[name]
		switch {
		case g != nil:
			dg.State, dg.ProtocolType, dg.Protocol = g.state.String(), g.protocolType, g.protocol
			for _, m := range g.sortedMembers() {
				dm := kmsg.NewDescribeGroupsResponseGroupMember()
				dm.MemberID, dm.InstanceID, dm.ClientID, dm.MemberAssignment = m.id, m.instanceID, m.clientID, m.assignment
				for _, p := range m.protocols {
					if p.Name == g.protocol {
						dm.ProtocolMetadata = p.Metadata
					}
				}
				dg.Members = append(dg.Members, dm)
			}
		case c.st.Offsets[name] != nil:
			dg.State = "Empty"
		default:
			dg.State = "Dead"
			if req.Version >= 6 {
				dg.ErrorCode = kerr.GroupIDNotFound.Code
			}
		}
		resp.Groups = append(resp.Groups, dg)
	}
	return resp
}

func (c *Core) listGroups(req *kmsg.ListGroupsRequest) *kmsg.ListGroupsResponse {
	resp := req.ResponseKind().(*kmsg.ListGroupsResponse)
	seen := map[string]bool{}
	var names []string
	for n := range c.groups {
		seen[n] = true
		names = append(names, n)
	}
	for n := range c.st.Offsets {
		if !seen[n] {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	for _, n := range names {
		lg := kmsg.NewListGroupsResponseGroup()
		lg.Group, lg.GroupState, lg.ProtocolType = n, "Empty", "consumer"
		if g := c.groups[n]; g != nil {
			lg.GroupState = g.state.String()
			if g.protocolType != "" {
				lg.ProtocolType = g.protocolType
			}
		}
		if len(req.StatesFilter) > 0 && !contains(req.StatesFilter, lg.GroupState) {
			continue
		}
		resp.Groups = append(resp.Groups, lg)
	}
	return resp
}
