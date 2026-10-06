package sequencer

import (
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/ayushhh1010/sluice/internal/bugs"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
)

type member struct {
	info     *pb.AgentInfo
	lastSeen int64
}

func (c *Core) handle(id uint64, req *pb.Request) {
	if req.EpochSeen > c.epochSeen {
		c.epochSeen = req.EpochSeen
	}
	if _, ok := req.Body.(*pb.Request_Ping); ok {
		ping := &pb.PingResponse{Leader: c.role == roleLeader, Epoch: c.Epoch(), Node: c.cfg.Node}
		if c.role == roleRecovering {
			ping.RecoveringMs = max(c.now-c.recoveringSince, 1)
		}
		c.reply(id, &pb.Response{Body: &pb.Response_Ping{Ping: ping}})
		return
	}
	if c.role != roleLeader {
		c.reply(id, &pb.Response{Code: pb.Code_CODE_NOT_LEADER, Message: "not the leader (" + c.role.String() + ")"})
		return
	}
	// A caller that has seen a newer epoch proves we were deposed.
	if req.EpochSeen > c.st.Epoch {
		c.stepDown("caller saw newer epoch")
		c.reply(id, &pb.Response{Code: pb.Code_CODE_NOT_LEADER, Message: "deposed"})
		return
	}
	switch b := req.Body.(type) {
	case *pb.Request_Commit:
		c.onCommit(id, b.Commit)
	case *pb.Request_Lookup:
		c.reply(id, &pb.Response{Body: &pb.Response_Lookup{Lookup: c.lookup(b.Lookup)}})
	case *pb.Request_Heartbeat:
		c.onHeartbeat(id, b.Heartbeat)
	case *pb.Request_Kafka:
		c.onKafka(id, b.Kafka)
	case *pb.Request_Inspect:
		c.onInspect(id, b.Inspect)
	case *pb.Request_Chaos:
		c.onChaos(id, b.Chaos)
	default:
		c.reply(id, &pb.Response{Code: pb.Code_CODE_INVALID, Message: "unknown request"})
	}
}

// onCommit commits an agent's segments in request order and replies once
// every segment has an outcome.
func (c *Core) onCommit(id uint64, req *pb.CommitRequest) {
	if len(req.Segments) == 0 {
		c.reply(id, &pb.Response{Code: pb.Code_CODE_INVALID, Message: "empty commit"})
		return
	}
	out := make([]*pb.SegmentOutcome, len(req.Segments))
	left := len(req.Segments)
	for i, seg := range req.Segments {
		c.commitSegment(seg, func(o *pb.SegmentOutcome) {
			out[i] = o
			if left--; left == 0 {
				c.reply(id, &pb.Response{Body: &pb.Response_Commit{Commit: &pb.CommitResponse{Segments: out}}})
			}
		})
	}
}

func invalidSegment(msg string) *pb.SegmentOutcome {
	return &pb.SegmentOutcome{Code: pb.Code_CODE_INVALID, Message: msg}
}

// commitSegment resolves one segment: from the dedup cache, by joining an
// attempt already queued or in flight, or by queueing it. Queue order is
// plan order, so segments of one request are planned in request order.
func (c *Core) commitSegment(seg *pb.SegmentCommit, done func(*pb.SegmentOutcome)) {
	key := seg.ObjectKey
	if prev, ok := c.st.RecentByKey[key]; ok {
		done(&pb.SegmentOutcome{Results: prev.Results})
		return
	}
	if ws, ok := c.commitWaiters[key]; ok {
		c.commitWaiters[key] = append(ws, done) // a retry while the first attempt is pending
		return
	}
	switch {
	case len(seg.Entries) == 0:
		done(invalidSegment("empty segment"))
		return
	case c.now-seg.ObjectCreatedMs > c.cfg.MaxSegmentAgeMs:
		// The orphan sweep may delete objects this old; committing one
		// could leave a ref to a deleted object.
		done(invalidSegment("segment too old"))
		return
	case c.st.RefCount[key] > 0 || c.st.Tombstones[key] != 0:
		done(invalidSegment("segment already committed"))
		return
	}
	c.commitWaiters[key] = []func(*pb.SegmentOutcome){done}
	var result *pb.CommitBatches
	c.enqueue(&queued{
		plan: func(pl *planner) { result = pl.planCommit(seg) },
		done: func(ok bool) {
			ws := c.commitWaiters[key]
			delete(c.commitWaiters, key)
			o := &pb.SegmentOutcome{Code: pb.Code_CODE_UNAVAILABLE, Message: "leadership lost; retry"}
			if ok {
				results := result.Results
				if bugs.On("ack-wrong-offset") && len(results) > 0 && results[0].BaseOffset%13 == 7 {
					results = append([]*pb.BatchResult{proto.Clone(results[0]).(*pb.BatchResult)}, results[1:]...)
					results[0].BaseOffset++
				}
				o = &pb.SegmentOutcome{Results: results}
			}
			for _, w := range ws {
				w(o)
			}
		},
	})
}

// Kafka error code for offsets outside [log start, high watermark].
const errOffsetOutOfRange = 1

func (c *Core) lookup(req *pb.LookupRequest) *pb.LookupResponse {
	tid, ok := c.st.ByName[req.Topic]
	part := c.st.Partition(tid, req.Partition)
	if !ok || part == nil {
		return &pb.LookupResponse{ErrorCode: errUnknownTopicOrPartition}
	}
	resp := &pb.LookupResponse{TopicId: tid[:], HighWatermark: part.Next, LogStart: part.LogStart}
	if req.Offset < part.LogStart || req.Offset > part.Next {
		resp.ErrorCode = errOffsetOutOfRange
		return resp
	}
	i := part.find(req.Offset)
	if i < 0 {
		return resp // offset == high watermark
	}
	var size int64
	for ; i < len(part.Refs); i++ {
		r := part.Refs[i]
		if len(resp.Refs) > 0 && size+int64(r.Length) > int64(req.MaxBytes) {
			break
		}
		resp.Refs = append(resp.Refs, r)
		size += int64(r.Length)
		if len(resp.Refs) >= 1000 {
			break
		}
	}
	return resp
}

// ---- Membership ----

func (c *Core) onHeartbeat(id uint64, hb *pb.HeartbeatRequest) {
	a := hb.Agent
	if a == nil || a.Id == "" {
		c.reply(id, &pb.Response{Code: pb.Code_CODE_INVALID, Message: "heartbeat without agent"})
		return
	}
	m := c.members[a.Id]
	if m == nil || m.info.Incarnation != a.Incarnation || len(m.info.Listeners) != len(a.Listeners) || m.info.NodeId != a.NodeId {
		c.membersVer++
		m = &member{}
		c.members[a.Id] = m
	}
	m.info, m.lastSeen = a, c.now
	c.countClassA(hb.ClassAOps)

	resp := &pb.HeartbeatResponse{
		TopicsVersion: c.topicsVer,
		ReadOnly:      c.readOnly(),
		ClassAToday:   c.classACount,
	}
	for _, mid := range c.sortedMembers() {
		resp.Members = append(resp.Members, c.members[mid].info)
	}
	if hb.TopicsVersion != c.topicsVer {
		resp.TopicsIncluded = true
		for _, t := range c.st.sortedTopics() {
			resp.Topics = append(resp.Topics, &pb.TopicInfo{Id: t.ID[:], Name: t.Name, Partitions: uint32(len(t.Parts)), RetentionMs: t.Retention})
		}
	}
	if c.chaosTargets[a.Id] {
		delete(c.chaosTargets, a.Id)
		resp.Die = true
	}
	c.reply(id, &pb.Response{Body: &pb.Response_Heartbeat{Heartbeat: resp}})
}

func (c *Core) expireMembers() {
	for _, id := range c.sortedMembers() {
		if c.now-c.members[id].lastSeen > c.cfg.MemberTTLms {
			delete(c.members, id)
			c.membersVer++
		}
	}
}

func (c *Core) sortedMembers() []string {
	ids := make([]string, 0, len(c.members))
	for id := range c.members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (c *Core) nodeIDs() []int32 {
	var ids []int32
	for _, m := range c.members {
		ids = append(ids, m.info.NodeId)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (c *Core) memberByNode(node int32) *pb.AgentInfo {
	for _, id := range c.sortedMembers() {
		if m := c.members[id]; m.info.NodeId == node {
			return m.info
		}
	}
	return nil
}

// ---- Cost guard ----

const dayMs = 86_400_000

func (c *Core) countClassA(n int64) {
	if day := c.now / dayMs; day != c.classADay {
		c.classADay, c.classACount = day, 0
	}
	c.classACount += n
}

func (c *Core) readOnly() bool {
	if c.now/dayMs != c.classADay {
		return false
	}
	return c.cfg.ClassADailyLimit > 0 && c.classACount >= c.cfg.ClassADailyLimit
}

// ---- Playground inspection and chaos ----

func (c *Core) onInspect(id uint64, req *pb.InspectRequest) {
	resp := &pb.InspectResponse{}
	switch w := req.What.(type) {
	case *pb.InspectRequest_Partition:
		ip := w.Partition
		tid, ok := c.st.ByName[ip.Topic]
		part := c.st.Partition(tid, ip.Partition)
		if !ok || part == nil {
			c.reply(id, &pb.Response{Code: pb.Code_CODE_INVALID, Message: "unknown topic or partition"})
			return
		}
		resp.HighWatermark, resp.LogStart = part.Next, part.LogStart
		limit := int(ip.Limit)
		if limit <= 0 || limit > 500 {
			limit = 100
		}
		i := part.find(max(ip.FromOffset, part.LogStart))
		if i < 0 {
			i = len(part.Refs)
		}
		for ; i < len(part.Refs) && len(resp.Refs) < limit; i++ {
			resp.Refs = append(resp.Refs, part.Refs[i])
		}
	case *pb.InspectRequest_Objects:
		limit := int(w.Objects.Limit)
		if limit <= 0 || limit > 200 {
			limit = 20
		}
		for i := len(c.st.Recent) - 1; i >= 0 && len(resp.Objects) < limit; i-- {
			resp.Objects = append(resp.Objects, c.st.Recent[i])
		}
	case *pb.InspectRequest_Cluster:
		ci := &pb.ClusterInfo{
			Leader: c.cfg.Node, Epoch: c.st.Epoch, MetalogSeq: c.st.Seq,
			ClassAToday: c.classACount, ReadOnly: c.readOnly(), Topics: int64(len(c.st.Topics)),
		}
		for _, mid := range c.sortedMembers() {
			ci.Agents = append(ci.Agents, c.members[mid].info)
		}
		resp.Cluster = ci
	}
	c.reply(id, &pb.Response{Body: &pb.Response_Inspect{Inspect: resp}})
}

func (c *Core) onChaos(id uint64, req *pb.ChaosRequest) {
	if c.now < c.chaosNext {
		c.reply(id, &pb.Response{Body: &pb.Response_Chaos{Chaos: &pb.ChaosResponse{RetryAfterMs: c.chaosNext - c.now}}})
		return
	}
	ids := c.sortedMembers()
	target := req.AgentId
	if target == "" && len(ids) > 0 {
		target = ids[c.rnd.Intn(len(ids))]
	}
	if c.members[target] == nil {
		c.reply(id, &pb.Response{Code: pb.Code_CODE_INVALID, Message: "no such live agent"})
		return
	}
	c.chaosNext = c.now + c.cfg.ChaosIntervalMs
	c.chaosTargets[target] = true
	c.reply(id, &pb.Response{Body: &pb.Response_Chaos{Chaos: &pb.ChaosResponse{Killed: target}}})
}
