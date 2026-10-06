package sequencer

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
	"github.com/ayushhh1010/sluice/internal/placement"
)

// kctx is one forwarded Kafka request.
type kctx struct {
	c   *Core
	id  uint64
	req *pb.KafkaRequest
}

func (k *kctx) respond(resp kmsg.Response) {
	k.c.reply(k.id, &pb.Response{Body: &pb.Response_Kafka{Kafka: &pb.KafkaResponse{Body: resp.AppendTo(nil)}}})
}

func (c *Core) onKafka(id uint64, kr *pb.KafkaRequest) {
	kreq := kmsg.RequestForKey(int16(kr.ApiKey))
	if kreq == nil {
		c.reply(id, &pb.Response{Code: pb.Code_CODE_INVALID, Message: fmt.Sprintf("unknown api key %d", kr.ApiKey)})
		return
	}
	kreq.SetVersion(int16(kr.ApiVersion))
	if err := kreq.ReadFrom(kr.Body); err != nil {
		c.reply(id, &pb.Response{Code: pb.Code_CODE_INVALID, Message: "undecodable request: " + err.Error()})
		return
	}
	k := &kctx{c: c, id: id, req: kr}
	switch r := kreq.(type) {
	case *kmsg.MetadataRequest:
		k.respond(c.metadata(k, r))
	case *kmsg.CreateTopicsRequest:
		c.createTopics(k, r)
	case *kmsg.DeleteTopicsRequest:
		c.deleteTopics(k, r)
	case *kmsg.ListOffsetsRequest:
		k.respond(c.listOffsets(r))
	case *kmsg.FindCoordinatorRequest:
		k.respond(c.findCoordinator(k, r))
	case *kmsg.JoinGroupRequest:
		c.joinGroup(k, r)
	case *kmsg.SyncGroupRequest:
		c.syncGroup(k, r)
	case *kmsg.HeartbeatRequest:
		k.respond(c.heartbeatGroup(r))
	case *kmsg.LeaveGroupRequest:
		k.respond(c.leaveGroup(r))
	case *kmsg.OffsetCommitRequest:
		c.offsetCommit(k, r)
	case *kmsg.OffsetFetchRequest:
		c.readBarrier(func(ok bool) {
			if !ok {
				c.reply(id, &pb.Response{Code: pb.Code_CODE_NOT_LEADER, Message: "leadership lost"})
				return
			}
			k.respond(c.offsetFetch(r))
		})
	case *kmsg.DescribeGroupsRequest:
		k.respond(c.describeGroups(r))
	case *kmsg.ListGroupsRequest:
		k.respond(c.listGroups(r))
	case *kmsg.InitProducerIDRequest:
		c.initProducerID(k, r)
	case *kmsg.DescribeConfigsRequest:
		k.respond(c.describeConfigs(r))
	default:
		c.reply(id, &pb.Response{Code: pb.Code_CODE_INVALID, Message: fmt.Sprintf("api key %d is not handled by the sequencer", kr.ApiKey)})
	}
}

func (c *Core) listenerAddr(a *pb.AgentInfo, listener string) (string, int32, bool) {
	for _, l := range a.Listeners {
		if l.Name == listener {
			return l.Host, l.Port, true
		}
	}
	return "", 0, false
}

func (c *Core) metadata(k *kctx, req *kmsg.MetadataRequest) *kmsg.MetadataResponse {
	resp := req.ResponseKind().(*kmsg.MetadataResponse)
	resp.ClusterID = &c.cfg.Cluster
	resp.ControllerID = -1
	for _, mid := range c.sortedMembers() {
		a := c.members[mid].info
		host, port, ok := c.listenerAddr(a, k.req.Listener)
		if !ok {
			continue
		}
		b := kmsg.NewMetadataResponseBroker()
		b.NodeID, b.Host, b.Port = a.NodeId, host, port
		resp.Brokers = append(resp.Brokers, b)
		if resp.ControllerID == -1 || a.NodeId < resp.ControllerID {
			resp.ControllerID = a.NodeId
		}
	}
	nodes := c.nodeIDs()
	addTopic := func(t *Topic) {
		mt := kmsg.NewMetadataResponseTopic()
		name := t.Name
		mt.Topic, mt.TopicID = &name, t.ID
		for i := range t.Parts {
			p := kmsg.NewMetadataResponseTopicPartition()
			p.Partition = int32(i)
			p.LeaderEpoch = 0
			p.Leader = placement.Owner(nodes, t.ID, uint32(i))
			if p.Leader < 0 {
				p.ErrorCode = kerr.LeaderNotAvailable.Code
			} else {
				p.Replicas, p.ISR = []int32{p.Leader}, []int32{p.Leader}
			}
			mt.Partitions = append(mt.Partitions, p)
		}
		resp.Topics = append(resp.Topics, mt)
	}
	if req.Topics == nil {
		for _, t := range c.st.sortedTopics() {
			addTopic(t)
		}
		return resp
	}
	for _, rt := range req.Topics {
		var t *Topic
		if rt.Topic != nil {
			if id, ok := c.st.ByName[*rt.Topic]; ok {
				t = c.st.Topics[id]
			}
		} else if rt.TopicID != ([16]byte{}) {
			t = c.st.Topics[TopicID(rt.TopicID)]
		}
		if t == nil {
			mt := kmsg.NewMetadataResponseTopic()
			mt.Topic, mt.TopicID = rt.Topic, rt.TopicID
			mt.ErrorCode = kerr.UnknownTopicOrPartition.Code
			if rt.Topic == nil {
				mt.ErrorCode = kerr.UnknownTopicID.Code
			}
			resp.Topics = append(resp.Topics, mt)
			continue
		}
		addTopic(t)
	}
	return resp
}

// validTopicName follows Kafka's rules.
func validTopicName(n string) bool {
	if n == "" || n == "." || n == ".." || len(n) > 249 {
		return false
	}
	for _, r := range n {
		if !topicChar(r) {
			return false
		}
	}
	return true
}

func topicChar(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-'
}

func (c *Core) publicTopics() int {
	n := 0
	for _, t := range c.st.Topics {
		if strings.HasPrefix(t.Name, c.cfg.PublicPrefix) {
			n++
		}
	}
	return n
}

func strp(s string) *string { return &s }

// topicSpec is a validated CreateTopics entry.
type topicSpec struct {
	name       string
	partitions int32
	retention  int64
}

func (c *Core) validateTopic(k *kctx, rt kmsg.CreateTopicsRequestTopic) (topicSpec, int16, string) {
	s := topicSpec{name: rt.Topic, partitions: rt.NumPartitions, retention: c.cfg.DefaultRetentionMs}
	if !validTopicName(rt.Topic) {
		return s, kerr.InvalidTopicException.Code, "invalid topic name"
	}
	if s.partitions == -1 {
		s.partitions = c.cfg.DefaultPartitions
	}
	if len(rt.ReplicaAssignment) > 0 {
		return s, kerr.InvalidRequest.Code, "manual replica assignment is not supported"
	}
	for _, cfg := range rt.Configs {
		if cfg.Name == "retention.ms" && cfg.Value != nil {
			v, err := strconv.ParseInt(*cfg.Value, 10, 64)
			if err != nil || (v <= 0 && v != -1) {
				return s, kerr.InvalidConfig.Code, "invalid retention.ms"
			}
			if v == -1 {
				v = 0 // unlimited
			}
			s.retention = v
		}
	}
	maxParts := c.cfg.MaxPartitions
	if k.req.Public {
		if !strings.HasPrefix(s.name, c.cfg.PublicPrefix) {
			return s, kerr.TopicAuthorizationFailed.Code, "public topics must start with " + c.cfg.PublicPrefix
		}
		maxParts = c.cfg.PublicMaxPartitions
		if s.retention == 0 || s.retention > c.cfg.PublicRetentionMs {
			s.retention = c.cfg.PublicRetentionMs
		}
	}
	if s.partitions < 1 || s.partitions > maxParts {
		return s, kerr.InvalidPartitions.Code, fmt.Sprintf("partitions must be between 1 and %d", maxParts)
	}
	if _, exists := c.st.ByName[s.name]; exists {
		return s, kerr.TopicAlreadyExists.Code, "topic already exists"
	}
	return s, 0, ""
}

func (c *Core) createTopics(k *kctx, req *kmsg.CreateTopicsRequest) {
	resp := req.ResponseKind().(*kmsg.CreateTopicsResponse)
	type pendingTopic struct {
		idx  int
		spec topicSpec
		id   TopicID
	}
	var pending []*pendingTopic
	seen := map[string]bool{}
	for _, rt := range req.Topics {
		t := kmsg.NewCreateTopicsResponseTopic()
		t.Topic = rt.Topic
		spec, code, msg := c.validateTopic(k, rt)
		if code == 0 && seen[rt.Topic] {
			code, msg = kerr.InvalidRequest.Code, "duplicate topic in request"
		}
		seen[rt.Topic] = true
		if code != 0 {
			t.ErrorCode, t.ErrorMessage = code, strp(msg)
		} else {
			t.NumPartitions, t.ReplicationFactor = spec.partitions, 1
			ret := strconv.FormatInt(spec.retention, 10)
			for _, kv := range [][2]string{{"retention.ms", ret}, {"cleanup.policy", "delete"}} {
				cfg := kmsg.NewCreateTopicsResponseTopicConfig()
				cfg.Name, cfg.Value = kv[0], strp(kv[1])
				cfg.Source = int8(kmsg.ConfigSourceDefaultConfig)
				if kv[0] == "retention.ms" && hasConfig(rt.Configs, "retention.ms") {
					cfg.Source = int8(kmsg.ConfigSourceDynamicTopicConfig)
				}
				t.Configs = append(t.Configs, cfg)
			}
			if !req.ValidateOnly {
				var id TopicID
				for i := 0; i < 16; i += 8 {
					v := c.rnd.Uint64()
					for j := 0; j < 8; j++ {
						id[i+j] = byte(v >> (8 * j))
					}
				}
				id[6] = id[6]&0x0f | 0x40 // UUIDv4 shape
				id[8] = id[8]&0x3f | 0x80
				t.TopicID = id
				pending = append(pending, &pendingTopic{idx: len(resp.Topics), spec: spec, id: id})
			}
		}
		resp.Topics = append(resp.Topics, t)
	}
	if len(pending) == 0 {
		k.respond(resp)
		return
	}
	c.enqueue(&queued{
		plan: func(pl *planner) {
			for _, p := range pending {
				t := &resp.Topics[p.idx]
				_, exists := c.st.ByName[p.spec.name]
				switch {
				case pl.newNames[p.spec.name] || exists:
					t.ErrorCode, t.ErrorMessage = kerr.TopicAlreadyExists.Code, strp("topic already exists")
				case k.req.Public && c.publicTopics()+pl.publicNew >= c.cfg.PublicMaxTopics:
					t.ErrorCode, t.ErrorMessage = kerr.PolicyViolation.Code, strp("public topic limit reached; try again later")
				default:
					pl.newNames[p.spec.name] = true
					if k.req.Public {
						pl.publicNew++
					}
					pl.add(kindCreate, &pb.Op{Body: &pb.Op_CreateTopic{CreateTopic: &pb.CreateTopic{
						TopicId: p.id[:], Name: p.spec.name, Partitions: uint32(p.spec.partitions),
						RetentionMs: p.spec.retention, Owner: k.req.Principal, CreatedUnixMs: c.now,
					}}})
				}
			}
		},
		done: func(ok bool) {
			if !ok {
				for _, p := range pending {
					t := &resp.Topics[p.idx]
					if t.ErrorCode == 0 {
						t.ErrorCode = kerr.NotController.Code
					}
				}
			} else {
				c.topicsVer++
			}
			k.respond(resp)
		},
	})
}

func (c *Core) deleteTopics(k *kctx, req *kmsg.DeleteTopicsRequest) {
	resp := req.ResponseKind().(*kmsg.DeleteTopicsResponse)
	type target struct {
		idx int
		id  TopicID
	}
	var targets []target
	add := func(name *string, tid [16]byte) {
		var t *Topic
		if name != nil {
			if id, ok := c.st.ByName[*name]; ok {
				t = c.st.Topics[id]
			}
		} else {
			t = c.st.Topics[TopicID(tid)]
		}
		rt := kmsg.NewDeleteTopicsResponseTopic()
		rt.Topic, rt.TopicID = name, tid
		switch {
		case t == nil:
			rt.ErrorCode = kerr.UnknownTopicOrPartition.Code
			if name == nil {
				rt.ErrorCode = kerr.UnknownTopicID.Code
			}
		case k.req.Public && !strings.HasPrefix(t.Name, c.cfg.PublicPrefix):
			rt.ErrorCode = kerr.TopicAuthorizationFailed.Code
		default:
			rt.Topic, rt.TopicID = strp(t.Name), t.ID
			targets = append(targets, target{len(resp.Topics), t.ID})
		}
		resp.Topics = append(resp.Topics, rt)
	}
	for _, n := range req.TopicNames {
		add(strp(n), [16]byte{})
	}
	for _, t := range req.Topics {
		add(t.Topic, t.TopicID)
	}
	if len(targets) == 0 {
		k.respond(resp)
		return
	}
	c.enqueue(&queued{
		plan: func(pl *planner) {
			for _, t := range targets {
				if c.st.Topics[t.id] == nil || pl.deleted[t.id] {
					resp.Topics[t.idx].ErrorCode = kerr.UnknownTopicOrPartition.Code
					continue
				}
				pl.deleted[t.id] = true
				pl.add(kindDelete, &pb.Op{Body: &pb.Op_DeleteTopic{DeleteTopic: &pb.DeleteTopic{TopicId: append([]byte(nil), t.id[:]...)}}})
			}
		},
		done: func(ok bool) {
			if !ok {
				for _, t := range targets {
					if resp.Topics[t.idx].ErrorCode == 0 {
						resp.Topics[t.idx].ErrorCode = kerr.NotController.Code
					}
				}
			} else {
				c.topicsVer++
			}
			k.respond(resp)
		},
	})
}

func (c *Core) listOffsets(req *kmsg.ListOffsetsRequest) *kmsg.ListOffsetsResponse {
	resp := req.ResponseKind().(*kmsg.ListOffsetsResponse)
	for _, rt := range req.Topics {
		t := kmsg.NewListOffsetsResponseTopic()
		t.Topic = rt.Topic
		tid, known := c.st.ByName[rt.Topic]
		for _, rp := range rt.Partitions {
			p := kmsg.NewListOffsetsResponseTopicPartition()
			p.Partition = rp.Partition
			part := c.st.Partition(tid, uint32(rp.Partition))
			if !known || part == nil || rp.Partition < 0 {
				p.ErrorCode = kerr.UnknownTopicOrPartition.Code
				t.Partitions = append(t.Partitions, p)
				continue
			}
			p.LeaderEpoch = 0
			switch rp.Timestamp {
			case -1: // latest
				p.Offset, p.Timestamp = part.Next, -1
			case -2: // earliest
				p.Offset, p.Timestamp = part.LogStart, -1
			default:
				// Batch granularity: the first batch whose max timestamp is
				// at or after the target.
				p.Offset, p.Timestamp = -1, -1
				for _, r := range part.Refs {
					if r.MaxTs >= rp.Timestamp {
						p.Offset, p.Timestamp = max(r.BaseOffset, part.LogStart), r.MaxTs
						break
					}
				}
			}
			if req.Version == 0 {
				p.OldStyleOffsets = []int64{p.Offset}
			}
			t.Partitions = append(t.Partitions, p)
		}
		resp.Topics = append(resp.Topics, t)
	}
	return resp
}

func (c *Core) findCoordinator(k *kctx, req *kmsg.FindCoordinatorRequest) *kmsg.FindCoordinatorResponse {
	resp := req.ResponseKind().(*kmsg.FindCoordinatorResponse)
	keys := req.CoordinatorKeys
	if req.Version < 4 {
		keys = []string{req.CoordinatorKey}
	}
	nodes := c.nodeIDs()
	for _, key := range keys {
		co := kmsg.NewFindCoordinatorResponseCoordinator()
		co.Key = key
		node := placement.Coordinator(nodes, key)
		var a *pb.AgentInfo
		if node >= 0 {
			a = c.memberByNode(node)
		}
		host, port, ok := "", int32(0), false
		if a != nil {
			host, port, ok = c.listenerAddr(a, k.req.Listener)
		}
		switch {
		case req.CoordinatorType != 0:
			co.ErrorCode, co.ErrorMessage = kerr.CoordinatorNotAvailable.Code, strp("transactions are not supported")
			co.NodeID = -1
		case !ok:
			co.ErrorCode, co.NodeID = kerr.CoordinatorNotAvailable.Code, -1
		default:
			co.NodeID, co.Host, co.Port = node, host, port
		}
		resp.Coordinators = append(resp.Coordinators, co)
	}
	if req.Version < 4 {
		co := resp.Coordinators[0]
		resp.ErrorCode, resp.ErrorMessage, resp.NodeID, resp.Host, resp.Port = co.ErrorCode, co.ErrorMessage, co.NodeID, co.Host, co.Port
		resp.Coordinators = nil
	}
	return resp
}

func (c *Core) initProducerID(k *kctx, req *kmsg.InitProducerIDRequest) {
	resp := req.ResponseKind().(*kmsg.InitProducerIDResponse)
	if req.TransactionalID != nil {
		resp.ErrorCode = kerr.TransactionalIDAuthorizationFailed.Code
		resp.ProducerID, resp.ProducerEpoch = -1, -1
		k.respond(resp)
		return
	}
	// Every allocation is logged, so producer IDs never repeat, even
	// across failovers.
	c.enqueue(&queued{
		plan: func(pl *planner) {
			resp.ProducerID, resp.ProducerEpoch = pl.nextPID, 0
			pl.nextPID++
			pl.add(kindAlloc, &pb.Op{Body: &pb.Op_AllocPids{AllocPids: &pb.AllocProducerIds{Next: pl.nextPID}}})
		},
		done: func(ok bool) {
			if !ok {
				resp.ErrorCode, resp.ProducerID, resp.ProducerEpoch = kerr.NotCoordinator.Code, -1, -1
			}
			k.respond(resp)
		},
	})
}

func (c *Core) describeConfigs(req *kmsg.DescribeConfigsRequest) *kmsg.DescribeConfigsResponse {
	resp := req.ResponseKind().(*kmsg.DescribeConfigsResponse)
	for _, rr := range req.Resources {
		r := kmsg.NewDescribeConfigsResponseResource()
		r.ResourceType, r.ResourceName = rr.ResourceType, rr.ResourceName
		var kvs [][2]string
		switch rr.ResourceType {
		case kmsg.ConfigResourceTypeTopic:
			id, ok := c.st.ByName[rr.ResourceName]
			if !ok {
				r.ErrorCode = kerr.UnknownTopicOrPartition.Code
				break
			}
			t := c.st.Topics[id]
			ret := t.Retention
			if ret == 0 {
				ret = -1
			}
			kvs = [][2]string{
				{"cleanup.policy", "delete"},
				{"retention.ms", strconv.FormatInt(ret, 10)},
				{"max.message.bytes", "1048588"},
				{"message.timestamp.type", "CreateTime"},
			}
		case kmsg.ConfigResourceTypeBroker:
			kvs = [][2]string{{"auto.create.topics.enable", "false"}}
		default:
			r.ErrorCode = kerr.InvalidRequest.Code
		}
		for _, kv := range kvs {
			if len(rr.ConfigNames) > 0 && !contains(rr.ConfigNames, kv[0]) {
				continue
			}
			cfg := kmsg.NewDescribeConfigsResponseResourceConfig()
			cfg.Name, cfg.Value = kv[0], strp(kv[1])
			cfg.ReadOnly = true
			cfg.Source = kmsg.ConfigSourceDynamicTopicConfig
			r.Configs = append(r.Configs, cfg)
		}
		resp.Resources = append(resp.Resources, r)
	}
	return resp
}

func hasConfig(cs []kmsg.CreateTopicsRequestTopicConfig, name string) bool {
	for _, c := range cs {
		if c.Name == name {
			return true
		}
	}
	return false
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
