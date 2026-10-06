// Package protocol is the Kafka wire protocol front end of an agent. It owns
// framing, version negotiation, SASL, quotas and request validation, and
// translates Produce and Fetch into agent core requests. Every other
// supported API is forwarded verbatim to the sequencer.
package protocol

import "github.com/twmb/franz-go/pkg/kmsg"

// Versions is the exact API surface ApiVersions advertises, so clients
// never call anything else. Ranges cover what franz-go, librdkafka and
// kafka-python negotiate by default; the compatibility tests pin them.
var Versions = map[int16][2]int16{
	kmsg.Produce.Int16():          {3, 9},  // RecordBatch v2 only
	kmsg.Fetch.Int16():            {4, 12}, // v13+ addresses topics by ID
	kmsg.ListOffsets.Int16():      {1, 6},
	kmsg.Metadata.Int16():         {1, 12},
	kmsg.OffsetCommit.Int16():     {2, 8},
	kmsg.OffsetFetch.Int16():      {1, 7}, // v8 batches groups
	kmsg.FindCoordinator.Int16():  {0, 4},
	kmsg.JoinGroup.Int16():        {0, 9},
	kmsg.Heartbeat.Int16():        {0, 4},
	kmsg.LeaveGroup.Int16():       {0, 5},
	kmsg.SyncGroup.Int16():        {0, 5},
	kmsg.DescribeGroups.Int16():   {0, 5},
	kmsg.ListGroups.Int16():       {0, 4},
	kmsg.SASLHandshake.Int16():    {1, 1},
	kmsg.ApiVersions.Int16():      {0, 3},
	kmsg.CreateTopics.Int16():     {2, 7},
	kmsg.DeleteTopics.Int16():     {1, 6},
	kmsg.InitProducerID.Int16():   {0, 4},
	kmsg.DescribeConfigs.Int16():  {0, 4},
	kmsg.SASLAuthenticate.Int16(): {0, 2},
}

// forwarded APIs are implemented by the sequencer.
var forwarded = map[int16]bool{
	kmsg.ListOffsets.Int16():     true,
	kmsg.Metadata.Int16():        true,
	kmsg.OffsetCommit.Int16():    true,
	kmsg.OffsetFetch.Int16():     true,
	kmsg.FindCoordinator.Int16(): true,
	kmsg.JoinGroup.Int16():       true,
	kmsg.Heartbeat.Int16():       true,
	kmsg.LeaveGroup.Int16():      true,
	kmsg.SyncGroup.Int16():       true,
	kmsg.DescribeGroups.Int16():  true,
	kmsg.ListGroups.Int16():      true,
	kmsg.CreateTopics.Int16():    true,
	kmsg.DeleteTopics.Int16():    true,
	kmsg.InitProducerID.Int16():  true,
	kmsg.DescribeConfigs.Int16(): true,
}

func supported(key, version int16) bool {
	r, ok := Versions[key]
	return ok && version >= r[0] && version <= r[1]
}
