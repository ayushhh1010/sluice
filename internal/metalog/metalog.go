// Package metalog defines the bucket key layout and the encoding of metadata
// log entries and snapshots, plus synchronous helpers for offline tools
// (sluicectl, the checker) that read a bucket directly.
//
// Layout under sluice/<cluster>/:
//
//	data/<yyyy>/<mm>/<dd>/<agent-id>-<ulid>.seg           agent segments
//	merged/<topic-id>/<partition>/<base>-<ulid>.seg       merged, one partition
//	meta/log/<seq:020d>.entry                             metadata log
//	meta/snap/<seq:020d>.snap                             snapshots
package metalog

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/ayushhh1010/sluice/gen/sluice/v1"
)

// Keys builds object keys for one cluster.
type Keys struct{ root string }

func NewKeys(cluster string) Keys { return Keys{root: "sluice/" + cluster + "/"} }

func (k Keys) Root() string         { return k.root }
func (k Keys) DataPrefix() string   { return k.root + "data/" }
func (k Keys) MergedPrefix() string { return k.root + "merged/" }
func (k Keys) LogPrefix() string    { return k.root + "meta/log/" }
func (k Keys) SnapPrefix() string   { return k.root + "meta/snap/" }

func (k Keys) Log(seq uint64) string  { return fmt.Sprintf("%smeta/log/%020d.entry", k.root, seq) }
func (k Keys) Snap(seq uint64) string { return fmt.Sprintf("%smeta/snap/%020d.snap", k.root, seq) }

// Data returns a new agent segment key.
func (k Keys) Data(nowMs int64, agent string, ulid string) string {
	t := time.UnixMilli(nowMs).UTC()
	return fmt.Sprintf("%sdata/%04d/%02d/%02d/%s-%s.seg", k.root, t.Year(), t.Month(), t.Day(), agent, ulid)
}

// Merged returns a new merged segment key.
func (k Keys) Merged(topicID []byte, partition uint32, base int64, ulid string) string {
	return fmt.Sprintf("%smerged/%s/%d/%020d-%s.seg", k.root, hex.EncodeToString(topicID), partition, base, ulid)
}

// SeqFromKey parses the sequence number out of a log or snapshot key.
func SeqFromKey(key string) (uint64, bool) {
	i := strings.LastIndexByte(key, '/')
	j := strings.IndexByte(key[i+1:], '.')
	if i < 0 || j < 0 {
		return 0, false
	}
	n, err := strconv.ParseUint(key[i+1:i+1+j], 10, 64)
	return n, err == nil
}

var marshal = proto.MarshalOptions{Deterministic: true}

func EncodeEntry(e *pb.Entry) []byte {
	b, err := marshal.Marshal(e)
	if err != nil {
		panic(err) // only fails for invalid UTF-8 or unknown types; a bug
	}
	return b
}

func DecodeEntry(b []byte) (*pb.Entry, error) {
	e := &pb.Entry{}
	return e, proto.Unmarshal(b, e)
}

func EncodeSnapshot(s *pb.Snapshot) []byte {
	b, err := marshal.Marshal(s)
	if err != nil {
		panic(err)
	}
	return b
}

func DecodeSnapshot(b []byte) (*pb.Snapshot, error) {
	s := &pb.Snapshot{}
	return s, proto.Unmarshal(b, s)
}

// Crockford's base32 alphabet, used by ULIDs.
const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// ULID formats a 48-bit millisecond timestamp and 80 random bits as a
// 26-character ULID. Lexical order follows time order.
func ULID(ms int64, r1, r2 uint64) string {
	var b [16]byte
	t := uint64(ms)
	for i := 5; i >= 0; i-- {
		b[i] = byte(t)
		t >>= 8
	}
	for i := 0; i < 8; i++ {
		b[6+i] = byte(r1 >> (8 * i))
	}
	b[14], b[15] = byte(r2), byte(r2>>8)
	// 128 bits -> 26 base32 chars; the first char carries 3 bits.
	var out [26]byte
	var acc uint64
	bits := 0
	j := 25
	for i := 15; i >= 0; i-- {
		acc |= uint64(b[i]) << bits
		bits += 8
		for bits >= 5 {
			out[j] = crockford[acc&31]
			acc >>= 5
			bits -= 5
			j--
		}
	}
	out[0] = crockford[acc&31]
	return string(out[:])
}

// ULIDTime extracts the millisecond timestamp from a ULID.
func ULIDTime(u string) (int64, bool) {
	if len(u) != 26 {
		return 0, false
	}
	var t uint64
	for _, c := range u[:10] {
		v := strings.IndexRune(crockford, c)
		if v < 0 {
			return 0, false
		}
		t = t<<5 | uint64(v)
	}
	return int64(t), true
}
