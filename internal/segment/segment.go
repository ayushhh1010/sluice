// Package segment encodes and decodes SLC1 segment objects.
//
// Layout (all integers big-endian):
//
//	header : magic "SLC1" | version u16 | flags u16 | created_unix_ms i64 | agent_id [16]B
//	body   : Kafka RecordBatch v2 bytes, exactly as received, grouped by (topic, partition)
//	footer : entries[] { topic_id [16]B, partition u32, offset_in_obj u32, length u32,
//	                     records u32, max_ts i64, producer_id i64, producer_epoch i16, base_seq i32 }
//	trailer: footer_len u32 | footer_crc32c u32 | magic "SLC1"
//
// Batch bytes are never re-encoded, so a reader that knows a batch's
// (offset_in_obj, length) can range-GET it without touching the footer.
package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
)

const (
	Magic       = "SLC1"
	Version     = 1
	HeaderSize  = 4 + 2 + 2 + 8 + 16
	EntrySize   = 16 + 4 + 4 + 4 + 4 + 8 + 8 + 2 + 4
	TrailerSize = 4 + 4 + 4
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt is returned for any structurally invalid object.
var ErrCorrupt = errors.New("segment: corrupt object")

// Entry describes one batch inside a segment.
type Entry struct {
	TopicID       [16]byte
	Partition     uint32
	Offset        uint32 // offset_in_obj
	Length        uint32
	Records       uint32
	MaxTS         int64
	ProducerID    int64
	ProducerEpoch int16
	BaseSeq       int32
}

// Header is the fixed segment header.
type Header struct {
	Version   uint16
	Flags     uint16
	CreatedMs int64
	AgentID   [16]byte
}

type pending struct {
	e     Entry
	batch []byte
	order int
}

// Builder accumulates batches for one segment.
type Builder struct {
	hdr   Header
	items []pending
	size  int
}

// NewBuilder starts a segment. agentID is truncated or zero-padded to 16 bytes.
func NewBuilder(createdMs int64, agentID string) *Builder {
	b := &Builder{hdr: Header{Version: Version, CreatedMs: createdMs}}
	copy(b.hdr.AgentID[:], agentID)
	return b
}

// Add appends a batch. Offset and Length in e are filled in by Finish.
func (b *Builder) Add(e Entry, batch []byte) {
	b.items = append(b.items, pending{e: e, batch: batch, order: len(b.items)})
	b.size += len(batch)
}

// Len reports the number of batches added.
func (b *Builder) Len() int { return len(b.items) }

// Size reports the body size in bytes.
func (b *Builder) Size() int { return b.size }

// Finish encodes the segment. Batches are grouped by (topic, partition);
// within a partition they keep insertion order. The returned entries are in
// body order, and Order[i] is the Add index of entries[i].
func (b *Builder) Finish() (obj []byte, entries []Entry, order []int) {
	items := append([]pending(nil), b.items...)
	sort.SliceStable(items, func(i, j int) bool {
		if c := bytes.Compare(items[i].e.TopicID[:], items[j].e.TopicID[:]); c != 0 {
			return c < 0
		}
		return items[i].e.Partition < items[j].e.Partition
	})
	obj = make([]byte, 0, HeaderSize+b.size+len(items)*EntrySize+TrailerSize)
	obj = append(obj, Magic...)
	obj = binary.BigEndian.AppendUint16(obj, b.hdr.Version)
	obj = binary.BigEndian.AppendUint16(obj, b.hdr.Flags)
	obj = binary.BigEndian.AppendUint64(obj, uint64(b.hdr.CreatedMs))
	obj = append(obj, b.hdr.AgentID[:]...)
	entries = make([]Entry, len(items))
	order = make([]int, len(items))
	for i, it := range items {
		e := it.e
		e.Offset = uint32(len(obj))
		e.Length = uint32(len(it.batch))
		obj = append(obj, it.batch...)
		entries[i] = e
		order[i] = it.order
	}
	footStart := len(obj)
	for _, e := range entries {
		obj = append(obj, e.TopicID[:]...)
		obj = binary.BigEndian.AppendUint32(obj, e.Partition)
		obj = binary.BigEndian.AppendUint32(obj, e.Offset)
		obj = binary.BigEndian.AppendUint32(obj, e.Length)
		obj = binary.BigEndian.AppendUint32(obj, e.Records)
		obj = binary.BigEndian.AppendUint64(obj, uint64(e.MaxTS))
		obj = binary.BigEndian.AppendUint64(obj, uint64(e.ProducerID))
		obj = binary.BigEndian.AppendUint16(obj, uint16(e.ProducerEpoch))
		obj = binary.BigEndian.AppendUint32(obj, uint32(e.BaseSeq))
	}
	footer := obj[footStart:]
	obj = binary.BigEndian.AppendUint32(obj, uint32(len(footer)))
	obj = binary.BigEndian.AppendUint32(obj, crc32.Checksum(footer, castagnoli))
	obj = append(obj, Magic...)
	return obj, entries, order
}

// Parse validates an entire segment object and returns its header and
// footer entries. Every entry is checked to lie within the body.
func Parse(obj []byte) (Header, []Entry, error) {
	var h Header
	if len(obj) < HeaderSize+TrailerSize || string(obj[:4]) != Magic || string(obj[len(obj)-4:]) != Magic {
		return h, nil, ErrCorrupt
	}
	h.Version = binary.BigEndian.Uint16(obj[4:])
	h.Flags = binary.BigEndian.Uint16(obj[6:])
	h.CreatedMs = int64(binary.BigEndian.Uint64(obj[8:]))
	copy(h.AgentID[:], obj[16:32])
	if h.Version != Version {
		return h, nil, fmt.Errorf("segment: unsupported version %d", h.Version)
	}
	t := obj[len(obj)-TrailerSize:]
	footLen := int(binary.BigEndian.Uint32(t[0:]))
	footCRC := binary.BigEndian.Uint32(t[4:])
	bodyEnd := len(obj) - TrailerSize - footLen
	if footLen%EntrySize != 0 || bodyEnd < HeaderSize {
		return h, nil, ErrCorrupt
	}
	footer := obj[bodyEnd : len(obj)-TrailerSize]
	if crc32.Checksum(footer, castagnoli) != footCRC {
		return h, nil, ErrCorrupt
	}
	entries := make([]Entry, footLen/EntrySize)
	for i := range entries {
		p := footer[i*EntrySize:]
		var e Entry
		copy(e.TopicID[:], p[:16])
		e.Partition = binary.BigEndian.Uint32(p[16:])
		e.Offset = binary.BigEndian.Uint32(p[20:])
		e.Length = binary.BigEndian.Uint32(p[24:])
		e.Records = binary.BigEndian.Uint32(p[28:])
		e.MaxTS = int64(binary.BigEndian.Uint64(p[32:]))
		e.ProducerID = int64(binary.BigEndian.Uint64(p[40:]))
		e.ProducerEpoch = int16(binary.BigEndian.Uint16(p[48:]))
		e.BaseSeq = int32(binary.BigEndian.Uint32(p[50:]))
		if uint64(e.Offset) < HeaderSize || uint64(e.Offset)+uint64(e.Length) > uint64(bodyEnd) {
			return h, nil, ErrCorrupt
		}
		entries[i] = e
	}
	return h, entries, nil
}

// Batch returns the bytes of entry e within obj.
func Batch(obj []byte, e Entry) []byte { return obj[e.Offset : e.Offset+e.Length] }
