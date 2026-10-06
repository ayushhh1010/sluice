// Package kbatch reads, validates and patches Kafka RecordBatch v2 (magic 2)
// headers without decoding records. Batches are stored exactly as clients
// sent them; the only bytes Sluice ever rewrites are baseOffset and
// partitionLeaderEpoch, which the batch CRC does not cover.
package kbatch

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// Header field offsets within a RecordBatch v2.
const (
	offBaseOffset      = 0
	offBatchLength     = 8
	offLeaderEpoch     = 12
	offMagic           = 16
	offCRC             = 17
	offAttributes      = 21
	offLastOffsetDelta = 23
	offBaseTimestamp   = 27
	offMaxTimestamp    = 35
	offProducerID      = 43
	offProducerEpoch   = 51
	offBaseSequence    = 53
	offRecordCount     = 57

	// HeaderSize is the size of the fixed RecordBatch v2 header.
	HeaderSize = 61
	// lengthPrefix is the part of the header not counted by batchLength.
	lengthPrefix = 12
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

var (
	ErrTruncated   = errors.New("kbatch: truncated batch")
	ErrMagic       = errors.New("kbatch: unsupported magic (only RecordBatch v2)")
	ErrCRC         = errors.New("kbatch: CRC32C mismatch")
	ErrRecordCount = errors.New("kbatch: record count does not match lastOffsetDelta")
	ErrControl     = errors.New("kbatch: control and transactional batches are not supported")
)

// Header is the decoded fixed part of a RecordBatch v2.
type Header struct {
	BaseOffset      int64
	Length          int // full size in bytes, including the 12-byte prefix
	Attributes      int16
	LastOffsetDelta int32
	BaseTimestamp   int64
	MaxTimestamp    int64
	ProducerID      int64
	ProducerEpoch   int16
	BaseSequence    int32
	NumRecords      int32
}

// Compression returns the codec bits of the attributes (0 = none).
func (h Header) Compression() int { return int(h.Attributes & 0x7) }

// Records is the number of offsets the batch occupies.
func (h Header) Records() int64 { return int64(h.LastOffsetDelta) + 1 }

// ReadHeader decodes the header of the batch starting at b[0]. It checks
// only that the header and the declared length fit in b.
func ReadHeader(b []byte) (Header, error) {
	if len(b) < HeaderSize {
		return Header{}, ErrTruncated
	}
	n := int(int32(binary.BigEndian.Uint32(b[offBatchLength:])))
	if n < HeaderSize-lengthPrefix || n > len(b)-lengthPrefix {
		return Header{}, ErrTruncated
	}
	if b[offMagic] != 2 {
		return Header{}, ErrMagic
	}
	return Header{
		BaseOffset:      int64(binary.BigEndian.Uint64(b[offBaseOffset:])),
		Length:          n + lengthPrefix,
		Attributes:      int16(binary.BigEndian.Uint16(b[offAttributes:])),
		LastOffsetDelta: int32(binary.BigEndian.Uint32(b[offLastOffsetDelta:])),
		BaseTimestamp:   int64(binary.BigEndian.Uint64(b[offBaseTimestamp:])),
		MaxTimestamp:    int64(binary.BigEndian.Uint64(b[offMaxTimestamp:])),
		ProducerID:      int64(binary.BigEndian.Uint64(b[offProducerID:])),
		ProducerEpoch:   int16(binary.BigEndian.Uint16(b[offProducerEpoch:])),
		BaseSequence:    int32(binary.BigEndian.Uint32(b[offBaseSequence:])),
		NumRecords:      int32(binary.BigEndian.Uint32(b[offRecordCount:])),
	}, nil
}

// Validate checks one complete batch b (len(b) == header length) the way a
// broker must before accepting it: magic, CRC32C, record counts and
// unsupported batch types.
func Validate(b []byte) (Header, error) {
	h, err := ReadHeader(b)
	if err != nil {
		return h, err
	}
	if h.Length != len(b) {
		return h, fmt.Errorf("kbatch: batch length %d, have %d bytes", h.Length, len(b))
	}
	want := binary.BigEndian.Uint32(b[offCRC:])
	if crc32.Checksum(b[offAttributes:], castagnoli) != want {
		return h, ErrCRC
	}
	if h.LastOffsetDelta < 0 || h.NumRecords != h.LastOffsetDelta+1 {
		return h, ErrRecordCount
	}
	// Bit 4: transactional, bit 5: control batch.
	if h.Attributes&(1<<4|1<<5) != 0 {
		return h, ErrControl
	}
	return h, nil
}

// Split splits a concatenation of batches (a Produce partition's records
// field) into individual validated batches. Every batch is checked; any
// invalid batch fails the whole set, as Kafka does.
func Split(b []byte) ([][]byte, []Header, error) {
	var (
		bs [][]byte
		hs []Header
	)
	for len(b) > 0 {
		h, err := ReadHeader(b)
		if err != nil {
			return nil, nil, err
		}
		h, err = Validate(b[:h.Length])
		if err != nil {
			return nil, nil, err
		}
		bs = append(bs, b[:h.Length:h.Length])
		hs = append(hs, h)
		b = b[h.Length:]
	}
	return bs, hs, nil
}

// Patch writes the assigned base offset into a batch and zeroes the
// partition leader epoch. Neither field is covered by the CRC.
func Patch(b []byte, baseOffset int64) {
	binary.BigEndian.PutUint64(b[offBaseOffset:], uint64(baseOffset))
	binary.BigEndian.PutUint32(b[offLeaderEpoch:], 0)
}

// Record is one record for Build.
type Record struct {
	Key, Value []byte
	Headers    [][2][]byte
}

// Build encodes an uncompressed RecordBatch v2 with base offset 0. It is used
// by the playground HTTP API and the load generator; real clients build
// their own batches.
func Build(timestampMs int64, recs []Record) []byte {
	b := make([]byte, HeaderSize, HeaderSize+64*len(recs))
	for i, r := range recs {
		var rec []byte
		rec = append(rec, 0)                     // attributes
		rec = binary.AppendVarint(rec, 0)        // timestamp delta
		rec = binary.AppendVarint(rec, int64(i)) // offset delta
		rec = appendVarBytes(rec, r.Key)
		rec = appendVarBytes(rec, r.Value)
		rec = binary.AppendVarint(rec, int64(len(r.Headers)))
		for _, h := range r.Headers {
			rec = appendVarBytes(rec, h[0])
			rec = appendVarBytes(rec, h[1])
		}
		b = binary.AppendVarint(b, int64(len(rec)))
		b = append(b, rec...)
	}
	binary.BigEndian.PutUint32(b[offBatchLength:], uint32(len(b)-lengthPrefix))
	b[offMagic] = 2
	binary.BigEndian.PutUint32(b[offLastOffsetDelta:], uint32(len(recs)-1))
	binary.BigEndian.PutUint64(b[offBaseTimestamp:], uint64(timestampMs))
	binary.BigEndian.PutUint64(b[offMaxTimestamp:], uint64(timestampMs))
	binary.BigEndian.PutUint64(b[offProducerID:], ^uint64(0)) // -1
	binary.BigEndian.PutUint16(b[offProducerEpoch:], ^uint16(0))
	binary.BigEndian.PutUint32(b[offBaseSequence:], ^uint32(0))
	binary.BigEndian.PutUint32(b[offRecordCount:], uint32(len(recs)))
	binary.BigEndian.PutUint32(b[offCRC:], crc32.Checksum(b[offAttributes:], castagnoli))
	return b
}

// SetProducer stamps idempotent-producer fields into a batch built by
// Build and recomputes its CRC.
func SetProducer(b []byte, producerID int64, epoch int16, baseSeq int32) {
	binary.BigEndian.PutUint64(b[offProducerID:], uint64(producerID))
	binary.BigEndian.PutUint16(b[offProducerEpoch:], uint16(epoch))
	binary.BigEndian.PutUint32(b[offBaseSequence:], uint32(baseSeq))
	binary.BigEndian.PutUint32(b[offCRC:], crc32.Checksum(b[offAttributes:], castagnoli))
}

func appendVarBytes(b, v []byte) []byte {
	if v == nil {
		return binary.AppendVarint(b, -1)
	}
	b = binary.AppendVarint(b, int64(len(v)))
	return append(b, v...)
}

// Records decodes the records of an uncompressed batch. Compressed batches
// return ok=false; Sluice never decompresses client data.
func Records(b []byte) (recs []Record, timestamps []int64, ok bool) {
	h, err := ReadHeader(b)
	if err != nil || h.Compression() != 0 || h.Length > len(b) {
		return nil, nil, false
	}
	p := b[HeaderSize:h.Length]
	for i := int32(0); i < h.NumRecords; i++ {
		n, k := binary.Varint(p)
		if k <= 0 || n < 0 || int64(len(p)-k) < n {
			return nil, nil, false
		}
		rec := p[k : k+int(n)]
		p = p[k+int(n):]
		if len(rec) < 1 {
			return nil, nil, false
		}
		rec = rec[1:] // attributes
		tsDelta, k1 := binary.Varint(rec)
		if k1 <= 0 {
			return nil, nil, false
		}
		rec = rec[k1:]
		if _, k2 := binary.Varint(rec); k2 <= 0 { // offset delta
			return nil, nil, false
		} else {
			rec = rec[k2:]
		}
		var r Record
		var good bool
		if r.Key, rec, good = readVarBytes(rec); !good {
			return nil, nil, false
		}
		if r.Value, rec, good = readVarBytes(rec); !good {
			return nil, nil, false
		}
		nh, k3 := binary.Varint(rec)
		if k3 <= 0 || nh < 0 || nh > int64(len(rec)) {
			return nil, nil, false
		}
		rec = rec[k3:]
		for j := int64(0); j < nh; j++ {
			var hk, hv []byte
			if hk, rec, good = readVarBytes(rec); !good {
				return nil, nil, false
			}
			if hv, rec, good = readVarBytes(rec); !good {
				return nil, nil, false
			}
			r.Headers = append(r.Headers, [2][]byte{hk, hv})
		}
		recs = append(recs, r)
		timestamps = append(timestamps, h.BaseTimestamp+tsDelta)
	}
	return recs, timestamps, true
}

func readVarBytes(b []byte) ([]byte, []byte, bool) {
	n, k := binary.Varint(b)
	if k <= 0 {
		return nil, nil, false
	}
	b = b[k:]
	if n < 0 {
		return nil, b, true
	}
	if n > int64(len(b)) {
		return nil, nil, false
	}
	return b[:n:n], b[n:], true
}
