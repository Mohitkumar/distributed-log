package segment

import (
	"io"
)

type LogEntry struct {
	Offset uint64
	Value  []byte
}

func Decode(data []byte) (*LogEntry, error) {
	// Payload-only (e.g. from Segment.Read): no header, entire slice is value
	if len(data) < 12 {
		return &LogEntry{Value: data}, nil
	}
	offset := endian.Uint64(data[0:8])
	msgLen := endian.Uint32(data[8:12])
	if len(data) < 12+int(msgLen) {
		return nil, io.ErrUnexpectedEOF
	}
	out := make([]byte, msgLen)
	copy(out, data[12:12+msgLen])
	return &LogEntry{
		Offset: offset,
		Value:  out,
	}, nil
}
