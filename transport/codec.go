package transport

import (
	"encoding/binary"
	"io"
)

var byteOrder = binary.BigEndian

// frameHeaderSize is the length prefix size (4 bytes = max ~4GB payload)
const frameHeaderSize = 4

// MaxFrameSize is the maximum allowed frame payload size (4MB) to avoid abuse.
const MaxFrameSize = 4 * 1024 * 1024

// EncodeFrame writes length-prefixed data to w: 4-byte big-endian length + payload.
// It flushes only if w implements io.Flusher (e.g. *bufio.Writer).
func EncodeFrame(w io.Writer, payload []byte) error {
	length := uint32(len(payload))
	if length > MaxFrameSize {
		return ErrFrameTooLarge
	}
	header := make([]byte, frameHeaderSize)
	byteOrder.PutUint32(header, length)
	if _, err := w.Write(header); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	if f, ok := w.(interface{ Flush() error }); ok {
		return f.Flush()
	}
	return nil
}

// DecodeFrame reads a length-prefixed frame from r and returns the payload.
func DecodeFrame(r io.Reader) ([]byte, error) {
	header := make([]byte, frameHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	length := byteOrder.Uint32(header)
	if length > MaxFrameSize {
		return nil, ErrFrameTooLarge
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}
