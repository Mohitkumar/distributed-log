package segment

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/mohitkumar/mlog/errs"
)

const (
	IndexIntervalBytes = 4 * 1024    // 4KB
	MaxSegmentBytes    = 1024 * 1024 // 1MB

	lenWidth         = 4 // 4 bytes for message length
	offWidth         = 8
	totalHeaderWidth = lenWidth + offWidth
)

var endian = binary.BigEndian

// Segment represents a log segment consisting of a log file and an index file.
// Base offset is the starting offset of the segment, NextOffset is the next offset to be assigned.
// Each log file is named as {baseOffset}.log and each index file is named as {baseOffset}.idx
type Segment struct {
	BaseOffset          uint64 // base offset of the segment
	NextOffset          uint64 // next offset of the segment
	MaxOffset           uint64 // max offset of the segment
	logFile             *os.File
	index               *Index
	bytesSinceLastIndex uint64
	writePos            atomic.Int64 // current end-of-log position in bytes
	mu                  sync.Mutex   // protects writes (NextOffset, MaxOffset, bytesSinceLastIndex, index)
}

func NewSegment(baseOffset uint64, dir string) (*Segment, error) {
	logFilePath := filepath.Join(dir, formatLogFileName(baseOffset))
	logFile, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	indexFilePath := filepath.Join(dir, formatIndexFileName(baseOffset))
	index, err := OpenIndex(indexFilePath)
	if err != nil {
		logFile.Close()
		return nil, err
	}
	return &Segment{
		BaseOffset: baseOffset,
		NextOffset: baseOffset,
		logFile:    logFile,
		index:      index,
	}, nil
}

func LoadExistingSegment(baseOffset uint64, dir string) (*Segment, error) {
	logFilePath := filepath.Join(dir, formatLogFileName(baseOffset))
	logFile, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	indexFilePath := filepath.Join(dir, formatIndexFileName(baseOffset))
	index, err := OpenIndex(indexFilePath)
	if err != nil {
		logFile.Close()
		return nil, err
	}
	segment := &Segment{
		BaseOffset: baseOffset,
		NextOffset: baseOffset,
		logFile:    logFile,
		index:      index,
	}
	if err := segment.Recover(); err != nil {
		logFile.Close()
		index.Close()
		return nil, err
	}
	return segment, nil
}

func formatLogFileName(baseOffset uint64) string {
	return fmt.Sprintf("%020d.log", baseOffset)
}

func formatIndexFileName(baseOffset uint64) string {
	return fmt.Sprintf("%020d.idx", baseOffset)
}

func (s *Segment) Append(value []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	offset := s.NextOffset
	buf := make([]byte, totalHeaderWidth+len(value))
	endian.PutUint64(buf[0:offWidth], offset)
	endian.PutUint32(buf[offWidth:totalHeaderWidth], uint32(len(value)))
	copy(buf[totalHeaderWidth:], value)
	if _, err := writeFull(s.logFile, buf); err != nil {
		return 0, err
	}

	currWritePos := s.writePos.Load()
	s.bytesSinceLastIndex += uint64(totalHeaderWidth + len(value))
	if s.bytesSinceLastIndex >= IndexIntervalBytes || offset == s.BaseOffset {
		if err := s.index.Write(uint32(offset-s.BaseOffset), uint64(currWritePos)); err != nil {
			return 0, err
		}
		s.bytesSinceLastIndex = 0
	}
	s.writePos.Add(int64(totalHeaderWidth + len(value)))
	s.NextOffset++
	s.MaxOffset = offset
	return offset, nil
}

// AppendBatch writes multiple records in a single syscall and returns the base offset.
// Offsets are sequential: baseOffset, baseOffset+1, ..., baseOffset+len(values)-1.
func (s *Segment) AppendBatch(values [][]byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(values) == 0 {
		return s.NextOffset, nil
	}

	baseOffset := s.NextOffset

	// Calculate total buffer size
	totalSize := 0
	for _, v := range values {
		totalSize += totalHeaderWidth + len(v)
	}

	// Build combined buffer — one allocation, one syscall
	buf := make([]byte, totalSize)
	pos := 0
	for i, value := range values {
		offset := baseOffset + uint64(i)
		endian.PutUint64(buf[pos:pos+offWidth], offset)
		endian.PutUint32(buf[pos+offWidth:pos+totalHeaderWidth], uint32(len(value)))
		copy(buf[pos+totalHeaderWidth:], value)
		pos += totalHeaderWidth + len(value)
	}

	// Single write syscall for the entire batch
	if _, err := writeFull(s.logFile, buf); err != nil {
		return 0, err
	}

	// Update index entries
	currWritePos := s.writePos.Load()
	pos = 0
	for i, value := range values {
		offset := baseOffset + uint64(i)
		recordSize := totalHeaderWidth + len(value)
		s.bytesSinceLastIndex += uint64(recordSize)
		if s.bytesSinceLastIndex >= IndexIntervalBytes || offset == s.BaseOffset {
			if err := s.index.Write(uint32(offset-s.BaseOffset), uint64(currWritePos+int64(pos))); err != nil {
				return 0, err
			}
			s.bytesSinceLastIndex = 0
		}
		pos += recordSize
	}

	s.writePos.Add(int64(totalSize))
	count := uint64(len(values))
	s.NextOffset = baseOffset + count
	s.MaxOffset = baseOffset + count - 1
	return baseOffset, nil
}

func (s *Segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.file.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.logFile.Name()); err != nil {
		return err
	}
	return nil
}

func (s *Segment) Read(offset uint64) ([]byte, error) {
	writePos := s.writePos.Load()

	if offset < s.BaseOffset || offset >= s.NextOffset {
		return nil, errs.ErrSegmentOffsetOutOfRange(offset, s.BaseOffset, s.NextOffset)
	}

	// Use sparse index to find starting position (floor entry <= offset)
	var startPos int64
	relOffset := uint32(offset - s.BaseOffset)
	if indexEntry, found := s.index.Find(relOffset); found {
		startPos = int64(indexEntry.Position)
	}

	// Bulk read the region from startPos to writePos into memory — one syscall
	regionSize := writePos - startPos
	region := make([]byte, regionSize)
	n, err := s.logFile.ReadAt(region, startPos)
	if err != nil && err != io.EOF {
		return nil, err
	}
	region = region[:n]

	// Scan in-memory buffer — no further syscalls
	pos := 0
	for pos+totalHeaderWidth <= len(region) {
		foundOffset := endian.Uint64(region[pos : pos+offWidth])
		msgLen := int(endian.Uint32(region[pos+offWidth : pos+totalHeaderWidth]))

		if foundOffset == offset {
			end := pos + totalHeaderWidth + msgLen
			if end > len(region) {
				return nil, io.ErrUnexpectedEOF
			}
			value := make([]byte, offWidth+msgLen)
			endian.PutUint64(value[0:offWidth], foundOffset)
			copy(value[offWidth:], region[pos+totalHeaderWidth:end])
			return value, nil
		}
		if foundOffset > offset {
			return nil, errs.ErrSegmentOffsetNotFound
		}
		pos += totalHeaderWidth + msgLen
	}
	return nil, io.EOF
}

// Reader returns an io.Reader that streams all records in the segment from BaseOffset.
// The stream format is: for each record, [Offset 8 bytes][Len 4 bytes][Value].
// If the segment is empty, returns a reader that yields EOF.
func (s *Segment) Reader() io.Reader {
	if s.BaseOffset >= s.NextOffset {
		return bytes.NewReader(nil)
	}
	r, err := s.NewStreamingReader(s.BaseOffset)
	if err != nil {
		return bytes.NewReader(nil)
	}
	return r
}

// NewStreamingReader returns an io.Reader starting at the physical position
// corresponding to startOffset. It will read until the current end-of-segment.
// No flush is needed: writes go directly to the file, so the OS page cache is the only buffer.
func (s *Segment) NewStreamingReader(startOffset uint64) (io.Reader, error) {
	currentWritePos := s.writePos.Load()

	//Range check
	if startOffset < s.BaseOffset || startOffset >= s.NextOffset {
		return nil, errs.ErrSegmentOffsetOutOfRangeSimple(startOffset)
	}

	// Use the index to find the starting physical position
	relOffset := uint32(startOffset - s.BaseOffset)
	indexEntry, ok := s.index.Find(relOffset)
	if !ok {
		return nil, errs.ErrSegmentIndexNotFound
	}

	//Calculate how many bytes are available to read from that position
	size := currentWritePos - int64(indexEntry.Position)

	// This acts like a private io.Reader for this consumer.
	// It reads from logFile starting at 'pos' for 'size' bytes.
	section := io.NewSectionReader(s.logFile, int64(indexEntry.Position), size)

	return s.catchUp(section, startOffset)
}

func (s *Segment) catchUp(r io.Reader, target uint64) (io.Reader, error) {
	for {
		// Peek at the header [Offset: 8][Len: 4]
		header := make([]byte, 12)
		if _, err := io.ReadFull(r, header); err != nil {
			return nil, err
		}

		offset := binary.BigEndian.Uint64(header[0:8])
		msgLen := binary.BigEndian.Uint32(header[8:12])

		if offset >= target {
			// We found the start!
			// We need to return a reader that INCLUDES this header we just read.
			// io.MultiReader joins the header back with the rest of the stream.
			return io.MultiReader(bytes.NewReader(header), r), nil
		}

		// Not there yet, skip the payload and keep looking
		if _, err := io.CopyN(io.Discard, r, int64(msgLen)); err != nil {
			return nil, err
		}
	}
}

func (s *Segment) Recover() error {
	var (
		startPos   int64 = 0
		nextOffset       = s.BaseOffset
	)

	// 1. Start from the last healthy index entry
	lastEntry, ok := s.index.Last()
	if ok {
		startPos = int64(lastEntry.Position)
		nextOffset = s.BaseOffset + uint64(lastEntry.RelativeOffset)
	}

	// 2. Seek to the checkpoint
	if _, err := s.logFile.Seek(startPos, io.SeekStart); err != nil {
		return errs.ErrSeekFailed(err)
	}

	// Buffered reader for recovery to avoid thousands of small read syscalls (read path only)
	reader := bufio.NewReader(s.logFile)
	currPos := startPos
	currOffset := nextOffset

	for {
		// Read Header: [Offset: 8][Len: 4]
		header, err := reader.Peek(12)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}

		recOffset := binary.BigEndian.Uint64(header[0:8])
		recLen := binary.BigEndian.Uint32(header[8:12])

		// Logic check: Is this the record we expect next?
		// Also prevents reading partial "ghost" writes from a crash
		if recOffset != currOffset {
			break
		}

		// Calculate total size of this entry
		entrySize := int64(12 + recLen)

		if _, err := reader.Discard(int(entrySize)); err != nil {
			break // Partial record at end of file
		}

		currPos += entrySize
		currOffset++
	}

	if err := s.logFile.Truncate(currPos); err != nil {
		return errs.ErrTruncateFailed(err)
	}

	if _, err := s.logFile.Seek(currPos, io.SeekStart); err != nil {
		return err
	}

	// 5. Reset state
	s.writePos.Store(currPos)
	s.NextOffset = currOffset
	if currOffset > s.BaseOffset {
		s.MaxOffset = currOffset - 1
	}
	// 6. Clean index
	// Remove any index entries that point to the truncated/corrupt area
	if err := s.index.TruncateAfter(uint64(currPos)); err != nil {
		return errs.ErrIndexSyncFailed(err)
	}

	return nil
}

func (s *Segment) IsFull() bool {
	return s.writePos.Load() >= MaxSegmentBytes
}

// writeFull writes the entire buffer to f, retrying on short writes.
func writeFull(f *os.File, buf []byte) (int, error) {
	total := 0
	for len(buf) > 0 {
		n, err := f.Write(buf)
		if err != nil {
			return total, err
		}
		total += n
		buf = buf[n:]
	}
	return total, nil
}

func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.logFile.Close(); err != nil {
		return err
	}
	if err := s.index.Close(); err != nil {
		return err
	}
	return nil
}
