package segment

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	IndexIntervalBytes = 4 * 1024    // 4KB
	MaxSegmentBytes    = 1024 * 1024 // 1MB
	WriteBufferSize    = 64 * 1024   // 64KB

	lenWidth         = 4 // 4 bytes for message length
	offWidth         = 8
	totalHeaderWidth = lenWidth + offWidth
)

var endian = binary.BigEndian

// Segement represents a log segment consisting of a log file and an index file.
// Base offset is the starting offset of the segment, NextOffset is the next offset to be assigned
// Each log file is named as {baseOffset}.log and each index file is named as {baseOffset}.idx
type Segment struct {
	BaseOffset          uint64 //base offset of the segment
	NextOffset          uint64 //next offset of the segment
	MaxOffset           uint64 //max offset of the segment
	logFile             *os.File
	bufWriter           *bufio.Writer // buffered writer for log file
	index               *Index
	bytesSinceLastIndex uint64
	writePos            int64      // current end-of-log position
	mu                  sync.Mutex // protects writes
}

func NewSegment(baseOffset uint64, dir string) (*Segment, error) {
	logFilePath := dir + "/" + formatLogFileName(baseOffset)
	logFile, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	indexFilePath := dir + "/" + formatIndexFileName(baseOffset)
	index, err := OpenIndex(indexFilePath)
	if err != nil {
		logFile.Close()
		return nil, err
	}
	return &Segment{
		BaseOffset: baseOffset,
		NextOffset: baseOffset,
		logFile:    logFile,
		bufWriter:  bufio.NewWriterSize(logFile, WriteBufferSize),
		index:      index,
		writePos:   0,
	}, nil
}

func LoadExistingSegment(baseOffset uint64, dir string) (*Segment, error) {
	logFilePath := dir + "/" + formatLogFileName(baseOffset)
	logFile, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	indexFilePath := dir + "/" + formatIndexFileName(baseOffset)
	index, err := OpenIndex(indexFilePath)
	if err != nil {
		logFile.Close()
		return nil, err
	}
	segment := &Segment{
		BaseOffset: baseOffset,
		NextOffset: baseOffset,
		logFile:    logFile,
		bufWriter:  bufio.NewWriterSize(logFile, WriteBufferSize),
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
	header := make([]byte, offWidth+lenWidth)
	endian.PutUint64(header[0:offWidth], offset)
	endian.PutUint32(header[offWidth:totalHeaderWidth], uint32(len(value)))
	_, err := s.bufWriter.Write(header)
	if err != nil {
		return 0, err
	}
	_, err = s.bufWriter.Write(value)
	if err != nil {
		return 0, err
	}

	s.bytesSinceLastIndex += uint64(totalHeaderWidth + len(value))
	if s.bytesSinceLastIndex >= IndexIntervalBytes || offset == s.BaseOffset {
		// Flush buffer before indexing to ensure data is written
		if err := s.bufWriter.Flush(); err != nil {
			return 0, err
		}
		if err := s.index.Write(uint32(offset-s.BaseOffset), uint64(s.writePos)); err != nil {
			return 0, err
		}
		s.bytesSinceLastIndex = 0
	}
	s.writePos += int64(totalHeaderWidth + len(value))
	s.NextOffset++
	s.MaxOffset = offset
	return offset, nil
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
	s.mu.Lock()
	if s.bufWriter != nil {
		_ = s.bufWriter.Flush()
	}
	writePos := s.writePos
	s.mu.Unlock()

	if offset < s.BaseOffset || offset >= s.NextOffset {
		return nil, fmt.Errorf("offset %d out of range [%d, %d)", offset, s.BaseOffset, s.NextOffset)
	}

	// Start position: use index if we have an entry (sparse index gives position of record <= offset),
	// otherwise start from the beginning of the segment file and do a full sequential search.
	var currPos int64
	relOffset := uint32(offset - s.BaseOffset)
	if indexEntry, found := s.index.Find(relOffset); found {
		currPos = int64(indexEntry.Position)
	} else {
		currPos = 0
	}

	for currPos < writePos {
		// Read header: [Offset (8 bytes)][Len (4 bytes)]
		header := make([]byte, totalHeaderWidth)
		n, err := s.logFile.ReadAt(header, currPos)
		if err != nil {
			return nil, err
		}
		if n < totalHeaderWidth {
			return nil, io.ErrUnexpectedEOF
		}
		foundOffset := endian.Uint64(header[0:offWidth])
		msgLen := endian.Uint32(header[offWidth:totalHeaderWidth])

		if foundOffset == offset {
			// Found the record: read and return value only
			if currPos+int64(totalHeaderWidth)+int64(msgLen) > writePos {
				return nil, io.ErrUnexpectedEOF
			}
			value := make([]byte, offWidth+msgLen)
			endian.PutUint64(header[0:offWidth], foundOffset)
			_, err = s.logFile.ReadAt(value[offWidth:], currPos+totalHeaderWidth)
			return value, err
		}
		if foundOffset > offset {
			return nil, fmt.Errorf("offset not found")
		}
		currPos += int64(totalHeaderWidth) + int64(msgLen)
	}
	return nil, io.EOF
}

// Reader returns an io.Reader that streams all records in the segment from BaseOffset.
// The stream format is: for each record, [Offset 8 bytes][Len 4 bytes][Value].
// If the segment is empty, returns a reader that yields EOF.
func (s *Segment) Reader() io.Reader {
	s.mu.Lock()
	next := s.NextOffset
	s.mu.Unlock()
	if s.BaseOffset >= next {
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
func (s *Segment) NewStreamingReader(startOffset uint64) (io.Reader, error) {
	s.mu.Lock()
	//Flush Go's buffer so the reader can see the most recent appends
	s.bufWriter.Flush()
	currentWritePos := s.writePos
	s.mu.Unlock()

	//Range check
	if startOffset < s.BaseOffset || startOffset >= s.NextOffset {
		return nil, fmt.Errorf("offset %d out of range", startOffset)
	}

	// Use the index to find the starting physical position
	relOffset := uint32(startOffset - s.BaseOffset)
	indexEntry, ok := s.index.Find(relOffset)
	if !ok {
		return nil, errors.New("index not found")
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
	if !ok {
		return errors.New("index not found")
	}
	startPos = int64(lastEntry.Position)
	nextOffset = s.BaseOffset + uint64(lastEntry.RelativeOffset)

	// 2. Seek to the checkpoint
	if _, err := s.logFile.Seek(startPos, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek: %w", err)
	}

	// Use a buffered reader for recovery to avoid thousands of small syscalls
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

		// Verification: Can we actually read the full payload?
		// We use Discard to advance the reader without allocating memory for the payload
		if _, err := reader.Discard(int(entrySize)); err != nil {
			break // Partial record at end of file
		}

		currPos += entrySize
		currOffset++
	}

	// 3. Truncate the log to the last confirmed valid byte
	if err := s.logFile.Truncate(currPos); err != nil {
		return fmt.Errorf("truncate failed: %w", err)
	}

	// 4. SYNC FILE POINTER: Crucial for bufio.Writer
	// Truncate doesn't update the file's internal cursor.
	if _, err := s.logFile.Seek(currPos, io.SeekStart); err != nil {
		return err
	}

	// 5. Reset State
	s.writePos = currPos
	s.NextOffset = currOffset

	// Reset the production writer to the new EOF
	if s.bufWriter != nil {
		s.bufWriter.Reset(s.logFile)
	}

	// 6. Clean Index
	// Remove any index entries that point to the truncated/corrupt area
	if err := s.index.TruncateAfter(uint64(currPos)); err != nil {
		return fmt.Errorf("index sync failed: %w", err)
	}

	return nil
}

func (s *Segment) IsFull() bool {
	s.mu.Lock()
	pos := s.writePos
	s.mu.Unlock()
	return pos >= MaxSegmentBytes
}

func (s *Segment) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.bufWriter.Flush(); err != nil {
		return err
	}
	return nil
}

func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.bufWriter != nil {
		if err := s.bufWriter.Flush(); err != nil {
			return err
		}
	}
	if err := s.logFile.Close(); err != nil {
		return err
	}
	if err := s.index.Close(); err != nil {
		return err
	}
	return nil
}
