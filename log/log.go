package log

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/mohitkumar/mlog/errs"
	"github.com/mohitkumar/mlog/segment"
)

type Log struct {
	mu            sync.RWMutex
	Dir           string
	segments      []*segment.Segment
	activeSegment *segment.Segment
}

func NewLog(dir string) (*Log, error) {
	log := &Log{
		Dir:      dir,
		segments: make([]*segment.Segment, 0),
	}

	err := os.MkdirAll(dir, 0755)

	if err != nil {
		fmt.Printf("Error creating directory: %v\n", err)
		return nil, err
	}

	dirEnt, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	if len(dirEnt) > 0 {
		// Load existing segments
		var baseOffsets []uint64
		for _, entry := range dirEnt {
			var baseOffset uint64
			n, err := fmt.Sscanf(entry.Name(), "%020d.log", &baseOffset)
			if n == 1 && err == nil {
				baseOffsets = append(baseOffsets, baseOffset)
			}
		}
		sort.Slice(baseOffsets, func(i int, j int) bool {
			return baseOffsets[i] < baseOffsets[j]
		})
		for _, baseOffset := range baseOffsets {
			seg, err := segment.LoadExistingSegment(baseOffset, dir)
			if err != nil {
				return nil, err
			}
			log.segments = append(log.segments, seg)
		}
		log.activeSegment = log.segments[len(log.segments)-1]
		return log, nil
	}
	// No existing segments, create a new one
	activeSegment, err := segment.NewSegment(0, dir)
	if err != nil {
		return nil, err
	}
	log.segments = append(log.segments, activeSegment)
	log.activeSegment = activeSegment
	return log, nil
}

// findSegment returns the segment containing offset using binary search. O(log n).
// Must be called with l.mu held (read or write).
func (l *Log) findSegment(offset uint64) *segment.Segment {
	n := len(l.segments)
	if n == 0 {
		return nil
	}
	// Binary search: find the last segment with BaseOffset <= offset
	i := sort.Search(n, func(i int) bool {
		return l.segments[i].BaseOffset > offset
	}) - 1
	if i < 0 {
		return nil
	}
	seg := l.segments[i]
	if offset >= seg.NextOffset {
		return nil
	}
	return seg
}

// findSegmentIndex returns the index of the segment containing offset using binary search.
// Returns -1 if not found. Must be called with l.mu held.
func (l *Log) findSegmentIndex(offset uint64) int {
	n := len(l.segments)
	if n == 0 {
		return -1
	}
	i := sort.Search(n, func(i int) bool {
		return l.segments[i].BaseOffset > offset
	}) - 1
	if i < 0 {
		return -1
	}
	if offset >= l.segments[i].NextOffset {
		return -1
	}
	return i
}

// rollSegment creates a new active segment. The old segment stays open for reads;
// it is only closed on Log.Close(), Delete(), or Truncate().
// Must be called with l.mu held for writing.
func (l *Log) rollSegment() error {
	newSeg, err := segment.NewSegment(l.activeSegment.NextOffset, l.Dir)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, newSeg)
	l.activeSegment = newSeg
	return nil
}

func (l *Log) Append(value []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	off, err := l.activeSegment.Append(value)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsFull() {
		if err := l.rollSegment(); err != nil {
			return 0, err
		}
	}
	return off, nil
}

// AppendBatch writes multiple records and returns the base offset.
// All records in the batch are written to the current segment in a single syscall.
func (l *Log) AppendBatch(values [][]byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	baseOff, err := l.activeSegment.AppendBatch(values)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsFull() {
		if err := l.rollSegment(); err != nil {
			return 0, err
		}
	}
	return baseOff, nil
}

func (l *Log) Read(offset uint64) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	seg := l.findSegment(offset)
	if seg == nil {
		return nil, errs.ErrLogOffsetOutOfRangef(offset)
	}
	return seg.Read(offset)
}

func (l *Log) LowestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.segments) == 0 {
		return 0
	}
	return l.segments[0].BaseOffset
}

// IsEmpty returns true if the log has no entries (no Append has been called).
// Used by Raft log store to distinguish "no entries" from "first entry at offset 0".
func (l *Log) IsEmpty() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.segments) == 0 {
		return true
	}
	seg := l.segments[0]
	return seg.NextOffset == seg.BaseOffset
}

func (l *Log) HighestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.activeSegment == nil {
		return 0
	}
	return l.activeSegment.MaxOffset
}

func (l *Log) SegmentCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.segments)
}

func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*segment.Segment
	for _, s := range l.segments {
		if s.NextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	if len(segments) == 0 {
		newSeg, err := segment.NewSegment(lowest+1, l.Dir)
		if err != nil {
			return err
		}
		l.activeSegment = newSeg
	} else {
		l.activeSegment = segments[len(segments)-1]
	}
	return nil
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, seg := range l.segments {
		if err := seg.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()
	readers := make([]io.Reader, 0, len(l.segments))
	for _, seg := range l.segments {
		r := seg.Reader()
		readers = append(readers, r)
	}
	return io.MultiReader(readers...)
}

func (l *Log) ReaderFrom(startOffset uint64) (io.Reader, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	endOffset := uint64(0)
	if l.activeSegment != nil {
		endOffset = l.activeSegment.NextOffset
	}
	if startOffset >= endOffset {
		return bytes.NewReader(nil), nil
	}
	targetIdx := l.findSegmentIndex(startOffset)
	if targetIdx < 0 {
		return nil, errs.ErrLogOffsetOutOfRangef(startOffset)
	}
	seg := l.segments[targetIdx]
	r, err := seg.NewStreamingReader(startOffset)
	if err != nil {
		return nil, err
	}
	if targetIdx == len(l.segments)-1 {
		return r, nil
	}
	readers := make([]io.Reader, 0, len(l.segments)-targetIdx)
	readers = append(readers, r)
	for i := targetIdx + 1; i < len(l.segments); i++ {
		readers = append(readers, l.segments[i].Reader())
	}
	return io.MultiReader(readers...), nil
}

func (l *Log) Delete() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	// Close file handles first
	for _, seg := range l.segments {
		err := seg.Close()
		if err != nil {
			return err
		}
	}
	// Remove the whole directory recursively
	err := os.RemoveAll(l.Dir)
	if err != nil {
		return err
	}
	return nil
}
