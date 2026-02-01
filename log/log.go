package log

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

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

func (l *Log) Append(value []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	off, err := l.activeSegment.Append(value)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsFull() {
		l.activeSegment, err = segment.NewSegment(l.activeSegment.NextOffset, l.Dir)
		if err != nil {
			return 0, err
		}
		l.segments = append(l.segments, l.activeSegment)
	}
	return off, nil
}

func (l *Log) Read(offset uint64) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var targetSegment *segment.Segment
	for _, seg := range l.segments {
		if offset >= seg.BaseOffset && offset < seg.NextOffset {
			targetSegment = seg
			break
		}
	}
	if targetSegment == nil || offset < targetSegment.BaseOffset || offset >= targetSegment.NextOffset {
		return nil, fmt.Errorf("offset %d out of range", offset)
	}
	r, err := targetSegment.Read(offset)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (l *Log) LowestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].BaseOffset
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
	return len(l.segments)
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
	if l.activeSegment != nil {
		_ = l.activeSegment.Flush()
	}
	endOffset := uint64(0)
	if l.activeSegment != nil {
		endOffset = l.activeSegment.NextOffset
	}
	if startOffset >= endOffset {
		l.mu.RUnlock()
		return bytes.NewReader(nil), nil
	}
	var targetIdx int = -1
	for i, seg := range l.segments {
		if startOffset >= seg.BaseOffset && startOffset < seg.NextOffset {
			targetIdx = i
			break
		}
	}
	if targetIdx < 0 {
		l.mu.RUnlock()
		return nil, fmt.Errorf("offset %d out of range", startOffset)
	}
	seg := l.segments[targetIdx]
	r, err := seg.NewStreamingReader(startOffset)
	if err != nil {
		l.mu.RUnlock()
		return nil, err
	}
	if targetIdx == len(l.segments)-1 {
		l.mu.RUnlock()
		return r, nil
	}
	readers := make([]io.Reader, 0, len(l.segments)-targetIdx)
	readers = append(readers, r)
	for i := targetIdx + 1; i < len(l.segments); i++ {
		readers = append(readers, l.segments[i].Reader())
	}
	l.mu.RUnlock()
	return io.MultiReader(readers...), nil
}

func (l *Log) Delete() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	// Close file handles first
	for _, seg := range l.segments {
		seg.Close()
	}
	// Remove the whole directory recursively
	err := os.RemoveAll(l.Dir)
	if err != nil {
		return err
	}
	return nil
}
