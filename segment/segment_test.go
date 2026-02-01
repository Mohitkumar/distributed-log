package segment

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func setupTestSegment(t *testing.T) (*Segment, string, func()) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "segment")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}
	segment, err := NewSegment(0, dir)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}

	return segment, dir, func() {
		segment.Close()
	}
}
func TestSegmentReadWrite(t *testing.T) {
	segment, _, teardown := setupTestSegment(t)
	defer teardown()

	records := [][]byte{
		[]byte("first record"),
		[]byte("second record"),
		[]byte("third record"),
	}
	segment.Flush()

	var offsets []uint64
	for _, r := range records {
		offset, err := segment.Append(r)
		if err != nil {
			t.Fatalf("failed to append record: %v", err)
		}
		offsets = append(offsets, offset)
	}
	for i, r := range records {
		recBytes, err := segment.Read(offsets[i])
		if err != nil {
			t.Fatalf("failed to read record: %v", err)
		}
		rec, err := Decode(recBytes)
		if err != nil {
			t.Fatalf("failed to read record: %v", err)
		}
		fmt.Printf("%s", rec.Value)
		if string(rec.Value) != string(r) {
			t.Errorf("record mismatch: got %s, want %s", rec.Value, r)
		}
	}
}

func TestSegmentReadWriteLarge(t *testing.T) {
	segment, _, teardown := setupTestSegment(t)
	defer teardown()

	numRecords := 10000
	for i := 0; i < numRecords; i++ {
		value := []byte("record number " + strconv.Itoa(i))
		_, err := segment.Append(value)
		if err != nil {
			t.Fatalf("failed to append record %d: %v", i, err)
		}
	}

	for i := 0; i < numRecords; i++ {
		expectedValue := []byte("record number " + strconv.Itoa(i))
		recBytes, err := segment.Read(uint64(i))
		if err != nil {
			t.Fatalf("failed to read record: %v", err)
		}
		rec, err := Decode(recBytes)
		if err != nil {
			t.Fatalf("failed to read record: %v", err)
		}
		if string(rec.Value) != string(expectedValue) {
			t.Errorf("record %d mismatch: got %s, want %s", i, rec.Value, expectedValue)
		}
	}
}

func TestSegmentOutOfRangeRead(t *testing.T) {
	segment, _, teardown := setupTestSegment(t)
	defer teardown()

	_, err := segment.Read(0)
	if err == nil {
		t.Fatalf("expected error for out-of-range read, got nil")
	}

	_, err = segment.Append([]byte("only record"))
	if err != nil {
		t.Fatalf("failed to append record: %v", err)
	}

	_, err = segment.Read(2)
	if err == nil {
		t.Fatalf("expected error for out-of-range read, got nil")
	}
}

func TestLoadExistingSegment(t *testing.T) {
	segment, dir, teardown := setupTestSegment(t)
	defer teardown()

	records := [][]byte{
		[]byte("first record"),
		[]byte("second record"),
		[]byte("third record"),
	}

	for _, r := range records {
		_, err := segment.Append(r)
		if err != nil {
			t.Fatalf("failed to append record: %v", err)
		}
	}

	if err := segment.Close(); err != nil {
		t.Fatalf("failed to close segment: %v", err)
	}
	loadedSegment, err := LoadExistingSegment(0, dir)
	if err != nil {
		t.Fatalf("failed to load existing segment: %v", err)
	}
	defer loadedSegment.Close()

	for i, r := range records {
		recBytes, err := loadedSegment.Read(uint64(i))
		if err != nil {
			t.Fatalf("failed to read record: %v", err)
		}
		rec, err := Decode(recBytes)
		if err != nil {
			t.Fatalf("failed to read record: %v", err)
		}
		if string(rec.Value) != string(r) {
			t.Errorf("record mismatch: got value: %s), want value: %s)",
				rec.Value, r)
		}
	}
}

func TestLoadExistingSegmentLarge(t *testing.T) {
	segment, dir, teardown := setupTestSegment(t)
	defer teardown()

	numRecords := 10000
	for i := 0; i < numRecords; i++ {
		value := []byte("record number " + strconv.Itoa(i))
		_, err := segment.Append(value)
		if err != nil {
			t.Fatalf("failed to append record %d: %v", i, err)
		}
	}

	if err := segment.Close(); err != nil {
		t.Fatalf("failed to close segment: %v", err)
	}
	loadedSegment, err := LoadExistingSegment(0, dir)
	if err != nil {
		t.Fatalf("failed to load existing segment: %v", err)
	}
	defer loadedSegment.Close()

	for i := 0; i < numRecords; i++ {
		expectedValue := []byte("record number " + strconv.Itoa(i))
		recBytes, err := loadedSegment.Read(uint64(i))
		if err != nil {
			t.Fatalf("failed to read record: %v", err)
		}
		rec, err := Decode(recBytes)
		if err != nil {
			t.Fatalf("failed to read record: %v", err)
		}
		if string(rec.Value) != string(expectedValue) {
			t.Errorf("record %d mismatch: got %s, want %s", i, rec.Value, expectedValue)
		}
	}
}

func TestSegmentNewStreamingReader(t *testing.T) {
	segment, _, teardown := setupTestSegment(t)
	defer teardown()

	records := [][]byte{
		[]byte("first record"),
		[]byte("second record"),
		[]byte("third record"),
	}

	for _, r := range records {
		_, err := segment.Append(r)
		if err != nil {
			t.Fatalf("failed to append record: %v", err)
		}
	}

	reader, err := segment.NewStreamingReader(0)
	if err != nil {
		t.Fatalf("failed to create streaming reader: %v", err)
	}

	for _, r := range records {
		header := make([]byte, 8+4)
		n, err := reader.Read(header)
		require.NoError(t, err)
		require.Equal(t, 12, n)
		size := binary.BigEndian.Uint32(header[8:12])

		payloadBuf := make([]byte, size)
		n, err = reader.Read(payloadBuf)
		require.NoError(t, err)
		require.Equal(t, int(size), n)
		require.Equal(t, string(r), string(payloadBuf))
	}
}

func TestSegmentStreamingReaderFromMiddle(t *testing.T) {
	segment, _, teardown := setupTestSegment(t)
	defer teardown()

	for i := 0; i < 10000; i++ {
		_, err := segment.Append([]byte("test record " + strconv.Itoa(i)))
		if err != nil {
			t.Fatalf("failed to append record: %v", err)
		}
	}

	reader, err := segment.NewStreamingReader(5000)
	if err != nil {
		t.Fatalf("failed to create streaming reader: %v", err)
	}

	for i := 5000; i < 10000; i++ {
		header := make([]byte, 8+4)
		n, err := reader.Read(header)
		require.NoError(t, err)
		require.Equal(t, 12, n)
		size := binary.BigEndian.Uint32(header[8:12])

		payloadBuf := make([]byte, size)
		n, err = reader.Read(payloadBuf)
		require.NoError(t, err)
		require.Equal(t, int(size), n)
		require.Equal(t, string("test record "+strconv.Itoa(i)), string(payloadBuf))
	}

	reader, err = segment.NewStreamingReader(0)
	if err != nil {
		t.Fatalf("failed to create streaming reader: %v", err)
	}
	for i := 0; i < 10000; i++ {
		header := make([]byte, 8+4)
		n, err := reader.Read(header)
		require.NoError(t, err)
		require.Equal(t, 12, n)
		size := binary.BigEndian.Uint32(header[8:12])

		payloadBuf := make([]byte, size)
		n, err = reader.Read(payloadBuf)
		require.NoError(t, err)
		require.Equal(t, int(size), n)
		require.Equal(t, string("test record "+strconv.Itoa(i)), string(payloadBuf))
	}
}

func TestSegmentReader(t *testing.T) {
	segment, _, teardown := setupTestSegment(t)
	defer teardown()

	records := [][]byte{
		[]byte("first record"),
		[]byte("second record"),
		[]byte("third record"),
	}

	for _, r := range records {
		_, err := segment.Append(r)
		if err != nil {
			t.Fatalf("failed to append record: %v", err)
		}
	}
	reader := segment.Reader()
	var readRecords []string
	for i := 0; i < len(records); i++ {
		// Header: offset (8 bytes) + length (4 bytes)
		header := make([]byte, 8+4)
		n, err := reader.Read(header)
		require.NoError(t, err)
		require.Equal(t, 12, n)
		size := binary.BigEndian.Uint32(header[8:12])

		payloadBuf := make([]byte, size)
		n, err = reader.Read(payloadBuf)
		require.NoError(t, err)
		require.Equal(t, int(size), n)
		readRecords = append(readRecords, string(payloadBuf))
	}
	for i, r := range records {
		if string(r) != readRecords[i] {
			t.Errorf("record mismatch: got %s, want %s", readRecords[i], r)
		}
	}
}

func BenchmarkSegmentWrite(b *testing.B) {
	b.TempDir()
	segment, _, teardown := setupTestSegment(&testing.T{})
	defer teardown()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := segment.Append([]byte("test record"))
		if err != nil {
			b.Fatalf("failed to append record: %v", err)
		}
	}
	seconds := b.Elapsed().Seconds()
	if seconds > 0 {
		b.ReportMetric(float64(b.N)/seconds, "req/s")
	}
}

func BenchmarkSegmentRead(b *testing.B) {
	b.TempDir()
	segment, _, teardown := setupTestSegment(&testing.T{})
	defer teardown()
	for i := 0; i < b.N; i++ {
		_, err := segment.Append([]byte("test record"))
		if err != nil {
			b.Fatalf("failed to append record: %v", err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := segment.Read(uint64(i))
		if err != nil {
			b.Fatalf("failed to read record: %v", err)
		}
	}
	seconds := b.Elapsed().Seconds()
	if seconds > 0 {
		b.ReportMetric(float64(b.N)/seconds, "req/s")
	}
}

func BenchmarkSegmentStreamingRead(b *testing.B) {
	b.TempDir()
	segment, _, teardown := setupTestSegment(&testing.T{})
	defer teardown()
	for i := 0; i < b.N; i++ {
		_, err := segment.Append([]byte("test record"))
		if err != nil {
			b.Fatalf("failed to append record: %v", err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := segment.Reader()
		_, err := reader.Read(nil)
		if err != nil {
			b.Fatalf("failed to read record: %v", err)
		}
	}
	seconds := b.Elapsed().Seconds()
	if seconds > 0 {
		b.ReportMetric(float64(b.N)/seconds, "req/s")
	}
}
