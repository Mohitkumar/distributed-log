package log

import (
	"encoding/binary"
	"os"
	"strconv"
	"testing"

	"github.com/mohitkumar/mlog/segment"
	"github.com/stretchr/testify/require"
)

func setupTestLog(t *testing.T) (*Log, func()) {
	t.Helper()

	log, err := NewLog("/tmp/mlog_test")
	if err != nil {
		t.Fatalf("failed to create log: %v", err)
	}

	return log, func() {
		log.Close()
		os.RemoveAll("/tmp/mlog_test")
	}
}

func TestLogAppendRead(t *testing.T) {
	log, teardown := setupTestLog(t)
	defer teardown()

	records := [][]byte{
		[]byte("first log record"),
		[]byte("second log record"),
		[]byte("third log record"),
	}

	var offsets []uint64
	for _, r := range records {
		offset, err := log.Append(r)

		if err != nil {
			t.Fatalf("failed to append record: %v", err)
		}
		offsets = append(offsets, offset)
	}

	for i, r := range records {
		recBytes, err := log.Read(offsets[i])
		if err != nil {
			t.Fatalf("failed to read record: %v", err)
		}
		rec, err := segment.Decode(recBytes)
		if err != nil {
			t.Fatalf("failed to decode record: %v", err)
		}
		if string(rec.Value) != string(r) {
			t.Errorf("record mismatch: got (payload: %s), want (payload: %s)",
				rec.Value, r)
		}
	}
}

func TestLogOutOfRangeRead(t *testing.T) {
	log, teardown := setupTestLog(t)
	defer teardown()

	_, err := log.Read(999)
	if err == nil {
		t.Fatalf("expected error for out of range read, got nil")
	}
}
func TestLogSegmentRotation(t *testing.T) {
	log, teardown := setupTestLog(t)
	defer teardown()

	numRecords := 100000
	var lastOffset uint64
	for i := 0; i < numRecords; i++ {
		offset, err := log.Append([]byte("log record " + strconv.Itoa(i)))
		if err != nil {
			t.Fatalf("failed to append record: %v", err)
		}
		lastOffset = offset
	}

	if len(log.segments) < 2 {
		t.Errorf("expected multiple segments after appending records, got %d", len(log.segments))
	}

	recBytes, err := log.Read(lastOffset)
	if err != nil {
		t.Fatalf("failed to read last record: %v", err)
	}
	rec, err := segment.Decode(recBytes)
	if err != nil {
		t.Fatalf("failed to decode last record: %v", err)
	}
	expectedValue := "log record " + strconv.Itoa(numRecords-1)
	if string(rec.Value) != expectedValue {
		t.Errorf("last record payload mismatch: got %s, want %s", rec.Value, expectedValue)
	}
}

func TestLogReader(t *testing.T) {
	log, teardown := setupTestLog(t)
	defer teardown()
	for i := 0; i < 100000; i++ {
		_, err := log.Append([]byte("log record " + strconv.Itoa(i)))
		if err != nil {
			t.Fatalf("failed to append record: %v", err)
		}
	}
	var readRecords []string
	reader := log.Reader()
	for i := 0; i < 100000; i++ {
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
	for i := 0; i < 100000; i++ {
		if string("log record "+strconv.Itoa(i)) != readRecords[i] {
			t.Errorf("record mismatch: got %s, want %s", readRecords[i], "log record "+strconv.Itoa(i))
		}
	}
}

func BenchmarkLogWrite(b *testing.B) {
	log, teardown := setupTestLog(&testing.T{})
	defer teardown()
	for i := 0; i < b.N; i++ {
		log.Append([]byte("log record " + strconv.Itoa(i)))
	}
	seconds := b.Elapsed().Seconds()
	if seconds > 0 {
		b.ReportMetric(float64(b.N)/seconds, "req/s")
	}
}
func BenchmarkLogRead(b *testing.B) {
	log, teardown := setupTestLog(&testing.T{})
	defer teardown()
	for i := 0; i < b.N; i++ {
		log.Append([]byte("log record " + strconv.Itoa(i)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := log.Read(uint64(i))
		if err != nil {
			b.Fatalf("failed to read record: %v", err)
		}
	}
	seconds := b.Elapsed().Seconds()
	if seconds > 0 {
		b.ReportMetric(float64(b.N)/seconds, "req/s")
	}
}

func BenchmarkLogReader(b *testing.B) {
	log, teardown := setupTestLog(&testing.T{})
	defer teardown()
	for i := 0; i < b.N; i++ {
		log.Append([]byte("log record " + strconv.Itoa(i)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := log.Reader()
		header := make([]byte, 8+4)
		n, err := reader.Read(header)
		require.NoError(b, err)
		require.Equal(b, 12, n)
		size := binary.BigEndian.Uint32(header[8:12])
		payloadBuf := make([]byte, size)
		n, err = reader.Read(payloadBuf)
		require.NoError(b, err)
		require.Equal(b, int(size), n)
		if err != nil {
			b.Fatalf("failed to read record: %v", err)
		}
	}
	seconds := b.Elapsed().Seconds()
	if seconds > 0 {
		b.ReportMetric(float64(b.N)/seconds, "req/s")
	}
}
