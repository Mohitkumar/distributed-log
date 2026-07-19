package segment

import (
	"os"
	"path/filepath"
	"testing"
)

func setupTest(t *testing.T) (index *Index, teardown func()) {
	t.Helper()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "test.id")
	index, err := OpenIndex(file)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	return index, func() {
		index.Close()
		os.Remove(file)
	}
}

func TestIndexWriteRead(t *testing.T) {
	index, teardown := setupTest(t)
	defer teardown()

	entries := []struct {
		relOffset uint32
		position  uint64
	}{
		{0, 0},
		{1, 100},
		{2, 205},
		{3, 309},
		{4, 400},
	}

	for _, e := range entries {
		if err := index.Write(e.relOffset, e.position); err != nil {
			t.Fatalf("failed to write index entry: %v", err)
		}
	}

	for _, e := range entries {
		entry, found := index.Find(e.relOffset)
		if !found {
			t.Fatalf("failed to read index entry: %v", entry)
		}
		if entry.RelativeOffset != e.relOffset || entry.Position != e.position {
			t.Errorf("index entry mismatch: got (%d, %d), want (%d, %d)",
				entry.RelativeOffset, entry.Position, e.relOffset, e.position)
		}
	}
}

func TestIndexGrow(t *testing.T) {
	index, teardown := setupTest(t)
	defer teardown()

	initialCap := len(index.mmap)

	entriesToWrite := (initialCap / IndexEntrySize) + 10 // Write more than initial capacity

	for i := 0; i < entriesToWrite; i++ {
		if err := index.Write(uint32(i), uint64(i*100)); err != nil {
			t.Fatalf("failed to write index entry: %v", err)
		}
	}

	if len(index.mmap) <= initialCap {
		t.Errorf("index mmap did not grow as expected")
	}

	// Verify last entry
	lastEntry, found := index.Find(uint32(entriesToWrite - 1))
	if !found {
		t.Fatalf("failed to read last index entry")
	}
	if lastEntry.RelativeOffset != uint32(entriesToWrite-1) || lastEntry.Position != uint64((entriesToWrite-1)*100) {
		t.Errorf("last index entry mismatch	: got (%d, %d), want (%d, %d)",
			lastEntry.RelativeOffset, lastEntry.Position, entriesToWrite-1, (entriesToWrite-1)*100)
	}
}

func TestIndexEntry(t *testing.T) {
	index, teardown := setupTest(t)
	defer teardown()

	entries := []struct {
		relOffset uint32
		position  uint64
	}{
		{0, 0},
		{1, 100},
		{2, 205},
	}

	for _, e := range entries {
		if err := index.Write(e.relOffset, e.position); err != nil {
			t.Fatalf("failed to write index entry: %v", err)
		}
	}

	for i, e := range entries {
		entry := index.Entry(int64(i))
		if entry.RelativeOffset != e.relOffset || entry.Position != e.position {
			t.Errorf("index entry mismatch: got (%d, %d), want (%d, %d)",
				entry.RelativeOffset, entry.Position, e.relOffset, e.position)
		}
	}
}

func BenchmarkIndexWrite(b *testing.B) {
	b.TempDir()
	index, err := OpenIndex(b.TempDir() + "/test.idx")
	if err != nil {
		b.Fatalf("failed to create index: %v", err)
	}
	defer index.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := index.Write(uint32(i), uint64(i*100)); err != nil {
			b.Fatalf("failed to write index entry: %v", err)
		}
	}
	seconds := b.Elapsed().Seconds()
	if seconds > 0 {
		b.ReportMetric(float64(b.N)/seconds, "req/s")
	}
}

func BenchmarkIndexFind(b *testing.B) {
	b.TempDir()
	index, err := OpenIndex(b.TempDir() + "/test.idx")
	if err != nil {
		b.Fatalf("failed to create index: %v", err)
	}
	defer index.Close()
	for i := 0; i < b.N; i++ {
		if err := index.Write(uint32(i), uint64(i*100)); err != nil {
			b.Fatalf("failed to write index entry: %v", err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, found := index.Find(uint32(i))
		if !found {
			b.Fatalf("failed to find index entry: %v", i)
		}
	}
	seconds := b.Elapsed().Seconds()
	if seconds > 0 {
		b.ReportMetric(float64(b.N)/seconds, "req/s")
	}
}
