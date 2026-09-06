package pgstore_test

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"runtime"
	"strings"
	"testing"

	"github.com/andybalholm/brotli"
)

// A row that expands past any document is refused before it is read.
//
// The bound lives in collab, and this is about the wiring: a row goes through
// collab.UnpackSnapshot rather than around it, so a blob planted in the table
// costs a megabyte or two rather than a gigabyte. Without the bound the same row
// is refused by its checksum -- after the memory has been spent.
func TestARowThatExpandsPastAnyDocumentIsRefusedBeforeItIsRead(t *testing.T) {
	db := connect(t)
	store, table := freshNamed(t, db)
	const size = 512 << 20
	row := bomb(t, size)
	if len(row) > 4096 {
		t.Fatalf("the row is %d bytes, which is not the shape being tested", len(row))
	}
	if _, err := db.Exec("INSERT INTO "+table+" (document, snapshot) VALUES ($1, $2)", "d", row); err != nil {
		t.Fatal(err)
	}

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	got, err := store.Load(t.Context(), "d")
	runtime.ReadMemStats(&after)
	spent := (after.TotalAlloc - before.TotalAlloc) / (1 << 20)

	if err == nil {
		t.Fatalf("a %d-byte row claiming %d bytes was served as %d", len(row), size, len(got))
	}
	if !strings.Contains(err.Error(), "expands past") {
		t.Fatalf("refused, but not before it was read: %v", err)
	}
	if spent > 64 {
		t.Errorf("refusing it cost %d MiB", spent)
	}
	t.Logf("%d-byte row claiming %d MiB: refused for %d MiB", len(row), size>>20, spent)
}

// bomb returns a stored document framed as collab.PackSnapshot frames one,
// tiny, that decompresses to size. Its checksum is deliberately wrong: the
// checksum is over what it decompresses TO, so it cannot refuse the bytes until
// the memory has been spent, and what is being tested is what happens before
// that.
func bomb(t *testing.T, size int) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := brotli.NewWriterOptions(&buf, brotli.WriterOptions{Quality: 9})
	zeroes := make([]byte, 1<<20)
	for range size / len(zeroes) {
		if _, err := w.Write(zeroes); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	out := []byte{'c', 'r', 'd', 't', 'h'}
	out = binary.BigEndian.AppendUint32(out, crc32.Checksum(nil, crc32.MakeTable(crc32.Castagnoli)))
	return append(out, buf.Bytes()...)
}
