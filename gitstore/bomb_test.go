package gitstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/andybalholm/brotli"
	"github.com/go-crdt/crdt"
)

// A state file that expands past any document is refused before it is read.
//
// This store writes the checked frame, which does not compress -- but it still
// READS the packed one, because a repository written before that change holds
// them. So a blob planted in a work tree reaches the decompressor, and the
// bound is what stops it. Without it the same file is refused by its checksum,
// after the memory has been spent.
func TestAStateFileThatExpandsPastAnyDocumentIsRefusedBeforeItIsRead(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir, WithAuthor("loom", "loom@example"))
	if err != nil {
		t.Fatal(err)
	}
	c := crdt.NewComposite(1)
	body, err := c.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, "a document a repository holds"); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := s.Save(ctx, "doc", c.Snapshot()); err != nil {
		t.Fatal(err)
	}
	at, err := dirFor("doc")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, at, stateFile)

	const size = 512 << 20
	planted := bomb(t, size)
	if len(planted) > 4096 {
		t.Fatalf("the file is %d bytes, which is not the shape being tested", len(planted))
	}
	if err := os.WriteFile(path, planted, 0o600); err != nil {
		t.Fatal(err)
	}

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	got, err := s.Load(ctx, "doc")
	runtime.ReadMemStats(&after)
	spent := (after.TotalAlloc - before.TotalAlloc) / (1 << 20)

	if err == nil {
		t.Fatalf("a %d-byte state file claiming %d bytes was served as %d", len(planted), size, len(got))
	}
	if !strings.Contains(err.Error(), "expands past") {
		t.Fatalf("refused, but not before it was read: %v", err)
	}
	if spent > 64 {
		t.Errorf("refusing it cost %d MiB", spent)
	}
	t.Logf("%d-byte state file claiming %d MiB: refused for %d MiB", len(planted), size>>20, spent)
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
