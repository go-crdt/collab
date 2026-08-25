//go:build !js

package collab

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/andybalholm/brotli"
	"github.com/go-crdt/crdt"
)

func written(t *testing.T, text string) []byte {
	t.Helper()
	made := crdt.NewComposite(3)
	body, err := made.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, text); err != nil {
		t.Fatal(err)
	}
	cells, err := made.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cells.Set("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	return made.Snapshot()
}

// A document on disk that has rotted is refused, not served.
//
// A snapshot carries no integrity check of its own, and a DirStore is the one
// store here with nothing underneath it: git is content-addressed, Postgres
// checksums its pages, a MemoryStore cannot rot, and a file is on whatever
// filesystem it landed on.
//
// Measured before the checksum existed: of 544 single-bit flips in one
// document's file, 479 were refused, 7 read back unchanged, and 58
// decompressed cleanly into a DIFFERENT document — which a server would serve
// and, at its next save, make permanent. One in ten was silent.
func TestADocumentThatHasRottedOnDiskIsRefused(t *testing.T) {
	dir := t.TempDir()
	store, err := NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	const text = "what somebody wrote"
	if err := store.Save(context.Background(), "d", written(t, text)); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("the store holds %d files, %v", len(entries), err)
	}
	path := filepath.Join(dir, entries[0].Name())
	sound, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	var refused, unchanged, different int
	for i := range sound {
		for bit := 0; bit < 8; bit++ {
			bad := append([]byte(nil), sound...)
			bad[i] ^= 1 << bit
			if err := os.WriteFile(path, bad, 0o600); err != nil {
				t.Fatal(err)
			}
			back, err := store.Load(context.Background(), "d")
			if err != nil {
				refused++
				continue
			}
			doc, err := crdt.LoadComposite(1, back)
			if err != nil {
				refused++
				continue
			}
			body, err := doc.Text("body")
			switch {
			case err != nil || body.String() != text:
				different++
			default:
				unchanged++
			}
		}
	}
	t.Logf("%d single-bit flips: %d refused, %d harmless, %d different", len(sound)*8, refused, unchanged, different)
	if different != 0 {
		t.Fatalf("%d corruptions read back as a different document", different)
	}
	if refused == 0 {
		t.Fatal("nothing was refused; the file may not be being read at all")
	}
}

// A store that was running before the checksum existed keeps working, and its
// documents gain one as they are next saved.
func TestADocumentWrittenBeforeThereWasAChecksumStillReads(t *testing.T) {
	snapshot := written(t, "written a while ago")

	// The two formats that came before: compressed without a checksum, and
	// nothing at all.
	var old bytes.Buffer
	old.Write(compressedMagic[:])
	w := brotli.NewWriterOptions(&old, brotli.WriterOptions{Quality: compressQuality})
	if _, err := w.Write(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		name   string
		stored []byte
	}{
		{"compressed, before there was a checksum", old.Bytes()},
		{"raw, before there was compression", snapshot},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := unpack(c.stored)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, snapshot) {
				t.Fatalf("read back %d bytes, want %d", len(got), len(snapshot))
			}
		})
	}

	// One of those older documents can be damaged too, and is refused where it
	// is decompressed — which is all that can be said about a format that has
	// nothing to check itself against.
	damaged := append([]byte(nil), old.Bytes()...)
	damaged[len(damaged)-1] ^= 0xff
	damaged[len(damaged)/2] ^= 0xff
	if _, err := unpack(damaged); err == nil {
		t.Log("a damaged pre-checksum document decompressed anyway, which is why the checksum exists")
	} else if !strings.Contains(err.Error(), "compressed document") {
		t.Fatalf("the refusal does not say where it happened: %v", err)
	}

	// And what is written now carries one.
	if !bytes.HasPrefix(pack(snapshot), checkedMagic[:]) {
		t.Fatal("a document saved now does not carry a checksum")
	}
}

// A checksummed document that is too short to hold its own checksum is refused
// rather than read past the end of.
func TestATruncatedChecksummedDocumentIsRefused(t *testing.T) {
	packed := pack(written(t, "a sentence"))
	for _, n := range []int{len(checkedMagic), len(checkedMagic) + 1, len(checkedMagic) + 3} {
		if _, err := unpack(packed[:n]); err == nil {
			t.Fatalf("a document of %d bytes was accepted", n)
		} else if !strings.Contains(err.Error(), "not enough") {
			t.Fatalf("the refusal does not say why: %v", err)
		}
	}
	// And one truncated in its compressed body is refused by brotli.
	if _, err := unpack(packed[:len(packed)-1]); err == nil {
		t.Fatal("a document missing its last byte was accepted")
	}
}

// Every truncation of a snapshot is refused by the decoder, so a torn write
// cannot become a shorter document that is otherwise fine.
func TestEveryTruncationOfASnapshotIsRefused(t *testing.T) {
	full := written(t, "the whole sentence somebody wrote")
	for n := 0; n < len(full); n++ {
		if _, err := crdt.LoadComposite(1, full[:n]); err == nil {
			t.Fatalf("a snapshot truncated to %d of %d bytes loaded", n, len(full))
		}
	}
}

// A MultiStore does not merge a corrupted store away: it says which one.
func TestAMultiStoreNamesTheStoreItCouldNotRead(t *testing.T) {
	sound, rotten := NewMemoryStore(), NewMemoryStore()
	if err := sound.Save(context.Background(), "d", written(t, "what somebody wrote")); err != nil {
		t.Fatal(err)
	}
	if err := rotten.Save(context.Background(), "d", []byte("this is not a snapshot at all")); err != nil {
		t.Fatal(err)
	}
	for _, order := range [][]Store{{sound, rotten}, {rotten, sound}} {
		if _, err := NewMultiStore(order...).Load(context.Background(), "d"); err == nil {
			t.Fatal("a corrupted store was merged away silently")
		} else if !strings.Contains(err.Error(), "store 1") {
			t.Fatalf("the error does not name a store: %v", err)
		}
	}
}
