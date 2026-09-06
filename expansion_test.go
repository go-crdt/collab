//go:build !js

package collab

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"runtime"
	"strings"
	"testing"

	"github.com/andybalholm/brotli"
)

// bomb returns a stored document that claims to be tiny and decompresses to
// size, framed exactly as [PackSnapshot] frames one.
//
// Its checksum is deliberately wrong. That is the point: the checksum is over
// what the bytes decompress TO, so it cannot refuse them until the memory has
// already been spent. What is being tested is what happens before it.
func bomb(t *testing.T, size int) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := brotli.NewWriterOptions(&buf, brotli.WriterOptions{Quality: compressQuality})
	zeroes := make([]byte, 1<<20)
	for range size / len(zeroes) {
		if _, err := w.Write(zeroes); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	out := append([]byte(nil), checkedMagic[:]...)
	out = binary.BigEndian.AppendUint32(out, crc32.Checksum(nil, checksumTable))
	return append(out, buf.Bytes()...)
}

// A stored document does not decompress into memory nobody has.
//
// crdt's snapshot reader refuses a document longer than it can hold. Putting
// compression in front of that reader put the bound behind the allocation: a
// blob of 1626 bytes decompressed to a gigabyte and cost 2094 MiB before its
// checksum refused it.
func TestAStoredDocumentThatExpandsPastAnyDocumentIsRefusedBeforeItIsRead(t *testing.T) {
	// Large enough that the two outcomes are not near each other: unbounded,
	// refusing this costs about a gigabyte; bounded, single-digit megabytes.
	// A threshold between them with sixteen times the margin on each side
	// cannot be reached by whatever else the process is doing.
	const size = 512 << 20
	stored := bomb(t, size)
	if len(stored) > 4096 {
		t.Fatalf("the bomb is %d bytes, which is not the shape being tested", len(stored))
	}

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	out, err := UnpackSnapshot(stored)
	runtime.ReadMemStats(&after)
	spent := (after.TotalAlloc - before.TotalAlloc) / (1 << 20)

	if err == nil {
		t.Fatalf("a %d-byte blob claiming %d bytes was accepted as %d", len(stored), size, len(out))
	}
	// Refused for THIS reason, not by the checksum. A refusal by the checksum
	// is a refusal after the memory has been spent, which is the failure and
	// not the fix -- and it is what this test sees if the bound is removed.
	if !strings.Contains(err.Error(), "expands past") {
		t.Fatalf("refused, but not before it was read: %v", err)
	}
	// Bounded rather than pinned: what matters is the magnitude, and the
	// deterministic half of the proof is the error above -- refused by the
	// bound means refused before the bytes existed.
	if spent > 64 {
		t.Errorf("refusing it cost %d MiB, which is the allocation the bound is for", spent)
	}
	t.Logf("%d bytes claiming %d MiB: refused for %d MiB", len(stored), size>>20, spent)
}

// A ratio no document reaches must not refuse the documents that exist. The
// widest measured is repeated prose at 85.6x, which this test is where it came from.
func TestADocumentThatCompressesWellIsStillRead(t *testing.T) {
	snapshot := written(t, strings.Repeat("Every document a store holds is mostly columns of identities. ", 200))
	stored := PackSnapshot(snapshot)
	ratio := float64(len(snapshot)) / float64(len(stored))
	got, err := UnpackSnapshot(stored)
	if err != nil {
		t.Fatalf("a document that compresses %.1fx was refused: %v", ratio, err)
	}
	if !bytes.Equal(got, snapshot) {
		t.Fatal("it came back as something else")
	}
	if ratio < 10 {
		t.Skipf("this document only compresses %.1fx, so it does not exercise the ratio", ratio)
	}
	t.Logf("a document compressing %.1fx is read, against a bound of %dx", ratio, maxExpansion)
}

// A tiny document is not caught by a ratio: a hundred bytes times a thousand is
// still small, and leastExpansion is the floor that keeps it readable.
func TestATinyDocumentIsNotRefusedByTheRatio(t *testing.T) {
	snapshot := written(t, "x")
	got, err := UnpackSnapshot(PackSnapshot(snapshot))
	if err != nil {
		t.Fatalf("a tiny document was refused: %v", err)
	}
	if !bytes.Equal(got, snapshot) {
		t.Fatal("it came back as something else")
	}
}
