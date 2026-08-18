package collab

import (
	"bytes"
	"fmt"
	"io"

	"github.com/andybalholm/brotli"
)

// Documents on disk are compressed, and this is where.
//
// # Why here and not in crdt
//
// crdt's Snapshot promises that the same state is the same bytes — two replicas
// that have applied the same operations produce identical output, so a snapshot
// doubles as a convergence check, and it can be compared, cached or signed by
// what it is. A compressor's output is deterministic for a build of that
// compressor and not promised across versions of it, so putting one inside that
// promise would weaken it into "the same state is the same bytes, as long as
// nobody upgraded". crdt's format version 5 does its half of the work by
// writing the fields in columns, which is what makes them worth compressing;
// this is the other half, on the side where bytes are stored rather than
// compared.
//
// # What it is worth
//
// Documents from a running loom server, at quality 9: 1.76 MB became 127 KB,
// which is 13.9×. A document is mostly a column of identities and a column of
// offsets, and those repeat in a way that a document's text does not.
//
// # Quality 9
//
// Measured rather than chosen. On a real editing trace brotli's ladder is flat
// from quality 5 to 9 — 108 KB to 106 — and then falls off a cliff: quality 10
// costs 167 milliseconds where 9 costs 14, for 7 KB. A store saves a document
// every few seconds, so 14 milliseconds is affordable and 167 is not.
const compressQuality = 9

// compressedMagic marks a compressed document. It is deliberately not a prefix
// of crdt's own magic, "crdtc", so that a file written before this existed is
// recognised by not starting with it and read as it is. A store that has been
// running keeps working; its documents become compressed as they are next
// saved, one at a time, with nothing to migrate.
var compressedMagic = [...]byte{'c', 'r', 'd', 't', 'z'}

// pack compresses a snapshot for storage.
//
// It returns no error, and the reason is that it cannot have one: the
// destination is a bytes.Buffer made here, and writing to one never fails.
// Checking anyway would put two branches in the package that no test can reach
// and a reader has to wonder about — and would put a third in Save, which would
// have to decide what to do about a failure that does not happen.
func pack(snapshot []byte) []byte {
	out := bytes.NewBuffer(make([]byte, 0, len(snapshot)/8))
	out.Write(compressedMagic[:])
	w := brotli.NewWriterOptions(out, brotli.WriterOptions{Quality: compressQuality})
	_, _ = w.Write(snapshot)
	_ = w.Close()
	return out.Bytes()
}

// unpack reverses pack, and passes through anything that was not packed — which
// is every document written before this existed.
func unpack(stored []byte) ([]byte, error) {
	if len(stored) < len(compressedMagic) || string(stored[:len(compressedMagic)]) != string(compressedMagic[:]) {
		return stored, nil
	}
	out, err := io.ReadAll(brotli.NewReader(bytes.NewReader(stored[len(compressedMagic):])))
	if err != nil {
		return nil, fmt.Errorf("collab: reading a compressed document: %w", err)
	}
	return out, nil
}
