package collab

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
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

// checkedMagic marks a compressed document that carries a checksum of what it
// decompresses to, and it is a new marker rather than a change to the old one
// for the same reason the old one was new: a store that has been running keeps
// working, and its documents gain the checksum as they are next saved.
//
// # Why a checksum at all
//
// A snapshot has none of its own, and it is the one thing here that has nothing
// underneath it. git is content-addressed, so gitstore cannot serve bytes that
// are not the bytes it stored; Postgres checksums its pages; a MemoryStore
// cannot rot. A DirStore is a file on whatever filesystem it landed on, and
// plenty of those do not checksum anything.
//
// What that costs was measured rather than assumed. Of 544 single-bit flips in
// one document's file, 479 were refused by brotli or by the decoder and 7 read
// back unchanged — but **58 decompressed cleanly into a different document**,
// which a server would then serve and, at its next save, make permanent. One in
// ten of the corruptions that can happen to a file was silent.
//
// # Castagnoli
//
// Rot, not forgery: anything that can write the file can recompute any checksum
// in it, so this is not authentication and does not pretend to be. CRC32C is
// what the hardware does in a single instruction on every architecture this
// builds for, and four bytes on a document that compresses to kilobytes is not
// a size anybody will notice.
var checkedMagic = [...]byte{'c', 'r', 'd', 't', 'h'}

// checksumTable is Castagnoli, built once.
var checksumTable = crc32.MakeTable(crc32.Castagnoli)

// pack compresses a snapshot for storage.
//
// It returns no error, and the reason is that it cannot have one: the
// destination is a bytes.Buffer made here, and writing to one never fails.
// Checking anyway would put two branches in the package that no test can reach
// and a reader has to wonder about — and would put a third in Save, which would
// have to decide what to do about a failure that does not happen.
func pack(snapshot []byte) []byte {
	out := bytes.NewBuffer(make([]byte, 0, len(snapshot)/8))
	out.Write(checkedMagic[:])
	// The checksum is of the snapshot rather than of the compressed bytes,
	// because rot in the compressed bytes usually makes brotli refuse them
	// anyway. What it is here to catch is the corruption that decompresses.
	out.Write(binary.BigEndian.AppendUint32(nil, crc32.Checksum(snapshot, checksumTable)))
	w := brotli.NewWriterOptions(out, brotli.WriterOptions{Quality: compressQuality})
	_, _ = w.Write(snapshot)
	_ = w.Close()
	return out.Bytes()
}

// unpack reverses pack, and passes through anything that was not packed — which
// is every document written before this existed.
func unpack(stored []byte) ([]byte, error) {
	switch {
	case hasMagic(stored, checkedMagic):
		body := stored[len(checkedMagic):]
		if len(body) < 4 {
			return nil, fmt.Errorf("collab: a checksummed document is %d bytes, which is not enough for one", len(stored))
		}
		want := binary.BigEndian.Uint32(body[:4])
		out, err := io.ReadAll(brotli.NewReader(bytes.NewReader(body[4:])))
		if err != nil {
			return nil, fmt.Errorf("collab: reading a compressed document: %w", err)
		}
		if got := crc32.Checksum(out, checksumTable); got != want {
			// Refusing is the whole point: served instead, this would be a
			// document nobody wrote, and the next save would make it the one
			// everybody has.
			return nil, fmt.Errorf("collab: a stored document does not match its checksum (%08x, want %08x); it has been corrupted", got, want)
		}
		return out, nil
	case hasMagic(stored, compressedMagic):
		// Written before there was a checksum. Read as it is, and it gains one
		// the next time it is saved.
		out, err := io.ReadAll(brotli.NewReader(bytes.NewReader(stored[len(compressedMagic):])))
		if err != nil {
			return nil, fmt.Errorf("collab: reading a compressed document: %w", err)
		}
		return out, nil
	default:
		// Written before there was compression either.
		return stored, nil
	}
}

func hasMagic(stored []byte, magic [5]byte) bool {
	return len(stored) >= len(magic) && string(stored[:len(magic)]) == string(magic[:])
}
