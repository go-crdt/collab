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
// A snapshot has none of its own, and what is underneath it depends on the
// store. git is content-addressed, so gitstore cannot serve bytes that are not
// the bytes it stored, and a MemoryStore cannot rot. A DirStore is a file on
// whatever filesystem it landed on, and plenty of those do not checksum
// anything.
//
// Postgres used to be named here as a store that checksums its own pages. It
// does — when it was told to, and the default is younger than most clusters.
// An initdb with no flags was run on both: PostgreSQL 17 leaves "Data page
// checksum version: 0" and 18 leaves 1. So a cluster created before 18, or
// upgraded in place from one, has no page checksums at all unless somebody ran
// pg_checksums --enable. A store cannot inherit the guarantee from the medium;
// it carries its own or it has none, which is why this is exported.
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

// maxExpansion bounds how much larger a document may be than the bytes it was
// stored as, and leastExpansion is the smallest ceiling that bound may give, so
// that a very small document is not refused by a ratio.
//
// # A decompressor in front of a bound is a bound that is not there
//
// crdt's snapshot reader refuses a document longer than it can hold, which is
// the bound that stops a malformed snapshot from reserving memory nobody has.
// Compression was then put in FRONT of it, here, so the bytes that reader is
// handed are already decompressed and the bound applies to what came out rather
// than to what was read.
//
// Measured: a stored blob of 1626 bytes decompressed to a gigabyte and cost
// 2094 MiB before its checksum refused it. The checksum is over what it
// decompresses TO, so it cannot fire until the memory has been spent -- the
// same shape as a bomb that was moved rather than removed.
//
// The ratio is measured rather than chosen. Real documents, stored:
//
//	one short line             1.1x
//	a thousand map keys        4.2x
//	a running loom server     13.9x
//	a page of prose           20.6x
//	edited by many hands      21.2x
//	repeated prose            85.6x
//
// The last of those is the widest, and it was not in the first list: it came
// out of the test written for this, which is the argument for keeping the
// margin large. 1000 leaves nearly twelve times it, while the blob above --
// six hundred and sixty thousand times -- is refused after a megabyte and a
// half instead of two gigabytes.
const maxExpansion = 1000
const leastExpansion = 1 << 20

// expansionCeiling caps the ratio's product so it cannot overflow an int on a
// build where int is 32 bits. This package has lanes on 386, arm and mips.
const expansionCeiling = 1 << 30

// checkedRawMagic marks a snapshot that carries a checksum and is NOT
// compressed, which is what a store wants when its medium does better with
// bytes it can compare than with bytes it cannot.
//
// A git repository is that medium. It deltas one version of a file against the
// one before, and two consecutive snapshots of a document differ in their
// columns rather than throughout, so the delta is small — while brotli output
// deltas against nothing at all. Measured over 400 saves of a growing document,
// the repository settled at 215 KB holding raw snapshots, 212 KB holding
// checked ones, and 394 KB holding packed ones. Compression made the store
// whose whole point is a git repository 83% larger.
var checkedRawMagic = [...]byte{'c', 'r', 'd', 't', 'k'}

// checksumTable is Castagnoli, built once.
var checksumTable = crc32.MakeTable(crc32.Castagnoli)

// PackSnapshot compresses a snapshot and gives it a checksum, for a [Store]
// that keeps bytes on a medium which can change them. [UnpackSnapshot] reverses
// it. [DirStore] calls it on every save.
//
// It is exported for the stores that are not in this package — pgstore is one,
// and so is whatever a consumer writes against S3, a key-value store or a
// blobstore. A [Store] is a public interface, so the answer to "who checksums
// this?" has to be available to whoever implements one.
//
// Rot, not forgery. Anything that can write the bytes can recompute the
// checksum in them, so this authenticates nothing and does not pretend to; it
// catches a medium that changed a byte nobody asked it to change. Of 1856
// single-bit flips in one composite snapshot, 1048 loaded with no error at all
// as a DIFFERENT document, which a server would then serve and, at its next
// save, make permanent. Framed, none of them do. Both halves are counted by
// TestEverySingleBitFlipIsRefusedOnceItIsFramed rather than quoted from a probe
// that no longer exists.
//
// It returns no error, and the reason is that it cannot have one: the
// destination is a bytes.Buffer made here, and writing to one never fails.
// Checking anyway would put two branches in the package that no test can reach
// and a reader has to wonder about — and would put a third in Save, which would
// have to decide what to do about a failure that does not happen.
func PackSnapshot(snapshot []byte) []byte {
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

// CheckSnapshot gives a snapshot a checksum and leaves it uncompressed, for a
// [Store] on a medium that stores differences between versions. [UnpackSnapshot]
// reverses this as well as [PackSnapshot]; the two are told apart by what they
// write at the front, so a store may change from one to the other and still
// read what it already holds.
//
// Prefer [PackSnapshot] unless the medium is one of these. A snapshot is mostly
// columns of identities and offsets, and it compresses about 13.9× — but a
// medium that deltas versions loses more to compression than compression saves,
// and the numbers for git are on [checkedRawMagic].
func CheckSnapshot(snapshot []byte) []byte {
	out := make([]byte, 0, len(checkedRawMagic)+4+len(snapshot))
	out = append(out, checkedRawMagic[:]...)
	out = binary.BigEndian.AppendUint32(out, crc32.Checksum(snapshot, checksumTable))
	return append(out, snapshot...)
}

// UnpackSnapshot reverses [PackSnapshot] and [CheckSnapshot], and passes through
// anything that was written by neither — which is every document written before this existed. A store
// that starts calling this pair reads what it already holds, and its documents
// gain the checksum one save at a time, with nothing to migrate.
//
// It refuses bytes whose checksum does not match them. Refusing is the point:
// served instead, they would be a document nobody wrote.
func UnpackSnapshot(stored []byte) ([]byte, error) {
	switch {
	case hasMagic(stored, checkedRawMagic):
		body := stored[len(checkedRawMagic):]
		if len(body) < 4 {
			return nil, fmt.Errorf("collab: a checksummed document is %d bytes, which is not enough for one", len(stored))
		}
		want := binary.BigEndian.Uint32(body[:4])
		out := body[4:]
		if got := crc32.Checksum(out, checksumTable); got != want {
			return nil, fmt.Errorf("collab: a stored document does not match its checksum (%08x, want %08x); it has been corrupted", got, want)
		}
		return out, nil
	case hasMagic(stored, checkedMagic):
		body := stored[len(checkedMagic):]
		if len(body) < 4 {
			return nil, fmt.Errorf("collab: a checksummed document is %d bytes, which is not enough for one", len(stored))
		}
		want := binary.BigEndian.Uint32(body[:4])
		out, err := expand(body[4:])
		if err != nil {
			return nil, err
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
		out, err := expand(stored[len(compressedMagic):])
		if err != nil {
			return nil, err
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

// expand decompresses stored bytes, refusing an expansion no document has.
//
// The limit is a ratio rather than a size because the size a document may reach
// is the size a document may reach: a hundred megabytes of text is a hundred
// megabytes whether it was stored well or badly. What no document does is come
// from a thousandth of itself. See [maxExpansion] for the measurements.
func expand(compressed []byte) ([]byte, error) {
	limit := int64(len(compressed)) * maxExpansion
	if limit < leastExpansion {
		limit = leastExpansion
	}
	if limit > expansionCeiling {
		limit = expansionCeiling
	}
	out, err := io.ReadAll(io.LimitReader(brotli.NewReader(bytes.NewReader(compressed)), limit+1))
	if err != nil {
		return nil, fmt.Errorf("collab: reading a compressed document: %w", err)
	}
	if int64(len(out)) > limit {
		// Refused before it is read, and before the checksum could refuse it:
		// the checksum is over what this produces, so it cannot fire until the
		// memory has already been spent.
		return nil, fmt.Errorf("collab: a stored document of %d bytes expands past %d, which no document does; refused", len(compressed), limit)
	}
	return out, nil
}
