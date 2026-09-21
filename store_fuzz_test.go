// Copyright (c) the go-crdt authors.
// SPDX-License-Identifier: BSD-3-Clause

package collab

import (
	"bytes"
	"strings"
	"testing"

	"github.com/go-crdt/crdt"
)

// The framing a store puts around a document is the least trusted input this
// package has: it comes back from a file, a database row or a git blob, and
// anything that can write one can write any bytes at all. Its unit tests say
// what a correct frame and a corrupted frame do. These say what EVERY input
// does.

// FuzzUnpackSnapshot: whatever comes out of a store, unframing it either refuses
// or produces bytes that frame back to themselves.
//
// The second half is the part worth fuzzing. A refusal is easy to get right; a
// frame that is accepted and then quietly altered is the failure that reaches a
// document, because the next save writes what was read.
func FuzzUnpackSnapshot(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("crdt"))                      // crdt's own text magic, four bytes: not a frame
	f.Add(CheckSnapshot([]byte("hello")))      // the checked-raw framing
	f.Add(PackSnapshot([]byte("hello")))       // the compressed framing
	f.Add(CheckSnapshot(nil))                  // an empty document, framed
	f.Add([]byte("crdtk"))                     // a frame with no room for its checksum
	f.Add([]byte("crdth\x00\x00\x00\x00"))     // a compressed frame with no body
	f.Add(append([]byte("crdtz"), 0xff, 0x00)) // legacy compressed, malformed body
	// A frame in each framing whose checksum does not match its body.
	//
	// Without these the refusal assertion below is never reached: every other
	// seed either unframes cleanly or is rejected before the checksum is
	// compared. Installing the defect it exists for -- a refusal that also
	// returns the bytes it refused -- did NOT fail this target until these two
	// seeds were here. A relaxed assertion that nothing exercises is not an
	// assertion.
	f.Add(wrongChecksum(CheckSnapshot([]byte("hello"))))
	f.Add(wrongChecksum(PackSnapshot([]byte("hello"))))

	f.Fuzz(func(t *testing.T, stored []byte) {
		out, err := UnpackSnapshot(stored)
		if err != nil {
			if out != nil {
				t.Fatalf("a refusal also returned %d bytes", len(out))
			}
			return
		}
		// Both framings are round trips over whatever was just read, so a
		// document read from one store can be written to another.
		for name, frame := range map[string]func([]byte) []byte{
			"CheckSnapshot": CheckSnapshot,
			"PackSnapshot":  PackSnapshot,
		} {
			again, err := UnpackSnapshot(frame(out))
			if err != nil {
				t.Fatalf("%s of what was read does not unframe: %v", name, err)
			}
			if !bytes.Equal(again, out) {
				t.Fatalf("%s round trip changed %d bytes into %d", name, len(out), len(again))
			}
		}
	})
}

// FuzzFrameRoundTrip: framing any document and unframing it gives that document
// back, byte for byte.
//
// The interesting inputs are documents that LOOK like frames. collab's magics
// live in the same five-byte namespace as crdt's snapshot magics -- crdtz,
// crdth, crdtk and crdts against crdt, crdtc, crdtm and crdl -- and
// UnpackSnapshot's last case passes an unrecognised document through untouched.
// A document whose own first bytes read as a frame would be unframed by
// mistake, which is why this asserts on the bytes rather than on the absence of
// an error.
func FuzzFrameRoundTrip(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("crdt\x08"))
	f.Add([]byte("crdtc"))
	f.Add([]byte("crdtk"))
	f.Add([]byte("crdth"))
	f.Add([]byte("crdtz"))
	f.Add([]byte("crdts"))
	f.Add(CheckSnapshot([]byte("nested")))
	f.Add(PackSnapshot([]byte("nested")))

	f.Fuzz(func(t *testing.T, document []byte) {
		for name, frame := range map[string]func([]byte) []byte{
			"CheckSnapshot": CheckSnapshot,
			"PackSnapshot":  PackSnapshot,
		} {
			out, err := UnpackSnapshot(frame(document))
			if err != nil {
				t.Fatalf("%s produced something that will not unframe: %v", name, err)
			}
			// bytes.Equal treats nil and empty as equal, which is right here: a
			// store that hands back an empty slice for an empty document has
			// lost nothing.
			if !bytes.Equal(out, document) {
				t.Fatalf("%s round trip: %x became %x", name, document, out)
			}
		}
	})
}

// FuzzMergeSnapshots: merging two stored documents either refuses or produces a
// document that loads.
//
// A merge that returns bytes nothing can read would be worse than a refusal:
// [Tiered] and [MultiStore] write the result back, so it becomes the document
// every replica is handed next.
func FuzzMergeSnapshots(f *testing.F) {
	one := crdt.NewComposite(1)
	oneText, err := one.Text("body")
	if err != nil {
		f.Fatal(err)
	}
	oneText.Insert(0, "ours")
	two := crdt.NewComposite(2)
	twoText, err := two.Text("body")
	if err != nil {
		f.Fatal(err)
	}
	twoText.Insert(0, "theirs")
	f.Add(one.Snapshot(), two.Snapshot())
	f.Add(one.Snapshot(), []byte(nil))
	f.Add([]byte(nil), two.Snapshot())
	f.Add([]byte("crdtc"), []byte("crdtc"))
	// One side empty and the other not a document at all.
	//
	// Found by fuzzing, and kept as a seed rather than as an opaque file under
	// testdata because what makes it interesting is a sentence: it broke an
	// earlier version of this target that required every successful merge to
	// load. It does not, and should not -- with one side empty the other is
	// returned VERBATIM and unread, which is the shortcut the assertions below
	// now state. A store holding bytes no reader accepts is a defect in that
	// store, not in merging it with nothing.
	f.Add([]byte("0"), []byte(nil))

	f.Fuzz(func(t *testing.T, ours, theirs []byte) {
		merged, err := MergeSnapshots(ours, theirs)
		if err != nil {
			return
		}
		// With one side empty there is nothing to merge, so the other side comes
		// back as it was -- unread, and therefore not necessarily loadable. That
		// is worth asserting as the identity it is: a merge with nothing must not
		// alter the one document it was given.
		if len(theirs) == 0 {
			if !bytes.Equal(merged, ours) {
				t.Fatalf("merging with nothing changed %d bytes into %d", len(ours), len(merged))
			}
			return
		}
		if len(ours) == 0 {
			if !bytes.Equal(merged, theirs) {
				t.Fatalf("merging nothing with %d bytes gave %d", len(theirs), len(merged))
			}
			return
		}
		// Both sides were documents, so both were read -- and what a real merge
		// produces must be readable by the same loader that read its inputs.
		// Anything else would be written back by [Tiered] and handed to every
		// replica that reads next.
		if len(merged) == 0 {
			t.Fatalf("merging %d and %d bytes gave nothing", len(ours), len(theirs))
		}
		if _, err := crdt.LoadComposite(1, merged); err != nil {
			t.Fatalf("a merge produced a document that will not load: %v", err)
		}
	})
}

// wrongChecksum flips a bit in a frame's checksum, leaving a frame whose body is
// intact and whose checksum lies about it. That is the corruption the checksum
// exists for: not bytes a decompressor refuses, but bytes it accepts.
func wrongChecksum(frame []byte) []byte {
	out := append([]byte(nil), frame...)
	out[len(checkedMagic)] ^= 0x01
	return out
}

// A raw crdt snapshot of every kind passes through unframing untouched.
//
// This is a guard on a namespace two modules share without either saying so.
// collab frames a stored document with crdtz, crdth or crdtk, keeps its site
// list under crdts, and [UnpackSnapshot]'s last case hands an unrecognised
// document straight back -- which is how a store reads everything written
// before the framing existed. crdt signs its snapshots crdt, crdtc, crdtm and
// crdl in the same five-byte space.
//
// Nothing collides today, and nothing checks. The text magic is the narrow one:
// it is four bytes, so the fifth byte of a text snapshot decides whether collab
// reads it as a document or as a frame, and the day crdt mints a magic ending
// in z, h, k or s every snapshot of that kind would be unframed by mistake --
// silently, because unframing such a document succeeds and returns something
// shorter.
//
// So this asserts the behaviour rather than the bytes: it takes a real snapshot
// of each kind from crdt's own API and requires unframing to be the identity on
// it. It goes red in collab the day crdt picks a colliding magic, which is the
// day somebody needs to know.
func TestEveryKindOfRawCrdtSnapshotSurvivesUnframing(t *testing.T) {
	text := crdt.New(1)
	text.Insert(0, "a text document")

	list := crdt.NewList(1)
	list.Insert(0, []byte("an element"))

	m := crdt.NewMap(1)
	m.Set("key", []byte("a value"))

	composite := crdt.NewComposite(1)
	part, err := composite.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	part.Insert(0, "a composite document")

	for _, c := range []struct {
		kind     string
		snapshot []byte
	}{
		{"text", text.Snapshot()},
		{"list", list.Snapshot()},
		{"map", m.Snapshot()},
		{"composite", composite.Snapshot()},
	} {
		t.Run(c.kind, func(t *testing.T) {
			out, err := UnpackSnapshot(c.snapshot)
			if err != nil {
				t.Fatalf("a raw %s snapshot was refused: %v", c.kind, err)
			}
			if !bytes.Equal(out, c.snapshot) {
				t.Fatalf("a raw %s snapshot (%d bytes, starting %q) came back as %d bytes: "+
					"its magic collides with one of collab's frame magics",
					c.kind, len(c.snapshot), firstBytes(c.snapshot), len(out))
			}
		})
	}
}

// firstBytes is the prefix a collision would show up in, for the failure to name.
func firstBytes(b []byte) string {
	if len(b) > 5 {
		b = b[:5]
	}
	return string(b)
}

// What a store would actually do with a text snapshot whose version byte lands
// on one of collab's frame magics.
//
// This is the measured consequence behind crdt's reservedVersions, and it is not
// the same consequence for each byte. Written as a test because the interesting
// part is not that something goes wrong: it is WHICH thing, since one of them
// reports corruption for a document that is perfectly intact, and an operator
// who reads that goes looking for a failing disk.
//
// crdt writes version 8 or 9 today, so these snapshots are made by hand -- a
// real snapshot with its version byte set to the number the counter would reach.
func TestATextSnapshotWhoseVersionLandsOnAFrameMagic(t *testing.T) {
	d := crdt.New(1)
	d.Insert(0, "a document somebody wrote")
	original := d.Snapshot()

	for _, c := range []struct {
		version byte
		refused bool
		says    string
	}{
		// crdth: read as the compressed framing, so brotli is handed document
		// bytes and says so.
		{'h', true, "reading a compressed document"},
		// crdtz: the same, through the legacy framing.
		{'z', true, "reading a compressed document"},
		// crdtk: read as the checked-raw framing, and this is the bad one. Four
		// bytes of the document become a checksum, the rest is compared against
		// it, and the refusal says the document HAS BEEN CORRUPTED -- of a
		// document nothing has touched.
		{'k', true, "has been corrupted"},
		// crdts is collab's site list rather than a document framing, so
		// unframing does not claim it and hands it back untouched. Harmless
		// today, and one reader away from being the case above.
		{'s', false, ""},
	} {
		t.Run(string(rune(c.version)), func(t *testing.T) {
			snapshot := append([]byte(nil), original...)
			snapshot[4] = c.version // the version byte, right after crdt's four-byte magic
			out, err := UnpackSnapshot(snapshot)
			if !c.refused {
				if err != nil {
					t.Fatalf("expected it to pass through, got %v", err)
				}
				if !bytes.Equal(out, snapshot) {
					t.Fatalf("passed through as %d bytes, not %d", len(out), len(snapshot))
				}
				return
			}
			if err == nil {
				t.Fatalf("an intact document was read as a frame and accepted, %d bytes", len(out))
			}
			if !strings.Contains(err.Error(), c.says) {
				t.Fatalf("the refusal says %q, wanted it to mention %q", err, c.says)
			}
		})
	}
}
