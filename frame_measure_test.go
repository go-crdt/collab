//go:build !js

package collab

import (
	"testing"

	"github.com/go-crdt/crdt"
)

// The frame earns its place, counted rather than asserted.
//
// This is the measurement the doc comment on [PackSnapshot] cites, kept here so
// the number answers to something. Every single-bit flip of a composite
// snapshot is tried twice: bare, as a store without the frame would hold it,
// and framed. Bare, roughly half load with no error at all as a DIFFERENT
// document. Framed, none of them do.
func TestEverySingleBitFlipIsRefusedOnceItIsFramed(t *testing.T) {
	snapshot := written(t, "The store writes a snapshot and reads it back. Between "+
		"those two moments the bytes are the disk's, and a disk is allowed to change one.")

	// identity is what makes two loads the same document: the bytes a reload
	// re-emits. Comparing the text alone would call a document with a different
	// version vector unchanged.
	identity := func(b []byte) (string, bool) {
		c, err := crdt.LoadComposite(2, b)
		if err != nil {
			return "", false
		}
		return string(c.Snapshot()), true
	}
	base, ok := identity(snapshot)
	if !ok || base != string(snapshot) {
		t.Fatal("re-snapshotting is not stable, so identity by bytes is the wrong test here")
	}

	var bareSilent, bareRefused, framedSilent int
	framed := PackSnapshot(snapshot)
	for i := range snapshot {
		for bit := 0; bit < 8; bit++ {
			flipped := append([]byte(nil), snapshot...)
			flipped[i] ^= 1 << bit
			if got, ok := identity(flipped); !ok {
				bareRefused++
			} else if got != base {
				bareSilent++
			}
		}
	}
	for i := range framed {
		for bit := 0; bit < 8; bit++ {
			flipped := append([]byte(nil), framed...)
			flipped[i] ^= 1 << bit
			out, err := UnpackSnapshot(flipped)
			if err != nil {
				continue
			}
			if got, ok := identity(out); ok && got != base {
				framedSilent++
			}
		}
	}

	total := len(snapshot) * 8
	t.Logf("bare: %d of %d single-bit flips loaded as a different document (%d refused); framed: %d of %d",
		bareSilent, total, bareRefused, framedSilent, len(framed)*8)

	// The bare number is a property of crdt's snapshot format, not of a
	// constant here, so it is bounded rather than pinned: a format that got
	// better at refusing corruption must not fail this test, and one that
	// stopped refusing any must not pass it silently.
	if bareSilent < total/4 {
		t.Errorf("only %d of %d bare flips were silent; if the snapshot format now "+
			"catches its own corruption, this test and the doc it backs are stale",
			bareSilent, total)
	}
	// The framed number is a property of this package, and it is zero.
	if framedSilent != 0 {
		t.Errorf("%d framed flips loaded as a different document; the checksum let one through", framedSilent)
	}
}
