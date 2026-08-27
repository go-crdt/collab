//go:build (js && wasm) || !js

package collab

import (
	"errors"

	"github.com/go-crdt/crdt"
)

// ErrTooFarBehind reports a participant whose replica holds work made against
// operations the document has since collected.
//
// It is what [crdt.Composite.Collect] costs, said out loud. Collecting drops
// operations every replica had delivered, so a replica that has them can always
// be caught up. A replica that has been away, has written while away, and
// wrote against something that is now gone, cannot: its operations name
// characters this document no longer holds, and applying them would strand them
// for ever.
//
// Such a participant is refused rather than served, and refused rather than
// quietly re-seeded. Re-seeding it would work — it would get the document as it
// now stands — and it would throw away what that person wrote while they were
// away, with nothing said. Handing back an error is the only option that lets
// an application do the one thing that is actually right, which is to show
// somebody their own work and let them decide.
var ErrTooFarBehind = errors.New("collab: this replica holds work made against operations the document has collected")

// behind reports whether held names operations the document no longer has,
// while also holding operations the document has never seen.
//
// The first without the second is a participant that has simply been away: it
// has nothing to lose, so it is sent the whole document. Both together is the
// one case nothing can be done about.
func behind(doc *crdt.Composite, held crdt.CompositeVersion) bool {
	if doc.CanReplay(held) {
		return false
	}
	return holdsSomethingNew(doc.Version(), held)
}

// holdsSomethingNew reports whether held names any operation mine does not.
func holdsSomethingNew(mine, held crdt.CompositeVersion) bool {
	for part, theirs := range held {
		ours := mine[part]
		for site, seq := range theirs {
			if seq > ours[site] {
				return true
			}
		}
	}
	return false
}
