//go:build (js && wasm) || !js

package collab

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/go-crdt/crdt"
)

// ErrUnmergeable reports two snapshots neither of which can be brought up to
// the other, each having discarded what the other still needs.
//
// It is the one divergence [MergeSnapshots] cannot resolve, and there is no
// operation that resolves it later either: what each side purged is in no
// operation, so neither can be told about the other's past. An operator who
// meets it has two documents and has to choose one; the merge will not choose
// for them, because choosing here is losing text somebody wrote.
var ErrUnmergeable = errors.New("collab: neither snapshot can serve the other")

// ErrDiverged reports two snapshots that claim the SAME history and do not hold
// the same document.
//
// A version vector counts operations per site, so two replicas whose vectors
// match have, by that account, applied the same operations — and must therefore
// hold the same document. When they do not, some site put its name on two
// different operations, and the two replicas each believe they are completely
// caught up with the other. Neither will ever ask for anything again, which is
// why this is worth a merge refusing rather than a note in a log: merging them
// grafts one history onto the other and produces a document that existed on
// neither side, with every character attributed to whoever the names say.
//
// It is not a corruption and the checksums will not see it. Both snapshots are
// well formed, and both replicas applied what they were sent. What differs is
// what the operations SAID, and only comparing the documents can show it —
// [crdt.Composite.Digest] is that comparison, and this is the one place in this
// package where two documents are both in hand.
//
// An operator who meets this has two replicas claiming one site identity. Within
// one deployment that means the identities it hands out are not unique; across
// two, it means one of them is speaking for the other's users, which is what
// [SpeaksFor] refuses at a link and nothing refuses in a shared repository.
//
// What it does NOT catch: a side that is genuinely ahead. If one version covers
// the other, the operations they disagree about are exactly the ones
// [crdt.Composite.OpsSince] will not carry, because it selects by name and the
// names match. That case is silent here and is why a link is the better place to
// federate from.
var ErrDiverged = errors.New("collab: two snapshots claim the same history and hold different documents")

// MergeSnapshots combines two snapshots of the same document into one that
// holds everything either of them still holds.
//
// It is the operation that makes a snapshot safe to keep in more than one
// place. Two copies of a document that were written separately have not
// disagreed about anything — a snapshot is a set of operations, and the union
// of two sets of operations is a document, which is the whole reason this
// project exists.
//
// Either argument may be nil, which is how a store says it has never held the
// document; merging with nothing gives back the other side.
//
// # It chooses a base, and the argument order is not what chooses it
//
// The union is built by carrying one side's operations onto the other, and the
// side carried onto — the base — brings something the operations cannot: the
// floors [crdt.Doc.Purge] and [crdt.Map.Collect] leave behind. Those live in
// the snapshot and in no operation, so taking the first argument as the base
// makes the argument order an input. That is not untidiness, it is loss in both
// directions:
//
//   - A replica that purged text emits neither the insertions nor the deletions
//     of a purged run, so making it the receiver instead of the base sends it a
//     history with a hole in it and hands back the text it discarded.
//     [crdt.Doc.CanServe] is the question to ask about that, and this used not
//     to ask it.
//   - A replica stale across a [crdt.Map.Collect], made the base over the
//     collected side, advances its version past a deletion it never learns, and
//     the deleted key is alive again. A map has no CanServe to catch that.
//
// So the base is chosen from the pair rather than from the order:
//
//   - A side the other cannot serve is the base. That is the correctness
//     condition and not a preference: a replica that cannot hand over its
//     deletions has to receive rather than send.
//   - Otherwise the side that has given up more — floors at least the other's
//     on every part — is the base, so that floors here only rise, as they do
//     everywhere else in this system. A merge that lowered one would undo an
//     operator's purge on every read and write the un-purged document back on
//     the next save, which is a purge that can never be made to stick.
//   - Floors that cross, each side having collected further than the other on a
//     different part, cannot both be kept by one snapshot, so one economy is
//     declined and the other side's tombstones are simply kept. Which side is
//     kept is settled by comparing the bytes: arbitrary, but a function of the
//     pair and not of the order, which is the property being bought.
//
// Merging is therefore still symmetric to the byte, and now for a reason it can
// state: the encoding is canonical, both results hold the same operations, and
// the argument order is not among the inputs.
//
// # One side empty is not a merge
//
// With either side empty the other is returned VERBATIM, unread. Nothing is
// decoded, so nothing is validated: a store holding bytes no reader accepts
// gets those bytes back with a nil error, and [Tiered] then writes them to the
// tier that was empty.
//
// That is deliberate rather than an oversight. The common case for an empty
// side is a tier that has not been filled yet, and decoding a whole document to
// confirm what will be written back unchanged would put that cost on every read
// of a cold tier -- to catch a corruption the framing's checksum already
// refuses one layer down ([UnpackSnapshot]). What it means for a caller is that
// a nil error here is not a statement about the bytes unless BOTH sides were
// documents.
//
// # What it cannot carry, it names
//
// It returns [ErrDiverged] when the two sides claim the same history and do not
// hold the same document, [ErrUnmergeable] when neither side can serve the
// other, and passes on [crdt.ErrStranded] when an operation cannot be carried
// onto the base — a
// write at or below a collected floor naming a key the base does not hold,
// which is a key that would otherwise come back alive. Both of those used to be
// a wrong document returned with a nil error, and a wrong document is found by
// the person who wrote the paragraph rather than by the operator.
func MergeSnapshots(ours, theirs []byte) ([]byte, error) {
	if len(theirs) == 0 {
		return ours, nil
	}
	if len(ours) == 0 {
		return theirs, nil
	}
	// The site is never used to mint anything: nothing here writes an operation
	// of its own, it only carries operations that already exist. Any site that
	// is not the server's own would do.
	mine, err := crdt.LoadComposite(1, ours)
	if err != nil {
		return nil, fmt.Errorf("collab: reading our side: %w", err)
	}
	yours, err := crdt.LoadComposite(1, theirs)
	if err != nil {
		return nil, fmt.Errorf("collab: reading their side: %w", err)
	}
	// Before choosing a base, and before carrying anything: two sides that
	// claim the same operations have to hold the same document, or the names
	// are not telling the truth. See [ErrDiverged].
	//
	// Cheap enough to do on every merge that gets this far -- both documents
	// are already decoded, and a digest is about twice what decoding one
	// costs -- and it is checked here rather than after the carry because the
	// carry would hide it: OpsSince selects by name, so an operation wearing a
	// name the base already holds is never sent, and the merge would return the
	// base unchanged and call it agreement.
	if mine.Version().Equal(yours.Version()) && mine.Digest() != yours.Digest() {
		return nil, ErrDiverged
	}
	base, from := chooseBase(mine, yours, ours, theirs)
	if base == nil {
		return nil, ErrUnmergeable
	}
	if err := base.Apply(from.OpsSince(base.Version())...); err != nil {
		return nil, fmt.Errorf("collab: carrying one side onto the other: %w", err)
	}
	return base.Snapshot(), nil
}

// chooseBase returns the side to carry operations onto and the side to take
// them from, or nil when neither side can be brought up to the other.
//
// ours and theirs are the bytes the two composites were read from, and are used
// only to settle a tie; see [MergeSnapshots] for why they settle it that way.
func chooseBase(mine, yours *crdt.Composite, ours, theirs []byte) (base, from *crdt.Composite) {
	canMine := yours.CanServe(mine.Version()) == nil  // is mine safe as the base?
	canYours := mine.CanServe(yours.Version()) == nil // is yours safe as the base?
	switch {
	case !canMine && !canYours:
		return nil, nil
	case !canMine:
		return yours, mine
	case !canYours:
		return mine, yours
	}
	// Both directions are open, so this is no longer about what can be served
	// but about which floors survive. A map has no CanServe, so for a collected
	// map this rule is the only thing standing between a stale side and a key
	// that comes back; for a purged text it agrees with the rule above wherever
	// that one has an opinion.
	fm, fy := floorsOf(mine), floorsOf(yours)
	switch {
	// Strictly, both ways. Two sides can reach the same floor on every part and
	// still have given up DIFFERENT tombstones -- the floors cannot see which,
	// only how far -- and a non-strict test makes both arms true, so the first
	// one wins and the base is whichever argument came first. Measured: two
	// maps collected to 99, one having dropped a's tombstone and the other b's,
	// merged to 34 bytes each way and they were not the same 34 bytes. Equal
	// floors have nothing to choose between them, so they fall to the tie-break
	// below, which reads the pair rather than the order.
	case dominates(fm, fy) && !dominates(fy, fm):
		return mine, yours
	case dominates(fy, fm) && !dominates(fm, fy):
		return yours, mine
	case bytes.Compare(ours, theirs) <= 0:
		return mine, yours
	}
	return yours, mine
}

// floorsOf returns what each part of a document has given up: the clock a text
// has purged below, or the one a map has collected below. A part with no floor
// is absent rather than zero, so that a document that has given up nothing
// dominates nothing and is dominated by everything.
//
// A list has neither, having had its collection withdrawn in crdt v0.35.0.
func floorsOf(c *crdt.Composite) map[crdt.Part]uint64 {
	floors := map[crdt.Part]uint64{}
	for _, part := range c.Parts() {
		// Parts names only parts this document already holds, so neither
		// lookup can refuse the name; the errors are checked rather than
		// dropped because a nil here would be a panic in a merge.
		switch part.Kind {
		case crdt.PartText:
			if text, err := c.Text(part.Name); err == nil && text.PurgedBelow() > 0 {
				floors[part] = text.PurgedBelow()
			}
		case crdt.PartMap:
			if keys, err := c.Map(part.Name); err == nil && keys.CollectedBelow() > 0 {
				floors[part] = keys.CollectedBelow()
			}
		}
	}
	return floors
}

// dominates reports whether a has given up at least as much as b everywhere b
// has given up anything, which is the condition for a to be the base without
// any floor going backwards.
func dominates(a, b map[crdt.Part]uint64) bool {
	for part, floor := range b {
		if a[part] < floor {
			return false
		}
	}
	return true
}

// A MultiStore keeps every document in several stores at once.
//
// # What it is for
//
// The stores in this project answer different questions. A database answers
// "what is the document now", quickly, which is what a server restarting needs.
// A git repository answers "what did it say last Tuesday, and who wrote this
// sentence", which is what a person needs. Neither answers the other's
// question, and an operator who wants both has until now had to choose.
//
// The alternative already in use is worse than choosing: writing to one store
// from the server and to the other from somewhere else — a browser, a sync
// job — which is two sources of truth held together by whichever of them
// happens to run last.
//
// # Reading merges rather than picking
//
// [Load] reads every store and merges what they return. The obvious design is
// to read the first store that has the document and stop, and it is wrong here
// for a reason particular to this problem: a save that failed halfway leaves
// the stores holding different documents, and reading only the first would
// quietly drop whatever only the second had. Merging is the only answer that
// loses nothing, and a CRDT is what makes it available.
//
// It has a consequence worth having on purpose: adding a store to a running
// server backfills it. The new store returns nothing, the merge is the other
// store's document unchanged, and the next save writes it across.
//
// The cost is that opening a document reads every store instead of one, and
// merges when more than one has content. That is paid once per document, when
// it is opened, and not per edit.
//
// # A store that cannot be read makes the document unavailable
//
// If any store fails to read, [Load] fails. It does not fall back to the stores
// that answered, because what came back would be a document that is missing
// whatever the unreadable store alone held — and the next save would then write
// that shortened document over the store that was merely unreachable. Serving a
// document that is quietly missing a paragraph is worse than serving none: an
// error stops at one document and an operator can fix it, while silent loss is
// discovered by the person who wrote the paragraph.
//
// A merge that refuses fails the same way and for the same reason. Two stores
// that have each discarded what the other still needs give [ErrUnmergeable],
// and one that holds a write the other can no longer accept gives
// [crdt.ErrStranded]; either way the document does not open, rather than
// opening as whichever of the two [Load] happened to reach first. It takes a
// store left behind by a purge or a collect to reach that at all — stores
// written together hold the same bytes, and merging those carries nothing.
//
// # Reading stops at the first store that refuses, and that is on purpose
//
// A member whose Load fails -- a file that rotted, a database that is down --
// fails the whole read. Redundancy here buys durability, not availability: one
// unreadable replica takes the document down even though a good copy is beside
// it, and an operator meeting that has to repair or remove the bad store rather
// than wait for a failover that is not coming.
//
// Serving the members that did answer would be worse than it looks. A snapshot
// is a set of operations and the merge is their union, so a member left out is
// not a smaller document, it is a document missing whatever only that member
// held -- and the [Save] that follows writes the merge of the others over it,
// which makes the loss permanent. Refusing keeps the operator's options open;
// answering closes them silently.
//
// # Writing tries every store, and fails if any refused
//
// [Save] writes to all of them even after one has failed, so that a store being
// down does not stop the others from being written, and then reports every
// failure together. It returns an error if any store refused, because a caller
// that gets nil back has to be able to believe the document is durable in all
// of them.
//
// They are written one after another rather than at the same time because there
// are two of them, not two hundred.
//
// It does not implement [SiteStore]. Go has no way to implement an interface
// only when what is underneath does, so a MultiStore that declared the methods
// would keep nothing whenever its stores could not — silently, which is worse than
// not offering it. A server given one falls back to the participants a document
// names, which is everyone who has written; see [SiteStore] for what that costs.
type MultiStore struct {
	stores []Store
}

// NewMultiStore returns a store that writes to all of the given stores and
// reads from all of them.
//
// It panics if given none: a store that silently keeps nothing would look like
// it was working, and there is no configuration in which that is what somebody
// meant. One is allowed, and behaves as that store does.
func NewMultiStore(stores ...Store) *MultiStore {
	if len(stores) == 0 {
		panic("collab: NewMultiStore needs at least one store")
	}
	return &MultiStore{stores: append([]Store(nil), stores...)}
}

// Load returns the merge of what every store holds, or nil if none of them has
// the document yet.
func (m *MultiStore) Load(ctx context.Context, document string) ([]byte, error) {
	var merged []byte
	for i, store := range m.stores {
		snapshot, err := store.Load(ctx, document)
		if err != nil {
			return nil, fmt.Errorf("collab: store %d reading %q: %w", i, document, err)
		}
		if len(snapshot) == 0 {
			continue
		}
		if merged == nil {
			// Nothing has been merged yet, so these bytes stand on their own.
			// Returning them unparsed is not only faster: in the ordinary case
			// where one store holds the document and the others are new, this
			// hands back exactly what was stored, and a snapshot this package
			// cannot read is diagnosed where it is loaded rather than here.
			merged = snapshot
			continue
		}
		if merged, err = MergeSnapshots(merged, snapshot); err != nil {
			return nil, fmt.Errorf("collab: merging %q from store %d: %w", document, i, err)
		}
	}
	return merged, nil
}

// Save writes the snapshot to every store, and reports every store that refused
// it.
func (m *MultiStore) Save(ctx context.Context, document string, snapshot []byte) error {
	var failures []error
	for i, store := range m.stores {
		if err := store.Save(ctx, document, snapshot); err != nil {
			failures = append(failures, fmt.Errorf("collab: store %d saving %q: %w", i, document, err))
		}
	}
	return errors.Join(failures...)
}
