//go:build (js && wasm) || !js

package collab

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-crdt/crdt"
)

// MergeSnapshots combines two snapshots of the same document into one that
// holds everything either of them held.
//
// It is the operation that makes a snapshot safe to keep in more than one
// place. Two copies of a document that were written separately have not
// disagreed about anything — a snapshot is a set of operations, and the union
// of two sets of operations is a document, which is the whole reason this
// project exists. Nothing here has to choose a side, and so nothing here can
// lose what the other side did.
//
// Either argument may be nil, which is how a store says it has never held the
// document; merging with nothing gives back the other side. Merging is
// symmetric to the byte, because the snapshot encoding is canonical and both
// results hold the same operations — with one exception, which is that a side
// that has collected ([crdt.Composite.Collect]) carries a record of what it
// gave back, and the merge is done from that side because it is the only one
// that can still produce a difference. The operations are the same either way;
// the bytes then say which side was collected.
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
	// Merging is done from whichever side can still say what it holds.
	//
	// Usually that is either of them and the first try is the answer. It is not
	// either of them once one side has collected: a side that gave back the
	// operations the other has not seen cannot produce a difference from where
	// the other stands, and says so. The other direction is then the one that
	// works, and it holds the same operations — which is why this is a fallback
	// and not a failure.
	merged, err := mergeInto(mine, yours)
	if errors.Is(err, crdt.ErrCollected) {
		merged, err = mergeInto(yours, mine)
	}
	if err != nil {
		// Both sides have collected past the other, so neither can say what the
		// other is missing and there is no third replica to ask. Whoever holds
		// these two has to choose one, which is a choice this cannot make for
		// them.
		return nil, fmt.Errorf("collab: neither side can be caught up from the other: %w", err)
	}
	return merged, nil
}

// mergeInto applies to base everything other holds that it does not, and
// returns the result.
//
// Apply's error is not carried: other loaded, so it is causally complete —
// OpsSince returns operations that validate, Apply accepts them, and nothing is
// left waiting for something that snapshot did not hold.
func mergeInto(base, other *crdt.Composite) ([]byte, error) {
	ops, err := other.OpsSince(base.Version())
	if err != nil {
		return nil, err
	}
	_ = base.Apply(ops...)
	return base.Snapshot(), nil
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
