//go:build (js && wasm) || !js

package collab

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"
)

// An Archivable store can say which of its documents have gone quiet and let
// one go, which is what it takes to be the hot half of a [Tiered].
//
// Both methods are here rather than in [Store] because most stores have no
// business implementing them: a store that is only ever written through is
// complete with Load and Save, and asking every one of them for a way to
// forget a document would be asking for a way to lose one.
type Archivable interface {
	Store

	// Idle returns the documents this store has not been asked to write for
	// longer than d.
	//
	// Not written is the closest thing to not used that a store can know, and
	// it is close enough for the reason that matters: a server persists a
	// document somebody is in every [Config.PersistEvery], so a document with
	// anybody in it is written constantly and never looks idle. What looks idle
	// is what nobody has opened since it was last put away.
	Idle(ctx context.Context, d time.Duration) ([]string, error)

	// Release forgets a document, but only if this store still holds exactly
	// want. It reports [ErrChanged] if it holds something else, and nil if it
	// held nothing.
	//
	// The condition is the whole of it. An archiver copies a document
	// elsewhere and then asks for it to be released, and between those two
	// moments somebody may have joined the document and saved a newer one. A
	// store that released it anyway would delete a version that is nowhere
	// else. The comparison and the removal therefore happen together, under
	// whatever the store uses to keep its own writes apart.
	Release(ctx context.Context, document string, want []byte) error
}

// ErrChanged reports a document that was written since it was read, so it was
// not released. It is not a failure: the next pass will archive the newer one.
var ErrChanged = errors.New("collab: the document changed since it was read")

// A Tiered store keeps documents somebody is using in one store and documents
// nobody has opened for a long time in another.
//
// # What it is for
//
// A server holds every document it has ever served until it is told to let go —
// see [Config.EvictAfter] — and a store holds every document it has ever been
// given, with nothing to let go of it at all. A year of a busy service is a
// directory of documents that were opened once, and there has been no way to
// move them anywhere cheaper without deleting them.
//
// # Nothing is deleted that is not already somewhere else
//
// Archiving is three steps in one order: read the hot store, write the cold
// one, then ask the hot one to release exactly what was read. Every way it can
// fail leaves the document somewhere. A cold store that will not take it
// releases nothing. A document that was written between the read and the
// release is not released, because [Archivable.Release] compares before it
// removes; it is archived on a later pass, when it has gone quiet again.
//
// # An archived document is not a missing one
//
// [Store.Load] returning nil means "there is no such document, start a new
// one", and a server acts on it: it opens an empty document and, at its next
// save, writes it over whatever was there. So a Tiered store never answers nil
// for a document the cold store has, and never answers nil because it could not
// reach the cold store — it fails instead. A document that is unreachable is
// not a document that does not exist, and confusing the two is how a store
// loses what it was given.
//
// Reading an archived document brings it back: it is written to the hot store
// on the way past, so the next read does not go looking again and the next
// save has somewhere to land.
//
// It does not implement [SiteStore]. Go has no way to implement an interface
// only when what is underneath does, so a Tiered that declared the methods
// would keep nothing whenever its tiers could not — silently, which is worse than
// not offering it. A server given one falls back to the participants a document
// names, which is everyone who has written; see [SiteStore] for what that costs.
type Tiered struct {
	hot  Archivable
	cold Store
}

// NewTiered returns a store that writes to hot and falls back to cold.
//
// It panics if either is nil, which is a mistake in the call rather than a
// state anything could recover from.
func NewTiered(hot Archivable, cold Store) *Tiered {
	if hot == nil || cold == nil {
		panic("collab: NewTiered needs both a hot store and a cold one")
	}
	return &Tiered{hot: hot, cold: cold}
}

// Load returns the document from the hot store, or from the cold one, bringing
// it back to the hot store on the way.
func (t *Tiered) Load(ctx context.Context, document string) ([]byte, error) {
	hot, err := t.hot.Load(ctx, document)
	if err != nil {
		return nil, fmt.Errorf("collab: reading %q from the hot store: %w", document, err)
	}
	// nil, and only nil, is "not in the hot store": a zero-length answer is a
	// store's own refusal or a torn file, and falling through to the archive
	// would serve a STALE document as the current one and write it back over
	// the hot copy at the next save.
	if hot != nil {
		return hot, nil
	}

	cold, err := t.cold.Load(ctx, document)
	if err != nil {
		// Not nil. Nil is how a store says "new document", and a server that
		// believes it opens an empty one and saves it over the archive.
		return nil, fmt.Errorf("collab: reading %q from the archive: %w", document, err)
	}
	if len(cold) == 0 {
		return nil, nil
	}
	// Bringing it back is a shortcut for the next read, not the answer to this
	// one: the document is in hand and the archive still has it, so a hot store
	// that will not take it changes nothing a reader can see.
	_ = t.hot.Save(ctx, document, cold)
	return cold, nil
}

// Save writes to the hot store, which is where a document being edited belongs.
func (t *Tiered) Save(ctx context.Context, document string, snapshot []byte) error {
	return t.hot.Save(ctx, document, snapshot)
}

// Archive moves every document the hot store has not been asked to write for
// idleFor into the cold store, and returns how many it moved.
//
// It is a method rather than a timer of its own because a server already has
// housekeeping running on a schedule an operator chose, and a store that starts
// goroutines is a store that has to be closed.
//
// A document that could not be archived does not stop the ones after it: the
// errors are returned together, and what was moved is reported whatever else
// happened.
func (t *Tiered) Archive(ctx context.Context, idleFor time.Duration) (int, error) {
	quiet, err := t.hot.Idle(ctx, idleFor)
	if err != nil {
		return 0, fmt.Errorf("collab: looking for documents to archive: %w", err)
	}

	moved := 0
	var failures []error
	for _, document := range quiet {
		snapshot, err := t.hot.Load(ctx, document)
		if err != nil {
			failures = append(failures, fmt.Errorf("collab: reading %q to archive it: %w", document, err))
			continue
		}
		if len(snapshot) == 0 {
			// Archived by somebody else, or never really there.
			continue
		}
		if err := t.cold.Save(ctx, document, snapshot); err != nil {
			// Nothing is released, so nothing is lost by giving up here.
			failures = append(failures, fmt.Errorf("collab: archiving %q: %w", document, err))
			continue
		}
		switch err := t.hot.Release(ctx, document, snapshot); {
		case errors.Is(err, ErrChanged):
			// Somebody wrote it while it was being copied. The copy in the
			// cold store is older than the hot one and will be replaced when
			// this document next goes quiet; releasing now would delete the
			// newer one.
			continue
		case err != nil:
			failures = append(failures, fmt.Errorf("collab: releasing %q after archiving it: %w", document, err))
		default:
			moved++
		}
	}
	return moved, errors.Join(failures...)
}

// stillHolds reports whether stored is what an archiver asked to release, which
// is the comparison every [Archivable] has to make and none of them should
// write twice.
func stillHolds(stored, want []byte) error {
	if !bytes.Equal(stored, want) {
		return ErrChanged
	}
	return nil
}
