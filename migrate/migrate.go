// Package migrate moves the documents in a collab store out of a snapshot
// format that is no longer read.
//
// crdt v0.42.0 stopped reading text format 6, on the reasoning that nobody holds
// a snapshot in it. A store is exactly where one is held: every document a
// server has persisted is a snapshot in whatever format its crdt wrote, and
// collab selected a crdt that wrote format 6 until collab v0.37.0.
//
// So a DirStore directory, a Postgres table or a git repository filled before
// then has documents a current build cannot open. This reads each one with a
// build that still can and writes it back in a format that will be read, which
// is the only way across: nothing converts a snapshot without understanding it.
//
// # Build this against the pinned versions
//
// The module pins crdt v0.41.0 because it reads format 6 and writes 8. Built
// against anything that has forgotten 6, [Rewrite] refuses rather than reporting
// that every document was already current -- which is what a silent no-op would
// look like from outside.
package migrate

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// ErrCannotRead reports a build that has itself forgotten the format it was
// meant to move documents out of.
var ErrCannotRead = errors.New("migrate: this build cannot read the format it exists to move")

// A Result counts what happened, because "it worked" is not a thing a person
// should have to take on trust after rewriting their documents.
type Result struct {
	// Moved were read in an old format and written back in a current one.
	Moved []string
	// Current were already in a format a current build reads, and were left
	// alone rather than rewritten for the sake of it.
	Current []string
	// Failed could not be read at all, with the reason. A document here is not
	// lost -- nothing was written over it -- but it is not moved either.
	Failed map[string]error
}

// Rewrite moves each named document into a format a current build can read.
//
// The names come from the caller because a store enumerates its documents in its
// own way and its own signature: DirStore.Documents takes no context, pgstore's
// does, MemoryStore's returns no error. Asking for the list rather than an
// interface keeps this from inventing a fourth shape.
//
// A document already in a readable format is left exactly as it is. Rewriting it
// would change its bytes for nothing, and a migration that touches what it need
// not is a migration nobody can check.
func Rewrite(ctx context.Context, store collab.Store, documents []string) (Result, error) {
	return rewrite(ctx, store, documents, reads(oldTextFormat))
}

// rewrite is Rewrite with the answer to "can this build still read the old
// format" passed in, so that a test can ask what happens when it cannot without
// being built against a crdt that cannot -- which is the one arrangement this
// module must never be shipped in.
func rewrite(ctx context.Context, store collab.Store, documents []string, canRead bool) (Result, error) {
	// The pin, enforced. If the format this exists to move is not among what
	// this build accepts, every document would be reported as unreadable for a
	// reason that is not about the document.
	if !canRead {
		return Result{}, fmt.Errorf("%w: text format %d", ErrCannotRead, oldTextFormat)
	}

	out := Result{Failed: map[string]error{}}
	for _, name := range documents {
		snapshot, err := store.Load(ctx, name)
		if err != nil {
			out.Failed[name] = err
			continue
		}
		if len(snapshot) == 0 {
			continue // a name with no document behind it
		}
		doc, err := crdt.LoadComposite(migrateSite, snapshot)
		if err != nil {
			out.Failed[name] = err
			continue
		}
		rewritten := doc.Snapshot()
		if string(rewritten) == string(snapshot) {
			out.Current = append(out.Current, name)
			continue
		}
		if err := store.Save(ctx, name, rewritten); err != nil {
			out.Failed[name] = err
			continue
		}
		out.Moved = append(out.Moved, name)
	}
	return out, nil
}

// oldTextFormat is the version crdt stopped reading in v0.42.0, and the reason
// this package exists.
const oldTextFormat = 6

// migrateSite is the replica identity a rewrite loads under. It writes no
// operations, so the identity never reaches a document -- but loading needs one,
// and a number nothing else uses is easier to recognise in a log than 1.
const migrateSite crdt.SiteID = 999

func reads(version byte) bool {
	for _, v := range crdt.Reads(crdt.FormatText) {
		if v == version {
			return true
		}
	}
	return false
}
