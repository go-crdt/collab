package pgstore_test

import (
	"errors"
	"testing"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/collab/pgstore"
)

// The compiler's own check that this store has the capability, so a signature that
// drifts is a build failure rather than a type assertion that quietly stops matching
// and a server that quietly goes back to saving blindly.
var _ collab.ConditionalStore = (*pgstore.Store)(nil)

// A save conditioned on a version lands while that version is what is there, and is
// refused otherwise.
//
// This is what turns collab's TestTwoServersOverOneStoreLoseTheEarlierSave from a
// silent loss into an error: the second server's save does not carry the first's work
// away, it is told no. See [collab.ConditionalStore].
func TestASaveIsRefusedOverAVersionItDidNotRead(t *testing.T) {
	db := connect(t)
	store := fresh(t, db)
	ctx := t.Context()

	// Nothing written yet: no snapshot and no token, which is what asks for an
	// insert rather than an update.
	snapshot, token, err := store.LoadToken(ctx, "paper")
	if err != nil {
		t.Fatalf("LoadToken on a new document: %v", err)
	}
	if snapshot != nil || token != nil {
		t.Fatalf("a document nobody wrote gave snapshot %v and token %q, want nil and nil", snapshot, token)
	}

	first, err := store.SaveIf(ctx, "paper", []byte("AAAA"), nil)
	if err != nil {
		t.Fatalf("creating a document with a nil token: %v", err)
	}
	if first == nil {
		t.Fatal("a successful SaveIf returned no token, so nothing can be saved after it")
	}

	// A second server that also read nothing asks for the same insert. A row is
	// there now, so it is refused rather than replacing what it never saw.
	if _, err := store.SaveIf(ctx, "paper", []byte("BBBB"), nil); !errors.Is(err, collab.ErrChanged) {
		t.Fatalf("a second create gave %v, want collab.ErrChanged", err)
	}

	// The token the save returned is what the next one is made against.
	second, err := store.SaveIf(ctx, "paper", []byte("CCCC"), first)
	if err != nil {
		t.Fatalf("saving against the token the previous save returned: %v", err)
	}
	// And the one before it is now stale, which is the whole mechanism.
	if _, err := store.SaveIf(ctx, "paper", []byte("DDDD"), first); !errors.Is(err, collab.ErrChanged) {
		t.Fatalf("saving against a spent token gave %v, want collab.ErrChanged", err)
	}

	got, err := store.Load(ctx, "paper")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(got) != "CCCC" {
		t.Errorf("the document holds %q, want CCCC: the refused saves must not have landed", got)
	}
	if _, err := store.SaveIf(ctx, "paper", []byte("EEEE"), second); err != nil {
		t.Errorf("saving against the current token: %v", err)
	}
}

// A blind Save spends the version too, so a token held across one is refused.
//
// Worth its own test because it is the way the two could have disagreed: Save is the
// unconditional path and it would have been easy to leave the version alone there,
// which would let a server hold a token across somebody else's blind write and then
// save over it — the exact loss this interface exists to refuse, reintroduced by the
// store that implements it.
func TestABlindSaveSpendsTheVersion(t *testing.T) {
	db := connect(t)
	store := fresh(t, db)
	ctx := t.Context()

	held, err := store.SaveIf(ctx, "paper", []byte("AAAA"), nil)
	if err != nil {
		t.Fatalf("creating the document: %v", err)
	}
	// Somebody else writes without a condition, as a server on an older build or a
	// migration tool would.
	if err := store.Save(ctx, "paper", []byte("BBBB")); err != nil {
		t.Fatalf("a blind Save: %v", err)
	}
	if _, err := store.SaveIf(ctx, "paper", []byte("CCCC"), held); !errors.Is(err, collab.ErrChanged) {
		t.Fatalf("a token held across a blind Save gave %v, want collab.ErrChanged", err)
	}
	got, err := store.Load(ctx, "paper")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(got) != "BBBB" {
		t.Errorf("the document holds %q, want BBBB", got)
	}
}

// An unusable token is a caller's mistake and is said so, rather than being taken for
// a version nobody holds.
func TestAnUnusableTokenIsNotARefusal(t *testing.T) {
	db := connect(t)
	store := fresh(t, db)
	ctx := t.Context()
	if _, err := store.SaveIf(ctx, "paper", []byte("AAAA"), collab.Token("not a version")); err == nil {
		t.Fatal("an unusable token was accepted")
	} else if errors.Is(err, collab.ErrChanged) {
		t.Errorf("an unusable token was reported as ErrChanged (%v), which says the store "+
			"holds something else rather than that the caller passed rubbish", err)
	}
}
