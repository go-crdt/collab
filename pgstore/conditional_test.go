package pgstore_test

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
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

// LoadToken reads the document as Load does, and the token it returns is one SaveIf
// accepts — which is the round trip the capability is for, and which the first
// version of these tests never exercised: it only ever asked for a token on a
// document nobody had written, so the path that returns one was never taken. The
// coverage gate said so.
func TestLoadTokenReadsTheDocumentAndAUsableToken(t *testing.T) {
	db := connect(t)
	store := fresh(t, db)
	ctx := t.Context()

	if _, err := store.SaveIf(ctx, "paper", []byte("AAAA"), nil); err != nil {
		t.Fatalf("creating the document: %v", err)
	}
	got, token, err := store.LoadToken(ctx, "paper")
	if err != nil {
		t.Fatalf("LoadToken: %v", err)
	}
	if string(got) != "AAAA" {
		t.Errorf("LoadToken read %q, want AAAA — it must read what Load reads", got)
	}
	if token == nil {
		t.Fatal("LoadToken gave no token for a document that is there")
	}
	// The round trip: a server that opened this document can save against what it
	// read, which is the whole point of reading the token with it.
	if _, err := store.SaveIf(ctx, "paper", []byte("BBBB"), token); err != nil {
		t.Errorf("saving against the token LoadToken gave: %v", err)
	}
}

// An emptied row is refused by LoadToken for the reason Load refuses it: nil is how a
// store says "new document", there is no row for that, so a row holding nothing is
// one somebody emptied and answering nil would make the loss permanent.
func TestLoadTokenRefusesAnEmptyRow(t *testing.T) {
	db := connect(t)
	store, table := freshNamed(t, db)
	if _, err := db.Exec("INSERT INTO "+table+" (document, snapshot) VALUES ($1, $2)", "d", []byte{}); err != nil {
		t.Fatal(err)
	}
	got, token, err := store.LoadToken(t.Context(), "d")
	if err == nil {
		t.Fatalf("an empty row was served as %d bytes and token %q rather than refused", len(got), token)
	}
	if !strings.Contains(err.Error(), "empty row") {
		t.Errorf("refused, but not for the reason this test is about: %v", err)
	}
}

// LoadToken checks the frame exactly as Load does: a token is no reason to serve
// bytes whose checksum no longer matches.
//
// Asserted as AGREEMENT rather than against a chosen byte, because which offsets
// break a frame is the frame's business and a test that picks one is a test of the
// layout. The first version of this flipped byte four and the row still read, so it
// proved nothing. It also requires that some offset really is refused, or agreement
// would be satisfied by a frame nothing could break.
func TestLoadTokenRefusesWhatLoadRefuses(t *testing.T) {
	db := connect(t)
	store, table := freshNamed(t, db)
	ctx := t.Context()
	refusals := 0
	for offset := range 24 {
		doc := fmt.Sprintf("d%d", offset)
		if _, err := store.SaveIf(ctx, doc, []byte("AAAABBBBCCCC"), nil); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("UPDATE "+table+
			" SET snapshot = set_byte(snapshot, $1, get_byte(snapshot, $1) # 1) WHERE document = $2",
			offset, doc); err != nil {
			t.Fatal(err)
		}
		_, loadErr := store.Load(ctx, doc)
		_, _, tokenErr := store.LoadToken(ctx, doc)
		if (loadErr == nil) != (tokenErr == nil) {
			t.Errorf("byte %d: Load said %v and LoadToken said %v — one is laxer than the other",
				offset, loadErr, tokenErr)
		}
		if loadErr != nil {
			refusals++
		}
	}
	if refusals == 0 {
		t.Error("no flipped byte was refused, so this test agreed about nothing")
	}
}

// And on a database that is gone, both of them say so rather than answering.
func TestTheConditionalPathReportsADatabaseThatIsGone(t *testing.T) {
	db := connect(t)
	_ = fresh(t, db)
	closed, err := sql.Open("pgx", os.Getenv("COLLAB_POSTGRES"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if err := closed.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	gone, err := pgstore.New(closed)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := t.Context()
	if _, _, err := gone.LoadToken(ctx, "doc"); err == nil {
		t.Error("LoadToken on a closed database reported success")
	}
	if _, err := gone.SaveIf(ctx, "doc", []byte("AAAA"), nil); err == nil {
		t.Error("SaveIf on a closed database reported success")
	} else if errors.Is(err, collab.ErrChanged) {
		t.Errorf("a closed database was reported as ErrChanged (%v), which says the store "+
			"holds something else rather than that it could not be asked", err)
	}
	if _, err := gone.SaveIf(ctx, "doc", []byte("AAAA"), collab.Token("1")); err == nil {
		t.Error("a conditional SaveIf on a closed database reported success")
	}
}
