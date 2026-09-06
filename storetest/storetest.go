// Package storetest is the contract every [collab.Store] keeps, written once
// and run by each implementation against itself.
//
// # Why it exists
//
// The stores in this repository drifted apart, and each divergence was found
// separately and late. A DirStore refused a zero-length snapshot as the torn
// write it is; pgstore answered it as a new document. A DirStore checksummed
// what it wrote; gitstore read a file git was not protecting. A DirStore
// refused two names a case-folding filesystem cannot tell apart; nothing else
// had to think about it. None of those are hard to get right. What was missing
// is a single place that says what right is, and asks every store the same
// questions.
//
// # What it does not do
//
// It asserts what [collab.Store] documents, and nothing it does not. Where the
// stores differ on something the contract leaves open — whether an empty
// document name is refused, say — this stays quiet rather than legislating from
// a test. A conformance suite that invents requirements makes implementations
// agree about the suite instead of about the contract.
//
// # Using it
//
//	func TestConformance(t *testing.T) {
//		storetest.Run(t, func(t *testing.T) storetest.Harness {
//			return storetest.Harness{Store: collab.NewMemoryStore()}
//		})
//	}
//
// The function is called again for every case, and must return a store with
// nothing in it: a suite that shared one store would let each case see what the
// case before it wrote, and a failure would name the wrong one.
package storetest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// A Harness is one store to ask the questions of, and what a test may do to the
// medium underneath it.
//
// The optional fields are nil when the concept does not apply, and the case
// that needs one skips rather than fails. A MemoryStore cannot rot and cannot
// be truncated, and saying so by leaving a field nil is clearer than a boolean
// that has to be read against the store it came from.
type Harness struct {
	// Store is the store under test, holding nothing.
	Store collab.Store

	// Corrupt changes the nth byte of what the store holds for document, as a
	// medium that rots would, without going through Save. It reports false when
	// there is no nth byte, which is how the case knows it has been through all
	// of them. nil when a test cannot reach the medium.
	//
	// One byte is not enough to ask about. The first version of this asked
	// about a single byte near the end, and a store with its checksum
	// comparison disabled PASSED: a change there breaks the compressed stream,
	// which the decompressor refuses on its own. The case has to walk the
	// whole thing, or it tests the compressor.
	Corrupt func(document string, nth int) (bool, error)

	// Truncate makes the store hold zero bytes for document, which is what a
	// crash between creating a file and writing it leaves behind. nil when the
	// store cannot be put in that state from outside.
	Truncate func(document string) error

	// Close releases whatever making the store took. May be nil.
	Close func()
}

// A Make returns a fresh [Harness]. It is called once per case.
type Make func(t *testing.T) Harness

// A T is the part of [testing.T] the cases use.
//
// It is an interface so that the suite can be run against stores that are
// deliberately wrong and the failures counted. Nothing else guards a
// conformance suite: a case that has stopped asking anything passes exactly as
// loudly as one that holds. The first version of the rot case here corrupted a
// single byte near the end of a compressed document, and a store with its
// checksum comparison switched off PASSED it -- the decompressor refused on its
// own, so the case tested the decompressor. selftest_test.go is what caught
// that class of mistake afterwards.
type T interface {
	Helper()
	Logf(format string, args ...any)
	Errorf(format string, args ...any)
	Fatal(args ...any)
	Fatalf(format string, args ...any)
	Skip(args ...any)
}

// Run asks make for a store and puts the contract to it.
func Run(t *testing.T, make Make) {
	t.Helper()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := make(t)
			if h.Close != nil {
				t.Cleanup(h.Close)
			}
			c.run(t, h)
		})
	}
}

var cases = []struct {
	name string
	run  func(T, Harness)
}{
	{"LoadOfADocumentNobodyHasSavedIsNilAndNotAnError", loadOfNothing},
	{"WhatWasSavedIsWhatComesBack", roundTrip},
	{"SavingAgainReplaces", replaces},
	{"TwoNamesAreTwoDocuments", twoNames},
	{"NamesAreEitherKeptApartOrRefused", awkwardNames},
	{"AZeroLengthSnapshotIsRefusedAndNotCalledANewDocument", tornWrite},
	{"AStoredSnapshotThatChangedIsRefused", rotted},
	{"ConcurrentUseIsSafe", concurrent},
}

// snapshot returns a composite snapshot with text in it, distinct per seed, so
// that a store confusing two documents is caught by what comes back rather than
// by how much.
func snapshot(t T, seed string) []byte {
	t.Helper()
	c := crdt.NewComposite(1)
	body, err := c.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, "this document is "+seed+", and nothing else is"); err != nil {
		t.Fatal(err)
	}
	return c.Snapshot()
}

// nil, and only nil, means "none yet".
func loadOfNothing(t T, h Harness) {
	got, err := h.Store.Load(context.Background(), "nobody has saved this")
	if err != nil {
		t.Fatalf("a document nobody has saved is not an error, got %v", err)
	}
	if got != nil {
		t.Fatalf("a document nobody has saved answered %d bytes, want nil", len(got))
	}
}

func roundTrip(t T, h Harness) {
	ctx := context.Background()
	want := snapshot(t, "the only one")
	if err := h.Store.Save(ctx, "doc", want); err != nil {
		t.Fatal(err)
	}
	got, err := h.Store.Load(ctx, "doc")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("came back as %d bytes, saved %d", len(got), len(want))
	}
}

func replaces(t T, h Harness) {
	ctx := context.Background()
	if err := h.Store.Save(ctx, "doc", snapshot(t, "first")); err != nil {
		t.Fatal(err)
	}
	second := snapshot(t, "second")
	if err := h.Store.Save(ctx, "doc", second); err != nil {
		t.Fatal(err)
	}
	got, err := h.Store.Load(ctx, "doc")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(second) {
		t.Fatal("the second save did not replace the first")
	}
}

// Two documents are two documents, including when their names differ only in
// case: the macOS default filesystem and NTFS fold it, and a store that lets
// them land in one place serves one document's history as the other's.
func twoNames(t T, h Harness) {
	ctx := context.Background()
	lower, upper := snapshot(t, "lower"), snapshot(t, "upper")
	if err := h.Store.Save(ctx, "doc", lower); err != nil {
		t.Fatal(err)
	}
	err := h.Store.Save(ctx, "DOC", upper)
	if err != nil {
		// Refusing is an answer, as long as it did not overwrite first. A
		// store on a filesystem it cannot tell names apart on must refuse
		// rather than write.
		got, loadErr := h.Store.Load(ctx, "doc")
		if loadErr != nil {
			t.Fatalf("after refusing to save %q it can no longer read %q: %v", "DOC", "doc", loadErr)
		}
		if string(got) != string(lower) {
			t.Fatal("the refused save changed the document it refused to be")
		}
		return
	}
	got, err := h.Store.Load(ctx, "doc")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(lower) {
		t.Fatal("saving DOC changed doc: two names landed in one document")
	}
}

// A document name is arbitrary UTF-8 and carries structure. A store may refuse
// one it cannot keep — but then it must refuse it consistently, and it must
// never let two of them become one.
func awkwardNames(t T, h Harness) {
	ctx := context.Background()
	names := []string{
		"project:default",
		"project:ods:chapter one.ods",
		"projet:défaut",
		"a/b",
		"..",
		".hidden",
		"one two three",
		strings.Repeat("n", 120),
	}
	kept := map[string][]byte{}
	for i, name := range names {
		want := snapshot(t, fmt.Sprintf("number %d", i))
		if err := h.Store.Save(ctx, name, want); err != nil {
			// Refused. It must stay refused, not come back as somebody else.
			if got, err := h.Store.Load(ctx, name); err == nil && got != nil {
				t.Errorf("%q was refused on save and answered %d bytes on load", name, len(got))
			}
			continue
		}
		kept[name] = want
	}
	for name, want := range kept {
		got, err := h.Store.Load(ctx, name)
		if err != nil {
			t.Errorf("%q was saved and cannot be read: %v", name, err)
			continue
		}
		if string(got) != string(want) {
			for other, theirs := range kept {
				if other != name && string(got) == string(theirs) {
					t.Errorf("%q came back holding %q: two names landed in one document", name, other)
				}
			}
			t.Errorf("%q came back as something it was not saved as", name)
		}
	}
}

// A zero-length snapshot is not an empty document. Answering nil would open an
// empty replica whose next save makes the loss permanent.
func tornWrite(t T, h Harness) {
	if h.Truncate == nil {
		t.Skip("this store cannot be left holding zero bytes from outside")
	}
	ctx := context.Background()
	if err := h.Store.Save(ctx, "doc", snapshot(t, "before the crash")); err != nil {
		t.Fatal(err)
	}
	if err := h.Truncate("doc"); err != nil {
		t.Fatal(err)
	}
	got, err := h.Store.Load(ctx, "doc")
	if err == nil {
		t.Fatalf("a torn write was answered as %d bytes rather than refused", len(got))
	}
}

// A store hands back what it was given or says it cannot. Serving bytes that
// changed underneath it is the one failure a caller cannot detect: the document
// opens, the server serves it, and the next save makes it the one everybody
// has.
//
// Every byte, one at a time, each from a fresh save. What is asserted is not
// that the store refuses -- a store may hand back bytes that no longer open,
// and the refusal is then one layer up -- but that no change is ever served as
// a DIFFERENT document.
func rotted(t T, h Harness) {
	if h.Corrupt == nil {
		t.Skip("this store has no medium a test can reach")
	}
	ctx := context.Background()
	want := snapshot(t, "what somebody wrote")
	base, err := crdt.LoadComposite(2, want)
	if err != nil {
		t.Fatal(err)
	}
	same := string(base.Snapshot())

	var refused, handedBack, different int
	for nth := 0; ; nth++ {
		if err := h.Store.Save(ctx, "doc", want); err != nil {
			t.Fatal(err)
		}
		more, err := h.Corrupt("doc", nth)
		if err != nil {
			t.Fatal(err)
		}
		if !more {
			break
		}
		got, err := h.Store.Load(ctx, "doc")
		if err != nil {
			refused++
			continue
		}
		handedBack++
		if c, err := crdt.LoadComposite(2, got); err == nil && string(c.Snapshot()) != same {
			different++
		}
	}
	if refused+handedBack == 0 {
		t.Fatal("Corrupt reported no bytes at all, so this proves nothing")
	}
	if different != 0 {
		t.Errorf("%d of %d one-byte changes were served as a different document", different, refused+handedBack)
	}
	t.Logf("%d one-byte changes: %d refused, %d handed back, %d a different document",
		refused+handedBack, refused, handedBack, different)
}

// Implementations must be safe for concurrent use, which is stated and was
// never asked of them all in one place.
func concurrent(t T, h Harness) {
	ctx := context.Background()
	// Built here rather than in the goroutines: t.Fatal belongs to the
	// goroutine running the test, and snapshot calls it.
	writes := make([][]byte, 8)
	for i := range writes {
		writes[i] = snapshot(t, fmt.Sprintf("writer %d", i))
	}
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = h.Store.Save(ctx, fmt.Sprintf("doc%d", i%3), writes[i])
		}()
		go func() {
			defer wg.Done()
			_, _ = h.Store.Load(ctx, fmt.Sprintf("doc%d", i%3))
		}()
	}
	wg.Wait()
}
