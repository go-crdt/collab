//go:build !js

package collab_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/collab"
)

// These hold guards that `mutsweep -sites all` found nothing holding, in the
// stores. Every test carries its control first: the thing the guard must leave
// alone is shown to be seen before the thing it must refuse is asserted.

// answerStore is a Store whose Load answers a fixed value.
type answerStore struct {
	answer []byte
}

func (s answerStore) Load(context.Context, string) ([]byte, error) { return s.answer, nil }
func (answerStore) Save(context.Context, string, []byte) error     { return nil }

// multistore.go:356, `len(snapshot) == 0 { continue }`. A store that answers a
// zero-length slice has nothing to add; deleted, the empty slice becomes
// "merged", Load answers non-nil for a document nobody has, and the next real
// snapshot is merged into nothing.
func TestAMultiStoreSkipsAStoreThatAnswersEmpty(t *testing.T) {
	ctx := context.Background()
	real := collab.NewMemoryStore()
	if err := real.Save(ctx, "d", writing(t, 1, "hello")); err != nil {
		t.Fatal(err)
	}
	// The control: the real snapshot is what comes back when an empty answer
	// does not come first.
	want, err := collab.NewMultiStore(real).Load(ctx, "d")
	if err != nil || len(want) == 0 {
		t.Fatalf("control: Load = %d bytes, %v", len(want), err)
	}

	got, err := collab.NewMultiStore(answerStore{answer: []byte{}}, real).Load(ctx, "d")
	if err != nil {
		t.Fatalf("an empty answer before a real one broke the merge: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("an empty answer before a real one changed the result: %d bytes, want %d", len(got), len(want))
	}

	only, err := collab.NewMultiStore(answerStore{answer: []byte{}}).Load(ctx, "d")
	if err != nil {
		t.Fatal(err)
	}
	if only != nil {
		t.Errorf("a store that answers empty made Load answer %v, want nil (no document)", only)
	}
}

// multistore.go:359, `if merged == nil`. The first snapshot stands on its own
// and is handed back unparsed: a snapshot this package cannot read is diagnosed
// where it is loaded, not here. Deleted, it is parsed by MergeSnapshots.
func TestAMultiStoreHandsBackTheFirstSnapshotUnparsed(t *testing.T) {
	ctx := context.Background()
	garbage := []byte("not a snapshot of anything")
	got, err := collab.NewMultiStore(answerStore{answer: garbage}, answerStore{}).Load(ctx, "d")
	if err != nil {
		t.Fatalf("one store holding the document was parsed on the way out: %v", err)
	}
	if !bytes.Equal(got, garbage) {
		t.Errorf("Load = %q, want exactly what the one store held", got)
	}
}

// parts.go:186. A negative length whose far end still lands inside the text is
// refused, not read as a backwards range. (0, -1), the case textundo_test.go
// asserts, is refused by the offset conversion whether or not the guard is there.
func TestDeletingANegativeLengthInsideTheTextIsRefused(t *testing.T) {
	_, conn := serve(t, collab.Config{Store: collab.NewMemoryStore()})
	c := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})
	doc := body(t, c)
	if err := doc.Insert(0, "hello"); err != nil {
		t.Fatal(err)
	}
	// The control: the same offsets the other way round are a real deletion.
	if err := doc.DeleteUTF16(1, 2); err != nil || doc.String() != "hlo" {
		t.Fatalf("control: DeleteUTF16(1, 2) = %v, text %q", err, doc.String())
	}
	if err := doc.DeleteUTF16(2, -1); err == nil {
		t.Errorf("DeleteUTF16(2, -1) was accepted, text %q", doc.String())
	}
	if got := doc.String(); got != "hlo" {
		t.Errorf("a refused deletion changed the text to %q", got)
	}
}

// dirstore.go:90 and :93: each refusal says WHICH thing is wrong. Deleting
// either still ends in an error (MkdirAll("") and the sweep both fail), so a
// test that asked only for an error could not see it.
func TestADirStoreNamesWhatIsWrongWithItsDirectory(t *testing.T) {
	if _, err := collab.NewDirStore(t.TempDir()); err != nil {
		t.Fatalf("control: %v", err)
	}
	_, err := collab.NewDirStore("")
	if err == nil || !strings.Contains(err.Error(), "needs a directory") {
		t.Errorf("an empty directory name = %v, want the refusal that says a directory is needed", err)
	}
	inTheWay := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(inTheWay, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = collab.NewDirStore(filepath.Join(inTheWay, "under"))
	if err == nil || !strings.Contains(err.Error(), "collab: making") {
		t.Errorf("a directory that cannot be made = %v, want the error that says it could not be made", err)
	}
}

// dirstore.go:255 and :277: a DIRECTORY whose name happens to decode is not a
// document. The other half of the same condition, the temp prefix, is held by
// its own name not being base64.
func TestADirectoryInAStoreIsNotADocument(t *testing.T) {
	dir := t.TempDir()
	store, err := collab.NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := store.Save(ctx, "real", []byte("snapshot")); err != nil {
		t.Fatal(err)
	}
	stray := filepath.Join(dir, base64.URLEncoding.EncodeToString([]byte("stray")))
	if err := os.Mkdir(stray, 0o700); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-48 * time.Hour)
	for _, p := range []string{stray, filepath.Join(dir, base64.URLEncoding.EncodeToString([]byte("real")))} {
		if err := os.Chtimes(p, old, old); err != nil {
			t.Fatal(err)
		}
	}

	docs, err := store.Documents()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(docs, []string{"real"}) {
		t.Errorf("Documents = %q, want only [real]: a directory was listed as a document", docs)
	}
	idle, err := store.Idle(ctx, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(idle, []string{"real"}) {
		t.Errorf("Idle = %q, want only [real]: a directory was offered for archiving", idle)
	}
}
