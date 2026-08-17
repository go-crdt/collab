//go:build !js

package collab_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// A handle is the whole of what a caller touches, so the rest of its surface —
// the parts the session tests do not happen to use — is exercised here against a
// real session, not against a bare replica: an edit that never reached the wire
// would still pass a test of the structure underneath.
func TestTheHandlesDoWhatTheySay(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	ada := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})
	grace := join(t, conn, collab.ClientConfig{Document: "doc", Site: 2})

	body, err := ada.Text("file:a.tex")
	if err != nil {
		t.Fatal(err)
	}
	notes, err := ada.List("notes")
	if err != nil {
		t.Fatal(err)
	}
	cells, err := ada.Map("cells")
	if err != nil {
		t.Fatal(err)
	}

	// Each handle knows its own name and part, which is how a caller matches a
	// reported change to the view watching it.
	if got := body.Name(); got != "file:a.tex" {
		t.Errorf("Text.Name() = %q", got)
	}
	if got := notes.Name(); got != "notes" {
		t.Errorf("List.Name() = %q", got)
	}
	if got := cells.Name(); got != "cells" {
		t.Errorf("Map.Name() = %q", got)
	}
	if got, want := body.Part(), (crdt.Part{Kind: crdt.PartText, Name: "file:a.tex"}); got != want {
		t.Errorf("Text.Part() = %v, want %v", got, want)
	}
	if got, want := notes.Part(), (crdt.Part{Kind: crdt.PartList, Name: "notes"}); got != want {
		t.Errorf("List.Part() = %v, want %v", got, want)
	}
	if got, want := cells.Part(), (crdt.Part{Kind: crdt.PartMap, Name: "cells"}); got != want {
		t.Errorf("Map.Part() = %v, want %v", got, want)
	}

	// A list is written at a position as well as appended to.
	if err := notes.Append([]byte("un"), []byte("trois")); err != nil {
		t.Fatal(err)
	}
	if err := notes.Insert(1, []byte("deux")); err != nil {
		t.Fatal(err)
	}
	graceNotes, err := grace.List("notes")
	if err != nil {
		t.Fatal(err)
	}
	await(t, grace, "the notes to arrive", func() bool { return graceNotes.Len() == 3 })
	for i, want := range []string{"un", "deux", "trois"} {
		got, err := graceNotes.Get(i)
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if string(got) != want {
			t.Errorf("note %d is %q, want %q", i, got, want)
		}
	}
	if _, err := graceNotes.Get(3); !errors.Is(err, crdt.ErrOutOfRange) {
		t.Errorf("Get past the end = %v, want ErrOutOfRange", err)
	}
	if err := notes.Delete(1, 1); err != nil {
		t.Fatal(err)
	}
	await(t, grace, "the note to go", func() bool { return graceNotes.Len() == 2 })
	if got := graceNotes.Values(); len(got) != 2 || string(got[0]) != "un" || string(got[1]) != "trois" {
		t.Errorf("the notes are %q", got)
	}

	// A map is read by key and by the set of them, and a key is removed.
	for key, value := range map[string]string{"A1": "1", "B2": "2"} {
		if err := cells.Set(key, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	graceCells, err := grace.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	await(t, grace, "the cells to arrive", func() bool { return graceCells.Len() == 2 })
	if got, want := graceCells.Keys(), []string{"A1", "B2"}; len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("Keys() = %q, want %q", got, want)
	}
	if err := cells.Delete("A1"); err != nil {
		t.Fatal(err)
	}
	await(t, grace, "the cell to go", func() bool { return graceCells.Len() == 1 })
	if _, ok := graceCells.Get("A1"); ok {
		t.Error("a deleted key is still present")
	}
	if got, ok := graceCells.Get("B2"); !ok || !bytes.Equal(got, []byte("2")) {
		t.Errorf("B2 holds %q", got)
	}

	// A text handle reports what a browser would count, which is not what a Go
	// program counts as soon as anything is outside the basic plane.
	if err := body.Insert(0, "a𝔄b"); err != nil {
		t.Fatal(err)
	}
	if got, want := body.Len(), 3; got != want {
		t.Errorf("Len() = %d, want %d", got, want)
	}
	if got, want := body.LenUTF16(), 4; got != want {
		t.Errorf("LenUTF16() = %d, want %d", got, want)
	}

	// Nothing about the document was disturbed by any of it: the two replicas
	// hold the same bytes. Waited on grace, because the server does not send a
	// participant its own edits back and every edit above was ada's — so ada has
	// nothing arriving to wake it.
	await(t, grace, "the replicas to agree", func() bool {
		return bytes.Equal(ada.Snapshot(), grace.Snapshot())
	})
}

// A key or a value a part could not hold is refused where the caller asked for
// it, and the session carries on.
func TestAHandleRefusesWhatThePartWouldRefuse(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	c := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})

	cells, err := c.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	if err := cells.Set("\xff", []byte("x")); !errors.Is(err, crdt.ErrInvalidText) {
		t.Fatalf("Set with an invalid key = %v, want ErrInvalidText", err)
	}
	if err := cells.Delete("\xff"); !errors.Is(err, crdt.ErrInvalidText) {
		t.Fatalf("Delete with an invalid key = %v, want ErrInvalidText", err)
	}
	notes, err := c.List("notes")
	if err != nil {
		t.Fatal(err)
	}
	if err := notes.Append([]byte{}); !errors.Is(err, crdt.ErrEmptyValue) {
		t.Fatalf("Append of an empty value = %v, want ErrEmptyValue", err)
	}
	if err := notes.Insert(4, []byte("x")); !errors.Is(err, crdt.ErrOutOfRange) {
		t.Fatalf("Insert past the end = %v, want ErrOutOfRange", err)
	}
	if err := notes.Delete(0, 1); !errors.Is(err, crdt.ErrOutOfRange) {
		t.Fatalf("Delete past the end = %v, want ErrOutOfRange", err)
	}

	// A refused edit sends nothing and leaves nothing behind, so the document
	// still has no parts at all.
	if got := c.Parts(); len(got) != 0 {
		t.Fatalf("refused edits created %v", got)
	}
	if err := c.Err(); err != nil {
		t.Fatalf("Err() = %v, want the session unharmed", err)
	}
}
