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
	// hold the same bytes.
	awaitBoth(t, ada, grace, "the replicas to agree", func() bool {
		return bytes.Equal(ada.Snapshot(), grace.Snapshot())
	})
}

// Everything a page is handed is addressed in UTF-16 code units, because a page
// counts in them and cannot be asked to convert. A document with a character
// outside the basic plane is the only one where the two counts differ, so it is
// the only one worth testing on.
func TestTheHandleAnswersInTheUnitsAPageCounts(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	ada := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})
	grace := join(t, conn, collab.ClientConfig{Document: "doc", Site: 2})

	// "a😀b": three characters, four code units, and the "b" is at rune 2 and
	// unit 3.
	if err := body(t, ada).Insert(0, "a😀b"); err != nil {
		t.Fatal(err)
	}
	adaBody := body(t, ada)

	anchor, err := adaBody.AnchorUTF16(3)
	if err != nil {
		t.Fatalf("AnchorUTF16(3): %v", err)
	}
	if runeAnchor, err := adaBody.Anchor(2); err != nil || runeAnchor != anchor {
		t.Fatalf("AnchorUTF16(3) = %v, want Anchor(2) = %v (err %v)", anchor, runeAnchor, err)
	}
	if pos, ok := adaBody.PositionUTF16(anchor); pos != 3 || !ok {
		t.Errorf("PositionUTF16 = (%d, %v), want (3, true)", pos, ok)
	}
	if _, ok := adaBody.PositionUTF16(crdt.ID{Site: 9, Seq: 9}); ok {
		t.Error("PositionUTF16 claims to know an anchor from another document")
	}

	// An offset landing between the emoji's two units names a place no cursor
	// was ever in, and is refused rather than moved to one of its sides.
	if _, err := adaBody.AnchorUTF16(2); !errors.Is(err, crdt.ErrSurrogateBoundary) {
		t.Errorf("AnchorUTF16 inside a character = %v, want ErrSurrogateBoundary", err)
	}
	if _, err := adaBody.AnchorUTF16(9); !errors.Is(err, crdt.ErrOutOfRange) {
		t.Errorf("AnchorUTF16 past the end = %v, want ErrOutOfRange", err)
	}

	// A deleted character still has a position — where the text closed up to —
	// and it is reported in the same units.
	if err := adaBody.DeleteUTF16(3, 1); err != nil {
		t.Fatal(err)
	}
	if pos, ok := adaBody.PositionUTF16(anchor); pos != 3 || !ok {
		t.Errorf("PositionUTF16 of a deleted character = (%d, %v), want (3, true)", pos, ok)
	}
	if adaBody.Visible(anchor) {
		t.Error("Visible says a deleted character is still in the text")
	}

	// Two authors, so the runs are worth splitting: grace writes an emoji of her
	// own in front, which moves everything ada wrote two units along.
	awaitText(t, grace, "a😀")
	if err := body(t, grace).InsertUTF16(0, "🙂"); err != nil {
		t.Fatal(err)
	}
	awaitText(t, ada, "🙂a😀")

	runs := adaBody.AuthorRunsUTF16()
	if len(runs) != 2 {
		t.Fatalf("AuthorRunsUTF16() = %+v, want one run per author", runs)
	}
	if runs[0] != (crdt.AuthorRun{Pos: 0, Len: 2, Site: 2}) {
		t.Errorf("grace's run is %+v, want {Pos:0 Len:2 Site:2}", runs[0])
	}
	if runs[1] != (crdt.AuthorRun{Pos: 2, Len: 3, Site: 1}) {
		t.Errorf("ada's run is %+v, want {Pos:2 Len:3 Site:1}", runs[1])
	}
	// The rune-counted answer is the one that differs, which is the whole point.
	if got := adaBody.AuthorRuns(); got[1].Pos != 1 || got[1].Len != 2 {
		t.Errorf("AuthorRuns() = %+v, want ada's run at rune 1 for 2 runes", got)
	}
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
