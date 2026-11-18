//go:build !js

package collab

import (
	"context"
	"errors"
	"testing"

	"github.com/go-crdt/crdt"
	"github.com/go-crdt/crdt/structured"
)

// undoSession joins n participants (sites 1..n) to one document over Pipe, each
// with undo enabled on the "body" part, and returns their handles.
func undoSession(t *testing.T, n int) []*Text {
	t.Helper()
	srv := NewServer(Config{})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var hs []*Text
	for i := 1; i <= n; i++ {
		local, server := Pipe()
		go func() { _ = srv.ServePipe(ctx, server) }()
		c, err := Join(t.Context(), local, ClientConfig{Document: "doc", Site: crdt.SiteID(i)})
		if err != nil {
			t.Fatalf("Join(site %d): %v", i, err)
		}
		t.Cleanup(func() { _ = c.Close() })
		h, err := c.Text("body")
		if err != nil {
			t.Fatal(err)
		}
		h.EnableUndo()
		h.EnableUndo() // a second call changes nothing
		hs = append(hs, h)
	}
	return hs
}

func awaitAll(t *testing.T, hs []*Text, want string) {
	t.Helper()
	for _, h := range hs {
		awaitPipe(t, h.c, "the text to be "+want, func() bool { return h.String() == want })
	}
}

// The defect this exists for (go-widgets/toolkit#591): a participant's undo must
// not delete what a peer typed after it. With any number of participants.
func TestUndoNeverTakesAPeersText(t *testing.T) {
	for _, n := range []int{2, 3, 5} {
		hs := undoSession(t, n)
		// Site i types its letter at the end, one after another, each waiting to
		// have seen the one before.
		want := ""
		for i, h := range hs {
			awaitAll(t, hs, want)
			letter := string(rune('a' + i))
			if err := h.Insert(len([]rune(want)), letter); err != nil {
				t.Fatal(err)
			}
			want += letter
		}
		awaitAll(t, hs, want)

		// The first participant undoes: only its own letter goes, on every replica.
		if err := hs[0].Undo(); err != nil {
			t.Fatalf("n=%d: Undo: %v", n, err)
		}
		awaitAll(t, hs, want[1:])

		// And redoes it: the letter comes back, theirs untouched.
		if !hs[0].CanRedo() {
			t.Fatalf("n=%d: nothing to redo after an undo", n)
		}
		if err := hs[0].Redo(); err != nil {
			t.Fatalf("n=%d: Redo: %v", n, err)
		}
		if got := hs[0].String(); len([]rune(got)) != n {
			t.Fatalf("n=%d: after redo %q, want %d letters", n, got, n)
		}
		for i := 1; i < n; i++ {
			if err := hs[i].Undo(); err != nil {
				t.Fatalf("n=%d: site %d Undo: %v", n, i+1, err)
			}
		}
		awaitAll(t, hs, string(rune('a')))
		if hs[0].CanUndo() != true {
			t.Fatalf("n=%d: the first participant's letter is still its own to undo", n)
		}
	}
}

// A group is undone as one, and the UTF-16 entry points are recorded too.
func TestUndoGroupsAndUTF16(t *testing.T) {
	hs := undoSession(t, 2)
	a, b := hs[0], hs[1]

	a.Begin()
	a.Begin() // inside a group: nothing
	if err := a.Insert(0, "one "); err != nil {
		t.Fatal(err)
	}
	if !a.CanUndo() {
		t.Fatal("the open group holds an edit, which Undo would take back")
	}
	if err := a.InsertUTF16(4, "two"); err != nil {
		t.Fatal(err)
	}
	a.Commit()
	a.Commit() // none open: nothing
	awaitAll(t, hs, "one two")
	if err := b.Insert(7, "!"); err != nil {
		t.Fatal(err)
	}
	awaitAll(t, hs, "one two!")

	if err := a.DeleteUTF16(0, 4); err != nil {
		t.Fatal(err)
	}
	awaitAll(t, hs, "two!")
	if err := a.Undo(); err != nil { // puts "one " back
		t.Fatal(err)
	}
	awaitAll(t, hs, "one two!")
	if err := a.Undo(); err != nil { // the whole group, not a half
		t.Fatal(err)
	}
	awaitAll(t, hs, "!")
	if a.CanUndo() {
		t.Fatal("the group is undone and nothing else was typed")
	}
	if err := a.Undo(); !errors.Is(err, ErrNothingToUndo) || !errors.Is(err, structured.ErrNoChange) {
		t.Fatalf("Undo with nothing = %v, want ErrNothingToUndo", err)
	}
	if err := a.Redo(); err != nil {
		t.Fatal(err)
	}
	awaitAll(t, hs, "one two!")

	// Bad offsets are refused as the plain entry points refuse them.
	if err := a.DeleteUTF16(0, -1); !errors.Is(err, crdt.ErrOutOfRange) {
		t.Fatalf("DeleteUTF16(0,-1) = %v", err)
	}
	if err := a.DeleteUTF16(-1, 1); err == nil {
		t.Fatal("DeleteUTF16(-1,1) accepted")
	}
	if err := a.DeleteUTF16(0, 99); err == nil {
		t.Fatal("DeleteUTF16(0,99) accepted")
	}
	if err := a.InsertUTF16(99, "x"); err == nil {
		t.Fatal("InsertUTF16(99) accepted")
	}
}

// Without EnableUndo nothing is recorded and every undo call says so.
func TestUndoIsOffUntilAskedFor(t *testing.T) {
	srv := NewServer(Config{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	local, server := Pipe()
	go func() { _ = srv.ServePipe(ctx, server) }()
	c, err := Join(t.Context(), local, ClientConfig{Document: "doc", Site: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	h, _ := c.Text("body")
	if err := h.Insert(0, "x"); err != nil {
		t.Fatal(err)
	}
	if err := h.DeleteUTF16(0, 1); err != nil {
		t.Fatal(err)
	}
	h.Begin()
	h.Commit()
	if h.CanUndo() || h.CanRedo() {
		t.Fatal("history recorded without being asked")
	}
	if err := h.Undo(); !errors.Is(err, ErrNothingToUndo) {
		t.Fatalf("Undo = %v", err)
	}
	if err := h.Redo(); !errors.Is(err, ErrNothingToUndo) {
		t.Fatalf("Redo = %v", err)
	}
}

// A reconnection replaces the replica; the history named characters of the one
// that is gone and starts over rather than acting on a stale document.
func TestUndoHistoryStartsOverOnANewReplica(t *testing.T) {
	hs := undoSession(t, 1)
	h := hs[0]
	if err := h.Insert(0, "x"); err != nil {
		t.Fatal(err)
	}
	if !h.CanUndo() {
		t.Fatal("nothing to undo")
	}
	h.c.mu.Lock()
	fresh, err := crdt.LoadComposite(h.c.doc.Site(), h.c.doc.Snapshot())
	if err != nil {
		h.c.mu.Unlock()
		t.Fatal(err)
	}
	h.c.doc = fresh
	h.c.mu.Unlock()
	if h.CanUndo() {
		t.Fatal("history survived the replica it was recorded on")
	}
	if err := h.Insert(1, "y"); err != nil {
		t.Fatal(err)
	}
	if err := h.Undo(); err != nil {
		t.Fatal(err)
	}
	if got := h.String(); got != "x" {
		t.Fatalf("after undoing y: %q", got)
	}
}
