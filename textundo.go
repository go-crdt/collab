package collab

import (
	"github.com/go-crdt/crdt"
	"github.com/go-crdt/crdt/structured"
)

// Undo in a shared text is the undo of this participant's own edits, never of
// the document's last change.
//
// An editor's usual undo restores an earlier buffer. Bound to a session that is
// wrong, and it is wrong quietly: between a participant's edit and their undo,
// peers have typed, so the earlier buffer lacks their work, and the editor reads
// the difference as "the participant deleted it" and publishes exactly that. The
// more participants there are, the likelier it is that anything older than the
// last keystroke predates somebody's.
//
// So an undo here is a new edit that takes back what this participant did, by
// the identity of the characters and not their position, and leaves every other
// participant's characters alone. It travels as an ordinary edit, which is why
// a peer needs no code to receive one. The semantics, and what they cannot
// promise, are those of [structured.Undo], which does the work.

// textUndo is one text part's own-edit history, bound to the replica it was
// recorded on.
type textUndo struct {
	doc *crdt.Doc
	u   *structured.Undo
}

// ErrNothingToUndo is what [Text.Undo] and [Text.Redo] return when there is
// nothing to put back or do again, including when [Text.EnableUndo] was never
// called.
var ErrNothingToUndo = structured.ErrNoChange

// EnableUndo starts recording this participant's edits to the part, so that
// [Text.Undo] can take them back. Only edits made through the handles of this
// client after the call are undoable; text that was already there, and what peers
// write, never is. It does nothing if undo is already enabled.
//
// It is off by default because the history keeps the identity of every
// character this participant typed, and a program that never undoes should not
// pay for that. The history is not bounded.
//
// A reconnection that replaces the replica (see [JoinWithRetry]) drops the
// history, since its characters are named on the replica that is gone.
func (t *Text) EnableUndo() {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	if t.c.undo == nil {
		t.c.undo = make(map[string]*textUndo)
	}
	if _, ok := t.c.undo[t.name]; !ok {
		d := t.doc()
		t.c.undo[t.name] = &textUndo{doc: d, u: structured.NewUndo(d)}
	}
}

// tracker returns the part's history, or nil when none was asked for. The
// caller holds the lock. A history recorded on a replica that has since been
// replaced is started over.
func (t *Text) tracker() *structured.Undo {
	h, ok := t.c.undo[t.name]
	if !ok {
		return nil
	}
	if d := t.doc(); h.doc != d {
		h.doc, h.u = d, structured.NewUndo(d)
	}
	return h.u
}

// Begin starts a group: everything this participant types until [Text.Commit]
// is put back by one [Text.Undo], so a word undoes as a word. Only the editor
// knows where a word ends. Begin inside a group does nothing.
func (t *Text) Begin() {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	if u := t.tracker(); u != nil {
		u.Begin()
	}
}

// Commit closes the group opened by [Text.Begin].
func (t *Text) Commit() {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	if u := t.tracker(); u != nil {
		u.Commit()
	}
}

// CanUndo reports whether [Text.Undo] has an edit of this participant's to take
// back.
func (t *Text) CanUndo() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	u := t.tracker()
	return u != nil && u.CanUndo()
}

// CanRedo reports whether [Text.Redo] has an undo to do again.
func (t *Text) CanRedo() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	u := t.tracker()
	return u != nil && u.CanRedo()
}

// Undo takes back this participant's last edit, or its last group, and sends the
// result to everyone. It touches nothing a peer wrote: not text typed around the
// participant's, and not text typed inside what is being taken back. It returns
// [ErrNothingToUndo] when there is nothing to take back.
func (t *Text) Undo() error {
	return t.c.edit(t.Part(), func() (crdt.PartOps, error) {
		u := t.tracker()
		if u == nil {
			return crdt.PartOps{}, ErrNothingToUndo
		}
		ops, err := u.Undo()
		return crdt.PartOps{Text: ops}, err
	})
}

// Redo does again what the last [Text.Undo] took back. A new edit clears what
// Redo was holding. It returns [ErrNothingToUndo] when there is nothing.
func (t *Text) Redo() error {
	return t.c.edit(t.Part(), func() (crdt.PartOps, error) {
		u := t.tracker()
		if u == nil {
			return crdt.PartOps{}, ErrNothingToUndo
		}
		ops, err := u.Redo()
		return crdt.PartOps{Text: ops}, err
	})
}
