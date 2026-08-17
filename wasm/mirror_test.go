package main

import (
	"errors"
	"testing"

	"github.com/go-crdt/crdt"
)

// The mirror is the one piece of the binding that can be tested without a
// browser, and it is the piece most worth testing: everything a page is told
// about a text edit passes through it, and an off-by-one here moves somebody
// else's keystroke to the wrong place in a way nothing downstream could notice.
//
// "a😀b" is the shape every case below is built on: three characters, four code
// units, and a rune offset that stops agreeing with a UTF-16 one at the second
// character.

func TestAMirrorCountsTheUnitsAPageCounts(t *testing.T) {
	m := newMirror("a😀b")
	if got, want := m.lenUTF16(), 4; got != want {
		t.Errorf("lenUTF16() = %d, want %d", got, want)
	}
	if got, want := len(m.runes), 3; got != want {
		t.Errorf("runes = %d, want %d", got, want)
	}
	for pos, want := range map[int]int{0: 0, 1: 1, 2: 3, 3: 4} {
		if got := m.utf16At(pos); got != want {
			t.Errorf("utf16At(%d) = %d, want %d", pos, got, want)
		}
	}
	// Nothing above the basic plane means the two counts are the same number,
	// and the conversion never reads the text at all.
	plain := newMirror("hello")
	if got := plain.utf16At(3); got != 3 {
		t.Errorf("utf16At(3) on plain text = %d, want 3", got)
	}
	if got, want := plain.lenUTF16(), 5; got != want {
		t.Errorf("lenUTF16() of plain text = %d, want %d", got, want)
	}
	if got := newMirror("").String(); got != "" {
		t.Errorf("an empty mirror holds %q", got)
	}
}

func TestAMirrorRefusesAnOffsetInsideACharacter(t *testing.T) {
	m := newMirror("a😀b")
	for pos, want := range map[int]int{0: 0, 1: 1, 3: 2, 4: 3} {
		got, err := m.runeAt(pos)
		if err != nil || got != want {
			t.Errorf("runeAt(%d) = (%d, %v), want (%d, nil)", pos, got, err, want)
		}
	}
	// Offset 2 is the second unit of the emoji: a position no cursor was ever
	// in, and one that has already lost what would be needed to honour it.
	if _, err := m.runeAt(2); !errors.Is(err, crdt.ErrSurrogateBoundary) {
		t.Errorf("runeAt(2) = %v, want ErrSurrogateBoundary", err)
	}
	for _, pos := range []int{-1, 5} {
		if _, err := m.runeAt(pos); !errors.Is(err, crdt.ErrOutOfRange) {
			t.Errorf("runeAt(%d) = %v, want ErrOutOfRange", pos, err)
		}
	}
	// The shortcut for text with nothing above the basic plane still has to
	// refuse what is not a position.
	plain := newMirror("hello")
	if got, err := plain.runeAt(5); err != nil || got != 5 {
		t.Errorf("runeAt at the end of plain text = (%d, %v)", got, err)
	}
	if _, err := plain.runeAt(6); !errors.Is(err, crdt.ErrOutOfRange) {
		t.Errorf("runeAt past the end of plain text = %v, want ErrOutOfRange", err)
	}
}

func TestASpliceReportsWhatTheRemovedCharactersWereWorth(t *testing.T) {
	// A change carries how many characters went, never how many units they were
	// worth. Only the text that held them knows, which is the whole reason this
	// type exists.
	m := newMirror("a😀b")
	at, gone := m.splice(1, 1, "")
	if at != 1 || gone != 2 {
		t.Errorf("removing the emoji reported (%d, %d), want (1, 2)", at, gone)
	}
	if got := m.String(); got != "ab" {
		t.Errorf("the mirror holds %q, want %q", got, "ab")
	}
	if m.sup != 0 || m.lenUTF16() != 2 {
		t.Errorf("after removing the only supplementary character sup = %d, len = %d", m.sup, m.lenUTF16())
	}

	// Inserting one puts the two counts back out of step, at the offset where
	// it landed rather than at the one it was asked for.
	at, gone = m.splice(2, 0, "🙂!")
	if at != 2 || gone != 0 {
		t.Errorf("appending reported (%d, %d), want (2, 0)", at, gone)
	}
	if got, want := m.String(), "ab🙂!"; got != want {
		t.Errorf("the mirror holds %q, want %q", got, want)
	}
	if got, want := m.lenUTF16(), 5; got != want {
		t.Errorf("lenUTF16() = %d, want %d", got, want)
	}
	// And a replacement past it is reported in the units a page would use.
	at, gone = m.splice(3, 1, "?")
	if at != 4 || gone != 1 {
		t.Errorf("replacing after the emoji reported (%d, %d), want (4, 1)", at, gone)
	}
	if got, want := m.String(), "ab🙂?"; got != want {
		t.Errorf("the mirror holds %q, want %q", got, want)
	}
}

func TestAMirrorKnowsWhatItDoesNotHold(t *testing.T) {
	m := newMirror("abc")
	for _, ch := range []crdt.Change{
		{Pos: 0, Removed: 3},
		{Pos: 3, Removed: 0, Text: "d"},
	} {
		if !m.holds(ch) {
			t.Errorf("holds(%+v) = false, want true", ch)
		}
	}
	for _, ch := range []crdt.Change{
		{Pos: -1},
		{Pos: 0, Removed: -1},
		{Pos: 2, Removed: 2},
		{Pos: 4},
	} {
		if m.holds(ch) {
			t.Errorf("holds(%+v) = true, want false", ch)
		}
	}
}
