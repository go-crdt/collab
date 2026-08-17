package main

import (
	"slices"

	"github.com/go-crdt/crdt"
)

// A [crdt.Change] is addressed in runes, and against the text as it stood after
// the changes before it. crdt's own documentation is explicit about what that
// costs anyone converting: "converting one of them against the finished
// document is right only for the last. A caller applying changes to its own copy
// holds that intermediate text and has to convert there, against the copy it is
// patching."
//
// A page counts UTF-16 code units, so somebody has to hold that intermediate
// text. A mirror is it: the text of one part as the page last saw it, kept in
// step by the very edits the page is told to make — and by the local ones,
// which the client does not report because the caller that made them already
// knows.
//
// It also answers the one thing a change does not carry. The change says how
// many characters were removed; only the text that held them says how many code
// units they were.
//
// A document holding nothing above the basic plane pays nothing for any of
// this: sup is zero and every conversion returns its argument untouched, which
// is the same shortcut [crdt.Doc] itself takes.
type mirror struct {
	runes []rune
	sup   int // how many of them cost two UTF-16 code units rather than one
}

// newMirror mirrors text as it stands.
func newMirror(text string) *mirror {
	m := &mirror{runes: []rune(text)}
	for _, r := range m.runes {
		if r > 0xFFFF {
			m.sup++
		}
	}
	return m
}

// lenUTF16 is what JavaScript's String.prototype.length would report.
func (m *mirror) lenUTF16() int { return len(m.runes) + m.sup }

// utf16At converts a rune offset into the UTF-16 offset naming the same place.
// pos must be within the text, or exactly at its end.
func (m *mirror) utf16At(pos int) int {
	if m.sup == 0 {
		return pos
	}
	at := pos
	for _, r := range m.runes[:pos] {
		if r > 0xFFFF {
			at++
		}
	}
	return at
}

// runeAt converts a UTF-16 offset into the rune offset naming the same place.
//
// An offset falling between the two units of one character is refused with
// [crdt.ErrSurrogateBoundary] rather than rounded, which is the rule the whole
// boundary holds to: half of an emoji is not a place a cursor has ever been, and
// rounding would move an edit somewhere nobody asked for.
func (m *mirror) runeAt(pos int) (int, error) {
	if pos < 0 || pos > m.lenUTF16() {
		return 0, crdt.ErrOutOfRange
	}
	if m.sup == 0 {
		return pos, nil
	}
	left := pos
	for i, r := range m.runes {
		if left == 0 {
			return i, nil
		}
		width := 1
		if r > 0xFFFF {
			width = 2
		}
		if left < width {
			return 0, crdt.ErrSurrogateBoundary
		}
		left -= width
	}
	return len(m.runes), nil
}

// splice applies one edit expressed in runes and reports it in code units:
// where it landed, and what the characters it removed were worth.
func (m *mirror) splice(pos, removed int, insert string) (at, gone int) {
	at = m.utf16At(pos)
	gone = removed
	for _, r := range m.runes[pos : pos+removed] {
		if r > 0xFFFF {
			gone++
			m.sup--
		}
	}
	added := []rune(insert)
	for _, r := range added {
		if r > 0xFFFF {
			m.sup++
		}
	}
	m.runes = slices.Replace(m.runes, pos, pos+removed, added...)
	return at, gone
}

// holds reports whether a change names a stretch this mirror actually has. It
// is a guard against a bug here rather than against anything the wire can do:
// the document accepted the operation, so the offsets are right for the text the
// document holds, and they are wrong here only if the mirror has drifted.
func (m *mirror) holds(ch crdt.Change) bool {
	return ch.Pos >= 0 && ch.Removed >= 0 && ch.Pos+ch.Removed <= len(m.runes)
}

// String is the text as the mirror holds it.
func (m *mirror) String() string { return string(m.runes) }
