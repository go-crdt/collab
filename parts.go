package collab

import "github.com/go-crdt/crdt"

// A document holds named parts, and a caller reaches for one by name: the text
// of a file, the comments on it, the cells of a sheet. What comes back is a
// handle, not the replicated structure itself.
//
// That is the whole of the design decision here, and it is forced. Handing over
// the [crdt.Doc] would let a caller edit it, and the client would never see the
// operations that edit produced — nothing would be sent, and the participant
// would drift away from everyone else while its own screen looked right. A
// handle edits and publishes in one step, so there is no way to make a change
// nobody hears about.
//
// A handle holds a name rather than a structure, and looks the part up under the
// client's lock on every call. So it stays valid across a reconnection, which
// replaces the whole replica underneath it.

// A Text is a handle on one text part: the buffer of a file, and what an editor
// binds to.
type Text struct {
	c    *Client
	name string
}

// A List is a handle on one list part — comments, a change log, the messages
// beside a document.
type List struct {
	c    *Client
	name string
}

// A Map is a handle on one map part, such as the cells of a sheet.
type Map struct {
	c    *Client
	name string
}

// Text returns a handle on the text part with this name, which is created the
// first time anybody writes to it. The name is arbitrary UTF-8 and is expected
// to carry structure — "file:src/main.tex". An empty or invalid name is refused;
// see [crdt.Part].
func (c *Client) Text(name string) (*Text, error) {
	if err := c.checkPart(crdt.PartText, name); err != nil {
		return nil, err
	}
	return &Text{c: c, name: name}, nil
}

// List returns a handle on the list part with this name.
func (c *Client) List(name string) (*List, error) {
	if err := c.checkPart(crdt.PartList, name); err != nil {
		return nil, err
	}
	return &List{c: c, name: name}, nil
}

// Map returns a handle on the map part with this name.
func (c *Client) Map(name string) (*Map, error) {
	if err := c.checkPart(crdt.PartMap, name); err != nil {
		return nil, err
	}
	return &Map{c: c, name: name}, nil
}

// Parts returns the parts this replica holds, in the canonical order. A part
// that has never been written to is not among them.
func (c *Client) Parts() []crdt.Part {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.doc.Parts()
}

// checkPart reports whether a name can be a part of this kind, by asking the
// document rather than by repeating its rules here.
func (c *Client) checkPart(kind crdt.PartKind, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var err error
	switch kind {
	case crdt.PartText:
		_, err = c.doc.Text(name)
	case crdt.PartList:
		_, err = c.doc.List(name)
	default:
		_, err = c.doc.Map(name)
	}
	return err
}

// edit runs one edit on a part and sends whatever it produced. The lock is
// released before sending, because a send can block on a slow connection and
// nothing else should wait behind it.
func (c *Client) edit(part crdt.Part, run func() (crdt.PartOps, error)) error {
	c.mu.Lock()
	batch, err := run()
	c.mu.Unlock()
	if err != nil {
		return err
	}
	batch.Part = part
	if batch.Text == nil && batch.List == nil && batch.Map == nil {
		return nil
	}
	return c.publish([]crdt.PartOps{batch})
}

// Name returns the part's name.
func (t *Text) Name() string { return t.name }

// Part names this handle's part, which is what a [crdt.PartChange] from
// [Client.TakeChanges] carries.
func (t *Text) Part() crdt.Part { return crdt.Part{Kind: crdt.PartText, Name: t.name} }

// String returns the text as it stands here.
func (t *Text) String() string {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	return t.doc().String()
}

// Len returns the number of characters, counted in runes.
func (t *Text) Len() int {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	return t.doc().Len()
}

// LenUTF16 returns the length a browser would report, counting UTF-16 code
// units. Its companions InsertUTF16 and DeleteUTF16 take offsets in the same
// units, so a caller in the browser never converts by hand; see [crdt.Doc].
func (t *Text) LenUTF16() int {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	return t.doc().LenUTF16()
}

// Insert adds text at rune offset pos, locally and then everywhere.
func (t *Text) Insert(pos int, text string) error {
	return t.c.edit(t.Part(), func() (crdt.PartOps, error) {
		ops, err := t.doc().Insert(pos, text)
		return crdt.PartOps{Text: ops}, err
	})
}

// Delete removes length runes at rune offset pos, locally and then everywhere.
func (t *Text) Delete(pos, length int) error {
	return t.c.edit(t.Part(), func() (crdt.PartOps, error) {
		ops, err := t.doc().Delete(pos, length)
		return crdt.PartOps{Text: ops}, err
	})
}

// InsertUTF16 adds text at an offset counted in UTF-16 code units.
func (t *Text) InsertUTF16(pos int, text string) error {
	return t.c.edit(t.Part(), func() (crdt.PartOps, error) {
		ops, err := t.doc().InsertUTF16(pos, text)
		return crdt.PartOps{Text: ops}, err
	})
}

// DeleteUTF16 removes length code units at an offset counted in the same units.
func (t *Text) DeleteUTF16(pos, length int) error {
	return t.c.edit(t.Part(), func() (crdt.PartOps, error) {
		ops, err := t.doc().DeleteUTF16(pos, length)
		return crdt.PartOps{Text: ops}, err
	})
}

// Anchor returns the identity of the character at rune offset pos, which keeps
// naming that character however the text moves around it. It is what a comment
// or a stored selection should hold; see [crdt.Doc.Anchor].
func (t *Text) Anchor(pos int) (crdt.ID, error) {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	return t.doc().Anchor(pos)
}

// Position returns where the character an anchor names sits now — or where it
// was, if it has been deleted. See [crdt.Doc.Position].
func (t *Text) Position(anchor crdt.ID) (int, bool) {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	return t.doc().Position(anchor)
}

// Visible reports whether the character an anchor names is still in the text.
func (t *Text) Visible(anchor crdt.ID) bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	return t.doc().Visible(anchor)
}

// AnchorUTF16 is [Text.Anchor] with pos counted in UTF-16 code units, the units
// a page counts in. An offset falling between the two units of one character is
// refused rather than rounded; see [crdt.ErrSurrogateBoundary].
func (t *Text) AnchorUTF16(pos int) (crdt.ID, error) {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	d := t.doc()
	at, err := d.RuneOffset(pos)
	if err != nil {
		return crdt.ID{}, err
	}
	return d.Anchor(at)
}

// PositionUTF16 is [Text.Position] with the offset reported in UTF-16 code
// units. ok is false for an anchor this replica has never seen, exactly as it
// is there — which is not the same question as whether the character is still
// in the text; that one is [Text.Visible].
func (t *Text) PositionUTF16(anchor crdt.ID) (pos int, ok bool) {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	d := t.doc()
	at, ok := d.Position(anchor)
	if !ok {
		return 0, false
	}
	// Position answers with an offset the document holds, and every such offset
	// converts, so this cannot fail.
	pos, _ = d.UTF16Offset(at)
	return pos, true
}

// AuthorRuns splits the visible text into stretches by who wrote them, which is
// what colouring a document by author needs.
func (t *Text) AuthorRuns() []crdt.AuthorRun {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	return t.doc().AuthorRuns()
}

// AuthorRunsUTF16 is [Text.AuthorRuns] with every offset and length counted in
// UTF-16 code units, so that a page can colour the string it holds without
// converting anything by hand.
func (t *Text) AuthorRunsUTF16() []crdt.AuthorRun {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	d := t.doc()
	runs := d.AuthorRuns()
	for i, run := range runs {
		// Both ends are offsets the document holds, so neither can fail.
		from, _ := d.UTF16Offset(run.Pos)
		to, _ := d.UTF16Offset(run.Pos + run.Len)
		runs[i].Pos, runs[i].Len = from, to-from
	}
	return runs
}

// doc returns the part. The name was accepted when the handle was made, so this
// cannot fail; the caller holds the lock.
func (t *Text) doc() *crdt.Doc {
	d, _ := t.c.doc.Text(t.name)
	return d
}

// Name returns the part's name.
func (l *List) Name() string { return l.name }

// Part names this handle's part.
func (l *List) Part() crdt.Part { return crdt.Part{Kind: crdt.PartList, Name: l.name} }

// Len returns how many values are present.
func (l *List) Len() int {
	l.c.mu.Lock()
	defer l.c.mu.Unlock()
	return l.list().Len()
}

// Get returns a copy of the value at index pos.
func (l *List) Get(pos int) ([]byte, error) {
	l.c.mu.Lock()
	defer l.c.mu.Unlock()
	return l.list().Get(pos)
}

// Values returns copies of every value present, in order. It is what a view of a
// list reads when it is told the list changed; see [crdt.PartChange].
func (l *List) Values() [][]byte {
	l.c.mu.Lock()
	defer l.c.mu.Unlock()
	return l.list().Values()
}

// Insert adds values at index pos, locally and then everywhere.
func (l *List) Insert(pos int, values ...[]byte) error {
	return l.c.edit(l.Part(), func() (crdt.PartOps, error) {
		ops, err := l.list().Insert(pos, values...)
		return crdt.PartOps{List: ops}, err
	})
}

// Append adds values after the last one, which is what a chat or a log does.
func (l *List) Append(values ...[]byte) error {
	return l.c.edit(l.Part(), func() (crdt.PartOps, error) {
		ops, err := l.list().Insert(l.list().Len(), values...)
		return crdt.PartOps{List: ops}, err
	})
}

// Delete removes count values from index pos.
func (l *List) Delete(pos, count int) error {
	return l.c.edit(l.Part(), func() (crdt.PartOps, error) {
		ops, err := l.list().Delete(pos, count)
		return crdt.PartOps{List: ops}, err
	})
}

func (l *List) list() *crdt.List {
	got, _ := l.c.doc.List(l.name)
	return got
}

// Name returns the part's name.
func (m *Map) Name() string { return m.name }

// Part names this handle's part.
func (m *Map) Part() crdt.Part { return crdt.Part{Kind: crdt.PartMap, Name: m.name} }

// Len returns how many keys are present, not counting deleted ones.
func (m *Map) Len() int {
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	return m.mp().Len()
}

// Keys returns the keys present, ascending.
func (m *Map) Keys() []string {
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	return m.mp().Keys()
}

// Get returns a copy of the value at key, and whether the key is present. It is
// what a view reads for each key a [crdt.PartChange] names.
func (m *Map) Get(key string) ([]byte, bool) {
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	return m.mp().Get(key)
}

// Set stores value at key, locally and then everywhere.
func (m *Map) Set(key string, value []byte) error {
	return m.c.edit(m.Part(), func() (crdt.PartOps, error) {
		op, err := m.mp().Set(key, value)
		if err != nil {
			return crdt.PartOps{}, err
		}
		return crdt.PartOps{Map: []crdt.MapOp{op}}, nil
	})
}

// Delete removes key, locally and then everywhere. It writes a tombstone whether
// or not this replica holds the key; see [crdt.Map].
func (m *Map) Delete(key string) error {
	return m.c.edit(m.Part(), func() (crdt.PartOps, error) {
		op, err := m.mp().Delete(key)
		if err != nil {
			return crdt.PartOps{}, err
		}
		return crdt.PartOps{Map: []crdt.MapOp{op}}, nil
	})
}

func (m *Map) mp() *crdt.Map {
	got, _ := m.c.doc.Map(m.name)
	return got
}

// Edit runs fn against the map part itself and sends whatever operations it
// produced to everyone else.
//
// It is what a structured type built on a map is driven through. A
// [github.com/go-crdt/crdt/structured.Sequence], a Tree or a RecordMap is a
// binding over a [crdt.Map] rather than a thing of its own, and each of its
// methods changes the map here and hands back the operations that change:
//
//	err := m.Edit(func(mp *crdt.Map) ([]crdt.MapOp, error) {
//	    _, ops, err := structured.SequenceOf(mp).Insert(after, value)
//	    return ops, err
//	})
//
// The lock is held for the whole of fn, so what it reads and what it writes are
// one moment. fn must not call back into the client.
func (m *Map) Edit(fn func(*crdt.Map) ([]crdt.MapOp, error)) error {
	return m.c.edit(m.Part(), func() (crdt.PartOps, error) {
		ops, err := fn(m.mp())
		if err != nil {
			return crdt.PartOps{}, err
		}
		return crdt.PartOps{Map: ops}, nil
	})
}

// Read runs fn against the map part for reading. It sends nothing, and
// anything fn writes to the map would be this replica's alone — so do not:
// use [Map.Edit].
//
// The lock is held for the whole of fn, so a view built from several reads sees
// one moment rather than a moment per read.
func (m *Map) Read(fn func(*crdt.Map)) {
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	fn(m.mp())
}
