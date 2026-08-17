//go:build js && wasm

package main

import (
	"errors"
	"syscall/js"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// A handle is one part of the document, as the page holds it — the same thing
// [collab.Text], [collab.List] and [collab.Map] are, and for the same reason.
// A page given the replicated structure could edit it, and the operations that
// edit produced would never be sent: the page's own screen would look right
// while it drifted away from everybody else.
type handle struct {
	s      *session
	part   crdt.Part
	fns    funcSet
	proxy  js.Value
	revoke js.Value

	text *collab.Text
	list *collab.List
	dict *collab.Map
}

func newHandle(s *session, part crdt.Part) (*handle, error) {
	h := &handle{s: s, part: part}
	obj := objectCtor.New()
	obj.Set("kind", kindName(part.Kind))
	obj.Set("name", part.Name)

	var err error
	switch part.Kind {
	case crdt.PartText:
		if h.text, err = s.c.Text(part.Name); err != nil {
			return nil, err
		}
		h.buildText(obj)
	case crdt.PartList:
		if h.list, err = s.c.List(part.Name); err != nil {
			return nil, err
		}
		h.buildList(obj)
	default:
		if h.dict, err = s.c.Map(part.Name); err != nil {
			return nil, err
		}
		h.buildMap(obj)
	}
	h.proxy, h.revoke = revocable(obj)
	return h, nil
}

func (h *handle) shutdown() {
	h.revoke.Invoke()
	h.fns.release()
}

// --- text ---------------------------------------------------------------

func (h *handle) buildText(obj js.Value) {
	h.fns.getter(obj, "length", func() any { return h.text.LenUTF16() })
	h.fns.method(obj, "toString", func([]js.Value) any { return h.text.String() })
	h.fns.method(obj, "insert", h.insert)
	h.fns.method(obj, "delete", h.deleteText)
	h.fns.method(obj, "anchor", h.anchor)
	h.fns.method(obj, "position", h.position)
	h.fns.method(obj, "visible", h.visible)
	h.fns.method(obj, "authorRuns", func([]js.Value) any { return h.authorRuns() })
}

func (h *handle) insert(args []js.Value) any {
	return settle(func() (any, error) {
		pos, err := argInt(args, 0, "an offset")
		if err != nil {
			return nil, err
		}
		text, err := argString(args, 1, "the text to insert")
		if err != nil {
			return nil, err
		}
		if err := h.text.InsertUTF16(pos, text); err != nil {
			return nil, err
		}
		h.mirrored(pos, 0, text)
		return js.Undefined(), nil
	})
}

func (h *handle) deleteText(args []js.Value) any {
	return settle(func() (any, error) {
		pos, err := argInt(args, 0, "an offset")
		if err != nil {
			return nil, err
		}
		length, err := argInt(args, 1, "a length")
		if err != nil {
			return nil, err
		}
		if err := h.text.DeleteUTF16(pos, length); err != nil {
			return nil, err
		}
		h.mirrored(pos, length, "")
		return js.Undefined(), nil
	})
}

// mirrored keeps the mirror in step with an edit this page just made.
//
// It has to be done here because a local edit is the one change the client does
// not report — a caller that made it already knows — and a mirror one edit
// behind converts the next remote change against the wrong text.
//
// The document has already accepted the edit, so the offsets convert. If they
// somehow do not, or if the two lengths disagree afterwards, the mirror is
// rebuilt rather than left quietly wrong.
func (h *handle) mirrored(pos, length int, insert string) {
	m := h.s.mirror(h.part.Name)
	from, errFrom := m.runeAt(pos)
	to, errTo := m.runeAt(pos + length)
	if errFrom == nil && errTo == nil {
		m.splice(from, to-from, insert)
	}
	if m.lenUTF16() != h.text.LenUTF16() {
		h.s.rebuild(h.part.Name)
	}
}

func (h *handle) anchor(args []js.Value) any {
	return settle(func() (any, error) {
		pos, err := argInt(args, 0, "an offset")
		if err != nil {
			return nil, err
		}
		id, err := h.text.AnchorUTF16(pos)
		if err != nil {
			return nil, err
		}
		return anchorValue(id), nil
	})
}

// position answers where an anchor's character sits now — or where it was, if
// it has gone. It answers with undefined for an anchor this replica has never
// seen, which is not a failure: it means the operations that would explain it
// have not arrived, or the anchor came from another document.
func (h *handle) position(args []js.Value) any {
	return settle(func() (any, error) {
		id, err := parseAnchor(arg(args, 0))
		if err != nil {
			return nil, err
		}
		pos, ok := h.text.PositionUTF16(id)
		if !ok {
			return js.Undefined(), nil
		}
		return pos, nil
	})
}

func (h *handle) visible(args []js.Value) any {
	return settle(func() (any, error) {
		id, err := parseAnchor(arg(args, 0))
		if err != nil {
			return nil, err
		}
		return h.text.Visible(id), nil
	})
}

func (h *handle) authorRuns() any {
	runs := h.text.AuthorRunsUTF16()
	out := make([]any, len(runs))
	for i, run := range runs {
		out[i] = map[string]any{
			"pos":  run.Pos,
			"len":  run.Len,
			"site": uint64Value(uint64(run.Site)),
		}
	}
	return out
}

// --- list ---------------------------------------------------------------

func (h *handle) buildList(obj js.Value) {
	h.fns.getter(obj, "length", func() any { return h.list.Len() })
	h.fns.method(obj, "values", func([]js.Value) any { return h.values() })
	h.fns.method(obj, "get", h.listGet)
	h.fns.method(obj, "insert", h.listInsert)
	h.fns.method(obj, "append", h.listAppend)
	h.fns.method(obj, "delete", h.listDelete)
}

func (h *handle) values() any {
	values := h.list.Values()
	out := make([]any, len(values))
	for i, v := range values {
		out[i] = fromBytes(v)
	}
	return out
}

func (h *handle) listGet(args []js.Value) any {
	return settle(func() (any, error) {
		pos, err := argInt(args, 0, "an index")
		if err != nil {
			return nil, err
		}
		value, err := h.list.Get(pos)
		if err != nil {
			return nil, err
		}
		return fromBytes(value), nil
	})
}

func (h *handle) listInsert(args []js.Value) any {
	return settle(func() (any, error) {
		pos, err := argInt(args, 0, "an index")
		if err != nil {
			return nil, err
		}
		// argInt has already refused an empty call, so there is a first argument
		// to step over here.
		values, err := bytesFrom(args[1:])
		if err != nil {
			return nil, err
		}
		return js.Undefined(), h.list.Insert(pos, values...)
	})
}

func (h *handle) listAppend(args []js.Value) any {
	return settle(func() (any, error) {
		values, err := bytesFrom(args)
		if err != nil {
			return nil, err
		}
		return js.Undefined(), h.list.Append(values...)
	})
}

func (h *handle) listDelete(args []js.Value) any {
	return settle(func() (any, error) {
		pos, err := argInt(args, 0, "an index")
		if err != nil {
			return nil, err
		}
		count, err := argInt(args, 1, "a count")
		if err != nil {
			return nil, err
		}
		return js.Undefined(), h.list.Delete(pos, count)
	})
}

// bytesFrom reads a run of values, every one of which must be a Uint8Array.
func bytesFrom(args []js.Value) ([][]byte, error) {
	if len(args) == 0 {
		return nil, errors.New("collab: a list needs at least one value")
	}
	values := make([][]byte, len(args))
	for i, v := range args {
		b, err := toBytes(v)
		if err != nil {
			return nil, err
		}
		values[i] = b
	}
	return values, nil
}

// --- map ----------------------------------------------------------------

func (h *handle) buildMap(obj js.Value) {
	h.fns.getter(obj, "size", func() any { return h.dict.Len() })
	h.fns.method(obj, "keys", func([]js.Value) any { return stringValues(h.dict.Keys()) })
	h.fns.method(obj, "get", h.mapGet)
	h.fns.method(obj, "has", h.mapHas)
	h.fns.method(obj, "set", h.mapSet)
	h.fns.method(obj, "delete", h.mapDelete)
}

// mapGet answers with undefined for a key that is not there. That is an answer
// rather than a failure: a page asking about a cell nobody has filled in is not
// a page that has gone wrong.
func (h *handle) mapGet(args []js.Value) any {
	return settle(func() (any, error) {
		key, err := argString(args, 0, "a key")
		if err != nil {
			return nil, err
		}
		value, ok := h.dict.Get(key)
		if !ok {
			return js.Undefined(), nil
		}
		return fromBytes(value), nil
	})
}

func (h *handle) mapHas(args []js.Value) any {
	return settle(func() (any, error) {
		key, err := argString(args, 0, "a key")
		if err != nil {
			return nil, err
		}
		_, ok := h.dict.Get(key)
		return ok, nil
	})
}

func (h *handle) mapSet(args []js.Value) any {
	return settle(func() (any, error) {
		key, err := argString(args, 0, "a key")
		if err != nil {
			return nil, err
		}
		value, err := argBytes(args, 1)
		if err != nil {
			return nil, err
		}
		return js.Undefined(), h.dict.Set(key, value)
	})
}

func (h *handle) mapDelete(args []js.Value) any {
	return settle(func() (any, error) {
		key, err := argString(args, 0, "a key")
		if err != nil {
			return nil, err
		}
		return js.Undefined(), h.dict.Delete(key)
	})
}
