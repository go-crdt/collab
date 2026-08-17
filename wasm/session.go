//go:build js && wasm

package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall/js"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
	"github.com/go-crdt/crdt/awareness"
)

// openSessions and rebuilds are what collab.stats() reports beside the function
// count. A rebuild means a mirror and the document came apart, which is a bug
// in this package rather than anything a peer can cause, so a test can insist
// the number stays at zero.
var (
	openSessions int
	rebuilds     int
)

// A session is one document, as a page holds it.
//
// Nothing here is locked, and that is not an oversight. A page has one thread;
// Go compiled to WebAssembly runs on it and holds it until it blocks, so
// JavaScript and Go never run at the same time, and a callback the page makes
// from inside another callback runs on the same goroutine — syscall/js says so.
// A mutex would therefore buy nothing and could deadlock the page the first
// time an onChange handler edited from inside itself.
type session struct {
	c      *collab.Client
	cancel context.CancelFunc

	fns    funcSet
	obj    js.Value
	proxy  js.Value
	revoke js.Value

	handles map[crdt.Part]*handle
	mirrors map[string]*mirror

	onChange js.Value
	onPeers  js.Value
	onClose  js.Value

	closing bool
	pumped  chan struct{}
	once    sync.Once
}

// newSession wraps a joined client and hands back what the page holds.
func newSession(c *collab.Client, cancel context.CancelFunc) js.Value {
	s := &session{
		c:        c,
		cancel:   cancel,
		handles:  map[crdt.Part]*handle{},
		onChange: js.Undefined(),
		onPeers:  js.Undefined(),
		onClose:  js.Undefined(),
		pumped:   make(chan struct{}),
	}
	s.baseline()
	s.build()
	openSessions++
	go s.pump()
	return s.proxy
}

// baseline seeds a mirror for every text part the welcome brought, and forgets
// whatever the client has recorded up to now.
//
// Dropping those costs nothing: the page has not been shown anything yet, and
// the first thing it does with a handle is read the whole text out of it. What
// it must not do is start from a text that already includes an edit it is about
// to be told to make.
//
// The loop is there because the receiving goroutine may still be working
// through messages that arrived while the join was in flight. Nothing new can
// join them — a message reaches the client through a WebSocket callback, and
// JavaScript does not run while Go does — so what is queued is finite and the
// loop settles on the first pass in every case anyone will ever see.
func (s *session) baseline() {
	for {
		s.c.TakeChanges()
		s.mirrors = map[string]*mirror{}
		for _, part := range s.c.Parts() {
			if part.Kind != crdt.PartText {
				continue
			}
			// A name the document already holds cannot be refused.
			h, _ := s.c.Text(part.Name)
			s.mirrors[part.Name] = newMirror(h.String())
		}
		if len(s.c.TakeChanges()) == 0 {
			return
		}
	}
}

// build makes the object the page is handed.
func (s *session) build() {
	obj := objectCtor.New()
	obj.Set("document", s.c.Document())
	obj.Set("site", uint64Value(uint64(s.c.Site())))

	s.fns.method(obj, "text", func(a []js.Value) any { return s.handle(crdt.PartText, a) })
	s.fns.method(obj, "list", func(a []js.Value) any { return s.handle(crdt.PartList, a) })
	s.fns.method(obj, "map", func(a []js.Value) any { return s.handle(crdt.PartMap, a) })
	s.fns.method(obj, "parts", func([]js.Value) any { return s.partsValue() })
	s.fns.method(obj, "peers", func([]js.Value) any { return s.peersValue() })
	s.fns.method(obj, "snapshot", func([]js.Value) any { return fromBytes(s.c.Snapshot()) })
	s.fns.method(obj, "isClosed", func([]js.Value) any { return s.c.Err() != nil })
	s.fns.method(obj, "setCursor", s.setCursor)
	s.fns.method(obj, "onChange", func(a []js.Value) any { return s.watch(&s.onChange, a) })
	s.fns.method(obj, "onPeers", func(a []js.Value) any { return s.watch(&s.onPeers, a) })
	s.fns.method(obj, "onClose", func(a []js.Value) any { return s.watch(&s.onClose, a) })
	s.fns.method(obj, "close", func([]js.Value) any { return s.close() })

	s.obj = obj
	s.proxy, s.revoke = revocable(obj)
}

// handle finds or makes the handle on one part. The same name asked for twice
// gives back the same object, so a page can compare handles and so that asking
// again costs nothing.
func (s *session) handle(kind crdt.PartKind, args []js.Value) any {
	return settle(func() (any, error) {
		name, err := argString(args, 0, "a part name")
		if err != nil {
			return nil, err
		}
		part := crdt.Part{Kind: kind, Name: name}
		if h := s.handles[part]; h != nil {
			return h.proxy, nil
		}
		h, err := newHandle(s, part)
		if err != nil {
			return nil, err
		}
		s.handles[part] = h
		return h.proxy, nil
	})
}

func (s *session) partsValue() any {
	parts := s.c.Parts()
	out := make([]any, len(parts))
	for i, part := range parts {
		out[i] = map[string]any{"kind": kindName(part.Kind), "name": part.Name}
	}
	return out
}

func (s *session) peersValue() any {
	peers := s.c.Peers()
	out := make([]any, len(peers))
	for i, peer := range peers {
		meta := map[string]any{}
		for k, v := range peer.Meta {
			meta[k] = v
		}
		out[i] = map[string]any{
			"site":   uint64Value(uint64(peer.Site)),
			"cursor": map[string]any{"anchor": peer.Cursor.Anchor, "head": peer.Cursor.Head},
			"meta":   meta,
		}
	}
	return out
}

func (s *session) setCursor(args []js.Value) any {
	return settle(func() (any, error) {
		cursor := arg(args, 0)
		if cursor.Type() != js.TypeObject {
			return nil, errors.New("collab: setCursor needs {anchor, head}")
		}
		anchor, err := valueInt(cursor.Get("anchor"), "a cursor's anchor")
		if err != nil {
			return nil, err
		}
		head, err := valueInt(cursor.Get("head"), "a cursor's head")
		if err != nil {
			return nil, err
		}
		meta, err := parseMeta(arg(args, 1))
		if err != nil {
			return nil, err
		}
		if err := s.c.SetCursor(awareness.Cursor{Anchor: anchor, Head: head}, meta); err != nil {
			return nil, err
		}
		return js.Undefined(), nil
	})
}

// watch sets one of the page's handlers, or clears it when given null.
//
// Registering is where a view's account of the document begins: whatever the
// session had recorded before is dropped, because the page is about to read the
// text out and start from that. The two can be done in one breath — register,
// then read — and nothing can slip between them, since a change only arrives
// through a WebSocket callback and JavaScript does not run in the middle of its
// own statement.
func (s *session) watch(slot *js.Value, args []js.Value) any {
	return settle(func() (any, error) {
		v := arg(args, 0)
		switch {
		case v.Type() == js.TypeFunction:
			s.c.TakeChanges()
			*slot = v
		case v.IsNull() || v.IsUndefined():
			*slot = js.Undefined()
		default:
			return nil, errors.New("collab: a handler must be a function, or null to clear it")
		}
		return js.Undefined(), nil
	})
}

// pump is the one goroutine that reads the session, for the life of it.
func (s *session) pump() {
	defer close(s.pumped)
	for {
		select {
		case <-s.c.Changes():
			s.deliver()
		case <-s.c.Done():
			// Whatever had already arrived is handed over before the end is
			// announced, so nothing is lost to the race between the two.
			s.deliver()
			s.fire(s.onClose, s.reason())
			return
		}
	}
}

// reason says why the session ended: nothing, when the page ended it, and an
// Error when anything else did.
func (s *session) reason() any {
	if s.closing {
		return js.Null()
	}
	err := s.c.Err()
	if err == nil {
		return js.Null()
	}
	return jsError(err)
}

// deliver converts what changed and tells the page.
func (s *session) deliver() {
	if changes := s.c.TakeChanges(); len(changes) > 0 {
		// Every mirror is brought up to date before the page is told anything,
		// so a handler that edits from inside the callback finds them whole.
		s.fire(s.onChange, s.convert(changes))
	}
	// Operations and presence share one wake-up, so a page watching peers hears
	// about every change to the document as well. Which of the two moved is not
	// something the client distinguishes, and drawing a cursor is cheap.
	s.fire(s.onPeers, s.peersValue())
}

// fire calls one of the page's handlers and survives it throwing.
//
// A handler that throws is the page's bug and must not take the session down
// with it: js.Value.Invoke turns a JavaScript exception into a Go panic, which
// would end this program and with it every other document the page has open.
func (s *session) fire(fn js.Value, args ...any) {
	if !fn.Truthy() {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(js.Error); ok {
				console.Call("error", "collab: a handler threw", e.Value)
				return
			}
			console.Call("error", "collab: a handler failed", fmt.Sprint(r))
		}
	}()
	fn.Invoke(args...)
}

// convert renders what TakeChanges returned as what a page can act on: which
// part moved, and for a text part the edits in the order a view has to make
// them, addressed in the units it counts.
func (s *session) convert(changes []crdt.PartChange) any {
	out := make([]any, len(changes))
	for i, change := range changes {
		part := map[string]any{
			"kind": kindName(change.Part.Kind),
			"name": change.Part.Name,
		}
		switch change.Part.Kind {
		case crdt.PartText:
			part["text"] = s.textEdits(change.Part.Name, change.Text)
		case crdt.PartMap:
			part["keys"] = stringValues(change.Keys)
		}
		// A list says that it moved and nothing more. That is deliberate and
		// crdt/docs/design.md says why: a list holds tens or hundreds of values,
		// the views written against one read it back whole, and naming positions
		// would be a second protocol to keep correct for nobody.
		out[i] = part
	}
	return out
}

func (s *session) textEdits(name string, changes []crdt.Change) []any {
	m := s.mirror(name)
	out := make([]any, 0, len(changes))
	for _, change := range changes {
		if !m.holds(change) {
			// The mirror and the document have come apart. Telling the view to
			// replace the part outright is a repair rather than a report, and it
			// is right whatever was converted before: what it is being given
			// already includes every edit of this batch.
			return []any{s.rebuild(name)}
		}
		at, gone := m.splice(change.Pos, change.Removed, change.Text)
		out = append(out, map[string]any{"pos": at, "removed": gone, "insert": change.Text})
	}
	return out
}

// mirror is the text of one part as the page last saw it. A part first heard of
// in a change starts empty, which is exactly right: a part exists because
// somebody wrote to it, and that write is the change being reported.
func (s *session) mirror(name string) *mirror {
	m := s.mirrors[name]
	if m == nil {
		m = newMirror("")
		s.mirrors[name] = m
	}
	return m
}

// rebuild throws a mirror away and starts it again from the document, and says
// so — loudly, and in the numbers stats() reports, because it means this package
// has a bug rather than that anything went wrong on the wire.
func (s *session) rebuild(name string) any {
	rebuilds++
	was := s.mirror(name).lenUTF16()
	h, _ := s.c.Text(name) // a name the document holds cannot be refused
	now := h.String()
	s.mirrors[name] = newMirror(now)
	console.Call("error", "collab: the mirror of "+name+" was rebuilt")
	return map[string]any{"pos": 0, "removed": was, "insert": now}
}

// close ends the session and gives everything back.
func (s *session) close() any {
	s.closing = true
	return async(func() (any, error) {
		// Close waits for the session's own goroutine to stop, and a wait cannot
		// happen inside a callback: a js.Func that blocks blocks the page.
		_ = s.c.Close()
		<-s.pumped
		s.shutdown()
		return js.Undefined(), nil
	})
}

// shutdown revokes what the page holds and releases what JavaScript holds, in
// that order, so that nothing can reach a released function.
func (s *session) shutdown() {
	s.once.Do(func() {
		for _, h := range s.handles {
			h.shutdown()
		}
		s.handles = nil
		s.mirrors = nil
		s.onChange, s.onPeers, s.onClose = js.Undefined(), js.Undefined(), js.Undefined()
		s.revoke.Invoke()
		s.fns.release()
		s.cancel()
		openSessions--
	})
}

func kindName(kind crdt.PartKind) string { return kind.String() }
