//go:build js && wasm

package collab

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"syscall/js"
	"time"
)

// The browser side of the BroadcastChannel carrier: a [bus] over the page's own
// BroadcastChannel, and the public API two tabs of one browser use to edit a
// document together. All of the routing, the handshake and the election live in
// bcast.go, against an abstract bus; this file only makes a real
// BroadcastChannel look like one, and is deliberately thin so that what is
// exercised natively there is the whole of the logic.
//
// # The zero-config path
//
// A page that wants "collaborate in this browser" does not know, and should not
// have to know, whether it is the first tab or the second. [HostOrJoin] decides:
// it returns [RoleHost] to the tab that should hold the document and [RoleClient]
// to one joining a tab that already does. The page then serves with
// [Server.ServeBroadcastChannel] or joins with [JoinBroadcastChannel] on the
// same room. There is nothing to configure and nothing for a person to carry
// between the windows — unlike a WebRTC data channel, which needs its connection
// descriptions swapped, a BroadcastChannel is simply there for every tab of the
// origin.

// DefaultElectionWindow is how long [HostOrJoin] listens before concluding no
// tab is already hosting. It is generous next to a same-origin round trip so
// that two tabs opening together reliably see each other's announcements and
// settle the host between them by the rule in [electRole].
const DefaultElectionWindow = 250 * time.Millisecond

// bcBus is a real BroadcastChannel presented as a [bus].
type bcBus struct {
	ch    js.Value
	onmsg js.Func

	mu     sync.Mutex
	cb     func([]byte)
	closed bool
}

// newBCBus opens the BroadcastChannel named room and wires its message event to
// the registered callback.
func newBCBus(room string) (*bcBus, error) {
	ctor := js.Global().Get("BroadcastChannel")
	if !ctor.Truthy() {
		return nil, fmt.Errorf("%w: this environment has no BroadcastChannel", ErrTransport)
	}
	b := &bcBus{ch: ctor.New(room)}
	b.onmsg = js.FuncOf(func(_ js.Value, args []js.Value) any {
		// A frame arrives as whatever was posted, structured-cloned. It is read
		// into Go bytes before the callback, which never blocks the page's thread.
		src := js.Global().Get("Uint8Array").New(args[0].Get("data"))
		raw := make([]byte, src.Length())
		js.CopyBytesToGo(raw, src)
		b.mu.Lock()
		cb := b.cb
		b.mu.Unlock()
		if cb != nil {
			cb(raw)
		}
		return nil
	})
	b.ch.Call("addEventListener", "message", b.onmsg)
	return b, nil
}

func (b *bcBus) post(frame []byte) {
	buf := js.Global().Get("Uint8Array").New(len(frame))
	js.CopyBytesToJS(buf, frame)
	b.ch.Call("postMessage", buf)
}

func (b *bcBus) onFrame(fn func([]byte)) {
	b.mu.Lock()
	b.cb = fn
	b.mu.Unlock()
}

func (b *bcBus) close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	b.mu.Unlock()
	b.ch.Call("close")
	b.onmsg.Release()
}

// randomID draws a tab's identifier. It is random rather than counted because
// there is nobody to count: each tab picks its own, and the space is wide enough
// that two colliding is not a thing that happens. It is never zero, which the
// framing keeps for "everyone".
func randomID() uint64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return binary.LittleEndian.Uint64(b[:]) | 1
}

// JoinBroadcastChannel returns a transport that joins a document another tab of
// this browser is holding, over the BroadcastChannel named room. It is the side
// that joins; [Server.ServeBroadcastChannel] is the side that holds. Like
// [WebSocket] and [DataChannel], it opens nothing until [Join] does — the bus is
// created, and the host dialled, when the session starts.
//
// Use it directly when the page already knows it is joining; use [HostOrJoin]
// when it should discover whether to host or join.
func JoinBroadcastChannel(room string) Transport { return &bcastTransport{room: room} }

type bcastTransport struct{ room string }

func (t *bcastTransport) open(ctx context.Context) (carrierConn, error) {
	b, err := newBCBus(t.room)
	if err != nil {
		return nil, err
	}
	conn, err := dialBus(ctx, attach(b, randomID()))
	if err != nil {
		b.close()
		return nil, err
	}
	return conn, nil
}

// ServeBroadcastChannel runs sessions over the BroadcastChannel named room, with
// this browser tab holding the document, for as many other tabs as join. It
// returns when ctx is cancelled.
//
// It is the counterpart of [Server.ServeDataChannel] for a shared bus rather than
// a point-to-point channel: there is no request to upgrade and no origin to
// check, because a BroadcastChannel is already scoped to this origin and every
// tab on it is this browser.
func (s *Server) ServeBroadcastChannel(ctx context.Context, room string) error {
	b, err := newBCBus(room)
	if err != nil {
		return err
	}
	defer b.close()
	return serveBus(ctx, s, attach(b, randomID()))
}

// HostOrJoin decides whether this tab should hold the document for the room or
// join one another tab already holds, so a page offering "collaborate in this
// browser" need not know which tab it is. It returns [RoleHost] or [RoleClient];
// the page then calls [Server.ServeBroadcastChannel] or [JoinBroadcastChannel]
// on the same room accordingly. window bounds the decision — pass
// [DefaultElectionWindow] unless there is a reason not to. See [electRole] for
// the rule that keeps two tabs opening together from both hosting.
func HostOrJoin(ctx context.Context, room string, window time.Duration) (Role, error) {
	b, err := newBCBus(room)
	if err != nil {
		return 0, err
	}
	defer b.close()
	return electRole(ctx, attach(b, randomID()), window)
}
