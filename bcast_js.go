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
// tab is hosting, and how long [OpenBroadcastSession] listens when it has no
// better rule available.
//
// It is the FALLBACK's window now, not the rule's. Where the browser has Web
// Locks, [OpenBroadcastSession] asks for an exclusive lock on the room and gets
// an immediate, exclusive answer, so no duration is waited out and two tabs
// cannot both host. Where it does not -- and in [HostOrJoin], whose context
// bounds only the election and so has no lifetime to hold a lock for -- this is
// how long a tab listens. See electByLock, and
// TestTwoTabsElectOneHostWithNoWindow for what the difference is worth: with a
// zero window the fallback leaves BOTH tabs hosting and the lock leaves one.
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
//
// Prefer [OpenBroadcastSession], which keeps the elected bus and identity and so
// leaves no window between electing and serving where an elected host is deaf.
// This two-call shape reopens the bus to serve, which is exactly that window; it
// is kept for a page that already knows its role.
func HostOrJoin(ctx context.Context, room string, window time.Duration) (Role, error) {
	b, err := newBCBus(room)
	if err != nil {
		return 0, err
	}
	defer b.close()
	return electRole(ctx, attach(b, randomID()), window)
}

// A BroadcastSession is a zero-config same-browser endpoint that has already
// elected its role and is live: a host that is answering hellos, or a client that
// has dialled its host — both on ONE BroadcastChannel and identity, kept from the
// election through to serving. That is what closes the serve-gap: an elected host
// answers the moment it wins, so a second tab opening while the first is still
// wiring its [Server] is welcomed rather than left to elect itself a rival host.
type BroadcastSession struct {
	role Role
	host *bcastHost  // set when RoleHost: already answering
	conn carrierConn // set when RoleClient: already dialled
	bus  *bcBus
}

// OpenBroadcastSession opens the room's BroadcastChannel, elects host or client on
// it, and returns a live [BroadcastSession] holding that decision — the
// gap-free replacement for [HostOrJoin] followed by a fresh bus. A host is
// already answering hellos and beaconing; a client has already dialled its host.
// ctx bounds the election (and, for a client, the dial); window is the election
// window — pass [DefaultElectionWindow].
//
// A page that hosts builds its [Server] and calls [BroadcastSession.Serve]; a
// page that joins calls [Join] with [BroadcastSession.Transport]. Either way ctx
// is the session's lifetime — cancel it to tear down — and [BroadcastSession.Close]
// releases the channel if the role is abandoned before serving or joining.
func OpenBroadcastSession(ctx context.Context, room string, window time.Duration) (*BroadcastSession, error) {
	b, err := newBCBus(room)
	if err != nil {
		return nil, err
	}
	bc := attach(b, randomID())
	// The lock first, where the browser has one. It answers immediately and
	// exclusively, so two tabs opening together cannot both host -- where
	// electRole's window can leave both of them hosting on a loaded machine, and
	// does. window is then the FALLBACK's window rather than the rule's.
	//
	// Only here and not in [HostOrJoin]: a lock is held for as long as the promise
	// its callback returned is unsettled, and this ctx is the session's lifetime,
	// which is the thing a lock can be tied to. See electByLock.
	role, host, conn, err := openWith(ctx, bc, room, window)
	if err != nil {
		b.close()
		return nil, err
	}
	return &BroadcastSession{role: role, host: host, conn: conn, bus: b}, nil
}

// Role is whether this tab elected to host the document or to join one.
func (bs *BroadcastSession) Role() Role { return bs.role }

// Serve holds the document for the room: it wires s into the answerer that has
// been welcoming and buffering tabs since the election, so those tabs' sessions
// begin at once, and blocks until the room is torn down (its ctx cancelled). It
// returns [ErrHostSuperseded] if another tab with priority took over the room, on
// which the caller should re-join rather than treat it as a failure. It is a
// no-op error for a client session.
func (bs *BroadcastSession) Serve(s *Server) error {
	if bs.role != RoleHost || bs.host == nil {
		return ErrProtocol
	}
	defer bs.bus.close()
	bs.host.attachServer(s)
	defer bs.host.close()
	return bs.host.wait()
}

// Transport hands the already-dialled host connection to [Join] for a client
// session, so joining reuses the carrier the election opened rather than dialling
// again. It is nil-safe to call on a host session, returning a transport whose
// open fails.
func (bs *BroadcastSession) Transport() Transport { return &openedCarrier{c: bs.conn} }

// Close releases the channel for a session abandoned before it served or joined
// (a host that hit a setup error before [BroadcastSession.Serve]); once serving
// or joined, the session's own teardown closes it.
func (bs *BroadcastSession) Close() error {
	if bs.host != nil {
		bs.host.close()
	}
	bs.bus.close()
	return nil
}

// openWith decides this tab's role and wires it, by lock where the browser has one
// and by window where it does not.
//
// The fallback is not a formality: a browser without Web Locks gets exactly the
// election it got before, and [ErrHostSuperseded] still heals the room when that
// election leaves two hosts. What the lock changes is that it cannot.
func openWith(ctx context.Context, bc *busConn, room string, window time.Duration) (Role, *bcastHost, carrierConn, error) {
	if role, byLock := electByLock(ctx, room); byLock {
		return wireRole(ctx, bc, role)
	}
	return hostOrJoinBus(ctx, bc, window)
}
