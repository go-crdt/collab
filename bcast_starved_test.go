package collab

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// A starvedHub is a bus that accepts posts and delivers none of them until it is
// released.
//
// It is what a saturated event loop does: posting succeeds, and the frames sit
// in a queue nobody reads. The election's re-announcement makes a hello reach
// every tab, and reaching is not the same as being read -- which is the
// condition the guarantee on electRole depends on and did not say.
type starvedHub struct {
	mu       sync.Mutex
	members  []*starvedBus
	held     [][2]any // frame, and the bus that posted it
	released bool
}

func (h *starvedHub) join() *starvedBus {
	b := &starvedBus{hub: h}
	h.mu.Lock()
	h.members = append(h.members, b)
	h.mu.Unlock()
	return b
}

// release delivers everything held so far and stops holding.
func (h *starvedHub) release() {
	h.mu.Lock()
	held, members := h.held, append([]*starvedBus(nil), h.members...)
	h.held, h.released = nil, true
	h.mu.Unlock()
	for _, item := range held {
		frame, from := item[0].([]byte), item[1].(*starvedBus)
		deliver(members, from, frame)
	}
}

func deliver(members []*starvedBus, from *starvedBus, frame []byte) {
	for _, m := range members {
		if m == from {
			continue
		}
		m.mu.Lock()
		cb, closed := m.cb, m.closed
		m.mu.Unlock()
		if cb != nil && !closed {
			cb(append([]byte(nil), frame...))
		}
	}
}

type starvedBus struct {
	hub    *starvedHub
	mu     sync.Mutex
	cb     func([]byte)
	closed bool
}

func (b *starvedBus) post(frame []byte) {
	cp := append([]byte(nil), frame...)
	b.hub.mu.Lock()
	if !b.hub.released {
		b.hub.held = append(b.hub.held, [2]any{cp, b})
		b.hub.mu.Unlock()
		return
	}
	members := append([]*starvedBus(nil), b.hub.members...)
	b.hub.mu.Unlock()
	deliver(members, b, cp)
}

func (b *starvedBus) onFrame(cb func([]byte)) {
	b.mu.Lock()
	b.cb = cb
	b.mu.Unlock()
}

func (b *starvedBus) close() {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
}

// Two tabs that read nothing in time BOTH host, and the room heals afterwards.
//
// The comment on electRole said "exactly one tab among any set that overlaps
// hosts, and every other joins it", flatly. That holds when what is posted is
// also read within the window. It is not what a loaded machine does: a browser
// running wasm on a CI runner leaves the hellos in a queue, both windows close
// having heard nothing, and both tabs elect themselves.
//
// It is not hypothetical. That is what reddened go-tex's browser proofs for a
// week, reproduced there by throttling the CPU 6x, 12x and 20x -- and the tab
// that met it showed "Connected" while holding nobody, because its caller
// treated ErrHostSuperseded as a failure rather than re-joining.
//
// So this pins both halves: the disease is reachable through the real election,
// and the cure is what makes the design sound rather than a backstop nobody
// expects to need.
func TestTwoTabsThatReadNothingInTimeBothHostAndTheRoomHeals(t *testing.T) {
	hub := &starvedHub{}
	lowBus, highBus := hub.join(), hub.join()
	low, high := attach(lowBus, 10), attach(highBus, 20)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Both elect while nothing is being read.
	const window = 40 * time.Millisecond
	type outcome struct {
		role Role
		err  error
	}
	roles := make(chan outcome, 2)
	for _, bc := range []*busConn{low, high} {
		go func() {
			r, err := electRole(ctx, bc, window)
			roles <- outcome{r, err}
		}()
	}
	for range 2 {
		select {
		case got := <-roles:
			if got.err != nil {
				t.Fatalf("the election failed: %v", got.err)
			}
			if got.role != RoleHost {
				t.Fatalf("a starved tab elected %v; the point of this test is that both host", got.role)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("an election never finished")
		}
	}

	// Two hosts, which is the state the flat guarantee says cannot happen.
	lowHost, highHost := newBcastHost(ctx, low), newBcastHost(ctx, high)
	defer lowHost.close()

	// The bus starts moving again, and the room settles on one document: the
	// higher identifier hears the lower's beacon and steps down.
	hub.release()

	stepped := make(chan error, 1)
	go func() { stepped <- highHost.wait() }()
	select {
	case err := <-stepped:
		if !errors.Is(err, ErrHostSuperseded) {
			t.Fatalf("the higher host returned %v, want ErrHostSuperseded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the higher host never stepped down, so two documents drift apart")
	}
	highHost.close()

	// And the lower one keeps the room rather than stepping down with it.
	kept := make(chan error, 1)
	go func() { kept <- lowHost.wait() }()
	select {
	case err := <-kept:
		t.Fatalf("the lower host also gave up the room: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
}
