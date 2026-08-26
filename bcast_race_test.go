//go:build !js

package collab

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The fix, proven fast and deterministic: a second tab opening DURING the host's
// serve-gap — after the host elected but before it attached its Server — is
// welcomed and elects client, and the two converge on one document. This is the
// race the sequential two-tab user hit; the old election left the second tab
// electing a rival host, giving two documents and no sync.
func TestBcastServeGapConverges(t *testing.T) {
	hub := &memHub{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const window = 60 * time.Millisecond

	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()

	// Tab A opens alone and elects host; its answerer is live at once, before its
	// Server is attached.
	roleA, hostA, connA, err := hostOrJoinBus(ctx, attach(hub.join(), 100), window)
	if err != nil {
		t.Fatalf("tab A hostOrJoinBus: %v", err)
	}
	if roleA != RoleHost || hostA == nil || connA != nil {
		t.Fatalf("tab A elected %v (host=%v conn=%v), want a hosting endpoint", roleA, hostA != nil, connA != nil)
	}

	// Tab B opens during the serve-gap — the window that used to strand it as a
	// second host. It must find A (still wiring its Server) and elect client.
	type result struct {
		role Role
		conn carrierConn
		err  error
	}
	bch := make(chan result, 1)
	go func() {
		time.Sleep(window + 20*time.Millisecond) // A has elected; A is mid serve-gap
		role, host, conn, err := hostOrJoinBus(ctx, attach(hub.join(), 200), window)
		if host != nil {
			t.Errorf("tab B got a host endpoint; the serve-gap must not spawn a second host")
		}
		bch <- result{role, conn, err}
	}()

	// The gap itself: A is elected but its Server is not yet attached.
	//
	// Waiting for A to have actually heard B, rather than sleeping for as long
	// as that ought to take. The whole point of this test is the tab that
	// arrives while the host has no Server — so the tab has to have arrived,
	// and a sleep only arranges that most of the time. When it did not, the
	// loop in attachServer that picks up tabs already waiting was never run and
	// this test passed anyway, guarding nothing.
	waitUntil(t, "tab B to reach the host", func() bool {
		hostA.mu.Lock()
		defer hostA.mu.Unlock()
		return len(hostA.ends) > 0
	})
	hostA.attachServer(srv)
	served := make(chan error, 1)
	go func() { served <- hostA.wait(); hostA.close() }()

	b := <-bch
	if b.err != nil {
		t.Fatalf("tab B hostOrJoinBus: %v", b.err)
	}
	if b.role != RoleClient {
		t.Fatalf("tab B elected %v, want RoleClient", b.role)
	}

	// B joins over the carrier the election dialled — reused, not redialled — and
	// types; the edit reaches the document the host holds. One shared document.
	bc, err := Join(ctx, &openedCarrier{c: b.conn}, ClientConfig{Document: "shared", Site: 2})
	if err != nil {
		t.Fatalf("tab B Join: %v", err)
	}
	defer func() { _ = bc.Close() }()
	bcastInsert(t, bc, 0, "hello")

	// A third tab joins the live host and catches up: the host really is serving.
	c := dialClient(t, ctx, hub, 300, ClientConfig{Document: "shared", Site: 3})
	defer func() { _ = c.Close() }()
	awaitBcast(t, c, "the third tab to catch up", func() bool { return bcastText(t, c) == "hello" })

	cancel()
	select {
	case <-served:
	case <-time.After(bcastSettle):
		t.Fatal("host did not return after its context was cancelled")
	}
}

// The self-healing half of the election: if two tabs ever both hold the room, the
// one with the higher identifier hears the lower's beacon and steps down with
// ErrHostSuperseded, while the lower keeps the document. No two documents drift
// apart.
func TestBcastHostDemotesToLowerHost(t *testing.T) {
	hub := &memHub{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	low := newBcastHost(ctx, attach(hub.join(), 10))
	defer low.close()
	high := newBcastHost(ctx, attach(hub.join(), 20))

	// The higher id steps down when it hears the lower's beacon.
	highErr := make(chan error, 1)
	go func() { highErr <- high.wait() }()
	select {
	case err := <-highErr:
		if !errors.Is(err, ErrHostSuperseded) {
			t.Fatalf("the higher host returned %v, want ErrHostSuperseded", err)
		}
	case <-time.After(bcastSettle):
		t.Fatal("the higher host did not step down")
	}
	high.close()

	// The lower host keeps the room: it must not step down.
	lowErr := make(chan error, 1)
	go func() { lowErr <- low.wait() }()
	select {
	case err := <-lowErr:
		t.Fatalf("the lower host stepped down with %v; it should keep the room", err)
	case <-time.After(150 * time.Millisecond):
	}
	cancel()
	if err := <-lowErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("the lower host returned %v after cancel, want context.Canceled", err)
	}
}

// A tab that opens after another's first hello — its bus not yet existing then —
// still hears the re-announced hello and defers to the lower identifier, rather
// than electing a rival host because it missed the one-shot announcement.
func TestBcastElectionReannounceReachesLateTab(t *testing.T) {
	hub := &memHub{}
	low := attach(hub.join(), 10)
	lowDone := make(chan Role, 1)
	go func() {
		r, _ := electRole(context.Background(), low, 400*time.Millisecond)
		lowDone <- r
	}()

	// The late tab opens after low's first hello but well within the window; the
	// re-announcement reaches it.
	time.Sleep(80 * time.Millisecond)
	high := attach(hub.join(), 20)
	r, err := electRole(context.Background(), high, 250*time.Millisecond)
	if err != nil {
		t.Fatalf("late tab electRole: %v", err)
	}
	if r != RoleClient {
		t.Fatalf("the late higher tab elected %v; the re-announced hello must make it defer, want RoleClient", r)
	}
	if got := <-lowDone; got != RoleHost {
		t.Fatalf("the first tab elected %v, want RoleHost", got)
	}
}

// A tab that hears a host beacon joins even if it was never welcomed — the beacon
// alone tells it a host holds the room, which is what covers a host still wiring
// its Server.
func TestBcastElectionDefersToHostBeacon(t *testing.T) {
	hub := &memHub{}
	self := attach(hub.join(), 5)
	beacon := hub.join()
	beacon.post(envelope{ctl: ctlHost, from: 99}.encode())

	r, err := electRole(context.Background(), self, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("electRole: %v", err)
	}
	if r != RoleClient {
		t.Fatalf("a tab hearing a host beacon elected %v, want RoleClient", r)
	}
}

// hostOrJoinBus surfaces the election's and the dial's errors rather than
// returning a false role.
func TestHostOrJoinBusErrors(t *testing.T) {
	// The election ending (a cancelled context) is returned, not swallowed.
	hub := &memHub{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, err := hostOrJoinBus(ctx, attach(hub.join(), 1), bcastSettle); !errors.Is(err, context.Canceled) {
		t.Fatalf("hostOrJoinBus on a cancelled context = %v, want context.Canceled", err)
	}

	// A tab that elects client from a beacon but is never welcomed on its dial
	// surfaces the dial's deadline, with no host and no carrier.
	hub2 := &memHub{}
	self := attach(hub2.join(), 5)
	beacon := hub2.join()
	beacon.post(envelope{ctl: ctlHost, from: 99}.encode()) // present, but never welcomes a dial
	ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()
	role, host, conn, err := hostOrJoinBus(ctx2, self, 40*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("hostOrJoinBus dial with no welcome = (%v, %v), want DeadlineExceeded", role, err)
	}
	if host != nil || conn != nil {
		t.Fatalf("a failed dial returned host=%v conn=%v, want both nil", host != nil, conn != nil)
	}
}

// waitUntil waits for something to become true, and says what it was waiting
// for if it never does.
func waitUntil(t *testing.T, what string, want func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !want() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}
