//go:build !js

package collab

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// Everything built for this server, running at once and being broken while it
// runs: participants that reconnect, a store that is two stores, an archive
// that documents are moved into while people are still arriving, a backlog far
// too small for the room, and a disk that intermittently refuses to write.
//
// What it asserts is the only thing worth asserting about a system like this:
// that every character anybody typed is in the document everybody ends up with,
// and that the document the store holds is that same document.
//
// Sized from the environment so the same harness can be run harder by hand than
// it is in CI — CHAOS_EDITORS, CHAOS_ROUNDS, CHAOS_CUTS.
func TestEverythingAtOnceAndBrokenWhileItRuns(t *testing.T) {
	// Off unless asked for, because it is slow and because somebody running the
	// suite while they work does not want it. CI sets COLLAB_CHAOS, so it runs
	// where a harness has to: one that never runs is one that has quietly
	// stopped working.
	//
	// It found collab#62 and it is what would catch it coming back, which is
	// why CI runs it with CHAOS_BACKLOG far under the number of participants —
	// the setting under which a rejoining participant used to be stranded.
	//
	//	COLLAB_CHAOS=1 CHAOS_BACKLOG=4 go test -race -run TestEverythingAtOnce -v .
	if os.Getenv("COLLAB_CHAOS") == "" {
		t.Skip("set COLLAB_CHAOS=1 to run the chaos harness")
	}

	editors := envInt("CHAOS_EDITORS", 40)
	rounds := envInt("CHAOS_ROUNDS", 25)
	cuts := envInt("CHAOS_CUTS", 8)
	// The backlog is configurable because it is the one knob that decides
	// whether this converges. At 256 — the default a deployment gets — it
	// converges every time. At 4 it does not, and that is collab#62: a
	// participant dropped for falling behind can rejoin, stay connected, and
	// remain permanently short, holding operations it can never apply. The
	// default here is the configuration a deployment actually has; the other
	// one reproduces the bug.
	backlog := envInt("CHAOS_BACKLOG", 256)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The store is a hot one beside an archive, and the hot one refuses to
	// write now and then the way a disk that is filling up does.
	hot := &flaky{MemoryStore: NewMemoryStore()}
	archive := NewMemoryStore()
	store := NewTiered(hot, archive)

	var persistErrors atomic.Int64
	srv := NewServer(Config{
		Store:        store,
		Backlog:      backlog,
		PersistEvery: 3 * time.Millisecond,
		OnPersistError: func(string, error) {
			persistErrors.Add(1)
		},
	})
	defer func() { _ = srv.Close(context.Background()) }()

	links := make([]*breakable, 0, editors)
	clients := make([]*Client, 0, editors)
	for i := 0; i < editors; i++ {
		link := &breakable{srv: srv, ctx: ctx}
		c, err := JoinWithRetry(ctx, link.dial,
			ClientConfig{Document: "paper", Site: crdt.SiteID(i + 1)},
			RetryPolicy{Wait: time.Millisecond, Ceiling: 20 * time.Millisecond})
		if err != nil {
			t.Fatalf("participant %d could not join: %v", i, err)
		}
		links = append(links, link)
		clients = append(clients, c)
	}
	defer func() {
		for _, c := range clients {
			_ = c.Close()
		}
	}()

	// Everybody types, all at once, for a while — and while they type, sessions
	// are cut, the store refuses, and documents are archived out from under the
	// server.
	var typed atomic.Int64
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func(i int, c *Client) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(i), 1))
			body, err := c.Text("body")
			if err != nil {
				t.Errorf("participant %d has no body: %v", i, err)
				return
			}
			for round := 0; round < rounds; round++ {
				// One character each, at a position of its own choosing, so
				// the operations genuinely interleave.
				at := 0
				if n := body.Len(); n > 0 {
					at = r.IntN(n + 1)
				}
				if err := body.Insert(at, "x"); err != nil {
					t.Errorf("participant %d: an edit failed: %v", i, err)
					return
				}
				typed.Add(1)
				time.Sleep(time.Duration(r.IntN(2000)) * time.Microsecond)
			}
		}(i, c)
	}

	breaking := make(chan struct{})
	var archived, refusedArchives atomic.Int64
	var wgBreak sync.WaitGroup
	wgBreak.Add(1)
	go func() {
		defer wgBreak.Done()
		r := rand.New(rand.NewPCG(99, 2))
		for turn := 0; ; turn++ {
			select {
			case <-breaking:
				return
			case <-time.After(10 * time.Millisecond):
			}
			// Cut a handful of sessions.
			for i := 0; i < cuts; i++ {
				links[r.IntN(len(links))].cut()
			}
			// Refuse every other turn, and for the whole of it. A coin flip
			// here made the test pass on a coincidence: the window has to be
			// certain to span a persist tick, or the guard below that says the
			// chaos happened is itself the flaky part.
			hot.refuse(persistErrors.Load() == 0 || turn%2 == 0)
			// Archive whatever has gone quiet, which for a document being
			// typed into means asking constantly and being told no.
			moved, err := store.Archive(ctx, time.Nanosecond)
			archived.Add(int64(moved))
			if err != nil {
				refusedArchives.Add(1)
			}
		}
	}()

	wg.Wait()
	// The breaker runs on until the chaos it exists to cause has demonstrably
	// happened, rather than until the typing stops. A short run could otherwise
	// end with no save ever attempted during a refusing window, and the guard
	// below that says the disk was made to fill up would be the flaky part of
	// this test rather than the certain one.
	waited := time.Now()
	for persistErrors.Load() == 0 && time.Since(waited) < 30*time.Second {
		time.Sleep(2 * time.Millisecond)
	}
	close(breaking)
	wgBreak.Wait()
	hot.refuse(false)

	want := int(typed.Load())
	if want != editors*rounds {
		t.Fatalf("only %d of %d edits were made", want, editors*rounds)
	}

	// Everything anybody typed has to be in the document everybody holds.
	deadline := time.Now().Add(20 * time.Second)
	for {
		short := 0
		for _, c := range clients {
			body, err := c.Text("body")
			if err != nil || body.Len() != want {
				short++
			}
		}
		if short == 0 {
			break
		}
		if time.Now().After(deadline) {
			for i, c := range clients {
				body, err := c.Text("body")
				if err != nil || body.Len() == want {
					continue
				}
				state := "connected"
				select {
				case <-c.Done():
					state = "GAVE UP: " + errText(c.Err())
				default:
				}
				t.Logf("  participant %d holds %d of %d, %s, %d sessions",
					i, body.Len(), want, state, links[i].sessions())
			}
			// What does the server itself hold? A participant arriving now is
			// handed whatever the server has, so this is the server's answer
			// rather than a guess about it.
			tr, sc := Pipe()
			go func() { _ = srv.ServePipe(ctx, sc) }()
			if fresh, err := Join(ctx, tr, ClientConfig{Document: "paper", Site: 9999}); err != nil {
				t.Logf("  a fresh participant could not join: %v", err)
			} else {
				if fb, err := fresh.Text("body"); err == nil {
					t.Logf("  THE SERVER hands out %d of %d", fb.Len(), want)
				}
				_ = fresh.Close()
			}
			t.Fatalf("%d of %d participants are still short of %d characters", short, editors, want)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// And they hold the same document, not merely the same number of
	// characters: a snapshot is canonical, so equal bytes is equal state.
	first := clients[0].Snapshot()
	for i, c := range clients {
		if got := c.Snapshot(); string(got) != string(first) {
			t.Fatalf("participant %d holds a different document (%d bytes against %d)", i, len(got), len(first))
		}
	}

	// The store has to end up holding it too, once it is allowed to write.
	if err := srv.Flush(ctx); err != nil {
		t.Fatalf("the final flush failed: %v", err)
	}
	kept, err := store.Load(ctx, "paper")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := crdt.LoadComposite(1, kept)
	if err != nil {
		t.Fatalf("what the store holds will not load: %v", err)
	}
	body, err := doc.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if body.Len() != want {
		t.Fatalf("the store holds %d characters, and %d were typed", body.Len(), want)
	}

	// The chaos has to have happened, or this test proves nothing.
	if persistErrors.Load() == 0 {
		t.Fatal("the store never refused a write; the disk was not made to fill up")
	}
	if hot.refusals.Load() == 0 {
		t.Fatal("no write was refused")
	}
	sessions := 0
	for _, l := range links {
		sessions += l.sessions()
	}
	if sessions <= editors {
		t.Fatalf("only %d sessions for %d participants; nobody reconnected", sessions, editors)
	}
	t.Logf("%d participants, %d characters, %d sessions (%.1f each)",
		editors, want, sessions, float64(sessions)/float64(editors))
	t.Logf("writes refused %d, reported %d; archived %d times, refused %d",
		hot.refusals.Load(), persistErrors.Load(), archived.Load(), refusedArchives.Load())
}

// flaky is a store that refuses to write while it is told to.
type flaky struct {
	*MemoryStore
	no       atomic.Bool
	refusals atomic.Int64
}

func (f *flaky) Save(ctx context.Context, document string, snapshot []byte) error {
	if f.no.Load() {
		f.refusals.Add(1)
		return fmt.Errorf("no space left on device")
	}
	return f.MemoryStore.Save(ctx, document, snapshot)
}

func (f *flaky) refuse(yes bool) { f.no.Store(yes) }

func envInt(name string, or int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return or
}

func errText(err error) string {
	if err == nil {
		return "no reason given"
	}
	return err.Error()
}
