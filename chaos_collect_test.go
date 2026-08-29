//go:build !js

package collab

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// Collecting, while everything else is going wrong.
//
// The harness beside this one never deletes anything, so it has no tombstones
// and nothing to collect: it proves that operations survive, which is a
// different question. This one deletes as it writes, turns collection on, and
// asks whether a server that is *giving data back* while sessions are cut, the
// store refuses and documents are archived under it still leaves every
// participant holding the same document.
//
// It writes to a map part, because that is what collects: a text and a list had
// a collection too and it was withdrawn for leaving two replicas holding
// different documents. See the note in the crdt package.
//
// That is the assertion worth making about collection and the only one worth
// making: not that it saved bytes, but that nobody ended up with a different
// text because of it. Collecting is the one thing this server does that removes
// what somebody wrote, so it is the one thing that has to be broken on purpose.
//
//	COLLAB_CHAOS=1 go test -race -run TestCollectingWhileEverythingBreaks -v .
func TestCollectingWhileEverythingBreaks(t *testing.T) {
	if os.Getenv("COLLAB_CHAOS") == "" {
		t.Skip("set COLLAB_CHAOS=1 to run the chaos harness")
	}
	editors := envInt("CHAOS_COLLECT_EDITORS", 24)
	rounds := envInt("CHAOS_COLLECT_ROUNDS", 30)
	cuts := envInt("CHAOS_COLLECT_CUTS", 6)
	backlog := envInt("CHAOS_COLLECT_BACKLOG", 256)
	// One turn in this many sends somebody away for a while. Zero is never.
	absences := envInt("CHAOS_COLLECT_ABSENCES", 3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hot := &flaky{MemoryStore: NewMemoryStore()}
	archive := NewMemoryStore()
	store := NewTiered(hot, archive)

	var persistErrors atomic.Int64
	srv := NewServer(Config{
		Store:        store,
		Backlog:      backlog,
		PersistEvery: 3 * time.Millisecond,
		CollectEvery: time.Duration(envInt("CHAOS_COLLECT_EVERY_MS", 3)) * time.Millisecond,
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

	// Everybody writes a word of their own and takes somebody else's away, so
	// whole runs die and there is something to collect. A word rather than a
	// character because a run is collected whole: one character at a time from
	// one site is one run that never dies.
	var wrote, removed atomic.Int64
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func(i int, c *Client) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(i), 3))
			cells, err := c.Map("cells")
			if err != nil {
				t.Errorf("participant %d has no map: %v", i, err)
				return
			}
			for round := 0; round < rounds; round++ {
				// A pool small enough that everybody collides on it, which is
				// what makes concurrent writes to one key happen.
				k := fmt.Sprintf("cell-%d", r.IntN(editors*2))
				if err := cells.Set(k, []byte(fmt.Sprintf("%d:%d", i, round))); err != nil {
					t.Errorf("participant %d: an edit failed: %v", i, err)
					return
				}
				wrote.Add(1)
				if round%3 == 2 {
					if keys := cells.Keys(); len(keys) > 0 {
						if err := cells.Delete(keys[r.IntN(len(keys))]); err != nil {
							t.Errorf("participant %d: a deletion failed: %v", i, err)
							return
						}
						removed.Add(1)
					}
				}
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
		r := rand.New(rand.NewPCG(101, 4))
		for turn := 0; ; turn++ {
			select {
			case <-breaking:
				return
			case <-time.After(10 * time.Millisecond):
			}
			for i := 0; i < cuts; i++ {
				links[r.IntN(len(links))].cut()
			}
			// And an absence, which is a different thing from a cut. A cut
			// carrier is an interruption: the supervised client redials before
			// the room has done anything. A participant that is off the air
			// while the others write, delete and are collected against is the
			// one that comes back needing to be told what it missed -- and for
			// as long as this harness could not arrange that, it could not find
			// what happens when the telling is wrong. It missed a divergence
			// that way. See TestCollectingPastAnAbsentParticipant.
			if absences > 0 && turn%absences == 0 {
				gone := links[r.IntN(len(links))]
				// How long, drawn here: the goroutine that waits it out is one
				// of several alive at once, and a generator is not safe for two
				// of them. The race detector said so on the first run of the
				// lane this commit adds, which is the lane doing its job.
				how := time.Duration(20+r.IntN(180)) * time.Millisecond
				gone.away()
				go func() {
					time.Sleep(how)
					gone.back()
				}()
			}
			if envInt("CHAOS_COLLECT_FLAKY", 1) != 0 {
				hot.refuse(persistErrors.Load() == 0 || turn%2 == 0)
			}
			if envInt("CHAOS_COLLECT_ARCHIVE", 1) != 0 {
				moved, err := store.Archive(ctx, time.Nanosecond)
				archived.Add(int64(moved))
				if err != nil {
					refusedArchives.Add(1)
				}
			}
		}
	}()

	wg.Wait()
	waited := time.Now()
	for persistErrors.Load() == 0 && time.Since(waited) < 30*time.Second {
		time.Sleep(2 * time.Millisecond)
	}
	close(breaking)
	wgBreak.Wait()
	hot.refuse(false)

	// Everybody has to end up holding the same document. Not a length — the
	// text itself, because a collection that dropped the wrong run would leave
	// two replicas the same length and saying different things.
	// A settling budget, not a performance bar. It is wall clock, so it measures
	// the machine as much as the code: sixty seconds is minutes of work for one
	// participant when something else on the box is using every core, and a
	// harness that calls that a divergence has cried wolf. It therefore scales
	// with the crowd and is generous, because what is being asserted is that
	// they agree in the end and not how soon.
	budget := time.Duration(60+2*editors) * time.Second
	if s := os.Getenv("CHAOS_COLLECT_SETTLE"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			budget = time.Duration(n) * time.Second
		}
	}
	deadline := time.Now().Add(budget)
	var agreed string
	contents := func(c *Client) (string, bool) {
		m, err := c.Map("cells")
		if err != nil {
			return "", false
		}
		var b strings.Builder
		for _, k := range m.Keys() {
			b.WriteString(k)
			b.WriteByte(0)
			v, _ := m.Get(k)
			b.Write(v)
			b.WriteByte(1)
		}
		return b.String(), true
	}
	for {
		texts := make([]string, 0, len(clients))
		for _, c := range clients {
			got, ok := contents(c)
			if !ok {
				break
			}
			texts = append(texts, got)
		}
		if len(texts) == len(clients) {
			same := true
			for _, s := range texts[1:] {
				if s != texts[0] {
					same = false
					break
				}
			}
			if same {
				agreed = texts[0]
				break
			}
		}
		if time.Now().After(deadline) {
			differing := map[string]int{}
			for _, s := range texts {
				differing[s]++
			}
			t.Fatalf("%d participants hold %d different documents after %s", len(texts), len(differing), budget)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// And a participant arriving now is handed the same thing, which is the
	// server's own answer rather than a guess about it.
	tr, sc := Pipe()
	go func() { _ = srv.ServePipe(ctx, sc) }()
	fresh, err := Join(ctx, tr, ClientConfig{Document: "paper", Site: 9999})
	if err != nil {
		t.Fatalf("a fresh participant could not join what was collected: %v", err)
	}
	defer func() { _ = fresh.Close() }()
	got, ok := contents(fresh)
	if !ok {
		t.Fatal("a fresh participant has no map")
	}
	if got != agreed {
		t.Fatalf("a fresh participant was handed %d bytes of map, the room holds %d",
			len(got), len(agreed))
	}

	// The chaos has to have happened, or this is a slow way of asserting
	// nothing. Collection especially: a run that collected nothing proves
	// nothing about collecting.
	if persistErrors.Load() == 0 {
		t.Fatal("the store never refused, so the disk filling up was not tested")
	}
	srv.mu.Lock()
	d := srv.docs["paper"]
	srv.mu.Unlock()
	if d == nil {
		t.Fatal("the document is gone")
	}
	d.mu.Lock()
	cells, err := d.doc.Map("cells")
	var tombstones int
	if err == nil {
		tombstones = cells.Tombstones()
	}
	d.mu.Unlock()
	if int64(tombstones) >= removed.Load() {
		t.Fatalf("%d characters were deleted and %d tombstones remain: nothing was collected",
			removed.Load(), tombstones)
	}
	t.Logf("%d participants wrote %d values and deleted %d; %d tombstones remain",
		editors, wrote.Load(), removed.Load(), tombstones)
	t.Logf("archived %d times, refused %d; the store refused %d writes",
		archived.Load(), refusedArchives.Load(), persistErrors.Load())
	t.Logf("everybody holds the same %d bytes of map", len(agreed))
}
