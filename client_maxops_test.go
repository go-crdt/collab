//go:build !js

package collab

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// A participant may bound what one message asks it to reserve, which until now
// only a server could do.
//
// The case it is for is the peer-to-peer carriers, where the far end is another
// person's browser rather than the server one's own operator runs: the page that
// hosts builds a Server and could always bound what it was sent, and the page
// that joins could not. One session, a bound in one direction only.
//
// Both cases run the same two participants over the same server and differ only
// in the bound. The control is not decoration: a client that refused everything
// would satisfy the first case, and a bound that refused an honest catch-up
// would be worse than none at all.
func TestAParticipantBoundsWhatOneMessageMayReserve(t *testing.T) {
	const ops = 40

	run := func(t *testing.T, max int) (error, string) {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		srv := NewServer(Config{Store: NewMemoryStore()})
		defer func() { _ = srv.Close(context.Background()) }()

		join := func(site crdt.SiteID, max int) *Client {
			t.Helper()
			tr, sc := Pipe()
			go func() { _ = srv.ServePipe(ctx, sc) }()
			c, err := Join(ctx, tr, ClientConfig{
				Document: "paper", Site: site, MaxOperations: max,
			})
			if err != nil {
				t.Fatalf("join as %d: %v", site, err)
			}
			t.Cleanup(func() { _ = c.Close() })
			return c
		}
		// The reader joins FIRST, so what the writer does next is fanned out to
		// it as operations. A participant that joined afterwards would be sent a
		// snapshot instead, which this does not bound and is not what this is
		// about.
		reader := join(2, max)
		writer := join(1, 0)

		text, err := writer.Text("body")
		if err != nil {
			t.Fatal(err)
		}
		// One call, so the fan-out is ONE message carrying `ops` operations
		// rather than `ops` messages carrying one each.
		if err := text.Insert(0, strings.Repeat("z", ops)); err != nil {
			t.Fatal(err)
		}

		body, err := reader.Text("body")
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			select {
			case <-reader.Done():
				return reader.Err(), body.String()
			default:
			}
			if body.Len() == ops {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		// Settled without the session ending: give a refusal that is in flight a
		// moment to arrive before calling it an acceptance.
		select {
		case <-reader.Done():
			return reader.Err(), body.String()
		case <-time.After(300 * time.Millisecond):
		}
		return reader.Err(), body.String()
	}

	t.Run("more than the bound ends the session", func(t *testing.T) {
		err, held := run(t, ops-1)
		if !errors.Is(err, crdt.ErrTooManyOps) {
			t.Fatalf("the session ended with %v; want crdt.ErrTooManyOps underneath", err)
		}
		if held != "" {
			t.Errorf("the participant holds %q; a refused message must land nothing", held)
		}
	})

	t.Run("the same message under a bound that admits it lands", func(t *testing.T) {
		err, held := run(t, ops)
		if err != nil {
			t.Fatalf("a message of %d operations under a bound of %d ended the session: %v", ops, ops, err)
		}
		if len(held) != ops {
			t.Errorf("the participant holds %q, want %d characters -- so the case above proves a refusal and not a broken fan-out", held, ops)
		}
	})

	t.Run("zero is no bound, which is what it was before", func(t *testing.T) {
		err, held := run(t, 0)
		if err != nil {
			t.Fatalf("an unbounded participant ended the session: %v", err)
		}
		if len(held) != ops {
			t.Errorf("an unbounded participant holds %q, want %d characters", held, ops)
		}
	})
}
