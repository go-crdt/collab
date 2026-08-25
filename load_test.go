//go:build !js

package collab

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// What the backlog bounds is how many people may edit at once, and this pins
// the relationship so that a change to either can be seen.
//
// One edit by each of P participants is P-1 messages into every other queue, so
// a document busier than the backlog reaches the limit in one instant however
// well everyone is keeping up. Measured on one server, 800 participants each
// making one edit simultaneously: a backlog of 256 disconnected 99% of them,
// 512 disconnected 32%, 1024 disconnected none.
//
// The consequence is not degradation. A disconnected participant is not slowed
// down, it is gone, and nothing in this package brings it back — see
// [Config.Backlog].
func TestTheBacklogBoundsHowManyMayEditAtOnce(t *testing.T) {
	const editors = 200

	survivors := func(backlog int) int {
		srv := NewServer(Config{Store: NewMemoryStore(), Backlog: backlog})
		defer func() { _ = srv.Close(context.Background()) }()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		clients := make([]*Client, 0, editors)
		for i := 0; i < editors; i++ {
			transport, conn := Pipe()
			go func() { _ = srv.ServePipe(ctx, conn) }()
			c, err := Join(ctx, transport, ClientConfig{Document: "one", Site: crdt.SiteID(i + 1)})
			if err != nil {
				t.Fatal(err)
			}
			clients = append(clients, c)
		}
		var wg sync.WaitGroup
		for _, c := range clients {
			wg.Add(1)
			go func(c *Client) {
				defer wg.Done()
				if body, err := c.Text("body"); err == nil {
					_ = body.Insert(0, "x")
				}
			}(c)
		}
		wg.Wait()

		// Long enough for a queue that was going to overflow to have done so.
		time.Sleep(2 * time.Second)
		live := 0
		for _, c := range clients {
			select {
			case <-c.Done():
			default:
				live++
			}
			_ = c.Close()
		}
		return live
	}

	// A backlog well under the number editing at once loses people.
	tight := survivors(16)
	if tight == editors {
		t.Fatalf("a backlog of 16 kept all %d simultaneous editors; the limit no longer bites", editors)
	}
	// A backlog over it loses nobody, which is the half that must not regress.
	roomy := survivors(editors * 2)
	if roomy != editors {
		t.Fatalf("a backlog of %d dropped %d of %d simultaneous editors", editors*2, editors-roomy, editors)
	}
	t.Logf("%d simultaneous editors: backlog 16 kept %d, backlog %d kept %d",
		editors, tight, editors*2, roomy)
}
