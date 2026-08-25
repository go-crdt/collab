//go:build !js

package collab

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// blocked is a carrier that accepts a join and then never takes another
// message, which is what a participant that has stopped reading looks like from
// the server's side.
type blocked struct {
	ctx     context.Context
	in      []scripted
	at      int
	release chan struct{}
}

func (b *blocked) Context() context.Context { return b.ctx }

func (b *blocked) Recv() (byte, any, error) {
	if b.at >= len(b.in) {
		<-b.ctx.Done()
		return 0, nil, b.ctx.Err()
	}
	m := b.in[b.at]
	b.at++
	return m.kind, m.msg, nil
}

func (b *blocked) Send(byte, any) error {
	<-b.release
	return nil
}

// What the backlog bounds is how many people may edit at once.
//
// One edit by each of P participants is P-1 messages into every other queue, so
// a document busier than the backlog reaches the limit in one instant however
// well everyone is keeping up. Measured on one server, 800 participants each
// making one edit simultaneously: a backlog of 256 disconnected 99% of them,
// 512 disconnected 32%, 1024 disconnected none — the cliff is the backlog and
// nothing else, shown by moving it.
//
// The consequence is not degradation. A disconnected participant is not slowed
// down, it is gone, and nothing in this package brings it back — see
// [Config.Backlog].
func TestTheBacklogBoundsHowManyMayEditAtOnce(t *testing.T) {
	const editors = 200

	// A backlog larger than the burst keeps everybody. This is the half that
	// must not regress, and it is the half a deployment is sized by.
	t.Run("a backlog over the burst keeps everybody", func(t *testing.T) {
		srv := NewServer(Config{Store: NewMemoryStore(), Backlog: editors * 2})
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
		if live != editors {
			t.Fatalf("a backlog of %d dropped %d of %d simultaneous editors", editors*2, editors-live, editors)
		}
	})

	// And the limit itself, without depending on who is scheduled when: a
	// participant that takes nothing is disconnected once more than the backlog
	// is waiting for it. Racing publishers against consumers would make this
	// test say different things on different machines, which is exactly the
	// kind of test this audit has spent its time removing.
	t.Run("a participant that stops reading is disconnected", func(t *testing.T) {
		const backlog = 8
		srv := NewServer(Config{Store: NewMemoryStore(), Backlog: backlog})
		defer func() { _ = srv.Close(context.Background()) }()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		join, err := encodeClient(kindJoin, joinMsg{Document: "one", Site: 1})
		if err != nil {
			t.Fatal(err)
		}
		kind, msg, err := decodeClient(join)
		if err != nil {
			t.Fatal(err)
		}
		stuck := &blocked{ctx: ctx, in: []scripted{{kind: kind, msg: msg}}, release: make(chan struct{})}
		ended := make(chan error, 1)
		go func() { ended <- srv.session(stuck) }()

		// Somebody else types, more times than the queue can hold.
		transport, conn := Pipe()
		go func() { _ = srv.ServePipe(ctx, conn) }()
		writer, err := Join(ctx, transport, ClientConfig{Document: "one", Site: 2})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = writer.Close() }()
		body, err := writer.Text("body")
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < backlog*4; i++ {
			if err := body.Insert(0, "x"); err != nil {
				t.Fatal(err)
			}
		}

		// The queue is full and the server has already let go of this
		// participant; what it cannot do is say so, because saying so goes
		// through the send that is blocked. Releasing it lets the session
		// notice the queue was closed under it and return, which is the
		// disconnection an operator would see.
		close(stuck.release)
		select {
		case err := <-ended:
			if err == nil {
				t.Fatal("a participant that read nothing was not disconnected")
			}
			t.Logf("disconnected after a backlog of %d filled: %v", backlog, err)
		case <-time.After(15 * time.Second):
			t.Fatal("a participant that read nothing was never disconnected")
		}
	})
}
