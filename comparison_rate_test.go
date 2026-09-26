//go:build !js

package collab

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// How often two replicas would have to compare, which is the number that
// decides what a comparison may cost.
//
// go-crdt/crdt#123 wants a digest of state exchanged beside the version vector,
// so that two replicas holding different text cannot both conclude they are
// caught up. Whether that digest has to be maintained incrementally, or may be
// a walk over the document computed when it is asked for, depends entirely on
// how often it would be computed -- and that is a count, not an opinion.
//
// Counting rather than timing is also what this machine can honestly do while
// other work is running on it: a message count is a property of the protocol
// and does not move with load.
//
// What it found:
//
//	participants   acknowledgements per keystroke   joins
//	           2                              2.0   one per participant, per session
//	          10                             10.2   "
//	          50                             51.0   "
//
// So an acknowledgement per participant per KEYSTROKE, against one join per
// participant per SESSION. Those are four orders of magnitude apart in a busy
// document, and crdt's companion measurement (TestWhatADigestWalkCostsAgainstA
// Snapshot) prices a walk at about ten times the snapshot a join already pays.
// Together they say: compare where a replica concludes it is caught up, and
// never on an acknowledgement.

type wireCounts struct {
	sent [8]atomic.Int64
	recv [8]atomic.Int64
}

func (c *wireCounts) String() string {
	return fmt.Sprintf("sent{join:%d ack:%d} recv{welcome:%d op:%d}",
		c.sent[kindJoin].Load(), c.sent[kindAcknowledge].Load(),
		c.recv[kindWelcome].Load(), c.recv[kindOperation].Load())
}

type countingTransport struct {
	inner Transport
	c     *wireCounts
}

func (t *countingTransport) open(ctx context.Context) (carrierConn, error) {
	inner, err := t.inner.open(ctx)
	if err != nil {
		return nil, err
	}
	return &countingConn{carrierConn: inner, c: t.c}, nil
}

type countingConn struct {
	carrierConn
	c *wireCounts
}

func (c *countingConn) Send(kind byte, msg any) error {
	if int(kind) < len(c.c.sent) {
		c.c.sent[kind].Add(1)
	}
	return c.carrierConn.Send(kind, msg)
}

func (c *countingConn) Recv() (byte, any, error) {
	kind, msg, err := c.carrierConn.Recv()
	if err == nil && int(kind) < len(c.c.recv) {
		c.c.recv[kind].Add(1)
	}
	return kind, msg, err
}

// TestHowOftenAReplicaWouldCompare counts what crosses a session while somebody
// types, so that crdt#123 can be decided on the rate rather than on a guess.
func TestHowOftenAReplicaWouldCompare(t *testing.T) {
	const keystrokes = 50

	for _, participants := range []int{2, 10, 50} {
		t.Run(fmt.Sprint(participants, " participants"), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			srv := NewServer(Config{Store: NewMemoryStore()})
			defer func() { _ = srv.Close(context.Background()) }()

			counts := make([]*wireCounts, participants)
			clients := make([]*Client, participants)
			for i := range participants {
				counts[i] = &wireCounts{}
				tr, sc := Pipe()
				go func() { _ = srv.ServePipe(ctx, sc) }()
				c, err := Join(ctx, &countingTransport{inner: tr, c: counts[i]},
					ClientConfig{Document: "paper", Site: crdt.SiteID(i + 1)})
				if err != nil {
					t.Fatalf("participant %d: %v", i, err)
				}
				defer func() { _ = c.Close() }()
				clients[i] = c
			}

			writer, err := clients[0].Text("body")
			if err != nil {
				t.Fatal(err)
			}
			// The LAST participant is the one waited on: if it has the text,
			// everybody before it has been offered it.
			watcher, err := clients[participants-1].Text("body")
			if err != nil {
				t.Fatal(err)
			}
			for i := range keystrokes {
				if err := writer.Insert(i, "x"); err != nil {
					t.Fatal(err)
				}
			}
			deadline := time.Now().Add(30 * time.Second)
			for watcher.Len() < keystrokes {
				if time.Now().After(deadline) {
					t.Fatalf("the last of %d participants saw %d of %d keystrokes",
						participants, watcher.Len(), keystrokes)
				}
				select {
				case <-clients[participants-1].Changes():
				case <-time.After(10 * time.Millisecond):
				}
			}
			// Everyone else has been offered it too; give the acknowledgements
			// they send back a moment to arrive, since nothing waits on them.
			time.Sleep(300 * time.Millisecond)

			var acks, opsIn, joins int64
			var mu sync.Mutex
			for _, c := range counts {
				mu.Lock()
				acks += c.sent[kindAcknowledge].Load()
				opsIn += c.recv[kindOperation].Load()
				joins += c.sent[kindJoin].Load()
				mu.Unlock()
			}

			t.Logf("%d participants, %d keystrokes: %d joins, %d operation messages received, %d acknowledgements sent",
				participants, keystrokes, joins, opsIn, acks)
			t.Logf("  acknowledgements per keystroke across the session: %.1f",
				float64(acks)/float64(keystrokes))
			t.Logf("  joins per keystroke: %.3f", float64(joins)/float64(keystrokes))

			// The claim being checked is Client.acknowledge's own: it "runs once
			// per operation received". If that holds, a digest carried on an
			// acknowledgement is computed once per participant per keystroke,
			// which is the same rate as the fan-out itself.
			if acks < opsIn {
				t.Errorf("%d acknowledgements for %d operation messages received: fewer than one each", acks, opsIn)
			}
			// And a join happens once per participant per session, whatever the
			// typing does. That is the rate a walk over the document can afford.
			if joins != int64(participants) {
				t.Errorf("%d joins for %d participants; expected one each", joins, participants)
			}
		})
	}
}
