//go:build !js

package collab_test

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/collab/collabpb"
	"github.com/go-crdt/crdt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// How many participants one server holds, and what each costs.
//
// The question this answers is whether clustering is a necessity or an
// elegance. A server that holds a few hundred participants on one document
// needs a cluster to serve a busy project; one that holds tens of thousands
// needs one only for latency and for surviving the loss of a site, which is a
// different thing to build.
//
// It is measured on one document on purpose. Participants on different
// documents share almost nothing — a document is its own lock, its own
// subscriber set, its own replica — so the interesting limit is the one where
// they contend: every edit by any of them is fanned out to all the others.
//
// What it found, on an Apple M4 Max over an in-memory connection:
//
//	participants   ns/participant/edit   bytes/participant
//	          10                 5 636             237 906
//	         100                 2 313              28 305
//	        1000                 2 444               3 076
//
// The marginal cost settles at about 3 KB and 2.4 µs a participant. Both are
// flat from a hundred upwards, which says the fan-out is linear and nothing
// about it degrades with the number watching.
//
// In load: one core sustains roughly 400 000 participant-edits a second. A
// document with a thousand people watching and five of them typing at ten
// keystrokes a second is 12% of a core; ten thousand watching and five typing
// is 122%. So a single server is not the limit for a document people read and
// a few people write, which is what a document is.
//
// That is worth stating plainly because it decides what a cluster would be
// for. Not capacity: latency, and surviving the loss of a site. Those are
// answered by putting a replica near each participant, not by splitting a
// document across servers.
//
// The connection here is in memory, so the numbers are the server's own cost
// and not a network's. A real one adds its own, and it adds it per participant
// in the same shape.

// serveBench is serve, for a benchmark rather than a test.
func serveBench(b *testing.B, cfg collab.Config) (*collab.Server, *grpc.ClientConn) {
	b.Helper()
	srv := collab.NewServer(cfg)
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	collabpb.RegisterCollabServer(gs, collab.GRPCService(srv))
	go func() { _ = gs.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///collab",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}))
	if err != nil {
		b.Fatalf("NewClient: %v", err)
	}
	b.Cleanup(func() {
		_ = conn.Close()
		gs.Stop()
		_ = lis.Close()
	})
	return srv, conn
}

// BenchmarkFanOut measures one edit reaching n participants: the cost the
// server pays per participant, per keystroke, on the document they share.
func BenchmarkFanOut(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprint(n, " participants"), func(b *testing.B) {
			_, conn := serveBench(b, collab.Config{Store: collab.NewMemoryStore()})
			ctx := b.Context()

			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)

			clients := make([]*collab.Client, 0, n)
			for i := range n {
				c, err := collab.Join(ctx, collab.GRPC(conn), collab.ClientConfig{
					Document: "one", Site: crdtSite(i + 1),
				})
				if err != nil {
					b.Fatalf("participant %d: %v", i, err)
				}
				clients = append(clients, c)
			}
			b.Cleanup(func() {
				for _, c := range clients {
					_ = c.Close()
				}
			})

			runtime.GC()
			runtime.ReadMemStats(&after)
			held := int64(after.HeapAlloc) - int64(before.HeapAlloc)
			perParticipant := float64(held) / float64(n)

			writer, err := clients[0].Text("body")
			if err != nil {
				b.Fatal(err)
			}
			// The last participant is the one watched: if it has seen the
			// edit, everybody before it has been offered it.
			watcher, err := clients[n-1].Text("body")
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := range b.N {
				if err := writer.Insert(writer.Len(), "x"); err != nil {
					b.Fatal(err)
				}
				deadline := time.After(30 * time.Second)
				for watcher.Len() <= i {
					select {
					case <-clients[n-1].Changes():
					case <-deadline:
						b.Fatalf("edit %d never reached the last of %d participants", i, n)
					}
				}
			}
			b.StopTimer()
			b.ReportMetric(perParticipant, "B/participant")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n), "ns/participant/edit")
		})
	}
}

func crdtSite(i int) crdt.SiteID { return crdt.SiteID(i) }
