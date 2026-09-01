//go:build !js

package collab

import (
	"context"
	"net"
	"testing"

	"github.com/go-crdt/collab/collabpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// The advertisement survives a real gRPC round trip.
//
// Everything else about it is tested against document.join and the encoders
// directly, which would all go on passing if the protobuf conversion dropped the
// field -- and gRPC is the only transport carrying it today, so the feature
// would be silently absent exactly where it is supposed to work.
//
// What is observed is the behaviour rather than the bytes: a peer that says it
// reads our formats is sent a snapshot, and one that says nothing is sent the
// history. If the field were lost in conversion both would get the history.
func TestTheAdvertisementSurvivesGRPC(t *testing.T) {
	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	collabpb.RegisterCollabServer(gs, GRPCService(srv))
	go func() { _ = gs.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///collab",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		gs.Stop()
		_ = lis.Close()
	})

	speaks, _ := Mine().MarshalBinary()
	for _, c := range []struct {
		name     string
		site     uint64
		speaks   []byte
		snapshot bool
	}{
		{"a peer that says what it reads", 2, speaks, true},
		{"a peer that says nothing", 3, nil, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			carrier, err := GRPC(conn).open(ctx)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer carrier.Close()

			if err := carrier.Send(kindJoin, joinMsg{
				Document: "d", Site: c.site, Speaks: c.speaks,
			}); err != nil {
				t.Fatalf("Send: %v", err)
			}
			kind, msg, err := carrier.Recv()
			if err != nil {
				t.Fatalf("Recv: %v", err)
			}
			if kind != kindWelcome {
				t.Fatalf("got kind %d, want a welcome", kind)
			}
			w := msg.(welcomeMsg)
			got := "operations"
			if len(w.Snapshot) > 0 {
				got = "snapshot"
			}
			want := "operations"
			if c.snapshot {
				want = "snapshot"
			}
			if got != want {
				t.Fatalf("the welcome carried %s, want %s -- the advertisement did not survive gRPC", got, want)
			}
			// And the server's own half comes back through the same conversion.
			if len(w.Speaks) == 0 {
				t.Fatal("the server's advertisement did not survive gRPC")
			}
			var said Capabilities
			if err := said.UnmarshalBinary(w.Speaks); err != nil {
				t.Fatalf("what came back does not decode: %v", err)
			}
			if !said.Accepts(CapComposite, 1) {
				t.Error("the server's advertisement came back changed")
			}
		})
	}
}
