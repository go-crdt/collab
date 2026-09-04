//go:build !js

package collab_test

import (
	"context"
	"strings"
	"testing"

	"github.com/go-crdt/collab"
)

// emptyAnswering is a store that answers a present, zero-length snapshot —
// what a store that does not know the contract does for a torn file — and
// remembers whether anything was saved over it.
type emptyAnswering struct{ saved bool }

func (*emptyAnswering) Load(context.Context, string) ([]byte, error) { return []byte{}, nil }
func (s *emptyAnswering) Save(context.Context, string, []byte) error {
	s.saved = true
	return nil
}

// The server's own last line, for a store somebody else wrote: a present,
// empty snapshot is refused as the torn write it is — by name, so that the
// person reading the error is told what to look at — and nothing is saved
// over it. Over gRPC the wording reaches the participant.
func TestTheServerRefusesAnEmptySnapshotFromAnyStore(t *testing.T) {
	store := &emptyAnswering{}
	srv, conn := serve(t, collab.Config{Store: store})
	_, err := collab.Join(t.Context(), collab.GRPC(conn), collab.ClientConfig{Document: "d", Site: 1})
	if err == nil {
		t.Fatal("the server opened an empty snapshot as a new document")
	}
	if !strings.Contains(err.Error(), "torn write") {
		t.Fatalf("Join: %v, want the torn write named", err)
	}
	if err := srv.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.saved {
		t.Fatal("the server saved over the torn document")
	}
}
