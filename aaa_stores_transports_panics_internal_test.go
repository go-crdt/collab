//go:build !js

package collab

import (
	"context"
	"errors"
	"testing"

	"github.com/go-crdt/crdt"
	"google.golang.org/grpc"
)

// noPanicS runs f and turns a panic into a failed assertion. These guards used
// to be held only because deleting one made the code after it dereference nil
// and the whole suite die; that is a crash, not an assertion.
func noPanicS(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("%s panicked: %v", what, r)
		}
	}()
	f()
}

func TestMergeSnapshotsRefusesUnreadableSidesWithoutPanicking(t *testing.T) {
	good := crdt.NewComposite(1).Snapshot()
	garbage := []byte("not a snapshot at all")
	noPanicS(t, "unreadable ours", func() {
		out, err := MergeSnapshots(garbage, good)
		if err == nil || out != nil {
			t.Errorf("unreadable ours: got %d bytes and %v", len(out), err)
		}
	})
	noPanicS(t, "unreadable theirs", func() {
		out, err := MergeSnapshots(good, garbage)
		if err == nil || out != nil {
			t.Errorf("unreadable theirs: got %d bytes and %v", len(out), err)
		}
	})
}

func TestMergeSnapshotsRefusesUnmergeableSidesWithoutPanicking(t *testing.T) {
	opening := crdt.NewComposite(1)
	body, err := opening.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, "world"); err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, "hello "); err != nil {
		t.Fatal(err)
	}
	shared := opening.Snapshot()
	discarding := func(site crdt.SiteID, at, count int) []byte {
		t.Helper()
		held, err := crdt.LoadComposite(site, shared)
		if err != nil {
			t.Fatal(err)
		}
		text, err := held.Text("body")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := text.Delete(at, count); err != nil {
			t.Fatal(err)
		}
		if n := text.Purge(); n != count {
			t.Fatalf("purged %d characters, want %d", n, count)
		}
		return held.Snapshot()
	}
	left, right := discarding(2, 0, 6), discarding(3, 6, 5)
	noPanicS(t, "unmergeable sides", func() {
		out, err := MergeSnapshots(left, right)
		if !errors.Is(err, ErrUnmergeable) || out != nil {
			t.Errorf("unmergeable sides: got %d bytes and %v", len(out), err)
		}
	})
}

func TestUnpackSnapshotRefusesShortChecksumsWithoutPanicking(t *testing.T) {
	for _, magic := range [][]byte{checkedRawMagic[:], checkedMagic[:]} {
		for n := 0; n < 4; n++ {
			stored := append(append([]byte(nil), magic...), make([]byte, n)...)
			noPanicS(t, "short checksummed document", func() {
				out, err := UnpackSnapshot(stored)
				if err == nil || out != nil {
					t.Errorf("%d body bytes: got %d bytes and %v", n, len(out), err)
				}
			})
		}
	}
}

func TestEnableUndoOnAClientWithoutHistoryDoesNotPanic(t *testing.T) {
	c := &Client{site: 1, doc: crdt.NewComposite(1)}
	h, err := c.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	noPanicS(t, "EnableUndo", func() {
		h.EnableUndo()
		if _, ok := c.undo["body"]; !ok {
			t.Error("EnableUndo recorded no history")
		}
	})
}

// refusingConn is a gRPC connection that cannot open a stream.
type refusingConn struct{ err error }

func (r refusingConn) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	return r.err
}

func (r refusingConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, r.err
}

func TestGRPCOpenReportsAStreamThatCannotBeOpened(t *testing.T) {
	want := errors.New("no stream")
	noPanicS(t, "open", func() {
		c, err := (&grpcTransport{conn: refusingConn{want}}).open(context.Background())
		if !errors.Is(err, want) || c != nil {
			t.Errorf("got %v and %v, want the refusal and no carrier", c, err)
		}
	})
}

func TestGRPCSendRefusesTheWrongMessageTypeWithoutPanicking(t *testing.T) {
	for _, kind := range []byte{kindJoin, kindOperation, kindPresence, kindAcknowledge, 0xff} {
		noPanicS(t, "Send", func() {
			if err := (&grpcConn{}).Send(kind, "not a message"); !errors.Is(err, ErrProtocol) {
				t.Errorf("kind %d: got %v, want ErrProtocol", kind, err)
			}
		})
	}
}

func TestAsStatusPassesPlainErrorsThrough(t *testing.T) {
	plain := errors.New("plain")
	noPanicS(t, "asStatus", func() {
		if got := asStatus(plain); got != plain {
			t.Errorf("got %v, want the error unchanged", got)
		}
	})
}

func TestFollowWithRetryRefusesWhatCannotBeRetriedWithoutPanicking(t *testing.T) {
	srv := NewServer(Config{})
	noPanicS(t, "nil dialler", func() {
		err := srv.FollowWithRetry(context.Background(), nil, "doc", 1, RetryPolicy{})
		if err == nil {
			t.Error("a nil dialler was accepted")
		}
	})
	noPanicS(t, "refused site", func() {
		dial := func(context.Context) (Transport, error) { return nil, errors.New("unreachable") }
		err := srv.FollowWithRetry(context.Background(), dial, "", 0, RetryPolicy{})
		if err == nil {
			t.Error("an unfollowable document was accepted")
		}
	})
}

func TestAttemptReportsADiallerThatReturnedNothingWithoutPanicking(t *testing.T) {
	srv := NewServer(Config{})
	dial := func(context.Context) (Transport, error) { return nil, nil }
	link, err := srv.reconnecting(dial, "doc", 1, RetryPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	noPanicS(t, "attempt", func() {
		if err := link.attempt(context.Background(), func() {}); err == nil {
			t.Error("a dialler returning no transport was accepted")
		}
	})
}

func TestClosingANormalSessionIsANormalClosure(t *testing.T) {
	noPanicS(t, "closing", func() {
		code, reason := closing(nil)
		if code != 1000 || reason != "" {
			t.Errorf("got %d %q, want 1000 and no reason", code, reason)
		}
	})
}

func TestWSCarrierSendRefusesWhatCannotBeEncodedWithoutPanicking(t *testing.T) {
	noPanicS(t, "Send", func() {
		if err := (&wsCarrier{ctx: context.Background()}).Send(0xff, nil); !errors.Is(err, ErrProtocol) {
			t.Errorf("got %v, want ErrProtocol", err)
		}
	})
}

func TestWebSocketOpenReportsADialFailureWithoutPanicking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	noPanicS(t, "open", func() {
		c, err := (&wsTransport{url: "ws://127.0.0.1:1/"}).open(ctx)
		if !errors.Is(err, ErrTransport) || c != nil {
			t.Errorf("got %v and %v, want ErrTransport and no carrier", c, err)
		}
	})
}
