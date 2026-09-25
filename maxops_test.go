//go:build !js

package collab

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/go-crdt/crdt"
)

// A server may say how much one message is allowed to ask it to reserve.
//
// The transport's size limit bounds the bytes; this bounds what they decode into,
// which is the thing that amplifies. crdt's counted headers already refuse a claim
// larger than the bytes that follow it, so what is left is a message that is honest
// and enormous: the worst claim they permit reserves sixteen to twenty-four times
// the input. See [Config.MaxOperations] and go-crdt/collab#169.
//
// Four cases, and the fourth is the one that connects this to the rest: a refusal an
// operator cannot hear is a refusal only the offending session knows about.
func TestAServerBoundsWhatOneMessageMayReserve(t *testing.T) {
	ops := func(t *testing.T, n int) []byte {
		t.Helper()
		c := crdt.NewComposite(7)
		text, err := c.Text("body")
		if err != nil {
			t.Fatal(err)
		}
		runes := make([]rune, n)
		for i := range runes {
			runes[i] = rune('a' + i%26)
		}
		if _, err := text.Insert(0, string(runes)); err != nil {
			t.Fatal(err)
		}
		raw, err := crdt.AppendPartOps(nil, c.OpsSince(nil))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}

	run := func(t *testing.T, max int, n int) (error, []error) {
		t.Helper()
		var mu sync.Mutex
		var heard []error
		srv := NewServer(Config{
			Store:         NewMemoryStore(),
			MaxOperations: max,
			OnOperationsRefused: func(_ string, _ crdt.SiteID, err error) {
				mu.Lock()
				heard = append(heard, err)
				mu.Unlock()
			},
		})
		t.Cleanup(func() { _ = srv.Close(context.Background()) })
		ctx := context.Background()
		doc, err := srv.open(ctx, "paper")
		if err != nil {
			t.Fatal(err)
		}
		sub, err := doc.enrol(joinMsg{Document: "paper", Site: 7}, false)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { doc.leave(context.WithoutCancel(ctx), sub) })
		applyErr := doc.applyOperations(ctx, sub, ops(t, n))
		mu.Lock()
		defer mu.Unlock()
		return applyErr, append([]error(nil), heard...)
	}

	t.Run("exactly the bound is taken", func(t *testing.T) {
		err, heard := run(t, 8, 8)
		if err != nil {
			t.Errorf("a message of 8 operations under a bound of 8 was refused: %v", err)
		}
		if len(heard) != 0 {
			t.Errorf("the operator was told about an accepted message: %v", heard)
		}
	})

	t.Run("one more is refused, and says which rule refused it", func(t *testing.T) {
		err, _ := run(t, 8, 9)
		if !errors.Is(err, crdt.ErrTooManyOps) {
			t.Fatalf("a message of 9 under a bound of 8 gave %v, want crdt.ErrTooManyOps underneath", err)
		}
	})

	t.Run("zero is no bound, which is what it was before", func(t *testing.T) {
		err, heard := run(t, 0, 2000)
		if err != nil {
			t.Errorf("an unbounded server refused 2000 operations: %v", err)
		}
		if len(heard) != 0 {
			t.Errorf("the operator was told about an accepted message: %v", heard)
		}
	})

	t.Run("the operator hears it, with the cause", func(t *testing.T) {
		_, heard := run(t, 8, 9)
		if len(heard) != 1 {
			t.Fatalf("the hook fired %d times, want once", len(heard))
		}
		if !errors.Is(heard[0], crdt.ErrTooManyOps) {
			t.Errorf("the operator was told %v, want crdt.ErrTooManyOps underneath -- a refusal "+
				"only the offending session can understand is one an operator cannot act on", heard[0])
		}
	})
}
