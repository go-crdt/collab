//go:build !js

package collab

import (
	"context"
	"testing"
	"time"
)

// A carrier that reads from a script and discards what is written, so a
// session can be given messages a real client could not send.
type scripted struct {
	kind byte
	msg  any
}

type scriptedCarrier struct {
	ctx  context.Context
	in   []scripted
	at   int
	sent []wireMsg
}

func (c *scriptedCarrier) Context() context.Context { return c.ctx }

func (c *scriptedCarrier) Recv() (byte, any, error) {
	if c.at >= len(c.in) {
		<-c.ctx.Done()
		return 0, nil, c.ctx.Err()
	}
	m := c.in[c.at]
	c.at++
	return m.kind, m.msg, nil
}

func (c *scriptedCarrier) Send(kind byte, msg any) error {
	c.sent = append(c.sent, wireMsg{kind: kind, msg: msg})
	return nil
}

// A message whose kind and body disagree is refused rather than misread.
//
// Neither carrier can produce one: the wire decoder pairs them, and the gRPC
// one converts. It is checked here because the session logic is written against
// a kind and a value now, rather than against a generated union that made the
// pairing impossible to get wrong — and what a type used to guarantee, a test
// has to.
func TestASessionRefusesAKindThatDoesNotMatchItsBody(t *testing.T) {
	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	for _, tt := range []struct {
		name string
		kind byte
		msg  any
	}{
		{"a join that is not a join", kindJoin, "not a message"},
		{"operations that are not operations", kindOperation, "not a message"},
		{"presence that is not presence", kindPresence, 42},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			c := &scriptedCarrier{ctx: ctx, in: []scripted{
				{kind: kindJoin, msg: joinMsg{Document: "doc", Site: 1}},
				{kind: tt.kind, msg: tt.msg},
			}}
			if tt.kind == kindJoin {
				c.in = c.in[1:] // the bad join is the first message
			}
			if err := srv.session(c); err == nil {
				t.Fatal("the session accepted a message whose body did not match its kind")
			}
		})
	}
}
