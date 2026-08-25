//go:build !js

package collab

import (
	"context"
	"encoding/binary"
	"runtime"
	"testing"
	"time"
)

// The decoders were fuzzed and the server was not. A decoder is half the
// surface: what a peer actually controls is the sequence of messages a session
// is driven with, and every value in them that the decoder was willing to
// accept.
//
// So this drives a live server. The bytes go through the real decoder, because
// only what the decoder accepts can reach the server, and what comes out is
// replayed into Server.session exactly as a connection would. What is asserted
// is not that the server liked it — it is free to refuse anything — but that
// afterwards it is still a server: a well-formed participant can join, edit,
// and read back what it wrote.

// frames splits a fuzzer's bytes into length-prefixed chunks, so that one input
// is a whole conversation rather than a single message.
func frames(data []byte) [][]byte {
	var out [][]byte
	for len(data) >= 2 {
		n := int(binary.BigEndian.Uint16(data[:2]))
		data = data[2:]
		if n > len(data) {
			n = len(data)
		}
		out = append(out, data[:n])
		data = data[n:]
		if len(out) > 64 {
			break // a conversation, not a denial of service against the fuzzer
		}
	}
	return out
}

// stillAServer is the assertion that matters: whatever just happened, a
// participant arriving now can do the thing this package exists for.
func stillAServer(t *testing.T, srv *Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	transport, conn := Pipe()
	go func() { _ = srv.ServePipe(ctx, conn) }()
	c, err := Join(ctx, transport, ClientConfig{Document: "afterwards", Site: 4242})
	if err != nil {
		t.Fatalf("a participant could not join after hostile input: %v", err)
	}
	defer func() { _ = c.Close() }()
	body, err := c.Text("body")
	if err != nil {
		t.Fatalf("Text after hostile input: %v", err)
	}
	if err := body.Insert(0, "still here"); err != nil {
		t.Fatalf("Insert after hostile input: %v", err)
	}
	if got := body.String(); got != "still here" {
		t.Fatalf("the document reads %q after hostile input", got)
	}
}

func FuzzHostileSession(f *testing.F) {
	// Seeds: a well-formed conversation, and the shapes worth starting from.
	add := func(msgs ...[]byte) {
		var data []byte
		for _, m := range msgs {
			data = binary.BigEndian.AppendUint16(data, uint16(len(m)))
			data = append(data, m...)
		}
		f.Add(data)
	}
	join, _ := encodeClient(kindJoin, joinMsg{Document: "d", Site: 1})
	ops, _ := encodeClient(kindOperation, opsMsg{Operations: []byte("o")})
	pres, _ := encodeClient(kindPresence, presenceMsg{Update: []byte("u")})
	add(join, ops, pres)
	add(ops, join)           // operations before a join
	add(join, join)          // two joins on one session
	add(pres)                // presence and nothing else
	add(join, ops, ops, ops) // a burst
	f.Add([]byte{0, 1, 0})   // a frame of one zero byte

	f.Fuzz(func(t *testing.T, data []byte) {
		srv := NewServer(Config{Store: NewMemoryStore()})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var msgs []scripted
		for _, frame := range frames(data) {
			kind, msg, err := decodeClient(frame)
			if err != nil {
				continue // only what the decoder accepts can reach the server
			}
			msgs = append(msgs, scripted{kind: kind, msg: msg})
		}
		if len(msgs) == 0 {
			return
		}
		// The server is free to refuse this in any way it likes. What it may
		// not do is panic, wedge, or stop being a server.
		_ = srv.session(&scriptedCarrier{ctx: ctx, in: msgs, hangsUp: true})

		stop, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		defer func() { _ = srv.Close(stop) }()
		stillAServer(t, srv)
	})
}

// A session that hangs up mid-conversation must not leave anything running.
//
// A goroutine per abandoned session would be a leak nobody sees until a server
// that has been up for a week stops answering, which is exactly the kind of
// failure this audit is for.
func TestAnAbandonedSessionLeavesNothingRunning(t *testing.T) {
	srv := NewServer(Config{Store: NewMemoryStore()})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	join, _ := encodeClient(kindJoin, joinMsg{Document: "d", Site: 1})
	kind, msg, err := decodeClient(join)
	if err != nil {
		t.Fatal(err)
	}

	settle := func() int {
		for i := 0; i < 50; i++ {
			runtime.GC()
			time.Sleep(10 * time.Millisecond)
		}
		return runtime.NumGoroutine()
	}
	before := settle()

	for i := 0; i < 200; i++ {
		_ = srv.session(&scriptedCarrier{ctx: ctx, in: []scripted{{kind: kind, msg: msg}}, hangsUp: true})
	}
	after := settle()

	// Some slack for the runtime's own goroutines; a leak of one per session
	// would be two hundred.
	if after-before > 20 {
		t.Fatalf("200 abandoned sessions left %d goroutines behind (%d -> %d)", after-before, before, after)
	}
	t.Logf("200 abandoned sessions: %d goroutines before, %d after", before, after)
}
