//go:build !js

package collab

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A store that cannot write one document and writes the rest.
type oneBadDocumentStore struct {
	*MemoryStore
	bad string
}

func (s oneBadDocumentStore) Save(ctx context.Context, name string, data []byte) error {
	if name == s.bad {
		return errors.New("no space left on device")
	}
	return s.MemoryStore.Save(ctx, name, data)
}

// server.go:386. A flush tells the caller of the documents that failed and
// only of those: a document that was written is not a failure to report.
func TestAFlushReportsOnlyTheDocumentThatFailed(t *testing.T) {
	srv := NewServer(Config{Store: oneBadDocumentStore{MemoryStore: NewMemoryStore(), bad: "bad"}})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	for _, name := range []string{"good", "bad"} {
		transport, conn := Pipe()
		go func() { _ = srv.ServePipe(ctx, conn) }()
		c, err := Join(ctx, transport, ClientConfig{Document: name, Site: 1})
		if err != nil {
			t.Fatal(err)
		}
		body, err := c.Text("body")
		if err != nil {
			t.Fatal(err)
		}
		if err := body.Insert(0, name); err != nil {
			t.Fatal(err)
		}
		arrived(t, ctx, srv, name, name)
		_ = c.Close()
	}

	var mu sync.Mutex
	told := map[string]error{}
	err := srv.flush(ctx, func(document string, err error) {
		mu.Lock()
		told[document] = err
		mu.Unlock()
	})
	if err == nil {
		t.Fatal("a flush with one unwritable document reported success")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(told) != 1 || told["bad"] == nil {
		t.Fatalf("the flush reported %v, want only the document that failed", told)
	}
}

// A carrier that counts what is read from it and can be made to fail what is
// written to it.
type countingCarrier struct {
	ctx     context.Context
	recvs   atomic.Int64
	sendErr error
}

func (c *countingCarrier) Context() context.Context { return c.ctx }

func (c *countingCarrier) Recv() (byte, any, error) {
	switch n := c.recvs.Add(1); {
	case n == 1:
		return kindJoin, joinMsg{Document: "d", Site: 1}, nil
	case n == 2 && c.sendErr == nil:
		return 0, nil, errors.New("the peer went away")
	}
	<-c.ctx.Done()
	return 0, nil, c.ctx.Err()
}

func (c *countingCarrier) Send(byte, any) error { return c.sendErr }

// server.go:765. The goroutine that reads stops at the first error. Read again,
// it would sit on a connection that has already failed.
func TestTheReaderStopsAtTheFirstError(t *testing.T) {
	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	c := &countingCarrier{ctx: ctx}
	if err := srv.session(c); err == nil {
		t.Fatal("a session whose peer went away reported success")
	}
	// Control: the join and the failing read were both made.
	if got := c.recvs.Load(); got < 2 {
		t.Fatalf("the session read %d times, want the join and the error", got)
	}
	time.Sleep(100 * time.Millisecond)
	if got := c.recvs.Load(); got != 2 {
		t.Fatalf("the session read %d times, want 2: the reader went on after an error", got)
	}
}

// server.go:811. A session whose carrier cannot be written to ends with that
// error, at once, rather than reading on until the caller gives up.
func TestASessionEndsWhenItCannotWrite(t *testing.T) {
	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	boom := errors.New("cannot write")
	c := &countingCarrier{ctx: ctx, sendErr: boom}
	start := time.Now()
	err := srv.session(c)
	if !errors.Is(err, boom) {
		t.Fatalf("session = %v, want the write error", err)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("the session took %v to notice it could not write", took)
	}
}

// server.go:917. A participant is counted the moment it joins, not at its first
// acknowledgement: a site nobody has heard from holds collection back.
func TestAJoiningSiteIsCountedAtOnce(t *testing.T) {
	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	c := &scriptedCarrier{ctx: ctx, hangsUp: true, in: []scripted{
		{kind: kindJoin, msg: joinMsg{Document: "d", Site: 7}},
	}}
	_ = srv.session(c)

	srv.mu.Lock()
	d := srv.docs["d"]
	srv.mu.Unlock()
	if d == nil {
		t.Fatal("the document is gone")
	}
	d.mu.Lock()
	_, known := d.seen[7]
	d.mu.Unlock()
	if !known {
		t.Fatal("a site that joined is not among those the server is waiting to hear from")
	}
}
