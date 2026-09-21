// Copyright (c) the go-crdt authors.
// SPDX-License-Identifier: BSD-3-Clause

//go:build !js

package collab

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// Two servers over one store lose the earlier save, and this is what that looks
// like.
//
// It is written down as a test because it is the shape somebody reaches for: a
// PostgreSQL two servers can both connect to looks like the way to run two of
// them, and every individual piece behaves as documented. [Store.Save] replaces,
// which it says; a server holds the document in memory while it serves it, which
// it must; and nothing in between is asked to combine them. The result is that
// the later save carries away whatever only the earlier one held, with no error
// anywhere.
//
// The sequential case is fine, and asserting it here is the point of the
// comparison: a server that loads, serves and stops hands the next one
// everything, so a rolling restart or a failover to a cold standby loses
// nothing. What loses is two of them up at once.
//
// The supported answers are [Server.Follow] and a gitstore with a remote; see
// the package documentation.
func TestTwoServersOverOneStoreLoseTheEarlierSave(t *testing.T) {
	t.Run("one after another keeps both", func(t *testing.T) {
		store := NewMemoryStore()
		writeThenStop(t, store, 1, "AAAA")
		writeThenStop(t, store, 2, "BBBB")
		// The second server loaded what the first left, so both are there. Which
		// order they appear in is the CRDT's business and not this test's.
		got := storedBody(t, store)
		for _, want := range []string{"AAAA", "BBBB"} {
			if !strings.Contains(got, want) {
				t.Fatalf("the store holds %q, which is missing %q", got, want)
			}
		}
	})

	t.Run("both at once keeps only the later save", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		store := NewMemoryStore()

		// Both open the document while the store is empty, which is what two
		// servers behind a load balancer do.
		srvA, clientA := openAndWrite(t, ctx, store, 1, "AAAA")
		srvB, clientB := openAndWrite(t, ctx, store, 2, "BBBB")
		t.Cleanup(func() {
			_ = clientA.Close()
			_ = clientB.Close()
			_ = srvA.Close(context.Background())
			_ = srvB.Close(context.Background())
		})
		awaitOperation(t, ctx, srvA, store, "AAAA")

		if err := srvA.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if got := storedBody(t, store); !strings.Contains(got, "AAAA") {
			t.Fatalf("after the first server saved the store holds %q", got)
		}

		if err := srvB.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		got := storedBody(t, store)
		if strings.Contains(got, "AAAA") {
			t.Fatalf("the store holds %q, which still has the first server's work: "+
				"if Save has learned to merge, the package documentation on sharing a "+
				"store is now wrong and should be the thing that changes", got)
		}
		if !strings.Contains(got, "BBBB") {
			t.Fatalf("the store holds %q, wanted the later save", got)
		}
	})
}

// openAndWrite brings up a server over store and puts text in the document.
func openAndWrite(t *testing.T, ctx context.Context, store Store, site crdt.SiteID, text string) (*Server, *Client) {
	t.Helper()
	srv := NewServer(Config{Store: store})
	tr, sc := Pipe()
	go func() { _ = srv.ServePipe(ctx, sc) }()
	client, err := Join(ctx, tr, ClientConfig{Document: "paper", Site: site})
	if err != nil {
		t.Fatal(err)
	}
	body, err := client.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, text); err != nil {
		t.Fatal(err)
	}
	return srv, client
}

// writeThenStop is one server's whole life: open, write, save, stop.
func writeThenStop(t *testing.T, store Store, site crdt.SiteID, text string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv, client := openAndWrite(t, ctx, store, site, text)
	awaitOperation(t, ctx, srv, store, text)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := srv.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// awaitOperation flushes until the store holds text, so the assertions are about
// saving rather than about whether an operation had arrived yet.
func awaitOperation(t *testing.T, ctx context.Context, srv *Server, store Store, text string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if err := srv.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		held, err := store.Load(ctx, "paper")
		if err != nil {
			t.Fatal(err)
		}
		if held != nil && strings.Contains(storedBody(t, store), text) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%q never reached the store", text)
}

// storedBody is the text the store holds for the document, or "" when it holds
// nothing.
func storedBody(t *testing.T, store Store) string {
	t.Helper()
	snapshot, err := store.Load(context.Background(), "paper")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot == nil {
		return ""
	}
	doc, err := crdt.LoadComposite(9, snapshot)
	if err != nil {
		t.Fatalf("the store holds something that will not load: %v", err)
	}
	body, err := doc.Text("body")
	if err != nil {
		t.Fatalf("the stored document has no body: %v", err)
	}
	return body.String()
}
