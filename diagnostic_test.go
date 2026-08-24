//go:build !js

package collab_test

import (
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/collab"
)

// The message a timeout produces has to name what the document actually holds,
// whatever parts the test uses. It used to read one hard-coded part name.
func TestATimeoutSaysWhatTheDocumentHeld(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	c, err := collab.Join(t.Context(), collab.GRPC(conn),
		collab.ClientConfig{Document: "d", Site: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// Parts with names nothing in this file hard-codes.
	body, err := c.Text("file:main.tex")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, "chapter one"); err != nil {
		t.Fatal(err)
	}
	chat, err := c.List("chat")
	if err != nil {
		t.Fatal(err)
	}
	if err := chat.Append([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	cells, err := c.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	if err := cells.Set("A1", []byte("1")); err != nil {
		t.Fatal(err)
	}

	// A part nothing has been written to is not in the version, so it does not
	// appear — which is a part holding nothing, not a part going missing.
	if _, err := c.Text("never-written"); err != nil {
		t.Fatal(err)
	}

	got := describe(t, c)
	for _, want := range []string{`text "file:main.tex": "chapter one"`, `list "chat"`, `map "cells"`, "peers:"} {
		if !strings.Contains(got, want) {
			t.Errorf("the description does not mention %s:\n%s", want, got)
		}
	}
	if strings.Contains(got, "never-written") {
		t.Errorf("a part nothing was written to is described:\n%s", got)
	}
	t.Logf("what a timeout would now report:\n%s", got)
	_ = time.Second
}
