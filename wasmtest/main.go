//go:build js && wasm

// Command wasmtest is the browser half of the end-to-end proof. It joins one
// document twice, as two independent participants, over the real WebSocket
// transport and the real client in this module — compiled to WebAssembly — edits
// concurrently with a native participant on the other side, and reports the text
// each of its replicas ended up with.
//
// It is driven by run.mjs, which is driven by TestWasmConverges.
package main

import (
	"context"
	"syscall/js"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
	wstransport "github.com/grpc-transports/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// total is how many characters the document holds once everyone has typed: one
// from the native participant and one from each of the two here.
const total = 3

func main() {
	text, err := run()
	result := map[string]any{"ok": err == nil}
	if err != nil {
		result["msg"] = err.Error()
	} else {
		result["first"] = text[0]
		result["second"] = text[1]
	}
	js.Global().Set("__collabResult", result)
}

func run() ([2]string, error) {
	var texts [2]string
	url := js.Global().Get("__collabURL").String()
	document := js.Global().Get("__collabDocument").String()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opt, err := wstransport.DialOption(url, wstransport.ClientConfig{})
	if err != nil {
		return texts, err
	}
	conn, err := grpc.NewClient("passthrough:///collab",
		grpc.WithTransportCredentials(insecure.NewCredentials()), opt)
	if err != nil {
		return texts, err
	}
	defer conn.Close()

	// Two participants, two sessions, two replicas — as far as the server is
	// concerned, two separate browser tabs.
	clients := [2]*collab.Client{}
	for i := range clients {
		c, err := collab.Join(ctx, conn, collab.ClientConfig{
			Document: document,
			Site:     crdt.SiteID(i + 2), // the native participant is site 1
		})
		if err != nil {
			return texts, err
		}
		defer c.Close()
		clients[i] = c
	}

	// Both type at the start at the same time, which is the case that needs the
	// merge to agree.
	for i, c := range clients {
		if err := c.Insert(0, string(rune('B'+i))); err != nil {
			return texts, err
		}
	}

	for i, c := range clients {
		if err := await(ctx, c); err != nil {
			return texts, err
		}
		texts[i] = c.Text()
	}
	return texts, nil
}

// await blocks until a participant holds every character, or the context ends.
func await(ctx context.Context, c *collab.Client) error {
	for c.Len() < total {
		select {
		case <-c.Changes():
		case <-c.Done():
			if c.Len() < total {
				return c.Err()
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
