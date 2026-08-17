//go:build js && wasm

// Command collab/wasm is the JavaScript binding to the collab client: compiled
// to WebAssembly it installs globalThis.collab and hands a page the client's
// surface, so that an editor written in TypeScript can join a document, edit it
// and be told what everybody else did — without calling Go.
//
// The shape of it, and why:
//
//	const session = await collab.join({url, document, site});
//	const body    = await session.text("file:main.tex");
//
//	await session.onChange(parts => { for (const p of parts) apply(p); });
//	const seed = body.toString();   // the same turn: nothing can slip between
//
//	await body.insert(0, "bonjour"); // offsets in UTF-16 code units
//	await session.close();           // gives every callback back
//
// See collab.d.ts beside this file for the whole of it, and the README for what
// a page does with it.
package main

import (
	"context"
	"errors"
	"syscall/js"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// root holds the three functions the page keeps for as long as it is loaded.
// They are never released, which is not the leak the per-session ones would be:
// there is one of them, and it is the door.
var root funcSet

func main() {
	install()
	// The page drives everything from here. Returning would tear the instance
	// down with every session still open on it.
	select {}
}

func install() {
	api := objectCtor.New()
	root.method(api, "join", join)
	root.method(api, "deriveSite", deriveSite)
	root.method(api, "stats", func([]js.Value) any { return stats() })
	global.Set("collab", api)
}

// defaultJoinTimeout bounds a join that neither succeeds nor fails. A socket
// that opens and then says nothing would otherwise leave the promise pending
// for the life of the page, which is the one failure a page cannot see.
const defaultJoinTimeout = 20 * time.Second

func join(args []js.Value) any {
	opts := arg(args, 0)
	if opts.Type() != js.TypeObject {
		return reject(errors.New("collab: join needs {url, document, site}"))
	}
	url := opts.Get("url")
	if url.Type() != js.TypeString {
		return reject(errors.New("collab: join needs a url"))
	}
	document := opts.Get("document")
	if document.Type() != js.TypeString {
		return reject(errors.New("collab: join needs a document name"))
	}
	site, err := parseUint64(opts.Get("site"), "a site")
	if err != nil {
		return reject(err)
	}
	cfg := collab.ClientConfig{Document: document.String(), Site: crdt.SiteID(site)}
	if resume := opts.Get("resume"); !resume.IsUndefined() && !resume.IsNull() {
		if cfg.Resume, err = toBytes(resume); err != nil {
			return reject(err)
		}
	}
	timeout := defaultJoinTimeout
	if given := opts.Get("timeoutMs"); !given.IsUndefined() && !given.IsNull() {
		ms, err := valueInt(given, "timeoutMs")
		if err != nil {
			return reject(err)
		}
		timeout = time.Duration(ms) * time.Millisecond
	}

	return async(func() (any, error) {
		return open(url.String(), cfg, timeout)
	})
}

// open joins, and bounds how long it may take without ending the session it is
// trying to start.
//
// The deadline cannot be a deadline on the context, because that context is the
// session's own: a session opened with one would end when it expired. So the
// timer cancels, and if it fired before the join returned, what came back is a
// session already over and is closed rather than handed out.
func open(url string, cfg collab.ClientConfig, timeout time.Duration) (any, error) {
	ctx, cancel := context.WithCancel(context.Background())
	var timer *time.Timer
	if timeout > 0 {
		timer = time.AfterFunc(timeout, cancel)
	}
	c, err := collab.Join(ctx, collab.WebSocket(url), cfg)
	if err != nil {
		cancel()
		return nil, err
	}
	if timer != nil && !timer.Stop() {
		_ = c.Close()
		cancel()
		return nil, errors.New("collab: joining took longer than the deadline allowed")
	}
	return newSession(c, cancel), nil
}

// deriveSite turns whatever names a participant — a session token, a tab
// identifier — into a replica identity. See [crdt.DeriveSiteID] for what it
// costs on the wire against a small number a server hands out.
func deriveSite(args []js.Value) any {
	return settle(func() (any, error) {
		from, err := argString(args, 0, "something to derive a site from")
		if err != nil {
			return nil, err
		}
		return uint64Value(uint64(crdt.DeriveSiteID([]byte(from)))), nil
	})
}

// stats is what a page — or a test — asks to prove this binding is not leaking.
// funcs is how many callbacks JavaScript is holding on Go's behalf, and closing
// every session must bring it back to what it was before the first one opened.
// rebuilds counts the times a mirror had to be thrown away, and is a bug in this
// package rather than anything a peer can cause: it should stay at zero.
func stats() any {
	return map[string]any{
		"sessions": openSessions,
		"funcs":    funcCount(),
		"rebuilds": rebuilds,
	}
}
