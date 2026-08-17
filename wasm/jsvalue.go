//go:build js && wasm

package main

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"syscall/js"

	"github.com/go-crdt/crdt"
)

// Everything a page touches crosses this file, and four rules hold across all
// of it.
//
// # A failure is a rejected promise
//
// Not a null, not a false, not a sentinel. A page that ignores a failure has to
// hear about it, and an ignored rejection is reported by every runtime there is.
//
// It is a promise rather than a thrown Error because a Go function cannot throw
// one. syscall/js hands JavaScript a wrapper built by wasm_exec.js, and that
// wrapper is `go._pendingEvent = event; go._resume(); return event.result` —
// there is no path in it by which the Go side can raise. So the rule became:
// anything that can fail answers with a promise, and anything that cannot
// answers with the value itself. Work that does not block is still done before
// the promise is handed back, so ordering and latency are exactly what a
// synchronous call would have given.
//
// The one thing that does throw is a session used after it was closed, and it
// throws because the objects are revocable proxies and closing revokes them.
// That is a real synchronous exception on any use at all, read or write, which
// is what a page that has closed a document and kept using it deserves.
//
// # Offsets are UTF-16 code units
//
// Always, and never runes. A JavaScript string is UTF-16 and a page counts in
// it. An offset that splits a character is refused, not rounded.
//
// # A value is bytes
//
// A map value and a list value are Uint8Array in both directions. This package
// does not interpret them, so the binding must not either — no strings, no JSON.
//
// # A 64-bit number is a decimal string
//
// A site identity and an anchor's sequence number are uint64, and a JavaScript
// number holds 53 bits of integer. They are accepted as a number, a bigint or a
// string alike, and always reported back as a decimal string, which survives
// JSON, comparison and 2^53.

var (
	global      = js.Global()
	objectCtor  = global.Get("Object")
	uint8Ctor   = global.Get("Uint8Array")
	errorCtor   = global.Get("Error")
	promiseCtor = global.Get("Promise")
	proxyCtor   = global.Get("Proxy")
	stringCtor  = global.Get("String")
	console     = global.Get("console")
)

// A js.Func that is never released keeps its Go closure alive for the life of
// the page, and with it everything the closure reaches — for a session, the
// whole document. A page opens one session per document, so getting this wrong
// leaks a document at a time. Every function this program hands JavaScript is
// counted, and collab.stats() reports the count, so that a test can insist
// closing a session gives every one of them back.
var (
	funcsMu   sync.Mutex
	liveFuncs int
)

func jsFunc(fn func(js.Value, []js.Value) any) js.Func {
	funcsMu.Lock()
	liveFuncs++
	funcsMu.Unlock()
	return js.FuncOf(fn)
}

func dropFunc(f js.Func) {
	f.Release()
	funcsMu.Lock()
	liveFuncs--
	funcsMu.Unlock()
}

func funcCount() int {
	funcsMu.Lock()
	defer funcsMu.Unlock()
	return liveFuncs
}

// A funcSet is every function one object handed JavaScript, released together
// when that object is done with.
type funcSet []js.Func

// method sets obj.name to a function. this is not passed on: nothing here reads
// it, so a method taken off its object still works until the session ends.
func (s *funcSet) method(obj js.Value, name string, fn func([]js.Value) any) {
	f := jsFunc(func(_ js.Value, args []js.Value) any { return fn(args) })
	*s = append(*s, f)
	obj.Set(name, f)
}

// getter defines obj.name as a property that is read rather than called, which
// is what `body.length` has to be.
func (s *funcSet) getter(obj js.Value, name string, fn func() any) {
	f := jsFunc(func(js.Value, []js.Value) any { return fn() })
	*s = append(*s, f)
	objectCtor.Call("defineProperty", obj, name, map[string]any{
		"get": f, "enumerable": true, "configurable": true,
	})
}

func (s *funcSet) release() {
	for _, f := range *s {
		dropFunc(f)
	}
	*s = nil
}

// revocable wraps obj so that it can be switched off. A page holding a closed
// session then fails on the next thing it does with it — synchronously, on a
// read as much as on a write — rather than reading a document that is no longer
// being kept in step with anybody.
func revocable(obj js.Value) (proxy, revoke js.Value) {
	got := proxyCtor.Call("revocable", obj, objectCtor.New())
	return got.Get("proxy"), got.Get("revoke")
}

// jsError renders a Go error as an Error a page can catch. The name is set so
// that a page can tell a failure from this package from one of its own without
// reading the message.
func jsError(err error) js.Value {
	e := errorCtor.New(err.Error())
	e.Set("name", "CollabError")
	return e
}

// reject answers with a promise already rejected.
func reject(err error) js.Value { return promiseCtor.Call("reject", jsError(err)) }

// settle does the work now and answers with a promise already settled, so that
// a failure can be reported without the call becoming asynchronous in any way
// that a caller could observe.
func settle(work func() (any, error)) js.Value {
	v, err := work()
	if err != nil {
		return reject(err)
	}
	return promiseCtor.Call("resolve", v)
}

// async runs work on a goroutine of its own and answers with a promise that
// settles when it finishes.
//
// It is for work that waits. A js.Func that blocks blocks the page's event
// loop — syscall/js says so outright — and anything waiting on a WebSocket is
// waiting on that same event loop, so a synchronous join would wait for a
// message that cannot arrive until it returns.
func async(work func() (any, error)) js.Value {
	var exec js.Func
	exec = jsFunc(func(_ js.Value, args []js.Value) any {
		resolve, rejectWith := args[0], args[1]
		go func() {
			v, err := work()
			if err != nil {
				rejectWith.Invoke(jsError(err))
			} else {
				resolve.Invoke(v)
			}
			dropFunc(exec)
		}()
		return nil
	})
	return promiseCtor.New(exec)
}

var errNotBytes = errors.New("collab: a value must be a Uint8Array")

// toBytes copies a value in. js.CopyBytesToGo wants a Uint8Array — an
// ArrayBuffer is not one, and neither is a Blob — so the type is checked here
// rather than left to panic inside the copy.
func toBytes(v js.Value) ([]byte, error) {
	if !v.InstanceOf(uint8Ctor) {
		return nil, errNotBytes
	}
	b := make([]byte, v.Length())
	js.CopyBytesToGo(b, v)
	return b, nil
}

// fromBytes copies a value out, as the same thing it was handed in as.
func fromBytes(b []byte) js.Value {
	v := uint8Ctor.New(len(b))
	js.CopyBytesToJS(v, b)
	return v
}

// arg reads one argument, treating one that was not passed as undefined rather
// than as a reason to panic.
func arg(args []js.Value, i int) js.Value {
	if i < len(args) {
		return args[i]
	}
	return js.Undefined()
}

func argString(args []js.Value, i int, what string) (string, error) {
	v := arg(args, i)
	if v.Type() != js.TypeString {
		return "", fmt.Errorf("collab: %s must be a string", what)
	}
	return v.String(), nil
}

func argInt(args []js.Value, i int, what string) (int, error) {
	return valueInt(arg(args, i), what)
}

func valueInt(v js.Value, what string) (int, error) {
	if v.Type() != js.TypeNumber {
		return 0, fmt.Errorf("collab: %s must be a number", what)
	}
	f := v.Float()
	n := int(f)
	// NaN and a fraction both fail this, which is what refuses 1.5 and "half of
	// an offset the caller computed by dividing something".
	if float64(n) != f {
		return 0, fmt.Errorf("collab: %s must be a whole number", what)
	}
	return n, nil
}

func argBytes(args []js.Value, i int) ([]byte, error) {
	return toBytes(arg(args, i))
}

// parseUint64 reads a 64-bit number however it was written. String() renders a
// number, a bigint and a string the same way, so all three are accepted through
// one path and none of them can lose a digit on the way.
func parseUint64(v js.Value, what string) (uint64, error) {
	if v.IsUndefined() || v.IsNull() {
		return 0, fmt.Errorf("collab: %s is missing", what)
	}
	s := stringCtor.Invoke(v).String()
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("collab: %s is %q, which is not a 64-bit number", what, s)
	}
	return n, nil
}

func uint64Value(n uint64) string { return strconv.FormatUint(n, 10) }

// anchorValue renders an anchor as a plain object a page can store and hand
// back — beside a comment, in local storage, in a URL — and JSON can carry.
func anchorValue(id crdt.ID) any {
	return map[string]any{
		"site": uint64Value(uint64(id.Site)),
		"seq":  uint64Value(id.Seq),
	}
}

func parseAnchor(v js.Value) (crdt.ID, error) {
	if v.Type() != js.TypeObject {
		return crdt.ID{}, errors.New("collab: an anchor must be the object anchor() returned")
	}
	site, err := parseUint64(v.Get("site"), "an anchor's site")
	if err != nil {
		return crdt.ID{}, err
	}
	seq, err := parseUint64(v.Get("seq"), "an anchor's seq")
	if err != nil {
		return crdt.ID{}, err
	}
	return crdt.ID{Site: crdt.SiteID(site), Seq: seq}, nil
}

// parseMeta reads what a page wants shown beside a cursor. It is strings to
// strings, and nothing here interprets it.
func parseMeta(v js.Value) (map[string]string, error) {
	if v.IsUndefined() || v.IsNull() {
		return nil, nil
	}
	if v.Type() != js.TypeObject {
		return nil, errors.New("collab: cursor metadata must be an object of strings")
	}
	keys := objectCtor.Call("keys", v)
	meta := make(map[string]string, keys.Length())
	for i := range keys.Length() {
		key := keys.Index(i).String()
		value := v.Get(key)
		if value.Type() != js.TypeString {
			return nil, fmt.Errorf("collab: cursor metadata %q must be a string", key)
		}
		meta[key] = value.String()
	}
	return meta, nil
}

// stringValues renders a Go slice as a JavaScript array. js.ValueOf takes []any
// and nothing more specific, so the conversion has to be spelled out.
func stringValues(in []string) []any {
	out := make([]any, len(in))
	for i, s := range in {
		out[i] = s
	}
	return out
}
