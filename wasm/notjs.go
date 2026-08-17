//go:build !(js && wasm)

package main

import (
	"fmt"
	"io"
	"os"
)

// This program is the browser binding and nothing else, so built for anything
// but WebAssembly it says so and stops. It exists in this build at all because
// `go build ./...` on a native machine has to have something here to build —
// and because the mirror, which is the part of the binding worth testing
// without a browser, is compiled and tested in it.

var osExit = os.Exit

func main() { osExit(run(os.Stderr)) }

func run(w io.Writer) int {
	fmt.Fprintln(w, "collab/wasm is the browser binding: build it with GOOS=js GOARCH=wasm.")
	return 2
}
