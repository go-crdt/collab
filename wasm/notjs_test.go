//go:build !(js && wasm)

package main

import (
	"strings"
	"testing"
)

func TestBuiltForTheWrongTargetItSaysSo(t *testing.T) {
	var out strings.Builder
	if got := run(&out); got != 2 {
		t.Errorf("run() = %d, want 2", got)
	}
	if !strings.Contains(out.String(), "GOOS=js GOARCH=wasm") {
		t.Errorf("run() said %q, which does not say how to build it", out.String())
	}
}

func TestMainExitsWithWhatRunReturned(t *testing.T) {
	var code int
	real := osExit
	osExit = func(c int) { code = c }
	defer func() { osExit = real }()
	main()
	if code != 2 {
		t.Errorf("main() exited %d, want 2", code)
	}
}
