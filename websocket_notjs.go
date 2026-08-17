//go:build !js

package collab

import "net/http"

// WithHTTPHeader sends these headers with the opening handshake, which is where
// a cookie or a bearer token goes when the participant is not a browser.
//
// It does not exist for a browser, because a page cannot put a header on a
// WebSocket handshake — and does not need to, since the browser sends the
// cookies for that origin itself. Code meant to run in both places should let
// the cookie do the work; see Config.Authorize.
func WithHTTPHeader(h http.Header) WebSocketOption {
	return func(t *wsTransport) { t.header = h }
}
