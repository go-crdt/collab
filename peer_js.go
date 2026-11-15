//go:build js && wasm

package collab

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"syscall/js"
	"time"
)

// The step before [DataChannel]: getting two browsers to the point where a data
// channel exists at all.
//
// # What is missing, and why it is here rather than there
//
// [DataChannel] and [Server.ServeDataChannel] take a channel that is already
// open. Something has to open it, and in a browser that something is the native
// RTCPeerConnection — a page cannot open a UDP socket, and a Go WebRTC stack
// compiled to wasm would carry a second copy of everything the browser already
// has. So a [Peer] is a thin cover over the browser's own connection: it makes
// the offer or the answer, waits for the addresses to be gathered, and hands
// back the channel the two describe.
//
// # One blob each way
//
// The two browsers still have to swap a block of text, and this is built for the
// case where a person carries it — pasted into the chat of the call they are
// already on. So the whole of it is gathered before the blob is handed back:
// non-trickle ICE, one description each way, no second message with the
// addresses in it. The blob is base64 over the connection description's JSON,
// which is one line and survives a chat box that would fold the newlines in the
// SDP itself.
//
// # Who is who
//
// The one that holds the document offers; it creates the channel and plays the
// [Server]. The one that joins answers; its channel arrives on the connection,
// through ondatachannel, rather than being made. Neither is listening for
// anything — this is the same protocol role as everywhere else in the package,
// not a machine that accepts.
//
//	host, _ := collab.NewPeer(collab.PeerConfig{})
//	offer, _ := host.Offer(ctx, "collab")       // paste offer to the guest
//	host.AcceptAnswer(answer)                    // paste the guest's answer back
//	ch, _ := host.DataChannel(ctx)               // open, ready to serve
//	srv.ServeDataChannel(ctx, ch)
//
//	guest, _ := collab.NewPeer(collab.PeerConfig{})
//	answer, _ := guest.Answer(ctx, offer)        // paste answer to the host
//	ch, _ := guest.DataChannel(ctx)
//	collab.Join(ctx, collab.DataChannel(ch), cfg)

// PeerConfig configures a [Peer]. ICEServers is a list of STUN or TURN URLs,
// each the "urls" of one entry in the RTCPeerConnection configuration — for
// example "stun:stun.l.google.com:19302". Empty is a working configuration for
// two browsers on the same network, where no server is needed to discover an
// address that both can reach.
type PeerConfig struct {
	ICEServers []string
	// ICEServersAuth adds STUN/TURN servers that carry long-term credentials —
	// what a TURN relay needs to authenticate and what the plain ICEServers URL
	// list cannot express. Both lists are used together, ICEServers first.
	ICEServersAuth []ICEServerAuth
}

// ICEServerAuth is one STUN/TURN server with long-term credentials. URLs holds
// one or more server URLs that share the same Username and Credential — the form
// a TURN relay needs. It is the credentialed counterpart of a bare ICEServers
// URL, mirroring one { urls, username, credential } entry of an
// RTCPeerConnection's iceServers.
type ICEServerAuth struct {
	URLs       []string
	Username   string
	Credential string
}

// A Peer is one browser's side of a WebRTC connection, from before a channel
// exists until it is open. It wraps the native RTCPeerConnection and is used
// once, by one goroutine: offer or answer, then take the channel, then close.
type Peer struct {
	pc js.Value

	mu     sync.Mutex
	role   string // "", "offerer", or "answerer"
	ch     js.Value
	closed bool
	funcs  []js.Func // callbacks released together on Close
	wires  []wired   // listeners taken off BEFORE those callbacks are released

	onData js.Func
	// incoming carries the answerer's data channel, which it does not make but
	// is handed through ondatachannel once the offer names one.
	incoming chan js.Value
	// quit is closed by Close, so a wait ends when the peer does.
	//
	// ⛔ WITHOUT IT, CLOSE DID NOT INTERRUPT ANYTHING. Checking p.closed after
	// a wait only helps once the wait is over, and a gathering wait ends on one
	// browser event or on the budget — so Close on a stalled connection left a
	// goroutine parked for up to ICEGatherBudget and returned as if it had
	// ended it. Found by a test written to prove the state check, which hung
	// instead: the assertion could not be made without this.
	quit chan struct{}
}

// NewPeer creates a Peer over a fresh RTCPeerConnection. It fails only where
// there is no RTCPeerConnection to create — outside a browser, or in one old
// enough not to have it — which a page can report before it offers to connect.
func NewPeer(cfg PeerConfig) (*Peer, error) {
	ctor := js.Global().Get("RTCPeerConnection")
	if !ctor.Truthy() {
		return nil, fmt.Errorf("%w: this environment has no RTCPeerConnection", ErrTransport)
	}
	p := &Peer{
		pc:       ctor.New(iceConfiguration(cfg)),
		incoming: make(chan js.Value, 1),
		quit:     make(chan struct{}),
	}
	// The answerer's channel arrives here. A buffer of one means the callback
	// never blocks the page's thread waiting for DataChannel to be called.
	p.onData = js.FuncOf(func(_ js.Value, args []js.Value) any {
		select {
		case p.incoming <- args[0].Get("channel"):
		default:
		}
		return nil
	})
	p.listen(p.pc, "datachannel", p.onData)
	return p, nil
}

// Offer plays the holder of the document: it creates the data channel, makes an
// offer, and returns the local description once every address has been gathered.
// Hand the returned blob to the other browser, then give its answer back through
// [Peer.AcceptAnswer].
//
// The channel is created here, before the offer, so that it is part of what the
// offer describes and is ordered — a document that arrived out of order would
// not be the document.
func (p *Peer) Offer(ctx context.Context, label string) (string, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return "", ErrClosed
	}
	if p.role != "" {
		p.mu.Unlock()
		return "", fmt.Errorf("%w: this peer already has a role", ErrTransport)
	}
	p.role = "offerer"
	ch := p.pc.Call("createDataChannel", label, map[string]any{"ordered": true})
	p.ch = ch
	p.mu.Unlock()

	offer, err := p.awaited(ctx, "createOffer", p.pc.Call("createOffer"))
	if err != nil {
		return "", err
	}
	if _, err := p.awaited(ctx, "setLocalDescription(offer)", p.pc.Call("setLocalDescription", offer)); err != nil {
		return "", err
	}
	if err := p.gatherICE(ctx); err != nil {
		return "", err
	}
	return p.localDescription(), nil
}

// AcceptAnswer takes the answer the other browser pasted back and completes the
// offerer's side of the exchange. Only an offerer accepts an answer; a peer that
// has not offered has nothing for the answer to complete.
func (p *Peer) AcceptAnswer(blob string) error {
	p.mu.Lock()
	role, closed := p.role, p.closed
	p.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if role != "offerer" {
		return fmt.Errorf("%w: only a peer that offered can accept an answer", ErrTransport)
	}
	sig, err := decodeSignal(blob)
	if err != nil {
		return err
	}
	// ⛔ THE SIXTH SITE, AND IT WAS THE ONE THAT DID NOT NAME ITSELF. #271 gave
	// five of the six browser promises an operation name, so that a wait which
	// stalls says WHICH call stalled; this one went through bare `await` and
	// stalled anonymously. The comment below is why the CONTEXT is
	// Background — a deliberate choice, still true — and it says nothing about
	// the naming, which is exactly how the gap read as intentional to two
	// reviews. Found by viaonly (go-fleettools#250) on this package, which is
	// what the command was written for.
	//
	// ⛔⛔ AND IT WAS NOT BOUNDED EITHER. `await(context.Background(), …)` leaves
	// the promise as the only way out, so a browser that never settles this call
	// parks AcceptAnswer forever — the #267 family, in a function that cannot
	// take a deadline from its caller because it has no context parameter. The
	// comment that stood here, "setRemoteDescription settles at once in a
	// browser", is an assumption ABOUT THE BROWSER, and #274 is the record of
	// what that assumption is worth: a browser that gathers and never says so.
	// It is true about the context and it was never a reason to be unbounded.
	ctx, cancel := context.WithTimeout(context.Background(), settleBudget)
	defer cancel()
	// The caller's context is still not used — there is none, and there is no
	// call after this one to be blocked — so this is a bound, not a deadline
	// somebody chose.
	_, err = p.awaited(ctx, "setRemoteDescription(answer)", p.pc.Call("setRemoteDescription", describe(sig)))
	return err
}

// Answer plays the browser that joins: it takes the offerer's pasted offer, sets
// it, makes an answer, and returns the local description once every address has
// been gathered. Hand the returned blob back to the offerer. The data channel is
// not made here — it arrives on the connection, and [Peer.DataChannel] hands it
// over once it is open.
func (p *Peer) Answer(ctx context.Context, remoteOffer string) (string, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return "", ErrClosed
	}
	if p.role != "" {
		p.mu.Unlock()
		return "", fmt.Errorf("%w: this peer already has a role", ErrTransport)
	}
	p.role = "answerer"
	p.mu.Unlock()

	sig, err := decodeSignal(remoteOffer)
	if err != nil {
		return "", err
	}
	// ⛔ NAMED (offer), because the same browser method is now waited on at two
	// sites with opposite meanings: here the remote OFFER, and in AcceptAnswer
	// the remote ANSWER. A bare "setRemoteDescription" in an error would not
	// say which side of the exchange stalled — the same reason
	// setLocalDescription carries (offer) and (answer).
	if _, err := p.awaited(ctx, "setRemoteDescription(offer)", p.pc.Call("setRemoteDescription", describe(sig))); err != nil {
		return "", err
	}
	answer, err := p.awaited(ctx, "createAnswer", p.pc.Call("createAnswer"))
	if err != nil {
		return "", err
	}
	if _, err := p.awaited(ctx, "setLocalDescription(answer)", p.pc.Call("setLocalDescription", answer)); err != nil {
		return "", err
	}
	if err := p.gatherICE(ctx); err != nil {
		return "", err
	}
	return p.localDescription(), nil
}

// DataChannel returns the open channel, for handing to [DataChannel] or
// [Server.ServeDataChannel]. It waits: for the answerer, until the channel has
// arrived on the connection; for either side, until its readyState is "open".
// Call it after the exchange — after [Peer.AcceptAnswer] on the offerer, after
// [Peer.Answer] on the answerer — since a channel opens only once both
// descriptions are set.
func (p *Peer) DataChannel(ctx context.Context) (js.Value, error) {
	p.mu.Lock()
	closed, role, ch := p.closed, p.role, p.ch
	p.mu.Unlock()
	if closed {
		return js.Value{}, ErrClosed
	}
	if role == "" {
		return js.Value{}, fmt.Errorf("%w: offer or answer before taking the channel", ErrTransport)
	}

	if !ch.Truthy() {
		select {
		case ch = <-p.incoming:
			p.mu.Lock()
			p.ch = ch
			p.mu.Unlock()
		// ⛔⛔ THE SAME WAIT, THE SAME CLOSE, AND #271 ALL OVER AGAIN IF IT IS
		// LEFT OUT. A review found that making Close end the GATHERING wait
		// left this one and awaitOpen selecting on the context alone: with
		// context.Background() a concurrent Close parks the goroutine FOR EVER,
		// because Close takes the listeners off and releases the callbacks, so
		// nothing will ever write to the channel it is waiting on. Fixing one
		// of three waits and filing the rest is the exact shape of the defect
		// this branch exists to retire.
		case <-p.quit:
			return js.Value{}, ErrClosed
		case <-ctx.Done():
			return js.Value{}, p.stalledAt("datachannel (waiting for the offerer's channel)", ctx.Err())
		}
	}
	return p.awaitOpen(ctx, ch)
}

// awaitOpen resolves once the channel's readyState is "open", or fails if it
// closes or errors first. A channel already open fires no further open event, so
// its state is read before anything is waited for.
func (p *Peer) awaitOpen(ctx context.Context, ch js.Value) (js.Value, error) {
	// ⛔ ASKED BEFORE THE EARLY RETURNS, which no select can cover. Both of
	// this function's `return ch, nil` paths leave without reaching the wait,
	// so p.quit cannot help them: an already-open channel was handed back for a
	// CLOSED peer, and the caller got a session that died later as a transport
	// error instead of ErrClosed. Pre-existing, found by a review of the branch
	// that made every OTHER exit ask — which is how a door gets missed: the ones
	// that look like fast paths are not read as exits at all.
	if err := p.stillOpen(); err != nil {
		return js.Value{}, err
	}
	if ch.Get("readyState").String() == "open" {
		return ch, nil
	}
	settled := make(chan error, 1)
	var once sync.Once
	finish := func(err error) { once.Do(func() { settled <- err }) }

	onOpen := js.FuncOf(func(js.Value, []js.Value) any { finish(nil); return nil })
	onError := js.FuncOf(func(js.Value, []js.Value) any {
		finish(fmt.Errorf("%w: the data channel failed", ErrTransport))
		return nil
	})
	onClose := js.FuncOf(func(js.Value, []js.Value) any { finish(ErrClosed); return nil })
	for event, fn := range map[string]js.Func{"open": onOpen, "error": onError, "close": onClose} {
		p.listen(ch, event, fn)
	}
	p.track(onOpen, onError, onClose)

	// Read once more, in case it opened between the check above and the listener
	// being wired: the event would already be gone.
	if ch.Get("readyState").String() == "open" {
		return ch, nil
	}
	select {
	case err := <-settled:
		if err != nil {
			return js.Value{}, err
		}
		return ch, nil
	// The third of the three waits: Close releases the callbacks that would
	// write to settled, so without this a Close on an unopened channel parks
	// this goroutine for as long as the caller's context lasts -- for ever,
	// when that context has no deadline.
	case <-p.quit:
		return js.Value{}, ErrClosed
	case <-ctx.Done():
		return js.Value{}, p.stalledAt("channel open", ctx.Err())
	}
}

// untilClosed derives a context that ends when [Peer.Close] does, so a wait on
// a browser promise is interrupted by closing the peer and not only by the
// caller's own deadline.
//
// ⛔ The goroutine is bounded by the returned cancel, which every caller defers:
// it ends when the wait ends, whichever way that happens.
func (p *Peer) untilClosed(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-p.quit:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// stalledAt is [Peer.stalled] with the operation named, for the two waits that
// are not promises and so cannot go through [Peer.awaited].
//
// ⛔ THE SEVENTH AND EIGHTH WAITS. #271 named five, this branch named the
// sixth, and a review of the sentence claiming "every wait" found two more:
// DataChannel waiting for an incoming channel, and awaitOpen waiting for one to
// open. Both returned the three state words and no operation. `viaonly` cannot
// see them — they are not calls to `await` — which is worth recording, because a
// static check covers the shape it was given and the claim was broader than the
// check.
// ⛔ NO nil GUARD. Both call sites are inside `case <-ctx.Done():`, where
// ctx.Err() is non-nil by construction, so a guard here could never fire — a
// mutsweep of the file found it surviving its own deletion, and this branch
// removed another unreachable guard for exactly that reason. [Peer.stalled]
// keeps its own, because it IS called where the error may be nil.
func (p *Peer) stalledAt(what string, err error) error {
	return p.stalled(fmt.Errorf("%s: %w", what, err))
}

// stalled says where a connection had got to when the caller gave up.
//
// A deadline on its own says only that nothing happened, which is the least
// useful thing a transport can tell somebody: "checking" is a network that has
// not answered, "failed" is one that answered no, and "new" with no candidates
// is a browser that never offered an address to try. Naming the state turns an
// afternoon of guessing into a sentence.
func (p *Peer) stalled(err error) error {
	if err == nil {
		return nil
	}
	p.mu.Lock()
	pc := p.pc
	p.mu.Unlock()
	if !pc.Truthy() {
		return err
	}
	state := func(name string) string {
		v := pc.Get(name)
		if v.Type() != js.TypeString {
			return "?"
		}
		return v.String()
	}
	return fmt.Errorf("%w (connection %s, ice %s, gathering %s)", err,
		state("connectionState"), state("iceConnectionState"), state("iceGatheringState"))
}

// awaited is await with the connection's state attached, and only when the wait
// ended on the context.
//
// Only then, because a browser that REFUSED an operation has already said what
// was wrong and the states would be noise beside an answer. A caller who gave up
// has been told nothing at all, which is the whole of what stalled is for.
//
// ⛔ It exists because gatherICE was not the only place. #268 named the state at
// the END of the offer path and left the four steps before it bare, and the
// consumer that bounded its call landed in the first of them — createOffer —
// and got back exactly the bare deadline that fix was meant to retire. One
// contract, five implementations, one of them keeping it: the same shape twice
// in the same file, the second time in a fix for the first.
// ⛔ AND IT NAMES THE OPERATION, because stalled does not. stalled reports the
// connection, ice and gathering states, which are the same three words whichever
// step was waiting: with one site that was unambiguous, with five it is not.
// "context deadline exceeded (connection connecting, ice checking, gathering
// gathering)" does not say whether setRemoteDescription or createAnswer was the
// step that never came back, and that is the question the caller is holding.
func (p *Peer) awaited(ctx context.Context, what string, promise js.Value) (js.Value, error) {
	// ⛔⛔ CLOSE HAD TO BE WIRED IN HERE TOO, AND A REVIEW OF THE PROSE FOUND
	// IT. `await`'s select has exactly two cases, the promise and ctx.Done() —
	// no p.quit — so a Close during any of these six waits left the goroutine
	// parked, and with a caller that passed context.Background() it parked for
	// ever. #274 made Close end the GATHERING wait and the two channel waits
	// and left these six; the README then said Close ends a wait in progress,
	// which was true of three waits out of nine. Fixing some and claiming all
	// is the defect this file keeps producing, so the claim and the code are
	// made to agree here rather than in the sentence.
	ctx, cancel := p.untilClosed(ctx)
	defer cancel()
	v, err := await(ctx, promise)
	// ⛔ Close OUTRANKS the deadline, the precedence #274 established: a peer
	// that was closed reports ErrClosed, not the context error that closing
	// produced. Asked BEFORE the naming below, because "setRemoteDescription
	// stalled" about a peer somebody closed is a true sentence that sends the
	// reader to the wrong place.
	if closedErr := p.stillOpen(); closedErr != nil {
		return js.Value{}, closedErr
	}
	// await returns ctx.Err() verbatim on that branch, and ctx.Err() is nil
	// while the context is live — so this names the state in exactly the case it
	// belongs to.
	//
	// ⚠ One direction only, said rather than left implied: if the promise
	// REJECTED at the instant the context ended, await's select has both cases
	// ready and Go picks between them, so the rejection's message can be lost
	// and the wait reported as a stall. That race is await's, it predates this
	// change, and errors.Is cannot sort it out — the earlier wording here
	// claimed it could.
	if err != nil && errors.Is(err, ctx.Err()) {
		return v, p.stalled(fmt.Errorf("%s: %w", what, err))
	}
	return v, err
}

// ICEGatherBudget is how long [Peer.Offer] and [Peer.Answer] wait for a browser
// to report that ICE gathering finished, when the caller's context carries no
// deadline of its own. A caller that set a deadline has already said what it
// will wait, and that answer is used instead of this one.
//
// ⛔ It is a bound on ONE browser event, not on the handshake, and the wait is
// not lost when it expires: if the description gathered an address by then the
// offer goes out with it (go-crdt/collab#267).
//
// ⚠ THIRTY SECONDS, and the earlier number was wrong in a way worth recording.
// It said "ten seconds, the healthy runs' 8-13 s window plus nothing" — and
// 10 < 13, so a healthy-but-slow gathering would have expired and shipped a
// partial address set. The 8-13 s is the consumer's whole handshake, not its
// gathering, and no measurement here splits the two; this is set above the
// slowest healthy run observed so that expiry means "stuck", which is the only
// thing it is allowed to mean.
const ICEGatherBudget = 30 * time.Second

// SettleBudget is how long [Peer.AcceptAnswer] gives the browser to settle
// setRemoteDescription. It exists because AcceptAnswer takes NO context — it
// accepts a pasted blob and has no call after it to be blocked — so there was
// nothing bounding that wait at all, and a browser that never settled it parked
// the caller forever.
//
// ⚠ IT IS NOT A MEASURED NUMBER, and saying so is the point. Nothing here has
// measured how long a browser takes to set a remote answer; it is local work
// with no network in it, so the honest statement is that this is an upper bound
// chosen to sit far above anything healthy. Its only job is to turn a hang into
// an error that names the operation. A bound that is too generous still does
// that; one that is too tight turns a slow machine into a failure, which is the
// mistake [ICEGatherBudget]'s first value made.
const SettleBudget = 30 * time.Second

// settleBudget is what the wait reads, so a test can shorten it. Unexported for
// the same reason as iceGatherBudget below.
var settleBudget = SettleBudget

// iceGatherBudget is what the wait actually reads, so a test can shorten it.
//
// ⛔ AN EXPORTED VAR WAS THE WRONG PRICE, and a review priced it: `const x =
// collab.ICEGatherBudget` stops compiling, which is an API break for a
// coverage problem, and an exported mutable global read from the wait is a data
// race that js/wasm has no detector to catch. The documented default stays a
// const; the shortening happens in-package, where it costs nobody anything.
var iceGatherBudget = ICEGatherBudget

// gatherICE waits for non-trickle ICE to finish, so the description handed back
// carries every address in it and no second message is needed. A connection
// whose gathering is already complete is not waited on.
func (p *Peer) gatherICE(ctx context.Context) error {
	if p.pc.Get("iceGatheringState").String() == "complete" {
		return p.gathered()
	}
	done := make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(done) }) }

	fn := js.FuncOf(func(js.Value, []js.Value) any {
		if p.pc.Get("iceGatheringState").String() == "complete" {
			finish()
		}
		return nil
	})
	p.listen(p.pc, "icegatheringstatechange", fn)
	p.track(fn)

	// ⛔ THE SECOND SIGNAL, and the cheap half of #267. `onicecandidate` with a
	// null candidate is the canonical end-of-gathering event, and the consumer's
	// healthy transcripts log `(end-of-candidates)` on every passing run. One
	// event the browser is not obliged to deliver promptly was the single exit
	// of this wait; two independent ones cost a listener.
	//
	// ⛔⛔ IT ONLY ENDS THE WAIT. IT DOES NOT COUNT. The first version counted
	// candidate events to decide whether the offer was usable, and an
	// independent review found two defects in that, both one-liners and both
	// WORSE than the bug being fixed:
	//
	//  1. a candidate whose `candidate` string is EMPTY is an end-of-candidates
	//     marker for a generation, not an address (MDN, RTCPeerConnection
	//     icecandidate event: null ends the whole gathering, "" ends a
	//     generation and is forwarded to the peer like a candidate). Counting
	//     it meant a connection with no address at all reached expiry with a
	//     count of one and handed back a DEAD offer, silently, where the old
	//     code had at least returned a clear error.
	//  2. gathering starts inside setLocalDescription, which has already
	//     resolved by the time this listener is wired. A browser that delivered
	//     its one candidate early and then stalled is invisible to a counter,
	//     and the offer is thrown away: #267 again, from the other side.
	//
	// A counter cannot be re-read. The description can, so the question "is
	// there an address in this offer" is asked of the SDP at the moment it
	// matters -- see hasCandidate.
	cand := js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) == 0 {
			return nil
		}
		if c := args[0].Get("candidate"); c.IsNull() || c.IsUndefined() {
			finish()
		}
		return nil
	})
	p.listen(p.pc, "icecandidate", cand)
	p.track(cand)

	// ⛔ THERE IS NO RE-READ HERE, AND THE ONE THAT WAS HERE COULD NOT FIRE. It
	// said it caught "a transition that happened in between" — which describes
	// a threaded runtime. Between the read at the top and this point there is
	// only Go code, and no browser task can run while Go runs: the state cannot
	// change under it, so the branch was unreachable for a Peer used as this
	// package documents. A guard nothing can reach is a guard that rots, and
	// keeping it would have meant carrying a fourth copy of the usability rule
	// for a case that cannot happen. Removed on a review's measurement rather
	// than kept on the comment's word.

	// ⛔ A DEFAULT BOUND, because the caller's context is not obliged to carry
	// one and an unbounded wait on a browser event is how #267 cost a consumer
	// ten weeks. A caller that HAS set a deadline has already said what it will
	// wait, and that answer is respected rather than shortened.
	wait := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		wait, cancel = context.WithTimeout(ctx, iceGatherBudget)
		defer cancel()
	}

	select {
	case <-done:
		return p.gathered()
	case <-p.quit:
		return ErrClosed
	case <-wait.Done():
		// ⛔⛔ CLOSED FIRST, and stated as an ORDER rather than left to a
		// nesting. This branch used to ask hasCandidate first and stillOpen
		// only inside it, so a peer CLOSED during the wait whose description
		// carried no address fell past both and was reported as a timeout. The
		// interleaving is ordinary -- wait.Done() and quit can both be ready
		// and Go picks between them -- and it is the one case no test can
		// schedule, which is exactly why the precedence is written here
		// instead of inferred from where the calls sit.
		if err := p.stillOpen(); err != nil {
			return err
		}
		// ⛔ A CANCELLATION IS NOT A TIMEOUT. The caller withdrew, so there is
		// nobody to hand a usable offer to; proceeding would be answering a
		// question that was taken back.
		if errors.Is(ctx.Err(), context.Canceled) {
			return p.stalled(fmt.Errorf("ICE gathering: %w", ctx.Err()))
		}
		// ⭐ Proceed with what the DESCRIPTION carries. In the measured stall a
		// host and a reflexive candidate had both arrived -- the network half
		// was healthy and the offer was usable -- and only "complete" never
		// came. Failing there threw away a working connection over a missing
		// event.
		//
		// ⛔ The same question as every other exit, asked of the same predicate:
		// a second spelling of "is this offer usable" is how #271 happened.
		// Only the FAILING sentence differs here, because nothing timed out on
		// the other exits.
		if p.hasCandidate() {
			return nil
		}
		// And a gathering that produced NO address is a different outcome, which
		// keeps the stalled error: there is nothing to proceed with.
		//
		// Through stalled, as [Peer.DataChannel] and awaitOpen already do, and
		// naming the operation as awaited does: a bare "context deadline
		// exceeded" is the least useful thing this transport can say, and
		// gathering is where it is most expensive to say it.
		if ctx.Err() != nil {
			return p.stalled(fmt.Errorf("ICE gathering: %w", ctx.Err()))
		}
		return p.stalled(fmt.Errorf("ICE gathering: reported no completion and carried no address within %s: %w",
			iceGatherBudget, context.DeadlineExceeded))
	}
}

// hasCandidate reports whether the local description carries at least one ICE
// address, which is the only question that decides whether an offer is worth
// handing back.
//
// ⛔ THE SDP, NOT A COUNTER, and the difference is a race. Gathering begins
// inside setLocalDescription, so candidates can be delivered before any
// listener of ours exists; a count of events then says zero for a connection
// that has an address, and #267's stall is thrown away from the other side.
// The description is cumulative and can be read at the moment the answer is
// needed. It also cannot mistake an end-of-candidates marker for an address:
// a marker is "a=end-of-candidates", never "a=candidate".
func (p *Peer) hasCandidate() bool {
	desc := p.pc.Get("localDescription")
	if !desc.Truthy() {
		return false
	}
	sdp := desc.Get("sdp")
	if sdp.Type() != js.TypeString {
		return false
	}
	for _, line := range strings.Split(sdp.String(), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "a=candidate:") {
			return true
		}
	}
	return false
}

// gathered is the whole contract of a SUCCESSFUL gather: the connection is
// still open, and the description carries an address.
//
// ⛔⛔ IT IS ASKED ON EVERY EXIT, because a review found it asked on ONE. The
// usability question was put only to the expiry branch, so a browser with no
// usable interface that REPORTED completion — or fired onicecandidate(null)
// with nothing gathered — walked out through the other three and handed back a
// blob with no address in it. That is the same silent dead offer the expiry
// branch had just been fixed to refuse, surviving on the paths nobody looked
// at: an invariant enforced at one of four doors is not an invariant.
func (p *Peer) gathered() error {
	if err := p.stillOpen(); err != nil {
		return err
	}
	if p.hasCandidate() {
		return nil
	}
	// Completion is not the problem here, so the sentence must not blame it:
	// gathering ENDED and produced nothing to connect with.
	return p.stalled(fmt.Errorf("%w: ICE gathering: finished and carried no address", ErrTransport))
}

// stillOpen refuses to hand back a description for a connection that was closed
// while the wait was running.
//
// ⛔ Close during gathering used to end with a blob for a dead connection: the
// wait returned nil, the caller read localDescription, and nothing said the
// peer was gone. An offer nobody can answer is worse than an error.
func (p *Peer) stillOpen() error {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return ErrClosed
	}
	return nil
}

// localDescription reads the gathered description off the connection and renders
// it as a blob to paste.
func (p *Peer) localDescription() string {
	desc := p.pc.Get("localDescription")
	return encodeSignal(signal{Type: desc.Get("type").String(), SDP: desc.Get("sdp").String()})
}

// track keeps a callback alive until Close, which is the only safe time to
// release it: a js.Func released while an event it listens for might still fire
// panics the moment it does.
func (p *Peer) track(fns ...js.Func) {
	p.mu.Lock()
	closed := p.closed
	if !closed {
		p.funcs = append(p.funcs, fns...)
	}
	p.mu.Unlock()
	// Same window as listen: after Close there is nobody left to release these,
	// so they are released here rather than kept alive by a slice nothing reads.
	if closed {
		for _, fn := range fns {
			fn.Release()
		}
	}
}

// wired is one event listener: what it is on, what it listens for, and the
// callback. Close needs all three, because giving a callback back without
// unwiring it leaves the browser holding a pointer to freed work.
type wired struct {
	target js.Value
	event  string
	fn     js.Func
}

// listen wires fn to event on target AND keeps the wiring, so Close can take it
// off before releasing the callback.
//
// ⛔ WHY THE TRIPLE, not just the func (go-crdt/collab#270). Release gives a
// callback back to the Go runtime; the listener stays registered, so an event
// already queued in the browser's task queue calls it afterwards and panics
// with "call to released function". The suite printed exactly that beside a
// PASS — a panic in a callback cannot fail the test that caused it, so the
// hazard was visible and unattributable for as long as it existed.
func (p *Peer) listen(target js.Value, event string, fn js.Func) {
	// ⛔ RECORDED AND WIRED UNDER ONE LOCK. Doing it in two critical sections
	// left a window — narrow, and on a cooperative scheduler probably not
	// reachable — where a Close landing between them wires a listener Close has
	// already finished removing, and track then releases its func: #270 again,
	// with the callback released, which is worse than the leak this guard was
	// added for. Holding the mutex across the Call costs nothing, since no
	// browser task can run while Go holds it anyway.
	p.mu.Lock()
	defer p.mu.Unlock()
	closed := p.closed
	if !closed {
		p.wires = append(p.wires, wired{target: target, event: event, fn: fn})
	}
	// ⛔ A LISTENER WIRED AFTER CLOSE WOULD NEVER BE UNWIRED. Close takes its
	// snapshot and nils the slices, so a Close racing the start of gatherICE or
	// awaitOpen left listeners registered on the connection with nothing left
	// to remove them: no panic, because their funcs are not released either,
	// just a permanent wiring and a closure kept alive for as long as the page.
	// Found by a review on the fix for the panic, which is the same window seen
	// from the other side.
	//
	// ⚠ It does NOT release here, because track owns the release and releasing
	// in both would release twice — resting on Release being idempotent is a
	// worse bargain than naming one owner.
	//
	// ⛔ AND THE OWNER IS NOT UNIFORM, which the first version of this comment
	// got wrong. It said "every caller in this file pairs listen with track",
	// and NewPeer's `listen(p.pc, "datachannel", p.onData)` does not: that one
	// is released because Close prepends p.onData by hand. It cannot bite today
	// — NewPeer's peer is not closed before it is returned, so the guard below
	// never fires there — but the sentence LICENSED dropping that hand-prepend
	// on the strength of "track owns the release", which would have left the
	// data-channel callback released by nothing. A review caught the sentence,
	// not the code.
	if closed {
		return
	}
	target.Call("addEventListener", event, fn)
}

// Close ends the connection and gives every callback back. It is safe to call
// more than once, and safe to call on a handshake that never finished.
func (p *Peer) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	funcs := append([]js.Func{p.onData}, p.funcs...)
	wires := p.wires
	p.funcs, p.wires = nil, nil
	p.mu.Unlock()

	// Wake every wait before anything is taken apart, so none of them is still
	// reading a connection that is being closed underneath it.
	close(p.quit)

	// ⛔ UNWIRE BEFORE RELEASING, and before closing the connection: an event
	// already in the browser's task queue will still reach a listener that is
	// registered, and calling a released callback panics. Taking the listeners
	// off first is the only ordering where the browser cannot reach freed work.
	for _, w := range wires {
		w.target.Call("removeEventListener", w.event, w.fn)
	}
	p.pc.Call("close")
	for _, f := range funcs {
		f.Release()
	}
	return nil
}

// A signal is a WebRTC connection description — the {type, sdp} the two browsers
// swap. It is the whole of what one side tells the other, and nothing about the
// document travels in it.
type signal struct {
	Type string `json:"type"`
	SDP  string `json:"sdp"`
}

// encodeSignal renders a description as one line to paste. It is base64 over the
// JSON rather than the JSON itself because the SDP is many lines, and a chat box
// or a mail client is free to reflow those; base64 has no newline to fold.
//
// Marshalling two strings cannot fail, so the error json returns here is one
// that never happens; it is dropped rather than dressed up as something a caller
// could act on.
func encodeSignal(s signal) string {
	raw, _ := json.Marshal(s)
	return base64.StdEncoding.EncodeToString(raw)
}

// decodeSignal reads a blob back. It refuses one that is not base64, that is not
// the JSON of a description, or that is missing a field — a half-pasted blob is
// a mistake to report, not a description to set.
func decodeSignal(blob string) (signal, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(blob))
	if err != nil {
		return signal{}, fmt.Errorf("%w: the pasted description is not valid base64", ErrTransport)
	}
	var s signal
	if err := json.Unmarshal(raw, &s); err != nil {
		return signal{}, fmt.Errorf("%w: the pasted description is not a connection description", ErrTransport)
	}
	if s.Type == "" || s.SDP == "" {
		return signal{}, fmt.Errorf("%w: the pasted description is missing its type or sdp", ErrTransport)
	}
	return s, nil
}

// describe builds the RTCSessionDescriptionInit that setLocalDescription and
// setRemoteDescription take.
func describe(s signal) js.Value {
	d := js.Global().Get("Object").New()
	d.Set("type", s.Type)
	d.Set("sdp", s.SDP)
	return d
}

// iceConfiguration builds the RTCPeerConnection configuration. The plain URL
// list and the credentialed servers are appended in that order; when neither is
// given iceServers is left unset, which is a working configuration on one
// network. A credentialed entry sets "username"/"credential" only when non-empty
// so a bare STUN URL given through ICEServersAuth is not sent empty strings.
func iceConfiguration(cfg PeerConfig) js.Value {
	config := js.Global().Get("Object").New()
	servers := js.Global().Get("Array").New()
	for _, url := range cfg.ICEServers {
		server := js.Global().Get("Object").New()
		server.Set("urls", url)
		servers.Call("push", server)
	}
	for _, s := range cfg.ICEServersAuth {
		server := js.Global().Get("Object").New()
		urls := js.Global().Get("Array").New()
		for _, u := range s.URLs {
			urls.Call("push", u)
		}
		server.Set("urls", urls)
		if s.Username != "" {
			server.Set("username", s.Username)
		}
		if s.Credential != "" {
			server.Set("credential", s.Credential)
		}
		servers.Call("push", server)
	}
	if servers.Length() > 0 {
		config.Set("iceServers", servers)
	}
	return config
}

// await turns a JavaScript promise into a Go return. It resolves on the page's
// thread and hands the result over a channel, so the goroutine that called it
// blocks without blocking the page.
//
// On the context being cancelled it returns at once but does not release its
// callbacks: the promise may still settle and call into them, and a released
// js.Func called is a panic. They are released once it settles, however late.
func await(ctx context.Context, promise js.Value) (js.Value, error) {
	type outcome struct {
		val js.Value
		err error
	}
	ch := make(chan outcome, 1)
	var onOK, onErr js.Func
	onOK = js.FuncOf(func(_ js.Value, args []js.Value) any {
		var v js.Value
		if len(args) > 0 {
			v = args[0]
		}
		ch <- outcome{val: v}
		return nil
	})
	onErr = js.FuncOf(func(_ js.Value, args []js.Value) any {
		msg := "the browser rejected a WebRTC operation"
		if len(args) > 0 && args[0].Truthy() {
			if m := args[0].Get("message"); m.Type() == js.TypeString {
				msg = m.String()
			}
		}
		ch <- outcome{err: fmt.Errorf("%w: %s", ErrTransport, msg)}
		return nil
	})
	promise.Call("then", onOK).Call("catch", onErr)

	release := func() { onOK.Release(); onErr.Release() }
	select {
	case r := <-ch:
		release()
		return r.val, r.err
	case <-ctx.Done():
		go func() { <-ch; release() }()
		return js.Value{}, ctx.Err()
	}
}
