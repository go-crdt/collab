# Design

## What the server is not

It is not an arbiter. The document is a CRDT, so the outcome of two concurrent
edits is decided by the operations themselves, not by whoever the server heard
from first. The server keeps a replica so it can answer a joiner and persist the
result, applies what it is sent, and passes it on unchanged.

Everything else follows from that:

- **Offline editing works** because a participant's replica is as authoritative
  as the server's.
- **The server can be restarted, replaced, or run behind a load balancer**
  without a handover protocol: a new one loads a snapshot and is immediately
  correct.
- **Operations need no sequencing on the wire.** Applying them is idempotent and
  order-independent, so a batch is relayed exactly as it arrived.

## Two counters, and where they show up here

`crdt` gives every operation a per-site sequence number and a Lamport clock (see
that project's `docs/design.md`). The sequence number is what makes this service
possible: because a site's numbers have no gaps, a version vector describes a
replica *exactly*, so "what have I missed" is answerable in one message rather
than by replaying a log.

## Joining, and the direction people forget

A `Welcome` carries three things:

1. the document — a snapshot for a new participant, or just the operations it
   missed for one that said what it had;
2. who else is present, so a joiner draws everyone's cursor immediately;
3. **the server's own version vector.**

The third is easy to leave out and doing so quietly breaks offline editing. A
participant resuming after a disconnection has work the server has never seen; if
it is only *sent* what it missed and never asked what to *send*, that work stays
on one replica for ever. With the server's vector in hand the client answers with
exactly what is missing. `TestResumeCarriesWorkBothWays` fails if either
direction is dropped.

## One replica identity per participant

`crdt` assumes every replica editing a document has an identity of its own. When
two do not, the failure is not a conflict anyone can see: both mint the same
`(site, sequence)` for different characters, the version vector keeps the first
of each pair and discards the second, and the lost characters are simply not
there. Nobody is told.

So the arriving session takes the identity and the one already holding it is
disconnected with `Aborted`. Refusing the newcomer instead would be worse exactly
where this happens: a participant whose connection dropped comes back long before
the server notices the old one is dead, and would be locked out until a TCP
timeout it cannot see. Displacing also makes a genuine clash loud — two tabs
would take turns evicting each other — rather than losing characters quietly.
Being displaced is not leaving, so no departure is announced: the identity is
still in the document, on the session that took it. Site zero, the server's own
replica, is refused outright.

This puts a requirement on whoever allocates identities: they must be unique
among *concurrent* participants of one document. Deriving one by hashing a
session token ([crdt.DeriveSiteID]) satisfies that, but note what it costs on the
wire — a 64-bit identity is carried twice in every operation, once for the
operation and once for its origin, which roughly doubles the encoded size against
small dense numbers. A server handing out small identities per session is
cheaper, and it is the server that can guarantee uniqueness anyway.

## Backpressure

A participant that stops reading cannot be allowed to hold up the document, and
cannot be silently skipped either — skipping an operation would leave that
replica permanently wrong. So it is disconnected with `ResourceExhausted` once
its queue overflows, and rejoins with its version vector to be caught up. The
queue depth is `Config.Backlog`.

This is the one place the server makes a decision, and it is a decision about the
connection, never about the document.

## Persistence

`Store` holds snapshots rather than an operation log, because a snapshot is
self-contained: `crdt` guarantees the whole history is recoverable from one, so a
document restored after a restart can still serve a participant that has been
away for a month.

Writes happen when the last participant leaves, and whenever `Server.Flush` is
called — a server wanting durability without waiting calls it on a timer. A write
that fails leaves the document marked as needing one, so the next `Flush` tries
again instead of losing the fact.

Documents stay in memory once opened. For the fleet's expected shape — a handful
of documents per server — that is the right trade; a server hosting very many
would want eviction, which is a change to `open` and `leave` alone.

## Testing

Three layers, because each catches what the others cannot:

- **In-process, over `bufconn`** — the fast bulk of the suite, including every
  rejection on both sides of the wire. A stub server lets the client be shown
  messages a real server would never send; a raw client lets the server be sent
  messages a real client would never send.
- **Over a real WebSocket** — `TestOverWebSocket`, natively.
- **Across two runtimes** — `TestWasmConverges`: two participants compiled to
  WebAssembly and executed by Node, editing concurrently with a native one,
  all three required to converge on the same text with no character lost. This is
  the acceptance gate for the claim that a browser and a server run the same
  merge logic, and compiling for `js/wasm` would not prove it.

A skipped test is not a passing one, so CI sets `COLLAB_REQUIRE_WASM` on the job
that exists to run the last of these: a missing Node or wasm glue fails it rather
than turning it green.

## Who may open what

`Config.Authorize` decides it, once per session, after the join arrives and
before the document is touched — so a refused session neither reads the store nor
reveals whether the document exists.

The obvious place for this is a gRPC interceptor, and that is the wrong place: an
interceptor sees the method and the request metadata, and the document being
joined is in neither. It arrives in the stream's first message, so anything
deciding *per document* has to run after that message. Authentication, being per
connection rather than per document, does belong in an interceptor; the context
carries whatever it put there.

Over a WebSocket, that authentication is a **cookie**, because a browser cannot
put a header on one — and a cookie only exists while the upgrade is still an HTTP
request. `grpc-transports/websocket` v0.2.0 carries it across:

```go
lis, _ := wstransport.ListenWebSocket(addr, wstransport.ServerConfig{
    OnUpgrade: func(r *http.Request) (any, error) {
        c, err := r.Cookie("session")            // still an HTTP request here
        if err != nil {
            return nil, &wstransport.UpgradeError{Code: 401, Message: "sign in"}
        }
        return c.Value, nil
    },
})
gs := grpc.NewServer(grpc.Creds(wstransport.ServerCredentials()))

collab.NewServer(collab.Config{
    Authorize: func(ctx context.Context, document string, _ crdt.SiteID) error {
        user, _ := wstransport.FromContext(ctx) // what the upgrade carried
        return acl.Check(user, document)
    },
})
```

`TestAuthorizeFromTheUpgradeCookie` is that chain end to end, because each link
works alone and the question is whether they meet. A session with no cookie never
becomes one: the refusal happens during the handshake, before any stream exists.

## Next

- Postgres-backed `Store` against [weft's HA datastore](https://github.com/openweft),
  keeping snapshots and an operation log.
- Wiring `weft-loom-server` and the loom browser client to this package.
