# collab — the wire for collaborative editing

[![CI](https://github.com/go-crdt/collab/actions/workflows/ci.yml/badge.svg)](https://github.com/go-crdt/collab/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/go-crdt/collab.svg)](https://pkg.go.dev/github.com/go-crdt/collab)
[![coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)](https://github.com/go-crdt/collab/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)

`github.com/go-crdt/collab` carries a [`go-crdt/crdt`](https://github.com/go-crdt/crdt)
document between the people editing it: a gRPC service, a server that hosts
documents, and a client that joins one. Pure Go, **CGO=0**.

The service is thin on purpose. The document is a CRDT, so the server never
transforms an operation and never decides an outcome — it applies what it is sent
to its own replica and hands it to everyone else. Two things follow that a
server-authoritative design cannot offer: **a participant may edit while
disconnected** and reconcile on return, and **the server may be restarted or
replaced** without anyone losing work.

Mounted on [`grpc-transports/websocket`](https://github.com/grpc-transports/websocket)
the same service reaches a browser, and the client here builds for `js/wasm`, so
**a browser tab and the server run the same code down to the merge**.

## Using it

Server — any `grpc.Server`, any carrier:

```go
srv := collab.NewServer(collab.Config{Store: myStore})
collabpb.RegisterCollabServer(grpcServer, srv)
```

Participant:

```go
c, err := collab.Join(ctx, conn, collab.ClientConfig{Document: "notes", Site: 1})
c.Insert(0, "hello")
c.SetCursor(awareness.Cursor{Anchor: 5, Head: 5}, map[string]string{"name": "ada"})

for range c.Changes() {
    render(c.Text(), c.Peers())
}
```

Coming back after a disconnection, keeping the work done offline:

```go
c, err := collab.Join(ctx, conn, collab.ClientConfig{
    Document: "notes",
    Site:     1,
    Resume:   savedSnapshot, // from Client.Snapshot()
})
```

## Shape of a session

One bidirectional stream per participant per document. The client opens with a
`Join`; the server answers with a `Welcome` holding either the whole document or,
for a participant that says what it already has, only what it missed — plus who
else is present and where the server stands, so the participant can push whatever
it wrote while away. After that, operations and presence flow both ways.

## What it guarantees

- **Convergence, proven end to end.** The acceptance test runs three replicas
  across two runtimes — one native, two compiled to WebAssembly and executed by
  Node through a real WebSocket — editing concurrently and converging on the same
  text.
- **Offline work is never stranded.** A resuming participant pushes what the
  server lacks and is sent what it missed. Both directions are tested.
- **Nobody stalls the document.** A participant that stops reading is
  disconnected with `ResourceExhausted` and caught up when it rejoins, rather
  than holding everyone up or being served state that is quietly out of date.
- **Nothing is trusted.** Malformed operations, presence, version vectors and
  snapshots are each refused with `InvalidArgument`, on both sides of the wire.
- **One replica identity per participant.** Two participants sharing a site is
  silent data loss rather than a conflict — both mint the same operation
  identities for different characters — so the arriving session takes the
  identity and the one already holding it is disconnected with `Aborted`. Site
  zero, the server's own replica, is refused outright.
- **Documents outlive sessions.** The last participant out writes the document;
  `Server.Flush` writes it without waiting. A write that fails is retried rather
  than forgotten.

## Who may open what

`Config.Authorize` is asked once per session, after the join arrives and before
the document is touched, so a refused session neither reads the store nor reveals
whether the document exists:

```go
collab.NewServer(collab.Config{
    Authorize: func(ctx context.Context, document string, site crdt.SiteID) error {
        return myACL.Check(userFrom(ctx), document)
    },
})
```

It lives here rather than in a gRPC interceptor, which is where one would first
look for it: an interceptor sees the method and the request metadata, and the
document being joined is in neither — it arrives in the stream's first message.
Authentication, being per connection rather than per document, still belongs in
an interceptor; the context carries whatever it put there.

## Persistence

`Store` is a two-method seam — `Load` and `Save` on snapshots, which are
self-contained, so a document restored from one can still serve a participant
that has been away. `MemoryStore` is the default.

[`collab/pgstore`](pgstore) keeps documents in PostgreSQL, over a plain `*sql.DB`
and with no driver of its own, so the caller picks one:

```go
db, _ := sql.Open("pgx", os.Getenv("DATABASE_URL"))
store, _ := pgstore.New(db)
store.Migrate(ctx)
srv := collab.NewServer(collab.Config{Store: store})
```

It is a module of its own, so importing `collab` does not drag a database driver
into anyone's build. Its tests run against a real PostgreSQL — CI fails the job
if one is missing rather than skipping it.

## Status

Version 0.1. **100% statement coverage**, race-clean, six-arch CI, and the
WebAssembly end-to-end test running on every pull request — where a missing
toolchain is a failure, not a skipped test.

## License

BSD-3-Clause — see [LICENSE](LICENSE). Copyright the go-crdt authors.
