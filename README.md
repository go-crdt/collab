<p align="center"><img src="https://raw.githubusercontent.com/go-crdt/brand/main/social/go-crdt.png" alt="go-crdt/collab" width="720"></p>

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

The client builds for `js/wasm`, so **a browser tab and the server run the same
code down to the merge** — over either of two carriers, from one server:

| carrier | for | browser client, gzipped |
|---|---|---|
| `collab.WebSocket` — the session's own framing | anywhere, browsers included | **919 KB** |
| `collab.GRPC` — over gRPC | native peers | 4 461 KB |

Everything a session carries is already bytes `crdt` encoded and will check on
arrival, so protobuf describes fields nobody reads through it — and compiled to
wasm its machinery cannot be linked away. For scale, the CRDT alone is 633 KB.
Outside a browser none of that matters, which is why gRPC is still there.

## From a page

The editor this was built for is TypeScript and cannot call Go, so `./wasm`
compiles to a binding a page uses directly. Offsets are UTF-16 code units
throughout — the units a JavaScript string counts in — and an offset splitting a
character is refused rather than rounded:

```js
const session = await collab.join({ url, document: "project:default", site });

const body  = await session.text("file:main.tex");
const chat  = await session.list("chat");
const cells = await session.map("cells");

await body.insert(0, "bonjour");
await cells.set("B7", new TextEncoder().encode("42"));

await session.onChange(parts => {
  for (const part of parts) {
    if (part.kind === "text") applyEdits(part.text);   // {pos, removed, insert}
    if (part.kind === "map")  reread(part.keys);       // the keys that changed
    if (part.kind === "list") rereadWhole(part.name);  // a list says only that it moved
  }
});
```

Types are in [`wasm/collab.d.ts`](wasm/collab.d.ts). A value is `Uint8Array` in
both directions, never a string and never JSON: the CRDT does not interpret it
and neither does the binding.

## Keeping what people wrote

A document was saved when its last participant left, and otherwise when somebody
called `Flush`. That is enough for a text people open and close, and not enough
for what a session actually carries — the comments on it, the record of who
changed what, the messages beside it. A server restarted while anybody was still
connected lost everything since the document was opened, and said nothing.

```go
store, err := collab.NewDirStore("/var/lib/loom/documents")

srv := collab.NewServer(collab.Config{
    Store:        store,
    PersistEvery: 5 * time.Second,  // bounds what a crash costs
    EvictAfter:   10 * time.Minute, // let go of what nobody is in
})
defer srv.Close(ctx)
```

`DirStore` keeps a document per file, written and renamed into place so a reader
sees one whole version or the one before. The file is named after the *encoding*
of the document name rather than the name: `project:default` and
`file:src/main.tex` are not file names, and escaping them would leave the
question of which characters, on which system. `Documents()` says what the file
names cannot.

Neither runs unless it is asked for, so a server configured as before behaves as
before. `EvictAfter` also stops a long-lived server holding every document it has
ever served; reopening one costs a read from the store, not anything anybody
wrote.

## Using it

Server — any `grpc.Server`, any carrier:

```go
srv := collab.NewServer(collab.Config{Store: myStore})
collabpb.RegisterCollabServer(grpcServer, srv)
```

Participant. A document holds **named parts**, so a caller reaches for the one it
means and gets a handle that edits *and* publishes:

```go
c, err := collab.Join(ctx, conn, collab.ClientConfig{Document: "notes", Site: 1})

body, _ := c.Text("file:main.tex")   // the buffer an editor binds to
chat, _ := c.List("chat")            // the messages beside it
cells, _ := c.Map("cells")           // a sheet

body.Insert(0, "hello")
chat.Append([]byte("on commence"))
cells.Set("B7", []byte("42"))
c.SetCursor(awareness.Cursor{Anchor: 5, Head: 5}, map[string]string{"name": "ada"})

for range c.Changes() {
    for _, part := range c.TakeChanges() {   // which part moved, and how
        render(part)
    }
    render(c.Peers())
}
```

A handle is what a caller touches rather than the replicated structure, and that
is forced: editing the structure directly would produce operations nobody ever
heard, so the participant would drift away from everyone else while its own
screen looked right.

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

## What a binding needs

An editor cannot be handed the whole text on every keystroke somebody else
makes: that throws away the selection, the scroll position and every decoration.
`Changes` says something happened, `TakeChanges` says what — the edits, in the
order they have to be made.

```go
c.TakeChanges()      // []crdt.Change: remove this many here, put this there
c.Anchor(pos)        // a handle on a character, for a comment or a stored selection
c.Position(anchor)   // where it is now, or where it was if it has gone
c.AuthorRuns()       // the text split by who wrote each stretch
c.InsertUTF16(pos, text)  // offsets in the units a browser counts
```

A browser counts UTF-16 code units, and an emoji is one character and two units.
A session that took the browser's offsets for runes would edit in the wrong
place, silently, from the first emoji onwards — so the same operations are
addressed both ways, and an offset landing inside a character is refused rather
than moved.

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

[`collab/gitstore`](gitstore) keeps documents in a git repository, so a document
can be versioned and released the way everything else is. One commit holds both
the state, which carries identities and authorship and the comments anchored to
characters, and the rendered text, which is what makes the repository readable
by a person. A release is a tag on a commit that already exists. It is also a
federation channel: two servers sharing a repository diverge, git reports a
conflict on the state file, and `gitstore.Merge` resolves it without anybody
having to choose a side.

`MultiStore` writes to several stores at once and reads from all of them:

```go
srv := collab.NewServer(collab.Config{
    Store: collab.NewMultiStore(database, repository),
})
```

Reading merges rather than picking, because a save that failed halfway leaves
the stores holding different documents and taking the first would drop whatever
only the second had — `MergeSnapshots` is what makes that free. Adding a store
to a running server therefore backfills it. A store that cannot be read fails
the load rather than serving a document quietly missing a paragraph.

`Tiered` is the other composition: a hot store for documents somebody is using
and a cold one for documents nobody has opened in a long time.

```go
store := collab.NewTiered(hot, archive)
srv := collab.NewServer(collab.Config{Store: store})

// On a timer, or by hand.
moved, err := store.Archive(ctx, 30*24*time.Hour)
```

**Nothing is deleted that is not already somewhere else.** Archiving reads the
hot store, writes the cold one, and only then asks the hot one to release
*exactly what was read* — so a cold store that refuses releases nothing, and a
document that somebody saved while it was being copied is not released at all;
it is archived on a later pass. Reading an archived document brings it back to
the hot store on the way past.

A `Tiered` store never answers "no such document" because it could not reach the
archive: nil means *start a new one*, and a server acts on it. An unreachable
archive fails the load instead.

What counts as idle is *not written for a while*, which is as close to *not
used* as a store can get — and close enough, because a server saves a document
somebody is in every `PersistEvery`, so a busy document never looks quiet.
`MemoryStore` and `DirStore` both implement `Archivable`; `Config.EvictAfter` is
the same idea one level up, for the server's memory rather than the store's.

## Status

Version 0.1. **100% statement coverage**, race-clean, six-arch CI, and the
WebAssembly end-to-end test running on every pull request — where a missing
toolchain is a failure, not a skipped test.

## License

BSD-3-Clause — see [LICENSE](LICENSE). Copyright the go-crdt authors.
