// Package collab carries a [github.com/go-crdt/crdt] document between the people
// editing it: a gRPC service, a server that hosts documents, and a client that
// joins one.
//
// The service is thin on purpose. The document is a CRDT, so the server never
// transforms an operation and never decides an outcome — it applies what it is
// sent to its own replica and hands it to everyone else. Two consequences follow
// that a server-authoritative design cannot offer: a participant may edit while
// disconnected and reconcile later, and the server may be restarted or replaced
// without any client losing work.
//
// # Over what
//
// Two carriers, and which one to use is decided by where the code runs rather
// than by taste. [WebSocket] carries a session's own framing over a plain
// WebSocket; [GRPC] carries it over gRPC. One server serves both at once —
// [Server.ServeWebSocket] beside the registered service — and a participant on
// each edits the same document.
//
// The reason there are two is measured. Everything a session carries is bytes
// some encoder in [github.com/go-crdt/crdt] produced and will check on arrival,
// so protobuf is describing fields nobody reads through it — and compiled to
// wasm its reflection and registry machinery cannot be linked away. The browser
// test client, gzipped, is 919 KB over the framing and 4 461 KB over gRPC,
// against 633 KB for the CRDT alone. Outside a browser none of that matters,
// and gRPC brings deadlines, interceptors and the tooling built around them.
//
// The client builds for js/wasm either way, so a browser tab and a server run
// the same code down to the merge. Two browsers with no server between them
// carry a session over a WebRTC data channel ([DataChannel]), and two tabs of
// one browser over a BroadcastChannel ([JoinBroadcastChannel]) with nothing to
// configure at all.
//
// # A document holds named parts
//
// What an editor holds is not one structure: the text of a file, the comments
// anchored into it, the record of who changed what, the messages beside it, the
// cells of a sheet. A document here is a [github.com/go-crdt/crdt.Composite], so
// they travel together — one snapshot, one version, one decision about who may
// open it, and no instant at which the set of them disagrees.
//
// A caller reaches for a part by name and gets a handle: [Client.Text],
// [Client.List], [Client.Map]. A handle edits and publishes in one step, which
// is why it exists rather than the replicated structure itself — a caller
// editing that directly would produce operations nobody ever heard, and drift
// away from everyone else while its own screen looked right.
//
// # Shape of a session
//
// One bidirectional stream per participant per document. The client opens with
// a [collabpb.Join]; the server answers with a [collabpb.Welcome] holding either
// the whole document or, for a participant that says what it already has, only
// what it missed. After that, operations and presence flow both ways until
// either side hangs up.
//
// # What this protects, and what it does not
//
// Nothing here is encrypted or signed. The server reads every document it
// holds, a store holds them in the clear, and a participant is whoever the
// transport says it is. That is a choice with a reason, and the reason is
// structural rather than a matter of effort.
//
// A server that merges cannot be blind, and a server that is blind cannot
// merge. This one merges: it applies operations, hands a joining participant a
// snapshot built from them, and collects tombstones once every participant has
// delivered. Every one of those reads the document. Encrypting it end to end
// would leave the server with bytes it cannot combine, which is a different
// design and not a setting.
//
// The field splits along exactly that line, and it is worth naming where the
// neighbours stand:
//
//   - Automerge and Yjs encrypt nothing in the format. Automerge's chunk header
//     carries four bytes of a truncated SHA-256, which detects a chunk that
//     changed and authenticates nobody; Yjs's updates carry no digest at all.
//   - Jazz states authorization as row-level policy that its serving node
//     applies before accepting a write and before shipping a row — which it can
//     only do by reading the row. Its relay links are a separate kind of peer,
//     defined as having no permission subject at all.
//   - Evolu does encrypt end to end, and pays for it exactly here: its server is
//     called a Relay and holds encrypted changes it reconciles by fingerprints
//     over ranges of timestamps. It never merges anything, because it cannot.
//
// So what is actually underneath a deployment of this package:
//
//   - The transport. A session runs over WebSocket or gRPC, and under TLS both
//     authenticate the server and give every message a MAC — which is stronger
//     than a checksum and covers forgery, not merely rot. Running either without
//     TLS puts the document on the wire in the clear.
//   - Whoever the caller lets in. This package does not authenticate: a server
//     serves the sessions its host hands it, and [Config.Authorize] is where a
//     host decides who those are.
//   - The store, against a medium that changes bytes rather than against
//     somebody who writes them. [PackSnapshot] and [CheckSnapshot] carry a
//     CRC32C, and anything that can write a stored document can recompute one.
//
// # What a session costs, since nothing here bounds how many there are
//
// [Config] has no capacity limit, on purpose: this package decides who may be
// in a document, not how many machines are worth buying. A deployment bounds
// that outside — a reverse proxy, a connection limit, a quota — and to do that
// it needs the numbers, which are these.
//
// A participant is cheap in time and not free in memory. Measured over an
// in-memory connection on one document: about 2.5 µs a participant an edit,
// flat from a hundred upwards, so a thousand watching and five typing at ten
// keystrokes a second is roughly 12% of a core. Bringing one in costs about
// 27 KB at a thousand, more below that while the document's own cost
// amortises. See BenchmarkFanOut for the table and for what its bytes column
// does and does not answer.
//
// One message is not cheap. A session may send up to a gigabyte in a single
// message, because a document's whole snapshot travels in one and that bound is
// the largest document a session may open — not the size of an edit. Nothing
// caps the sum of those across sessions, so the arithmetic a deployment has to
// do is per-session peak times sessions allowed, and the lever is the second
// factor.
//
// And what a participant can do once it is in: everything a replica can do to a
// document it holds. It can write anywhere and delete anything — a CRDT
// converges on what it is told, and does not adjudicate — and it can hand over
// operations another site made, which is worse than it sounds and is not
// refused by default. [OwnSiteOnly] is the one-line policy that refuses it, and
// [Config.AuthorizeOperations] is where a federating deployment writes a
// narrower one. Neither is a stricter merge: the merge is not the lever.
//
// # A site identity is claimed, not proved
//
// This bounds who may federate with whom, so it is worth being exact about.
// A [github.com/go-crdt/crdt.SiteID] is a number a joining session states, and
// nothing here binds the number to whoever states it: TLS authenticates the
// server rather than the site, [Config.Authorize] decides whether a session may
// join and cannot check a number it did not issue, and
// [github.com/go-crdt/crdt.DeriveSiteID] is a pure function of a name, so anyone
// who knows the name computes the identity.
//
// Within one operator that is a topology decision — both ends hand out site
// identities, so a site is as trustworthy as the deployment. Across operators it
// is not, and the cost is measured in
// TestAFederatedPeerCanSpeakAsAnotherServersUser: a followed server whose
// participant claims a site belonging to the follower's user makes the follower
// hold a document that existed on neither server, my user's genuine prefix
// grafted to the tail of a forged sequence, with every character attributed to
// that user — and both replicas then report the SAME version vector, so each
// believes it is completely caught up with the other and neither will ever ask
// for anything again. Nothing returns an error: the operations were well formed
// and the merge converged on what it was told.
//
// [OwnSiteOnly] is not the answer, for a reason worth knowing before reaching
// for it: the side it breaks is the FOLLOWER, because what arrives over a link
// names sites that server never authorised
// (TestALinkCarryingOtherSitesMeetsOwnSiteOnly). A server that federates cannot
// install it, which is why nothing on the wire distinguishes a link from a
// participant.
//
// # Federating with somebody else's server, then
//
// It takes two rules, and both are the operator's to state because only the
// operator knows who the other actors are. Neither needs a change to the format
// or to this package, and there is a worked example of both in gitstore's
// federation_example_test.go, which is written as a consumer of this package
// rather than part of it so that anything it needs and cannot reach is a gap.
//
//  1. SCOPE the site identity, so two actors cannot mint the same one. The
//     example derives a site from an eduGAIN identifier —
//     "ada@paris.example.ac" rather than "ada" — because only the home
//     organisation issues inside its own scope. A bare name is the one failure
//     this design cannot merge its way out of:
//     [github.com/go-crdt/crdt.DeriveSiteID] is a function, so "42" is the same
//     replica on every instance in the world.
//  2. Write [Config.AuthorizeOperations] about the RELATION: for every batch,
//     which sites it carries, and whether the session carrying them may speak
//     for those. Two details decide whether it works, and both were measured
//     the hard way.
//
// Every KIND of operation, because a document holds three. gitstore's example
// read a batch's text and nothing else, so an unfederated site was refused when
// it wrote a character and allowed when it wrote a map entry, until
// TestTheScopeCheckSeesEveryKindOfOperation.
//
// And this server's OWN scope must not be among the scopes a link may carry.
// Listing it is how a link comes to be allowed to write as one of this server's
// own users, which is the attack rather than a refinement of it — and the example
// listed it. Our users need no entry in any register: a session may always speak
// for the site it joined as, which is [OwnSiteOnly]'s rule, and composing the two
// is what makes the policy about the relation. Held to it by
// TestAScopedPolicyStopsALinkSpeakingForOurOwnUsers, whose third case is the
// measured attack: a peer claiming the very user who wrote here, with a longer
// history so the tail is actually sent. The link's session ends at once naming
// the site it may not speak for, and this replica keeps what it had.
//
// One trap in testing this, because it turns a defect into something that looks
// like a defence. When a peer claims a site one of our participants used and
// writes LESS than that participant did, our link joins saying it holds that site
// up to a higher clock, so the followed server sends nothing at all. Nothing
// arrives and nothing is refused. That is the version-vector collision measured
// in TestAFederatedPeerCanSpeakAsAnotherServersUser, and a test that only watched
// the document would read it as the policy working.
//
// Two limits remain, and they are properties of this shape rather than gaps in
// it. Trust is hop by hop: if A follows B and B follows C, B relays C's sites, so
// A grants B the union and thereby trusts B about C — which is how mail and
// Matrix federation trust. And an operation carries no signature, so a link is
// believed about the attribution of everything it relays; what a server can check
// is which sites a link may speak for, not that a site really said this. Whether
// to close that second one, and at what cost to the snapshot and to
// [github.com/go-crdt/crdt.Doc.Purge], is
// https://github.com/go-crdt/collab/issues/175.
//
// # Two servers do not share a store
//
// A [Store] holds snapshots and [Store.Save] replaces. A server holds the
// document in memory while it serves it, so two servers holding the same
// document at the same time each save their own replica and the later save
// replaces the earlier — losing every operation the other held and this one
// never saw. Measured, in
// TestTwoServersOverOneStoreLoseTheEarlierSave: one writes AAAA, the other
// BBBB, and the store ends holding BBBB alone. There is no error anywhere,
// because each save did exactly what Save is documented to do.
//
// It is the sequential case that works, and it is the one that makes this
// tempting: a server that starts, loads, serves and stops hands the next server
// everything, so a rolling restart is fine and a failover to a cold standby is
// fine. What is not fine is two of them up at once.
//
// There are two supported ways to run more than one server, and both replace
// the shared store rather than adding to it:
//
//   - Federation. [Server.Follow] and [Server.FollowWithRetry] make one server
//     a participant in another's document, so the operations travel and each
//     server's own store holds the union. The cost is that both servers are up
//     and reachable — and that a link is trusted for every site it carries, so
//     both ends must be yours. See "A site identity is claimed, not proved".
//   - A gitstore with a remote. Each server has its own repository and pulls the
//     other's, merging snapshots rather than replacing them, with
//     [ErrUnmergeable] when two replicas have each discarded what the other
//     needs. The cost is a pull interval instead of a link, and the benefit is
//     that neither server has to be reachable from the other.
//
// A shared PostgreSQL looks like a third way and is not one: the database is
// shared, the document in memory is not.
package collab
