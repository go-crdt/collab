// Node host for the JavaScript binding, driven by TestTheJavaScriptAPIConverges.
//
// Everything below goes through globalThis.collab and nothing else: this file is
// the page, and a page cannot call Go. It joins the document a native
// participant is already editing, edits text, list and map, checks that what it
// is told about somebody else's edits is addressed in the units it counts, and
// proves that closing gives every callback back.
//
// Env: URL (ws:// server), DOCUMENT (document name), WASM (the built binding),
//      WASM_EXEC (the toolchain's wasm_exec.js).
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const { URL: wsURL, DOCUMENT, WASM, WASM_EXEC } = process.env;
if (!wsURL || !DOCUMENT || !WASM || !WASM_EXEC) {
  console.error("WASM_FAIL missing env (URL/DOCUMENT/WASM/WASM_EXEC)");
  process.exit(2);
}

const BODY = "file:main.tex";
const enc = new TextEncoder();
const dec = new TextDecoder();
const bytes = (s) => enc.encode(s);
const text = (b) => dec.decode(b);

const assert = (ok, what) => {
  if (!ok) throw new Error("not true: " + what);
};

const waitFor = async (cond, what, ms = 20000) => {
  const until = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > until) throw new Error("timed out waiting for " + what);
    await new Promise((r) => setTimeout(r, 5));
  }
};

// rejects insists a call fails, and fails as an Error rather than as a null a
// caller could walk past.
const rejects = async (run, what) => {
  let caught = null;
  try {
    await run();
  } catch (e) {
    caught = e;
  }
  assert(caught !== null, what + " should have been refused");
  assert(caught instanceof Error, what + " was refused with something that is not an Error");
  return caught.message;
};

const throwsSync = (run, what) => {
  try {
    run();
  } catch (e) {
    assert(e instanceof Error, what + " threw something that is not an Error");
    return e.message;
  }
  throw new Error(what + " should have thrown");
};

await import(pathToFileURL(WASM_EXEC).href); // defines globalThis.Go
const go = new globalThis.Go();
const { instance } = await WebAssembly.instantiate(await readFile(WASM), go.importObject);
// The binding never returns from main — it is a library, and the page drives it
// — so this promise is only ever a way to hear that the instance died.
go.run(instance).then(
  () => bail("the WebAssembly instance exited"),
  (e) => bail("the WebAssembly instance failed: " + e),
);
await waitFor(() => globalThis.collab, "collab to be installed", 10000);

function bail(msg) {
  console.error("WASM_FAIL " + msg);
  process.exit(1);
}

try {
  console.log("WASM_OK " + JSON.stringify(await run(globalThis.collab)));
  process.exit(0);
} catch (e) {
  bail(e && e.stack ? e.stack : String(e));
}

async function run(collab) {
  const refusals = {};
  const baseline = collab.stats().funcs;

  // A document with no name is refused before a socket is opened for it.
  refusals.document = await rejects(
    () => collab.join({ url: wsURL, document: "", site: 2 }),
    "joining a document with no name",
  );
  refusals.options = await rejects(() => collab.join("ws://nowhere"), "joining with no options");
  refusals.site = await rejects(
    () => collab.join({ url: wsURL, document: DOCUMENT, site: "not a number" }),
    "joining with a site that is not a number",
  );

  const session = await collab.join({ url: wsURL, document: DOCUMENT, site: 2 });
  assert(session.document === DOCUMENT, "the session knows its document");
  // A site is 64 bits and a JavaScript number is not, so it comes back as a
  // decimal string.
  assert(session.site === "2", "the session reports its site as a string");
  assert(collab.stats().sessions === 1, "one session is open");

  const body = await session.text(BODY);
  const chat = await session.list("chat");
  const cells = await session.map("cells");
  refusals.part = await rejects(() => session.text(""), "a part with no name");
  // Asking twice gives the same object back, so a page can hold either.
  assert((await session.text(BODY)) === body, "the same part gives the same handle");

  // The page's own copy of the text, patched only by what onChange reports —
  // which is the only thing an editor can afford to do, and the only thing that
  // proves the offsets are the ones it counts in.
  let view = "";
  const seen = [];
  let peerUpdates = 0;
  let ended = "not yet";

  // Registering and reading the text out is one breath: registering drops
  // whatever the session had recorded, and nothing can arrive between two
  // statements of the same synchronous turn, because a message arrives through a
  // callback and JavaScript does not run in the middle of its own statement.
  const registered = session.onChange((parts) => {
    for (const part of parts) {
      seen.push(part);
      if (part.kind !== "text" || part.name !== BODY) continue;
      for (const edit of part.text) {
        view = view.slice(0, edit.pos) + edit.insert + view.slice(edit.pos + edit.removed);
      }
    }
  });
  view = body.toString();
  await registered;
  await session.onPeers(() => peerUpdates++);
  await session.onClose((why) => {
    ended = why === null ? "we closed it" : "them: " + why.message;
  });
  refusals.handler = await rejects(() => session.onChange(42), "a handler that is not a function");

  assert(view === "A", `the page joined onto ${JSON.stringify(view)}, want "A"`);
  assert(body.length === 1, "one code unit");

  // An emoji, so that runes and code units stop agreeing from here on.
  await body.insert(0, "😀");
  view = "😀" + view; // local edits are not reported: we made them, so we know
  assert(body.toString() === "😀A", "the emoji went in");
  assert(body.length === 3, `length is ${body.length}, want 3 code units`);

  // An offset landing between the emoji's two units names a place no cursor has
  // ever been, and is refused rather than moved to one side of it.
  refusals.surrogate = await rejects(() => body.insert(1, "x"), "an offset inside a character");
  refusals.delete = await rejects(() => body.delete(1, 1), "a deletion inside a character");
  assert(body.toString() === "😀A", "a refused edit changed nothing");

  // An anchor names a character rather than a place, and is reported in the same
  // units. The "A" is at unit 2.
  const anchor = await body.anchor(2);
  assert(typeof anchor.site === "string" && typeof anchor.seq === "string", "an anchor is strings");
  assert((await body.position(anchor)) === 2, "the anchor is where it was put");
  assert((await body.visible(anchor)) === true, "the anchor is visible");
  assert((await body.position({ site: "9", seq: "9" })) === undefined, "an unknown anchor has no position");
  refusals.anchor = await rejects(() => body.position("nonsense"), "an anchor that is not one");

  // A value is bytes in both directions, and nothing else is accepted.
  await chat.append(bytes("on commence"));
  await cells.set("B7", bytes("42"));
  refusals.bytes = await rejects(() => cells.set("B8", "42"), "a map value that is not bytes");
  refusals.listBytes = await rejects(() => chat.append("aussi"), "a list value that is not bytes");
  await session.setCursor({ anchor: 3, head: 3 }, { name: "la page" });
  refusals.cursor = await rejects(() => session.setCursor({ anchor: 0 }), "a cursor with no head");
  refusals.meta = await rejects(
    () => session.setCursor({ anchor: 0, head: 0 }, { name: 7 }),
    "cursor metadata that is not strings",
  );

  // The native participant now appends and then deletes the emoji, so that the
  // page is told about two edits whose offsets differ between runes and units.
  await waitFor(
    () => body.toString() === "AZ" && cells.size === 2 && chat.length === 2,
    "the native participant's round",
  );

  const edits = seen.filter((p) => p.kind === "text").flatMap((p) => p.text);
  // "Z" went to the end of "😀A" — unit 3, and rune 2. Reporting the rune would
  // have put it inside the emoji.
  assert(edits[0].pos === 3 && edits[0].removed === 0 && edits[0].insert === "Z",
    `the append was reported as ${JSON.stringify(edits[0])}, want {pos:3,removed:0,insert:"Z"}`);
  // And the emoji is one character but two units gone.
  assert(edits[1].pos === 0 && edits[1].removed === 2 && edits[1].insert === "",
    `the deletion was reported as ${JSON.stringify(edits[1])}, want {pos:0,removed:2}`);
  assert(view === body.toString(),
    `applying only what we were told gives ${JSON.stringify(view)}, but the document holds ${JSON.stringify(body.toString())}`);

  // A map names the keys that changed; a list says only that it moved, which is
  // deliberate — a view of one reads it back whole.
  const mapChange = seen.find((p) => p.kind === "map");
  assert(mapChange && mapChange.keys.join() === "C8", "the map named the key that changed");
  const listChange = seen.find((p) => p.kind === "list");
  assert(listChange && listChange.text === undefined && listChange.keys === undefined,
    "the list reported that it moved and nothing else");

  assert(text(await chat.get(1)) === "depuis le serveur", "the server's message arrived");
  assert(chat.values().map(text).join("|") === "on commence|depuis le serveur", "both messages, in order");
  assert(text(await cells.get("C8")) === "7", "the server's cell arrived");
  assert((await cells.has("C8")) === true, "the key is there");
  assert((await cells.has("Z9")) === false, "a key nobody wrote is not");
  assert((await cells.get("Z9")) === undefined, "and reading it is not a failure");
  assert(cells.keys().join() === "B7,C8", "both keys, ascending");
  refusals.index = await rejects(() => chat.get(9), "a list index past the end");

  // The anchor still names the "A", which has not moved in runes and has moved
  // by two units.
  assert((await body.position(anchor)) === 0, "the anchor followed its character");

  const peers = session.peers();
  assert(peers.length === 2, `peers() = ${JSON.stringify(peers)}, want the page and the native one`);
  const native = peers.find((p) => p.site === "1");
  assert(native && native.meta.name === "ada", "the native participant's name came with it");
  assert(native.cursor.anchor === 1 && native.cursor.head === 2, "and where it is");
  assert(peerUpdates > 0, "onPeers was called");

  assert(session.parts().length === 3, "three parts");

  // The page writes one more character, which is what the native participant
  // waits for before it says we are done.
  await body.insert(2, "!");
  view += "!";
  await waitFor(async () => await cells.has("done"), "the native participant to be satisfied");

  const runs = body.authorRuns();
  assert(runs.length === 2, `authorRuns() = ${JSON.stringify(runs)}, want one run per author`);
  assert(runs[0].site === "1" && runs[0].pos === 0 && runs[0].len === 2, "the native participant's run");
  assert(runs[1].site === "2" && runs[1].pos === 2 && runs[1].len === 1, "the page's run");

  const snapshot = session.snapshot();
  assert(snapshot instanceof Uint8Array && snapshot.length > 0, "a snapshot is bytes");

  const finished = { text: body.toString(), view, edits, refusals };

  // Closing gives every callback back. A js.Func that is never released keeps
  // its Go closure — and the whole document behind it — alive for the life of
  // the page, and a page opens one session per document.
  await session.close();
  assert(collab.stats().sessions === 0, "no session is open");
  assert(collab.stats().funcs === baseline,
    `closing left ${collab.stats().funcs - baseline} callbacks behind`);
  assert(collab.stats().rebuilds === 0, "a mirror had to be rebuilt, which is a bug here");
  assert(ended === "we closed it", `onClose said ${JSON.stringify(ended)}`);

  // And a session used after it was closed fails at once, on a read as much as
  // on a write, rather than answering from a document nobody is keeping in step.
  refusals.closedRead = throwsSync(() => body.toString(), "reading a closed session");
  refusals.closedWrite = throwsSync(() => body.length, "the length of a closed session");
  refusals.closedSession = throwsSync(() => session.text(BODY), "reaching into a closed session");

  // A second session, resumed from that snapshot, proves the release was not a
  // one-off and that what the page kept is what it comes back with.
  const again = await collab.join({ url: wsURL, document: DOCUMENT, site: 3, resume: snapshot });
  const againBody = await again.text(BODY);
  assert(againBody.toString() === finished.text, "the resumed session holds what was snapshotted");
  assert((await collab.deriveSite("un jeton")) === (await collab.deriveSite("un jeton")),
    "deriveSite is a function of what it is given");
  await again.close();
  assert(collab.stats().funcs === baseline, "the second session was released too");

  return finished;
}
