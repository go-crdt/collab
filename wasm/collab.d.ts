// Type declarations for globalThis.collab, installed by the WebAssembly binding
// in this directory.
//
// Two conventions run through the whole of it, and both are deliberate:
//
//   - Every offset and length is in UTF-16 code units, the units a JavaScript
//     string counts in. An offset falling between the two units of one
//     character is refused rather than rounded: half of an emoji is not a place
//     a cursor has ever been.
//   - A value is bytes. A map value and a list value are Uint8Array in both
//     directions, never strings and never JSON, because the CRDT underneath
//     does not interpret them and neither does this.
//
// A failure is a rejected promise or a thrown Error, never a null return.

declare global {
  const collab: Collab;
}

export interface Collab {
  /** Opens a session. The promise settles when the document has arrived. */
  join(options: JoinOptions): Promise<Session>;
  /**
   * Turns whatever names a participant — a session token, a tab identifier —
   * into a replica identity, as a decimal string because it does not fit a
   * JavaScript number.
   */
  deriveSite(from: string): Promise<string>;
  /** What this binding is holding, for a page or a test that wants to prove it is not leaking. */
  stats(): Stats;
}

export interface JoinOptions {
  /** "ws://" or "wss://" and the path the server's handler is mounted at. */
  url: string;
  /** The document to join, created if it does not exist. */
  document: string;
  /**
   * This participant's replica identity, distinct from every other
   * participant's in the document. A number or a decimal string; see
   * `deriveSite`.
   */
  site: number | string;
  /** A snapshot from an earlier session, to keep work done while disconnected. */
  resume?: Uint8Array;
  /** How long a join may take before it is abandoned. Defaults to 20 000. */
  timeoutMs?: number;
}

export interface Stats {
  /** How many sessions are open. */
  sessions: number;
  /** How many callbacks JavaScript is holding on Go's behalf. Closing every session returns this to where it started. */
  funcs: number;
  /** How many times a mirror had to be rebuilt. A bug in the binding; it should stay at zero. */
  rebuilds: number;
}

export interface Session {
  readonly document: string;
  readonly site: string;

  /** A handle on a text part, created the first time anybody writes to it. */
  text(name: string): Promise<Text>;
  /** A handle on a list part. */
  list(name: string): Promise<List>;
  /** A handle on a map part. */
  map(name: string): Promise<MapPart>;
  /** The parts this replica holds, in the canonical order. */
  parts(): Part[];

  /**
   * Registers what to call when somebody else changes something, with what
   * changed, per part. Pass null to clear it. Local edits are not reported: a
   * caller that made them already knows.
   */
  onChange(handler: ((parts: PartChange[]) => void) | null): Promise<void>;
  /** Registers what to call when the participants change. */
  onPeers(handler: (() => void) | null): Promise<void>;
  /** Registers what to call when the session ends. The reason is null when it was closed here. */
  onClose(handler: ((why: Error | null) => void) | null): Promise<void>;

  /** The other participants and where their cursors are, ordered by site. */
  peers(): Peer[];
  /** Publishes where this participant is. Cursor positions are ephemeral and never persisted. */
  setCursor(cursor: Cursor, meta?: Record<string, string>): Promise<void>;

  /** The whole document, in the form `join`'s `resume` accepts. */
  snapshot(): Uint8Array;
  /** Ends the session and gives every callback back. */
  close(): Promise<void>;
  isClosed(): boolean;
}

export interface Part {
  kind: "text" | "list" | "map";
  name: string;
}

/** What one part did. A part that did nothing is not reported at all. */
export interface PartChange extends Part {
  /** The edits to make, in order. Text parts only. */
  text?: TextEdit[];
  /** The keys whose value or presence changed, ascending. Map parts only. */
  keys?: string[];
  /**
   * A list part fills in neither: that it is reported is the whole of the news.
   * The views written against one read it back whole, and naming positions
   * would be a second protocol to keep correct for nobody.
   */
}

export interface TextEdit {
  /** Where, in UTF-16 code units, against the text as it stands after the edits before this one. */
  pos: number;
  /** How many code units to remove. */
  removed: number;
  /** What to put there. */
  insert: string;
}

export interface Text {
  readonly name: string;
  toString(): string;
  /** The length a JavaScript string would report. */
  readonly length: number;

  insert(pos: number, text: string): Promise<void>;
  delete(pos: number, length: number): Promise<void>;

  /**
   * The identity of the character at pos, which keeps naming that character
   * however the text moves around it. What a comment or a stored selection
   * should hold.
   */
  anchor(pos: number): Promise<Anchor>;
  /** Where the character an anchor names sits now, or undefined for an anchor this replica has never seen. */
  position(anchor: Anchor): Promise<number | undefined>;
  /** Whether the character an anchor names is still in the text. */
  visible(anchor: Anchor): Promise<boolean>;
  /** The visible text split into stretches by who wrote them, for colouring by author. */
  authorRuns(): AuthorRun[];
}

/** Opaque: the object `anchor()` returned, and nothing else. */
export interface Anchor {
  readonly site: string;
  readonly seq: string;
}

export interface AuthorRun {
  site: string;
  /** In UTF-16 code units. */
  pos: number;
  len: number;
}

export interface List {
  readonly name: string;
  readonly length: number;
  get(index: number): Promise<Uint8Array>;
  values(): Uint8Array[];
  insert(index: number, ...values: Uint8Array[]): Promise<void>;
  append(...values: Uint8Array[]): Promise<void>;
  delete(index: number, count: number): Promise<void>;
}

export interface MapPart {
  readonly name: string;
  /** How many keys are present, not counting deleted ones. */
  readonly size: number;
  get(key: string): Promise<Uint8Array | undefined>;
  has(key: string): Promise<boolean>;
  keys(): string[];
  set(key: string, value: Uint8Array): Promise<void>;
  delete(key: string): Promise<void>;
}

export interface Cursor {
  /** In UTF-16 code units. */
  anchor: number;
  head: number;
}

export interface Peer {
  site: string;
  cursor: Cursor;
  meta: Record<string, string>;
}

export {};
