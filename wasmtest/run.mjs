// Node host for the end-to-end proof. Loads the Go wasm glue, instantiates the
// client, points it at the server, and prints WASM_OK / WASM_FAIL plus the text
// each WebAssembly replica converged on, for TestWasmConverges to assert.
//
// Env: URL (ws:// server), DOCUMENT (document name), WASM (client.wasm),
//      WASM_EXEC (the toolchain's wasm_exec.js).
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const { URL: wsURL, DOCUMENT, WASM, WASM_EXEC } = process.env;
if (!wsURL || !DOCUMENT || !WASM || !WASM_EXEC) {
  console.error("WASM_FAIL missing env (URL/DOCUMENT/WASM/WASM_EXEC)");
  process.exit(2);
}

await import(pathToFileURL(WASM_EXEC).href); // defines globalThis.Go
globalThis.__collabURL = wsURL;
globalThis.__collabDocument = DOCUMENT;

const go = new globalThis.Go();
const { instance } = await WebAssembly.instantiate(await readFile(WASM), go.importObject);
await go.run(instance); // resolves when wasm main() returns

const res = globalThis.__collabResult;
if (res && res.ok) {
  console.log("WASM_OK " + JSON.stringify({ first: res.first, second: res.second }));
  process.exit(0);
}
console.error("WASM_FAIL " + (res ? res.msg : "no result"));
process.exit(1);
