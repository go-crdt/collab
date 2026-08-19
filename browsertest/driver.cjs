// Headless-browser driver for the WebRTC peer proof.
//
// It loads the page in a real Chrome, forwards everything the page logs, waits
// for the wasm to leave its verdict on globalThis.__result, prints it, and exits
// 0 only if the participants converged. It is CommonJS so that require() finds
// puppeteer-core through NODE_PATH, which ES-module bare imports do not consult.
//
// Env: PAGE_URL (the page), CHROME (the browser binary). NODE_PATH must contain
// a node_modules with puppeteer-core in it.
const puppeteer = require("puppeteer-core");

(async () => {
  const url = process.env.PAGE_URL;
  const executablePath = process.env.CHROME;
  if (!url || !executablePath) {
    console.error("DRIVER_FAIL missing PAGE_URL or CHROME");
    process.exit(2);
  }

  const browser = await puppeteer.launch({
    executablePath,
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-dev-shm-usage",
      // Two peers in one headless page reach each other over loopback host
      // candidates. Chrome otherwise hides local IPs behind mDNS ".local"
      // candidates, which have nothing to resolve them in a headless browser,
      // and the connection never completes.
      "--disable-features=WebRtcHideLocalIpsWithMdns",
    ],
  });
  try {
    const page = await browser.newPage();
    page.on("console", (m) => console.log("[page] " + m.text()));
    page.on("pageerror", (e) => console.log("[pageerror] " + e.message));

    await page.goto(url, { waitUntil: "load", timeout: 30000 });
    const handle = await page.waitForFunction(
      () => globalThis.__result || null,
      { timeout: 40000, polling: 200 },
    );
    const result = await handle.jsonValue();
    console.log("RESULT " + JSON.stringify(result));
    process.exitCode = result && result.ok ? 0 : 1;
  } catch (err) {
    console.error("DRIVER_FAIL " + (err && err.message ? err.message : err));
    process.exitCode = 2;
  } finally {
    await browser.close();
  }
})();
