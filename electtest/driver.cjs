// Headless-browser driver for the host-election proof.
//
// Two pages, one room, and the question is how many of them host. A one-page
// driver cannot ask it: a Web Lock is exclusive across tabs and invisible within
// one. It is CommonJS so require() finds puppeteer-core through NODE_PATH.
//
// Env: PAGE_URL, CHROME. NODE_PATH must hold puppeteer-core.
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
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });
  try {
    const open = async (tag) => {
      const page = await browser.newPage();
      page.on("console", (m) => console.log(`[${tag}] ` + m.text()));
      page.on("pageerror", (e) => console.log(`[${tag} error] ` + e.message));
      await page.goto(url, { waitUntil: "load", timeout: 30000 });
      return page;
    };
    // Both opened before either is read, so they elect against each other rather
    // than one finding a settled room.
    const a = await open("a");
    const b = await open("b");

    const roleOf = async (page, tag) => {
      const handle = await page.waitForFunction(
        () => globalThis.__role || globalThis.__err || null,
        { timeout: 40000, polling: 100 },
      );
      const v = await handle.jsonValue();
      console.log(`[${tag}] settled as ` + JSON.stringify(v));
      return v;
    };
    const [ra, rb] = await Promise.all([roleOf(a, "a"), roleOf(b, "b")]);
    const roles = [ra, rb];
    const hosts = roles.filter((r) => r === "host").length;
    const locks = await a.evaluate(() => !!(navigator.locks && navigator.locks.request));
    const result = {
      ok: hosts === 1,
      hosts,
      roles,
      locksAvailable: locks,
      detail: hosts === 1
        ? "exactly one tab hosts, with a zero election window"
        : `${hosts} tabs host; a zero window means the fallback election was taken`,
    };
    console.log("RESULT " + JSON.stringify(result));
    process.exitCode = result.ok ? 0 : 1;
  } catch (err) {
    console.error("DRIVER_FAIL " + (err && err.message ? err.message : err));
    process.exitCode = 2;
  } finally {
    await browser.close();
  }
})();
