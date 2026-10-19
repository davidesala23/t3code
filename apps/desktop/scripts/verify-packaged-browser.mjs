import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeHttp from "node:http";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import { _electron } from "playwright-core";
import { verifyPackagedBrowserMcp } from "../../server/scripts/verify-packaged-browser-mcp.mjs";

const Asar = NodeModule.createRequire(new URL("../../../scripts/package.json", import.meta.url))(
  "@electron/asar",
);
const executablePath = process.argv[2];
NodeAssert.ok(executablePath, "Pass the packaged executable path.");
const platform = await Effect.runPromise(HostProcess.Platform);
NodeAssert.equal(platform, "win32", "This first preflight requires Windows isolation.");
const output = NodePath.resolve(process.argv[3] ?? "native-browser-evidence");
await NodeFSP.mkdir(output, { recursive: true });
const isolated = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-native-browser-proof-"));
const appData = NodePath.join(isolated, "app-data");
const serverHome = NodePath.join(isolated, "t3-home");
await NodeFSP.mkdir(appData, { recursive: true });
const environment = Object.fromEntries(
  Object.entries(process.env).filter(([key]) =>
    /^(PATH|SYSTEMROOT|WINDIR|COMSPEC|PATHEXT|USERPROFILE|LOCALAPPDATA|TEMP|TMP|PROCESSOR_ARCHITECTURE|NUMBER_OF_PROCESSORS)$/i.test(
      key,
    ),
  ),
);
Object.assign(environment, {
  APPDATA: appData,
  T3CODE_HOME: serverHome,
  T3CODE_DISABLE_AUTO_UPDATE: "1",
});
const sourceHead = NodeChildProcess.execFileSync("git", ["rev-parse", "HEAD"], {
  encoding: "utf8",
}).trim();
const requests = [];
const fixture = NodeHttp.createServer((request, response) => {
  const pathname = new URL(request.url, "http://fixture.invalid").pathname;
  requests.push(pathname);
  response.setHeader("content-type", "text/html; charset=utf-8");
  const signedIn =
    pathname === "/login" ||
    /(?:^|;\s*)native_fixture_session=synthetic-login(?:;|$)/.test(request.headers.cookie ?? "");
  if (pathname === "/login") {
    response.setHeader(
      "set-cookie",
      "native_fixture_session=synthetic-login; Path=/; HttpOnly; SameSite=Lax",
    );
  }
  response.end(
    `<!doctype html><title>Native browser verification</title><p id="auth">${signedIn ? "signed-in" : "signed-out"}</p><p id="page-id">${NodeCrypto.randomUUID()}</p><label>Draft <input id="draft"></label><button id="apply" onclick="document.querySelector('#result').textContent=document.querySelector('#draft').value">Apply</button><output id="result"></output>`,
  );
});
const waitForFixtureRequest = (pathname) => {
  const completion = Promise.withResolvers();
  const received = (request) => {
    if (new URL(request.url, "http://fixture.invalid").pathname !== pathname) return;
    close();
    completion.resolve();
  };
  const timer = setTimeout(() => {
    close();
    completion.reject(new Error("The fixture did not observe the browser input."));
  }, 30_000);
  const close = () => {
    clearTimeout(timer);
    fixture.off("request", received);
  };
  fixture.on("request", received);
  void completion.promise.catch(() => undefined);
  return { promise: completion.promise, close };
};
await new Promise((resolve, reject) => {
  fixture.once("error", reject);
  fixture.listen(0, "127.0.0.1", resolve);
});
const origin = `http://127.0.0.1:${fixture.address().port}`;
const report = {
  sourceHead,
  verifierHead: process.env.GITHUB_SHA ?? sourceHead,
  platform,
  scope: "packaged native environment preflight",
  checks: {},
  acceptanceComplete: false,
};
let application;
try {
  application = await _electron.launch({
    executablePath: NodePath.resolve(executablePath),
    env: environment,
    timeout: 60_000,
  });
  const window = await application.firstWindow();
  await window.waitForFunction(() => Boolean(window.desktopBridge?.preview), undefined, {
    timeout: 60_000,
  });
  const identity = await application.evaluate(({ app }) => ({
    packaged: app.isPackaged,
    userData: app.getPath("userData"),
    archive: app.getAppPath(),
  }));
  identity.commit = JSON.parse(
    Asar.extractFile(identity.archive, "package.json").toString(),
  ).t3codeCommitHash;
  NodeAssert.equal(identity.packaged, true);
  NodeAssert.equal(
    identity.commit,
    sourceHead.slice(0, 12),
    "Packaged app must match the checked-out commit.",
  );
  NodeAssert.equal(NodePath.relative(appData, identity.userData).startsWith(".."), false);
  NodeAssert.notEqual(NodePath.relative(appData, identity.userData), "");
  NodeAssert.equal(NodePath.isAbsolute(NodePath.relative(appData, identity.userData)), false);
  report.checks.packagedIdentityAndIsolation = identity;

  const runtimeTabId = `packaged-proof-${NodeCrypto.randomUUID()}`;
  const serverTab = { threadId: NodeCrypto.randomUUID(), tabId: NodeCrypto.randomUUID() };
  const guestId = await window.evaluate(
    async (input) => {
      const bridge = window.desktopBridge.preview;
      const config = await bridge.getPreviewConfig(input.environmentId, "default");
      await bridge.createTab(input.runtimeTabId, { serverTab: input.serverTab });
      const guest = document.createElement("webview");
      guest.id = "packaged-native-proof";
      guest.setAttribute("partition", config.partition);
      guest.setAttribute("webpreferences", config.webPreferences);
      if (config.preloadUrl) guest.setAttribute("preload", config.preloadUrl);
      guest.style.cssText = "position:fixed;inset:60px 20px 20px;z-index:100000;background:white";
      const ready = new Promise((resolve, reject) => {
        guest.addEventListener("dom-ready", resolve, { once: true });
        guest.addEventListener(
          "did-fail-load",
          (event) => reject(new Error(event.errorDescription)),
          { once: true },
        );
      });
      guest.setAttribute("src", "about:blank");
      document.body.append(guest);
      await ready;
      const id = guest.getWebContentsId();
      await bridge.registerWebview(input.runtimeTabId, id);
      await bridge.navigate(input.runtimeTabId, input.url);
      return id;
    },
    { runtimeTabId, serverTab, environmentId: NodeCrypto.randomUUID(), url: origin },
  );

  const controlled = await application.evaluate(
    async ({ webContents }, input) => {
      const guest = webContents.fromId(input.guestId);
      assertNativeGuest(guest);
      function assertNativeGuest(value) {
        if (
          !value ||
          value.getType() !== "webview" ||
          !value.hostWebContents ||
          !value.debugger.isAttached()
        )
          throw new Error("Production preview manager did not attach a native guest.");
      }
      const result = await guest.debugger.sendCommand("Runtime.evaluate", {
        expression:
          'document.querySelector("#draft").value = "same native page"; document.querySelector("#apply").click(); document.querySelector("#result").textContent',
        returnByValue: true,
      });
      return {
        type: guest.getType(),
        debuggerAttached: guest.debugger.isAttached(),
        value: result.result.value,
      };
    },
    { guestId },
  );
  NodeAssert.equal(controlled.value, "same native page");
  NodeAssert.ok(requests.includes("/"), "The native page must request the real HTTP fixture.");
  report.checks.productionPreviewGuestAndCdp = controlled;
  await application.evaluate(({ BrowserWindow }) => BrowserWindow.getAllWindows()[0].minimize());
  const minimizedValue = await application.evaluate(
    async ({ webContents }, input) => {
      const result = await webContents
        .fromId(input.guestId)
        .debugger.sendCommand("Runtime.evaluate", {
          expression: 'document.querySelector("#result").textContent',
          returnByValue: true,
        });
      return result.result.value;
    },
    { guestId },
  );
  NodeAssert.equal(minimizedValue, "same native page");
  report.checks.minimizedGuestStillControllable = true;
  await application.evaluate(({ BrowserWindow }) => {
    const window = BrowserWindow.getAllWindows()[0];
    window.restore();
    window.show();
  });
  await window.screenshot({ path: NodePath.join(output, "native-page.png") });
  await window.evaluate(async (id) => {
    await window.desktopBridge.preview.closeTab(id);
    document.querySelector("#packaged-native-proof").remove();
  }, runtimeTabId);
  report.scope = "packaged native preflight and live MCP browser checks";
  await verifyPackagedBrowserMcp({
    mainWindow: window,
    isolatedRoot: isolated,
    fixtureOrigin: origin,
    outputDir: output,
    checks: report.checks,
    waitForFixtureRequest,
  });
  report.status = "passed";
} catch (error) {
  report.status = "failed";
  report.error = String(error.stack ?? error)
    .replace(/Bearer\s+[^\s"'<>]+/gi, "Bearer [redacted]")
    .replace(/https?:\/\/[^\s"'<>]+/g, (value) => {
      try {
        const url = new URL(value);
        return url.origin + url.pathname;
      } catch {
        return "[redacted URL]";
      }
    });
  process.exitCode = 1;
  const failedWindow = application?.windows()[0];
  if (failedWindow) {
    await failedWindow
      .screenshot({ path: NodePath.join(output, "failed-window.png"), timeout: 5_000 })
      .catch(() => undefined);
  }
} finally {
  await NodeFSP.writeFile(
    NodePath.join(output, "report.json"),
    JSON.stringify(report, null, 2) + "\n",
  );
  console.log(JSON.stringify(report));
  if (application) await application.close().catch(() => undefined);
  await new Promise((resolve) => fixture.close(resolve));
  await NodeFSP.rm(isolated, { recursive: true, force: true });
}
