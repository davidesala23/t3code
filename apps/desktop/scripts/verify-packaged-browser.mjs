import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeHttp from "node:http";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts";
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
NodeAssert.ok(["win32", "darwin"].includes(platform), "Use a Windows or macOS package.");
if (platform === "darwin") {
  NodeAssert.equal(process.env.RUNNER_ENVIRONMENT, "github-hosted");
  NodeAssert.equal(process.env.RUNNER_OS, "macOS");
}
const output = NodePath.resolve(process.argv[3] ?? "native-browser-evidence");
await NodeFSP.mkdir(output, { recursive: true });
const isolated = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-native-browser-proof-"));
const appData = NodePath.join(isolated, "app-data");
const serverHome = NodePath.join(isolated, "t3-home");
const profileId = `native-${NodeCrypto.randomUUID()}`;
const otherProfileId = `other-${NodeCrypto.randomUUID()}`;
await NodeFSP.mkdir(appData, { recursive: true });
const profileLink =
  platform === "darwin"
    ? {
        path: NodePath.join(NodeOS.homedir(), "Library", "Application Support", "t3code-v2"),
        target: NodePath.join(appData, "t3code-v2"),
      }
    : undefined;
if (profileLink) {
  const exists = await NodeFSP.lstat(profileLink.path).then(
    () => true,
    (error) => {
      if (error.code === "ENOENT") return false;
      throw error;
    },
  );
  NodeAssert.equal(exists, false, "The hosted Mac must have no existing T3 profile.");
  await NodeFSP.mkdir(NodePath.dirname(profileLink.path), { recursive: true });
  await NodeFSP.mkdir(profileLink.target);
  await NodeFSP.symlink(profileLink.target, profileLink.path, "dir");
}
await NodeFSP.mkdir(NodePath.join(serverHome, "userdata"), { recursive: true });
await NodeFSP.writeFile(
  NodePath.join(serverHome, "userdata", "client-settings.json"),
  JSON.stringify({
    ...DEFAULT_CLIENT_SETTINGS,
    onboardingCompletedAt: new Date().toISOString(),
    confirmQuit: "direct",
    browserAutoShowFloatingPreview: false,
    browserProfiles: [
      ...DEFAULT_CLIENT_SETTINGS.browserProfiles,
      { id: profileId, name: "Native proof", kind: "persistent" },
      { id: otherProfileId, name: "Other proof", kind: "persistent" },
    ],
  }),
  { mode: 0o600 },
);
const environment = Object.fromEntries(
  Object.entries(process.env).filter(([key]) =>
    /^(PATH|HOME|USER|LOGNAME|SHELL|TMPDIR|LANG|LC_ALL|LC_CTYPE|__CF_USER_TEXT_ENCODING|SYSTEMROOT|WINDIR|COMSPEC|PATHEXT|USERPROFILE|LOCALAPPDATA|TEMP|TMP|PROCESSOR_ARCHITECTURE|NUMBER_OF_PROCESSORS)$/i.test(
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
  const fixtureUrl = new URL(request.url, "http://fixture.invalid");
  const pathname = fixtureUrl.pathname;
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
  const state = JSON.stringify(fixtureUrl.searchParams.get("state"));
  let oauth = "";
  if (pathname === "/oauth/start") {
    oauth = `<button id="oauth-open">Sign in with fixture</button><output id="oauth-result">pending</output><script>
      const state = ${state};
      document.querySelector('#oauth-open').onclick = () => {
        const url = new URL('/oauth/authorize', location.origin);
        url.searchParams.set('state', state);
        window.fixtureOAuthPopup = window.open(url.href, 'fixture-oauth', 'popup,width=520,height=600');
      };
      window.addEventListener('message', event => {
        if (event.origin !== location.origin || event.source !== window.fixtureOAuthPopup || event.data?.type !== 'fixture-oauth' || event.data.state !== state) return;
        document.querySelector('#oauth-result').textContent = 'complete';
      });
    </script>`;
  }
  if (pathname === "/oauth/authorize") {
    response.setHeader(
      "set-cookie",
      "native_fixture_oauth=synthetic-login; Path=/; HttpOnly; SameSite=Lax",
    );
    oauth = `<p id="opener-present"></p><button id="oauth-finish">Finish sign in</button><script>
      document.querySelector('#opener-present').textContent = window.opener ? 'present' : 'missing';
      document.querySelector('#oauth-finish').onclick = () => {
        window.opener.postMessage({type:'fixture-oauth', state:${state}}, location.origin);
        window.close();
      };
    </script>`;
  }
  if (pathname === "/oauth/protected") {
    const authenticated = /(?:^|;\s*)native_fixture_oauth=synthetic-login(?:;|$)/.test(
      request.headers.cookie ?? "",
    );
    oauth = `<p id="oauth-auth">${authenticated ? "signed-in" : "signed-out"}</p>`;
  }
  response.end(
    `<!doctype html><title>Native browser verification</title><p id="auth">${signedIn ? "signed-in" : "signed-out"}</p><p id="page-id">${NodeCrypto.randomUUID()}</p><label>Draft <input id="draft"></label><button id="apply" onclick="document.querySelector('#result').textContent=document.querySelector('#draft').value">Apply</button><output id="result"></output>${oauth}`,
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
    recordVideo: { dir: NodePath.join(output, "video"), size: { width: 1024, height: 720 } },
    timeout: 60_000,
  });
  const window = await application.firstWindow();
  await window.waitForFunction(
    () =>
      Boolean(
        window.desktopBridge?.preview &&
        window.desktopBridge.getLocalEnvironmentBootstraps().find((entry) => entry.id === "primary")
          ?.httpBaseUrl,
      ),
    undefined,
    { timeout: 60_000 },
  );
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
  identity.canonicalUserData = await NodeFSP.realpath(identity.userData);
  const relativeProfile = NodePath.relative(
    await NodeFSP.realpath(appData),
    identity.canonicalUserData,
  );
  NodeAssert.equal(relativeProfile.startsWith(".."), false);
  NodeAssert.notEqual(relativeProfile, "");
  NodeAssert.equal(NodePath.isAbsolute(relativeProfile), false);
  if (profileLink)
    NodeAssert.equal(identity.canonicalUserData, await NodeFSP.realpath(profileLink.target));
  report.checks.packagedIdentityAndIsolation = identity;

  const httpBaseUrl = await window.evaluate(
    () =>
      window.desktopBridge.getLocalEnvironmentBootstraps().find((entry) => entry.id === "primary")
        .httpBaseUrl,
  );
  const descriptorResponse = await fetch(new URL("/.well-known/t3/environment", httpBaseUrl));
  NodeAssert.equal(descriptorResponse.ok, true);
  const { environmentId } = await descriptorResponse.json();
  NodeAssert.ok(environmentId);
  const runtimeTabId = `packaged-proof-${NodeCrypto.randomUUID()}`;
  const serverTab = { threadId: NodeCrypto.randomUUID(), tabId: NodeCrypto.randomUUID() };
  const guestId = await window.evaluate(
    async (input) => {
      const bridge = window.desktopBridge.preview;
      const config = await bridge.getPreviewConfig(input.environmentId, input.profileId);
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
      await bridge.navigate(input.runtimeTabId, input.loginUrl);
      await bridge.navigate(input.runtimeTabId, input.url);
      return id;
    },
    {
      runtimeTabId,
      serverTab,
      environmentId,
      profileId,
      loginUrl: new URL("/login", origin).href,
      url: new URL("/protected", origin).href,
    },
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
      const authentication = await guest.debugger.sendCommand("Runtime.evaluate", {
        expression: 'document.querySelector("#auth").textContent',
        returnByValue: true,
      });
      return {
        type: guest.getType(),
        debuggerAttached: guest.debugger.isAttached(),
        value: result.result.value,
        authentication: authentication.result.value,
      };
    },
    { guestId },
  );
  NodeAssert.equal(controlled.value, "same native page");
  NodeAssert.equal(controlled.authentication, "signed-in");
  NodeAssert.ok(
    requests.includes("/login") && requests.includes("/protected"),
    "The native profile must retain the fixture cookie before hidden recovery is tested.",
  );
  report.checks.productionPreviewGuestAndCdp = controlled;
  report.checks.nativeProfileCookieSeededBeforeHiddenRecovery = true;
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
    application,
    mainWindow: window,
    isolatedRoot: isolated,
    fixtureOrigin: origin,
    outputDir: output,
    checks: report.checks,
    profileId,
    otherProfileId,
    waitForFixtureRequest,
    getFixtureRequests: () => [...requests],
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
  report.fixtureRequests = requests;
  await NodeFSP.writeFile(
    NodePath.join(output, "report.json"),
    JSON.stringify(report, null, 2) + "\n",
  );
  console.log(JSON.stringify(report));
  if (application) await application.close().catch(() => undefined);
  await new Promise((resolve) => fixture.close(resolve));
  if (profileLink) {
    NodeAssert.ok((await NodeFSP.lstat(profileLink.path)).isSymbolicLink());
    NodeAssert.equal(await NodeFSP.readlink(profileLink.path), profileLink.target);
    await NodeFSP.unlink(profileLink.path);
  }
  await NodeFSP.rm(isolated, { recursive: true, force: true });
}
