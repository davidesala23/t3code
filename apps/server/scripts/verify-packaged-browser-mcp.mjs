import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { execScriptSource, writeFakeCli } from "@t3tools/provider-testing/fakeCli";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";

function waitForRendererRpc(page, tag, accepts) {
  const sockets = new Map();
  const observedRequestTags = new Set();
  const diagnostics = {
    socketCount: 0,
    sentFrameCount: 0,
    receivedFrameCount: 0,
    sentStringFrameCount: 0,
    sentBufferFrameCount: 0,
    receivedStringFrameCount: 0,
    receivedBufferFrameCount: 0,
    parseFailureCount: 0,
    acceptedRequestCount: 0,
    exitCount: 0,
  };
  let close;
  const promise = new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      close();
      reject(
        new Error(
          `Renderer RPC milestone deadline: ${tag}; ${JSON.stringify({ ...diagnostics, observedRequestTags: [...observedRequestTags].sort() })}`,
        ),
      );
    }, 60_000);
    const onSocket = (socket) => {
      diagnostics.socketCount++;
      const ids = new Set();
      const messages = (frame, direction) => {
        diagnostics[`${direction}FrameCount`]++;
        if (typeof frame.payload !== "string") {
          diagnostics[`${direction}BufferFrameCount`]++;
          return [];
        }
        diagnostics[`${direction}StringFrameCount`]++;
        try {
          const parsed = JSON.parse(frame.payload);
          return Array.isArray(parsed) ? parsed : [parsed];
        } catch {
          diagnostics.parseFailureCount++;
          return [];
        }
      };
      const sent = (frame) => {
        for (const message of messages(frame, "sent")) {
          if (message._tag === "Request" && typeof message.tag === "string")
            observedRequestTags.add(message.tag);
          if (message._tag === "Request" && message.tag === tag && accepts(message.payload)) {
            diagnostics.acceptedRequestCount++;
            ids.add(String(message.id));
          }
        }
      };
      const received = (frame) => {
        for (const message of messages(frame, "received")) {
          if (message._tag === "Exit") diagnostics.exitCount++;
          if (message._tag !== "Exit" || !ids.has(String(message.requestId))) continue;
          close();
          if (message.exit._tag === "Success") resolve();
          else reject(new Error(`Renderer RPC milestone rejected: ${tag}`));
          return;
        }
      };
      sockets.set(socket, { sent, received });
      socket.on("framesent", sent);
      socket.on("framereceived", received);
    };
    close = () => {
      clearTimeout(timer);
      page.off("websocket", onSocket);
      for (const [socket, handlers] of sockets) {
        socket.off("framesent", handlers.sent);
        socket.off("framereceived", handlers.received);
      }
      sockets.clear();
    };
    page.on("websocket", onSocket);
  });
  void promise.catch(() => undefined);
  return { promise, close: () => close() };
}

function captureMcpConfiguration(directory, scriptPath) {
  let close;
  const promise = new Promise((resolve, reject) => {
    const watcher = NodeFS.watch(directory, async () => {
      try {
        const recorded = await NodeFSP.readFile(`${scriptPath}.requests`, "utf8").catch((error) => {
          if (error.code === "ENOENT") return "";
          throw error;
        });
        const config = recorded
          .split("\n")
          .slice(0, -1)
          .map((line) => JSON.parse(line))
          .find((request) => request.method === "thread/start")?.params.config.mcp_servers?.[
          "t3-code"
        ];
        if (!config) return;
        close();
        resolve(config);
      } catch (error) {
        close();
        reject(error);
      }
    });
    const timer = setTimeout(() => {
      close();
      reject(new Error("Provider MCP configuration deadline"));
    }, 60_000);
    close = () => {
      clearTimeout(timer);
      watcher.close();
    };
    watcher.on("error", (error) => {
      close();
      reject(error);
    });
  });
  void promise.catch(() => undefined);
  return { promise, close: () => close() };
}

async function connectRpc(bootstrap) {
  const response = await fetch(new URL("/api/auth/websocket-ticket", bootstrap.httpBaseUrl), {
    method: "POST",
    headers: { authorization: `Bearer ${bootstrap.token}`, "content-type": "application/json" },
    body: "{}",
    signal: AbortSignal.timeout(30_000),
  });
  NodeAssert.ok(response.ok, "Desktop bearer must authorize the real server.");
  const ticket = await response.json();
  const endpoint = new URL("/ws", bootstrap.wsBaseUrl);
  endpoint.searchParams.set("orchestrationProtocol", "2");
  endpoint.searchParams.set("wsTicket", ticket.ticket);
  const socket = new WebSocket(endpoint);
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      socket.close();
      reject(new Error("Packaged RPC connect deadline"));
    }, 30_000);
    socket.addEventListener(
      "open",
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
    socket.addEventListener(
      "error",
      () => {
        clearTimeout(timer);
        socket.close();
        reject(new Error("Packaged RPC connection failed"));
      },
      { once: true },
    );
  });
  const pending = new Map();
  let sequence = 0;
  socket.addEventListener("message", (event) => {
    const decoded = JSON.parse(event.data);
    for (const message of Array.isArray(decoded) ? decoded : [decoded]) {
      if (message._tag === "Ping") {
        socket.send(JSON.stringify({ _tag: "Pong" }));
        continue;
      }
      const request = pending.get(String(message.requestId));
      if (!request || message._tag !== "Exit") continue;
      pending.delete(String(message.requestId));
      clearTimeout(request.timer);
      if (message.exit._tag === "Success") request.resolve(message.exit.value);
      else request.reject(new Error(`Packaged RPC rejected ${request.tag}`));
    }
  });
  return {
    request: (tag, payload) =>
      new Promise((resolve, reject) => {
        const id = String(++sequence);
        const timer = setTimeout(() => {
          pending.delete(id);
          reject(new Error(`Packaged RPC deadline: ${tag}`));
        }, 60_000);
        pending.set(id, { resolve, reject, timer, tag });
        socket.send(JSON.stringify({ _tag: "Request", id, tag, payload, headers: [] }));
      }),
    close: () => {
      socket.close();
      for (const request of pending.values()) {
        clearTimeout(request.timer);
        request.reject(new Error("Packaged RPC closed"));
      }
      pending.clear();
    },
  };
}

export async function verifyPackagedBrowserMcp(input) {
  const checks = input.checks;
  const platform = await Effect.runPromise(HostProcess.Platform);
  const privateRoot = await NodeFSP.mkdtemp(NodePath.join(input.isolatedRoot, "mcp-private-"));
  const fixtureDirectory = NodeURL.fileURLToPath(
    new URL("../src/provider/testFixtures/", import.meta.url),
  );
  const peerPath = NodePath.join(privateRoot, "codexCollabMockPeer.mjs");
  const scriptPath = NodePath.join(privateRoot, "script.json");
  const threadId = NodeCrypto.randomUUID();
  const parkedThreadId = NodeCrypto.randomUUID();
  const projectId = NodeCrypto.randomUUID();
  const instanceId = `browser_fixture_${NodeCrypto.randomUUID()}`;
  const modelSelection = { instanceId, model: "gpt-5.6-sol" };
  const profileId = input.profileId;
  const otherProfileId = input.otherProfileId;
  const nativeTabIds = [];
  let phase = "fixture preparation";
  let rpc;
  let client;
  let providerCreated = false;
  let credentialCapture;
  const rendererMilestones = [];
  const secrets = [];
  const progress = (name) => {
    phase = name;
    console.log(`[packaged-mcp] ${name}`);
  };
  const observeProfileSettings = async (key) => {
    const ids = await input.mainWindow.evaluate(async () => {
      const settings = await window.desktopBridge.getClientSettings();
      return settings?.browserProfiles?.map((profile) => profile.id) ?? [];
    });
    const syntheticIds = ids.filter((id) => id === profileId || id === otherProfileId);
    checks.profileSettingsIds = syntheticIds;
    checks[key] = syntheticIds;
    checks.rendererHash = new URL(input.mainWindow.url()).hash;
  };
  try {
    const peer = await NodeFSP.readFile(
      NodePath.join(fixtureDirectory, "codexCollabMockPeer.mjs"),
      "utf8",
    );
    const anchor = '  if (method === "thread/start") {\n';
    NodeAssert.equal(
      peer.split(anchor).length,
      2,
      "Expected one fixture thread/start recorder site.",
    );
    const recorder = `${anchor}    if (script.recordRequests) NodeFS.appendFileSync(process.env.T3_CODEX_COLLAB_SCRIPT + ".requests", JSON.stringify({method, params: message.params}) + "\\n");\n`;
    await NodeFSP.writeFile(peerPath, peer.replace(anchor, recorder), { mode: 0o600 });
    const wirePath = NodePath.join(fixtureDirectory, "codexMultiAgentWire.json");
    const wire = JSON.parse(await NodeFSP.readFile(wirePath, "utf8"));
    await NodeFSP.copyFile(wirePath, NodePath.join(privateRoot, "codexMultiAgentWire.json"));
    await NodeFSP.writeFile(
      scriptPath,
      JSON.stringify({
        rootThreadId: wire.responses.threadStart.thread.id,
        notifications: [],
        holdTurnOpen: true,
        recordRequests: true,
      }),
      { mode: 0o600 },
    );
    const binaryPath = writeFakeCli({
      directory: privateRoot,
      name: "codex-browser-fixture",
      source:
        'if (process.argv.includes("--version")) { process.stdout.write("codex-cli 0.145.0\\n"); process.exit(0); }\n' +
        execScriptSource({ scriptPath: peerPath, expectedArgs: ["app-server"] }),
    });

    progress("desktop authorization");
    const bootstrap = await input.mainWindow.evaluate(async () => {
      const primary = window.desktopBridge
        .getLocalEnvironmentBootstraps()
        .find((entry) => entry.id === "primary");
      if (!primary?.httpBaseUrl || !primary.wsBaseUrl)
        throw new Error("Primary native backend is unavailable.");
      return {
        httpBaseUrl: primary.httpBaseUrl,
        wsBaseUrl: primary.wsBaseUrl,
        token: await window.desktopBridge.getLocalEnvironmentBearerToken(),
      };
    });
    secrets.push(bootstrap.token);
    NodeAssert.equal(new URL(bootstrap.httpBaseUrl).hostname, "127.0.0.1");
    rpc = await connectRpc(bootstrap);
    const config = await rpc.request("server.getConfig", {});
    const environmentId = config.environment.environmentId;
    checks.serverBrowserCapability = config.environment.capabilities.serverBrowser === true;
    const workspaceRoot = NodePath.join(privateRoot, "workspace");
    await NodeFSP.mkdir(workspaceRoot);

    progress("ordinary provider configuration");
    await rpc.request("server.updateSettings", {
      patch: {},
      providerInstanceMutation: {
        operation: "create",
        instanceId,
        instance: {
          driver: "codex",
          enabled: true,
          environment: [{ name: "T3_CODEX_COLLAB_SCRIPT", value: scriptPath, sensitive: false }],
          config: {
            setupMode: "existing",
            enabled: true,
            binaryPath,
            homePath: NodePath.join(privateRoot, "codex-home"),
            customModels: [modelSelection.model],
          },
        },
      },
    });
    providerCreated = true;
    await rpc.request("projects.mutate", {
      type: "project.create",
      commandId: NodeCrypto.randomUUID(),
      projectId,
      title: "Packaged browser proof",
      workspaceRoot,
    });
    const createThread = (id, title) =>
      rpc.request("orchestration.dispatchCommand", {
        type: "thread.create",
        commandId: NodeCrypto.randomUUID(),
        threadId: id,
        projectId,
        title,
        createdBy: "user",
        creationSource: "web",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
    await createThread(parkedThreadId, "Parked browser proof");

    progress("native profiles and React host");
    await observeProfileSettings("profileSettingsBeforeReloadIds");
    NodeAssert.ok(
      [profileId, otherProfileId].every((id) => checks.profileSettingsIds.includes(id)),
      "The isolated app must start with both configured browser profiles.",
    );
    const profilesReported = waitForRendererRpc(
      input.mainWindow,
      "preview.reportProfiles",
      (payload) =>
        [profileId, otherProfileId].every((id) =>
          payload.profiles?.some((profile) => profile.id === id),
        ),
    );
    rendererMilestones.push(profilesReported);
    const target = new URL(input.mainWindow.url());
    target.hash = `/${encodeURIComponent(environmentId)}/${encodeURIComponent(parkedThreadId)}`;
    await input.mainWindow.goto(target.href);
    await input.mainWindow.reload();
    await input.mainWindow.waitForFunction(
      () => Boolean(document.querySelector("[data-electron-browser-host]")),
      undefined,
      { timeout: 60_000 },
    );
    await observeProfileSettings("profileSettingsAfterReloadIds");
    await profilesReported.promise;
    // Creating this thread after the parked renderer is ready means its native
    // preview host has never mounted. The real attach deadline can fall back.
    await createThread(threadId, "Synthetic browser proof");

    progress("ordinary held-open provider turn");
    credentialCapture = captureMcpConfiguration(privateRoot, scriptPath);
    await rpc.request("orchestration.dispatchCommand", {
      type: "message.dispatch",
      commandId: NodeCrypto.randomUUID(),
      threadId,
      messageId: NodeCrypto.randomUUID(),
      text: "Hold the synthetic provider turn for native browser verification.",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
      modelSelection,
      dispatchMode: { type: "start_immediately" },
    });
    const mcpConfig = await credentialCapture.promise;
    secrets.push(mcpConfig.http_headers.Authorization);
    NodeAssert.equal(new URL(mcpConfig.url).origin, new URL(bootstrap.httpBaseUrl).origin);
    const projection = await rpc.request("orchestration.getThreadProjection", { threadId });
    NodeAssert.ok(
      projection.runs.some((run) => ["preparing", "starting", "running"].includes(run.status)),
      "Ordinary dispatch must create a live run before provider startup.",
    );
    checks.ordinaryProviderRunAndMcpCredential = true;

    client = new Client({ name: "packaged-native-browser-proof", version: "1.0.0" });
    await client.connect(
      new StreamableHTTPClientTransport(new URL(mcpConfig.url), {
        requestInit: { headers: mcpConfig.http_headers },
      }),
    );
    const call = async (name, args = {}) => {
      const result = await client.callTool({ name, arguments: args }, undefined, {
        timeout: 60_000,
      });
      NodeAssert.ok(!result.isError, `Real MCP tool failed: ${name}`);
      if (result.structuredContent !== undefined) return result.structuredContent;
      const text = result.content?.find((part) => part.type === "text")?.text;
      try {
        return JSON.parse(text);
      } catch {
        return text;
      }
    };
    const available = await call("preview_status");
    NodeAssert.ok(
      available.profiles?.some((profile) => profile.id === profileId),
      "The configured native browser profile must be available.",
    );

    progress("genuine MCP desktop timeout fallback");
    const hidden = await call("preview_open", {
      open: false,
      reuseExistingTab: false,
      url: new URL("/public", input.fixtureOrigin).href,
    });
    NodeAssert.ok(hidden.tabId);
    nativeTabIds.push(hidden.tabId);
    NodeAssert.equal(hidden.serverPageMode, "headless", "An unvisited thread must fall back.");
    const hiddenGuest = input.mainWindow.locator(
      `webview[data-preview-server-tab="${hidden.tabId}"]`,
    );
    NodeAssert.equal(
      await hiddenGuest.count(),
      0,
      "Hidden headless page must have no native guest.",
    );
    const readHeadless = async () =>
      (
        await call("preview_evaluate", {
          tabId: hidden.tabId,
          expression:
            '({pageId: document.querySelector("#page-id").textContent, draft: document.querySelector("#draft").value, result: document.querySelector("#result").textContent})',
        })
      ).value;
    const draft = "unsaved headless draft";
    await call("preview_type", {
      tabId: hidden.tabId,
      selector: "#draft",
      text: draft,
      clear: true,
    });
    await call("preview_click", { tabId: hidden.tabId, selector: "#apply" });
    const beforeReveal = await readHeadless();
    NodeAssert.ok(beforeReveal.pageId);
    NodeAssert.equal(beforeReveal.draft, draft);
    NodeAssert.equal(beforeReveal.result, draft);
    checks.genuineMcpDesktopTimeoutFallback = true;

    progress("React reveal of the same headless page");
    const targetListed = waitForRendererRpc(
      input.mainWindow,
      "preview.list",
      (payload) => payload.threadId === threadId,
    );
    rendererMilestones.push(targetListed);
    target.hash = `/${encodeURIComponent(environmentId)}/${encodeURIComponent(threadId)}`;
    await input.mainWindow.goto(target.href);
    await input.mainWindow.reload();
    await targetListed.promise;
    await input.mainWindow
      .locator('[data-chat-header] button[aria-label="Thread actions for Synthetic browser proof"]')
      .waitFor({ state: "visible" });
    const shown = await call("preview_status", { tabId: hidden.tabId });
    NodeAssert.equal(shown.tabId, hidden.tabId);
    NodeAssert.equal(shown.serverPageMode, "headless");
    await input.mainWindow.keyboard.press(
      platform === "darwin" ? "Meta+Shift+j" : "Control+Shift+j",
    );
    await input.mainWindow
      .locator(`[data-server-browser-surface="${hidden.tabId}"] canvas`)
      .waitFor({ state: "visible", timeout: 30_000 });
    await input.mainWindow
      .getByRole("button", { name: "Float preview over chat", exact: true })
      .click();
    const surface = input.mainWindow.locator(
      `[data-preview-mini-player="browser:${hidden.tabId}"] [data-server-browser-surface="${hidden.tabId}"]`,
    );
    await surface.locator("canvas").waitFor({ state: "visible", timeout: 30_000 });
    await input.mainWindow.waitForFunction(
      (tabId) => {
        const canvas = document.querySelector(
          `[data-preview-mini-player="browser:${tabId}"] [data-server-browser-surface="${tabId}"] canvas`,
        );
        return canvas?.getContext("2d")?.getImageData(0, 0, 1, 1).data[3] > 0;
      },
      hidden.tabId,
      { timeout: 30_000 },
    );
    NodeAssert.equal(await hiddenGuest.count(), 0, "Revealed headless page must remain streamed.");
    NodeAssert.deepEqual(
      await readHeadless(),
      beforeReveal,
      "Reveal must preserve the page and draft.",
    );
    checks.genuineMcpHeadlessRevealPreservesPage = true;

    // Observe the fixture's normal input event over HTTP before releasing
    // control, so release cannot cancel keyboard work still queued remotely.
    await call("preview_click", { tabId: hidden.tabId, selector: "#draft" });
    const inputPathname = `/input-${NodeCrypto.randomUUID()}`;
    const typed = input.waitForFixtureRequest(inputPathname);
    try {
      await call("preview_evaluate", {
        tabId: hidden.tabId,
        expression: `(() => { const input = document.querySelector("#draft"); const onInput = () => { if (input.value !== ${JSON.stringify(`${draft}-human`)}) return; input.removeEventListener("input", onInput); void fetch(${JSON.stringify(inputPathname)}).catch(() => undefined); }; input.addEventListener("input", onInput); return true; })()`,
      });
      await surface.getByRole("button", { name: "Take control", exact: true }).click();
      const release = surface.getByRole("button", { name: "Release control", exact: true });
      await release.waitFor({ state: "visible" });
      const keyboard = surface.getByRole("textbox", { name: "Browser page", exact: true });
      await keyboard.focus();
      await keyboard.press("End");
      await keyboard.pressSequentially("-human");
      await typed.promise;
      await release.click();
    } finally {
      typed.close();
    }
    await surface
      .getByRole("button", { name: "Take control", exact: true })
      .waitFor({ state: "visible" });
    const afterTyping = await readHeadless();
    NodeAssert.equal(afterTyping.pageId, beforeReveal.pageId);
    NodeAssert.equal(afterTyping.draft, `${draft}-human`);
    NodeAssert.equal(afterTyping.result, draft);
    checks.genuineMcpHeadlessReactKeyboardInput = true;
    await input.mainWindow.screenshot({
      path: NodePath.join(input.outputDir, "genuine-mcp-headless.png"),
    });
    await call("t3_preview_close", { tabId: hidden.tabId });
    nativeTabIds.splice(nativeTabIds.indexOf(hidden.tabId), 1);

    const open = async (profile, pathname) => {
      const status = await call("preview_open", {
        profileId: profile,
        open: true,
        reuseExistingTab: false,
        url: new URL(pathname, input.fixtureOrigin).href,
      });
      NodeAssert.ok(status.tabId);
      nativeTabIds.push(status.tabId);
      NodeAssert.equal(
        status.serverPageMode,
        "native",
        "Explicit profile must use its native page.",
      );
      const guest = input.mainWindow.locator(`webview[data-preview-server-tab="${status.tabId}"]`);
      await guest.waitFor({ state: "attached", timeout: 30_000 });
      const nativeDraft = `native input ${pathname}`;
      await call("preview_type", {
        tabId: status.tabId,
        selector: "#draft",
        text: nativeDraft,
        clear: true,
      });
      await call("preview_click", { tabId: status.tabId, selector: "#apply" });
      const expression =
        '({auth: document.querySelector("#auth")?.textContent, pageId: document.querySelector("#page-id")?.textContent, draft: document.querySelector("#draft").value, result: document.querySelector("#result").textContent})';
      const evaluated = await call("preview_evaluate", {
        tabId: status.tabId,
        expression,
      });
      const value = evaluated.value;
      const visible = await guest.evaluate(
        (element, expression) => element.executeJavaScript(expression),
        expression,
      );
      NodeAssert.deepEqual(
        value,
        visible,
        "Real MCP and the React-mounted native guest must see one page.",
      );
      NodeAssert.equal(value.draft, nativeDraft);
      NodeAssert.equal(value.result, nativeDraft);
      return { tabId: status.tabId, value };
    };

    progress("genuine MCP native login");
    const loggedIn = await open(profileId, "/login");
    NodeAssert.equal(loggedIn.value.auth, "signed-in");
    await call("t3_preview_close", { tabId: loggedIn.tabId });
    nativeTabIds.splice(nativeTabIds.indexOf(loggedIn.tabId), 1);
    const restored = await open(profileId, "/protected");
    NodeAssert.equal(restored.value.auth, "signed-in");
    checks.genuineMcpNativeCookieReuse = true;
    checks.genuineMcpAndReactShareNativePage = true;
    checks.genuineMcpNativeTypeAndClick = true;
    progress("genuine MCP profile isolation");
    const other = await open(otherProfileId, "/protected");
    NodeAssert.equal(other.value.auth, "signed-out");
    checks.genuineMcpNativeProfileIsolation = true;
    await input.mainWindow.screenshot({
      path: NodePath.join(input.outputDir, "genuine-mcp-native.png"),
    });
    for (const tabId of nativeTabIds.splice(0)) await call("t3_preview_close", { tabId });
    checks.genuineMcpTabsClosed = true;
    return { checks, threadId, nativeTabIds: [] };
  } catch (error) {
    await observeProfileSettings("profileSettingsAtFailureIds").catch(() => {
      checks.profileSettingsFailureReadbackFailed = true;
    });
    let detail = String(error.message ?? error);
    for (const secret of secrets) if (secret) detail = detail.replaceAll(secret, "[redacted]");
    detail = detail.replace(/Bearer\s+[^\s"'<>]+/g, "Bearer [redacted]");
    throw new Error(`Packaged MCP failed during ${phase}: ${detail}`, { cause: error });
  } finally {
    if (client) {
      for (const tabId of nativeTabIds)
        await client
          .callTool({ name: "t3_preview_close", arguments: { tabId } })
          .catch(() => undefined);
      await client.close().catch(() => undefined);
    }
    if (rpc) {
      const projection = await rpc
        .request("orchestration.getThreadProjection", { threadId })
        .catch(() => null);
      for (const run of projection?.runs ?? [])
        if (["running", "starting", "queued"].includes(run.status))
          await rpc
            .request("orchestration.dispatchCommand", {
              type: "run.interrupt",
              commandId: NodeCrypto.randomUUID(),
              threadId,
              runId: run.id,
            })
            .catch(() => undefined);
      for (const session of projection?.providerSessions ?? [])
        await rpc
          .request("orchestration.dispatchCommand", {
            type: "provider-session.detach",
            commandId: NodeCrypto.randomUUID(),
            threadId,
            providerSessionId: session.id,
            reason: "Packaged browser proof finished",
          })
          .catch(() => undefined);
      if (providerCreated)
        await rpc
          .request("server.updateSettings", {
            patch: {},
            providerInstanceMutation: { operation: "remove", instanceId },
          })
          .catch(() => undefined);
      rpc.close();
    }
    credentialCapture?.close();
    for (const milestone of rendererMilestones) milestone.close();
  }
}
