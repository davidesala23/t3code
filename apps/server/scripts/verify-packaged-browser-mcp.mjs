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
import { chromium } from "playwright-core";

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
  const previewEvents = [];
  const eventWaiters = new Set();
  let previewSubscriptionId;
  let sequence = 0;
  socket.addEventListener("message", (event) => {
    const decoded = JSON.parse(event.data);
    for (const message of Array.isArray(decoded) ? decoded : [decoded]) {
      if (message._tag === "Ping") {
        socket.send(JSON.stringify({ _tag: "Pong" }));
        continue;
      }
      if (message._tag === "Chunk" && String(message.requestId) === previewSubscriptionId) {
        for (const event of message.values) {
          previewEvents.push(event);
          for (const waiter of eventWaiters) {
            if (!waiter.accepts(event)) continue;
            clearTimeout(waiter.timer);
            eventWaiters.delete(waiter);
            waiter.resolve(event);
          }
        }
        socket.send(JSON.stringify({ _tag: "Ack", requestId: message.requestId }));
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
    subscribePreviewEvents: () => {
      previewSubscriptionId = String(++sequence);
      socket.send(
        JSON.stringify({
          _tag: "Request",
          id: previewSubscriptionId,
          tag: "subscribePreviewEvents",
          payload: {},
          headers: [],
        }),
      );
    },
    previewEventCount: () => previewEvents.length,
    waitForPreviewEvent: (accepts, after = 0) => {
      const existing = previewEvents.slice(after).find(accepts);
      if (existing) return Promise.resolve(existing);
      const promise = new Promise((resolve, reject) => {
        const waiter = { accepts, resolve, reject, timer: undefined };
        waiter.timer = setTimeout(() => {
          eventWaiters.delete(waiter);
          reject(new Error("Preview event milestone deadline"));
        }, 30_000);
        eventWaiters.add(waiter);
      });
      void promise.catch(() => undefined);
      return promise;
    },
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
      if (previewSubscriptionId)
        socket.send(JSON.stringify({ _tag: "Interrupt", requestId: previewSubscriptionId }));
      socket.close();
      for (const waiter of eventWaiters) {
        clearTimeout(waiter.timer);
        waiter.reject(new Error("Packaged RPC closed"));
      }
      eventWaiters.clear();
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
  const boundaryErrors = [];
  checks.additionalBoundaryFailures = [];
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
  const boundary = async (name, run) => {
    progress(name);
    try {
      await run();
    } catch (error) {
      boundaryErrors.push(error);
      let message = String(error.message ?? error);
      for (const secret of secrets) if (secret) message = message.replaceAll(secret, "[redacted]");
      message = message
        .replace(/Bearer\s+[^\s"'<>]+/g, "Bearer [redacted]")
        .replace(/https?:\/\/[^\s"'<>]+/g, (value) => {
          try {
            const url = new URL(value);
            return url.origin + url.pathname;
          } catch {
            return "[redacted URL]";
          }
        });
      checks.additionalBoundaryFailures.push({ name, message });
    }
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
    rpc.subscribePreviewEvents();
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

    const verifyAttachFailure = async (closePending) => {
      await rpc.waitForPreviewEvent(
        (event) => event.type === "opened" && event.tabId === hidden.tabId,
      );
      const baseline = await rpc.request("preview.list", { threadId });
      const baselineIds = baseline.sessions.map((session) => session.tabId).sort();
      const eventCursor = rpc.previewEventCount();
      const pathname = `/must-not-visit-${NodeCrypto.randomUUID()}`;
      let settled = false;
      const opening = client
        .callTool(
          {
            name: "preview_open",
            arguments: {
              profileId,
              open: true,
              reuseExistingTab: false,
              url: new URL(pathname, input.fixtureOrigin).href,
            },
          },
          undefined,
          { timeout: 60_000 },
        )
        .then((result) => {
          settled = true;
          return result;
        });
      void opening.catch(() => undefined);
      try {
        const opened = await rpc.waitForPreviewEvent(
          (event) =>
            event.type === "opened" &&
            event.threadId === threadId &&
            event.snapshot.profileId === profileId &&
            !baselineIds.includes(event.tabId),
          eventCursor,
        );
        NodeAssert.equal(
          opened.snapshot.navStatus._tag,
          "Idle",
          "Profile recovery must stage a blank page.",
        );
        if (closePending) {
          NodeAssert.equal(
            settled,
            false,
            "Close must occur while the ordinary MCP open is pending.",
          );
          await rpc.request("preview.close", { threadId, tabId: opened.tabId });
        }
        const result = await opening;
        NodeAssert.equal(
          result.isError,
          true,
          "An unavailable native profile must fail its real MCP open.",
        );
        await rpc.waitForPreviewEvent(
          (event) => event.type === "closed" && event.tabId === opened.tabId,
          eventCursor,
        );
        const remaining = await rpc.request("preview.list", { threadId });
        NodeAssert.deepEqual(
          remaining.sessions.map((session) => session.tabId).sort(),
          baselineIds,
          "Only the attempted blank session may disappear.",
        );
        NodeAssert.equal(
          (await input.getFixtureRequests()).includes(pathname),
          false,
          "The authenticated target must never be requested.",
        );
        const status = await call("preview_status", { tabId: hidden.tabId });
        NodeAssert.equal(status.serverPageMode, "headless");
        NodeAssert.equal(
          status.tabs.some((tab) => tab.tabId === opened.tabId),
          false,
        );
        NodeAssert.deepEqual(
          await readHeadless(),
          beforeReveal,
          "The existing fallback page must survive profile recovery failure.",
        );
        checks[
          closePending
            ? "genuineMcpClosePendingOpenCleanup"
            : "genuineMcpNativeAttachFailureCleanup"
        ] = true;
      } finally {
        const remaining = await rpc.request("preview.list", { threadId });
        for (const session of remaining.sessions)
          if (!baselineIds.includes(session.tabId))
            await rpc.request("preview.close", { threadId, tabId: session.tabId });
        await opening.catch(() => undefined);
      }
    };
    await boundary("hidden native profile attach failure", () => verifyAttachFailure(false));
    await boundary("close while native open is pending", () => verifyAttachFailure(true));

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

    const nativeInputErrors = [];
    checks.nativeInputFailures = [];
    const nativePage = async (profile, pathname) => {
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
      return { status, guest };
    };
    const open = async (profile, pathname) => {
      const { status, guest } = await nativePage(profile, pathname);
      const nativeDraft = `native input ${pathname}`;
      const expression =
        '({auth: document.querySelector("#auth")?.textContent, pageId: document.querySelector("#page-id")?.textContent, draft: document.querySelector("#draft")?.value, result: document.querySelector("#result")?.textContent, activeElementId: document.activeElement?.id, hasFocus: document.hasFocus()})';
      const readNative = async () =>
        (await call("preview_evaluate", { tabId: status.tabId, expression })).value;
      const stages = { pathname, beforeType: await readNative() };
      (checks.nativeInteractionStages ??= []).push(stages);
      await call("preview_type", {
        tabId: status.tabId,
        selector: "#draft",
        text: nativeDraft,
        clear: true,
      });
      stages.afterType = await readNative();
      await call("preview_click", { tabId: status.tabId, selector: "#apply" });
      const value = await readNative();
      stages.afterClick = value;
      const visible = await guest.evaluate(
        (element, expression) => element.executeJavaScript(expression),
        expression,
      );
      NodeAssert.deepEqual(
        value,
        visible,
        "Real MCP and the React-mounted native guest must see one page.",
      );
      for (const [field, message] of [
        ["draft", `Native draft must retain typed input: ${pathname}`],
        ["result", `Native Apply must show the typed input: ${pathname}`],
      ]) {
        try {
          NodeAssert.equal(value[field], nativeDraft, message);
        } catch (error) {
          nativeInputErrors.push(error);
          checks.nativeInputFailures.push({ pathname, message: String(error.message ?? error) });
        }
      }
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
    progress("genuine MCP profile isolation");
    const other = await open(otherProfileId, "/protected");
    NodeAssert.equal(other.value.auth, "signed-out");
    checks.genuineMcpNativeProfileIsolation = true;

    await boundary("synthetic native popup authorization", async () => {
      const state = NodeCrypto.randomUUID();
      const { status: opener } = await nativePage(profileId, `/oauth/start?state=${state}`);
      const initial = (
        await call("preview_evaluate", {
          tabId: opener.tabId,
          expression: 'document.querySelector("#page-id").textContent',
        })
      ).value;
      const windows = new Set(input.application.windows());
      const popupReady = input.application.waitForEvent("window", {
        predicate: (page) => !windows.has(page),
        timeout: 30_000,
      });
      void popupReady.catch(() => undefined);
      let popup;
      try {
        await call("preview_click", { tabId: opener.tabId, selector: "#oauth-open" });
        popup = await popupReady;
        checks.genuineNativePopupWindowOpened = true;
        await popup.waitForURL(
          (url) => url.pathname === "/oauth/authorize" && url.searchParams.get("state") === state,
        );
        NodeAssert.equal(
          await popup.evaluate(() => Boolean(window.opener)),
          true,
          "The real native authorization window must retain its opener.",
        );
        const closed = popup.waitForEvent("close", { timeout: 30_000 });
        void closed.catch(() => undefined);
        await popup.locator("#oauth-finish").click();
        await closed;
        checks.genuineNativePopupWindowClosed = true;
        await call("preview_wait_for", {
          tabId: opener.tabId,
          text: "complete",
          timeoutMs: 30_000,
        });
        const completed = (
          await call("preview_evaluate", {
            tabId: opener.tabId,
            expression:
              '({pageId: document.querySelector("#page-id").textContent, result: document.querySelector("#oauth-result").textContent, popupClosed: window.fixtureOAuthPopup?.closed})',
          })
        ).value;
        NodeAssert.equal(
          completed.pageId,
          initial,
          "Authorization must preserve the original opener page.",
        );
        NodeAssert.equal(completed.result, "complete");
        NodeAssert.equal(completed.popupClosed, true);
        const signedIn = await nativePage(profileId, "/oauth/protected");
        const signedOut = await nativePage(otherProfileId, "/oauth/protected");
        const auth = async (tabId) =>
          (
            await call("preview_evaluate", {
              tabId,
              expression: 'document.querySelector("#oauth-auth").textContent',
            })
          ).value;
        NodeAssert.equal(await auth(signedIn.status.tabId), "signed-in");
        NodeAssert.equal(await auth(signedOut.status.tabId), "signed-out");
        checks.genuineNativeSyntheticPopupOpenerAndCookieIsolation = true;
        await input.mainWindow.screenshot({
          path: NodePath.join(input.outputDir, "genuine-mcp-popup.png"),
        });
      } finally {
        if (popup && !popup.isClosed()) await popup.close().catch(() => undefined);
      }
    });

    await boundary("paired web client streams the desktop native page", async () => {
      const response = await fetch(new URL("/api/auth/pairing-token", bootstrap.httpBaseUrl), {
        method: "POST",
        headers: { authorization: `Bearer ${bootstrap.token}`, "content-type": "application/json" },
        body: JSON.stringify({ label: "Packaged browser web proof" }),
        signal: AbortSignal.timeout(30_000),
      });
      NodeAssert.equal(
        response.ok,
        true,
        "The normal admin API must issue the web pairing credential.",
      );
      const pairing = await response.json();
      secrets.push(pairing.credential);
      const browser = await chromium.launch({ headless: true });
      let surface;
      let remote;
      try {
        const context = await browser.newContext();
        const settings = await input.mainWindow.evaluate(() =>
          window.desktopBridge.getClientSettings(),
        );
        await context.addInitScript(
          ({ origin, settings }) => {
            if (location.origin === origin)
              localStorage.setItem("t3code:client-settings:v1", JSON.stringify(settings));
          },
          { origin: new URL(bootstrap.httpBaseUrl).origin, settings },
        );
        remote = await context.newPage();
        const profiles = waitForRendererRpc(remote, "preview.reportProfiles", (payload) =>
          [profileId, otherProfileId].every((id) =>
            payload.profiles?.some((profile) => profile.id === id),
          ),
        );
        rendererMilestones.push(profiles);
        const pairingUrl = new URL("/", bootstrap.httpBaseUrl);
        pairingUrl.hash = new URLSearchParams({ token: pairing.credential }).toString();
        await remote.goto(pairingUrl.href);
        await profiles.promise;
        NodeAssert.equal(await remote.evaluate(() => Boolean(window.desktopBridge)), false);
        NodeAssert.equal(
          await remote.evaluate(
            async () => (await (await fetch("/api/auth/session")).json()).authenticated,
          ),
          true,
        );
        NodeAssert.equal(new URL(remote.url()).hash.includes("token="), false);
        checks.genuinePairedWebClientAuthenticated = true;
        const { status, guest } = await nativePage(profileId, "/public");
        const listed = waitForRendererRpc(
          remote,
          "preview.list",
          (payload) => payload.threadId === threadId,
        );
        rendererMilestones.push(listed);
        await remote.goto(
          new URL(
            `/${encodeURIComponent(environmentId)}/${encodeURIComponent(threadId)}`,
            bootstrap.httpBaseUrl,
          ).href,
        );
        await listed.promise;
        await remote
          .locator(
            '[data-chat-header] button[aria-label="Thread actions for Synthetic browser proof"]',
          )
          .waitFor({ state: "visible" });
        await remote.keyboard.press(platform === "darwin" ? "Meta+Shift+j" : "Control+Shift+j");
        surface = remote.locator(`[data-server-browser-surface="${status.tabId}"]`);
        await surface.locator("canvas").waitFor({ state: "visible", timeout: 30_000 });
        await remote.waitForFunction(
          (tabId) =>
            document
              .querySelector(`[data-server-browser-surface="${tabId}"] canvas`)
              ?.getContext("2d")
              ?.getImageData(0, 0, 1, 1).data[3] > 0,
          status.tabId,
          { timeout: 30_000 },
        );
        NodeAssert.equal(await remote.locator("webview").count(), 0);
        const read = async () =>
          (
            await call("preview_evaluate", {
              tabId: status.tabId,
              expression:
                '({pageId: document.querySelector("#page-id").textContent, draft: document.querySelector("#draft").value})',
            })
          ).value;
        const initial = await read();
        NodeAssert.ok(initial.pageId);
        const target = "remote browser input";
        await call("preview_click", { tabId: status.tabId, selector: "#draft" });
        const inputPath = `/remote-input-${NodeCrypto.randomUUID()}`;
        const typed = input.waitForFixtureRequest(inputPath);
        try {
          await call("preview_evaluate", {
            tabId: status.tabId,
            expression: `(() => { const input = document.querySelector("#draft"); const listener = () => { if(input.value !== ${JSON.stringify(target)}) return; input.removeEventListener("input", listener); void fetch(${JSON.stringify(inputPath)}).catch(() => undefined); }; input.addEventListener("input", listener); return true; })()`,
          });
          await surface.getByRole("button", { name: "Take control", exact: true }).click();
          await surface
            .getByRole("button", { name: "Release control", exact: true })
            .waitFor({ state: "visible" });
          const keyboard = surface.getByRole("textbox", { name: "Browser page", exact: true });
          await keyboard.focus();
          await keyboard.pressSequentially(target);
          await typed.promise;
          await surface.getByRole("button", { name: "Release control", exact: true }).click();
          await surface
            .getByRole("button", { name: "Take control", exact: true })
            .waitFor({ state: "visible" });
        } finally {
          typed.close();
        }
        const final = await read();
        NodeAssert.equal(final.pageId, initial.pageId);
        NodeAssert.equal(final.draft, target);
        NodeAssert.deepEqual(
          await guest.evaluate((element) =>
            element.executeJavaScript(
              '({pageId: document.querySelector("#page-id").textContent, draft: document.querySelector("#draft").value})',
            ),
          ),
          final,
        );
        checks.genuinePairedWebClientSharesDesktopNativePage = true;
        await remote.screenshot({ path: NodePath.join(input.outputDir, "genuine-remote-web.png") });
      } finally {
        if (
          surface &&
          (await surface
            .getByRole("button", { name: "Release control", exact: true })
            .isVisible()
            .catch(() => false))
        ) {
          await surface
            .getByRole("button", { name: "Release control", exact: true })
            .click()
            .catch(() => undefined);
          await surface
            .getByRole("button", { name: "Take control", exact: true })
            .waitFor({ state: "visible" })
            .catch(() => undefined);
        }
        await browser.close();
      }
    });
    await input.mainWindow.screenshot({
      path: NodePath.join(input.outputDir, "genuine-mcp-native.png"),
    });
    for (const tabId of nativeTabIds.splice(0)) await call("t3_preview_close", { tabId });
    NodeAssert.equal(
      (await rpc.request("preview.list", { threadId })).sessions.length,
      0,
      "All sessions, including any adopted popup, must be closed.",
    );
    NodeAssert.equal(
      (await call("preview_status")).available,
      false,
      "No server browser page may survive the final tab cleanup.",
    );
    checks.genuineMcpTabsClosed = true;
    checks.genuineMcpNativeTypeAndClick = nativeInputErrors.length === 0;
    if (nativeInputErrors.length + boundaryErrors.length > 0)
      throw new AggregateError(
        [...nativeInputErrors, ...boundaryErrors],
        `${nativeInputErrors.length} native input assertions and ${boundaryErrors.length} additional boundaries failed; see checks.nativeInputFailures and checks.additionalBoundaryFailures. Independent checks and tab cleanup completed.`,
      );
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
