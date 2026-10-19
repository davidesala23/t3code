import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts";
import { execScriptSource, writeFakeCli } from "@t3tools/provider-testing/fakeCli";

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
  const privateRoot = await NodeFSP.mkdtemp(NodePath.join(input.isolatedRoot, "mcp-private-"));
  const fixtureDirectory = NodeURL.fileURLToPath(
    new URL("../src/provider/testFixtures/", import.meta.url),
  );
  const peerPath = NodePath.join(privateRoot, "codexCollabMockPeer.mjs");
  const scriptPath = NodePath.join(privateRoot, "script.json");
  const threadId = NodeCrypto.randomUUID();
  const projectId = NodeCrypto.randomUUID();
  const instanceId = `browser_fixture_${NodeCrypto.randomUUID()}`;
  const modelSelection = { instanceId, model: "gpt-5.6-sol" };
  const profileId = `native-${NodeCrypto.randomUUID()}`;
  const otherProfileId = `other-${NodeCrypto.randomUUID()}`;
  const nativeTabIds = [];
  let phase = "fixture preparation";
  let rpc;
  let client;
  let originalSettings;
  let providerCreated = false;
  let credentialCapture;
  const secrets = [];
  const progress = (name) => {
    phase = name;
    console.log(`[packaged-mcp] ${name}`);
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
    await rpc.request("orchestration.dispatchCommand", {
      type: "thread.create",
      commandId: NodeCrypto.randomUUID(),
      threadId,
      projectId,
      title: "Synthetic native browser proof",
      createdBy: "user",
      creationSource: "web",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
    });

    progress("native profiles and React host");
    originalSettings = await input.mainWindow.evaluate(() =>
      window.desktopBridge.getClientSettings(),
    );
    const settings = { ...DEFAULT_CLIENT_SETTINGS, ...originalSettings };
    await input.mainWindow.evaluate((next) => window.desktopBridge.setClientSettings(next), {
      ...settings,
      confirmQuit: "direct",
      browserAutoShowFloatingPreview: false,
      browserProfiles: [
        ...settings.browserProfiles,
        { id: profileId, name: "Native proof", kind: "persistent" },
        { id: otherProfileId, name: "Other proof", kind: "persistent" },
      ],
    });
    const target = new URL(input.mainWindow.url());
    target.hash = `/${environmentId}/${threadId}`;
    await input.mainWindow.goto(target.href);
    await input.mainWindow.reload();
    await input.mainWindow.waitForFunction(
      () => Boolean(document.querySelector("[data-electron-browser-host]")),
      undefined,
      { timeout: 60_000 },
    );

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
      projection.thread.activeRunId,
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
      const evaluated = await call("preview_evaluate", {
        tabId: status.tabId,
        expression:
          '({auth: document.querySelector("#auth")?.textContent, pageId: document.querySelector("#page-id")?.textContent})',
      });
      const value = evaluated.value;
      const visible = await guest.evaluate((element) =>
        element.executeJavaScript(
          '({auth: document.querySelector("#auth")?.textContent, pageId: document.querySelector("#page-id")?.textContent})',
        ),
      );
      NodeAssert.deepEqual(
        value,
        visible,
        "Real MCP and the React-mounted native guest must see one page.",
      );
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
    await input.mainWindow.screenshot({
      path: NodePath.join(input.outputDir, "genuine-mcp-native.png"),
    });
    for (const tabId of nativeTabIds.splice(0)) await call("t3_preview_close", { tabId });
    checks.genuineMcpTabsClosed = true;
    return { checks, threadId, nativeTabIds: [] };
  } catch (error) {
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
    if (originalSettings !== undefined)
      await input.mainWindow
        .evaluate(
          (settings) => window.desktopBridge.setClientSettings(settings),
          originalSettings ?? DEFAULT_CLIENT_SETTINGS,
        )
        .catch(() => undefined);
    credentialCapture?.close();
  }
}
