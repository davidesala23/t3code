import { scopedThreadKey } from "@t3tools/client-runtime/environment";
import { EnvironmentId, ThreadId, type PreviewSessionSnapshot } from "@t3tools/contracts";
import {
  act,
  createElement,
  useEffect,
  useImperativeHandle,
  type ComponentProps,
  type ReactNode,
} from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import type { HostedBrowserWebview } from "~/browser/HostedBrowserWebview";
import type { ServerBrowserSurface } from "~/browser/ServerBrowserSurface";

const mocks = vi.hoisted(() => ({
  mode: "native" as "native" | "headless",
  nativeMount: vi.fn(),
  nativeUnmount: vi.fn(),
  streamMount: vi.fn(),
  streamUnmount: vi.fn(),
  reportPreview: vi.fn(),
  clearPreview: vi.fn(),
  nativePopOut: vi.fn(),
  nativeClosePopOut: vi.fn(),
  serverPopOut: vi.fn(),
}));

const threadRef = {
  environmentId: EnvironmentId.make("local"),
  threadId: ThreadId.make("thread-1"),
};
function previewState() {
  return {
    serverEpoch: "server-1",
    sessions: {
      "tab-1": {
        threadId: threadRef.threadId,
        tabId: "tab-1",
        runtime: "server",
        serverPageMode: mocks.mode,
        navStatus: { _tag: "Success", url: "https://example.com/", title: "Example" },
        viewport: { _tag: "freeform", width: 800, height: 600 },
        profileId: "default",
        canGoBack: false,
        canGoForward: false,
        updatedAt: "2026-10-11T00:00:00.000Z",
      } satisfies PreviewSessionSnapshot,
    },
    desktopByTabId: {
      "tab-1": { hasWebContents: true, zoomFactor: 2, pictureInPicture: true },
    },
  };
}

vi.mock("~/env", () => ({ isElectron: true }));
vi.mock("@effect/atom-react", () => ({ useAtomValue: () => threadRef.environmentId }));
vi.mock("~/state/primaryEnvironment", () => ({ primaryEnvironmentIdAtom: {} }));
vi.mock("~/state/entities", () => ({
  readEnvironmentSupportsServerBrowser: () => true,
  useEnvironmentSupportsServerBrowser: () => true,
}));
vi.mock("~/previewStateStore", () => ({
  useThreadPreviewState: previewState,
  useActivePreviewSessions: () => ({ [scopedThreadKey(threadRef)]: previewState() }),
  isPreviewSupportedInRuntime: () => true,
}));
vi.mock("~/hooks/useSettings", () => ({ useClientSettingsHydrated: () => true }));
vi.mock("~/hooks/useTheme", () => ({ useTheme: () => ({ resolvedTheme: "light" }) }));
vi.mock("~/browser/browserDefaults", () => ({
  useBrowserDefaults: () => ({ profileId: "default" }),
}));
vi.mock("~/browser/browserPointerStore", () => ({ useBrowserPointerStore: {} }));
vi.mock("~/state/session", () => ({ useEnvironmentScope: () => true }));
vi.mock("~/state/preview", () => ({ previewEnvironment: { open: {} } }));
vi.mock("~/state/use-atom-command", () => ({ useAtomCommand: () => vi.fn() }));
vi.mock("~/browser/openFileInPreview", () => ({ openUrlInPreview: vi.fn() }));
vi.mock("~/browser/HostedBrowserWebview", () => ({
  HostedBrowserWebview: (props: ComponentProps<typeof HostedBrowserWebview>) => {
    useEffect(() => {
      mocks.nativeMount(props.runtimeTabId);
      return () => {
        mocks.nativeUnmount(props.runtimeTabId);
      };
    }, [props.runtimeTabId]);
    return createElement("webview");
  },
}));
vi.mock("~/browser/browserRecording", () => ({
  findActiveBrowserRecordingRuntimeTabId: () => null,
  useActiveBrowserRecordingTabIds: () => new Set(),
}));
vi.mock("~/browser/browserSurfaceStore", () => ({
  useBrowserSurfaceStore: (select: (state: { byTabId: Record<string, never> }) => unknown) =>
    select({ byTabId: {} }),
}));
vi.mock("~/browser/BrowserSurfaceSlot", () => ({
  BrowserSurfaceSlot: () => createElement("span", { "aria-label": "Native surface" }),
}));
vi.mock("~/browser/ServerBrowserSurface", () => ({
  ServerBrowserSurface: (props: ComponentProps<typeof ServerBrowserSurface>) => {
    useImperativeHandle(
      props.ref,
      () => ({
        navigate: vi.fn(),
        history: vi.fn(),
        reload: vi.fn(),
        viewport: vi.fn(),
        canvas: () => null,
      }),
      [],
    );
    useEffect(() => {
      mocks.streamMount(props.tabId);
      return () => {
        mocks.streamUnmount(props.tabId);
      };
    }, [props.tabId]);
    return createElement("stream-surface");
  },
}));
vi.mock("~/browser/serverPictureInPicture", () => ({
  closeServerPictureInPicture: vi.fn(),
  openServerPictureInPicture: mocks.serverPopOut,
  serverPictureInPictureKey: (threadId: string, tabId: string) => `${threadId}:${tabId}`,
  supportsServerPictureInPicture: () => true,
  useServerPictureInPictureKey: () => null,
}));
vi.mock("./previewBridge", () => ({
  previewBridge: { pictureInPicture: { open: mocks.nativePopOut, close: mocks.nativeClosePopOut } },
}));
vi.mock("../chat/ChatCanvasContext", () => ({
  useChatCanvas: () => ({
    previewKey: "browser:tab-1",
    layout: { frame: { x: 0, y: 0, width: 240, height: 180 } },
    reportPreview: mocks.reportPreview,
    clearPreview: mocks.clearPreview,
  }),
}));
vi.mock("~/state/device", () => ({
  useDeviceState: () => ({ state: { devices: [], hosts: [] } }),
}));
vi.mock("../device/DeviceStreamView", () => ({ DeviceStreamView: () => null }));
vi.mock("~/components/ui/button", () => ({
  Button: (props: ComponentProps<"button">) => createElement("button", props),
}));
vi.mock("~/components/ui/tooltip", () => ({
  Tooltip: (props: { children: ReactNode }) => props.children,
  TooltipTrigger: (props: { render: ReactNode }) => props.render,
  TooltipPopup: () => null,
}));

import { ElectronBrowserHost } from "~/browser/ElectronBrowserHost";
import { ThreadPreviewMiniPlayer } from "./ThreadPreviewMiniPlayer";
import { previewRuntimeTabId } from "~/browser/previewRuntimeTabId";

let root: Root;
let container: HTMLDivElement;
beforeEach(() => {
  vi.clearAllMocks();
  mocks.mode = "native";
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(() => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

describe("headless fallback surfaces", () => {
  it("unmounts the hosted native guest and never remounts it for a headless snapshot", async () => {
    const runtimeTabId = previewRuntimeTabId(threadRef, "server-1", "tab-1");
    await act(() => root.render(<ElectronBrowserHost />));
    expect(mocks.nativeMount).toHaveBeenCalledExactlyOnceWith(runtimeTabId);
    expect(container.querySelectorAll("webview")).toHaveLength(1);
    mocks.mode = "headless";
    await act(() => root.render(<ElectronBrowserHost />));
    expect(mocks.nativeUnmount).toHaveBeenCalledExactlyOnceWith(runtimeTabId);
    expect(container.querySelectorAll("webview")).toHaveLength(0);
    await act(() => root.render(<ElectronBrowserHost />));
    expect(mocks.nativeMount).toHaveBeenCalledTimes(1);
  });

  it("moves a floating tab to its stream and uses server pop-out instead of stale native PiP", async () => {
    const view = () => (
      <ThreadPreviewMiniPlayer
        threadRef={threadRef}
        miniPlayer={{
          source: { kind: "browser", tabId: "tab-1" },
          position: null,
          width: null,
          lastInteraction: "drag",
        }}
      />
    );
    await act(() => root.render(view()));
    expect(container.querySelectorAll('[aria-label="Native surface"]')).toHaveLength(1);
    const nativeCloseButton = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Close popped-out preview"]',
    );
    expect(nativeCloseButton).not.toBeNull();
    await act(() => nativeCloseButton!.click());
    expect(mocks.nativeClosePopOut).toHaveBeenCalledOnce();

    mocks.mode = "headless";
    await act(() => root.render(view()));
    expect(container.querySelectorAll('[aria-label="Native surface"]')).toHaveLength(0);
    expect(mocks.streamMount).toHaveBeenCalledExactlyOnceWith("tab-1");
    expect(mocks.reportPreview).toHaveBeenLastCalledWith(
      expect.objectContaining({ source: { width: 800, height: 600 } }),
    );
    expect(container.querySelector("stream-surface")).not.toBeNull();
    const serverOpenButton = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Pop preview into separate window"]',
    );
    expect(serverOpenButton).not.toBeNull();
    await act(() => serverOpenButton!.click());
    expect(mocks.serverPopOut).toHaveBeenCalledExactlyOnceWith({
      ...threadRef,
      tabId: "tab-1",
      seed: null,
    });
    expect(mocks.nativeClosePopOut).toHaveBeenCalledOnce();
    expect(mocks.nativePopOut).not.toHaveBeenCalled();
  });
});
