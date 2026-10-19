import { assert, describe, it } from "@effect/vitest";
import { EnvironmentId, ThreadId, WS_METHODS, type PreviewEvent } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import * as RpcClient from "effect/rpc/RpcClient";
import type * as RpcMessage from "effect/rpc/RpcMessage";
import * as RpcSerialization from "effect/rpc/RpcSerialization";

import type { ConnectionCatalogEntry } from "../connection/catalog.ts";
import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type NetworkStatus,
  type PreparedConnection,
  type SupervisorConnectionState,
} from "../connection/model.ts";
import * as EnvironmentRegistry from "../connection/registry.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import { makeWsRpcProtocolClient } from "../rpc/protocol.ts";
import type { RpcSession } from "../rpc/session.ts";
import { createPreviewEnvironmentAtoms } from "./preview.ts";

describe("preview event delivery", () => {
  it.effect("delivers every close received in one transport chunk", () =>
    Effect.gen(function* () {
      const environmentId = EnvironmentId.make("preview-events");
      const threadId = ThreadId.make("thread-preview-events");
      const requested = yield* Deferred.make<RpcMessage.RequestEncoded>();
      const sawTerminal = yield* Deferred.make<void>();
      const first: PreviewEvent = {
        type: "closed",
        threadId,
        tabId: "tab-a",
        serverEpoch: "epoch-1",
        revision: 1,
        createdAt: "2026-10-19T00:00:00.000Z",
      };
      const events: [PreviewEvent, PreviewEvent, PreviewEvent] = [
        first,
        { ...first, tabId: "tab-b", revision: 2 },
        { ...first, tabId: "tab-c", revision: 3 },
      ];
      const responses = yield* Effect.acquireRelease(
        Queue.unbounded<RpcMessage.FromServerEncoded>(),
        Queue.shutdown,
      );
      const client = yield* makeWsRpcProtocolClient.pipe(
        Effect.provideService(RpcClient.Protocol, {
          codecFor: RpcSerialization.json.codecFor,
          supportsAck: true,
          supportsTransferables: false,
          run: (_clientId, receive) =>
            Queue.take(responses).pipe(Effect.flatMap(receive), Effect.forever),
          send: (_clientId, message) => {
            if (message._tag !== "Request") return Effect.void;
            if (message.tag !== WS_METHODS.subscribePreviewEvents)
              return Effect.die(new Error(`Unexpected RPC request: ${message.tag}`));
            return Deferred.succeed(requested, message).pipe(Effect.asVoid);
          },
        }),
      );
      const session: RpcSession = {
        client,
        initialConfig: Effect.never,
        subscribeServerConfig: (input) => client.subscribeServerConfig(input),
        ready: Effect.void,
        probe: Effect.void,
        closed: Effect.never,
      };
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: new PrimaryConnectionTarget({
          environmentId,
          label: "Preview events",
          httpBaseUrl: "https://preview.example.test",
          wsBaseUrl: "wss://preview.example.test",
        }),
        state: yield* SubscriptionRef.make<SupervisorConnectionState>({
          ...AVAILABLE_CONNECTION_STATE,
          phase: "connected",
        }),
        session: yield* SubscriptionRef.make(Option.some(session)),
        prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      });
      const runtime = Atom.runtime(
        Layer.mock(EnvironmentRegistry.EnvironmentRegistry)({
          entries: yield* SubscriptionRef.make<ReadonlyMap<EnvironmentId, ConnectionCatalogEntry>>(
            new Map(),
          ),
          networkStatus: yield* SubscriptionRef.make<NetworkStatus>("online"),
          followStream: (_environmentId, stream) =>
            Stream.provideService(stream, EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        }),
      );
      const eventAtom = createPreviewEnvironmentAtoms(runtime).events({
        environmentId,
        input: {},
      });
      const observed: string[] = [];
      const syncAtom = Atom.make((get) => {
        get.once(eventAtom);
        get.subscribe(eventAtom, (result) => {
          if (!AsyncResult.isSuccess(result)) return;
          observed.push(result.value.tabId);
          if (result.value.tabId === "tab-c") Deferred.doneUnsafe(sawTerminal, Exit.void);
        });
        get.mount(eventAtom);
      });
      const registry = AtomRegistry.make();
      const unmount = registry.mount(syncAtom);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          unmount();
          registry.dispose();
        }),
      );
      const request = yield* Deferred.await(requested);
      yield* Queue.offer(responses, { _tag: "Chunk", requestId: request.id, values: events });
      yield* Deferred.await(sawTerminal);
      assert.deepStrictEqual(observed, ["tab-a", "tab-b", "tab-c"]);
    }),
  );
});
