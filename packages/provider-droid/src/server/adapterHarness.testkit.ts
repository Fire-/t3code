/**
 * Shared harness for the adapter tests: opens a runtime on a fake Droid, binds a provider thread,
 * starts turns, and collects the events the runtime publishes.
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert } from "@effect/vitest";
import {
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  type ChatAttachment,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type RuntimeMode,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as References from "effect/References";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";

import { DROID_PROVIDER, makeDroidAdapterV2 } from "./adapter.ts";
import { DroidAuthSignal, type DroidAuthSignalService } from "./authSignal.ts";
import { type FakeDroid } from "./fakeDroid.testkit.ts";

export const layerTest = Layer.mergeAll(
  NodeServices.layer,
  IdAllocator.layer,
  TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
);

export const DROID_INSTANCE_ID = ProviderInstanceId.make("droid");
export const THREAD_ID = ThreadId.make("thread-droid-test");
export const SESSION_ID = ProviderSessionId.make("provider-session-droid-test");
export const PROVIDER_THREAD_ID = ProviderThreadId.make("provider-thread:droid-test");
export const MODEL = "claude-opus-5-5";

/** `null` selects a model that has no reasoning effort option. */
export const selection = (effort: string | null = "high", model = MODEL): ModelSelection => ({
  instanceId: DROID_INSTANCE_ID,
  model,
  ...(effort === null ? {} : { options: [{ id: "reasoningEffort", value: effort }] }),
});

/** A turn or thread option that names `effort: undefined` asks for no effort at all. */
const effortOf = (options: { readonly effort?: string | undefined }): string | null =>
  "effort" in options ? (options.effort ?? null) : "high";

export const policy = (
  runtimeMode: RuntimeMode = "approval-required",
  interactionMode: "default" | "plan" = "default",
) =>
  ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({ runtimeMode, interactionMode, cwd: null });

export const captureLogs = () => {
  const entries: Array<{ readonly level: string; readonly text: string }> = [];
  const logger = Logger.make<unknown, void>((options) => {
    const message = Array.isArray(options.message) ? options.message : [options.message];
    const annotations = options.fiber.getRef(References.CurrentLogAnnotations);
    entries.push({
      level: options.logLevel,
      text: [
        ...message.map(String),
        ...Object.entries(annotations).map(([key, value]) => `${key}=${String(value)}`),
      ].join(" "),
    });
  });
  const provide = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.provide(Logger.layer([logger], { mergeWithExisting: false })),
      Effect.provideService(References.MinimumLogLevel, "All"),
    );
  return { entries, provide };
};

export const makeAdapter = Effect.fnUntraced(function* (
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  authSignal?: DroidAuthSignalService,
  binaryPath = "droid",
) {
  const make = makeDroidAdapterV2({
    instanceId: DROID_INSTANCE_ID,
    settings: { enabled: true, binaryPath, customModels: [] },
    environment: {},
  }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
  return yield* authSignal === undefined
    ? make
    : make.pipe(Effect.provideService(DroidAuthSignal, authSignal));
});

export interface OpenOptions {
  readonly runtimeMode?: RuntimeMode;
  readonly interactionMode?: "default" | "plan";
  readonly effort?: string | undefined;
  readonly authSignal?: DroidAuthSignalService;
  readonly spawner?: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly binaryPath?: string;
  /** Closing it closes the session while the test keeps collecting the events it publishes. */
  readonly sessionScope?: Scope.Closeable;
}

export const openRuntime = Effect.fnUntraced(function* (
  fake: FakeDroid | undefined,
  options: OpenOptions = {},
) {
  const spawner = options.spawner ?? fake?.spawner;
  assert.isDefined(spawner);
  const adapter = yield* makeAdapter(spawner!, options.authSignal, options.binaryPath);
  const open = adapter.openSession({
    threadId: THREAD_ID,
    providerSessionId: SESSION_ID,
    modelSelection: selection(effortOf(options)),
    runtimePolicy: policy(options.runtimeMode, options.interactionMode),
  });
  const runtime = yield* options.sessionScope === undefined
    ? open
    : open.pipe(Scope.provide(options.sessionScope));
  const queue = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  const all: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
  const streamEnded =
    yield* Deferred.make<Exit.Exit<void, ProviderAdapter.ProviderAdapterV2Error>>();
  yield* runtime.events.pipe(
    Stream.runForEach((event) =>
      Effect.suspend(() => {
        all.push(event);
        return Queue.offer(queue, event);
      }),
    ),
    Effect.exit,
    Effect.flatMap((exit) => Deferred.succeed(streamEnded, exit)),
    Effect.forkScoped,
  );
  const takeEvent = (predicate: (event: ProviderAdapter.ProviderAdapterV2Event) => boolean) =>
    Effect.gen(function* () {
      while (true) {
        const event = yield* Queue.take(queue);
        if (predicate(event)) return event;
      }
    });
  return { adapter, runtime, takeEvent, all, streamEnded };
});

export type OpenRuntime = Effect.Success<ReturnType<typeof openRuntime>>;

const makeAppThread = Effect.fnUntraced(function* (runtimeMode: RuntimeMode = "approval-required") {
  const now = yield* DateTime.now;
  return {
    createdBy: "user",
    creationSource: "web",
    id: THREAD_ID,
    projectId: "project:fixture:droid" as OrchestrationV2AppThread["projectId"],
    title: "Droid test thread",
    providerInstanceId: DROID_INSTANCE_ID,
    modelSelection: selection(),
    runtimeMode,
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: THREAD_ID },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  } satisfies OrchestrationV2AppThread;
});

export const makeProviderThread = Effect.fnUntraced(function* (nativeId: string | null = null) {
  const now = yield* DateTime.now;
  return {
    id: PROVIDER_THREAD_ID,
    driver: DROID_PROVIDER,
    providerInstanceId: DROID_INSTANCE_ID,
    providerSessionId: SESSION_ID,
    appThreadId: THREAD_ID,
    ownerNodeId: null,
    nativeThreadRef:
      nativeId === null ? null : { driver: DROID_PROVIDER, nativeId, strength: "strong" as const },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: null,
    pendingBackgroundTasks: [],
    contextUsage: null,
    nativeMetadata: null,
    goal: null,
    createdAt: now,
    updatedAt: now,
  } satisfies OrchestrationV2ProviderThread;
});

export const messageIdFor = (runOrdinal: number) => `message:${THREAD_ID}:${runOrdinal}`;

export const runIdFor = (runOrdinal: number) => RunId.make(`run:${THREAD_ID}:${runOrdinal}`);

export interface TurnOptions {
  readonly runOrdinal?: number;
  readonly runtimeMode?: RuntimeMode;
  readonly interactionMode?: "default" | "plan";
  readonly model?: string;
  readonly effort?: string | undefined;
  readonly text?: string;
  readonly attachments?: ReadonlyArray<ChatAttachment>;
}

export const startTurn = Effect.fnUntraced(function* (
  runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
  providerThread: OrchestrationV2ProviderThread,
  options: TurnOptions = {},
) {
  const runOrdinal = options.runOrdinal ?? 1;
  const runtimeMode = options.runtimeMode ?? "approval-required";
  const appThread = yield* makeAppThread(runtimeMode);
  const runId = runIdFor(runOrdinal);
  yield* runtime.startTurn({
    appThread,
    threadId: THREAD_ID,
    runId,
    runOrdinal,
    providerTurnOrdinal: runOrdinal,
    attemptId: RunAttemptId.make(`run-attempt:${runId}:1`),
    rootNodeId: NodeId.make(`node:${runId}:root`),
    providerThread,
    message: {
      messageId: messageIdFor(runOrdinal) as never,
      text: options.text ?? "Hello droid",
      attachments: options.attachments ?? [],
      createdBy: "user",
      creationSource: "web",
    },
    modelSelection: selection(effortOf(options), options.model ?? MODEL),
    runtimePolicy: policy(runtimeMode, options.interactionMode),
  });
});

export const completed = (runOrdinal: number, reason = "completed") => ({
  type: "agent_turn_completed",
  reason,
  turnId: messageIdFor(runOrdinal),
  tokenUsage: {
    inputTokens: 10,
    outputTokens: 5,
    cacheCreationTokens: 0,
    cacheReadTokens: 0,
    thinkingTokens: 0,
  },
});

export const isTerminal = (event: ProviderAdapter.ProviderAdapterV2Event) =>
  event.type === "turn.terminal";

export const terminals = (events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) =>
  events.flatMap((event) => (event.type === "turn.terminal" ? [event] : []));

/** `ensureThread` for a fresh thread, then a bound provider thread row. */
export const ensure = Effect.fnUntraced(function* (
  { runtime }: OpenRuntime,
  options: { readonly runtimeMode?: RuntimeMode; readonly effort?: string | undefined } = {},
) {
  const existing = yield* makeProviderThread();
  return yield* runtime.ensureThread({
    threadId: THREAD_ID,
    modelSelection: selection(effortOf(options)),
    runtimePolicy: policy(options.runtimeMode),
    existingProviderThread: existing,
  });
});

/** The items whose latest published state is still in flight. */
export const openItems = (events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) => {
  const latest = new Map<string, string>();
  for (const event of events) {
    if (event.type === "turn_item.updated") {
      latest.set(String(event.turnItem.id), event.turnItem.status);
    }
  }
  return [...latest].filter(([, status]) =>
    ["idle", "pending", "running", "waiting"].includes(status),
  );
};

export const sessionStatuses = (events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) =>
  events.flatMap((event) =>
    event.type === "provider_session.updated" ? [event.providerSession.status] : [],
  );

export const methodsOf = (fake: FakeDroid) => fake.current().methods();

export const requestsOf = (fake: FakeDroid, method: string) =>
  fake
    .current()
    .requests()
    .filter((request) => request.method === method);

export const asRecord = (value: unknown): Record<string, unknown> =>
  Predicate.isObject(value) ? value : assert.fail(`expected an object, got ${typeof value}`);

/** Opens a runtime, binds a thread, and starts the first turn once its provider turn is published. */
export const beginTurn = Effect.fnUntraced(function* (
  fake: FakeDroid,
  options: TurnOptions & OpenOptions = {},
) {
  const opened = yield* openRuntime(fake, options);
  const providerThread = yield* ensure(opened, options);
  yield* startTurn(opened.runtime, providerThread, options);
  const published = yield* opened.takeEvent((event) => event.type === "provider_turn.updated");
  if (published.type !== "provider_turn.updated") return assert.fail("no provider turn");
  return { opened, providerThread, providerTurn: published.providerTurn, process: fake.current() };
});

export type BegunTurn = Effect.Success<ReturnType<typeof beginTurn>>;

export const steerMessageIdFor = (ordinal: number) => `message:${THREAD_ID}:steer:${ordinal}`;

export const steerBegunTurn = (
  begun: BegunTurn,
  options: {
    readonly ordinal?: number;
    readonly text?: string;
    readonly providerTurnId?: BegunTurn["providerTurn"]["id"];
  } = {},
) =>
  begun.opened.runtime.steerTurn({
    threadId: THREAD_ID,
    runId: runIdFor(1),
    providerThread: begun.providerThread,
    providerTurnId: options.providerTurnId ?? begun.providerTurn.id,
    message: {
      messageId: steerMessageIdFor(options.ordinal ?? 1) as never,
      text: options.text ?? "Also add tests",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
  });

/** Runs one whole turn on an already-bound thread and returns when its terminal arrived. */
export const runTurn = Effect.fnUntraced(function* (
  opened: OpenRuntime,
  fake: FakeDroid,
  providerThread: OrchestrationV2ProviderThread,
  options: TurnOptions & { readonly reason?: string } = {},
) {
  yield* startTurn(opened.runtime, providerThread, options);
  yield* fake.current().emit(completed(options.runOrdinal ?? 1, options.reason));
  return yield* opened.takeEvent(isTerminal);
});
