/**
 * Shared harness for the adapter tests: opens a runtime on a fake Droid, binds a provider thread,
 * starts turns, and collects the events the runtime publishes.
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert } from "@effect/vitest";
import {
  EnvironmentId,
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ServerSettings,
  ThreadId,
  type ChatAttachment,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
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
import * as Schema from "effect/Schema";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import * as HostProcess from "@t3tools/shared/HostProcess";

import {
  DROID_PROVIDER,
  DroidAdapterV2Driver,
  makeDroidAdapterV2,
  type DroidAdapterV2Options,
} from "./adapter.ts";
import { DroidAuthSignal, type DroidAuthSignalService } from "./authSignal.ts";
import { type FakeDroid, type FakeDroidProcess } from "./fakeDroid.testkit.ts";

export const layerTest = Layer.mergeAll(
  NodeServices.layer,
  IdAllocator.layer,
  McpProviderSessions.layer,
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

const T3_MCP_ENDPOINT = "http://127.0.0.1:3773/mcp";

/**
 * Issues a thread's T3 MCP credential, as the session manager does before it opens a session, and
 * returns the `mcpServers` Droid should then be passed.
 */
export const issueT3Mcp = Effect.fnUntraced(function* (threadId = THREAD_ID, token = "test-token") {
  const sessions = yield* McpProviderSessions.McpProviderSessions;
  yield* sessions.set({
    environmentId: EnvironmentId.make("environment-droid-test"),
    threadId,
    providerSessionId: `mcp:${threadId}`,
    providerInstanceId: DROID_INSTANCE_ID,
    endpoint: T3_MCP_ENDPOINT,
    authorizationHeader: `Bearer ${token}`,
    browserToolsAvailable: false,
  });
  return [
    {
      type: "http",
      name: "t3-code",
      url: T3_MCP_ENDPOINT,
      headers: [{ name: "Authorization", value: `Bearer ${token}` }],
    },
  ];
});

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

/** Keeps the adapter away from the real `~/.factory`; a test that needs Droid's files names its own home. */
const NO_HOME = "/nonexistent/t3-droid-test-home";

export const makeAdapter = Effect.fnUntraced(function* (
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  authSignal?: DroidAuthSignalService,
  binaryPath = "droid",
  continuationRequests?: DroidAdapterV2Options["continuationRequests"],
  homeDirectory = NO_HOME,
) {
  const make = makeDroidAdapterV2({
    instanceId: DROID_INSTANCE_ID,
    settings: { enabled: true, binaryPath, customModels: [] },
    environment: {},
    ...(continuationRequests === undefined ? {} : { continuationRequests }),
  }).pipe(
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    Effect.provideService(HostProcess.HomeDirectory, homeDirectory),
  );
  return yield* authSignal === undefined
    ? make
    : make.pipe(Effect.provideService(DroidAuthSignal, authSignal));
});

const SAVED_BLOCK = {
  driver: "droid",
  enabled: true,
  config: { binaryPath: "droid", customModels: [] },
};

/** The adapter a restart builds: the saved `droid` provider instance block through the driver. */
export const adapterFromSavedBlock = Effect.fnUntraced(function* (fake: FakeDroid) {
  const settings = yield* Schema.decodeEffect(ServerSettings)({
    providerInstances: { droid: SAVED_BLOCK },
  });
  const saved = settings.providerInstances[DROID_INSTANCE_ID];
  if (saved === undefined || !Predicate.isObject(saved.config)) {
    return assert.fail("the saved droid block did not decode");
  }
  const config = yield* Schema.decodeEffect(DroidAdapterV2Driver.configSchema)({
    enabled: saved.enabled,
    ...saved.config,
  });
  return yield* DroidAdapterV2Driver.create({
    instanceId: DROID_INSTANCE_ID,
    displayName: undefined,
    accentColor: undefined,
    enabled: saved.enabled ?? false,
    environment: [],
    config,
  }).pipe(
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner),
    Effect.provideService(HostProcess.Environment, {}),
    Effect.provideService(HostProcess.HomeDirectory, NO_HOME),
  );
});

export interface OpenOptions {
  readonly runtimeMode?: RuntimeMode;
  readonly interactionMode?: "default" | "plan";
  readonly effort?: string | undefined;
  readonly authSignal?: DroidAuthSignalService;
  readonly spawner?: ChildProcessSpawner.ChildProcessSpawner["Service"];
  /** Replaces the adapter `makeAdapter` would build, for a test that builds it another way. */
  readonly adapter?: ProviderAdapter.ProviderAdapterV2["Service"];
  readonly binaryPath?: string;
  /** Closing it closes the session while the test keeps collecting the events it publishes. */
  readonly sessionScope?: Scope.Closeable;
  /** Where the adapter offers the continuations Droid starts on its own; without it none is offered. */
  readonly continuationRequests?: DroidAdapterV2Options["continuationRequests"];
  /** The home Droid keeps its sessions under. */
  readonly homeDirectory?: string;
}

export const openRuntime = Effect.fnUntraced(function* (
  fake: FakeDroid | undefined,
  options: OpenOptions = {},
) {
  const spawner = options.spawner ?? fake?.spawner;
  assert.isDefined(spawner);
  const adapter =
    options.adapter ??
    (yield* makeAdapter(
      spawner!,
      options.authSignal,
      options.binaryPath,
      options.continuationRequests,
      options.homeDirectory,
    ));
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
  /** The turn the orchestrator starts for a continuation the adapter offered. */
  readonly continuation?: boolean;
}

const turnInputFor = Effect.fnUntraced(function* (
  providerThread: OrchestrationV2ProviderThread,
  options: TurnOptions,
) {
  const runOrdinal = options.runOrdinal ?? 1;
  const runtimeMode = options.runtimeMode ?? "approval-required";
  const appThread = yield* makeAppThread(runtimeMode);
  const runId = runIdFor(runOrdinal);
  return {
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
      createdBy: options.continuation === true ? "agent" : "user",
      creationSource: options.continuation === true ? "provider" : "web",
    },
    modelSelection: selection(effortOf(options), options.model ?? MODEL),
    runtimePolicy: policy(runtimeMode, options.interactionMode),
  } satisfies ProviderAdapter.ProviderAdapterV2TurnInput;
});

export const startTurn = Effect.fnUntraced(function* (
  runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
  providerThread: OrchestrationV2ProviderThread,
  options: TurnOptions = {},
) {
  yield* runtime.startTurn(yield* turnInputFor(providerThread, options));
});

/** The turn the orchestrator starts for a `/compact` message. */
export const compactTurn = Effect.fnUntraced(function* (
  runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
  providerThread: OrchestrationV2ProviderThread,
  options: TurnOptions = {},
) {
  if (runtime.compactThread === undefined) return assert.fail("the runtime has no compactThread");
  yield* runtime.compactThread(
    yield* turnInputFor(providerThread, { text: "/compact", ...options }),
  );
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

/** The queue a test reads the adapter's continuation offers from. */
export const makeOfferQueue = Effect.fnUntraced(function* () {
  const offers = yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
  const continuationRequests = {
    offer: (request: ProviderContinuationRequests.ProviderContinuationRequest) =>
      Queue.offer(offers, request).pipe(Effect.asVoid),
  };
  return { offers, continuationRequests };
});

/** The message Droid starts a turn of its own with when a background task ends. */
export const wakeMessage = (wakeId: string, text = "Background task completed.") => ({
  type: "create_message",
  message: {
    id: `task-completion:${wakeId}`,
    role: "system",
    content: [{ type: "text", text }],
    visibility: "llm_only",
  },
});

export const wakeCompleted = (wakeId: string, reason = "completed") => ({
  type: "agent_turn_completed",
  reason,
  turnId: `task-completion:${wakeId}`,
  tokenUsage: {
    inputTokens: 10,
    outputTokens: 5,
    cacheCreationTokens: 0,
    cacheReadTokens: 0,
    thinkingTokens: 0,
  },
});

/** One finished assistant message, streamed as Droid does: a delta and then its completion. */
export const assistantSays = Effect.fnUntraced(function* (
  process: FakeDroidProcess,
  messageId: string,
  text: string,
) {
  yield* process.emit({ type: "assistant_text_delta", messageId, blockIndex: 0, textDelta: text });
  yield* process.emit({ type: "assistant_text_complete", messageId, blockIndex: 0 });
});

export const BACKGROUND_TASK_ID = "5fd3ffdd-4f3f-41c8-8873-e1fd3f6a1378";

/** A background Task subagent: the launch Droid streams and its acknowledgement, so the task runs on. */
export const launchBackgroundTask = Effect.fnUntraced(function* (process: FakeDroidProcess) {
  const toolUseId = "chatcmpl-tool-0d8e2c1e5251402e8a35e8edbb902ac5";
  const task = (input: Record<string, unknown>) => ({
    type: "tool_call",
    toolUse: { type: "tool_use", id: toolUseId, name: "Task", input },
  });
  yield* process.emit(task({}));
  yield* process.emit(
    task({ subagent_type: "worker", description: "dig", prompt: "Dig in.", await: false }),
  );
  yield* process.emit({
    type: "child_session_available",
    childSessionId: BACKGROUND_TASK_ID,
    toolUseId,
    subagentType: "worker",
    description: "dig",
  });
  yield* process.emit({
    type: "tool_result",
    toolUseId,
    content:
      `Task launched in background.\ntask_id: ${BACKGROUND_TASK_ID}\n` +
      `session_id: ${BACKGROUND_TASK_ID}\nsubagent_type: worker\ndescription: dig\n` +
      "The task is running in a subagent session.",
    isError: false,
  });
});

/** The assistant message items the runtime published, in publication order. */
export const assistantItems = (events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) =>
  events.flatMap((event) =>
    event.type === "turn_item.updated" && event.turnItem.type === "assistant_message"
      ? [event.turnItem]
      : [],
  );

export const isTerminal = (event: ProviderAdapter.ProviderAdapterV2Event) =>
  event.type === "turn.terminal";

export const terminals = (events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) =>
  events.flatMap((event) => (event.type === "turn.terminal" ? [event] : []));

/** `ensureThread` for a fresh thread, then a bound provider thread row. */
export const ensure = Effect.fnUntraced(function* (
  { runtime }: OpenRuntime,
  options: {
    readonly runtimeMode?: RuntimeMode;
    readonly effort?: string | undefined;
    readonly model?: string;
  } = {},
) {
  const existing = yield* makeProviderThread();
  return yield* runtime.ensureThread({
    threadId: THREAD_ID,
    modelSelection: selection(effortOf(options), options.model ?? MODEL),
    runtimePolicy: policy(options.runtimeMode),
    existingProviderThread: existing,
  });
});

/** `resumeThread` for a stored session: a bound provider thread row on `nativeId`. */
export const resume = Effect.fnUntraced(function* ({ runtime }: OpenRuntime, nativeId: string) {
  return yield* runtime.resumeThread({ providerThread: yield* makeProviderThread(nativeId) });
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

export type Events = ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>;

/** The latest published state of each provider turn, oldest first. */
export const providerTurnsOf = (events: Events): ReadonlyArray<OrchestrationV2ProviderTurn> => {
  const latest = new Map<string, OrchestrationV2ProviderTurn>();
  for (const event of events) {
    if (event.type === "provider_turn.updated")
      latest.set(event.providerTurn.id, event.providerTurn);
  }
  return [...latest.values()];
};

export const providerThreadUpdates = (events: Events) =>
  events.flatMap((event) =>
    event.type === "provider_thread.updated" ? [event.providerThread] : [],
  );

export const refIds = (threads: ReadonlyArray<OrchestrationV2ProviderThread>) =>
  threads.map((thread) => thread.nativeThreadRef?.nativeId);

/** Waits for the `provider_thread.updated` that moves the thread to session `nativeId`. */
export const awaitRef = (opened: OpenRuntime, nativeId: string) =>
  opened.takeEvent(
    (event) =>
      event.type === "provider_thread.updated" &&
      event.providerThread.nativeThreadRef?.nativeId === nativeId,
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
