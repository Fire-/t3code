import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CheckpointId,
  EnvironmentId,
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type ChatAttachment,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { handoffBudget } from "@t3tools/provider-core/server/handoffBudget";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { toolOutputImages, compactDynamicToolOutput } from "@t3tools/shared/toolOutput";
import {
  makePrimeAdapterV2,
  PrimeAdapterV2Driver,
  PRIME_PROVIDER,
  type PrimeAdapterV2Options,
} from "./adapter.ts";
import { makePrimeRpcConnection, type PrimeRpcRecord } from "./rpc.ts";
import { loadMcpBridge } from "./mcpBridge.testkit.ts";
import { turnItemOutputText } from "../../../client-runtime/src/work-log/itemDetail.ts";

const layerTest = Layer.mergeAll(
  NodeServices.layer,
  IdAllocator.layer,
  McpProviderSessions.layer,
  TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
);

const decodeJsonLine = Schema.decodeSync(Schema.fromJsonString(Schema.Unknown));
const encodeJsonLine = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const PRIME_INSTANCE_ID = ProviderInstanceId.make("prime-agent");
const THREAD_ID = ThreadId.make("thread-prime-test");
const SESSION_ID = ProviderSessionId.make("provider-session-prime-test");
const FAKE_SESSION_FILE = "/fake/.prime/agent/sessions/--workspace--/0001_abc.jsonl";
/** Deliberately outside the valid pid range so a group-kill can never land. */
const FAKE_PID = 999_999_999;

const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: null,
});

const modelSelection = (model: string): ModelSelection => ({
  instanceId: PRIME_INSTANCE_ID,
  model,
});

interface FakePrime {
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly emit: (record: PrimeRpcRecord, chunkBytes?: number) => Effect.Effect<void>;
  readonly takeRequest: (type: string) => Effect.Effect<PrimeRpcRecord>;
  readonly queueMessages: (data: unknown) => void;
  readonly vetoNextSwitch: () => void;
  readonly queueState: (data: Record<string, unknown>) => void;
  readonly setStreaming: (streaming: boolean) => void;
  readonly deferNextState: () => void;
  readonly resolveDeferredState: (data: unknown) => Effect.Effect<void>;
  readonly failNextState: () => void;
  readonly deferNextLifecycle: (type: "switch_session" | "new_session" | "fork") => void;
  readonly deferNextModelSelection: () => void;
  readonly queueModels: (models: ReadonlyArray<unknown>) => void;
  readonly vetoNextNewSession: () => void;
  readonly allRequests: () => ReadonlyArray<PrimeRpcRecord>;
  readonly queueStats: (data: unknown) => void;
  readonly closeStdout: Effect.Effect<void>;
  /** The process exits with `code`, then its stdout closes. */
  readonly exit: (code: number) => Effect.Effect<void>;
  readonly lastSpawn: () => {
    readonly args: ReadonlyArray<string>;
    readonly env: NodeJS.ProcessEnv;
  };
  readonly spawns: () => ReadonlyArray<{
    readonly args: ReadonlyArray<string>;
    readonly env: NodeJS.ProcessEnv;
  }>;
  /** Fails the next `count` switch_session requests with Prime's error text. */
  readonly failSwitches: (count: number, error: string) => void;
  /**
   * Sets the live session-action snapshot that `get_state` reports and, like
   * Prime (core/agent-session.ts _emitQueueUpdate), pushes
   * `session_action_update` only when the snapshot changed.
   */
  readonly setSessionActions: (actions: FakeSessionActions) => Effect.Effect<void>;
  readonly setCompacting: (compacting: boolean) => void;
  /** Prime's id-less error response (daemon post-acceptance failure, unknown command). */
  readonly emitOrphanError: (command: string, error: string) => Effect.Effect<void>;
  /**
   * Answers `type` the way Prime answers a command it lacks: an id-less
   * unknown-command error. `get_entries` is always answered this way.
   */
  readonly answerAsUnknownCommand: (type: string) => void;
  /** Fails every `type` request with `error`, keeping the request id. */
  readonly failCommand: (type: string, error: string) => void;
  /** Types of every command T3 wrote to stdin, in order. */
  readonly commandLog: () => ReadonlyArray<string>;
}

interface FakeSessionActions {
  readonly queuedCount: number;
  readonly steering: ReadonlyArray<string>;
  readonly followUps: ReadonlyArray<string>;
  readonly active?: {
    readonly kind: "turn" | "session_command";
    readonly phase: "preparing" | "committing" | "running";
    readonly label?: string;
  };
}

const IDLE_ACTIONS: FakeSessionActions = { queuedCount: 0, steering: [], followUps: [] };
const RUNNING_TURN_ACTIONS: FakeSessionActions = {
  ...IDLE_ACTIONS,
  active: { kind: "turn", phase: "running", label: "Hello prime-agent" },
};

/** Prime's agent_end carries only the run's messages; an aborted run ends with an aborted assistant message. */
const agentEnd = (aborted = false): PrimeRpcRecord => ({
  type: "agent_end",
  messages: aborted ? [{ role: "assistant", content: [], stopReason: "aborted" }] : [],
});

/**
 * Prime's idle `get_state` reply as observed live (research/dependency-readiness.md),
 * minus the model object. Prime omits `sessionName` until one is set and has
 * no `pendingMessageCount`.
 */
const recordedIdleState = (sessionFile: string) => ({
  thinkingLevel: "high",
  isStreaming: false,
  isCompacting: false,
  steeringMode: "one-at-a-time",
  followUpMode: "one-at-a-time",
  sessionFile,
  sessionId: "00000000-0000-4000-8000-000000000002",
  autoCompactionEnabled: true,
  messageCount: 0,
  sessionActions: { queuedCount: 0, steering: [], followUps: [] },
  goal: { active: false, status: "idle", tokensUsed: 0, timeUsedSeconds: 0, continuationsUsed: 0 },
});
const makeFakePrime: Effect.Effect<FakePrime> = Effect.gen(function* () {
  const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
  const exitCode = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
  const requests = yield* Queue.unbounded<PrimeRpcRecord>();
  const messagesQueue: Array<unknown> = [];
  const stateQueue: Array<Record<string, unknown>> = [];
  const statsQueue: Array<unknown> = [];
  const allRequests: Array<PrimeRpcRecord> = [];
  let deferState = false;
  let deferredStateRequest: PrimeRpcRecord | undefined;
  let failState = false;
  let vetoSwitch = false;
  let switchFailures = 0;
  let switchFailureError = "";
  let vetoNewSession = false;
  let deferredLifecycle: string | undefined;
  let sessionFile = FAKE_SESSION_FILE;
  let sessionGeneration = 0;
  let models: ReadonlyArray<unknown> = [];
  let isStreaming: boolean | null = null;
  let isCompacting = false;
  let sessionActions: FakeSessionActions = IDLE_ACTIONS;
  let stdinBuffer = "";
  // Prime 1.0.0 has no get_entries (modes/rpc/rpc-mode.ts default branch).
  const unknownCommands = new Set<string>(["get_entries"]);
  const commandFailures = new Map<string, string>();

  const emit = (record: PrimeRpcRecord, chunkBytes?: number) =>
    Effect.gen(function* () {
      const bytes = new TextEncoder().encode(`${encodeJsonLine(record)}\n`);
      const size = chunkBytes ?? bytes.byteLength;
      for (let offset = 0; offset < bytes.byteLength; offset += size) {
        yield* Queue.offer(stdout, bytes.subarray(offset, offset + size));
      }
    });

  const respondTo = (record: PrimeRpcRecord): PrimeRpcRecord | null => {
    if (typeof record["id"] !== "string" && !(record["type"] === "prompt" && isStreaming !== null))
      return null;
    if (unknownCommands.has(String(record["type"]))) {
      return {
        type: "response",
        command: String(record["type"]),
        success: false,
        error: `Unknown command: ${String(record["type"])}`,
      };
    }
    const base = {
      type: "response",
      id: record["id"],
      command: String(record["type"]),
      success: true,
    };
    const commandFailure = commandFailures.get(String(record["type"]));
    if (commandFailure !== undefined) return { ...base, success: false, error: commandFailure };
    switch (record["type"]) {
      case "get_state":
        if (failState) {
          failState = false;
          return { ...base, success: false, error: "state unavailable" };
        }
        return {
          ...base,
          data: {
            ...recordedIdleState(sessionFile),
            isStreaming: isStreaming ?? false,
            isCompacting,
            sessionActions,
            ...stateQueue.shift(),
          },
        };
      case "prompt":
        if (isStreaming && record["streamingBehavior"] === undefined) {
          return {
            ...base,
            success: false,
            error:
              "Agent is already processing. Specify streamingBehavior ('steer' or 'followUp') to queue the message.",
          };
        }
        return base;
      case "get_available_models":
        return { ...base, data: { models } };
      case "new_session": {
        const cancelled = vetoNewSession;
        vetoNewSession = false;
        if (!cancelled) sessionFile = `/fake/new-${++sessionGeneration}.jsonl`;
        return { ...base, data: { cancelled } };
      }
      case "switch_session": {
        if (switchFailures > 0) {
          switchFailures -= 1;
          return { ...base, success: false, error: switchFailureError };
        }
        const cancelled = vetoSwitch;
        vetoSwitch = false;
        return { ...base, data: { cancelled } };
      }
      case "get_messages":
        return { ...base, data: messagesQueue.shift() ?? { messages: [] } };
      case "get_session_stats":
        return { ...base, data: statsQueue.shift() ?? {} };
      case "fork":
        return { ...base, data: { text: "Hello prime-agent", cancelled: false } };
      default:
        return base;
    }
  };

  const handleStdinChunk = (chunk: Uint8Array) =>
    Effect.gen(function* () {
      stdinBuffer += new TextDecoder().decode(chunk);
      while (true) {
        const newline = stdinBuffer.indexOf("\n");
        if (newline === -1) return;
        const line = stdinBuffer.slice(0, newline);
        stdinBuffer = stdinBuffer.slice(newline + 1);
        if (line.length === 0) continue;
        const record = decodeJsonLine(line) as PrimeRpcRecord;
        allRequests.push(record);
        yield* Queue.offer(requests, record);
        if (record["type"] === "get_state" && deferState) {
          deferState = false;
          deferredStateRequest = record;
          continue;
        }
        if (record["type"] === deferredLifecycle) {
          deferredLifecycle = undefined;
          continue;
        }
        const response = respondTo(record);
        if (response !== null) yield* emit(response);
      }
    });

  const spawns: Array<{ readonly args: ReadonlyArray<string>; readonly env: NodeJS.ProcessEnv }> =
    [];
  const spawner = ChildProcessSpawner.make((command) =>
    Effect.sync(() => {
      if (ChildProcess.isStandardCommand(command)) {
        spawns.push({
          args: command.args,
          env: command.options.env ?? {},
        });
      }
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(FAKE_PID),
        exitCode: Deferred.await(exitCode),
        isRunning: Effect.succeed(true),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach(handleStdinChunk),
        stdout: Stream.fromQueue(stdout),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
    }),
  );

  const takeRequest = (type: string): Effect.Effect<PrimeRpcRecord> =>
    Effect.gen(function* () {
      while (true) {
        const record = yield* Queue.take(requests);
        if (record["type"] === type) return record;
      }
    });

  return {
    spawner,
    emit,
    takeRequest,
    queueMessages: (data) => messagesQueue.push(data),
    deferNextState: () => {
      deferState = true;
    },
    resolveDeferredState: (data) =>
      Effect.gen(function* () {
        const record = deferredStateRequest;
        assert.isDefined(record);
        deferredStateRequest = undefined;
        yield* emit({
          type: "response",
          id: record!["id"],
          command: "get_state",
          success: true,
          data,
        });
      }),
    failNextState: () => {
      failState = true;
    },
    deferNextLifecycle: (type) => {
      deferredLifecycle = type;
    },
    deferNextModelSelection: () => {
      deferredLifecycle = "set_model";
    },
    queueModels: (value) => {
      models = value;
    },
    vetoNextNewSession: () => {
      vetoNewSession = true;
    },
    allRequests: () => allRequests,
    vetoNextSwitch: () => {
      vetoSwitch = true;
    },
    queueState: (data) => stateQueue.push(data),
    setStreaming: (streaming) => {
      isStreaming = streaming;
    },
    queueStats: (data) => statsQueue.push(data),
    closeStdout: Queue.end(stdout),
    exit: (code) =>
      Deferred.succeed(exitCode, ChildProcessSpawner.ExitCode(code)).pipe(
        Effect.andThen(Queue.end(stdout)),
      ),
    lastSpawn: () => spawns.at(-1) ?? { args: [], env: {} },
    spawns: () => spawns,
    failSwitches: (count, error) => {
      switchFailures = count;
      switchFailureError = error;
    },
    setSessionActions: (actions) =>
      Effect.suspend(() => {
        if (encodeJsonLine(actions) === encodeJsonLine(sessionActions)) return Effect.void;
        sessionActions = actions;
        return emit({ type: "session_action_update", actions });
      }),
    setCompacting: (compacting) => {
      isCompacting = compacting;
    },
    emitOrphanError: (command, error) => emit({ type: "response", command, success: false, error }),
    answerAsUnknownCommand: (type) => {
      unknownCommands.add(type);
    },
    failCommand: (type, error) => {
      commandFailures.set(type, error);
    },
    commandLog: () => allRequests.map((request) => String(request["type"])),
  } satisfies FakePrime;
});

const makeAdapter = Effect.fnUntraced(function* (
  fake: FakePrime,
  launchArgs = "",
  forkFake?: FakePrime,
  continuationRequests?: PrimeAdapterV2Options["continuationRequests"],
) {
  const spawner =
    forkFake === undefined
      ? fake.spawner
      : ChildProcessSpawner.make((command) =>
          ChildProcess.isStandardCommand(command) && command.args.includes("--fork")
            ? forkFake.spawner.spawn(command)
            : fake.spawner.spawn(command),
        );
  if (continuationRequests !== undefined) {
    return yield* PrimeAdapterV2Driver.create({
      instanceId: PRIME_INSTANCE_ID,
      displayName: undefined,
      enabled: true,
      environment: [],
      config: { enabled: true, binaryPath: "prime-agent", launchArgs, customModels: [] },
    }).pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Effect.provideService(HostProcess.Environment, {}),
      Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, {
        ...continuationRequests,
        take: Effect.never,
      }),
    );
  }
  return yield* makePrimeAdapterV2({
    instanceId: PRIME_INSTANCE_ID,
    settings: { enabled: true, binaryPath: "prime-agent", launchArgs, customModels: [] },
    environment: {},
  }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
});

const openRuntime = Effect.fnUntraced(function* (
  fake: FakePrime,
  model = "default",
  threadId = THREAD_ID,
  providerSessionId = SESSION_ID,
  forkFake?: FakePrime,
  continuationRequests?: PrimeAdapterV2Options["continuationRequests"],
) {
  const adapter = yield* makeAdapter(fake, "", forkFake, continuationRequests);
  const runtime = yield* adapter.openSession({
    threadId,
    providerSessionId,
    modelSelection: modelSelection(model),
    runtimePolicy,
  });
  const emitted = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  yield* runtime.events.pipe(
    Stream.runForEach((event) => Queue.offer(emitted, event)),
    Effect.forkScoped,
  );
  const takeEvent = (predicate: (event: ProviderAdapter.ProviderAdapterV2Event) => boolean) =>
    Effect.gen(function* () {
      while (true) {
        const event = yield* Queue.take(emitted);
        if (predicate(event)) return event;
      }
    });
  return { runtime, takeEvent };
});

/**
 * Advances virtual time in 1 s steps until `effect` completes. A retry's sleep
 * can register after any single adjustment, so one fixed adjust could miss it.
 */
const advanceClockUntil = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.raceFirst(effect, Effect.forever(TestClock.adjust(Duration.seconds(1))));

const makeAppThread = Effect.fnUntraced(function* (
  model: string,
  threadId = THREAD_ID,
  title = "Prime test thread",
) {
  const now = yield* DateTime.now;
  return {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId: "project:fixture:prime" as OrchestrationV2AppThread["projectId"],
    title,
    providerInstanceId: PRIME_INSTANCE_ID,
    modelSelection: modelSelection(model),
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
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

const startTurn = Effect.fnUntraced(function* (
  runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
  providerThread: OrchestrationV2ProviderThread,
  model = "default",
  attachments: ReadonlyArray<ChatAttachment> = [],
  text = "Hello prime-agent",
  selection?: ModelSelection,
  runOrdinal = 1,
  threadId = THREAD_ID,
  continuation = false,
  title?: string,
) {
  const appThread = yield* makeAppThread(model, threadId, title);
  const runId = RunId.make(`run:${threadId}:${runOrdinal}`);
  yield* runtime.startTurn({
    appThread,
    threadId,
    runId,
    runOrdinal,
    providerTurnOrdinal: runOrdinal,
    attemptId: RunAttemptId.make(`run-attempt:${runId}:1`),
    rootNodeId: NodeId.make(`node:${runId}:root`),
    providerThread,
    message: {
      messageId: `message:${threadId}:${runOrdinal}` as never,
      text,
      attachments,
      createdBy: continuation ? "agent" : "user",
      creationSource: continuation ? "provider" : "web",
    },
    modelSelection: selection ?? modelSelection(model),
    runtimePolicy,
  });
});

const expectModelFailure = (errorMessage: string) =>
  Effect.gen(function* () {
    const fake = yield* makeFakePrime;
    const { runtime, takeEvent } = yield* openRuntime(fake);
    const providerThread = yield* runtime.ensureThread({
      threadId: THREAD_ID,
      modelSelection: modelSelection("default"),
      runtimePolicy,
    });
    yield* startTurn(runtime, providerThread);
    yield* fake.takeRequest("prompt");
    yield* fake.emit({ type: "agent_start" });
    yield* fake.emit({
      type: "message_end",
      message: {
        role: "assistant",
        content: [],
        stopReason: "error",
        errorMessage,
      },
    });
    yield* fake.emit({ type: "agent_end", messages: [] });

    const sessionError = yield* takeEvent(
      (event) =>
        event.type === "provider_session.updated" && event.providerSession.status === "error",
    );
    assert.isTrue(
      sessionError.type === "provider_session.updated" &&
        sessionError.providerSession.lastError === errorMessage,
    );
    const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
    assert.isTrue(
      terminal.type === "turn.terminal" &&
        terminal.status === "failed" &&
        terminal.failure.message === errorMessage,
    );
  }).pipe(Effect.scoped, Effect.provide(layerTest));

describe("PrimeAdapterV2", () => {
  it.effect.each([false, true])(
    "completes a resumed user turn before native wake events arrive, Prime streaming=%s",
    (streaming) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        fake.setStreaming(streaming);
        yield* runtime.resumeThread({ providerThread });
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({
          type: "message_end",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "Resumed prompt completed" }],
            stopReason: "stop",
          },
        });
        fake.queueState({ isStreaming: false });
        yield* fake.emit({ type: "agent_end", messages: [] });
        const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([false, true])(
    "serializes rollback fork hooks with native wake ownership, wake=%s",
    (nativeWake) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const offers =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const { runtime, takeEvent } = yield* openRuntime(
          fake,
          "default",
          THREAD_ID,
          SESSION_ID,
          undefined,
          {
            offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          },
        );
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        fake.deferNextLifecycle("fork");
        const rollingBack = yield* runtime
          .rollbackThread({
            providerThread,
            target: {
              type: "thread_start",
              checkpointId: CheckpointId.make("checkpoint-start"),
              appRunOrdinal: 0,
            },
            providerThreadTurns: [
              {
                id: ProviderTurnId.make("previous-turn"),
                providerThreadId: providerThread.id,
                nodeId: NodeId.make("previous-node"),
                runAttemptId: null,
                nativeTurnRef: {
                  driver: PRIME_PROVIDER,
                  nativeId: "user-before-wake",
                  strength: "strong",
                },
                ordinal: 1,
                status: "completed",
                startedAt: providerThread.createdAt,
                completedAt: providerThread.createdAt,
              },
            ],
          })
          .pipe(Effect.result, Effect.forkScoped);
        const fork = yield* fake.takeRequest("fork");
        const overlappingTurn = yield* startTurn(runtime, providerThread).pipe(Effect.result);
        assert.equal(overlappingTurn._tag, "Failure");
        const overlappingRegistration = yield* runtime
          .ensureThread({
            threadId: THREAD_ID,
            modelSelection: modelSelection("default"),
            runtimePolicy,
          })
          .pipe(Effect.result);
        assert.equal(overlappingRegistration._tag, "Failure");
        yield* fake.emit({
          type: "extension_ui_request",
          id: "fork-hook-dialog",
          method: "confirm",
          title: "Restore this branch?",
        });
        const forkDialog = yield* takeEvent((event) => event.type === "runtime_request.updated");
        assert.isTrue(forkDialog.type === "runtime_request.updated");
        if (forkDialog.type !== "runtime_request.updated") return;
        assert.isNull(forkDialog.runtimeRequest.providerTurnId);
        yield* runtime.respondToRuntimeRequest({
          requestId: forkDialog.runtimeRequest.id,
          decision: "accept",
        });
        yield* fake.takeRequest("extension_ui_response");
        if (nativeWake) {
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit({ type: "message_start", message: { role: "assistant" } });
          yield* fake.emit({
            type: "message_update",
            assistantMessageEvent: {
              type: "text_end",
              contentIndex: 0,
              content: "Discarded branch wake output",
            },
          });
          yield* fake.emit({ type: "agent_end", messages: [] });
          const retired = yield* takeEvent(
            (event) =>
              event.type === "provider_session.updated" &&
              event.providerSession.status === "stopped",
          ).pipe(Effect.as(true), Effect.raceFirst(Queue.take(offers).pipe(Effect.as(false))));
          assert.isTrue(
            retired,
            "Prime offered native work while rollback was changing its binding",
          );
          const result = yield* Fiber.join(rollingBack);
          assert.equal(result._tag, "Failure");
          assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
          const staleTurn = yield* startTurn(
            runtime,
            providerThread,
            "default",
            [],
            "Synthetic wake",
            undefined,
            1,
            THREAD_ID,
            true,
          ).pipe(Effect.result);
          assert.equal(staleTurn._tag, "Failure");
          assert.isFalse(fake.allRequests().some((request) => request["type"] === "prompt"));
          return;
        }
        fake.queueState({ sessionFile: "/fake/rolled-back-session.jsonl" });
        yield* fake.emit({
          type: "response",
          id: fork["id"],
          command: "fork",
          success: true,
          data: { cancelled: false },
        });
        const result = yield* Fiber.join(rollingBack);
        assert.equal(result._tag, "Success");
        if (result._tag !== "Success") return;
        assert.equal(
          result.success.providerThread.nativeThreadRef?.nativeId,
          "/fake/rolled-back-session.jsonl",
        );
        yield* startTurn(runtime, result.success.providerThread);
        const prompt = yield* fake.takeRequest("prompt");
        assert.equal(prompt["message"], "Hello prime-agent");
        yield* fake.emit({ type: "response", command: "prompt", success: true });
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "agent_end", messages: [] });
        const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([false, true])(
    "retains native wake ownership after a rejected joining prompt, compaction aborted=%s",
    (aborted) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const offers =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const { runtime, takeEvent } = yield* openRuntime(
          fake,
          "default",
          THREAD_ID,
          SESSION_ID,
          undefined,
          {
            offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          },
        );
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* fake.takeRequest("get_state");
        yield* fake.emit({ type: "agent_start" });
        yield* Queue.take(offers);
        yield* fake.emit({ type: "agent_end", messages: [] });
        yield* fake.emit({ type: "compaction_start", reason: "manual" });
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        const rejection = "Cannot submit a prompt while compaction is in progress.";
        yield* fake.emit({ type: "response", command: "prompt", success: false, error: rejection });
        yield* fake.emit({
          type: "extension_ui_request",
          id: "native-compaction-dialog",
          method: "confirm",
          title: "Continue native work?",
        });
        const dialog = yield* takeEvent((event) => event.type === "runtime_request.updated");
        assert.isTrue(dialog.type === "runtime_request.updated");
        if (dialog.type !== "runtime_request.updated") return;
        assert.isNotNull(dialog.runtimeRequest.providerTurnId);
        yield* runtime.respondToRuntimeRequest({
          requestId: dialog.runtimeRequest.id,
          decision: "accept",
        });
        yield* fake.takeRequest("extension_ui_response");
        if (aborted) {
          yield* fake.emit({ type: "compaction_end", result: null, aborted: true });
          const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
          assert.isTrue(
            terminal.type === "turn.terminal" &&
              terminal.status === "failed" &&
              terminal.failure.message === rejection,
          );
          return;
        }
        fake.queueState({ isStreaming: true });
        yield* fake.emit({
          type: "compaction_end",
          result: { summary: "Native compaction finished" },
          willRetry: true,
        });
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "message_start", message: { role: "assistant" } });
        yield* fake.emit({
          type: "message_update",
          assistantMessageEvent: {
            type: "text_end",
            contentIndex: 0,
            content: "Native work completed",
          },
        });
        const message = yield* takeEvent((event) => event.type === "message.updated");
        assert.isTrue(message.type === "message.updated" && message.message.runId !== null);
        yield* fake.takeRequest("get_state");
        yield* fake.emit({ type: "agent_end", messages: [] });
        const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
        assert.isTrue(
          terminal.type === "turn.terminal" &&
            terminal.status === "failed" &&
            terminal.failure.message === rejection,
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("rejects rollback while native wake output is awaiting ownership", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const offers =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const { runtime, takeEvent } = yield* openRuntime(
        fake,
        "default",
        THREAD_ID,
        SESSION_ID,
        undefined,
        {
          offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
        },
      );
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({ type: "agent_start" });
      yield* Queue.take(offers);
      yield* fake.emit({ type: "message_start", message: { role: "assistant" } });
      yield* fake.emit({
        type: "message_update",
        assistantMessageEvent: {
          type: "text_end",
          contentIndex: 0,
          content: "Current branch wake output",
        },
      });
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* takeEvent(
        (event) =>
          event.type === "provider_session.updated" && event.providerSession.status === "ready",
      );
      const rolledBack = yield* runtime
        .rollbackThread({
          providerThread,
          target: {
            type: "thread_start",
            checkpointId: CheckpointId.make("checkpoint-start"),
            appRunOrdinal: 0,
          },
          providerThreadTurns: [
            {
              id: ProviderTurnId.make("previous-turn"),
              providerThreadId: providerThread.id,
              nodeId: NodeId.make("previous-node"),
              runAttemptId: null,
              nativeTurnRef: {
                driver: PRIME_PROVIDER,
                nativeId: "user-before-wake",
                strength: "strong",
              },
              ordinal: 1,
              status: "completed",
              startedAt: providerThread.createdAt,
              completedAt: providerThread.createdAt,
            },
          ],
        })
        .pipe(Effect.result);
      assert.equal(
        rolledBack._tag,
        "Failure",
        "Rollback changed the native session while its wake was still buffered",
      );
      assert.isFalse(fake.allRequests().some((request) => request["type"] === "fork"));
      assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
      yield* startTurn(
        runtime,
        providerThread,
        "default",
        [],
        "Synthetic wake",
        undefined,
        1,
        THREAD_ID,
        true,
      );
      const message = yield* takeEvent((event) => event.type === "message.updated");
      assert.equal(
        message.type === "message.updated" && message.message.text,
        "Current branch wake output",
      );
      yield* takeEvent((event) => event.type === "turn.terminal");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("answers model-selection dialogs before a user turn claims a native wake", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const offers =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const { runtime, takeEvent } = yield* openRuntime(
        fake,
        "default",
        THREAD_ID,
        SESSION_ID,
        undefined,
        { offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid) },
      );
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({ type: "agent_start" });
      yield* Queue.take(offers);
      const requests: Array<
        Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "runtime_request.updated" }>
      > = [];
      fake.deferNextModelSelection();
      const starting = yield* startTurn(
        runtime,
        providerThread,
        "default",
        [],
        "Continue the review",
        modelSelection("xai/grok-4.6"),
      ).pipe(Effect.forkScoped);
      const modelRequest = yield* fake.takeRequest("set_model");
      yield* fake.emit({
        type: "extension_ui_request",
        id: "model-selection-dialog",
        method: "confirm",
        title: "Use this model?",
      });
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* takeEvent((event) => {
        if (event.type === "runtime_request.updated") requests.push(event);
        return (
          event.type === "provider_session.updated" && event.providerSession.status === "ready"
        );
      });
      const dialog = requests[0]?.runtimeRequest;
      assert.isDefined(dialog, "Prime buffered the dialog needed to finish model selection");
      assert.equal(requests[0]?.threadId, THREAD_ID);
      assert.isNull(dialog!.providerTurnId);
      yield* runtime.respondToRuntimeRequest({ requestId: dialog!.id, decision: "accept" });
      const response = yield* fake.takeRequest("extension_ui_response");
      assert.equal(response["id"], "model-selection-dialog");
      assert.isTrue(response["confirmed"]);
      yield* fake.emit({
        type: "response",
        id: modelRequest["id"],
        command: "set_model",
        success: true,
        data: { provider: "xai", id: "grok-4.6" },
      });
      yield* Fiber.join(starting);
      const prompt = yield* fake.takeRequest("prompt");
      assert.equal(prompt["message"], "Continue the review");
      assert.equal(prompt["streamingBehavior"], "steer");
      const resolved = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" && event.runtimeRequest.id === dialog!.id,
      );
      assert.isTrue(
        resolved.type === "runtime_request.updated" &&
          resolved.runtimeRequest.status === "resolved",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("adopts a completed native wake without sending a synthetic prompt", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const offers =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const { runtime, takeEvent } = yield* openRuntime(
        fake,
        "default",
        THREAD_ID,
        SESSION_ID,
        undefined,
        {
          offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
        },
      );
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({ type: "agent_start" });
      const offer = yield* Queue.take(offers).pipe(
        Effect.raceFirst(
          takeEvent(
            (event) =>
              event.type === "provider_session.updated" && event.providerSession.status === "error",
          ).pipe(
            Effect.andThen(Effect.sync(() => assert.fail("Prime killed a routable native wake"))),
          ),
        ),
      );
      assert.equal(offer.threadId, THREAD_ID);
      assert.equal(offer.providerThreadId, providerThread.id);
      assert.equal(offer.delivery, "adapter_buffered");
      assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
      let dispatchCommitted = false;
      yield* offer.dispatchIfCurrent!(
        Effect.sync(() => {
          dispatchCommitted = true;
        }),
      );
      assert.isTrue(dispatchCommitted);
      assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
      yield* fake.emit({ type: "message_start", message: { role: "assistant" } });
      yield* fake.emit({
        type: "message_update",
        assistantMessageEvent: {
          type: "text_delta",
          contentIndex: 0,
          delta: "Background review finished",
        },
      });
      yield* fake.emit({ type: "message_end", message: { role: "assistant" } });
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* takeEvent(
        (event) =>
          event.type === "provider_session.updated" && event.providerSession.status === "ready",
      );
      assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
      yield* startTurn(
        runtime,
        providerThread,
        "default",
        [],
        "Synthetic wake",
        undefined,
        1,
        THREAD_ID,
        true,
      );
      const message = yield* takeEvent((event) => event.type === "message.updated");
      assert.equal(
        message.type === "message.updated" && message.message.text,
        "Background review finished",
      );
      yield* takeEvent((event) => event.type === "turn.terminal");
      assert.equal(fake.allRequests().filter((request) => request["type"] === "prompt").length, 0);
      assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
      let dispatched = false;
      yield* offer.dispatchIfCurrent!(
        Effect.sync(() => {
          dispatched = true;
        }),
      );
      assert.isFalse(dispatched);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([false, true])(
    "a user prompt takes the wake without premature settlement, native wake settled=%s",
    (settled) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const offers =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const { runtime, takeEvent } = yield* openRuntime(
          fake,
          "default",
          THREAD_ID,
          SESSION_ID,
          undefined,
          {
            offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          },
        );
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* fake.emit({ type: "agent_start" });
        const offer = yield* Queue.take(offers);
        if (settled) {
          yield* fake.emit({ type: "agent_end", messages: [] });
          yield* takeEvent(
            (event) =>
              event.type === "provider_session.updated" && event.providerSession.status === "ready",
          );
        }
        yield* startTurn(runtime, providerThread);
        const prompt = yield* fake.takeRequest("prompt");
        assert.equal(prompt["message"], "Hello prime-agent");
        assert.equal(prompt["streamingBehavior"], "steer");
        yield* fake.emit({ type: "response", command: "prompt", success: true });
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({
          type: "extension_ui_request",
          id: "owned-dialog",
          method: "confirm",
          title: "Continue?",
        });
        const dialog = yield* takeEvent((event) => event.type === "runtime_request.updated");
        assert.isTrue(dialog.type === "runtime_request.updated");
        if (dialog.type !== "runtime_request.updated") return;
        assert.isNotNull(dialog.runtimeRequest.providerTurnId);
        yield* offer.clearIfCurrent!();
        yield* runtime.respondToRuntimeRequest({
          requestId: dialog.runtimeRequest.id,
          decision: "accept",
        });
        const response = yield* fake.takeRequest("extension_ui_response");
        assert.isTrue(response["confirmed"]);
        yield* fake.emit({ type: "message_start", message: { role: "assistant" } });
        yield* fake.emit({
          type: "message_update",
          assistantMessageEvent: {
            type: "text_end",
            contentIndex: 0,
            content: "User prompt completed",
          },
        });
        yield* fake.emit({ type: "agent_end", messages: [] });
        const message = yield* takeEvent(
          (event) =>
            event.type === "message.updated" && event.message.text === "User prompt completed",
        );
        assert.isTrue(message.type === "message.updated" && message.message.runId !== null);
        yield* takeEvent((event) => event.type === "turn.terminal");
        let dispatched = false;
        yield* offer.dispatchIfCurrent!(
          Effect.sync(() => {
            dispatched = true;
          }),
        );
        assert.isFalse(dispatched);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([false, true])(
    "defers a wake's late settlement until the joined user prompt is acknowledged, aborted=%s",
    (aborted) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const offers =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const { runtime, takeEvent } = yield* openRuntime(
          fake,
          "default",
          THREAD_ID,
          SESSION_ID,
          undefined,
          {
            offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          },
        );
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* fake.emit({ type: "agent_start" });
        yield* Queue.take(offers);
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit(agentEnd(aborted));
        yield* fake.emit({
          type: "extension_ui_request",
          method: "notify",
          message: "Ordering barrier",
        });
        yield* takeEvent(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "dynamic_tool" &&
            event.turnItem.toolName === "notify",
        );
        assert.equal(runtime.providerSession.status, "running");
        fake.queueState({ isStreaming: true });
        yield* fake.emit({ type: "response", command: "prompt", success: true });
        yield* fake.takeRequest("get_state");
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "agent_end", messages: [] });
        const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    { aborted: false, resumes: false, expected: "completed" },
    { aborted: true, resumes: false, expected: "interrupted" },
    { aborted: true, resumes: true, expected: "completed" },
  ])(
    "settles a completed wake, aborted=$aborted, resumes=$resumes",
    ({ aborted, resumes, expected }) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const offers =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const { runtime, takeEvent } = yield* openRuntime(
          fake,
          "default",
          THREAD_ID,
          SESSION_ID,
          undefined,
          {
            offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          },
        );
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* fake.emit({ type: "agent_start" });
        yield* Queue.take(offers);
        yield* fake.emit(agentEnd(aborted));
        yield* takeEvent(
          (event) =>
            event.type === "provider_session.updated" && event.providerSession.status === "ready",
        );
        yield* startTurn(runtime, providerThread, "default", [], "/hello");
        yield* fake.takeRequest("prompt");
        if (resumes) yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "response", command: "prompt", success: true });
        if (resumes) yield* fake.emit(agentEnd(false));
        const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === expected);
        yield* startTurn(
          runtime,
          providerThread,
          "default",
          [],
          "Stale continuation",
          undefined,
          2,
          THREAD_ID,
          true,
        );
        yield* takeEvent((event) => event.type === "turn.terminal");
        assert.equal(
          fake.allRequests().filter((request) => request["type"] === "prompt").length,
          1,
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps a pending wake on its thread and lets Stop retire its process", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const offers =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const { runtime, takeEvent } = yield* openRuntime(
        fake,
        "default",
        THREAD_ID,
        SESSION_ID,
        undefined,
        {
          offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
        },
      );
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({ type: "agent_start" });
      const offer = yield* Queue.take(offers);
      assert.isTrue(yield* runtime.hasPendingBackgroundWorkForThread!(providerThread));
      const otherThread = {
        ...providerThread,
        appThreadId: ThreadId.make("other-thread"),
        id: ProviderThreadId.make("other-provider-thread"),
      };
      assert.isFalse(yield* runtime.hasPendingBackgroundWorkForThread!(otherThread));
      const failure = yield* runtime
        .resumeThread({ providerThread: otherThread })
        .pipe(Effect.flip);
      assert.equal(failure._tag, "ProviderAdapterResumeThreadError");
      yield* runtime.interruptTurn({
        providerThread,
        providerTurnId: ProviderTurnId.make("settled-turn"),
        requestRuntimeRestart: true,
      });
      yield* takeEvent(
        (event) =>
          event.type === "provider_session.updated" && event.providerSession.status === "stopped",
      );
      assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
      let dispatched = false;
      yield* offer.dispatchIfCurrent!(
        Effect.sync(() => {
          dispatched = true;
        }),
      );
      assert.isFalse(dispatched);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each(["archive", "stop", "transport death"] as const)(
    "retiring a wake by %s cancels visible dialogs and invalidates its offer",
    (retirement) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const offers =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const { runtime, takeEvent } = yield* openRuntime(
          fake,
          "default",
          THREAD_ID,
          SESSION_ID,
          undefined,
          {
            offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          },
        );
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* fake.emit({ type: "agent_start" });
        const offer = yield* Queue.take(offers);
        yield* fake.emit({
          type: "extension_ui_request",
          id: "wake-dialog",
          method: "confirm",
          title: "Continue background work?",
        });
        const dialog = yield* takeEvent((event) => event.type === "runtime_request.updated");
        assert.isTrue(dialog.type === "runtime_request.updated");
        if (dialog.type !== "runtime_request.updated") return;
        assert.isNull(dialog.runtimeRequest.providerTurnId);
        if (retirement === "archive") yield* offer.clearIfCurrent!();
        else if (retirement === "stop") {
          yield* runtime.interruptTurn({
            providerThread,
            providerTurnId: ProviderTurnId.make("settled-turn"),
            requestRuntimeRestart: true,
          });
        } else yield* fake.closeStdout;
        let requestCancelled = false;
        let nodeCancelled = false;
        let itemCancelled = false;
        yield* takeEvent((event) => {
          if (
            event.type === "runtime_request.updated" &&
            event.runtimeRequest.id === dialog.runtimeRequest.id
          )
            requestCancelled = event.runtimeRequest.status === "cancelled";
          if (event.type === "node.updated" && event.node.id === dialog.runtimeRequest.nodeId)
            nodeCancelled = event.node.status === "cancelled";
          if (
            event.type === "turn_item.updated" &&
            event.turnItem.nodeId === dialog.runtimeRequest.nodeId
          )
            itemCancelled = event.turnItem.status === "cancelled";
          return (
            event.type === "provider_session.updated" &&
            event.providerSession.status ===
              (retirement === "transport death" ? "error" : "stopped")
          );
        });
        assert.isTrue(requestCancelled, "Retired wake left a pending runtime request");
        assert.isTrue(nodeCancelled);
        assert.isTrue(itemCancelled);
        assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
        let dispatched = false;
        yield* offer.dispatchIfCurrent!(
          Effect.sync(() => {
            dispatched = true;
          }),
        );
        assert.isFalse(dispatched);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("stops provider-initiated work that has no T3 turn owner", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });

      yield* fake.emit({ type: "agent_start" });

      const sessionError = yield* takeEvent(
        (event) =>
          event.type === "provider_session.updated" && event.providerSession.status === "error",
      );
      assert.isTrue(
        sessionError.type === "provider_session.updated" &&
          sessionError.providerSession.lastError?.includes("invisible tool execution") === true,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-LAUNCH-001] launches --mode rpc with the T3 bridge, its env, and no Pi-only or daemon flags",
    () =>
      Effect.gen(function* () {
        yield* (yield* McpProviderSessions.McpProviderSessions).set({
          environmentId: EnvironmentId.make("environment-prime-mcp"),
          threadId: THREAD_ID,
          providerSessionId: "mcp-session-prime",
          providerInstanceId: PRIME_INSTANCE_ID,
          endpoint: "http://127.0.0.1:43123/mcp",
          authorizationHeader: "Bearer secret-prime-token",
          browserToolsAvailable: true,
        });
        const fake = yield* makeFakePrime;
        yield* openRuntime(fake);
        assert.equal(fake.spawns().length, 1);
        const spawn = fake.lastSpawn();
        assert.deepEqual(spawn.args.slice(0, 2), ["--mode", "rpc"]);
        const extensions = spawn.args.flatMap((arg, index) =>
          arg === "-e" ? [spawn.args[index + 1]] : [],
        );
        assert.equal(extensions.length, 1);
        assert.isTrue(extensions[0]?.endsWith("prime-t3-mcp-extension.ts"));
        for (const flag of [
          "--session",
          "--approve",
          "--no-approve",
          "--exclude-tools",
          "--name",
          "--tui-mode",
          "--use-theme",
          "--daemon-socket",
          "--no-extensions",
          "--no-skills",
          "--no-context-files",
          "--no-session",
        ]) {
          assert.notInclude(spawn.args, flag);
        }
        assert.equal(spawn.env.T3_MCP_URL, "http://127.0.0.1:43123/mcp");
        assert.equal(spawn.env.T3_MCP_BEARER_TOKEN, "secret-prime-token");
        assert.equal(spawn.env.T3_PRIME_RUNTIME_MODE, "full-access");
        assert.equal(spawn.env.T3_PRIME_MCP_EXTENSION_PATH, extensions[0]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-LAUNCH-001] rejects reserved launch arguments before spawning and passes others through",
    () =>
      Effect.gen(function* () {
        const rejected = yield* makeFakePrime;
        const adapter = yield* makeAdapter(rejected, "--daemon-socket /tmp/other.sock");
        const error = yield* adapter
          .openSession({
            threadId: THREAD_ID,
            providerSessionId: SESSION_ID,
            modelSelection: modelSelection("default"),
            runtimePolicy,
          })
          .pipe(Effect.flip);
        assert.include(
          JSON.stringify(error),
          "Prime launch argument '--daemon-socket' is controlled by T3 Code and cannot be overridden.",
        );
        assert.equal(rejected.spawns().length, 0);

        const accepted = yield* makeFakePrime;
        const passthrough = yield* makeAdapter(accepted, "--offline --skill review --goal=ship");
        yield* passthrough.openSession({
          threadId: THREAD_ID,
          providerSessionId: SESSION_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        assert.deepEqual(accepted.lastSpawn().args.slice(0, 8), [
          "--mode",
          "rpc",
          "--offline",
          "--skill",
          "review",
          "--goal",
          "ship",
          "-e",
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-LAUNCH-002] reopens a thread in a new process with switch_session and completes a turn",
    () =>
      Effect.gen(function* () {
        const first = yield* makeFakePrime;
        const providerThread = yield* Effect.gen(function* () {
          const { runtime, takeEvent } = yield* openRuntime(first);
          const created = yield* runtime.ensureThread({
            threadId: THREAD_ID,
            modelSelection: modelSelection("default"),
            runtimePolicy,
          });
          yield* startTurn(runtime, created);
          yield* first.takeRequest("prompt");
          yield* first.emit({ type: "agent_start" });
          yield* first.emit({
            type: "message_end",
            message: {
              role: "assistant",
              content: [{ type: "text", text: "One" }],
              stopReason: "stop",
            },
          });
          yield* first.emit({ type: "agent_end", messages: [] });
          const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          return created;
        }).pipe(Effect.scoped);
        assert.equal(providerThread.nativeThreadRef?.nativeId, FAKE_SESSION_FILE);

        const second = yield* makeFakePrime;
        const { runtime, takeEvent } = yield* openRuntime(
          second,
          "default",
          THREAD_ID,
          ProviderSessionId.make("provider-session-prime-reopened"),
        );
        const resumed = yield* runtime.resumeThread({ providerThread });
        const switched = yield* second.takeRequest("switch_session");
        assert.equal(switched.sessionPath, FAKE_SESSION_FILE);
        assert.equal(resumed.nativeThreadRef?.nativeId, FAKE_SESSION_FILE);
        assert.equal(resumed.id, providerThread.id);
        assert.notInclude(second.lastSpawn().args, "--resume");
        assert.isFalse(second.allRequests().some((request) => request.type === "new_session"));

        yield* startTurn(runtime, resumed, "default", [], "Two", undefined, 2);
        const prompt = yield* second.takeRequest("prompt");
        assert.equal(prompt.message, "Two");
        yield* second.emit({ type: "agent_start" });
        yield* second.emit({
          type: "message_end",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "Two" }],
            stopReason: "stop",
          },
        });
        yield* second.emit({ type: "agent_end", messages: [] });
        const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-LAUNCH-003] retries a resume while Prime reports the session already active, then opens",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const { runtime } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        fake.failSwitches(3, `Session is already active in prime-worker-7: ${FAKE_SESSION_FILE}`);
        const startedAt = yield* Clock.currentTimeMillis;
        const resumed = yield* advanceClockUntil(runtime.resumeThread({ providerThread }));
        assert.isBelow((yield* Clock.currentTimeMillis) - startedAt, 45_000);
        assert.equal(resumed.nativeThreadRef?.nativeId, FAKE_SESSION_FILE);
        assert.equal(
          fake.allRequests().filter((request) => request.type === "switch_session").length,
          4,
        );
        yield* startTurn(runtime, resumed);
        yield* fake.takeRequest("prompt");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    {
      error: `Session is already active in prime-worker-7: ${FAKE_SESSION_FILE}`,
      sessionId: "prime-worker-7",
    },
    {
      error: `Session is already active in another process: ${FAKE_SESSION_FILE}`,
      sessionId: "0001_abc",
    },
  ])(
    "[VAL-LAUNCH-003] fails a resume still blocked after ~45 s, naming $sessionId and prime-agent stop",
    ({ error, sessionId }) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const { runtime } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        fake.failSwitches(Number.POSITIVE_INFINITY, error);
        const startedAt = yield* Clock.currentTimeMillis;
        const failure = yield* advanceClockUntil(
          runtime.resumeThread({ providerThread }).pipe(Effect.flip),
        );
        const elapsed = (yield* Clock.currentTimeMillis) - startedAt;
        assert.isAtLeast(elapsed, 45_000);
        assert.isBelow(elapsed, 50_000);
        assert.equal(failure._tag, "ProviderAdapterResumeThreadError");
        const message = JSON.stringify(failure.cause);
        assert.include(message, `Prime session ${sessionId} is still open`);
        assert.include(message, `prime-agent stop ${sessionId}`);
        assert.isAbove(
          fake.allRequests().filter((request) => request.type === "switch_session").length,
          40,
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("[VAL-LAUNCH-003] does not retry other switch_session failures", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      fake.failSwitches(1, "Session file not found");
      const failure = yield* runtime.resumeThread({ providerThread }).pipe(Effect.flip);
      assert.include(JSON.stringify(failure.cause), "Session file not found");
      assert.equal(
        fake.allRequests().filter((request) => request.type === "switch_session").length,
        1,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("[VAL-LAUNCH-001] ignores unknown and unmapped Prime events during a turn", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "future_prime_event", payload: { nested: true } });
      yield* fake.emit({
        type: "session_action_update",
        actions: { queuedCount: 0, steering: [], followUps: [] },
      });
      yield* fake.emit({
        type: "rlm_child_update",
        child: { id: "child-1", label: "explore", status: "running", sessionDir: "/tmp/c" },
      });
      yield* fake.emit({
        type: "message_end",
        message: {
          role: "assistant",
          content: [{ type: "text", text: "Done" }],
          stopReason: "stop",
        },
      });
      yield* fake.emit({ type: "agent_end", messages: [] });
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
  it.effect("rejects a resume while a turn is active", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const error = yield* runtime.resumeThread({ providerThread }).pipe(Effect.flip);
      assert.equal(error._tag, "ProviderAdapterResumeThreadError");
      assert.match(String(error.cause), /while a turn is active/);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("waits for a slow Prime resume without starting a replacement", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      fake.deferNextLifecycle("switch_session");
      const resumed = yield* runtime.resumeThread({ providerThread }).pipe(Effect.forkChild);
      const request = yield* fake.takeRequest("switch_session");
      yield* TestClock.adjust(Duration.millis(16_820));
      yield* fake.emit({
        type: "response",
        id: request.id,
        command: "switch_session",
        success: true,
        data: { cancelled: false },
      });
      assert.equal((yield* Fiber.join(resumed)).nativeThreadRef?.nativeId, FAKE_SESSION_FILE);
      assert.isFalse(fake.allRequests().some((request) => request.type === "new_session"));
      yield* startTurn(runtime, providerThread, "default");
      yield* fake.takeRequest("prompt");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("creates a distinct native session after a failed resume", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      fake.vetoNextSwitch();
      yield* runtime.resumeThread({ providerThread }).pipe(Effect.flip);
      const replacement = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
        existingProviderThread: { ...providerThread, nativeThreadRef: null },
      });
      assert.equal(replacement.id, providerThread.id);
      assert.notEqual(
        replacement.nativeThreadRef?.nativeId,
        providerThread.nativeThreadRef?.nativeId,
      );
      assert.equal(
        fake.allRequests().filter((request) => request.type === "new_session").length,
        1,
      );
      yield* startTurn(runtime, replacement, "default");
      yield* fake.takeRequest("prompt");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each(["veto", "same identity"] as const)(
    "rejects a replacement with %s",
    (invalidReplacement) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const { runtime } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        if (invalidReplacement === "veto") fake.vetoNextNewSession();
        else fake.queueState({ sessionFile: FAKE_SESSION_FILE });
        const error = yield* runtime
          .ensureThread({
            threadId: THREAD_ID,
            modelSelection: modelSelection("default"),
            runtimePolicy,
            existingProviderThread: { ...providerThread, nativeThreadRef: null },
          })
          .pipe(Effect.flip);
        assert.equal(error._tag, "ProviderAdapterEnsureThreadError");
        assert.match(
          String(error.cause),
          invalidReplacement === "veto" ? /cancelled/ : /distinct session/,
        );
        yield* startTurn(runtime, providerThread, "default").pipe(Effect.flip);
        assert.isFalse(fake.allRequests().some((request) => request.type === "prompt"));
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("retires a timed-out lifecycle process before a late switch can race replacement", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      fake.deferNextLifecycle("switch_session");
      const resumed = yield* runtime
        .resumeThread({ providerThread })
        .pipe(Effect.flip, Effect.forkChild);
      const request = yield* fake.takeRequest("switch_session");
      yield* TestClock.adjust(Duration.seconds(60));
      const error = yield* Fiber.join(resumed);
      assert.match(String(error.cause), /timed out after 60000ms/);
      yield* takeEvent(
        (event) =>
          event.type === "provider_session.updated" && event.providerSession.status === "error",
      );
      yield* fake.emit({
        type: "response",
        id: request.id,
        command: "switch_session",
        success: true,
        data: { cancelled: false },
      });
      yield* runtime
        .ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
          existingProviderThread: { ...providerThread, nativeThreadRef: null },
        })
        .pipe(Effect.flip);
      assert.isFalse(
        fake
          .allRequests()
          .some((request) => request.type === "new_session" || request.type === "prompt"),
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "uses the lifecycle deadline for fresh sessions and drops replaced native metadata",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const { runtime } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        fake.deferNextLifecycle("new_session");
        fake.queueState({ sessionFile: "/fake/fresh-after-delay.jsonl" });
        const replacing = yield* runtime
          .ensureThread({
            threadId: THREAD_ID,
            modelSelection: modelSelection("default"),
            runtimePolicy,
            existingProviderThread: {
              ...providerThread,
              nativeThreadRef: null,
              nativeConversationHeadRef: {
                driver: PRIME_PROVIDER,
                nativeId: "old-leaf",
                strength: "strong",
              },
              contextUsage: { usedTokens: 314_551, maxTokens: 1_000_000 },
            },
          })
          .pipe(Effect.forkChild);
        const request = yield* fake.takeRequest("new_session");
        yield* TestClock.adjust(Duration.millis(16_820));
        yield* fake.emit({
          type: "response",
          id: request.id,
          command: "new_session",
          success: true,
          data: { cancelled: false },
        });
        const replacement = yield* Fiber.join(replacing);
        assert.equal(replacement.nativeThreadRef?.nativeId, "/fake/fresh-after-delay.jsonl");
        assert.isNull(replacement.contextUsage);
        assert.isNull(replacement.nativeConversationHeadRef);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("replaces a native session when the first resume's state refresh fails", () =>
    Effect.gen(function* () {
      const original = yield* makeFakePrime;
      const originalRuntime = yield* openRuntime(original);
      const providerThread = yield* originalRuntime.runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const fake = yield* makeFakePrime;
      const { runtime } = yield* openRuntime(fake);
      fake.failNextState();
      yield* runtime.resumeThread({ providerThread }).pipe(Effect.flip);
      const replacement = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
        existingProviderThread: { ...providerThread, nativeThreadRef: null },
      });
      assert.notEqual(
        replacement.nativeThreadRef?.nativeId,
        providerThread.nativeThreadRef?.nativeId,
      );
      yield* startTurn(runtime, replacement, "default");
      yield* fake.takeRequest("prompt");
      assert.isTrue(fake.allRequests().some((request) => request.type === "new_session"));
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("retires an interrupted switch before accepting further requests", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      fake.deferNextLifecycle("switch_session");
      const resumed = yield* runtime.resumeThread({ providerThread }).pipe(Effect.forkChild);
      yield* fake.takeRequest("switch_session");
      yield* Fiber.interrupt(resumed);
      yield* takeEvent(
        (event) =>
          event.type === "provider_session.updated" && event.providerSession.status === "error",
      );
      yield* runtime
        .ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
          existingProviderThread: { ...providerThread, nativeThreadRef: null },
        })
        .pipe(Effect.flip);
      assert.isFalse(fake.allRequests().some((request) => request.type === "new_session"));
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("budgets legacy native history with Prime's selected model capacity", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      fake.queueState({
        sessionFile: FAKE_SESSION_FILE,
        model: { provider: "anthropic", id: "large", contextWindow: 1_000_000 },
      });
      fake.queueModels([{ provider: "anthropic", id: "small", contextWindow: 32_000 }]);
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const budget = (model: string) =>
        handoffBudget({
          tokenCap: 16_000,
          userText: "$handoff",
          attachments: [],
          providerThread,
          nativeContextEstimate: 307_543,
          modelContextWindow: runtime.getModelContextWindow?.(modelSelection(model)),
        });
      assert.equal(budget("default"), 16_000);
      assert.equal(budget("anthropic/large"), 16_000);
      assert.equal(runtime.getModelContextWindow?.(modelSelection("anthropic/small")), 32_000);
      assert.equal(budget("anthropic/small"), 0);
      assert.isUndefined(runtime.getModelContextWindow?.(modelSelection("anthropic/unknown")));
      assert.isUndefined(
        runtime.getModelContextWindow?.({
          instanceId: ProviderInstanceId.make("other-prime"),
          model: "anthropic/large",
        }),
      );
      fake.queueState({
        sessionFile: "/fake/replacement.jsonl",
        model: { provider: "anthropic", id: "small", contextWindow: 32_000 },
      });
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
        existingProviderThread: { ...providerThread, nativeThreadRef: null },
      });
      assert.equal(runtime.getModelContextWindow?.(modelSelection("default")), 32_000);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("adopts the run's provider thread identity instead of minting a second row", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const now = yield* DateTime.now;
      const placeholder: OrchestrationV2ProviderThread = {
        id: ProviderThreadId.make(
          "thread:provider:prime-agent:native-thread:pending:run:thread-prime-test:1",
        ),
        driver: PRIME_PROVIDER,
        providerInstanceId: PRIME_INSTANCE_ID,
        providerSessionId: SESSION_ID,
        appThreadId: THREAD_ID,
        ownerNodeId: null,
        nativeThreadRef: null,
        nativeConversationHeadRef: null,
        status: "not_loaded",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
        existingProviderThread: placeholder,
      });
      assert.equal(providerThread.id, placeholder.id);
      assert.equal(providerThread.nativeThreadRef?.nativeId, FAKE_SESSION_FILE);
      assert.isFalse(fake.allRequests().some((request) => request.type === "new_session"));
      const updated = yield* takeEvent((event) => event.type === "provider_thread.updated");
      assert.isTrue(
        updated.type === "provider_thread.updated" && updated.providerThread.id === placeholder.id,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("resets applied thinking when returning to Prime default", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      fake.queueState({
        model: { provider: "xai", id: "grok-4.6" },
        thinkingLevel: "medium",
        sessionFile: FAKE_SESSION_FILE,
        sessionId: "abc",
      });
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });

      yield* startTurn(runtime, providerThread, "default", [], "Hello prime-agent", {
        instanceId: PRIME_INSTANCE_ID,
        model: "xai/grok-4.6",
        options: [{ id: "thinking", value: "high" }],
      });
      const modelRequest = yield* fake.takeRequest("set_model");
      assert.equal(modelRequest["provider"], "xai");
      assert.equal(modelRequest["modelId"], "grok-4.6");
      const levelRequest = yield* fake.takeRequest("set_thinking_level");
      assert.equal(levelRequest["level"], "high");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* takeEvent((event) => event.type === "turn.terminal");

      yield* startTurn(
        runtime,
        providerThread,
        "default",
        [],
        "Hello prime-agent",
        {
          instanceId: PRIME_INSTANCE_ID,
          model: "default",
        },
        2,
      );
      const replayModel = yield* fake.takeRequest("set_model");
      assert.equal(replayModel["provider"], "xai");
      assert.equal(replayModel["modelId"], "grok-4.6");
      const resetLevel = yield* fake.takeRequest("set_thinking_level");
      assert.equal(resetLevel["level"], "medium");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("[VAL-SET-003] selects a saved model and ignores its reasoningEffort option", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });

      yield* startTurn(runtime, providerThread, "default", [], "Hello prime-agent", {
        instanceId: PRIME_INSTANCE_ID,
        model: "mistral/zai-glm-5-3",
        options: [{ id: "reasoningEffort", value: "high" }],
      });
      const modelRequest = yield* fake.takeRequest("set_model");
      assert.equal(modelRequest["provider"], "mistral");
      assert.equal(modelRequest["modelId"], "zai-glm-5-3");
      yield* fake.takeRequest("prompt");
      assert.isFalse(
        fake.allRequests().some((request) => request["type"] === "set_thinking_level"),
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("preserves selected skill references for the Prime extension to expand", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });

      yield* startTurn(
        runtime,
        providerThread,
        "default",
        [],
        "Review this change please $repo-review",
      );
      const prompt = yield* fake.takeRequest("prompt");
      assert.equal(prompt["message"], "Review this change please $repo-review");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    { historical: false, label: "the latest turn" },
    { historical: true, label: "a historical turn" },
  ])("natively forks $label into an independent session", ({ historical }) =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const forkFake = yield* makeFakePrime;
      const forkFile = "/fake/forked.jsonl";
      const { runtime, takeEvent } = yield* openRuntime(
        fake,
        "default",
        THREAD_ID,
        SESSION_ID,
        forkFake,
      );
      const source = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const turn = (ordinal: number): OrchestrationV2ProviderTurn => ({
        id: ProviderTurnId.make(`turn-${ordinal}`),
        providerThreadId: source.id,
        nodeId: NodeId.make(`node-${ordinal}`),
        runAttemptId: null,
        nativeTurnRef: { driver: PRIME_PROVIDER, nativeId: `u${ordinal}`, strength: "strong" },
        ordinal,
        status: "completed",
        startedAt: null,
        completedAt: null,
      });
      forkFake.queueState({ sessionFile: forkFile });
      fake.queueState({ sessionFile: forkFile });
      const target = ThreadId.make("fork-target");
      const forked = yield* runtime.forkThread({
        sourceProviderThread: source,
        sourceProviderTurns: historical ? [turn(1), turn(2)] : [turn(1)],
        providerTurnId: turn(1).id,
        targetThreadId: target,
      });
      assert.equal(forked.appThreadId, target);
      assert.equal(forked.nativeThreadRef?.nativeId, forkFile);
      assert.notEqual(forked.id, source.id);
      assert.equal(source.nativeThreadRef?.nativeId, FAKE_SESSION_FILE);
      const args = forkFake.lastSpawn().args;
      assert.equal(args[args.indexOf("--fork") + 1], FAKE_SESSION_FILE);
      assert.include(args, "--no-extensions");
      assert.include(args, "--no-tools");
      assert.notInclude(args, "--no-session");
      assert.deepEqual(
        forkFake
          .allRequests()
          .filter((request) => request.type === "fork")
          .map((request) => request.entryId),
        historical ? ["u2"] : [],
      );
      assert.isFalse(
        fake.allRequests().some((request) => request.type === "fork" || request.type === "clone"),
      );
      const adopted = { ...forked, id: ProviderThreadId.make("pending-fork-row") };
      yield* startTurn(runtime, adopted, "default", [], "Continue", undefined, 1, target);
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "agent_end", messages: [] });
      const updated = yield* takeEvent(
        (event) =>
          event.type === "provider_thread.updated" && event.providerThread.appThreadId === target,
      );
      assert.isTrue(
        updated.type === "provider_thread.updated" && updated.providerThread.id === adopted.id,
      );
      yield* takeEvent((event) => event.type === "turn.terminal");
      yield* runtime.resumeThread({ providerThread: adopted });
      assert.equal(
        fake.allRequests().findLast((request) => request.type === "switch_session")?.sessionPath,
        forkFile,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("presents explicitly namespaced MCP extension tools", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      for (const type of ["tool_execution_start", "tool_execution_end"]) {
        yield* fake.emit({
          type,
          toolCallId: "weather-call",
          toolName: "mcp__weather__get_weather",
          args: { city: "Berlin" },
          result: { content: [{ type: "text", text: "Sunny" }] },
          isError: false,
        });
        const event = yield* takeEvent(
          (event) => event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool",
        );
        if (event.type !== "turn_item.updated" || event.turnItem.type !== "dynamic_tool")
          return yield* Effect.die("Expected an MCP tool item");
        assert.equal(event.turnItem.title, "get weather");
        assert.equal(
          event.turnItem.status,
          type === "tool_execution_start" ? "running" : "completed",
        );
        assert.deepEqual(event.turnItem.toolSource, {
          key: "mcp:weather",
          name: "weather",
          kind: "integration",
        });
        assert.deepEqual(event.turnItem.input, { city: "Berlin" });
      }
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "keeps native and MCP images available to the shared asset reader with typed results",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "agent_start" });
        const image = {
          type: "image",
          mimeType: "image/png",
          data: Buffer.alloc(64 * 1024, 0x61).toString("base64"),
        };
        const note = "Read image file [image/png]";
        const content = [{ type: "text", text: note }, image];
        const structured = { count: 2, ready: true, paths: ["one.png", "two.png"] };
        const scriptResult = { content, structuredContent: { threadId: "child-thread" } };
        const cases = [
          {
            toolName: "read",
            result: { content, structuredContent: { ...image, note } },
            expected: { content },
          },
          {
            toolName: "image_generate",
            result: { content, structuredContent: structured },
            expected: { content, structuredContent: structured },
          },
          {
            toolName: "mcp__t3-code__preview_snapshot",
            result: {
              content,
              structuredContent: scriptResult,
              details: { server: "t3-code", tool: "preview_snapshot" },
            },
            expected: { content, structuredContent: { threadId: "child-thread" } },
          },
          {
            toolName: "structured_tool",
            result: { content: [], structuredContent: [false, 2, null, { ready: true }] },
            expected: { content: [], structuredContent: [false, 2, null, { ready: true }] },
          },
          {
            toolName: "mcp__custom__extension",
            result: { content, structuredContent: { content: ["domain content"], ready: true } },
            expected: { content, structuredContent: { content: ["domain content"], ready: true } },
          },
          {
            toolName: "mcp__t3-code__preview_snapshot",
            result: {
              content: [
                { type: "text", text: "first" },
                { type: "text", text: "second" },
              ],
              structuredContent: {
                content: [
                  { type: "text", text: "first" },
                  { type: "text", text: "second" },
                ],
              },
              details: { server: "t3-code", tool: "preview_snapshot" },
            },
            expected: {
              content: [
                { type: "text", text: "first" },
                { type: "text", text: "second" },
              ],
            },
          },
        ];
        for (const [index, test] of cases.entries()) {
          for (const phase of ["update", "end"] as const) {
            yield* fake.emit({
              type: `tool_execution_${phase}`,
              toolCallId: `image-${index}`,
              toolName: test.toolName,
              ...(phase === "end" ? { result: test.result } : { partialResult: test.result }),
              isError: false,
            });
            const event = yield* takeEvent(
              (event) =>
                event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool",
            );
            if (event.type !== "turn_item.updated" || event.turnItem.type !== "dynamic_tool")
              return yield* Effect.die("Expected an image tool item");
            assert.equal(event.turnItem.status, phase === "end" ? "completed" : "running");
            assert.deepEqual(event.turnItem.output, test.expected);
            assert.deepEqual(
              toolOutputImages(event.turnItem.output),
              test.expected.content.some((block) => block.type === "image")
                ? [{ mimeType: "image/png", data: image.data }]
                : [],
            );
            if (index === 0) assert.equal(turnItemOutputText(event.turnItem), note);
            if (index === 5) assert.equal(turnItemOutputText(event.turnItem), "first\nsecond");
            if (index === 2) {
              assert.deepEqual(compactDynamicToolOutput(event.turnItem.output), {
                threadId: "child-thread",
              });
              assert.equal(JSON.stringify(event.turnItem.output).split(image.data).length - 1, 1);
            }
          }
        }
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "keeps large tool text readable, drops identical read mirrors, and bounds structured values",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "agent_start" });
        const text = "hello\nworld\n" + "🙂".repeat(40_000);
        const content = [{ type: "text", text }];
        const cases = [
          { content, structuredContent: text },
          { content, structuredContent: { oversized: "x".repeat(128 * 1024) } },
          { content, structuredContent: Array.from({ length: 10_000 }, () => 1) },
          {
            content,
            structuredContent: Array.from({ length: 40 }).reduce<unknown>(
              (nested) => ({ nested }),
              null,
            ),
          },
        ];
        for (const [index, result] of cases.entries()) {
          yield* fake.emit({
            type: "tool_execution_end",
            toolCallId: `structured-${index}`,
            toolName: "read",
            result,
            isError: true,
          });
          const event = yield* takeEvent(
            (event) => event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool",
          );
          if (event.type !== "turn_item.updated" || event.turnItem.type !== "dynamic_tool")
            return yield* Effect.die("Expected a tool item");
          assert.equal(event.turnItem.status, "failed");
          assert.notProperty(event.turnItem.output, "structuredContent");
          const displayed = turnItemOutputText(event.turnItem);
          if (index === 0) {
            assert.equal(event.turnItem.output, text);
            assert.equal(displayed, text);
          } else {
            assert.equal(
              displayed,
              text + "\nStructured output omitted because it exceeds the stored result limit.",
            );
            assert.deepEqual(Object.keys(event.turnItem.output as object), ["content"]);
          }
        }
        yield* fake.emit({
          type: "tool_execution_end",
          toolCallId: "distinct-metadata",
          toolName: "read",
          result: { content, structuredContent: { path: "read.ts", ready: true } },
        });
        const event = yield* takeEvent(
          (event) => event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool",
        );
        if (event.type !== "turn_item.updated" || event.turnItem.type !== "dynamic_tool")
          return yield* Effect.die("Expected a tool item");
        assert.deepEqual(event.turnItem.output, {
          content,
          structuredContent: { path: "read.ts", ready: true },
        });
        assert.equal(turnItemOutputText(event.turnItem), text);
        const note = "Read image file [image/png]";
        const image = { type: "image", data: "AAAA", mimeType: "image/png" };
        const imageContent = [{ type: "text", text: note }, image];
        for (const structuredContent of [
          { ...image, note, width: 1200 },
          { ...image, note: "A distinct note" },
          { ...image, note, data: "AQID" },
        ]) {
          yield* fake.emit({
            type: "tool_execution_end",
            toolCallId: "distinct-image-metadata",
            toolName: "read",
            result: { content: imageContent, structuredContent },
          });
          const event = yield* takeEvent(
            (event) => event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool",
          );
          if (event.type !== "turn_item.updated" || event.turnItem.type !== "dynamic_tool")
            return yield* Effect.die("Expected a read image tool item");
          assert.deepEqual(event.turnItem.output, { content: imageContent, structuredContent });
          assert.equal(turnItemOutputText(event.turnItem), note);
        }
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "delivers realistic MCP screenshots through the bridge, JSONL transport, and adapter",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "agent_start" });
        for (const size of [3.25 * 1024 * 1024, 10 * 1024 * 1024]) {
          const pixels = Buffer.alloc(size, 0x61);
          const data = pixels.toString("base64");
          const content = [
            { type: "text", text: "Screenshot captured" },
            { type: "image", data, mimeType: "image/png" },
          ];
          const metadata = { screenshot: { width: 1200, height: 800 }, ready: true };
          const bridge = yield* Effect.promise(() =>
            loadMcpBridge({ modern: true, result: { content, structuredContent: metadata } }),
          );
          const tool = bridge.tools.find((tool) => tool.name === "mcp__t3-code__preview_snapshot")!;
          const result = yield* Effect.promise(() => tool.execute("screenshot", {}));
          assert.deepEqual(result.structuredContent, { content, structuredContent: metadata });
          yield* fake.emit(
            {
              type: "tool_execution_end",
              toolCallId: "screenshot",
              toolName: tool.name,
              result,
              isError: false,
            },
            64 * 1024,
          );
          yield* fake.emit({
            type: "tool_execution_end",
            toolCallId: "after-screenshot",
            toolName: "marker",
            result: { content: [{ type: "text", text: "after" }] },
          });
          const event = yield* takeEvent(
            (event) => event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool",
          );
          if (event.type !== "turn_item.updated" || event.turnItem.type !== "dynamic_tool")
            return yield* Effect.die("Expected a screenshot tool item");
          assert.equal(event.turnItem.toolName, tool.name);
          assert.equal(event.turnItem.status, "completed");
          assert.deepEqual(event.turnItem.output, { content, structuredContent: metadata });
          assert.equal(turnItemOutputText(event.turnItem), "Screenshot captured");
          const image = toolOutputImages(event.turnItem.output)[0];
          assert.equal(image?.mimeType, "image/png");
          assert.deepEqual(Buffer.from(image!.data!, "base64"), pixels);
          yield* takeEvent(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "dynamic_tool" &&
              event.turnItem.toolName === "marker",
          );
        }
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("observes official subagent results without inventing child threads", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "tool_execution_update",
        toolCallId: "call_sub",
        toolName: "subagent",
        partialResult: {
          content: [{ type: "text", text: "(running...)" }],
          details: {
            mode: "single",
            results: [
              {
                agent: "scout",
                task: "map the repo",
                exitCode: 0,
                stderr: "",
                sessionFile: "/ignored/custom-extension-session.jsonl",
                messages: [
                  { role: "assistant", content: [{ type: "text", text: "scanning files" }] },
                ],
              },
            ],
          },
        },
      });
      const running = yield* takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.status === "running",
      );
      assert.isTrue(
        running.type === "subagent.updated" &&
          running.subagent.title === "scout" &&
          running.subagent.prompt === "map the repo" &&
          running.subagent.progress === "scanning files" &&
          running.subagent.childThreadId === null,
      );

      yield* fake.emit({
        type: "tool_execution_end",
        toolCallId: "call_sub",
        toolName: "subagent",
        isError: false,
        result: {
          content: [{ type: "text", text: "done" }],
          details: {
            mode: "single",
            results: [
              {
                agent: "scout",
                task: "map the repo",
                exitCode: 0,
                stopReason: "stop",
                stderr: "",
                messages: [
                  { role: "assistant", content: [{ type: "text", text: "repo has one file" }] },
                ],
              },
            ],
          },
        },
      });
      const doneCard = yield* takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.status === "completed",
      );
      assert.isTrue(
        doneCard.type === "subagent.updated" &&
          doneCard.subagent.result === "repo has one file" &&
          doneCard.subagent.childThreadId === null,
      );
      const subagentItem = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "subagent" &&
          event.turnItem.status === "completed",
      );
      assert.isTrue(
        subagentItem.type === "turn_item.updated" &&
          subagentItem.turnItem.type === "subagent" &&
          subagentItem.turnItem.childThreadId === null,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("persists edit patches and write content on file change items", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      const patch = "--- a.ts\n+++ a.ts\n@@ -1 +1 @@\n-old\n+new\n";
      yield* fake.emit({
        type: "tool_execution_start",
        toolCallId: "call_edit",
        toolName: "edit",
        args: { path: "a.ts", edits: [{ oldText: "old", newText: "new" }] },
      });
      yield* fake.emit({
        type: "tool_execution_end",
        toolCallId: "call_edit",
        toolName: "edit",
        isError: false,
        result: {
          content: [{ type: "text", text: "Successfully replaced 1 block(s) in a.ts." }],
          details: { diff: "-1 old\n+1 new", patch, firstChangedLine: 1 },
        },
      });
      const edit = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "file_change" &&
          event.turnItem.status === "completed",
      );
      assert.isTrue(
        edit.type === "turn_item.updated" &&
          edit.turnItem.type === "file_change" &&
          edit.turnItem.fileName === "a.ts" &&
          edit.turnItem.diffStr === patch,
      );

      yield* fake.emit({
        type: "tool_execution_start",
        toolCallId: "call_write",
        toolName: "write",
        args: { path: "b.ts", content: "export {};\n" },
      });
      yield* fake.emit({
        type: "tool_execution_end",
        toolCallId: "call_write",
        toolName: "write",
        isError: false,
        result: { content: [{ type: "text", text: "Successfully wrote to b.ts" }] },
      });
      const write = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "file_change" &&
          event.turnItem.status === "completed",
      );
      assert.isTrue(
        write.type === "turn_item.updated" &&
          write.turnItem.type === "file_change" &&
          write.turnItem.fileName === "b.ts" &&
          write.turnItem.newStr === "export {};\n",
      );

      yield* fake.emit({
        type: "tool_execution_start",
        toolCallId: "call_edit_failed",
        toolName: "edit",
        args: { path: "c.ts", edits: [{ oldText: "missing", newText: "new" }] },
      });
      yield* fake.emit({
        type: "tool_execution_end",
        toolCallId: "call_edit_failed",
        toolName: "edit",
        isError: true,
        result: { content: [{ type: "text", text: "Could not find the text in c.ts." }] },
      });
      const failedEdit = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "file_change" &&
          event.turnItem.status === "failed",
      );
      assert.isTrue(
        failedEdit.type === "turn_item.updated" &&
          failedEdit.turnItem.type === "file_change" &&
          failedEdit.turnItem.diffStr === "Could not find the text in c.ts.",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "sends no get_entries when registering a thread or settling turns, so each turn keeps its synthetic ref",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        for (const runOrdinal of [1, 2]) {
          yield* startTurn(
            runtime,
            providerThread,
            "default",
            [],
            `prompt ${runOrdinal}`,
            undefined,
            runOrdinal,
          );
          yield* fake.takeRequest("prompt");
          yield* fake.emit({ type: "response", command: "prompt", success: true });
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit(agentEnd());
          const turn = yield* takeEvent(
            (event) =>
              event.type === "provider_turn.updated" && event.providerTurn.status === "completed",
          );
          const thread = yield* takeEvent(
            (event) =>
              event.type === "provider_thread.updated" && event.providerThread.status === "idle",
          );
          yield* takeEvent((event) => event.type === "turn.terminal");
          assert.equal(
            turn.type === "provider_turn.updated"
              ? turn.providerTurn.nativeTurnRef?.strength
              : null,
            "weak",
          );
          assert.isTrue(
            thread.type === "provider_thread.updated" &&
              thread.providerThread.nativeConversationHeadRef === null &&
              thread.retainedNativeTurnIds === undefined,
          );
        }
        assert.notInclude(fake.commandLog(), "get_entries");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "does not resend a failed set_session_name until the thread title changes, and the turns still complete",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        // Prime's answer on a private daemon socket, observed while recording.
        fake.failCommand("set_session_name", "Daemon catalog exited during startup (1)");
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        const runTurn = Effect.fnUntraced(function* (runOrdinal: number, title: string) {
          yield* startTurn(
            runtime,
            providerThread,
            "default",
            [],
            "Hello prime-agent",
            undefined,
            runOrdinal,
            THREAD_ID,
            false,
            title,
          );
          yield* fake.takeRequest("prompt");
          yield* fake.emit({ type: "response", command: "prompt", success: true });
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit(agentEnd());
          const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
        });
        yield* runTurn(1, "First title");
        yield* runTurn(2, "First title");
        yield* runTurn(3, "Renamed title");
        yield* runTurn(4, "Renamed title");
        assert.deepEqual(
          fake
            .allRequests()
            .filter((request) => request["type"] === "set_session_name")
            .map((request) => request["name"]),
          ["First title", "Renamed title"],
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("rolls back past turns that left nothing in the session tree", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const turn = (ordinal: number, entryId: string | null): OrchestrationV2ProviderTurn => ({
        id: ProviderTurnId.make(`turn-${ordinal}`),
        providerThreadId: providerThread.id,
        nodeId: NodeId.make(`node-${ordinal}`),
        runAttemptId: null,
        nativeTurnRef:
          entryId === null
            ? null
            : { driver: PRIME_PROVIDER, nativeId: entryId, strength: "strong" },
        ordinal,
        status: "completed",
        startedAt: null,
        completedAt: null,
      });
      const rollBackToFirst = (turns: ReadonlyArray<OrchestrationV2ProviderTurn>) =>
        runtime.rollbackThread({
          providerThread,
          target: {
            type: "provider_turn",
            checkpointId: CheckpointId.make("checkpoint-1"),
            appRunOrdinal: 1,
            providerTurn: turns[0]!,
          },
          providerThreadTurns: turns,
        });
      const forkedEntries = () =>
        fake
          .allRequests()
          .filter((request) => request.type === "fork")
          .map((request) => request.entryId);

      yield* rollBackToFirst([turn(1, "u1"), turn(2, null)]);
      assert.deepEqual(forkedEntries(), []);
      yield* rollBackToFirst([turn(1, "u1"), turn(2, null), turn(3, "u3")]);
      assert.deepEqual(forkedEntries(), ["u3"]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("settles a command-only prompt from its deferred ack and idle probe", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread, "default", [], "/command-only");
      yield* fake.takeRequest("prompt");
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-cmd",
        method: "notify",
        message: "done",
        notifyType: "info",
      });
      yield* fake.emit({ type: "response", command: "prompt", success: true });
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("leaves /compacted as an ordinary prompt", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread, "default", [], "/compacted please");
      const prompt = yield* fake.takeRequest("prompt");
      assert.equal(prompt["message"], "/compacted please");
      assert.isFalse(fake.allRequests().some((request) => request["type"] === "compact"));
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("fails a compact that never started", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread, "default", [], "/compact");
      yield* fake.takeRequest("compact");
      yield* fake.emit({
        type: "response",
        command: "compact",
        success: false,
        error: "Nothing to compact (session too small)",
      });
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(
        terminal.type === "turn.terminal" &&
          terminal.status === "failed" &&
          terminal.failure.message === "Nothing to compact (session too small)",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("restarts Prime when Stop interrupts a user compact", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread, "default", [], "/compact");
      yield* fake.takeRequest("compact");
      const running = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      const providerTurnId =
        running.type === "provider_turn.updated" ? running.providerTurn.id : undefined;
      assert.isDefined(providerTurnId);
      yield* fake.emit({ type: "compaction_start", reason: "manual" });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      yield* runtime.interruptTurn({ providerThread, providerTurnId: providerTurnId! });
      assert.isFalse(fake.allRequests().some((request) => request["type"] === "abort"));
      // Prime has no session-tree listing to read before the restart.
      assert.notInclude(fake.commandLog(), "get_entries");
      yield* fake.closeStdout;
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "interrupted");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("steers /compact as RPC compact instead of a prompt", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const running = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      const providerTurnId =
        running.type === "provider_turn.updated" ? running.providerTurn.id : undefined;
      yield* fake.emit({ type: "agent_start" });
      yield* runtime.steerTurn({
        threadId: THREAD_ID,
        runId: RunId.make("run:thread-prime-test:1"),
        providerThread,
        providerTurnId: providerTurnId!,
        message: {
          messageId: "message:thread-prime-test:steer-compact" as never,
          text: "/compact keep the tests",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
      });
      const compact = yield* fake.takeRequest("compact");
      assert.equal(compact["customInstructions"], "keep the tests");
      assert.isUndefined(compact["streamingBehavior"]);
      yield* fake.emit({ type: "compaction_start", reason: "manual" });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      fake.queueState({ isStreaming: false, isCompacting: false });
      yield* fake.emit({
        type: "compaction_end",
        reason: "manual",
        result: { summary: "smaller", tokensBefore: 10_000, estimatedTokensAfter: 2_000 },
        aborted: false,
        willRetry: false,
      });
      yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "compaction" &&
          event.turnItem.status === "completed",
      );
      yield* fake.emit({ type: "response", command: "compact", success: true });
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("persists current xAI capacity text for the thread error banner", () =>
    expectModelFailure("The model is currently at capacity due to high demand."),
  );

  it.effect("persists extension-normalized xAI capacity text for the thread error banner", () =>
    expectModelFailure(
      "Provider overloaded: The model is currently at capacity due to high demand.",
    ),
  );

  it.effect.each([
    { toolName: "bash", exitCode: 0, legacy: false },
    { toolName: "powershell", exitCode: 2, legacy: false },
    { toolName: "bash", exitCode: 1, legacy: true },
    { toolName: "powershell", exitCode: 0, legacy: true },
  ])(
    "normalizes $toolName command results, exit=$exitCode legacy=$legacy",
    ({ toolName, exitCode, legacy }) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({
          type: "tool_execution_start",
          toolCallId: "shell-call",
          toolName,
          args: { command: "echo hello" },
        });
        const running = yield* takeEvent(
          (event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "command_execution",
        );
        assert.isTrue(
          running.type === "turn_item.updated" &&
            running.turnItem.type === "command_execution" &&
            running.turnItem.status === "running" &&
            running.turnItem.input === "echo hello",
        );
        yield* fake.emit({
          type: "tool_execution_end",
          toolCallId: "shell-call",
          toolName,
          isError: exitCode !== 0,
          result: {
            content: [{ type: "text", text: "hello" }],
            details: { exitCode: legacy ? exitCode : 99 },
            ...(legacy ? {} : { structuredContent: { exit_code: exitCode } }),
          },
        });
        const completed = yield* takeEvent(
          (event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "command_execution",
        );
        assert.isTrue(
          completed.type === "turn_item.updated" &&
            completed.turnItem.type === "command_execution" &&
            completed.turnItem.title === toolName &&
            completed.turnItem.input === "echo hello" &&
            completed.turnItem.output === "hello" &&
            completed.turnItem.exitCode === exitCode &&
            completed.turnItem.status === (exitCode === 0 ? "completed" : "failed"),
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("emits session-start dialogs before a turn exists", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-trust",
        method: "confirm",
        title: "Run project extensions?",
        message: "This project has .prime/extensions.",
      });
      const pending = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
      );
      const requestId =
        pending.type === "runtime_request.updated" ? pending.runtimeRequest.id : undefined;
      yield* runtime.respondToRuntimeRequest({ requestId: requestId!, decision: "accept" });
      const uiResponse = yield* fake.takeRequest("extension_ui_response");
      assert.equal(uiResponse["id"], "ui-trust");
      assert.equal(uiResponse["confirmed"], true);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("expires native dialogs and rejects stale answers without caching approval", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const dialog = {
        type: "extension_ui_request",
        method: "confirm",
        title: "Allow extension?",
        message: "Approve this operation",
      };
      yield* fake.emit({ ...dialog, id: "timed-dialog", timeout: 1000 });
      const pending = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "approval_request",
      );
      assert.isTrue(
        pending.type === "turn_item.updated" && pending.turnItem.type === "approval_request",
      );
      if (pending.type !== "turn_item.updated" || pending.turnItem.type !== "approval_request")
        return;
      const requestId = pending.turnItem.requestId;
      yield* TestClock.adjust(Duration.seconds(1));
      yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" &&
          event.runtimeRequest.id === requestId &&
          event.runtimeRequest.status === "cancelled",
      );
      assert.equal((yield* fake.takeRequest("extension_ui_response"))["cancelled"], true);
      const late = yield* Effect.result(
        runtime.respondToRuntimeRequest({
          requestId: pending.turnItem.requestId,
          decision: "acceptForSession",
        }),
      );
      assert.equal(late._tag, "Failure");
      yield* fake.emit({ ...dialog, id: "fresh-dialog", timeout: 1000 });
      const fresh = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" &&
          event.runtimeRequest.status === "pending" &&
          event.runtimeRequest.nativeRequestRef?.nativeId === "fresh-dialog",
      );
      if (fresh.type !== "runtime_request.updated") return;
      yield* runtime.respondToRuntimeRequest({
        requestId: fresh.runtimeRequest.id,
        decision: "accept",
      });
      assert.equal((yield* fake.takeRequest("extension_ui_response"))["id"], "fresh-dialog");
      yield* fake.emit({ ...dialog, id: "zero-timeout", timeout: 0 });
      const zero = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" &&
          event.runtimeRequest.status === "pending" &&
          event.runtimeRequest.nativeRequestRef?.nativeId === "zero-timeout",
      );
      yield* TestClock.adjust(Duration.seconds(60));
      if (zero.type !== "runtime_request.updated") return;
      yield* runtime.respondToRuntimeRequest({
        requestId: zero.runtimeRequest.id,
        decision: "decline",
      });
      const response = yield* fake.takeRequest("extension_ui_response");
      assert.equal(response["id"], "zero-timeout");
      assert.equal(response["confirmed"], false);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("cancels startup dialogs when Prime exits before a turn exists", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({
        type: "extension_ui_request",
        id: "startup",
        method: "confirm",
        title: "Trust project?",
      });
      const pending = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
      );
      if (pending.type !== "runtime_request.updated") return;
      yield* fake.closeStdout;
      const cancelled = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" &&
          event.runtimeRequest.id === pending.runtimeRequest.id &&
          event.runtimeRequest.status === "cancelled",
      );
      assert.isTrue(cancelled.type === "runtime_request.updated");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("remembers session approvals only for identical confirmation content", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-trust",
        method: "confirm",
        title: "Run project extensions?",
        message: "This project has .prime/extensions.",
      });
      const pending = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
      );
      const requestId =
        pending.type === "runtime_request.updated" ? pending.runtimeRequest.id : undefined;
      yield* runtime.respondToRuntimeRequest({
        requestId: requestId!,
        decision: "acceptForSession",
      });
      const uiResponse = yield* fake.takeRequest("extension_ui_response");
      assert.equal(uiResponse["id"], "ui-trust");
      assert.equal(uiResponse["confirmed"], true);
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-trust-again",
        method: "confirm",
        title: "Run project extensions?",
        message: "This project has .prime/extensions.",
      });
      assert.equal((yield* fake.takeRequest("extension_ui_response"))["id"], "ui-trust-again");
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-other",
        method: "confirm",
        title: "Run project extensions?",
        message: "A different project.",
      });
      const other = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
      );
      assert.isTrue(
        other.type === "runtime_request.updated" &&
          other.runtimeRequest.nativeRequestRef?.nativeId === "ui-other",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("offers an explicit empty value for extension input dialogs", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-input",
        method: "input",
        title: "Optional value",
      });
      const event = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "user_input_request",
      );
      assert.isTrue(
        event.type === "turn_item.updated" && event.turnItem.type === "user_input_request",
      );
      if (event.type !== "turn_item.updated" || event.turnItem.type !== "user_input_request")
        return;
      assert.equal(event.turnItem.questions[0]?.options[0]?.value, "");
      yield* runtime.respondToRuntimeRequest({
        requestId: event.turnItem.requestId,
        answers: { "ui-input": "" },
      });
      const response = yield* fake.takeRequest("extension_ui_response");
      assert.equal(response["value"], "");
      assert.isUndefined(response["cancelled"]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps the complete editor prefill editable and returns exact edited text", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const prefill = `  fix(server): retain full editor content\n\n${"detail\n".repeat(500)}  `;
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-editor",
        method: "editor",
        title: "Commit message",
        prefill,
      });
      const event = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "user_input_request",
      );
      if (event.type !== "turn_item.updated" || event.turnItem.type !== "user_input_request")
        return;
      assert.equal(event.turnItem.questions[0]?.initialAnswer, prefill);
      assert.deepEqual(event.turnItem.questions[0]?.options, []);
      assert.equal(event.turnItem.questions[0]?.question, "Commit message");
      const edited = "  fix(server): edited commit message\n\n";
      yield* runtime.respondToRuntimeRequest({
        requestId: event.turnItem.requestId,
        answers: { "ui-editor": edited },
      });
      const response = yield* fake.takeRequest("extension_ui_response");
      assert.equal(response["value"], edited);
      assert.isUndefined(response["cancelled"]);
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-editor-cleared",
        method: "editor",
        title: "Commit message",
        prefill,
      });
      const cleared = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "user_input_request" &&
          event.turnItem.questions.some((question) => question.id === "ui-editor-cleared"),
      );
      if (cleared.type !== "turn_item.updated" || cleared.turnItem.type !== "user_input_request")
        return;
      yield* runtime.respondToRuntimeRequest({
        requestId: cleared.turnItem.requestId,
        answers: { "ui-editor-cleared": "" },
      });
      const emptyResponse = yield* fake.takeRequest("extension_ui_response");
      assert.equal(emptyResponse["value"], "");
      assert.isUndefined(emptyResponse["cancelled"]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("raises bridge edit confirmations as file-change approvals", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      for (const [id, title, requestKind] of [
        ["ui-edit", "Allow edit?", "file-change"],
        ["ui-bash", "Allow bash?", "command"],
        ["ui-ext", "Deploy to staging?", "command"],
      ] as const) {
        yield* fake.emit({ type: "extension_ui_request", id, method: "confirm", title });
        const item = yield* takeEvent(
          (event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "approval_request",
        );
        assert.isTrue(
          item.type === "turn_item.updated" &&
            item.turnItem.type === "approval_request" &&
            item.turnItem.requestKind === requestKind,
          `${title} should be ${requestKind}`,
        );
      }
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("reads a thread snapshot from Prime's active branch", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      fake.queueMessages({
        messages: [
          {
            role: "user",
            content: "hello prime-agent",
            timestamp: 1700000000000,
          },
          {
            role: "assistant",
            content: [{ type: "text", text: "hello back" }],
            timestamp: 1700000001000,
          },
          { role: "toolResult", content: [] },
        ],
      });
      const snapshot = yield* runtime.readThreadSnapshot({ providerThread });
      assert.equal(snapshot.messages.length, 2);
      assert.equal(snapshot.messages[0]!.role, "user");
      assert.equal(snapshot.messages[0]!.text, "hello prime-agent");
      assert.equal(snapshot.messages[1]!.role, "assistant");
      assert.equal(snapshot.messages[1]!.text, "hello back");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps snapshot message identities distinct across native sessions", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime } = yield* openRuntime(fake);
      const first = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const messages = {
        messages: [{ role: "user", content: "same text", timestamp: 1700000000000 }],
      };
      fake.queueMessages(messages);
      const a = yield* runtime.readThreadSnapshot({ providerThread: first });
      fake.queueState({ sessionFile: "/fake/another-session.jsonl" });
      const second = yield* runtime.ensureThread({
        threadId: ThreadId.make("second-thread"),
        modelSelection: modelSelection("default"),
        runtimePolicy,
        existingProviderThread: {
          ...first,
          nativeThreadRef: {
            driver: PRIME_PROVIDER,
            nativeId: "/fake/another-session.jsonl",
            strength: "strong",
          },
        },
      });
      fake.queueMessages(messages);
      const b = yield* runtime.readThreadSnapshot({ providerThread: second });
      assert.notEqual(a.messages[0]!.id, b.messages[0]!.id);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("rejects a nonpersistent session UUID instead of treating it as a resumable path", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime } = yield* openRuntime(fake);
      fake.queueState({ sessionFile: undefined });
      const result = yield* runtime
        .ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        })
        .pipe(Effect.result);
      assert.equal(result._tag, "Failure");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps a settled turn's late prompt rejection off the next turn", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread, "default", [], "/my-command");
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "agent_end", messages: [] });
      const firstTerminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(firstTerminal.type === "turn.terminal" && firstTerminal.status === "completed");

      yield* startTurn(runtime, providerThread, "default", [], "Second turn", undefined, 2);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({
        type: "response",
        command: "prompt",
        success: false,
        error: "late command rejection",
      });
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "agent_end", messages: [] });
      const secondTerminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(
        secondTerminal.type === "turn.terminal" && secondTerminal.status === "completed",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows compaction progress and completes the same activity row", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "compaction_start", reason: "threshold" });

      const runningNode = yield* takeEvent(
        (event) => event.type === "node.updated" && event.node.kind === "system",
      );
      const runningItem = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      assert.isTrue(
        runningNode.type === "node.updated" &&
          runningNode.node.status === "running" &&
          runningItem.type === "turn_item.updated" &&
          runningItem.turnItem.type === "compaction" &&
          runningItem.turnItem.status === "running" &&
          runningItem.turnItem.title === "Compacting context...",
      );

      yield* fake.emit({
        type: "compaction_end",
        reason: "threshold",
        result: { summary: "smaller", tokensBefore: 200_000, estimatedTokensAfter: 3_400 },
        aborted: false,
        willRetry: false,
      });
      const completedNode = yield* takeEvent(
        (event) => event.type === "node.updated" && event.node.kind === "system",
      );
      const completedItem = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      assert.isTrue(
        runningNode.type === "node.updated" &&
          completedNode.type === "node.updated" &&
          runningItem.type === "turn_item.updated" &&
          runningItem.turnItem.type === "compaction" &&
          completedItem.type === "turn_item.updated" &&
          completedItem.turnItem.type === "compaction" &&
          completedNode.node.id === runningNode.node.id &&
          completedNode.node.status === "completed" &&
          completedItem.turnItem.id === runningItem.turnItem.id &&
          completedItem.turnItem.ordinal === runningItem.turnItem.ordinal &&
          completedItem.turnItem.startedAt === runningItem.turnItem.startedAt &&
          completedItem.turnItem.status === "completed" &&
          completedItem.turnItem.title === "Context compacted" &&
          completedItem.turnItem.beforeTokenCount === 200_000 &&
          completedItem.turnItem.afterTokenCount === 3_400,
      );

      yield* fake.emit({ type: "agent_end", messages: [] });
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("uses distinct compaction IDs for first turns in separate threads", () =>
    Effect.gen(function* () {
      const firstFake = yield* makeFakePrime;
      const { runtime: firstRuntime, takeEvent: takeFirstEvent } = yield* openRuntime(firstFake);
      const firstProviderThread = yield* firstRuntime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(firstRuntime, firstProviderThread);
      yield* firstFake.takeRequest("prompt");
      yield* firstFake.emit({ type: "agent_start" });
      yield* firstFake.emit({ type: "compaction_start", reason: "threshold" });
      const first = yield* takeFirstEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );

      const secondThreadId = ThreadId.make("thread-prime-test-second");
      const secondFake = yield* makeFakePrime;
      const { runtime: secondRuntime, takeEvent: takeSecondEvent } = yield* openRuntime(
        secondFake,
        "default",
        secondThreadId,
        ProviderSessionId.make("provider-session-prime-test-second"),
      );
      const secondProviderThread = yield* secondRuntime.ensureThread({
        threadId: secondThreadId,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(
        secondRuntime,
        secondProviderThread,
        "default",
        [],
        "Hello from another thread",
        undefined,
        1,
        secondThreadId,
      );
      yield* secondFake.takeRequest("prompt");
      yield* secondFake.emit({ type: "agent_start" });
      yield* secondFake.emit({ type: "compaction_start", reason: "threshold" });
      const second = yield* takeSecondEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );

      assert.isTrue(
        first.type === "turn_item.updated" &&
          first.turnItem.type === "compaction" &&
          second.type === "turn_item.updated" &&
          second.turnItem.type === "compaction" &&
          first.turnItem.ordinal === second.turnItem.ordinal &&
          first.turnItem.id !== second.turnItem.id,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows aborted compactions as stopped", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "compaction_start", reason: "manual" });
      const running = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      yield* fake.emit({
        type: "compaction_end",
        reason: "manual",
        aborted: true,
        willRetry: false,
      });
      const stopped = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      assert.isTrue(
        running.type === "turn_item.updated" &&
          running.turnItem.type === "compaction" &&
          stopped.type === "turn_item.updated" &&
          stopped.turnItem.type === "compaction" &&
          stopped.turnItem.id === running.turnItem.id &&
          stopped.turnItem.status === "cancelled" &&
          stopped.turnItem.title === "Context compaction stopped",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps the turn open and updates one retry row through final failure", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");

      yield* fake.setSessionActions(RUNNING_TURN_ACTIONS);
      yield* fake.emit({ type: "agent_start" });
      // Prime retries inside the same turn action, so agent_end does not settle.
      yield* fake.emit(agentEnd());
      yield* fake.emit({
        type: "auto_retry_start",
        attempt: 1,
        maxAttempts: 3,
        delayMs: 3_000,
        errorMessage: "529 overloaded",
      });
      const firstRetry = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      assert.isTrue(
        firstRetry.type === "turn_item.updated" &&
          firstRetry.turnItem.type === "error" &&
          firstRetry.turnItem.status === "running" &&
          firstRetry.turnItem.title === "Provider retry" &&
          firstRetry.turnItem.failure.retryable === true &&
          firstRetry.turnItem.retry?.attempt === 1 &&
          firstRetry.turnItem.retry.maxAttempts === 3 &&
          firstRetry.turnItem.retry.retryDelayMs === 3_000,
      );

      yield* fake.emit({
        type: "auto_retry_start",
        attempt: 3,
        maxAttempts: 3,
        delayMs: 12_000,
        errorMessage: "529 still overloaded",
      });
      const lastRetry = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      assert.isTrue(
        firstRetry.type === "turn_item.updated" &&
          firstRetry.turnItem.type === "error" &&
          lastRetry.type === "turn_item.updated" &&
          lastRetry.turnItem.type === "error" &&
          lastRetry.turnItem.id === firstRetry.turnItem.id &&
          lastRetry.turnItem.startedAt === firstRetry.turnItem.startedAt &&
          lastRetry.turnItem.retry?.attempt === 3,
      );

      yield* fake.emit({
        type: "auto_retry_end",
        success: false,
        attempt: 3,
        finalError: "529 overloaded",
      });
      const failedRetry = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "error" &&
          event.turnItem.status === "failed",
      );
      assert.isTrue(
        firstRetry.type === "turn_item.updated" &&
          firstRetry.turnItem.type === "error" &&
          failedRetry.type === "turn_item.updated" &&
          failedRetry.turnItem.type === "error" &&
          failedRetry.turnItem.id === firstRetry.turnItem.id &&
          failedRetry.turnItem.title === "Provider error" &&
          failedRetry.turnItem.failure.retryable === false &&
          failedRetry.turnItem.retry?.attempt === 3 &&
          failedRetry.turnItem.retry.maxAttempts === 3,
      );
      yield* fake.setSessionActions(IDLE_ACTIONS);

      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "failed");
      assert.isTrue(
        firstRetry.type === "turn_item.updated" &&
          firstRetry.turnItem.type === "error" &&
          terminal.type === "turn.terminal" &&
          terminal.status === "failed" &&
          terminal.failure.message.includes("overloaded") &&
          terminal.retry?.attempt === 3 &&
          terminal.retry.maxAttempts === 3 &&
          terminal.retryStartedAt === firstRetry.turnItem.startedAt,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("preserves exhausted retry failure through non-retrying compaction", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "auto_retry_start",
        attempt: 5,
        maxAttempts: 5,
        delayMs: 48_000,
        errorMessage: "socket timed out",
      });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      yield* fake.emit({
        type: "auto_retry_end",
        success: false,
        attempt: 5,
        finalError: "socket timed out",
      });
      yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "error" &&
          event.turnItem.status === "failed",
      );
      yield* fake.emit({ type: "compaction_start", reason: "threshold" });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      yield* fake.emit({
        type: "compaction_end",
        reason: "threshold",
        result: { summary: "smaller", tokensBefore: 200_000, estimatedTokensAfter: 3_400 },
        aborted: false,
        willRetry: false,
      });
      yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "compaction" &&
          event.turnItem.status === "completed",
      );
      yield* fake.emit({ type: "agent_end", messages: [] });

      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(
        terminal.type === "turn.terminal" &&
          terminal.status === "failed" &&
          terminal.failure.message === "socket timed out" &&
          terminal.retry?.attempt === 5 &&
          terminal.retry.maxAttempts === 5,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("marks retry progress recovered when Prime succeeds", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "message_end",
        message: {
          role: "assistant",
          content: [],
          stopReason: "error",
          errorMessage: "temporary network failure",
        },
      });
      yield* fake.emit({
        type: "auto_retry_start",
        attempt: 1,
        maxAttempts: 5,
        delayMs: 3_000,
        errorMessage: "temporary network failure",
      });
      const running = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      yield* fake.emit({ type: "auto_retry_end", success: true, attempt: 1 });
      const recovered = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      assert.isTrue(
        running.type === "turn_item.updated" &&
          running.turnItem.type === "error" &&
          recovered.type === "turn_item.updated" &&
          recovered.turnItem.type === "error" &&
          recovered.turnItem.id === running.turnItem.id &&
          recovered.turnItem.status === "completed" &&
          recovered.turnItem.title === "Provider recovered",
      );

      yield* fake.emit({ type: "agent_end", messages: [] });
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([undefined, false, true])(
    "preserves the native settlement outcome, aborted=%s",
    (aborted) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit(agentEnd(aborted === true));
        const completed = yield* takeEvent(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.status !== "running",
        );
        const status = aborted === true ? "interrupted" : "completed";
        assert.isTrue(
          completed.type === "provider_turn.updated" && completed.providerTurn.status === status,
        );
        const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
        assert.isTrue(
          terminal.type === "turn.terminal" &&
            terminal.status === status &&
            terminal.failure === null,
        );
        assert.equal(runtime.providerSession.status, "ready");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("stops retry progress when Prime aborts the native run", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "auto_retry_start",
        attempt: 2,
        maxAttempts: 5,
        delayMs: 6_000,
        errorMessage: "temporary network failure",
      });
      const retrying = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      yield* fake.emit(agentEnd(true));
      const stopped = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      assert.isTrue(
        retrying.type === "turn_item.updated" &&
          stopped.type === "turn_item.updated" &&
          stopped.turnItem.id === retrying.turnItem.id &&
          stopped.turnItem.status === "interrupted",
      );
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "interrupted");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("stops active retry progress when the turn is interrupted", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      const runningTurn = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      const providerTurnId =
        runningTurn.type === "provider_turn.updated" ? runningTurn.providerTurn.id : undefined;
      assert.isDefined(providerTurnId);

      yield* fake.emit({
        type: "auto_retry_start",
        attempt: 2,
        maxAttempts: 5,
        delayMs: 6_000,
        errorMessage: "temporary network failure",
      });
      const retrying = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      yield* runtime.interruptTurn({ providerThread, providerTurnId: providerTurnId! });
      yield* fake.takeRequest("abort");
      yield* fake.emit({ type: "agent_end", messages: [] });

      const stopped = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      assert.isTrue(
        retrying.type === "turn_item.updated" &&
          retrying.turnItem.type === "error" &&
          stopped.type === "turn_item.updated" &&
          stopped.turnItem.type === "error" &&
          stopped.turnItem.id === retrying.turnItem.id &&
          stopped.turnItem.status === "interrupted" &&
          stopped.turnItem.title === "Provider retry stopped",
      );
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "interrupted");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps extension-started compaction and recovery in the settled turn", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });

      fake.queueState({ isStreaming: false, isCompacting: true });
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      yield* fake.emit({ type: "compaction_start", reason: "manual" });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );

      fake.queueState({ isStreaming: true, isCompacting: false });
      yield* fake.emit({
        type: "compaction_end",
        reason: "manual",
        result: { summary: "smaller", tokensBefore: 10_000, estimatedTokensAfter: 2_000 },
        aborted: false,
        willRetry: false,
      });
      yield* fake.emit({ type: "agent_start" });
      yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "compaction" &&
          event.turnItem.status === "completed",
      );
      yield* fake.takeRequest("get_state");

      fake.queueState({ isStreaming: false, isCompacting: false });
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps working after a settle probe fails before detached compaction", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });

      fake.failNextState();
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      yield* fake.emit({ type: "compaction_start", reason: "manual" });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );

      fake.queueState({ isStreaming: false, isCompacting: false });
      yield* fake.emit({
        type: "compaction_end",
        reason: "manual",
        result: { summary: "smaller", tokensBefore: 10_000, estimatedTokensAfter: 2_000 },
        aborted: false,
        willRetry: false,
      });
      yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "compaction" &&
          event.turnItem.status === "completed",
      );
      yield* fake.takeRequest("get_state");
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("restarts Prime when Stop interrupts detached compaction", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const running = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      assert.equal(running.type, "provider_turn.updated");
      const providerTurnId =
        running.type === "provider_turn.updated" ? running.providerTurn.id : undefined;
      assert.isDefined(providerTurnId);
      yield* fake.emit({ type: "agent_start" });

      fake.queueState({ isStreaming: false, isCompacting: true });
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      yield* fake.emit({ type: "compaction_start", reason: "manual" });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );

      yield* runtime.interruptTurn({ providerThread, providerTurnId: providerTurnId! });
      assert.isFalse(fake.allRequests().some((request) => request["type"] === "abort"));
      yield* fake.closeStdout;
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "interrupted");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("ignores an idle snapshot made stale by a steer", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const running = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      assert.equal(running.type, "provider_turn.updated");
      const providerTurnId =
        running.type === "provider_turn.updated" ? running.providerTurn.id : undefined;
      assert.isDefined(providerTurnId);
      yield* fake.emit({ type: "agent_start" });

      fake.deferNextState();
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      yield* runtime.steerTurn({
        threadId: THREAD_ID,
        runId: RunId.make("run:thread-prime-test:1"),
        providerThread,
        providerTurnId: providerTurnId!,
        message: {
          messageId: "message:thread-prime-test:late-steer" as never,
          text: "Continue after settlement",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
      });
      yield* fake.takeRequest("prompt");
      yield* fake.resolveDeferredState(recordedIdleState(FAKE_SESSION_FILE));

      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: { role: "assistant" } });
      yield* fake.emit({
        type: "message_update",
        assistantMessageEvent: { type: "text_end", contentIndex: 0, content: "Recovered" },
      });
      yield* fake.emit({
        type: "message_end",
        message: {
          role: "assistant",
          content: [{ type: "text", text: "Recovered" }],
          stopReason: "stop",
        },
      });
      const assistantItem = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "assistant_message" &&
          event.turnItem.streaming === false,
      );
      assert.isTrue(
        assistantItem.type === "turn_item.updated" &&
          assistantItem.turnItem.type === "assistant_message" &&
          assistantItem.turnItem.text === "Recovered",
      );

      fake.queueState({ isStreaming: false, isCompacting: false });
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});

describe("Prime settle engine", () => {
  type AdapterEvent = ProviderAdapter.ProviderAdapterV2Event;

  const openTurn = Effect.fnUntraced(function* (configure?: (fake: FakePrime) => void) {
    const fake = yield* makeFakePrime;
    configure?.(fake);
    const { runtime, takeEvent } = yield* openRuntime(fake);
    const providerThread = yield* runtime.ensureThread({
      threadId: THREAD_ID,
      modelSelection: modelSelection("default"),
      runtimePolicy,
    });
    const seen: Array<AdapterEvent> = [];
    const record = (predicate: (event: AdapterEvent) => boolean) =>
      takeEvent((event) => {
        seen.push(event);
        return predicate(event);
      });
    const begin = Effect.fnUntraced(function* (text = "Hello prime-agent", runOrdinal = 1) {
      yield* startTurn(runtime, providerThread, "default", [], text, undefined, runOrdinal);
      yield* fake.takeRequest("prompt");
      // Prime acknowledges a prompt on acceptance, before the run's events.
      yield* fake.emit({ type: "response", command: "prompt", success: true });
      return fake.commandLog().length;
    });
    return { fake, runtime, providerThread, seen, record, begin };
  });

  const replyText = (fake: FakePrime, text: string) =>
    Effect.gen(function* () {
      yield* fake.emit({ type: "message_start", message: { role: "assistant" } });
      yield* fake.emit({
        type: "message_update",
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: text },
      });
      yield* fake.emit({
        type: "message_update",
        assistantMessageEvent: { type: "text_end", contentIndex: 0, content: text },
      });
      yield* fake.emit({
        type: "message_end",
        message: { role: "assistant", content: [{ type: "text", text }], stopReason: "stop" },
      });
    });

  const isAssistantText = (text: string) => (event: AdapterEvent) =>
    event.type === "turn_item.updated" &&
    event.turnItem.type === "assistant_message" &&
    event.turnItem.streaming === false &&
    event.turnItem.text === text;

  /** A notify dialog becomes a turn item, so seeing it proves the pump got this far. */
  const pumpBarrier = (
    fake: FakePrime,
    record: (predicate: (event: AdapterEvent) => boolean) => Effect.Effect<AdapterEvent>,
    message: string,
  ) =>
    Effect.gen(function* () {
      yield* fake.emit({ type: "extension_ui_request", method: "notify", message });
      yield* record(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "dynamic_tool" &&
          event.turnItem.toolName === "notify",
      );
    });

  const commandsSince = (fake: FakePrime, index: number) => fake.commandLog().slice(index);
  const probesSince = (fake: FakePrime, index: number) =>
    commandsSince(fake, index).filter((type) => type === "get_state").length;
  const terminals = (seen: ReadonlyArray<AdapterEvent>) =>
    seen.filter((event) => event.type === "turn.terminal");

  it.effect(
    "[VAL-TURN-001] a plain turn settles once on agent_end plus an idle get_state, and get_state is sent only after the trigger",
    () =>
      Effect.gen(function* () {
        const { fake, seen, record, begin } = yield* openTurn();
        const afterPrompt = yield* begin();
        yield* fake.emit({ type: "agent_start" });
        yield* replyText(fake, "Plain reply");
        yield* record(isAssistantText("Plain reply"));
        assert.equal(probesSince(fake, afterPrompt), 0);
        assert.lengthOf(terminals(seen), 0);

        yield* fake.emit(agentEnd());
        const terminal = yield* record((event) => event.type === "turn.terminal");
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
        assert.equal(probesSince(fake, afterPrompt), 1);
        assert.lengthOf(terminals(seen), 1);
        assert.isTrue(seen.findIndex(isAssistantText("Plain reply")) < seen.indexOf(terminal));
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-TURN-001] Prime's id-less unknown-command answer fails the matching request at once, so settling never waits out a request timeout",
    () =>
      Effect.gen(function* () {
        // The clock stays frozen, so only an immediate failure of the
        // settle-time stats read lets the turn finish.
        const { fake, seen, record, begin } = yield* openTurn((fake) =>
          fake.answerAsUnknownCommand("get_session_stats"),
        );
        const afterPrompt = yield* begin();
        yield* fake.emit({ type: "agent_start" });
        yield* replyText(fake, "Plain reply");
        yield* fake.emit(agentEnd());
        const terminal = yield* record((event) => event.type === "turn.terminal");
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
        assert.include(commandsSince(fake, afterPrompt), "get_session_stats");
        assert.lengthOf(terminals(seen), 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each(["turn", "session_command"] as const)(
    "[VAL-TURN-002] agent_end while a %s action is active does not settle until the action clears and the probe is idle",
    (kind) =>
      Effect.gen(function* () {
        const { fake, seen, record, begin } = yield* openTurn();
        const afterPrompt = yield* begin();
        yield* fake.setSessionActions({
          ...IDLE_ACTIONS,
          active: { kind, phase: "running", label: "busy" },
        });
        yield* fake.emit({ type: "agent_start" });
        yield* replyText(fake, "Still working");
        yield* fake.emit(agentEnd());
        yield* pumpBarrier(fake, record, "after agent_end");
        assert.equal(probesSince(fake, afterPrompt), 0);
        assert.lengthOf(terminals(seen), 0);

        yield* fake.setSessionActions(IDLE_ACTIONS);
        const terminal = yield* record((event) => event.type === "turn.terminal");
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
        assert.equal(probesSince(fake, afterPrompt), 1);
        assert.lengthOf(terminals(seen), 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-TURN-003] a probe reporting isStreaming true defers settle to the next idle transition",
    () =>
      Effect.gen(function* () {
        const { fake, seen, record, begin } = yield* openTurn();
        const afterPrompt = yield* begin();
        yield* fake.emit({ type: "agent_start" });
        yield* replyText(fake, "Streaming reply");
        fake.queueState({ isStreaming: true });
        yield* fake.emit(agentEnd());
        yield* fake.takeRequest("get_state");
        yield* pumpBarrier(fake, record, "after streaming probe");

        yield* fake.setSessionActions(RUNNING_TURN_ACTIONS);
        yield* fake.setSessionActions(IDLE_ACTIONS);
        const terminal = yield* record((event) => event.type === "turn.terminal");
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
        assert.equal(probesSince(fake, afterPrompt), 2);
        assert.lengthOf(terminals(seen), 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-TURN-004] compaction during a run blocks settle until compaction_end and an idle probe, and emits a compaction item",
    () =>
      Effect.gen(function* () {
        const { fake, seen, record, begin } = yield* openTurn();
        const afterPrompt = yield* begin();
        yield* fake.emit({ type: "agent_start" });
        yield* replyText(fake, "Before compaction");
        // Prime reports isCompacting before its compaction_start frame arrives.
        fake.setCompacting(true);
        yield* fake.emit(agentEnd());
        yield* fake.takeRequest("get_state");
        yield* fake.emit({ type: "compaction_start", reason: "threshold" });
        const running = yield* record(
          (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
        );
        assert.isTrue(
          running.type === "turn_item.updated" &&
            running.turnItem.type === "compaction" &&
            running.turnItem.status === "running",
        );
        yield* fake.emit({ type: "message_start", message: { role: "assistant" } });
        yield* pumpBarrier(fake, record, "during compaction");
        assert.equal(probesSince(fake, afterPrompt), 1);
        assert.lengthOf(terminals(seen), 0);

        fake.setCompacting(false);
        yield* fake.emit({
          type: "compaction_end",
          reason: "threshold",
          result: { summary: "smaller", tokensBefore: 9_000, estimatedTokensAfter: 1_500 },
          aborted: false,
          willRetry: false,
        });
        const terminal = yield* record((event) => event.type === "turn.terminal");
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
        const completed = seen.find(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "compaction" &&
            event.turnItem.status === "completed",
        );
        assert.isDefined(completed);
        assert.isTrue(seen.indexOf(completed!) < seen.indexOf(terminal));
        assert.equal(probesSince(fake, afterPrompt), 2);
        assert.lengthOf(terminals(seen), 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("[VAL-TURN-005] queued work blocks settling until the queue drains", () =>
    Effect.gen(function* () {
      const { fake, seen, record, begin } = yield* openTurn();
      const afterPrompt = yield* begin();
      yield* fake.emit({ type: "agent_start" });
      yield* replyText(fake, "First answer");
      yield* fake.setSessionActions({
        queuedCount: 2,
        steering: [],
        followUps: ["also check the tests", "and the docs"],
      });
      yield* fake.emit(agentEnd());
      yield* pumpBarrier(fake, record, "queued after agent_end");
      assert.equal(probesSince(fake, afterPrompt), 0);

      yield* fake.setSessionActions({
        queuedCount: 1,
        steering: [],
        followUps: ["and the docs"],
        active: { kind: "turn", phase: "running", label: "also check the tests" },
      });
      yield* fake.emit({ type: "agent_start" });
      yield* replyText(fake, "Follow-up answer");
      yield* fake.emit(agentEnd());
      yield* fake.setSessionActions({
        queuedCount: 1,
        steering: [],
        followUps: ["and the docs"],
      });
      yield* pumpBarrier(fake, record, "one follow-up left");
      assert.equal(probesSince(fake, afterPrompt), 0);
      assert.lengthOf(terminals(seen), 0);

      yield* fake.setSessionActions(IDLE_ACTIONS);
      const terminal = yield* record((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      assert.isTrue(seen.findIndex(isAssistantText("Follow-up answer")) < seen.indexOf(terminal));
      assert.equal(probesSince(fake, afterPrompt), 1);
      assert.lengthOf(terminals(seen), 1);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-TURN-006] agent_start after agent_end re-arms the same turn, a stale probe is ignored, and one terminal follows the second run",
    () =>
      Effect.gen(function* () {
        const { fake, seen, record, begin } = yield* openTurn();
        const afterPrompt = yield* begin();
        yield* fake.emit({ type: "agent_start" });
        yield* replyText(fake, "First run");
        fake.deferNextState();
        yield* fake.emit(agentEnd());
        yield* fake.takeRequest("get_state");

        // A held goal or queued follow-up starts a new run before the probe answers.
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "message_start", message: { role: "assistant" } });
        yield* pumpBarrier(fake, record, "second run started");
        yield* fake.resolveDeferredState(recordedIdleState(FAKE_SESSION_FILE));
        yield* replyText(fake, "Second run");
        yield* record(isAssistantText("Second run"));
        assert.lengthOf(terminals(seen), 0);

        yield* fake.emit(agentEnd());
        const terminal = yield* record((event) => event.type === "turn.terminal");
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
        assert.equal(probesSince(fake, afterPrompt), 2);
        assert.lengthOf(terminals(seen), 1);
        const firstRun = seen.find(isAssistantText("First run"));
        const secondRun = seen.find(isAssistantText("Second run"));
        assert.isTrue(
          terminal.type === "turn.terminal" &&
            firstRun?.type === "turn_item.updated" &&
            secondRun?.type === "turn_item.updated" &&
            firstRun.turnItem.providerTurnId === secondRun.turnItem.providerTurnId &&
            terminal.providerTurnId === secondRun.turnItem.providerTurnId,
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    { command: "prompt", error: "Provider rejected the request: quota exceeded" },
    { command: "frobnicate", error: "Unknown command: frobnicate" },
  ])(
    "[VAL-TURN-011] an id-less $command error response fails the open turn with its text",
    ({ command, error }) =>
      Effect.gen(function* () {
        const { fake, runtime, providerThread, seen, record, begin } = yield* openTurn();
        yield* begin();
        yield* fake.emit({ type: "agent_start" });
        yield* replyText(fake, "Partial answer");
        // The failed run's agent_end can arrive first; its idle probe must
        // not complete a turn that the error has already failed.
        fake.deferNextState();
        yield* fake.emit(agentEnd());
        yield* fake.takeRequest("get_state");
        yield* fake.emitOrphanError(command, error);
        const failed = yield* record((event) => event.type === "turn.terminal");
        assert.isTrue(
          failed.type === "turn.terminal" &&
            failed.status === "failed" &&
            failed.failure.message === error,
        );
        assert.equal(runtime.providerSession.lastError, error);
        yield* fake.resolveDeferredState(recordedIdleState(FAKE_SESSION_FILE));

        // A late agent_end from the failed run lands ahead of the next prompt's ack.
        yield* fake.emit(agentEnd());
        const afterSecondPrompt = yield* begin("Try again", 2);
        yield* fake.emit({ type: "agent_start" });
        yield* replyText(fake, "Recovered");
        yield* fake.emit(agentEnd());
        const completed = yield* record((event) => event.type === "turn.terminal");
        assert.isTrue(completed.type === "turn.terminal" && completed.status === "completed");
        assert.isTrue(seen.findIndex(isAssistantText("Recovered")) < seen.indexOf(completed));
        assert.lengthOf(terminals(seen), 2);
        assert.equal(probesSince(fake, afterSecondPrompt), 1);
        assert.isTrue(
          failed.type === "turn.terminal" &&
            completed.type === "turn.terminal" &&
            failed.providerTurnId !== completed.providerTurnId,
        );
        assert.equal(providerThread.status, "idle");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-TURN-013] two sequential turns on one session each settle once with their own text",
    () =>
      Effect.gen(function* () {
        const { fake, seen, record, begin } = yield* openTurn();
        const afterFirstPrompt = yield* begin("First question", 1);
        yield* fake.setSessionActions(RUNNING_TURN_ACTIONS);
        yield* fake.emit({ type: "agent_start" });
        yield* replyText(fake, "First answer");
        yield* fake.emit(agentEnd());
        yield* fake.setSessionActions(IDLE_ACTIONS);
        const first = yield* record((event) => event.type === "turn.terminal");

        const afterSecondPrompt = yield* begin("Second question", 2);
        assert.equal(probesSince(fake, afterFirstPrompt) - probesSince(fake, afterSecondPrompt), 1);
        yield* fake.setSessionActions(RUNNING_TURN_ACTIONS);
        yield* fake.emit({ type: "agent_start" });
        yield* replyText(fake, "Second answer");
        yield* fake.emit(agentEnd());
        yield* fake.setSessionActions(IDLE_ACTIONS);
        const second = yield* record((event) => event.type === "turn.terminal");

        assert.lengthOf(terminals(seen), 2);
        assert.equal(probesSince(fake, afterSecondPrompt), 1);
        const firstText = seen.find(isAssistantText("First answer"));
        const secondText = seen.find(isAssistantText("Second answer"));
        assert.isTrue(
          first.type === "turn.terminal" &&
            second.type === "turn.terminal" &&
            first.status === "completed" &&
            second.status === "completed" &&
            firstText?.type === "turn_item.updated" &&
            secondText?.type === "turn_item.updated" &&
            firstText.turnItem.providerTurnId === first.providerTurnId &&
            secondText.turnItem.providerTurnId === second.providerTurnId &&
            first.providerTurnId !== second.providerTurnId,
        );
        assert.isTrue(seen.indexOf(first) < seen.indexOf(secondText!));
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
  describe("turn control paths", () => {
    const FAIL_ME_ERROR =
      'Mistral API error (400): {"object":"error","message":"Invalid request payload: Expected at most one leading ThinkChunk, got 2","type":"invalid_request_invalid_args","code":"3051"}';
    const failedAssistant = (errorMessage: string) => ({
      role: "assistant",
      id: "mock-failed-asst",
      content: [],
      stopReason: "error",
      errorMessage,
    });

    const runningTurnId = (
      record: (predicate: (event: AdapterEvent) => boolean) => Effect.Effect<AdapterEvent>,
    ) =>
      Effect.gen(function* () {
        const running = yield* record(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.status === "running",
        );
        assert.isTrue(running.type === "provider_turn.updated");
        return (running as Extract<AdapterEvent, { type: "provider_turn.updated" }>).providerTurn
          .id;
      });

    const steerMessage = (text: string) => ({
      messageId: `message:thread-prime-test:steer:${text}` as never,
      text,
      attachments: [],
      createdBy: "user" as const,
      creationSource: "web" as const,
    });

    const promptsSince = (fake: FakePrime, index: number) =>
      fake
        .allRequests()
        .slice(index)
        .filter((request) => request["type"] === "prompt");

    const finalToolStatus = (seen: ReadonlyArray<AdapterEvent>, toolCallId: string) =>
      seen
        .flatMap((event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.nativeItemRef?.nativeId === toolCallId
            ? [event.turnItem.status]
            : [],
        )
        .at(-1);

    it.effect(
      "[VAL-TURN-007] STEER_ME: input during an open turn goes out as prompt with streamingBehavior steer and lands in the same turn",
      () =>
        Effect.gen(function* () {
          const { fake, runtime, providerThread, seen, record, begin } = yield* openTurn();
          const afterPrompt = yield* begin();
          const providerTurnId = yield* runningTurnId(record);
          yield* fake.emit({ type: "agent_start" });
          yield* replyText(fake, "mock reply");
          yield* record(isAssistantText("mock reply"));

          yield* runtime.steerTurn({
            threadId: THREAD_ID,
            runId: RunId.make("run:thread-prime-test:1"),
            providerThread,
            providerTurnId,
            message: steerMessage("focus on tests"),
          });
          const steer = yield* fake.takeRequest("prompt");
          assert.equal(steer["message"], "focus on tests");
          assert.equal(steer["streamingBehavior"], "steer");
          yield* fake.emit({ type: "response", command: "prompt", success: true });
          yield* fake.emit({
            type: "message_start",
            message: { role: "user", content: [{ type: "text", text: "focus on tests" }] },
          });
          yield* fake.emit({
            type: "message_end",
            message: { role: "user", content: [{ type: "text", text: "focus on tests" }] },
          });
          yield* replyText(fake, "steered: focus on tests");
          yield* fake.emit(agentEnd());

          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(
            terminal.type === "turn.terminal" &&
              terminal.status === "completed" &&
              terminal.providerTurnId === providerTurnId,
          );
          assert.isTrue(seen.some(isAssistantText("steered: focus on tests")));
          assert.lengthOf(terminals(seen), 1);
          assert.notInclude(commandsSince(fake, afterPrompt), "abort");
          assert.notInclude(commandsSince(fake, afterPrompt), "steer");
          assert.lengthOf(promptsSince(fake, 0), 2);
          assert.lengthOf(
            seen.filter(
              (event) =>
                event.type === "provider_turn.updated" && event.providerTurn.status === "running",
            ),
            1,
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-007] a busy Prime that rejects plain prompts still receives the turn and the steer, and a busy rejection does not fail the turn",
      () =>
        Effect.gen(function* () {
          // FakePrime answers a prompt without streamingBehavior with Prime's
          // busy error while streaming, and acks everything else on its own.
          const { fake, runtime, providerThread, seen, record } = yield* openTurn((fake) =>
            fake.setStreaming(true),
          );
          yield* startTurn(runtime, providerThread);
          const prompt = yield* fake.takeRequest("prompt");
          assert.equal(prompt["streamingBehavior"], "steer");
          const providerTurnId = yield* runningTurnId(record);
          yield* fake.emit({ type: "agent_start" });
          yield* replyText(fake, "busy after this");

          yield* runtime.steerTurn({
            threadId: THREAD_ID,
            runId: RunId.make("run:thread-prime-test:1"),
            providerThread,
            providerTurnId,
            message: steerMessage("continue?"),
          });
          const steer = yield* fake.takeRequest("prompt");
          assert.equal(steer["message"], "continue?");
          assert.equal(steer["streamingBehavior"], "steer");
          // Prime's busy rejection without a request id, as its daemon reports it.
          yield* fake.emitOrphanError(
            "prompt",
            "Agent has queued work. Specify streamingBehavior ('steer' or 'followUp') to queue the message.",
          );
          yield* pumpBarrier(fake, record, "after busy rejection");
          assert.lengthOf(terminals(seen), 0);

          yield* replyText(fake, "mock reply");
          fake.setStreaming(false);
          yield* fake.emit(agentEnd());
          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.lengthOf(terminals(seen), 1);
          assert.isFalse(
            seen.some(
              (event) =>
                event.type === "provider_session.updated" &&
                event.providerSession.status === "error",
            ),
          );
          for (const request of promptsSince(fake, 0)) {
            assert.equal(request["streamingBehavior"], "steer");
          }
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-008] ABORT_ME: interrupt sends abort, the aborted agent_end yields one interrupted terminal, the open tool item closes, and the queued steer is not resent",
      () =>
        Effect.gen(function* () {
          const { fake, runtime, providerThread, seen, record, begin } = yield* openTurn();
          yield* begin();
          const providerTurnId = yield* runningTurnId(record);
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit({
            type: "tool_execution_start",
            toolCallId: "tool-abort-1",
            toolName: "bash",
            args: { command: "sleep 60" },
          });
          yield* record(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.nativeItemRef?.nativeId === "tool-abort-1" &&
              event.turnItem.status === "running",
          );
          yield* runtime.steerTurn({
            threadId: THREAD_ID,
            runId: RunId.make("run:thread-prime-test:1"),
            providerThread,
            providerTurnId,
            message: steerMessage("queued while the tool runs"),
          });
          yield* fake.takeRequest("prompt");
          yield* fake.emit({ type: "response", command: "prompt", success: true });

          yield* runtime.interruptTurn({ providerThread, providerTurnId });
          yield* fake.takeRequest("abort");
          const afterAbort = fake.commandLog().length;
          yield* fake.emit({ type: "turn_end" });
          yield* fake.emit(agentEnd(true));

          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(
            terminal.type === "turn.terminal" &&
              terminal.status === "interrupted" &&
              terminal.providerTurnId === providerTurnId,
          );
          assert.equal(finalToolStatus(seen, "tool-abort-1"), "interrupted");
          assert.notInclude(commandsSince(fake, afterAbort), "prompt");
          assert.notInclude(commandsSince(fake, afterAbort), "steer");

          // The next turn sends only its own prompt; the queued steer stays Prime's.
          const afterNext = fake.allRequests().length;
          yield* begin("Next question", 2);
          assert.deepEqual(
            promptsSince(fake, afterNext).map((request) => request["message"]),
            ["Next question"],
          );
          yield* fake.emit({ type: "agent_start" });
          yield* replyText(fake, "Next answer");
          yield* fake.emit(agentEnd());
          const next = yield* record(
            (event) => event.type === "turn.terminal" && event.providerTurnId !== providerTurnId,
          );
          assert.isTrue(next.type === "turn.terminal" && next.status === "completed");
          assert.lengthOf(terminals(seen), 2);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect.each([
      { shape: "message_end and agent_end", withMessageEnd: true },
      { shape: "agent_end only", withMessageEnd: false },
    ])(
      "[VAL-TURN-009] FAIL_ME: the errorMessage from $shape reaches turn.terminal failure.message",
      ({ withMessageEnd }) =>
        Effect.gen(function* () {
          const { fake, runtime, seen, record, begin } = yield* openTurn();
          yield* begin();
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit({ type: "turn_start" });
          if (withMessageEnd) {
            yield* fake.emit({ type: "message_start", message: failedAssistant(FAIL_ME_ERROR) });
            yield* fake.emit({ type: "message_end", message: failedAssistant(FAIL_ME_ERROR) });
          }
          yield* fake.emit({ type: "turn_end" });
          yield* fake.emit({
            type: "agent_end",
            messages: [
              { role: "user", content: "did something happen?" },
              failedAssistant(FAIL_ME_ERROR),
            ],
          });
          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "failed");
          const failure = terminal.type === "turn.terminal" ? terminal.failure : null;
          assert.include(failure?.message ?? "", "Expected at most one leading ThinkChunk");
          assert.equal(failure?.class, "provider_error");
          assert.isTrue(
            seen.some(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.type === "error" &&
                event.turnItem.failure.message === failure?.message,
            ),
          );
          assert.isFalse(
            seen.some(
              (event) =>
                event.type === "turn_item.updated" && event.turnItem.type === "assistant_message",
            ),
            "the empty failed assistant message leaves no assistant bubble",
          );
          assert.include(runtime.providerSession.lastError ?? "", "leading ThinkChunk");
          assert.lengthOf(terminals(seen), 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-009] a provider error retried by Prime, in the live frame order, settles once as completed",
      () =>
        Effect.gen(function* () {
          // Order recorded live in message_steering: the failed run's agent_end
          // arrives before auto_retry_start, while the turn action stays running.
          const capacityError =
            'Mistral API error (429): {"object":"error","message":"Not enough capacity available for this request, please retry later.","code":"3801"}';
          const { fake, seen, record, begin } = yield* openTurn();
          const afterPrompt = yield* begin();
          yield* fake.setSessionActions(RUNNING_TURN_ACTIONS);
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit({ type: "turn_start" });
          yield* fake.emit({ type: "message_start", message: failedAssistant(capacityError) });
          yield* fake.emit({ type: "message_end", message: failedAssistant(capacityError) });
          yield* fake.emit({ type: "turn_end" });
          yield* fake.emit({ type: "agent_end", messages: [failedAssistant(capacityError)] });
          yield* fake.emit({
            type: "auto_retry_start",
            attempt: 1,
            maxAttempts: 30,
            delayMs: 976,
            errorMessage: capacityError,
          });
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit({ type: "turn_start" });
          yield* replyText(fake, "steering fixture observed");
          yield* fake.emit({ type: "auto_retry_end", success: true, attempt: 1 });
          yield* fake.emit({ type: "turn_end" });
          yield* fake.emit(agentEnd());
          yield* record(isAssistantText("steering fixture observed"));
          assert.equal(probesSince(fake, afterPrompt), 0, "a running turn action defers the probe");
          assert.lengthOf(terminals(seen), 0);

          yield* fake.setSessionActions(IDLE_ACTIONS);
          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.lengthOf(terminals(seen), 1);
          assert.equal(probesSince(fake, afterPrompt), 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-009] an error stopReason with a blank errorMessage fails the turn with a generic message",
      () =>
        Effect.gen(function* () {
          const { fake, seen, record, begin } = yield* openTurn();
          yield* begin();
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit({ type: "agent_end", messages: [failedAssistant("   ")] });
          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(
            terminal.type === "turn.terminal" &&
              terminal.status === "failed" &&
              terminal.failure.message === "Prime reported a model error.",
          );
          assert.lengthOf(terminals(seen), 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-010] DIE: Prime exiting mid-turn fails the turn once with the exit reason, closes its tool item, and reports the session errored",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakePrime;
          const adapter = yield* makeAdapter(fake);
          const runtime = yield* adapter.openSession({
            threadId: THREAD_ID,
            providerSessionId: SESSION_ID,
            modelSelection: modelSelection("default"),
            runtimePolicy,
          });
          const seen: Array<AdapterEvent> = [];
          const stream = yield* runtime.events.pipe(
            Stream.runForEach((event) => Effect.sync(() => seen.push(event))),
            Effect.flip,
            Effect.forkScoped,
          );
          const providerThread = yield* runtime.ensureThread({
            threadId: THREAD_ID,
            modelSelection: modelSelection("default"),
            runtimePolicy,
          });
          yield* startTurn(runtime, providerThread);
          yield* fake.takeRequest("prompt");
          yield* fake.emit({ type: "response", command: "prompt", success: true });
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit({
            type: "tool_execution_start",
            toolCallId: "tool-die-1",
            toolName: "bash",
            args: { command: "make" },
          });
          yield* fake.exit(1);

          const error = yield* Fiber.join(stream);
          assert.equal(error._tag, "ProviderAdapterEventStreamError");
          const ended = terminals(seen);
          assert.lengthOf(ended, 1);
          const terminal = ended[0];
          assert.isTrue(terminal?.type === "turn.terminal" && terminal.status === "failed");
          const failure = terminal?.type === "turn.terminal" ? terminal.failure : null;
          assert.include(failure?.message ?? "", "prime-agent process exited with code 1");
          assert.equal(failure?.class, "transport_error");
          assert.equal(finalToolStatus(seen, "tool-die-1"), "failed");
          const sessionStates = seen.flatMap((event) =>
            event.type === "provider_session.updated" ? [event.providerSession] : [],
          );
          const last = sessionStates.at(-1);
          assert.equal(last?.status, "error");
          assert.include(last?.lastError ?? "", "prime-agent process exited with code 1");
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-012] assistant, tool, and custom frames with no open turn and no pending wake are dropped without a turn or session error, and the next turn behaves normally",
      () =>
        Effect.gen(function* () {
          const { fake, runtime, seen, record, begin } = yield* openTurn();
          const probesBefore = fake.commandLog().filter((type) => type === "get_state").length;
          yield* fake.emit({ type: "turn_start" });
          yield* fake.emit({ type: "message_start", message: { role: "assistant" } });
          yield* fake.emit({
            type: "message_update",
            assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "stray text" },
          });
          yield* fake.emit({
            type: "message_end",
            message: { role: "assistant", content: [{ type: "text", text: "stray text" }] },
          });
          yield* fake.emit({
            type: "tool_execution_start",
            toolCallId: "stray-tool",
            toolName: "bash",
            args: { command: "ls" },
          });
          yield* fake.emit({
            type: "tool_execution_end",
            toolCallId: "stray-tool",
            toolName: "bash",
            result: { content: [{ type: "text", text: "README.md" }] },
          });
          const custom = {
            role: "custom",
            customType: "autonomous_status",
            content: "stray notice",
            display: true,
            details: {},
          };
          yield* fake.emit({ type: "message_start", message: custom });
          yield* fake.emit({ type: "message_end", message: custom });
          yield* fake.emit({ type: "turn_end" });
          yield* fake.emit(agentEnd());
          // A session-scoped dialog is the first event the pump can surface,
          // so answering it proves every stray frame above was handled.
          yield* fake.emit({
            type: "extension_ui_request",
            id: "ui-barrier",
            method: "confirm",
            title: "Barrier",
          });
          const pending = yield* record(
            (event) =>
              event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
          );
          assert.isTrue(pending.type === "runtime_request.updated");
          yield* runtime.respondToRuntimeRequest({
            requestId: (pending as Extract<AdapterEvent, { type: "runtime_request.updated" }>)
              .runtimeRequest.id,
            decision: "accept",
          });
          yield* fake.takeRequest("extension_ui_response");
          assert.isFalse(
            seen.some(
              (event) =>
                event.type === "provider_turn.updated" ||
                event.type === "turn_item.updated" ||
                event.type === "turn.terminal",
            ),
          );
          assert.equal(
            fake.commandLog().filter((type) => type === "get_state").length,
            probesBefore,
            "stray frames never trigger a settle probe",
          );

          yield* begin("Real question");
          yield* fake.emit({ type: "agent_start" });
          yield* replyText(fake, "Real answer");
          yield* fake.emit(agentEnd());
          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.lengthOf(terminals(seen), 1);
          const texts = seen.flatMap((event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "assistant_message"
              ? [event.turnItem.text]
              : [],
          );
          assert.notInclude(texts.join("\n"), "stray text");
          assert.isFalse(
            seen.some(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.nativeItemRef?.nativeId === "stray-tool",
            ),
          );
          assert.isFalse(
            seen.some(
              (event) =>
                event.type === "provider_session.updated" &&
                event.providerSession.status === "error",
            ),
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-012] an agent_start with no open turn and no continuation path is rejected as a session error and never becomes a turn",
      () =>
        Effect.gen(function* () {
          const { fake, seen, record } = yield* openTurn();
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit({ type: "message_start", message: { role: "assistant" } });
          yield* fake.emit({
            type: "message_update",
            assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "invisible" },
          });
          const sessionError = yield* record(
            (event) =>
              event.type === "provider_session.updated" && event.providerSession.status === "error",
          );
          assert.isTrue(
            sessionError.type === "provider_session.updated" &&
              sessionError.providerSession.lastError?.includes("outside an active T3 turn") ===
                true,
          );
          assert.isFalse(
            seen.some(
              (event) =>
                event.type === "provider_turn.updated" ||
                event.type === "turn_item.updated" ||
                event.type === "turn.terminal",
            ),
          );
          assert.notInclude(fake.commandLog(), "prompt");
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });
});

describe("PrimeRpc framing", () => {
  it.effect("reassembles records across chunk boundaries and strips CR", () =>
    Effect.gen(function* () {
      const stdout = yield* Queue.unbounded<Uint8Array>();
      const spawner = ChildProcessSpawner.make(() =>
        Effect.succeed(
          ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(FAKE_PID),
            exitCode: Effect.never,
            isRunning: Effect.succeed(true),
            kill: () => Effect.void,
            unref: Effect.succeed(Effect.void),
            stdin: Sink.drain,
            stdout: Stream.fromQueue(stdout),
            stderr: Stream.empty,
            all: Stream.empty,
            getInputFd: () => Sink.drain,
            getOutputFd: () => Stream.empty,
          }),
        ),
      );
      const connection = yield* makePrimeRpcConnection({
        command: "prime-agent",
        args: ["--mode", "rpc"],
        cwd: undefined,
        env: {},
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));

      const push = (text: string) =>
        Queue.offer(stdout, new TextEncoder().encode(text)).pipe(Effect.asVoid);
      yield* push('{"type":"agent_');
      yield* push('start"}\r\n{"type":"agent_end"}\nnot json\n{"type":"queue_update"}\n');

      yield* push('{"type":"must_not_emit","text":"');
      const chunk = new TextEncoder().encode("x".repeat(8 * 1024 * 1024));
      for (let index = 0; index < 32; index++) yield* Queue.offer(stdout, chunk);
      yield* push('"}\n{"type":"after_oversized"}\n');

      const first = yield* Queue.take(connection.events);
      assert.equal(first["type"], "agent_start");
      const second = yield* Queue.take(connection.events);
      assert.equal(second["type"], "agent_end");
      const third = yield* Queue.take(connection.events);
      assert.equal(third["type"], "queue_update");
      assert.equal((yield* Queue.take(connection.events))["type"], "after_oversized");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("PrimeRpc early process exit", () => {
  const makeHandle = (options: {
    readonly exitCode: Effect.Effect<ChildProcessSpawner.ExitCode>;
    readonly stderr: Stream.Stream<Uint8Array>;
  }) =>
    ChildProcessSpawner.makeHandle({
      pid: ChildProcessSpawner.ProcessId(FAKE_PID),
      exitCode: options.exitCode,
      isRunning: Effect.succeed(true),
      kill: () => Effect.void,
      unref: Effect.succeed(Effect.void),
      stdin: Sink.drain,
      stdout: Stream.empty,
      stderr: options.stderr,
      all: Stream.empty,
      getInputFd: () => Sink.drain,
      getOutputFd: () => Stream.empty,
    });

  it.effect("reports a nonzero exit code instead of an unexplained stdout close", () =>
    Effect.gen(function* () {
      const secret = "API_KEY=super-secret\n";
      const spawner = ChildProcessSpawner.make(() =>
        Effect.succeed(
          makeHandle({
            exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
            stderr: Stream.fromIterable([new TextEncoder().encode(secret)]),
          }),
        ),
      );
      const connection = yield* makePrimeRpcConnection({
        command: "prime-agent",
        args: ["--mode", "rpc"],
        cwd: undefined,
        env: {},
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));

      const error = yield* Queue.take(connection.events).pipe(Effect.flip);
      assert.equal(error._tag, "PrimeRpcError");
      assert.equal(error.operation, "read");
      assert.equal(error.detail, "prime-agent process exited with code 1");
      assert.isFalse((error.detail ?? "").includes("API_KEY"));
      assert.isFalse((error.detail ?? "").includes("super-secret"));
      assert.isFalse(error.message.includes("API_KEY"));
      assert.isFalse(error.message.includes("super-secret"));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps the unexplained stdout-close message when the process has not exited", () =>
    Effect.gen(function* () {
      const spawner = ChildProcessSpawner.make(() =>
        Effect.succeed(
          makeHandle({
            exitCode: Effect.never,
            stderr: Stream.empty,
          }),
        ),
      );
      const connection = yield* makePrimeRpcConnection({
        command: "prime-agent",
        args: ["--mode", "rpc"],
        cwd: undefined,
        env: {},
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));

      const fiber = yield* Effect.forkChild(Queue.take(connection.events).pipe(Effect.flip));
      yield* TestClock.adjust(Duration.millis(300));
      const error = yield* Fiber.join(fiber);
      assert.equal(error._tag, "PrimeRpcError");
      assert.equal(error.operation, "read");
      assert.equal(error.detail, "prime-agent process closed stdout");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps the exit-code diagnosis when stdin breaks while exit is still pending", () =>
    Effect.gen(function* () {
      const spawner = ChildProcessSpawner.make(() =>
        Effect.succeed(
          ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(FAKE_PID),
            exitCode: Effect.sleep(Duration.millis(50)).pipe(
              Effect.andThen(Effect.succeed(ChildProcessSpawner.ExitCode(1))),
            ),
            isRunning: Effect.succeed(true),
            kill: () => Effect.void,
            unref: Effect.succeed(Effect.void),
            stdin: Sink.fail(
              PlatformError.systemError({
                _tag: "Unknown",
                module: "ChildProcess",
                method: "stdin",
                description: "broken pipe",
              }),
            ),
            stdout: Stream.empty,
            stderr: Stream.empty,
            all: Stream.empty,
            getInputFd: () => Sink.drain,
            getOutputFd: () => Stream.empty,
          }),
        ),
      );
      const connection = yield* makePrimeRpcConnection({
        command: "prime-agent",
        args: ["--mode", "rpc"],
        cwd: undefined,
        env: {},
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));

      const fiber = yield* Effect.forkChild(Queue.take(connection.events).pipe(Effect.flip));
      yield* TestClock.adjust(Duration.millis(300));
      const error = yield* Fiber.join(fiber);
      assert.equal(error._tag, "PrimeRpcError");
      assert.equal(error.operation, "read");
      assert.equal(error.detail, "prime-agent process exited with code 1");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
