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
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
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
import {
  makePrimeRpcConnection,
  primeRecordField,
  primeRecordString,
  type PrimeRpcRecord,
} from "./rpc.ts";
import {
  primeContentText,
  primeTurnEntries,
  primeTurnFingerprint,
  primeUserEntryHash,
  type PrimeTurnLead,
} from "./events.ts";
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
  /**
   * Prime accepting a session-command prompt, in the order the free live
   * probe recorded (live-runs.md, prime-session-commands): the action is
   * already admitted when the id-less ack goes out, so a `get_state` answered
   * after the ack reports it; the `preparing` and `running` updates follow.
   */
  readonly acceptSessionCommand: (label: string) => Effect.Effect<void>;
  /** What `list_schedules` and `list_heartbeats` report (modes/rpc/rpc-mode.ts:351-362). */
  readonly setBackgroundJobs: (
    jobs: ReadonlyArray<unknown>,
    heartbeats: ReadonlyArray<unknown>,
  ) => void;
  /**
   * Replaces the session's user entries. Emitted user `message_end`s append
   * to them, as Prime persists each one; `get_fork_messages` lists them and
   * `fork` cuts them before its entry and switches to a new session file
   * (core/agent-session-runtime.ts fork).
   */
  readonly setForkMessages: (messages: ReadonlyArray<{ entryId: string; text: string }>) => void;
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

const sessionCommandOf = (text: string) => {
  const match = /^\/(\S+)(?:\s+([\s\S]*))?$/u.exec(text);
  return { name: match?.[1] ?? "", args: (match?.[2] ?? "").trim(), text };
};

/** Prime's echo of the slash text (core/messages.ts createSessionSlashCommandMessage). */
const sessionCommandEcho = (text: string) => ({
  role: "custom",
  customType: "session_slash_command",
  content: text,
  display: true,
  details: { command: sessionCommandOf(text) },
});

/** Prime's result row (core/agent-session.ts _executeQueuedSessionCommand). */
const sessionCommandResult = (text: string, content: string, success = true, display = true) => ({
  role: "custom",
  customType: "session_slash_command_result",
  content,
  display,
  details: {
    command: sessionCommandOf(text),
    success,
    severity: success ? "info" : "error",
    ...(success ? {} : { error: content.replace(/^Command failed:\s*/u, "") }),
  },
});

/** A refused rollback or fork carries Prime's typed refusal as its cause. */
const boundaryRefusal = (
  error: { readonly _tag: string; readonly cause?: unknown },
  tag: string,
) => {
  assert.equal(error._tag, tag);
  assert.equal(primeRecordString(error.cause, "_tag"), "PrimeRollbackBoundaryError");
  return primeRecordString(error.cause, "message") ?? "";
};

const emitCustomMessage = (fake: FakePrime, message: Record<string, unknown>) =>
  Effect.gen(function* () {
    yield* fake.emit({ type: "message_start", message });
    yield* fake.emit({ type: "message_end", message });
  });
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
/** A settled turn as the adapter records it: synthetic weak ref plus the user entries it added. */
const recordedTurn = (
  providerThreadId: ProviderThreadId,
  ordinal: number,
  texts: ReadonlyArray<string>,
  lead: PrimeTurnLead = texts.length > 0 ? "user" : "none",
): OrchestrationV2ProviderTurn => ({
  id: ProviderTurnId.make(`turn-${ordinal}`),
  providerThreadId,
  nodeId: NodeId.make(`node-${ordinal}`),
  runAttemptId: null,
  nativeTurnRef: {
    driver: PRIME_PROVIDER,
    nativeId: `${providerThreadId}:attempt:${ordinal}`,
    strength: "weak",
    fingerprint: primeTurnFingerprint({ lead, hashes: texts.map(primeUserEntryHash) }),
  },
  ordinal,
  status: "completed",
  startedAt: null,
  completedAt: null,
});

const SESSION_ENTRY_TIME = "2026-10-17T10:00:00.000Z";
/** Prime's v3 session header (core/session-manager.ts SessionHeader). */
const primeSessionHeader = {
  type: "session",
  version: 3,
  id: "019a0000-0000-7000-8000-000000000001",
  timestamp: SESSION_ENTRY_TIME,
  cwd: "/workspace",
};
const primeSessionMessage = (id: string, message: Record<string, unknown>) => ({
  type: "message",
  id,
  parentId: null,
  timestamp: SESSION_ENTRY_TIME,
  message: { timestamp: 1, ...message },
});
const primeSessionUser = (id: string, text: string) =>
  primeSessionMessage(id, { role: "user", content: [{ type: "text", text }] });
const primeSessionAssistant = (id: string, text: string) =>
  primeSessionMessage(id, {
    role: "assistant",
    content: [{ type: "text", text }],
    stopReason: "stop",
  });
const primeSessionCustom = (id: string, customType: string, content: string) => ({
  type: "custom_message",
  id,
  parentId: null,
  timestamp: SESSION_ENTRY_TIME,
  customType,
  content,
  display: true,
});
/** A Prime session JSONL in a scoped temp dir; `undefined` leaves the path absent. */
const writePrimeSession = Effect.fnUntraced(function* (
  entries: ReadonlyArray<unknown> | undefined,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "prime-fork-source-" });
  const file = `${directory}/source.jsonl`;
  if (entries !== undefined) {
    yield* fileSystem.writeFileString(
      file,
      entries.map((entry) => `${encodeJsonLine(entry)}\n`).join(""),
    );
  }
  return file;
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
  let scheduledJobs: ReadonlyArray<unknown> = [];
  let heartbeatJobs: ReadonlyArray<unknown> = [];
  let stdinBuffer = "";
  let forkMessages: Array<{ entryId: string; text: string }> = [];
  let nextEntry = 0;
  // Prime 1.0.0 has no get_entries (modes/rpc/rpc-mode.ts default branch).
  const unknownCommands = new Set<string>(["get_entries"]);
  const commandFailures = new Map<string, string>();

  const emit = (record: PrimeRpcRecord, chunkBytes?: number) =>
    Effect.gen(function* () {
      const message = record["message"];
      if (record["type"] === "message_end" && primeRecordString(message, "role") === "user") {
        const text = primeContentText(primeRecordField(message, "content"));
        if (text.length > 0) forkMessages.push({ entryId: `entry-${++nextEntry}`, text });
      }
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
      case "get_fork_messages":
        return { ...base, data: { messages: [...forkMessages] } };
      case "fork": {
        const index = forkMessages.findIndex((entry) => entry.entryId === record["entryId"]);
        if (index === -1) return { ...base, success: false, error: "Invalid entry ID for forking" };
        const text = forkMessages[index]!.text;
        forkMessages = forkMessages.slice(0, index);
        sessionFile = `/fake/fork-${++sessionGeneration}.jsonl`;
        return { ...base, data: { text, cancelled: false } };
      }
      case "list_schedules":
        return { ...base, data: { jobs: scheduledJobs } };
      case "list_heartbeats":
        return { ...base, data: { heartbeats: heartbeatJobs } };
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
    acceptSessionCommand: (label) =>
      Effect.gen(function* () {
        sessionActions = {
          ...IDLE_ACTIONS,
          active: { kind: "session_command", phase: "preparing", label },
        };
        yield* emit({ type: "response", command: "prompt", success: true });
        yield* emit({ type: "session_action_update", actions: sessionActions });
        sessionActions = {
          ...IDLE_ACTIONS,
          active: { kind: "session_command", phase: "running", label },
        };
        yield* emit({ type: "session_action_update", actions: sessionActions });
      }),
    setBackgroundJobs: (jobs, heartbeats) => {
      scheduledJobs = jobs;
      heartbeatJobs = heartbeats;
    },
    setForkMessages: (messages) => {
      forkMessages = [...messages];
    },
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
  policy = runtimePolicy,
) {
  const adapter = yield* makeAdapter(fake, "", forkFake, continuationRequests);
  const runtime = yield* adapter.openSession({
    threadId,
    providerSessionId,
    modelSelection: modelSelection(model),
    runtimePolicy: policy,
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
        fake.setForkMessages([{ entryId: "user-before-wake", text: "before wake" }]);
        fake.deferNextLifecycle("fork");
        const rollingBack = yield* runtime
          .rollbackThread({
            providerThread,
            target: {
              type: "thread_start",
              checkpointId: CheckpointId.make("checkpoint-start"),
              appRunOrdinal: 0,
            },
            providerThreadTurns: [recordedTurn(providerThread.id, 1, ["before wake"])],
          })
          .pipe(Effect.result, Effect.forkScoped);
        const fork = yield* fake.takeRequest("fork");
        assert.equal(fork["entryId"], "user-before-wake");
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
          providerThreadTurns: [recordedTurn(providerThread.id, 1, ["before wake"])],
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
    "[VAL-LAUNCH-003] fails a resume still blocked after ~45 s with a typed lease conflict naming $sessionId and prime-agent stop, logged as a warning",
    ({ error, sessionId }) => {
      const warnings: Array<string> = [];
      const logger = Logger.make(({ logLevel, message }) => {
        if (logLevel !== "Warn") return;
        warnings.push((Array.isArray(message) ? message : [message]).map(String).join(" "));
      });
      return Effect.gen(function* () {
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
        const leaseConflict =
          failure._tag === "ProviderAdapterResumeThreadError" ? failure.cause : undefined;
        assert.instanceOf(leaseConflict, Error);
        assert.equal(
          (leaseConflict as { readonly _tag?: unknown })._tag,
          "PrimeSessionLeaseConflictError",
        );
        const message = (leaseConflict as Error).message;
        assert.include(message, `Prime session ${sessionId} is still open`);
        assert.include(message, `prime-agent stop ${sessionId}`);
        assert.isTrue(
          warnings.some((warning) => warning.includes(message)),
          `expected a warning log carrying "${message}"`,
        );
        assert.isAbove(
          fake.allRequests().filter((request) => request.type === "switch_session").length,
          40,
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(layerTest, Logger.layer([logger], { mergeWithExisting: false })),
        ),
      );
    },
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
    {
      label: "the latest turn",
      turns: [["one"]],
      sourceEntries: ["one"],
      expectedForks: [],
    },
    {
      label: "a historical turn whose successor was steered",
      turns: [["one"], ["two", "steer"]],
      sourceEntries: ["one", "two", "steer"],
      expectedForks: ["entry-2"],
    },
    {
      label: "a turn followed by a rolled-back turn",
      turns: [["one"], ["rolled back"], ["three"]],
      sourceEntries: ["one", "three"],
      expectedForks: ["entry-2"],
    },
  ])(
    "[VAL-FORK-002] natively forks $label into an independent session",
    ({ turns, sourceEntries, expectedForks }) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const forkFake = yield* makeFakePrime;
        const forkFile = "/fake/forked.jsonl";
        const sourceFile = yield* writePrimeSession([
          primeSessionHeader,
          ...sourceEntries.flatMap((text, index) => [
            primeSessionUser(`entry-${index + 1}`, text),
            primeSessionAssistant(`reply-${index + 1}`, "ok"),
          ]),
        ]);
        const { runtime, takeEvent } = yield* openRuntime(
          fake,
          "default",
          THREAD_ID,
          SESSION_ID,
          forkFake,
        );
        fake.queueState({ sessionFile: sourceFile });
        const source = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        const sourceTurns = turns.map((texts, index) => recordedTurn(source.id, index + 1, texts));
        // `--fork` copies the source session's entries with their ids.
        forkFake.setForkMessages(
          sourceEntries.map((text, index) => ({ entryId: `entry-${index + 1}`, text })),
        );
        forkFake.queueState({ sessionFile: forkFile });
        fake.queueState({ sessionFile: forkFile });
        const target = ThreadId.make("fork-target");
        const forked = yield* runtime.forkThread({
          sourceProviderThread: source,
          sourceProviderTurns: sourceTurns,
          providerTurnId: sourceTurns[0]!.id,
          targetThreadId: target,
        });
        assert.equal(forked.appThreadId, target);
        assert.equal(forked.nativeThreadRef?.nativeId, forkFile);
        assert.notEqual(forked.id, source.id);
        assert.equal(source.nativeThreadRef?.nativeId, sourceFile);
        const args = forkFake.lastSpawn().args;
        assert.equal(args[args.indexOf("--fork") + 1], sourceFile);
        assert.include(args, "--no-extensions");
        assert.include(args, "--no-tools");
        assert.notInclude(args, "--no-session");
        assert.deepEqual(
          forkFake
            .allRequests()
            .filter((request) => request.type === "fork")
            .map((request) => request.entryId),
          expectedForks,
        );
        assert.include(forkFake.commandLog(), "get_fork_messages");
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

  it.effect("[VAL-FORK-002] rolls back past turns that added no user entries to the session", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
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

      fake.setForkMessages([{ entryId: "u1", text: "one" }]);
      yield* rollBackToFirst([
        recordedTurn(providerThread.id, 1, ["one"]),
        recordedTurn(providerThread.id, 2, []),
      ]);
      assert.deepEqual(forkedEntries(), []);
      assert.notInclude(fake.commandLog(), "get_fork_messages");

      fake.setForkMessages([
        { entryId: "u1", text: "one" },
        { entryId: "u3", text: "three" },
      ]);
      const rolledBack = yield* rollBackToFirst([
        recordedTurn(providerThread.id, 1, ["one"]),
        recordedTurn(providerThread.id, 2, []),
        recordedTurn(providerThread.id, 3, ["three"]),
      ]);
      assert.deepEqual(forkedEntries(), ["u3"]);
      assert.equal(rolledBack.providerThread.nativeThreadRef?.nativeId, "/fake/fork-1.jsonl");
      assert.isNull(rolledBack.providerThread.nativeConversationHeadRef);
      assert.notInclude(fake.commandLog(), "get_entries");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-FORK-002] rolls back a live thread to before the discarded turn's prompt, steer included",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        const settled: Array<OrchestrationV2ProviderTurn> = [];
        for (const [runOrdinal, texts] of [
          [1, ["first prompt"]],
          [2, ["second prompt", "second steer"]],
        ] as const) {
          yield* startTurn(runtime, providerThread, "default", [], texts[0], undefined, runOrdinal);
          yield* fake.takeRequest("prompt");
          yield* fake.emit({ type: "response", command: "prompt", success: true });
          yield* fake.emit({ type: "agent_start" });
          for (const text of texts) {
            yield* fake.emit({
              type: "message_end",
              message: { role: "user", content: [{ type: "text", text }] },
            });
          }
          yield* emitCustomMessage(fake, {
            role: "custom",
            customType: "harness_digest",
            content: "not a user entry",
            display: false,
          });
          yield* fake.emit(agentEnd());
          const terminal = yield* takeEvent(
            (event) =>
              event.type === "provider_turn.updated" && event.providerTurn.status === "completed",
          );
          if (terminal.type === "provider_turn.updated") settled.push(terminal.providerTurn);
          yield* takeEvent((event) => event.type === "turn.terminal");
        }
        assert.deepEqual(
          settled.map((turn) => [turn.nativeTurnRef?.strength, turn.nativeTurnRef?.fingerprint]),
          [
            [
              "weak",
              primeTurnFingerprint({ lead: "user", hashes: [primeUserEntryHash("first prompt")] }),
            ],
            [
              "weak",
              primeTurnFingerprint({
                lead: "user",
                hashes: ["second prompt", "second steer"].map(primeUserEntryHash),
              }),
            ],
          ],
        );
        const rolledBack = yield* runtime.rollbackThread({
          providerThread,
          target: {
            type: "provider_turn",
            checkpointId: CheckpointId.make("checkpoint-1"),
            appRunOrdinal: 1,
            providerTurn: settled[0]!,
          },
          providerThreadTurns: settled,
        });
        assert.deepEqual(
          fake
            .allRequests()
            .filter((request) => request.type === "fork")
            .map((request) => request.entryId),
          ["entry-2"],
        );
        assert.equal(rolledBack.providerThread.nativeThreadRef?.nativeId, "/fake/fork-1.jsonl");
        const listed = yield* fake.takeRequest("get_fork_messages");
        assert.isString(listed["id"]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    {
      label: "the session no longer holds the discarded turn's entries",
      second: recordedTurn(ProviderThreadId.make("unused"), 2, ["two"]),
      entries: [
        { entryId: "u1", text: "one" },
        { entryId: "u2", text: "something else" },
      ],
      listsEntries: true,
    },
    {
      label: "a discarded turn has no recorded user entries",
      second: {
        ...recordedTurn(ProviderThreadId.make("unused"), 2, ["two"]),
        nativeTurnRef: { driver: PRIME_PROVIDER, nativeId: "legacy", strength: "weak" as const },
      },
      entries: [{ entryId: "u1", text: "one" }],
      listsEntries: false,
    },
  ])(
    "[VAL-FORK-002] fails rollback without forking when $label",
    ({ second, entries, listsEntries }) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const { runtime } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        fake.setForkMessages(entries);
        const first = recordedTurn(providerThread.id, 1, ["one"]);
        const result = yield* runtime
          .rollbackThread({
            providerThread,
            target: {
              type: "provider_turn",
              checkpointId: CheckpointId.make("checkpoint-1"),
              appRunOrdinal: 1,
              providerTurn: first,
            },
            providerThreadTurns: [first, { ...second, providerThreadId: providerThread.id }],
          })
          .pipe(Effect.result);
        assert.equal(result._tag, "Failure");
        assert.notInclude(fake.commandLog(), "fork");
        assert.equal(fake.commandLog().includes("get_fork_messages"), listsEntries);
        const resumed = yield* startTurn(runtime, providerThread).pipe(Effect.result);
        assert.equal(resumed._tag, "Success", "a refused rollback leaves the session usable");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-FORK-002] rolls back to the first discarded prompt even when a heartbeat continuation follows it",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const { runtime } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        fake.setForkMessages([
          { entryId: "u1", text: "one" },
          { entryId: "u2", text: "two" },
        ]);
        const turns = [
          recordedTurn(providerThread.id, 1, ["one"]),
          recordedTurn(providerThread.id, 2, ["two"]),
          recordedTurn(providerThread.id, 3, [], "heartbeat"),
          recordedTurn(providerThread.id, 4, []),
        ];
        const rolledBack = yield* runtime.rollbackThread({
          providerThread,
          target: {
            type: "provider_turn",
            checkpointId: CheckpointId.make("checkpoint-1"),
            appRunOrdinal: 1,
            providerTurn: turns[0]!,
          },
          providerThreadTurns: turns,
        });
        assert.deepEqual(
          fake
            .allRequests()
            .filter((request) => request.type === "fork")
            .map((request) => request.entryId),
          ["u2"],
        );
        assert.equal(rolledBack.providerThread.nativeThreadRef?.nativeId, "/fake/fork-1.jsonl");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    {
      label: "an idle heartbeat continuation",
      lead: "heartbeat",
      kind: "a heartbeat continuation",
    },
    { label: "a /goal objective run", lead: "goal", kind: "a goal run" },
  ] as const)(
    "[VAL-FORK-002] refuses a historical fork across $label without copying or forking the session",
    ({ lead, kind }) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePrime;
        const forkFake = yield* makeFakePrime;
        const { runtime } = yield* openRuntime(fake, "default", THREAD_ID, SESSION_ID, forkFake);
        const source = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        const sourceTurns = [
          recordedTurn(source.id, 1, ["one"]),
          recordedTurn(source.id, 2, [], lead),
          recordedTurn(source.id, 3, ["three"]),
        ];
        forkFake.setForkMessages([
          { entryId: "entry-1", text: "one" },
          { entryId: "entry-2", text: "three" },
        ]);
        const error = yield* runtime
          .forkThread({
            sourceProviderThread: source,
            sourceProviderTurns: sourceTurns,
            providerTurnId: sourceTurns[0]!.id,
            targetThreadId: ThreadId.make("fork-target"),
          })
          .pipe(Effect.flip);
        const message = boundaryRefusal(error, "ProviderAdapterForkThreadError");
        assert.include(message, `turn 2 (${kind})`);
        assert.include(message, "The source thread is unchanged.");
        assert.deepEqual(forkFake.lastSpawn().args, [], "no --fork copy of the source was made");
        assert.isFalse(
          fake.allRequests().some((request) => request.type === "fork" || request.type === "clone"),
        );
        yield* startTurn(runtime, source);
        yield* fake.takeRequest("prompt");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  /**
   * Core opens the fork target's session and calls forkThread there
   * (ProviderTurnStartService), so the source thread's Prime process is not the
   * one doing the fork.
   */
  const openForkTarget = Effect.fnUntraced(function* (sourceFile: string) {
    const sourceFake = yield* makeFakePrime;
    const sourceRuntime = yield* openRuntime(sourceFake);
    sourceFake.queueState({ sessionFile: sourceFile });
    const source = yield* sourceRuntime.runtime.ensureThread({
      threadId: THREAD_ID,
      modelSelection: modelSelection("default"),
      runtimePolicy,
    });
    const targetFake = yield* makeFakePrime;
    const forkFake = yield* makeFakePrime;
    const target = ThreadId.make("fork-target");
    const { runtime } = yield* openRuntime(
      targetFake,
      "default",
      target,
      ProviderSessionId.make("provider-session-fork-target"),
      forkFake,
    );
    return { source, sourceFake, targetFake, forkFake, runtime, target };
  });
  const copiesOrForks = (fake: FakePrime) =>
    fake.allRequests().some((request) => request.type === "fork" || request.type === "clone");

  it.effect(
    "[VAL-FORK-002] refuses before copying the session when a rolled-back turn hides a later goal run (target runtime)",
    () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const sourceFile = yield* writePrimeSession([
          primeSessionHeader,
          primeSessionUser("e1", "one"),
          primeSessionAssistant("a1", "reply one"),
          primeSessionCustom("g3", "goal_context", "Goal: ship it"),
          primeSessionAssistant("a3", "working on the goal"),
        ]);
        const sourceBytes = yield* fileSystem.readFile(sourceFile);
        const { source, sourceFake, targetFake, forkFake, runtime, target } =
          yield* openForkTarget(sourceFile);
        const sourceRequests = sourceFake.allRequests().length;
        // Turn 2 was rolled back, so its user entry is gone from the source session.
        const sourceTurns = [
          recordedTurn(source.id, 1, ["one"]),
          recordedTurn(source.id, 2, ["two"]),
          recordedTurn(source.id, 3, [], "goal"),
        ];
        forkFake.setForkMessages([{ entryId: "e1", text: "one" }]);
        const error = yield* runtime
          .forkThread({
            sourceProviderThread: source,
            sourceProviderTurns: sourceTurns,
            providerTurnId: sourceTurns[0]!.id,
            targetThreadId: target,
          })
          .pipe(Effect.flip);
        const message = boundaryRefusal(error, "ProviderAdapterForkThreadError");
        assert.include(message, "turn 3 (a goal run)");
        assert.include(message, "The source thread is unchanged.");
        assert.deepEqual(forkFake.lastSpawn().args, [], "no --fork copy of the source was made");
        assert.isFalse(copiesOrForks(forkFake) || copiesOrForks(targetFake));
        assert.isFalse(copiesOrForks(sourceFake));
        assert.equal(sourceFake.allRequests().length, sourceRequests);
        assert.deepEqual(yield* fileSystem.readFile(sourceFile), sourceBytes);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-FORK-002] forks at the first discarded prompt when a heartbeat follows it (target runtime)",
    () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const sourceFile = yield* writePrimeSession([
          primeSessionHeader,
          primeSessionUser("e1", "one"),
          primeSessionAssistant("a1", "reply one"),
          primeSessionUser("e2", "two"),
          primeSessionAssistant("a2", "reply two"),
          primeSessionCustom("h3", "heartbeat_prompt", "heartbeat"),
          primeSessionAssistant("a3", "heartbeat reply"),
        ]);
        const sourceBytes = yield* fileSystem.readFile(sourceFile);
        const { source, sourceFake, targetFake, forkFake, runtime, target } =
          yield* openForkTarget(sourceFile);
        const sourceRequests = sourceFake.allRequests().length;
        const sourceTurns = [
          recordedTurn(source.id, 1, ["one"]),
          recordedTurn(source.id, 2, ["two"]),
          recordedTurn(source.id, 3, [], "heartbeat"),
        ];
        const forkFile = "/fake/forked-on-target.jsonl";
        forkFake.setForkMessages([
          { entryId: "e1", text: "one" },
          { entryId: "e2", text: "two" },
        ]);
        forkFake.queueState({ sessionFile: forkFile });
        targetFake.queueState({ sessionFile: forkFile });
        const forked = yield* runtime.forkThread({
          sourceProviderThread: source,
          sourceProviderTurns: sourceTurns,
          providerTurnId: sourceTurns[0]!.id,
          targetThreadId: target,
        });
        assert.equal(forked.appThreadId, target);
        assert.equal(forked.nativeThreadRef?.nativeId, forkFile);
        assert.equal(forked.forkedFrom?.providerThreadId, source.id);
        const args = forkFake.lastSpawn().args;
        assert.equal(args[args.indexOf("--fork") + 1], sourceFile);
        assert.deepEqual(
          forkFake
            .allRequests()
            .filter((request) => request.type === "fork")
            .map((request) => request.entryId),
          ["e2"],
        );
        assert.equal(
          targetFake.allRequests().findLast((request) => request.type === "switch_session")
            ?.sessionPath,
          forkFile,
        );
        assert.equal(sourceFake.allRequests().length, sourceRequests);
        assert.deepEqual(yield* fileSystem.readFile(sourceFile), sourceBytes);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    { label: "is missing", entries: undefined, detail: "could not read" },
    {
      label: "does not start with a session header",
      entries: [primeSessionUser("e1", "one"), primeSessionUser("e2", "two")],
      detail: "not a Prime session",
    },
    {
      label: "uses a newer session format",
      entries: [
        { ...primeSessionHeader, version: 4 },
        primeSessionUser("e1", "one"),
        primeSessionUser("e2", "two"),
      ],
      detail: "format version 4",
    },
  ])(
    "[VAL-FORK-002] refuses a historical fork before launch when the source session file $label",
    ({ entries, detail }) =>
      Effect.gen(function* () {
        const sourceFile = yield* writePrimeSession(entries);
        const fake = yield* makeFakePrime;
        const forkFake = yield* makeFakePrime;
        const { runtime } = yield* openRuntime(fake, "default", THREAD_ID, SESSION_ID, forkFake);
        fake.queueState({ sessionFile: sourceFile });
        const source = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        const sourceTurns = [
          recordedTurn(source.id, 1, ["one"]),
          recordedTurn(source.id, 2, ["two"]),
        ];
        forkFake.setForkMessages([
          { entryId: "e1", text: "one" },
          { entryId: "e2", text: "two" },
        ]);
        const error = yield* runtime
          .forkThread({
            sourceProviderThread: source,
            sourceProviderTurns: sourceTurns,
            providerTurnId: sourceTurns[0]!.id,
            targetThreadId: ThreadId.make("fork-target"),
          })
          .pipe(Effect.flip);
        assert.equal(error._tag, "ProviderAdapterForkThreadError");
        const message = primeRecordString(error.cause, "detail") ?? "";
        assert.include(message, detail);
        assert.include(message, "The source thread is unchanged");
        assert.deepEqual(forkFake.lastSpawn().args, [], "no --fork copy of the source was made");
        assert.isFalse(copiesOrForks(fake) || copiesOrForks(forkFake));
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

  it.effect("sends /compact to Prime as a session-command prompt, not the compact RPC", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePrime;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread, "default", [], "/compact");
      const prompt = yield* fake.takeRequest("prompt");
      assert.equal(prompt["message"], "/compact");
      assert.isFalse(fake.allRequests().some((request) => request["type"] === "compact"));
      // Prime skips a compaction on a short session with a warning-only
      // compaction_end and no result row (live probe, live-runs.md).
      yield* fake.acceptSessionCommand("/compact");
      yield* fake.emit({ type: "compaction_start", reason: "manual" });
      yield* fake.emit({
        type: "compaction_end",
        reason: "manual",
        aborted: false,
        willRetry: false,
        errorMessage: "Session is too short to compact — try again once it grows",
        errorSeverity: "warning",
      });
      yield* fake.setSessionActions(IDLE_ACTIONS);
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
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
      yield* fake.takeRequest("prompt");
      const running = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      const providerTurnId =
        running.type === "provider_turn.updated" ? running.providerTurn.id : undefined;
      assert.isDefined(providerTurnId);
      yield* fake.acceptSessionCommand("/compact");
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

  it.effect("steers /compact as a session-command prompt", () =>
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
      yield* fake.emit({ type: "response", command: "prompt", success: true });
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
      const steer = yield* fake.takeRequest("prompt");
      assert.equal(steer["message"], "/compact keep the tests");
      assert.equal(steer["streamingBehavior"], "steer");
      assert.isFalse(fake.allRequests().some((request) => request["type"] === "compact"));
      yield* fake.emit({ type: "response", command: "prompt", success: true });
      yield* fake.setSessionActions({
        ...IDLE_ACTIONS,
        active: { kind: "session_command", phase: "running", label: "/compact keep the tests" },
      });
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.emit({ type: "compaction_start", reason: "manual" });
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
      yield* fake.setSessionActions(IDLE_ACTIONS);
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

  const openTurn = Effect.fnUntraced(function* (
    configure?: (fake: FakePrime) => void,
    runtimeMode: ProviderAdapter.ProviderAdapterV2RuntimePolicy["runtimeMode"] = "full-access",
    continuationRequests?: PrimeAdapterV2Options["continuationRequests"],
  ) {
    const fake = yield* makeFakePrime;
    configure?.(fake);
    const { runtime, takeEvent } = yield* openRuntime(
      fake,
      "default",
      THREAD_ID,
      SESSION_ID,
      undefined,
      continuationRequests,
      ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({ ...runtimePolicy, runtimeMode }),
    );
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

  const runningTurnId = (
    record: (predicate: (event: AdapterEvent) => boolean) => Effect.Effect<AdapterEvent>,
  ) =>
    Effect.gen(function* () {
      const running = yield* record(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      assert.isTrue(running.type === "provider_turn.updated");
      return (running as Extract<AdapterEvent, { type: "provider_turn.updated" }>).providerTurn.id;
    });

  const steerMessage = (text: string) => ({
    messageId: `message:thread-prime-test:steer:${text}` as never,
    text,
    attachments: [],
    createdBy: "user" as const,
    creationSource: "web" as const,
  });

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
  describe("settle probe failures", () => {
    const GET_STATE_DETAIL = "session state store is locked";
    type SettlePath = "agent-activity" | "command-only";
    const SETTLE_PATHS: ReadonlyArray<SettlePath> = ["agent-activity", "command-only"];

    /** Starts a turn whose settle trigger sends the first get_state, returning the log index before it. */
    const triggerSettle = (
      turn: Effect.Success<ReturnType<typeof openTurn>>,
      path: SettlePath,
      beforeTrigger: () => void,
    ) =>
      Effect.gen(function* () {
        if (path === "command-only") {
          // The deferred prompt ack of a command-only prompt is its settle trigger.
          beforeTrigger();
          return yield* turn.begin("/command-only");
        }
        const afterPrompt = yield* turn.begin();
        yield* turn.fake.emit({ type: "agent_start" });
        yield* replyText(turn.fake, "Reply before settle");
        beforeTrigger();
        yield* turn.fake.emit(agentEnd());
        return afterPrompt;
      });

    const isCompletedTerminal = (event: AdapterEvent) =>
      event.type === "turn.terminal" && event.status === "completed";

    it.effect.each(SETTLE_PATHS)(
      "[VAL-TURN-014] on the %s path a failed settle get_state is retried on the clock and completes once after an idle read",
      (path) =>
        Effect.gen(function* () {
          const turn = yield* openTurn();
          const { fake, seen, record } = turn;
          const afterPrompt = yield* triggerSettle(turn, path, () => fake.failNextState());
          yield* fake.takeRequest("get_state");
          yield* pumpBarrier(fake, record, "after the failed probe");
          assert.lengthOf(terminals(seen), 0, "a failed probe never ends the turn");

          yield* advanceClockUntil(fake.takeRequest("get_state"));
          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.equal(probesSince(fake, afterPrompt), 2);
          assert.lengthOf(terminals(seen), 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect.each(SETTLE_PATHS)(
      "[VAL-TURN-014] on the %s path three failed settle get_state reads end the turn failed once with the get_state detail",
      (path) =>
        Effect.gen(function* () {
          const turn = yield* openTurn();
          const { fake, seen, record } = turn;
          const afterPrompt = yield* triggerSettle(turn, path, () =>
            fake.failCommand("get_state", GET_STATE_DETAIL),
          );
          yield* fake.takeRequest("get_state");
          yield* advanceClockUntil(fake.takeRequest("get_state"));
          yield* advanceClockUntil(fake.takeRequest("get_state"));
          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "failed");
          assert.include(
            terminal.type === "turn.terminal" ? terminal.failure?.message : undefined,
            GET_STATE_DETAIL,
          );
          assert.equal(probesSince(fake, afterPrompt), 3);
          assert.lengthOf(terminals(seen), 1);
          assert.isUndefined(seen.find(isCompletedTerminal));

          yield* TestClock.adjust(Duration.minutes(1));
          assert.equal(probesSince(fake, afterPrompt), 3, "retries stop after three attempts");
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-014] a timed-out settle get_state never completes the turn and is retried on the clock",
      () =>
        Effect.gen(function* () {
          const turn = yield* openTurn();
          const { fake, seen, record } = turn;
          const afterPrompt = yield* triggerSettle(turn, "agent-activity", () =>
            fake.deferNextState(),
          );
          yield* fake.takeRequest("get_state");
          yield* advanceClockUntil(fake.takeRequest("get_state"));
          assert.isUndefined(seen.find(isCompletedTerminal));
          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.equal(probesSince(fake, afterPrompt), 2);
          assert.lengthOf(terminals(seen), 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-014] a settle get_state that answers busy sends no timer-driven get_state and waits for the next transition",
      () =>
        Effect.gen(function* () {
          const turn = yield* openTurn();
          const { fake, seen, record } = turn;
          const afterPrompt = yield* triggerSettle(turn, "agent-activity", () =>
            fake.queueState({ isStreaming: true }),
          );
          yield* fake.takeRequest("get_state");
          yield* pumpBarrier(fake, record, "after the busy probe");
          yield* TestClock.adjust(Duration.minutes(1));
          yield* pumpBarrier(fake, record, "a minute after the busy probe");
          assert.equal(probesSince(fake, afterPrompt), 1);
          assert.lengthOf(terminals(seen), 0);

          yield* fake.setSessionActions(RUNNING_TURN_ACTIONS);
          yield* fake.setSessionActions(IDLE_ACTIONS);
          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.equal(probesSince(fake, afterPrompt), 2);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-014] agent_start after a failed settle get_state cancels the pending retry",
      () =>
        Effect.gen(function* () {
          const turn = yield* openTurn();
          const { fake, seen, record } = turn;
          const afterPrompt = yield* triggerSettle(turn, "agent-activity", () =>
            fake.failNextState(),
          );
          yield* fake.takeRequest("get_state");
          yield* pumpBarrier(fake, record, "after the failed probe");
          yield* fake.emit({ type: "agent_start" });
          yield* replyText(fake, "Second run");
          yield* record(isAssistantText("Second run"));
          yield* TestClock.adjust(Duration.minutes(1));
          yield* pumpBarrier(fake, record, "a minute after agent_start");
          assert.equal(probesSince(fake, afterPrompt), 1, "the stale retry was cancelled");
          assert.lengthOf(terminals(seen), 0);

          yield* fake.emit(agentEnd());
          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.equal(probesSince(fake, afterPrompt), 2);
          assert.lengthOf(terminals(seen), 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

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
      "[VAL-TURN-009] an idle probe answered after auto_retry_start does not settle the retried turn",
      () =>
        Effect.gen(function* () {
          // Order recorded live (prime_thread_rollback_after_stop, first attempt):
          // agent_end of the failed run, auto_retry_start, then the settle probe
          // answers idle because Prime reports no active action while it waits
          // out the retry delay.
          const degenerate = "Provider returned a degenerate response (degenerate_stream)";
          const { fake, seen, record, begin } = yield* openTurn();
          const afterPrompt = yield* begin();
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit({ type: "message_end", message: failedAssistant(degenerate) });
          fake.deferNextState();
          yield* fake.emit({ type: "agent_end", messages: [failedAssistant(degenerate)] });
          yield* fake.takeRequest("get_state");
          yield* fake.emit({
            type: "auto_retry_start",
            attempt: 1,
            maxAttempts: 3,
            delayMs: 2_458,
            errorMessage: degenerate,
          });
          yield* record(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "error" &&
              event.turnItem.status === "running",
          );
          yield* fake.resolveDeferredState(recordedIdleState(FAKE_SESSION_FILE));
          yield* fake.takeRequest("list_heartbeats");
          yield* fake.emit({
            type: "extension_ui_request",
            method: "notify",
            message: "probe seen",
          });
          yield* record(
            (event) =>
              event.type === "turn.terminal" ||
              (event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool"),
          );
          assert.lengthOf(terminals(seen), 0, "an idle probe during a retry delay does not settle");

          yield* fake.emit({ type: "agent_start" });
          yield* replyText(fake, "retried reply");
          yield* fake.emit({ type: "auto_retry_end", success: true, attempt: 1 });
          yield* fake.emit({ type: "turn_end" });
          yield* fake.emit(agentEnd());
          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(
            terminal.type === "turn.terminal" && terminal.status === "completed",
            "the turn settles only after the retried run",
          );
          assert.isTrue(seen.some(isAssistantText("retried reply")));
          assert.lengthOf(terminals(seen), 1);
          assert.equal(probesSince(fake, afterPrompt), 2);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-009] a retry that gives up after agent_end settles the turn as failed with Prime's final error",
      () =>
        Effect.gen(function* () {
          // Prime emits no further agent_end when retries run out
          // (core/agent-session.ts _handleRetryableError), so auto_retry_end is the trigger.
          const degenerate = "Provider returned a degenerate response (degenerate_stream)";
          const finalError = "Mistral API error (503): service unavailable";
          const { fake, seen, record, begin } = yield* openTurn();
          yield* begin();
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit({ type: "message_end", message: failedAssistant(degenerate) });
          fake.deferNextState();
          yield* fake.emit({ type: "agent_end", messages: [failedAssistant(degenerate)] });
          yield* fake.takeRequest("get_state");
          yield* fake.emit({
            type: "auto_retry_start",
            attempt: 1,
            maxAttempts: 1,
            delayMs: 1_000,
            errorMessage: degenerate,
          });
          yield* record(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "error" &&
              event.turnItem.status === "running",
          );
          yield* fake.resolveDeferredState(recordedIdleState(FAKE_SESSION_FILE));
          yield* fake.takeRequest("list_heartbeats");
          yield* fake.emit({
            type: "extension_ui_request",
            method: "notify",
            message: "probe seen",
          });
          yield* record(
            (event) =>
              event.type === "turn.terminal" ||
              (event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool"),
          );
          assert.lengthOf(terminals(seen), 0, "an idle probe during a retry delay does not settle");

          yield* fake.emit({ type: "auto_retry_end", success: false, attempt: 1, finalError });
          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(
            terminal.type === "turn.terminal" &&
              terminal.status === "failed" &&
              terminal.failure.message === finalError,
          );
          assert.lengthOf(terminals(seen), 1);
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

  describe("extension UI requests", () => {
    type RuntimeRequestEvent = Extract<AdapterEvent, { type: "runtime_request.updated" }>;
    type BridgeToolCallHook = (
      event: { toolName: string; input: unknown },
      ctx: { ui: { confirm: (title: string, message?: string) => Promise<boolean> } },
    ) => Promise<{ block: true; reason: string } | undefined>;

    const isPendingRequest = (event: AdapterEvent) =>
      event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending";

    const takePendingRequest = (
      record: (predicate: (event: AdapterEvent) => boolean) => Effect.Effect<AdapterEvent>,
    ) =>
      Effect.map(
        record(isPendingRequest),
        (event) => (event as RuntimeRequestEvent).runtimeRequest,
      );

    /** The last status each runtime request reported, by request id. */
    const requestStatuses = (seen: ReadonlyArray<AdapterEvent>) => {
      const statuses = new Map<string, string>();
      for (const event of seen) {
        if (event.type === "runtime_request.updated") {
          statuses.set(String(event.runtimeRequest.id), event.runtimeRequest.status);
        }
      }
      return statuses;
    };

    const requestDecisions = (seen: ReadonlyArray<AdapterEvent>) =>
      seen.flatMap((event) =>
        event.type === "runtime_request.updated" && event.runtimeRequest.status === "resolved"
          ? [event.runtimeRequest.decision]
          : [],
      );

    /**
     * Runs the shipped bridge's `tool_call` hook in the runtime mode the adapter
     * launched Prime with. Its `ctx.ui.confirm` travels over FakePrime's stdout
     * the way Prime's RPC UI context sends it, and resolves from the adapter's
     * `extension_ui_response` (modes/rpc/rpc-extension-ui-context.ts).
     */
    const runBridgeToolCall = (fake: FakePrime, toolName: string, input: unknown) =>
      Effect.gen(function* () {
        const runtimeMode = fake.lastSpawn().env.T3_PRIME_RUNTIME_MODE;
        const bridge = yield* Effect.promise(() =>
          loadMcpBridge({
            ...(runtimeMode === undefined ? {} : { runtimeMode }),
            unreachable: true,
          }),
        );
        const hook = bridge.handlers.get("tool_call") as unknown as BridgeToolCallHook;
        const dialogs: Array<{ readonly id: string; readonly response: PrimeRpcRecord }> = [];
        const titles: Array<string> = [];
        const result = yield* Effect.promise(() =>
          hook(
            { toolName, input },
            {
              ui: {
                confirm: (title, message) => {
                  titles.push(title);
                  const id = `dlg-${titles.length}`;
                  return Effect.runPromise(
                    Effect.gen(function* () {
                      yield* fake.emit({
                        type: "extension_ui_request",
                        id,
                        method: "confirm",
                        title,
                        message,
                      });
                      const response = yield* fake.takeRequest("extension_ui_response");
                      dialogs.push({ id, response });
                      return response["cancelled"] !== true && response["confirmed"] === true;
                    }),
                  );
                },
              },
            },
          ),
        );
        return { result, titles, dialogs };
      });

    const ipythonCell = (fake: FakePrime, toolCallId: string, code: string, blockedBy?: string) =>
      Effect.gen(function* () {
        yield* fake.emit({
          type: "tool_execution_start",
          toolCallId,
          toolName: "ipython",
          args: { code },
        });
        yield* fake.emit({
          type: "tool_execution_end",
          toolCallId,
          toolName: "ipython",
          isError: blockedBy !== undefined,
          result:
            blockedBy === undefined
              ? {
                  content: [{ type: "text", text: "approved\n" }],
                  details: { status: "ok", stdout: "approved\n" },
                }
              : { content: [{ type: "text", text: blockedBy }], details: {} },
        });
      });

    const finalCommandStatus = (seen: ReadonlyArray<AdapterEvent>, toolCallId: string) =>
      seen
        .flatMap((event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "command_execution" &&
          event.turnItem.nativeItemRef?.nativeId === toolCallId
            ? [event.turnItem.status]
            : [],
        )
        .at(-1);

    it.effect.each(["approval-required", "auto-accept-edits"] as const)(
      "[VAL-REQ-001] APPROVE_ME in %s: the bridge's ipython confirm becomes one pending approval, accept answers {confirmed: true} with the same id, the cell runs, and the turn completes once",
      (runtimeMode) =>
        Effect.gen(function* () {
          const { fake, runtime, seen, record, begin } = yield* openTurn(undefined, runtimeMode);
          assert.equal(fake.lastSpawn().env.T3_PRIME_RUNTIME_MODE, runtimeMode);
          yield* begin("APPROVE_ME");
          yield* fake.emit({ type: "agent_start" });
          const gate = yield* runBridgeToolCall(fake, "ipython", {
            code: "print('approved')",
          }).pipe(Effect.forkScoped);

          const request = yield* takePendingRequest(record);
          assert.equal(request.kind, "command");
          assert.equal(request.nativeRequestRef?.nativeId, "dlg-1");
          assert.isNotNull(request.providerTurnId);
          const approval = yield* record(
            (event) =>
              event.type === "turn_item.updated" && event.turnItem.type === "approval_request",
          );
          assert.isTrue(
            approval.type === "turn_item.updated" &&
              approval.turnItem.type === "approval_request" &&
              approval.turnItem.title === "Allow ipython?" &&
              approval.turnItem.requestKind === "command" &&
              approval.turnItem.status === "waiting",
          );

          yield* runtime.respondToRuntimeRequest({ requestId: request.id, decision: "accept" });
          const { result, titles, dialogs } = yield* Fiber.join(gate);
          assert.deepEqual(titles, ["Allow ipython?"]);
          assert.isUndefined(result, "the bridge lets the approved cell run");
          assert.deepEqual(dialogs[0]?.response, {
            type: "extension_ui_response",
            id: "dlg-1",
            confirmed: true,
          });
          yield* record(
            (event) =>
              event.type === "runtime_request.updated" &&
              event.runtimeRequest.id === request.id &&
              event.runtimeRequest.status === "resolved",
          );

          yield* ipythonCell(fake, "cell-1", "print('approved')");
          yield* replyText(fake, "approved");
          yield* fake.emit(agentEnd());
          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.lengthOf(terminals(seen), 1);
          assert.equal(finalCommandStatus(seen, "cell-1"), "completed");
          assert.deepEqual([...requestStatuses(seen).values()], ["resolved"]);
          assert.deepEqual(requestDecisions(seen), ["accept"]);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-REQ-001] APPROVE_ME in full-access: the bridge runs ipython without a dialog, no runtime request opens, and the turn completes",
      () =>
        Effect.gen(function* () {
          const { fake, seen, record, begin } = yield* openTurn(undefined, "full-access");
          assert.equal(fake.lastSpawn().env.T3_PRIME_RUNTIME_MODE, "full-access");
          yield* begin("APPROVE_ME");
          yield* fake.emit({ type: "agent_start" });
          const { result, titles } = yield* runBridgeToolCall(fake, "ipython", {
            code: "print('approved')",
          });
          assert.isUndefined(result);
          assert.deepEqual(titles, []);
          yield* ipythonCell(fake, "cell-1", "print('approved')");
          yield* replyText(fake, "approved");
          yield* fake.emit(agentEnd());
          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.equal(finalCommandStatus(seen, "cell-1"), "completed");
          assert.isFalse(seen.some((event) => event.type === "runtime_request.updated"));
          assert.notInclude(fake.commandLog(), "extension_ui_response");
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-REQ-001] a confirm that still arrives in full-access opens an approval as in Pi's v2 adapter, and completing the turn cancels it so none stays pending",
      () =>
        Effect.gen(function* () {
          const { fake, seen, record, begin } = yield* openTurn(undefined, "full-access");
          yield* begin();
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit({
            type: "extension_ui_request",
            id: "other-extension",
            method: "confirm",
            title: "Deploy to staging?",
            message: "A user extension asks this.",
          });
          const request = yield* takePendingRequest(record);
          assert.equal(request.kind, "command");
          yield* replyText(fake, "done");
          yield* fake.emit(agentEnd());
          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.equal(requestStatuses(seen).get(String(request.id)), "cancelled");
          const cancelledAt = seen.findIndex(
            (event) =>
              event.type === "runtime_request.updated" &&
              event.runtimeRequest.id === request.id &&
              event.runtimeRequest.status === "cancelled",
          );
          assert.isBelow(cancelledAt, seen.indexOf(terminal));
          const response = yield* fake.takeRequest("extension_ui_response");
          assert.deepEqual(response, {
            type: "extension_ui_response",
            id: "other-extension",
            cancelled: true,
          });
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-REQ-002] declining answers {confirmed: false} with the same id; the bridge blocks the cell and the turn continues and settles once",
      () =>
        Effect.gen(function* () {
          const { fake, runtime, seen, record, begin } = yield* openTurn(
            undefined,
            "approval-required",
          );
          yield* begin("APPROVE_ME");
          yield* fake.emit({ type: "agent_start" });
          const gate = yield* runBridgeToolCall(fake, "ipython", {
            code: "import shutil; shutil.rmtree('build')",
          }).pipe(Effect.forkScoped);
          const request = yield* takePendingRequest(record);
          yield* runtime.respondToRuntimeRequest({ requestId: request.id, decision: "decline" });
          const { result, dialogs } = yield* Fiber.join(gate);
          assert.deepEqual(dialogs[0]?.response, {
            type: "extension_ui_response",
            id: "dlg-1",
            confirmed: false,
          });
          assert.deepEqual(result, { block: true, reason: "ipython was declined in T3 Code." });
          yield* ipythonCell(fake, "cell-1", "import shutil", result?.reason);
          yield* replyText(fake, "The cell was declined, so nothing ran.");
          yield* fake.emit(agentEnd());
          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.lengthOf(terminals(seen), 1);
          assert.equal(finalCommandStatus(seen, "cell-1"), "failed");
          assert.isTrue(seen.some(isAssistantText("The cell was declined, so nothing ran.")));
          assert.deepEqual([...requestStatuses(seen).values()], ["resolved"]);
          assert.deepEqual(requestDecisions(seen), ["decline"]);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect.each([
      {
        method: "select",
        dialog: { title: "Deploy where?", options: ["staging", "production"] },
        answer: "production",
      },
      {
        method: "input",
        dialog: { title: "Release name", placeholder: "v1.2.3" },
        answer: "v2.0.0",
      },
      {
        method: "editor",
        dialog: { title: "Commit message", prefill: "fix: draft" },
        answer: "fix: final message\n",
      },
    ])(
      "[VAL-REQ-003] $method becomes a user-input request; an answer returns {value} and a cancel returns {cancelled: true}",
      ({ method, dialog, answer }) =>
        Effect.gen(function* () {
          const { fake, runtime, seen, record, begin } = yield* openTurn(
            undefined,
            "approval-required",
          );
          yield* begin();
          yield* fake.emit({ type: "agent_start" });

          yield* fake.emit({ type: "extension_ui_request", id: `${method}-1`, method, ...dialog });
          const request = yield* takePendingRequest(record);
          assert.equal(request.kind, "user_input");
          const item = yield* record(
            (event) =>
              event.type === "turn_item.updated" && event.turnItem.type === "user_input_request",
          );
          assert.isTrue(
            item.type === "turn_item.updated" && item.turnItem.type === "user_input_request",
          );
          if (item.type !== "turn_item.updated" || item.turnItem.type !== "user_input_request")
            return;
          const question = item.turnItem.questions[0];
          assert.equal(question?.id, `${method}-1`);
          assert.equal(question?.header, dialog.title);
          if (method === "select") {
            assert.deepEqual(
              question?.options.map((option) => option.value),
              ["staging", "production"],
              "Prime's option strings are the answer values",
            );
          }
          yield* runtime.respondToRuntimeRequest({
            requestId: request.id,
            answers: { [`${method}-1`]: answer },
          });
          assert.deepEqual(yield* fake.takeRequest("extension_ui_response"), {
            type: "extension_ui_response",
            id: `${method}-1`,
            value: answer,
          });

          yield* fake.emit({ type: "extension_ui_request", id: `${method}-2`, method, ...dialog });
          const second = yield* takePendingRequest(record);
          yield* runtime.respondToRuntimeRequest({ requestId: second.id, decision: "cancel" });
          assert.deepEqual(yield* fake.takeRequest("extension_ui_response"), {
            type: "extension_ui_response",
            id: `${method}-2`,
            cancelled: true,
          });

          yield* replyText(fake, "answered");
          yield* fake.emit(agentEnd());
          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.notInclude([...requestStatuses(seen).values()], "pending");
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect.each([{ path: "interrupt" }, { path: "failure" }, { path: "process exit" }] as const)(
      "[VAL-REQ-004] $path cancels every open request before turn.terminal and leaves none pending",
      ({ path }) =>
        Effect.gen(function* () {
          const { fake, runtime, providerThread, seen, record, begin } = yield* openTurn(
            undefined,
            "approval-required",
          );
          yield* begin();
          const running = yield* record(
            (event) =>
              event.type === "provider_turn.updated" && event.providerTurn.status === "running",
          );
          const providerTurnId =
            running.type === "provider_turn.updated" ? running.providerTurn.id : undefined;
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit({
            type: "extension_ui_request",
            id: "open-confirm",
            method: "confirm",
            title: "Allow ipython?",
            message: "{}",
          });
          const confirm = yield* takePendingRequest(record);
          yield* fake.emit({
            type: "extension_ui_request",
            id: "open-select",
            method: "select",
            title: "Pick one",
            options: ["a", "b"],
          });
          const select = yield* takePendingRequest(record);

          if (path === "interrupt") {
            yield* runtime.interruptTurn({ providerThread, providerTurnId: providerTurnId! });
            yield* fake.takeRequest("abort");
            yield* fake.emit(agentEnd(true));
          } else if (path === "failure") {
            yield* fake.emit({
              type: "agent_end",
              messages: [
                { role: "assistant", content: [], stopReason: "error", errorMessage: "boom" },
              ],
            });
          } else {
            yield* fake.exit(1);
          }
          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(
            terminal.type === "turn.terminal" &&
              terminal.status === (path === "interrupt" ? "interrupted" : "failed"),
          );
          const statuses = requestStatuses(seen);
          assert.equal(statuses.get(String(confirm.id)), "cancelled");
          assert.equal(statuses.get(String(select.id)), "cancelled");
          const terminalIndex = seen.indexOf(terminal);
          for (const id of [confirm.id, select.id]) {
            const cancelledAt = seen.findIndex(
              (event) =>
                event.type === "runtime_request.updated" &&
                event.runtimeRequest.id === id &&
                event.runtimeRequest.status === "cancelled",
            );
            assert.isBelow(cancelledAt, terminalIndex);
          }
          const requestItemStatuses = new Map<string, string>();
          for (const event of seen) {
            if (
              event.type === "turn_item.updated" &&
              (event.turnItem.type === "approval_request" ||
                event.turnItem.type === "user_input_request")
            ) {
              requestItemStatuses.set(String(event.turnItem.id), event.turnItem.status);
            }
          }
          assert.deepEqual([...requestItemStatuses.values()], ["cancelled", "cancelled"]);
          const late = yield* Effect.result(
            runtime.respondToRuntimeRequest({ requestId: confirm.id, decision: "accept" }),
          );
          assert.equal(late._tag, "Failure", "a closed request cannot be answered");
          if (path !== "process exit") {
            const cancelled = fake
              .allRequests()
              .filter((request) => request["type"] === "extension_ui_response");
            assert.deepEqual(
              cancelled.map((request) => [request["id"], request["cancelled"]]),
              [
                ["open-confirm", true],
                ["open-select", true],
              ],
            );
          }
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });
  describe("session commands", () => {
    const beginCommand = Effect.fnUntraced(function* (
      open: {
        readonly fake: FakePrime;
        readonly runtime: Parameters<typeof startTurn>[0];
        readonly providerThread: Parameters<typeof startTurn>[1];
      },
      text: string,
    ) {
      yield* startTurn(open.runtime, open.providerThread, "default", [], text);
      const prompt = yield* open.fake.takeRequest("prompt");
      assert.equal(prompt["message"], text);
      return prompt;
    });

    const isTerminal = (event: AdapterEvent) => event.type === "turn.terminal";
    const itemsMentioning = (seen: ReadonlyArray<AdapterEvent>, text: string) =>
      seen.filter(
        (event) =>
          event.type === "turn_item.updated" && JSON.stringify(event.turnItem).includes(text),
      );
    const idleGoal = {
      active: false,
      status: "idle",
      tokensUsed: 0,
      timeUsedSeconds: 0,
      continuationsUsed: 0,
    };

    it.effect(
      "[VAL-CMD-005] /goal status settles once after its action clears, in the live-probed frame order, with no agent_end and no item for the echo",
      () =>
        Effect.gen(function* () {
          const open = yield* openTurn();
          const { fake, seen, record } = open;
          yield* beginCommand(open, "/goal status");
          // live-runs.md (prime-session-commands probe): ack, preparing, running,
          // echo, goal_update, result row, then the clearing update.
          yield* fake.acceptSessionCommand("/goal status");
          yield* emitCustomMessage(fake, sessionCommandEcho("/goal status"));
          yield* fake.emit({ type: "goal_update", goal: idleGoal });
          yield* emitCustomMessage(fake, sessionCommandResult("/goal status", "No active goal."));
          yield* pumpBarrier(fake, record, "before the action clears");
          assert.lengthOf(terminals(seen), 0);

          yield* fake.setSessionActions(IDLE_ACTIONS);
          const terminal = yield* record(isTerminal);
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.lengthOf(terminals(seen), 1);
          assert.notInclude(fake.commandLog(), "abort");
          assert.lengthOf(itemsMentioning(seen, "/goal status"), 0);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-CMD-005] /goal status settles once when Prime finished it before any active action was observed",
      () =>
        Effect.gen(function* () {
          const open = yield* openTurn();
          const { fake, seen, record } = open;
          yield* beginCommand(open, "/goal status");
          yield* fake.emit({ type: "response", command: "prompt", success: true });
          yield* emitCustomMessage(fake, sessionCommandEcho("/goal status"));
          yield* fake.emit({ type: "goal_update", goal: idleGoal });
          yield* emitCustomMessage(fake, sessionCommandResult("/goal status", "No active goal."));
          const terminal = yield* record(isTerminal);
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.lengthOf(terminals(seen), 1);
          assert.lengthOf(itemsMentioning(seen, "/goal status"), 0);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect.each(["/goal pause", "/goal clear", "/reload"])(
      "[VAL-CMD-007] %s settles once on its action clearing and an idle probe",
      (text) =>
        Effect.gen(function* () {
          const open = yield* openTurn();
          const { fake, seen, record } = open;
          yield* beginCommand(open, text);
          yield* fake.acceptSessionCommand(text);
          yield* emitCustomMessage(fake, sessionCommandEcho(text));
          yield* emitCustomMessage(fake, sessionCommandResult(text, "Done."));
          yield* pumpBarrier(fake, record, "before the action clears");
          assert.lengthOf(terminals(seen), 0);
          yield* fake.setSessionActions(IDLE_ACTIONS);
          const terminal = yield* record(isTerminal);
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.lengthOf(terminals(seen), 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-CMD-002] /compact goes to Prime as a prompt and settles once after compaction ends and its action clears",
      () =>
        Effect.gen(function* () {
          const open = yield* openTurn();
          const { fake, seen, record } = open;
          yield* beginCommand(open, "/compact keep the tests");
          assert.notInclude(fake.commandLog(), "compact");
          yield* fake.acceptSessionCommand("/compact keep the tests");
          yield* emitCustomMessage(fake, sessionCommandEcho("/compact keep the tests"));
          fake.setCompacting(true);
          yield* fake.emit({ type: "compaction_start", reason: "manual" });
          yield* record(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "compaction" &&
              event.turnItem.status === "running",
          );
          fake.setCompacting(false);
          yield* fake.emit({
            type: "compaction_end",
            reason: "manual",
            result: { summary: "smaller", tokensBefore: 10_000, estimatedTokensAfter: 2_000 },
            aborted: false,
            willRetry: false,
          });
          yield* record(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "compaction" &&
              event.turnItem.status === "completed",
          );
          yield* pumpBarrier(fake, record, "after compaction_end");
          assert.lengthOf(terminals(seen), 0);

          yield* fake.setSessionActions(IDLE_ACTIONS);
          const terminal = yield* record(isTerminal);
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.lengthOf(terminals(seen), 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect.each([
      { outcome: "complete", status: "completed" },
      { outcome: "failed", status: "failed" },
    ] as const)(
      "[VAL-CMD-003] /refine that ends with refine_$outcome settles once as $status",
      ({ outcome, status }) =>
        Effect.gen(function* () {
          const open = yield* openTurn();
          const { fake, seen, record } = open;
          yield* beginCommand(open, "/refine");
          yield* fake.acceptSessionCommand("/refine");
          yield* emitCustomMessage(fake, sessionCommandEcho("/refine"));
          if (outcome === "complete") {
            yield* fake.emit({
              type: "refine_complete",
              result: { appliedEdits: [{ applied: true }] },
            });
            yield* emitCustomMessage(
              fake,
              sessionCommandResult(
                "/refine",
                "Refined continual harness state: 1 edit applied.",
                true,
                false,
              ),
            );
          } else {
            yield* fake.emit({ type: "refine_failed", error: "No refinement model configured" });
            yield* emitCustomMessage(
              fake,
              sessionCommandResult(
                "/refine",
                "Command failed: No refinement model configured",
                false,
              ),
            );
          }
          yield* pumpBarrier(fake, record, "before the action clears");
          assert.lengthOf(terminals(seen), 0);

          yield* fake.setSessionActions(IDLE_ACTIONS);
          const terminal = yield* record(isTerminal);
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === status);
          if (terminal.type === "turn.terminal" && terminal.status === "failed") {
            assert.equal(terminal.failure.message, "No refinement model configured");
          }
          assert.lengthOf(terminals(seen), 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-CMD-003] a refine_failed from a self-started refinement does not fail an ordinary turn",
      () =>
        Effect.gen(function* () {
          const { fake, seen, record, begin } = yield* openTurn();
          yield* begin();
          yield* fake.emit({ type: "agent_start" });
          yield* replyText(fake, "Plain reply");
          yield* fake.emit({ type: "refine_failed", error: "Refinement skipped by extension" });
          yield* fake.emit(agentEnd());
          const terminal = yield* record(isTerminal);
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.lengthOf(terminals(seen), 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    const steerOrdinaryTurn = Effect.fnUntraced(function* (
      open: Effect.Success<ReturnType<typeof openTurn>>,
      text: string,
      outcome: "accepted" | "rejected" = "accepted",
    ) {
      const { fake } = open;
      yield* open.begin();
      const running = yield* open.record(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      assert.isTrue(running.type === "provider_turn.updated");
      yield* fake.emit({ type: "agent_start" });
      yield* replyText(fake, "Plain reply");
      yield* open.record(isAssistantText("Plain reply"));
      yield* open.runtime.steerTurn({
        threadId: THREAD_ID,
        runId: RunId.make("run:thread-prime-test:1"),
        providerThread: open.providerThread,
        providerTurnId: (running as Extract<AdapterEvent, { type: "provider_turn.updated" }>)
          .providerTurn.id,
        message: {
          messageId: `message:thread-prime-test:steer:${text}` as never,
          text,
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
      });
      const steer = yield* fake.takeRequest("prompt");
      assert.equal(steer["message"], text);
      assert.equal(steer["streamingBehavior"], "steer");
      if (outcome === "rejected") {
        yield* fake.emit({
          type: "response",
          command: "prompt",
          success: false,
          error: "Steering message rejected",
        });
        return;
      }
      // Same order as the steered /compact probe: ack, then the queued command's action.
      yield* fake.emit({ type: "response", command: "prompt", success: true });
      yield* fake.setSessionActions({
        ...IDLE_ACTIONS,
        active: { kind: "session_command", phase: "running", label: text },
      });
      yield* fake.emit(agentEnd());
      yield* emitCustomMessage(fake, sessionCommandEcho(text));
    });

    it.effect(
      "[VAL-CMD-003] an ordinary turn steered with /refine that ends in refine_failed settles once as failed with the error",
      () =>
        Effect.gen(function* () {
          const open = yield* openTurn();
          const { fake, seen, record } = open;
          yield* steerOrdinaryTurn(open, "/refine rollback");
          yield* fake.emit({ type: "refine_failed", error: "Rollback requires a refinement id" });
          yield* emitCustomMessage(
            fake,
            sessionCommandResult(
              "/refine rollback",
              "Command failed: Rollback requires a refinement id",
              false,
            ),
          );
          yield* pumpBarrier(fake, record, "before the action clears");
          assert.lengthOf(terminals(seen), 0);

          yield* fake.setSessionActions(IDLE_ACTIONS);
          const terminal = yield* record(isTerminal);
          assert.isTrue(
            terminal.type === "turn.terminal" &&
              terminal.status === "failed" &&
              terminal.failure.message === "Rollback requires a refinement id",
          );
          assert.lengthOf(terminals(seen), 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-CMD-003] an ordinary turn steered with a successful /refine completes", () =>
      Effect.gen(function* () {
        const open = yield* openTurn();
        const { fake, seen, record } = open;
        yield* steerOrdinaryTurn(open, "/refine");
        yield* fake.emit({
          type: "refine_complete",
          result: { appliedEdits: [{ applied: true }] },
        });
        yield* emitCustomMessage(
          fake,
          sessionCommandResult(
            "/refine",
            "Refined continual harness state: 1 edit applied.",
            true,
            false,
          ),
        );
        yield* fake.setSessionActions(IDLE_ACTIONS);
        const terminal = yield* record(isTerminal);
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
        assert.lengthOf(terminals(seen), 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-CMD-003] a /refine steer that Prime rejects does not make a later self-started refine_failed fail the turn",
      () =>
        Effect.gen(function* () {
          const open = yield* openTurn();
          const { fake, seen, record } = open;
          yield* steerOrdinaryTurn(open, "/refine", "rejected");
          yield* fake.emit({ type: "refine_failed", error: "Refinement skipped by extension" });
          yield* fake.emit(agentEnd());
          const terminal = yield* record(isTerminal);
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.lengthOf(terminals(seen), 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-CMD-007] an ordinary turn steered with a session command whose result row fails settles once as failed",
      () =>
        Effect.gen(function* () {
          const open = yield* openTurn();
          const { fake, seen, record } = open;
          yield* steerOrdinaryTurn(open, "/reload");
          yield* emitCustomMessage(
            fake,
            sessionCommandResult(
              "/reload",
              "Command failed: Wait for compaction to finish before reloading.",
              false,
            ),
          );
          yield* pumpBarrier(fake, record, "before the action clears");
          assert.lengthOf(terminals(seen), 0);

          yield* fake.setSessionActions(IDLE_ACTIONS);
          const terminal = yield* record(isTerminal);
          assert.isTrue(
            terminal.type === "turn.terminal" &&
              terminal.status === "failed" &&
              terminal.failure.message === "Wait for compaction to finish before reloading.",
          );
          assert.lengthOf(terminals(seen), 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-CMD-004] /goal <objective> stays open across agent_end while the goal action is active and settles once after it clears",
      () =>
        Effect.gen(function* () {
          const open = yield* openTurn();
          const { fake, seen, record } = open;
          yield* beginCommand(open, "/goal ship the release");
          yield* fake.acceptSessionCommand("/goal ship the release");
          yield* emitCustomMessage(fake, sessionCommandEcho("/goal ship the release"));
          yield* fake.emit({
            type: "goal_update",
            goal: { ...idleGoal, active: true, status: "active", objective: "ship the release" },
          });
          yield* fake.emit({ type: "agent_start" });
          yield* replyText(fake, "First goal step");
          yield* fake.emit(agentEnd());
          yield* fake.emit({ type: "agent_start" });
          yield* replyText(fake, "Goal reached");
          yield* fake.emit(agentEnd());
          yield* record(isAssistantText("Goal reached"));
          yield* pumpBarrier(fake, record, "after the goal's agent_end");
          assert.lengthOf(terminals(seen), 0);

          yield* emitCustomMessage(
            fake,
            sessionCommandResult("/goal ship the release", "Goal complete: ship the release"),
          );
          yield* fake.setSessionActions(IDLE_ACTIONS);
          const terminal = yield* record(isTerminal);
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.lengthOf(terminals(seen), 1);
          assert.isTrue(
            seen.findIndex(isAssistantText("First goal step")) < seen.indexOf(terminal),
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect.each(["/autonomous on --max-turns 3", "/autonomous off", "/autonomous status"])(
      "[VAL-CMD-006] %s keeps the turn open while its action is active and settles once as completed",
      (text) =>
        Effect.gen(function* () {
          const open = yield* openTurn();
          const { fake, seen, record } = open;
          yield* beginCommand(open, text);
          yield* fake.acceptSessionCommand(text);
          yield* emitCustomMessage(fake, sessionCommandEcho(text));
          // Live probe: /autonomous reports through its own custom message, no result row.
          yield* emitCustomMessage(fake, {
            role: "custom",
            customType: "autonomous_status",
            content: `[autonomous-status: ${text.includes("on") ? "on" : "off"}]`,
            display: true,
          });
          yield* pumpBarrier(fake, record, "before the action clears");
          assert.lengthOf(terminals(seen), 0);
          yield* fake.setSessionActions(IDLE_ACTIONS);
          const terminal = yield* record(isTerminal);
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.lengthOf(terminals(seen), 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-CMD-007] a failed session command result fails its turn with Prime's error once the action clears",
      () =>
        Effect.gen(function* () {
          const open = yield* openTurn();
          const { fake, seen, record } = open;
          yield* beginCommand(open, "/reload");
          yield* fake.acceptSessionCommand("/reload");
          yield* emitCustomMessage(fake, sessionCommandEcho("/reload"));
          yield* emitCustomMessage(
            fake,
            sessionCommandResult(
              "/reload",
              "Command failed: Wait for compaction to finish before reloading.",
              false,
            ),
          );
          yield* fake.setSessionActions(IDLE_ACTIONS);
          const terminal = yield* record(isTerminal);
          assert.isTrue(
            terminal.type === "turn.terminal" &&
              terminal.status === "failed" &&
              terminal.failure.message === "Wait for compaction to finish before reloading.",
          );
          assert.lengthOf(terminals(seen), 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-CMD-008] an unknown slash command fails the turn with Prime's message", () =>
      Effect.gen(function* () {
        const open = yield* openTurn();
        const { fake, seen, record } = open;
        const prompt = yield* beginCommand(open, "/goa");
        const error = "Unknown command: /goa. Did you mean /goal?";
        yield* fake.emit({
          type: "response",
          ...(prompt["id"] === undefined ? {} : { id: prompt["id"] }),
          command: "prompt",
          success: false,
          error,
        });
        const terminal = yield* record(isTerminal);
        assert.isTrue(
          terminal.type === "turn.terminal" &&
            terminal.status === "failed" &&
            terminal.failure.message === error,
        );
        assert.lengthOf(terminals(seen), 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("continuations, goals, and wakes", () => {
    type Offer = ProviderContinuationRequests.ProviderContinuationRequest;
    type Terminal = Extract<AdapterEvent, { type: "turn.terminal" }>;
    type Open = Effect.Success<ReturnType<typeof openTurn>>;

    const WAKE_FAILURE = "Mistral API error (400): mock wake failure";
    const AUTONOMOUS_CONTINUATION_PROMPT =
      "[autonomous-continuation]\n\nNo human input is available in autonomous mode. Continue working until the host evaluator, verifier, or configured autonomous limits stop the run.";
    const GOAL_COMMAND = "/goal ship the release";
    const primeGoal = (status: string) => ({
      active: status === "active",
      status,
      objective: "ship the release",
      tokensUsed: 1_200,
      tokenBudget: 50_000,
      timeUsedSeconds: 42,
      continuationsUsed: 1,
    });
    const IDLE_GOAL = {
      active: false,
      status: "idle",
      tokensUsed: 0,
      timeUsedSeconds: 0,
      continuationsUsed: 0,
    };
    /** Prime's heartbeat injection (core/messages.ts createHeartbeatPromptMessage). */
    const heartbeatPrompt = {
      role: "custom",
      customType: "heartbeat_prompt",
      content: "[heartbeat: */5 * * * * run#3]\n\nCheck the build",
      display: true,
      details: { jobId: "job-heartbeat-1", schedule: "*/5 * * * *", status: "active" },
    };
    const cronJob = (status: string) => ({
      id: `job-${status}`,
      status,
      prompt: "Check the build",
      schedule: { kind: "cron", expression: "*/5 * * * *" },
      runCount: 0,
    });

    const openWake = Effect.fnUntraced(function* (configure?: (fake: FakePrime) => void) {
      const offers = yield* Queue.unbounded<Offer>();
      const open = yield* openTurn(configure, "full-access", {
        offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
      });
      /** The orchestrator's continuation turn carries a provider-created message. */
      const adopt = (runOrdinal: number) =>
        startTurn(
          open.runtime,
          open.providerThread,
          "default",
          [],
          "Continue",
          undefined,
          runOrdinal,
          THREAD_ID,
          true,
        );
      const takeOffer = Effect.gen(function* () {
        const offer = yield* Queue.take(offers);
        assert.equal(offer.delivery, "adapter_buffered");
        assert.isNull(offer.detail);
        assert.equal(offer.threadId, THREAD_ID);
        assert.equal(offer.providerThreadId, open.providerThread.id);
        let dispatched = false;
        yield* offer.dispatchIfCurrent!(
          Effect.sync(() => {
            dispatched = true;
          }),
        );
        assert.isTrue(dispatched);
        return offer;
      });
      return { ...open, offers, adopt, takeOffer };
    });

    const nextTerminal = (open: Pick<Open, "record">) =>
      Effect.map(
        open.record((event) => event.type === "turn.terminal"),
        (event) => event as Terminal,
      );

    const plainTurn = (open: Open, reply: string) =>
      Effect.gen(function* () {
        yield* open.begin();
        yield* open.fake.emit({ type: "agent_start" });
        yield* replyText(open.fake, reply);
        yield* open.fake.emit(agentEnd());
        const terminal = yield* nextTerminal(open);
        assert.equal(terminal.status, "completed");
        return terminal;
      });

    /** Prime finishing a buffered run reports the session ready. */
    const bufferedRunEnded = (open: Pick<Open, "record">) =>
      open.record(
        (event) =>
          event.type === "provider_session.updated" && event.providerSession.status === "ready",
      );

    const assistantTexts = (seen: ReadonlyArray<AdapterEvent>, providerTurnId: unknown) => [
      ...new Set(
        seen.flatMap((event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "assistant_message" &&
          event.turnItem.streaming === false &&
          event.turnItem.providerTurnId === providerTurnId
            ? [event.turnItem.text]
            : [],
        ),
      ),
    ];
    const promptCount = (fake: FakePrime) =>
      fake.commandLog().filter((type) => type === "prompt").length;
    const goalOf = (event: AdapterEvent) =>
      event.type === "provider_thread.updated" ? event.providerThread.goal : undefined;
    const assertNeverPromoted = (fake: FakePrime) => {
      assert.notInclude(fake.commandLog(), "add_schedule");
      assert.notInclude(fake.commandLog(), "set_heartbeat");
    };

    /** A `/goal <objective>` turn whose goal action is running and whose first run is underway. */
    const beginGoal = (open: Open) =>
      Effect.gen(function* () {
        yield* open.begin(GOAL_COMMAND);
        const providerTurnId = yield* runningTurnId(open.record);
        yield* open.fake.setSessionActions({
          ...IDLE_ACTIONS,
          active: { kind: "session_command", phase: "running", label: GOAL_COMMAND },
        });
        yield* emitCustomMessage(open.fake, sessionCommandEcho(GOAL_COMMAND));
        yield* open.fake.emit({ type: "goal_update", goal: primeGoal("active") });
        yield* open.fake.emit({ type: "agent_start" });
        return providerTurnId;
      });

    it.effect(
      "[VAL-CONT-001] WAKE_LATER: a self-started run after settle is buffered, offered adapter_buffered, and adopted without a prompt as one completed turn",
      () =>
        Effect.gen(function* () {
          const open = yield* openWake();
          const { fake, runtime, seen } = open;
          yield* plainTurn(open, "mock reply");
          assert.isFalse(yield* runtime.hasPendingBackgroundWork!);

          yield* fake.emit({ type: "agent_start" });
          yield* open.takeOffer;
          assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
          yield* replyText(fake, "wake reply");
          yield* fake.emit(agentEnd());
          yield* bufferedRunEnded(open);
          assert.lengthOf(terminals(seen), 1);

          const prompts = promptCount(fake);
          yield* open.adopt(2);
          const wake = yield* nextTerminal(open);
          assert.equal(wake.status, "completed");
          assert.deepEqual(assistantTexts(seen, wake.providerTurnId), ["wake reply"]);
          assert.equal(promptCount(fake), prompts);
          assert.lengthOf(terminals(seen), 2);
          assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-CONT-002] WAKE_JOIN: a user message during the adopted wake is a steer into it, two turns in total",
      () =>
        Effect.gen(function* () {
          const open = yield* openWake();
          const { fake, runtime, providerThread, seen, record } = open;
          yield* plainTurn(open, "mock reply");
          yield* fake.emit({ type: "agent_start" });
          yield* open.takeOffer;
          yield* replyText(fake, "wake reply");

          yield* open.adopt(2);
          const wakeTurnId = yield* runningTurnId(record);
          yield* record(isAssistantText("wake reply"));
          yield* runtime.steerTurn({
            threadId: THREAD_ID,
            runId: RunId.make("run:thread-prime-test:2"),
            providerThread,
            providerTurnId: wakeTurnId,
            message: steerMessage("join this"),
          });
          const steer = yield* fake.takeRequest("prompt");
          assert.equal(steer["message"], "join this");
          assert.equal(steer["streamingBehavior"], "steer");
          yield* fake.emit({ type: "response", command: "prompt", success: true });
          yield* replyText(fake, "steered: join this");
          yield* fake.emit(agentEnd());

          const wake = yield* nextTerminal(open);
          assert.isTrue(wake.status === "completed" && wake.providerTurnId === wakeTurnId);
          assert.deepEqual(assistantTexts(seen, wakeTurnId), ["wake reply", "steered: join this"]);
          assert.lengthOf(terminals(seen), 2);
          assert.equal(promptCount(fake), 2);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-CONT-003] WAKE_CONT: a wake started by an isAutonomousContinuation message is a provider-created turn with only the wake reply",
      () =>
        Effect.gen(function* () {
          const open = yield* openWake();
          const { fake, seen } = open;
          yield* plainTurn(open, "mock reply");
          yield* fake.emit({ type: "agent_start" });
          yield* open.takeOffer;
          const continuation = {
            role: "user",
            content: [{ type: "text", text: AUTONOMOUS_CONTINUATION_PROMPT }],
            isAutonomousContinuation: true,
          };
          yield* fake.emit({ type: "message_start", message: continuation });
          yield* fake.emit({ type: "message_end", message: continuation });
          yield* replyText(fake, "wake reply");
          yield* fake.emit(agentEnd());
          yield* bufferedRunEnded(open);

          yield* open.adopt(2);
          const wake = yield* nextTerminal(open);
          assert.equal(wake.status, "completed");
          assert.deepEqual(assistantTexts(seen, wake.providerTurnId), ["wake reply"]);
          assert.equal(promptCount(fake), 1);
          assert.isFalse(
            seen.some(
              (event) => event.type === "message.updated" && event.message.createdBy === "user",
            ),
            "the autonomous continuation became a user bubble",
          );
          const continuationItem = seen.findLast(
            (event) => event.type === "turn_item.updated" && event.turnItem.type === "user_message",
          );
          assert.isTrue(
            continuationItem?.type === "turn_item.updated" &&
              continuationItem.turnItem.type === "user_message" &&
              continuationItem.turnItem.providerTurnId === wake.providerTurnId &&
              continuationItem.turnItem.text === AUTONOMOUS_CONTINUATION_PROMPT,
            "the replayed continuation is a provider-created item on the wake turn",
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-CONT-004] WAKE_FAIL: a wake whose agent_end carries an errorMessage fails its continuation turn with that exact text",
      () =>
        Effect.gen(function* () {
          const open = yield* openWake();
          const { fake, seen } = open;
          yield* plainTurn(open, "mock reply");
          yield* fake.emit({ type: "agent_start" });
          yield* open.takeOffer;
          yield* replyText(fake, "wake reply");
          yield* fake.emit({
            type: "agent_end",
            messages: [
              { role: "assistant", content: [], stopReason: "error", errorMessage: WAKE_FAILURE },
            ],
          });
          yield* bufferedRunEnded(open);

          yield* open.adopt(2);
          const wake = yield* nextTerminal(open);
          assert.isTrue(wake.status === "failed" && wake.failure.message === WAKE_FAILURE);
          assert.lengthOf(terminals(seen), 2);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-CONT-005] goal_update inside a run sets providerThread.goal with the mapped status, and thread registration re-sends the latest goal",
      () =>
        Effect.gen(function* () {
          const open = yield* openTurn();
          const { fake, runtime, providerThread, seen, record } = open;
          assert.isNull(providerThread.goal ?? null);
          yield* open.begin();
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit({ type: "goal_update", goal: primeGoal("active") });
          const active = yield* record((event) => goalOf(event)?.status === "active");
          assert.deepEqual(goalOf(active), {
            objective: "ship the release",
            status: "active",
            tokensUsed: 1_200,
            tokenBudget: 50_000,
            timeUsedSeconds: 42,
          });
          yield* fake.emit({ type: "goal_update", goal: primeGoal("error") });
          yield* record((event) => goalOf(event)?.status === "blocked");
          yield* replyText(fake, "Blocked on credentials");
          yield* fake.emit(agentEnd());
          yield* nextTerminal(open);
          const threadUpdates = seen.filter((event) => event.type === "provider_thread.updated");
          assert.equal(goalOf(threadUpdates.at(-1)!)?.status, "blocked");

          // Outside a run the orchestrator drops thread updates; registration re-sends.
          yield* fake.emit({ type: "goal_update", goal: primeGoal("complete") });
          fake.queueState({ goal: primeGoal("complete") });
          const resumed = yield* runtime.resumeThread({ providerThread });
          assert.equal(resumed.goal?.status, "complete");
          const published = yield* record(
            (event) =>
              event.type === "provider_thread.updated" &&
              event.providerThread.goal?.status === "complete",
          );
          assert.equal(goalOf(published)?.objective, "ship the release");
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-CONT-006] a held goal continuation that starts before the turn settles stays in the same turn, which settles once when the goal is idle",
      () =>
        Effect.gen(function* () {
          const open = yield* openWake();
          const { fake, seen, record } = open;
          const providerTurnId = yield* beginGoal(open);
          yield* replyText(fake, "First goal step");
          yield* fake.emit(agentEnd());
          fake.deferNextState();
          yield* fake.setSessionActions(IDLE_ACTIONS);
          yield* fake.takeRequest("get_state");

          // Prime held the goal's next step and starts it as a new run.
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit({ type: "message_start", message: { role: "assistant" } });
          yield* pumpBarrier(fake, record, "held goal run started");
          yield* fake.resolveDeferredState(recordedIdleState(FAKE_SESSION_FILE));
          yield* replyText(fake, "Goal reached");
          yield* fake.emit({ type: "goal_update", goal: primeGoal("complete") });
          yield* fake.emit(agentEnd());

          const terminal = yield* nextTerminal(open);
          assert.isTrue(
            terminal.status === "completed" && terminal.providerTurnId === providerTurnId,
          );
          assert.deepEqual(assistantTexts(seen, providerTurnId), [
            "First goal step",
            "Goal reached",
          ]);
          assert.lengthOf(terminals(seen), 1);
          assert.equal(yield* Queue.size(open.offers), 0);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-CONT-007] a user message while a goal runs is a steer into the goal turn, not a new turn",
      () =>
        Effect.gen(function* () {
          const open = yield* openWake();
          const { fake, runtime, providerThread, seen } = open;
          const providerTurnId = yield* beginGoal(open);
          yield* replyText(fake, "Working on the goal");
          yield* fake.emit(agentEnd());

          yield* runtime.steerTurn({
            threadId: THREAD_ID,
            runId: RunId.make("run:thread-prime-test:1"),
            providerThread,
            providerTurnId,
            message: steerMessage("also update the changelog"),
          });
          const steer = yield* fake.takeRequest("prompt");
          assert.equal(steer["message"], "also update the changelog");
          assert.equal(steer["streamingBehavior"], "steer");
          yield* fake.emit({ type: "response", command: "prompt", success: true });
          yield* fake.emit({ type: "agent_start" });
          yield* replyText(fake, "Changelog updated");
          yield* fake.emit(agentEnd());
          yield* fake.setSessionActions(IDLE_ACTIONS);

          const terminal = yield* nextTerminal(open);
          assert.isTrue(
            terminal.status === "completed" && terminal.providerTurnId === providerTurnId,
          );
          assert.include(assistantTexts(seen, providerTurnId), "Changelog updated");
          assert.lengthOf(terminals(seen), 1);
          assert.equal(promptCount(fake), 2);
          assert.equal(yield* Queue.size(open.offers), 0);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-CONT-008] abort during a goal sends abort, yields one interrupted terminal, starts no turn, and mirrors the goal Prime reports afterward",
      () =>
        Effect.gen(function* () {
          const open = yield* openWake();
          const { fake, runtime, providerThread, seen } = open;
          const providerTurnId = yield* beginGoal(open);
          yield* replyText(fake, "Working on the goal");
          yield* runtime.interruptTurn({ providerThread, providerTurnId });
          yield* fake.takeRequest("abort");
          yield* fake.emit(agentEnd(true));
          const terminal = yield* nextTerminal(open);
          assert.isTrue(
            terminal.status === "interrupted" && terminal.providerTurnId === providerTurnId,
          );

          yield* fake.emit({ type: "goal_update", goal: primeGoal("paused") });
          yield* fake.setSessionActions(IDLE_ACTIONS);
          const afterAbort = fake.commandLog().length;
          assert.equal(yield* Queue.size(open.offers), 0);
          yield* open.begin("Next question", 2);
          yield* fake.emit({ type: "agent_start" });
          yield* replyText(fake, "Next answer");
          yield* fake.emit(agentEnd());
          const next = yield* nextTerminal(open);
          assert.equal(next.status, "completed");
          const threadUpdates = seen.filter((event) => event.type === "provider_thread.updated");
          assert.equal(goalOf(threadUpdates.at(-1)!)?.status, "paused");
          assert.lengthOf(terminals(seen), 2);
          assert.equal(yield* Queue.size(open.offers), 0);
          assert.deepEqual(
            fake
              .allRequests()
              .slice(afterAbort)
              .filter((request) => request["type"] === "prompt")
              .map((request) => request["message"]),
            ["Next question"],
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-CONT-009][VAL-CONT-011] a heartbeat_prompt inside an open run is a notification item in that turn, with no new turn",
      () =>
        Effect.gen(function* () {
          const open = yield* openWake();
          const { fake, seen, record } = open;
          yield* open.begin();
          const providerTurnId = yield* runningTurnId(record);
          yield* fake.emit({ type: "agent_start" });
          yield* replyText(fake, "Working");
          yield* emitCustomMessage(fake, heartbeatPrompt);
          const notification = yield* record(
            (event) => event.type === "turn_item.updated" && event.turnItem.type === "notification",
          );
          assert.isTrue(
            notification.type === "turn_item.updated" &&
              notification.turnItem.type === "notification" &&
              notification.turnItem.providerTurnId === providerTurnId &&
              notification.turnItem.source.kind === "monitor" &&
              notification.turnItem.summary === "Heartbeat */5 * * * *" &&
              notification.turnItem.detail === "Check the build",
          );
          yield* replyText(fake, "Build is green");
          yield* fake.emit(agentEnd());
          const terminal = yield* nextTerminal(open);
          assert.isTrue(
            terminal.status === "completed" && terminal.providerTurnId === providerTurnId,
          );
          assert.lengthOf(terminals(seen), 1);
          assert.equal(yield* Queue.size(open.offers), 0);
          assertNeverPromoted(fake);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect.each(["heartbeat", "cron"] as const)(
      "[VAL-CONT-009][VAL-CONT-011] a %s firing while idle starts a run that goes through the continuation offer",
      (trigger) =>
        Effect.gen(function* () {
          const open = yield* openWake();
          const { fake, seen } = open;
          yield* plainTurn(open, "mock reply");
          yield* fake.emit({ type: "agent_start" });
          yield* open.takeOffer;
          if (trigger === "heartbeat") {
            yield* emitCustomMessage(fake, heartbeatPrompt);
          } else {
            const cronPrompt = {
              role: "user",
              content: [{ type: "text", text: "Check the build" }],
            };
            yield* fake.emit({ type: "message_start", message: cronPrompt });
            yield* fake.emit({ type: "message_end", message: cronPrompt });
          }
          yield* replyText(fake, "Scheduled reply");
          yield* fake.emit(agentEnd());
          yield* bufferedRunEnded(open);

          yield* open.adopt(2);
          const wake = yield* nextTerminal(open);
          assert.equal(wake.status, "completed");
          assert.deepEqual(assistantTexts(seen, wake.providerTurnId), ["Scheduled reply"]);
          assert.equal(
            seen.some(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.type === "notification" &&
                event.turnItem.providerTurnId === wake.providerTurnId,
            ),
            trigger === "heartbeat",
          );
          assert.equal(promptCount(fake), 1);
          assertNeverPromoted(fake);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect.each([
      { reported: "an active schedule", jobs: [cronJob("active")], heartbeats: [], pinned: true },
      {
        reported: "an active heartbeat",
        jobs: [],
        heartbeats: [{ job: cronJob("active") }],
        pinned: true,
      },
      {
        reported: "only a paused heartbeat",
        jobs: [],
        heartbeats: [{ job: cronJob("paused") }],
        pinned: false,
      },
    ])(
      "[VAL-CONT-010][VAL-CONT-011] a settle whose probe reports $reported sets hasPendingBackgroundWork=$pinned, probes only after the trigger, and adds no background tasks",
      ({ jobs, heartbeats, pinned }) =>
        Effect.gen(function* () {
          const open = yield* openWake((fake) => fake.setBackgroundJobs(jobs, heartbeats));
          const { fake, runtime, providerThread, seen, record } = open;
          const isScheduleProbe = (type: string) =>
            type === "list_schedules" || type === "list_heartbeats";
          assert.isFalse(fake.commandLog().some(isScheduleProbe));

          const afterPrompt = yield* open.begin();
          yield* fake.emit({ type: "agent_start" });
          yield* replyText(fake, "Scheduled the check");
          yield* pumpBarrier(fake, record, "before agent_end");
          assert.isFalse(commandsSince(fake, afterPrompt).some(isScheduleProbe));
          assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
          yield* fake.emit(agentEnd());
          yield* nextTerminal(open);
          const probes = commandsSince(fake, afterPrompt);
          assert.deepEqual(probes.filter(isScheduleProbe).toSorted(), [
            "list_heartbeats",
            "list_schedules",
          ]);
          assert.isBelow(
            probes.indexOf("get_state"),
            Math.min(probes.indexOf("list_schedules"), probes.indexOf("list_heartbeats")),
          );
          assert.equal(yield* runtime.hasPendingBackgroundWork!, pinned);
          assert.isFalse(yield* runtime.hasPendingBackgroundWorkForThread!(providerThread));

          fake.setBackgroundJobs([], []);
          const afterSecondPrompt = yield* open.begin("Cancel the check", 2);
          yield* fake.emit({ type: "agent_start" });
          yield* replyText(fake, "Cancelled");
          yield* fake.emit(agentEnd());
          yield* nextTerminal(open);
          assert.equal(commandsSince(fake, afterSecondPrompt).filter(isScheduleProbe).length, 2);
          assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
          assert.isTrue(
            seen.every(
              (event) =>
                event.type !== "provider_thread.updated" ||
                (event.providerThread.pendingBackgroundTasks ?? []).length === 0,
            ),
          );
          assertNeverPromoted(fake);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    const isScheduleProbe = (type: string) =>
      type === "list_schedules" || type === "list_heartbeats";
    /** Output is ordered, so a duplicate terminal would be seen before the next turn starts. */
    const startNextTurn = (open: Open, runOrdinal: number) =>
      Effect.gen(function* () {
        yield* open.begin("Next question", runOrdinal);
        yield* runningTurnId(open.record);
      });

    it.effect.each([
      { created: "a schedule", jobs: [cronJob("active")], heartbeats: [] },
      { created: "a heartbeat", jobs: [], heartbeats: [{ job: cronJob("active") }] },
    ])(
      "[VAL-CONT-010] an interrupted turn that created $created reads background jobs before its single interrupted terminal, so hasPendingBackgroundWork is true",
      ({ jobs, heartbeats }) =>
        Effect.gen(function* () {
          const open = yield* openWake();
          const { fake, runtime, providerThread, seen, record } = open;
          yield* open.begin();
          const providerTurnId = yield* runningTurnId(record);
          yield* fake.emit({ type: "agent_start" });
          yield* replyText(fake, "Scheduled the check");
          fake.setBackgroundJobs(jobs, heartbeats);
          yield* pumpBarrier(fake, record, "before the interrupt");
          assert.isFalse(fake.commandLog().some(isScheduleProbe));
          assert.isFalse(yield* runtime.hasPendingBackgroundWork!);

          yield* runtime.interruptTurn({ providerThread, providerTurnId });
          yield* fake.takeRequest("abort");
          const afterAbort = fake.commandLog().length;
          yield* fake.emit(agentEnd(true));
          const terminal = yield* nextTerminal(open);
          assert.isTrue(
            terminal.status === "interrupted" && terminal.providerTurnId === providerTurnId,
          );
          assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
          assert.isFalse(yield* runtime.hasPendingBackgroundWorkForThread!(providerThread));

          const commands = commandsSince(fake, afterAbort);
          assert.deepEqual(commands.filter(isScheduleProbe).toSorted(), [
            "list_heartbeats",
            "list_schedules",
          ]);
          assert.notInclude(commands, "get_state");
          // get_session_stats is read while the terminal is built, so the job
          // reads ahead of it happened before the turn ended.
          assert.isBelow(
            Math.max(commands.indexOf("list_schedules"), commands.indexOf("list_heartbeats")),
            commands.indexOf("get_session_stats"),
          );
          yield* startNextTurn(open, 2);
          assert.lengthOf(terminals(seen), 1);
          assert.equal(commandsSince(fake, afterAbort).filter(isScheduleProbe).length, 2);
          assertNeverPromoted(fake);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-CONT-010] a failed background-job read at an interrupt keeps the last known pin, and the turn still ends interrupted once",
      () =>
        Effect.gen(function* () {
          const open = yield* openWake((fake) => fake.setBackgroundJobs([cronJob("active")], []));
          const { fake, runtime, providerThread, seen, record } = open;
          yield* plainTurn(open, "Scheduled the check");
          assert.isTrue(yield* runtime.hasPendingBackgroundWork!);

          // Had the read succeeded it would report no jobs and clear the pin.
          fake.setBackgroundJobs([], []);
          fake.failCommand("list_schedules", "schedule store unavailable");
          yield* open.begin("Keep going", 2);
          const providerTurnId = yield* runningTurnId(record);
          yield* fake.emit({ type: "agent_start" });
          yield* replyText(fake, "Still working");
          yield* runtime.interruptTurn({ providerThread, providerTurnId });
          yield* fake.takeRequest("abort");
          const afterAbort = fake.commandLog().length;
          yield* fake.emit(agentEnd(true));
          const terminal = yield* nextTerminal(open);
          assert.isTrue(
            terminal.status === "interrupted" && terminal.providerTurnId === providerTurnId,
          );
          assert.include(commandsSince(fake, afterAbort), "list_schedules");
          assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
          yield* startNextTurn(open, 3);
          assert.deepEqual(
            terminals(seen).map((event) => event.type === "turn.terminal" && event.status),
            ["completed", "interrupted"],
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect.each([
      { when: "after the agent ran", ran: true },
      { when: "before the agent ran", ran: false },
    ])(
      "[VAL-CONT-010] a turn Prime fails with an id-less error $when reads background jobs before its single failed terminal only if the agent ran",
      ({ ran }) =>
        Effect.gen(function* () {
          const open = yield* openWake();
          const { fake, runtime, seen } = open;
          const afterPrompt = yield* open.begin();
          if (ran) {
            yield* fake.emit({ type: "agent_start" });
            yield* replyText(fake, "Scheduled the check");
          }
          fake.setBackgroundJobs([cronJob("active")], []);
          yield* fake.emitOrphanError("prompt", "Prime worker lost the session");
          const terminal = yield* nextTerminal(open);
          assert.equal(terminal.status, "failed");
          assert.include(terminal.failure?.message, "Prime worker lost the session");
          const commands = commandsSince(fake, afterPrompt);
          assert.equal(commands.filter(isScheduleProbe).length, ran ? 2 : 0);
          assert.notInclude(commands, "get_state");
          assert.equal(yield* runtime.hasPendingBackgroundWork!, ran);
          yield* startNextTurn(open, 2);
          assert.lengthOf(terminals(seen), 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-CONT-010] a turn that ends failed after three failed settle get_state reads still reads background jobs first",
      () =>
        Effect.gen(function* () {
          const open = yield* openWake();
          const { fake, runtime, seen } = open;
          const afterPrompt = yield* open.begin();
          yield* fake.emit({ type: "agent_start" });
          yield* replyText(fake, "Scheduled the check");
          fake.setBackgroundJobs([], [{ job: cronJob("active") }]);
          fake.failCommand("get_state", "session state store is locked");
          yield* fake.emit(agentEnd());
          yield* fake.takeRequest("get_state");
          yield* advanceClockUntil(fake.takeRequest("get_state"));
          yield* advanceClockUntil(fake.takeRequest("get_state"));
          const terminal = yield* nextTerminal(open);
          assert.equal(terminal.status, "failed");
          assert.include(terminal.failure?.message, "session state store is locked");
          const commands = commandsSince(fake, afterPrompt);
          assert.equal(commands.filter((type) => type === "get_state").length, 3);
          assert.deepEqual(commands.filter(isScheduleProbe).toSorted(), [
            "list_heartbeats",
            "list_schedules",
          ]);
          assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
          yield* startNextTurn(open, 2);
          assert.lengthOf(terminals(seen), 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect.each(["Stop with a runtime restart", "a process exit"] as const)(
      "[VAL-CONT-010] a turn ended by %s reads no background jobs",
      (ending) =>
        Effect.gen(function* () {
          const open = yield* openWake();
          const { fake, runtime, providerThread, seen, record } = open;
          const afterPrompt = yield* open.begin();
          const providerTurnId = yield* runningTurnId(record);
          yield* fake.emit({ type: "agent_start" });
          yield* replyText(fake, "Scheduled the check");
          fake.setBackgroundJobs([cronJob("active")], []);
          yield* pumpBarrier(fake, record, "before the turn ends");
          if (ending === "a process exit") yield* fake.exit(1);
          else {
            yield* runtime.interruptTurn({
              providerThread,
              providerTurnId,
              requestRuntimeRestart: true,
            });
          }
          const terminal = yield* nextTerminal(open);
          assert.equal(terminal.status, ending === "a process exit" ? "failed" : "interrupted");
          yield* record(
            (event) =>
              event.type === "provider_session.updated" &&
              event.providerSession.status === (ending === "a process exit" ? "error" : "stopped"),
          );
          assert.isFalse(commandsSince(fake, afterPrompt).some(isScheduleProbe));
          assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
          assert.lengthOf(terminals(seen), 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect.each([
      { command: "/goal pause", before: "active", after: "paused", expected: "paused" },
      { command: "/goal resume", before: "paused", after: "active", expected: "active" },
      { command: "/goal clear", before: "active", after: null, expected: null },
    ])(
      "[VAL-CMD-009] $command settles once and mirrors goal state $expected",
      ({ command, before, after, expected }) =>
        Effect.gen(function* () {
          const open = yield* openTurn((fake) => fake.queueState({ goal: primeGoal(before) }));
          const { fake, providerThread, seen, record } = open;
          assert.equal(providerThread.goal?.status, before);
          yield* startTurn(open.runtime, providerThread, "default", [], command);
          yield* fake.takeRequest("prompt");
          yield* fake.acceptSessionCommand(command);
          yield* emitCustomMessage(fake, sessionCommandEcho(command));
          yield* fake.emit({
            type: "goal_update",
            goal: after === null ? IDLE_GOAL : primeGoal(after),
          });
          yield* emitCustomMessage(fake, sessionCommandResult(command, "Done."));
          yield* pumpBarrier(fake, record, "before the action clears");
          assert.lengthOf(terminals(seen), 0);
          yield* fake.setSessionActions(IDLE_ACTIONS);
          const terminal = yield* nextTerminal(open);
          assert.equal(terminal.status, "completed");
          assert.lengthOf(terminals(seen), 1);
          const threadUpdates = seen.filter((event) => event.type === "provider_thread.updated");
          assert.equal(goalOf(threadUpdates.at(-1)!)?.status ?? null, expected);
          assert.isTrue(
            threadUpdates.some((event) => (goalOf(event)?.status ?? null) === expected),
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
    /** A prompt turn as Prime runs it: the prompt echoed as a user entry, then the reply. */
    const promptTurn = (open: Open, prompt: string, runOrdinal: number) =>
      Effect.gen(function* () {
        yield* open.begin(prompt, runOrdinal);
        yield* open.fake.emit({ type: "agent_start" });
        yield* open.fake.emit({
          type: "message_end",
          message: { role: "user", content: [{ type: "text", text: prompt }] },
        });
        yield* replyText(open.fake, `reply to ${prompt}`);
        yield* open.fake.emit(agentEnd());
        assert.equal((yield* nextTerminal(open)).status, "completed");
      });
    const settledTurns = (seen: ReadonlyArray<AdapterEvent>) => {
      const turns = new Map<string, OrchestrationV2ProviderTurn>();
      for (const event of seen) {
        if (event.type === "provider_turn.updated" && event.providerTurn.status === "completed") {
          turns.set(event.providerTurn.id, event.providerTurn);
        }
      }
      return [...turns.values()];
    };
    const leadOf = (turn: OrchestrationV2ProviderTurn) =>
      primeTurnEntries(turn.nativeTurnRef?.fingerprint)?.lead;
    const rollBackToFirst = (open: Open, turns: ReadonlyArray<OrchestrationV2ProviderTurn>) =>
      open.runtime.rollbackThread({
        providerThread: open.providerThread,
        target: {
          type: "provider_turn",
          checkpointId: CheckpointId.make("checkpoint-1"),
          appRunOrdinal: 1,
          providerTurn: turns[0]!,
        },
        providerThreadTurns: turns,
      });
    const assertNoNativeRollback = (fake: FakePrime) => {
      assert.notInclude(fake.commandLog(), "get_fork_messages");
      assert.notInclude(fake.commandLog(), "fork");
      assert.notInclude(fake.commandLog(), "clone");
    };

    it.effect(
      "[VAL-FORK-002] refuses to roll back past a completed idle heartbeat continuation, sends no fork, and keeps the session usable",
      () =>
        Effect.gen(function* () {
          const open = yield* openWake();
          const { fake } = open;
          yield* promptTurn(open, "Watch the build", 1);
          yield* fake.emit({ type: "agent_start" });
          yield* open.takeOffer;
          yield* emitCustomMessage(fake, heartbeatPrompt);
          yield* replyText(fake, "Build is green");
          yield* fake.emit(agentEnd());
          yield* bufferedRunEnded(open);
          yield* open.adopt(2);
          assert.equal((yield* nextTerminal(open)).status, "completed");

          const turns = settledTurns(open.seen);
          assert.deepEqual(turns.map(leadOf), ["user", "heartbeat"]);
          const error = yield* rollBackToFirst(open, turns).pipe(Effect.flip);
          const message = boundaryRefusal(error, "ProviderAdapterRollbackThreadError");
          assert.include(message, "Prime cannot roll back past turn 2 (a heartbeat continuation)");
          assert.include(message, "The thread is unchanged.");
          assertNoNativeRollback(fake);

          yield* open.begin("Still there?", 3);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-FORK-002] refuses to roll back past a /goal objective run, whose goal context reaches the model before any user entry",
      () =>
        Effect.gen(function* () {
          const open = yield* openWake();
          const { fake } = open;
          yield* promptTurn(open, "Prepare the release", 1);
          yield* open.begin(GOAL_COMMAND, 2);
          yield* fake.setSessionActions({
            ...IDLE_ACTIONS,
            active: { kind: "session_command", phase: "running", label: GOAL_COMMAND },
          });
          yield* emitCustomMessage(fake, sessionCommandEcho(GOAL_COMMAND));
          yield* fake.emit({ type: "goal_update", goal: primeGoal("active") });
          // Live order (prime_goal_autonomous): the harness digest, then the goal context.
          yield* fake.emit({ type: "agent_start" });
          yield* emitCustomMessage(fake, {
            role: "custom",
            customType: "harness_digest",
            content: "<harness-digest>",
            display: false,
          });
          yield* emitCustomMessage(fake, {
            role: "custom",
            customType: "goal_context",
            content: "[goal: continuation]\n\nship the release",
            display: true,
          });
          yield* replyText(fake, "Working on the goal");
          yield* fake.emit(agentEnd());
          yield* fake.setSessionActions(IDLE_ACTIONS);
          assert.equal((yield* nextTerminal(open)).status, "completed");

          const turns = settledTurns(open.seen);
          assert.deepEqual(turns.map(leadOf), ["user", "goal"]);
          const error = yield* rollBackToFirst(open, turns).pipe(Effect.flip);
          const message = boundaryRefusal(error, "ProviderAdapterRollbackThreadError");
          assert.include(message, "turn 2 (a goal run)");
          assertNoNativeRollback(fake);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-FORK-002] rolls back past a /goal status turn without a fork, because its command rows never reach the model",
      () =>
        Effect.gen(function* () {
          const open = yield* openWake();
          const { fake, providerThread } = open;
          yield* promptTurn(open, "Prepare the release", 1);
          yield* startTurn(
            open.runtime,
            providerThread,
            "default",
            [],
            "/goal status",
            undefined,
            2,
          );
          yield* fake.takeRequest("prompt");
          yield* fake.acceptSessionCommand("/goal status");
          yield* emitCustomMessage(fake, sessionCommandEcho("/goal status"));
          yield* fake.emit({ type: "goal_update", goal: IDLE_GOAL });
          yield* emitCustomMessage(fake, sessionCommandResult("/goal status", "No active goal."));
          yield* fake.setSessionActions(IDLE_ACTIONS);
          assert.equal((yield* nextTerminal(open)).status, "completed");

          const turns = settledTurns(open.seen);
          assert.deepEqual(turns.map(leadOf), ["user", "none"]);
          const rolledBack = yield* rollBackToFirst(open, turns);
          assert.equal(
            rolledBack.providerThread.nativeThreadRef?.nativeId,
            providerThread.nativeThreadRef?.nativeId,
          );
          assertNoNativeRollback(fake);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("autonomous mode", () => {
    const PROMPT = "Hello prime-agent";
    /** core/autonomous.ts buildAutonomousGateFailureContinuation, minus the timestamp. */
    const GATE_FAILED_CONTINUATION =
      "[autonomous-continuation: gate-failed]\n\nAutonomous quality gate failed (attempt 1/3): `npm test` exited 1.\n\nOutput:\nFAIL src/x.test.ts\n\nContinue working. Fix the failure, then produce terminal evidence.";
    const PLAIN_CONTINUATION =
      "[autonomous-continuation]\n\nNo human input is available in autonomous mode.";
    const GATE_NOTICES = [
      ["autonomous_gate_started", "Autonomous gate: running `npm test` (attempt 1/3)…"],
      ["autonomous_gate_failed", "Autonomous gate failed (attempt 1/3): `npm test` exited 1."],
      ["autonomous_gate_started", "Autonomous gate: running `npm test` (attempt 2/3)…"],
      ["autonomous_gate_passed", "Autonomous gate passed: `npm test`."],
      ["autonomous_stopped", "Autonomous run complete: quality gates passed."],
    ] as const;

    /** Prime's autonomous lifecycle row (core/agent-session.ts _emitAutonomousEvent). */
    const emitNotice = (fake: FakePrime, [customType, content]: readonly [string, string]) =>
      emitCustomMessage(fake, { role: "custom", customType, content, display: true, details: {} });
    const emitUserMessage = (fake: FakePrime, text: string, flagged: boolean) =>
      Effect.gen(function* () {
        const message = {
          role: "user",
          content: [{ type: "text", text }],
          ...(flagged ? { isAutonomousContinuation: true } : {}),
        };
        yield* fake.emit({ type: "message_start", message });
        yield* fake.emit({ type: "message_end", message });
      });
    const nextTerminal = (
      record: (predicate: (event: AdapterEvent) => boolean) => Effect.Effect<AdapterEvent>,
    ) =>
      Effect.map(
        record((event) => event.type === "turn.terminal"),
        (event) => event as Extract<AdapterEvent, { type: "turn.terminal" }>,
      );
    const latestItems = (seen: ReadonlyArray<AdapterEvent>) => {
      const items = new Map<
        string,
        Extract<AdapterEvent, { type: "turn_item.updated" }>["turnItem"]
      >();
      for (const event of seen) {
        if (event.type === "turn_item.updated") items.set(event.turnItem.id, event.turnItem);
      }
      return [...items.values()];
    };
    const noticeTexts = (seen: ReadonlyArray<AdapterEvent>) =>
      latestItems(seen).flatMap((item) => (item.type === "system_notice" ? [item.message] : []));
    const assistantLegs = (seen: ReadonlyArray<AdapterEvent>) =>
      latestItems(seen).flatMap((item) => (item.type === "assistant_message" ? [item.text] : []));
    const continuationItems = (seen: ReadonlyArray<AdapterEvent>) =>
      latestItems(seen).flatMap((item) => (item.type === "user_message" ? [item] : []));
    const userRoleMessages = (seen: ReadonlyArray<AdapterEvent>) =>
      seen.flatMap((event) =>
        event.type === "message.updated" && event.message.role === "user" ? [event.message] : [],
      );

    it.effect(
      "[VAL-AUTO-001] AUTONOMOUS: legs are assistant messages, gates are system notices, the continuation is provider-created, the echo is hidden, and the turn settles once after autonomous_stopped",
      () =>
        Effect.gen(function* () {
          const open = yield* openTurn();
          const { fake, seen, record } = open;
          yield* open.begin(PROMPT);
          const providerTurnId = yield* runningTurnId(record);
          yield* fake.setSessionActions(RUNNING_TURN_ACTIONS);
          yield* fake.emit({ type: "agent_start" });
          yield* emitUserMessage(fake, PROMPT, false);
          yield* replyText(fake, "first leg");
          yield* emitNotice(fake, GATE_NOTICES[0]);
          yield* emitNotice(fake, GATE_NOTICES[1]);
          yield* emitUserMessage(fake, GATE_FAILED_CONTINUATION, true);
          yield* replyText(fake, "second leg");
          yield* emitNotice(fake, GATE_NOTICES[2]);
          yield* emitNotice(fake, GATE_NOTICES[3]);
          yield* emitNotice(fake, GATE_NOTICES[4]);
          yield* fake.emit(agentEnd());
          yield* pumpBarrier(fake, record, "after agent_end while the action is active");
          assert.lengthOf(terminals(seen), 0, "the active autonomous action keeps the turn open");

          yield* fake.setSessionActions(IDLE_ACTIONS);
          const terminal = yield* nextTerminal(record);
          assert.equal(terminal.status, "completed");
          assert.lengthOf(terminals(seen), 1);

          assert.deepEqual(assistantLegs(seen), ["first leg", "second leg"]);
          assert.deepEqual(
            noticeTexts(seen),
            GATE_NOTICES.map(([, content]) => content),
          );
          const continuations = continuationItems(seen);
          assert.lengthOf(continuations, 1);
          const continuation = continuations[0]!;
          assert.equal(continuation.text, GATE_FAILED_CONTINUATION);
          assert.equal(continuation.title, "Autonomous gate failed");
          assert.equal(continuation.status, "completed");
          assert.equal(continuation.providerTurnId, providerTurnId);
          const messages = userRoleMessages(seen);
          assert.isNotEmpty(messages);
          for (const message of messages) {
            assert.equal(message.createdBy, "agent", "never a user bubble");
            assert.equal(message.creationSource, "provider");
            assert.equal(message.id, continuation.messageId);
            assert.equal(message.text, GATE_FAILED_CONTINUATION);
          }
          assert.isFalse(
            latestItems(seen).some((item) => JSON.stringify(item).includes(PROMPT)),
            "the prompt echo produced an item",
          );
          const itemIndex = (predicate: (event: AdapterEvent) => boolean) =>
            seen.findIndex(predicate);
          const continuationIndex = itemIndex(
            (event) => event.type === "turn_item.updated" && event.turnItem.type === "user_message",
          );
          assert.isTrue(itemIndex(isAssistantText("first leg")) < continuationIndex);
          assert.isTrue(continuationIndex < itemIndex(isAssistantText("second leg")));
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect.each([
      {
        name: "a flagged gate-failed marker",
        text: GATE_FAILED_CONTINUATION,
        flagged: true,
        title: "Autonomous gate failed",
      },
      {
        name: "a plain flagged marker",
        text: PLAIN_CONTINUATION,
        flagged: true,
        title: "Autonomous continuation",
      },
      {
        name: "a held continuation with the text marker and no flag",
        text: PLAIN_CONTINUATION,
        flagged: false,
        title: "Autonomous continuation",
      },
    ])(
      "[VAL-AUTO-002] $name becomes one provider-created continuation item",
      ({ text, flagged, title }) =>
        Effect.gen(function* () {
          const open = yield* openTurn();
          const { fake, seen, record } = open;
          yield* open.begin(PROMPT);
          yield* fake.emit({ type: "agent_start" });
          yield* replyText(fake, "first leg");
          yield* emitUserMessage(fake, text, flagged);
          yield* replyText(fake, "second leg");
          yield* fake.emit(agentEnd());
          const terminal = yield* nextTerminal(record);
          assert.equal(terminal.status, "completed");
          const continuations = continuationItems(seen);
          assert.lengthOf(continuations, 1);
          assert.equal(continuations[0]!.text, text);
          assert.equal(continuations[0]!.title, title);
          assert.isTrue(
            userRoleMessages(seen).every(
              (message) => message.createdBy === "agent" && message.creationSource === "provider",
            ),
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-AUTO-002] an ordinary user message inside a run produces no item", () =>
      Effect.gen(function* () {
        const open = yield* openTurn();
        const { fake, seen, record } = open;
        yield* open.begin(PROMPT);
        yield* fake.emit({ type: "agent_start" });
        yield* emitUserMessage(fake, PROMPT, false);
        yield* replyText(fake, "reply");
        yield* fake.emit(agentEnd());
        yield* nextTerminal(record);
        assert.lengthOf(continuationItems(seen), 0);
        assert.lengthOf(userRoleMessages(seen), 0);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-AUTO-003] gate notices arriving as bare message pairs between runs map onto the open turn, which settles once after autonomous_stopped",
      () =>
        Effect.gen(function* () {
          const open = yield* openTurn();
          const { fake, seen, record } = open;
          yield* open.begin(PROMPT);
          const providerTurnId = yield* runningTurnId(record);
          yield* fake.setSessionActions(RUNNING_TURN_ACTIONS);
          yield* fake.emit({ type: "agent_start" });
          yield* replyText(fake, "first leg");
          yield* fake.emit(agentEnd());
          yield* emitNotice(fake, GATE_NOTICES[0]);
          yield* emitNotice(fake, GATE_NOTICES[1]);
          yield* pumpBarrier(fake, record, "after the bare gate failure");
          assert.lengthOf(terminals(seen), 0);

          yield* fake.emit({ type: "agent_start" });
          yield* emitUserMessage(fake, PLAIN_CONTINUATION, false);
          yield* replyText(fake, "second leg");
          yield* fake.emit(agentEnd());
          yield* emitNotice(fake, GATE_NOTICES[2]);
          yield* emitNotice(fake, GATE_NOTICES[3]);
          yield* emitNotice(fake, GATE_NOTICES[4]);
          yield* pumpBarrier(fake, record, "after autonomous_stopped");
          assert.lengthOf(terminals(seen), 0);

          yield* fake.setSessionActions(IDLE_ACTIONS);
          const terminal = yield* nextTerminal(record);
          assert.equal(terminal.status, "completed");
          assert.lengthOf(terminals(seen), 1);
          const notices = latestItems(seen).flatMap((item) =>
            item.type === "system_notice" ? [item] : [],
          );
          assert.deepEqual(
            notices.map((item) => item.message),
            GATE_NOTICES.map(([, content]) => content),
          );
          for (const notice of notices) {
            assert.equal(notice.providerTurnId, providerTurnId);
            assert.equal(notice.status, "completed");
          }
          assert.deepEqual(assistantLegs(seen), ["first leg", "second leg"]);
          assert.lengthOf(continuationItems(seen), 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-AUTO-001][VAL-AUTO-003][VAL-MSG-003] two threads whose turns share a local ordinal keep distinct autonomous notice, custom notice, continuation, heartbeat, notify, and extension-error items",
      () =>
        Effect.gen(function* () {
          /** Each thread runs its first turn, so both start at the same local item ordinal. */
          const runThread = Effect.fnUntraced(function* (index: number) {
            const fake = yield* makeFakePrime;
            fake.queueState({
              sessionFile: `/fake/.prime/agent/sessions/--workspace--/000${index}_thread.jsonl`,
            });
            const threadId = ThreadId.make(`thread-prime-test-${index}`);
            const { runtime, takeEvent } = yield* openRuntime(
              fake,
              "default",
              threadId,
              ProviderSessionId.make(`provider-session-prime-test-${index}`),
            );
            const providerThread = yield* runtime.ensureThread({
              threadId,
              modelSelection: modelSelection("default"),
              runtimePolicy,
            });
            const seen: Array<AdapterEvent> = [];
            const record = (predicate: (event: AdapterEvent) => boolean) =>
              takeEvent((event) => {
                seen.push(event);
                return predicate(event);
              });
            yield* startTurn(
              runtime,
              providerThread,
              "default",
              [],
              PROMPT,
              undefined,
              1,
              threadId,
            );
            yield* fake.takeRequest("prompt");
            yield* fake.emit({ type: "response", command: "prompt", success: true });
            const providerTurnId = yield* runningTurnId(record);
            yield* fake.emit({ type: "agent_start" });
            yield* emitNotice(fake, ["autonomous_gate_failed", `gate failed in thread ${index}`]);
            yield* emitUserMessage(fake, `${PLAIN_CONTINUATION}\n\nthread ${index}`, true);
            yield* emitCustomMessage(fake, {
              role: "custom",
              customType: "heartbeat_prompt",
              content: `[heartbeat: */5 * * * * run#1]\n\nheartbeat in thread ${index}`,
              display: true,
              details: { jobId: `job-${index}`, schedule: "*/5 * * * *", status: "active" },
            });
            yield* emitCustomMessage(fake, {
              role: "custom",
              customType: "prime_future_notice",
              content: `custom notice in thread ${index}`,
              display: true,
              details: {},
            });
            yield* fake.emit({
              type: "extension_ui_request",
              method: "notify",
              message: `notify in thread ${index}`,
            });
            yield* fake.emit({
              type: "extension_error",
              extensionPath: "/ext.ts",
              event: "tool_call",
              error: `extension failed in thread ${index}`,
            });
            yield* fake.emit(agentEnd());
            yield* record((event) => event.type === "turn.terminal");
            return { threadId, providerThread, providerTurnId, seen };
          });

          const first = yield* runThread(1);
          const second = yield* runThread(2);
          assert.notEqual(first.providerThread.id, second.providerThread.id);
          assert.notEqual(first.providerTurnId, second.providerTurnId);

          const syntheticItems = (seen: ReadonlyArray<AdapterEvent>) =>
            latestItems(seen).filter(
              (item) =>
                item.type === "system_notice" ||
                item.type === "user_message" ||
                item.type === "notification" ||
                item.type === "error" ||
                (item.type === "dynamic_tool" && item.toolName === "notify"),
            );
          const nodeIds = (seen: ReadonlyArray<AdapterEvent>) =>
            seen.flatMap((event) => (event.type === "node.updated" ? [event.node.id] : []));
          const messageIds = (seen: ReadonlyArray<AdapterEvent>) =>
            seen.flatMap((event) => (event.type === "message.updated" ? [event.message.id] : []));

          for (const thread of [first, second]) {
            const items = syntheticItems(thread.seen);
            assert.sameMembers(
              items.map((item) => item.type),
              [
                "system_notice",
                "system_notice",
                "user_message",
                "notification",
                "dynamic_tool",
                "error",
              ],
            );
            assert.lengthOf(messageIds(thread.seen), 1);
          }

          const disjoint = (left: ReadonlyArray<string>, right: ReadonlyArray<string>) =>
            assert.isEmpty(
              left.filter((id) => right.includes(id)),
              "derived ids collide across threads",
            );
          disjoint(
            syntheticItems(first.seen).map((item) => item.id),
            syntheticItems(second.seen).map((item) => item.id),
          );
          disjoint(nodeIds(first.seen), nodeIds(second.seen));
          disjoint(messageIds(first.seen), messageIds(second.seen));

          // ProjectionStore upserts by id, so replaying both threads into one
          // keyed store shows whether the second thread replaces the first's rows.
          const projected = new Map<string, { threadId: string; text: string }>();
          for (const thread of [first, second]) {
            for (const item of syntheticItems(thread.seen)) {
              projected.set(item.id, { threadId: item.threadId, text: JSON.stringify(item) });
            }
          }
          for (const thread of [first, second]) {
            const index = thread === first ? 1 : 2;
            for (const item of syntheticItems(thread.seen)) {
              const row = projected.get(item.id);
              assert.equal(row?.threadId, thread.threadId);
              assert.include(row?.text ?? "", `thread ${index}`);
              assert.include(item.nativeItemRef?.nativeId ?? "", thread.providerTurnId);
            }
          }
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("custom messages and error detail", () => {
    const custom = (customType: string, content: string, display = true) => ({
      role: "custom",
      customType,
      content,
      display,
      details: {},
    });
    const latestItems = (seen: ReadonlyArray<AdapterEvent>) => {
      const items = new Map<
        string,
        Extract<AdapterEvent, { type: "turn_item.updated" }>["turnItem"]
      >();
      for (const event of seen) {
        if (event.type === "turn_item.updated") items.set(event.turnItem.id, event.turnItem);
      }
      return [...items.values()];
    };
    /** Items other than the reply and the barrier the test itself produced. */
    const customItems = (seen: ReadonlyArray<AdapterEvent>) =>
      latestItems(seen).filter(
        (item) =>
          item.type !== "assistant_message" &&
          !(item.type === "dynamic_tool" && item.toolName === "notify"),
      );
    const runWith = Effect.fnUntraced(function* (messages: ReadonlyArray<Record<string, unknown>>) {
      const open = yield* openTurn();
      const { fake, seen, record } = open;
      yield* open.begin();
      const providerTurnId = yield* runningTurnId(record);
      yield* fake.emit({ type: "agent_start" });
      for (const message of messages) yield* emitCustomMessage(fake, message);
      yield* replyText(fake, "done");
      yield* fake.emit(agentEnd());
      const terminal = yield* record((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      assert.lengthOf(terminals(seen), 1);
      return { seen, providerTurnId };
    });

    it.effect(
      "[VAL-MSG-001] session_slash_command, goal_context, and display:false custom messages produce no item",
      () =>
        Effect.gen(function* () {
          const { seen } = yield* runWith([
            custom("session_slash_command", "/goal ship it"),
            custom("goal_context", "[goal: continuation]\n\nContinue working toward the goal."),
            custom("prime_future_notice", "hidden future row", false),
            custom("autonomous_status", "hidden status row", false),
            custom("heartbeat_prompt", "[heartbeat: */5 * * * * run#1]\n\nhidden heartbeat", false),
            custom("ipython_state", "[python-state]\n\nYour kernel persisted.", false),
          ]);
          assert.deepEqual(customItems(seen), []);
          for (const text of ["/goal ship it", "[goal: continuation]", "hidden", "python-state"]) {
            assert.isFalse(
              seen.some(
                (event) =>
                  event.type === "turn_item.updated" &&
                  JSON.stringify(event.turnItem).includes(text),
              ),
              `${text} leaked into an item`,
            );
          }
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-MSG-002] RESTORE_KERNEL: the ipython restore row is one system_notice with no raw dump",
      () =>
        Effect.gen(function* () {
          const { seen, providerTurnId } = yield* runWith([
            custom(
              "ipython_state_restored",
              "[python-state-restored]\n\nYour Python kernel state was revived from your previous session. These names are available again: df, model, helpers.",
            ),
          ]);
          const items = customItems(seen);
          assert.lengthOf(items, 1);
          const notice = items[0]!;
          assert.isTrue(
            notice.type === "system_notice" &&
              notice.message === "Python kernel state restored — 3 names revived." &&
              notice.status === "completed" &&
              notice.providerTurnId === providerTurnId,
          );
          assert.include(notice.nativeItemRef?.nativeId ?? "", providerTurnId);
          assert.isFalse(
            seen.some(
              (event) =>
                event.type === "turn_item.updated" &&
                /python-state-restored|available again/u.test(JSON.stringify(event.turnItem)),
            ),
            "the raw restore text never reaches an item",
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect.each([
      {
        name: "autonomous_status",
        message: custom("autonomous_status", "[autonomous-status: on]\n\nContinuations: 0/3."),
        type: "system_notice",
        text: "[autonomous-status: on]\n\nContinuations: 0/3.",
      },
      {
        name: "heartbeat_prompt",
        message: custom("heartbeat_prompt", "[heartbeat: */5 * * * * run#3]\n\nCheck the build"),
        type: "notification",
        text: "Check the build",
      },
      {
        name: "an unknown displayed customType",
        message: custom("prime_future_notice", "Something Prime wants shown."),
        type: "system_notice",
        text: "Something Prime wants shown.",
      },
      {
        name: "a session_slash_command_result row",
        message: custom("session_slash_command_result", "No active goal."),
        type: "system_notice",
        text: "No active goal.",
      },
    ] as const)(
      "[VAL-MSG-003] $name becomes exactly one $type item with its text",
      ({ message, type, text }) =>
        Effect.gen(function* () {
          const { seen, providerTurnId } = yield* runWith([message]);
          const items = customItems(seen);
          assert.lengthOf(items, 1);
          const item = items[0]!;
          assert.equal(item.type, type);
          assert.equal(item.providerTurnId, providerTurnId);
          assert.include(item.nativeItemRef?.nativeId ?? "", providerTurnId);
          if (item.type === "system_notice") assert.equal(item.message, text);
          else if (item.type === "notification") assert.equal(item.detail, text);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-MSG-003] a failed session-command result fails its turn once and adds no notice",
      () =>
        Effect.gen(function* () {
          const open = yield* openTurn();
          const { fake, seen, record } = open;
          yield* startTurn(open.runtime, open.providerThread, "default", [], "/goal pause");
          yield* fake.takeRequest("prompt");
          yield* fake.acceptSessionCommand("/goal pause");
          yield* emitCustomMessage(fake, sessionCommandEcho("/goal pause"));
          yield* emitCustomMessage(
            fake,
            sessionCommandResult("/goal pause", "Command failed: No active goal to pause.", false),
          );
          yield* fake.setSessionActions(IDLE_ACTIONS);
          const terminal = yield* record((event) => event.type === "turn.terminal");
          assert.isTrue(
            terminal.type === "turn.terminal" &&
              terminal.status === "failed" &&
              terminal.failure.message === "No active goal to pause.",
          );
          assert.lengthOf(terminals(seen), 1);
          assert.isFalse(
            latestItems(seen).some((item) => item.type === "system_notice"),
            "the failure is reported once, by the terminal",
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    const MISTRAL_400 =
      'Mistral API error (400): {"message":"Invalid request payload: Expected at most one leading ThinkChunk, got 2","code":"3051"}';
    it.effect.each([
      {
        name: "a Mistral 400 errorMessage",
        messages: [
          { role: "user", content: "did something happen?" },
          { role: "assistant", content: [], stopReason: "error", errorMessage: MISTRAL_400 },
        ],
        failure: "Expected at most one leading ThinkChunk",
      },
      {
        name: "stopReason stop with no error",
        messages: [{ role: "assistant", content: [], stopReason: "stop" }],
        failure: null,
      },
      { name: "empty messages", messages: [], failure: null },
      {
        name: "a whitespace-only errorMessage",
        messages: [{ role: "assistant", content: [], errorMessage: "  " }],
        failure: null,
      },
    ])("[VAL-ERR-001] agent_end with $name reaches turn.terminal", ({ messages, failure }) =>
      Effect.gen(function* () {
        const { fake, seen, record, begin } = yield* openTurn();
        yield* begin();
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "agent_end", messages });
        const terminal = yield* record((event) => event.type === "turn.terminal");
        assert.lengthOf(terminals(seen), 1);
        if (terminal.type !== "turn.terminal") return assert.fail("expected a terminal");
        if (failure === null) {
          assert.equal(terminal.status, "completed");
          assert.isNull(terminal.failure);
          assert.isFalse(
            seen.some(
              (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
            ),
          );
        } else {
          assert.equal(terminal.status, "failed");
          assert.include(terminal.failure?.message ?? "", failure);
        }
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("rlm children", () => {
    type Subagent = Extract<AdapterEvent, { type: "subagent.updated" }>["subagent"];
    type SubagentItem = Extract<
      Extract<AdapterEvent, { type: "turn_item.updated" }>["turnItem"],
      { type: "subagent" }
    >;
    const RUN_1 = RunId.make(`run:${THREAD_ID}:1`);
    const childUpdate = (child: Record<string, unknown>) => ({
      type: "rlm_child_update",
      child: { id: "child-1", label: "repo-scout", sessionDir: "/tmp/prime/sub-1", ...child },
    });
    const subagentUpdates = (seen: ReadonlyArray<AdapterEvent>): Array<Subagent> =>
      seen.flatMap((event) => (event.type === "subagent.updated" ? [event.subagent] : []));
    const subagentItems = (seen: ReadonlyArray<AdapterEvent>): Array<SubagentItem> =>
      seen.flatMap((event) =>
        event.type === "turn_item.updated" && event.turnItem.type === "subagent"
          ? [event.turnItem]
          : [],
      );
    const latestByTitle = <A extends { readonly title: string | null }>(rows: ReadonlyArray<A>) =>
      new Map(rows.map((row) => [row.title, row]));
    /** The overlay's SPAWN_CHILD scenario (rpc-mock-agent.ts). */
    const SPAWN_CHILD = [
      { status: "queued" },
      { status: "running", toolUseCount: 3, activity: { kind: "executing", toolName: "bash" } },
      { status: "running", answerPreview: "half done", toolUseCount: 7 },
      { status: "running", answerPreview: "half done", toolUseCount: 7 },
      { status: "done", answerPreview: "155 repos found", toolUseCount: 9, durationMs: 42_000 },
    ];
    const runChildren = Effect.fnUntraced(function* (
      snapshots: ReadonlyArray<Record<string, unknown>>,
    ) {
      const open = yield* openTurn();
      const { fake, seen, record } = open;
      yield* open.begin();
      const providerTurnId = yield* runningTurnId(record);
      yield* fake.emit({ type: "agent_start" });
      for (const snapshot of snapshots) yield* fake.emit(childUpdate(snapshot));
      yield* replyText(fake, "child work done");
      yield* fake.emit(agentEnd());
      const terminal = yield* record((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      return { seen, providerTurnId, providerThread: open.providerThread };
    });

    it.effect(
      "[VAL-RLM-001] SPAWN_CHILD: child snapshots become one provider-native subagent and its turn item, queued -> waiting -> running -> completed",
      () =>
        Effect.gen(function* () {
          const { seen, providerTurnId, providerThread } = yield* runChildren(SPAWN_CHILD);
          const updates = subagentUpdates(seen);
          assert.deepEqual(
            updates.map((update) => update.status),
            ["waiting", "running", "running", "completed"],
          );
          assert.equal(new Set(updates.map((update) => update.id)).size, 1);
          for (const update of updates) {
            assert.equal(update.origin, "provider_native");
            assert.isNull(update.childThreadId);
            assert.equal(update.title, "repo-scout");
            assert.equal(update.runId, RUN_1);
            assert.equal(update.threadId, THREAD_ID);
            assert.equal(update.providerThreadId, providerThread.id);
            assert.include(update.nativeTaskRef?.nativeId ?? "", "child-1");
          }
          assert.equal(updates[1]?.progress, "Running bash");
          assert.equal(updates[2]?.progress, "half done");
          const done = updates.at(-1)!;
          assert.equal(done.result, "155 repos found");
          assert.isNotNull(done.completedAt);

          const items = subagentItems(seen);
          assert.lengthOf(items, 4);
          assert.equal(new Set(items.map((item) => item.id)).size, 1);
          const item = items.at(-1)!;
          assert.equal(item.subagentId, done.id);
          assert.equal(item.status, "completed");
          assert.equal(item.origin, "provider_native");
          assert.isNull(item.childThreadId);
          assert.equal(item.title, "repo-scout");
          assert.equal(item.result, "155 repos found");
          assert.equal(item.runId, RUN_1);
          assert.equal(item.providerTurnId, providerTurnId);

          const nodes = seen.flatMap((event) =>
            event.type === "node.updated" && event.node.id === done.id ? [event.node] : [],
          );
          assert.deepEqual(
            nodes.map((node) => node.status),
            ["waiting", "running", "running", "completed"],
          );
          assert.isTrue(nodes.every((node) => node.kind === "subagent" && node.runId === RUN_1));
          assert.equal(item.nodeId, done.id);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-RLM-002] an exact duplicate snapshot, or one that differs only in Prime's activity clock, adds no update",
      () =>
        Effect.gen(function* () {
          const [queued, tool, half, , done] = SPAWN_CHILD;
          const { seen } = yield* runChildren([
            queued!,
            tool!,
            half!,
            half!,
            { ...half, lastActivityAt: 1_700_000_000_000, activityStaleMs: 90_000 },
            done!,
            done!,
          ]);
          assert.lengthOf(subagentUpdates(seen), 4);
          assert.lengthOf(subagentItems(seen), 4);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-RLM-003] error and cancelled children end failed and cancelled with their text; a removal after completion keeps the completion",
      () =>
        Effect.gen(function* () {
          const { seen } = yield* runChildren([
            { id: "child-err", label: "lint", status: "queued" },
            { id: "child-err", label: "lint", status: "running" },
            { id: "child-err", label: "lint", status: "error", error: "kernel died" },
            { id: "child-cancel", label: "tests", status: "running" },
            { id: "child-cancel", label: "tests", status: "cancelled", error: "Cancelled by user" },
            { id: "child-docs", label: "docs", status: "done", answerPreview: "docs ok" },
            {
              id: "child-docs",
              label: "docs",
              status: "cancelled",
              error: "Deleted by parent orchestrator",
            },
          ]);
          const updates = latestByTitle(subagentUpdates(seen));
          const items = latestByTitle(subagentItems(seen));
          for (const [title, status, result] of [
            ["lint", "failed", "kernel died"],
            ["tests", "cancelled", "Cancelled by user"],
            ["docs", "completed", "docs ok"],
          ] as const) {
            assert.equal(updates.get(title)?.status, status, title);
            assert.equal(updates.get(title)?.result, result, title);
            assert.isNotNull(updates.get(title)?.completedAt ?? null, title);
            assert.equal(items.get(title)?.status, status, title);
            assert.equal(items.get(title)?.result, result, title);
          }
          assert.lengthOf(
            subagentUpdates(seen).filter((update) => update.title === "docs"),
            1,
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-RLM-004] a child that outlives its turn is non-terminal before turn.terminal, and its later update stays on the original run without a new turn or continuation offer",
      () =>
        Effect.gen(function* () {
          const offers =
            yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
          const open = yield* openTurn(undefined, "full-access", {
            offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          });
          const { fake, seen, record } = open;
          const afterPrompt = yield* open.begin();
          const providerTurnId = yield* runningTurnId(record);
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit(childUpdate({ status: "queued" }));
          yield* fake.emit(childUpdate({ status: "running" }));
          yield* replyText(fake, "spawned a scout");
          yield* fake.emit(agentEnd());
          yield* record((event) => event.type === "turn.terminal");
          const terminalIndex = seen.findIndex((event) => event.type === "turn.terminal");
          const beforeTerminal = subagentUpdates(seen.slice(0, terminalIndex));
          assert.equal(beforeTerminal.at(-1)?.status, "running");

          // No turn is open now: an unknown child has nothing to attach to.
          yield* fake.emit(childUpdate({ id: "child-stray", label: "stray", status: "running" }));
          yield* fake.emit(childUpdate({ status: "done", answerPreview: "155 repos found" }));
          const late = yield* record(
            (event) => event.type === "subagent.updated" && event.subagent.status === "completed",
          );
          const lateItem = yield* record(
            (event) => event.type === "turn_item.updated" && event.turnItem.type === "subagent",
          );
          assert.isTrue(late.type === "subagent.updated");
          if (late.type !== "subagent.updated" || lateItem.type !== "turn_item.updated") return;
          assert.equal(late.subagent.id, beforeTerminal[0]?.id);
          assert.equal(late.subagent.runId, RUN_1);
          assert.equal(late.subagent.result, "155 repos found");
          assert.equal(lateItem.turnItem.runId, RUN_1);
          assert.equal(lateItem.turnItem.providerTurnId, providerTurnId);
          assert.equal(lateItem.turnItem.status, "completed");

          const afterTerminal = seen.slice(terminalIndex + 1);
          assert.isFalse(
            afterTerminal.some(
              (event) => event.type === "turn.terminal" || event.type === "provider_turn.updated",
            ),
            "a late child update starts no turn",
          );
          assert.isFalse(
            subagentUpdates(seen).some((update) => update.title === "stray"),
            "an unattributable child is dropped",
          );
          assert.equal(yield* Queue.size(offers), 0);
          assert.notInclude(fake.commandLog().slice(afterPrompt), "prompt");
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-RLM-001] two threads whose children share Prime's session-local child id keep distinct subagents and items",
      () =>
        Effect.gen(function* () {
          const runThread = Effect.fnUntraced(function* (index: number) {
            const fake = yield* makeFakePrime;
            fake.queueState({
              sessionFile: `/fake/.prime/agent/sessions/--workspace--/000${index}_rlm.jsonl`,
            });
            const threadId = ThreadId.make(`thread-prime-rlm-${index}`);
            const { runtime, takeEvent } = yield* openRuntime(
              fake,
              "default",
              threadId,
              ProviderSessionId.make(`provider-session-prime-rlm-${index}`),
            );
            const providerThread = yield* runtime.ensureThread({
              threadId,
              modelSelection: modelSelection("default"),
              runtimePolicy,
            });
            const seen: Array<AdapterEvent> = [];
            const record = (predicate: (event: AdapterEvent) => boolean) =>
              takeEvent((event) => {
                seen.push(event);
                return predicate(event);
              });
            yield* startTurn(
              runtime,
              providerThread,
              "default",
              [],
              "Spawn a scout",
              undefined,
              1,
              threadId,
            );
            yield* fake.takeRequest("prompt");
            yield* fake.emit({ type: "response", command: "prompt", success: true });
            yield* fake.emit({ type: "agent_start" });
            yield* fake.emit(
              childUpdate({
                id: "sub-1a2b3c4d",
                label: `scout ${index}`,
                status: "done",
                answerPreview: `thread ${index}`,
              }),
            );
            yield* fake.emit(agentEnd());
            yield* record((event) => event.type === "turn.terminal");
            return { threadId, providerThread, seen };
          });
          const first = yield* runThread(1);
          const second = yield* runThread(2);
          assert.notEqual(first.providerThread.id, second.providerThread.id);
          const [firstUpdate] = subagentUpdates(first.seen);
          const [secondUpdate] = subagentUpdates(second.seen);
          const [firstItem] = subagentItems(first.seen);
          const [secondItem] = subagentItems(second.seen);
          assert.notEqual(firstUpdate?.id, secondUpdate?.id);
          assert.notEqual(firstItem?.id, secondItem?.id);
          assert.notEqual(firstItem?.nodeId, secondItem?.nodeId);
          for (const [thread, update, item] of [
            [first, firstUpdate, firstItem],
            [second, secondUpdate, secondItem],
          ] as const) {
            assert.equal(update?.threadId, thread.threadId);
            assert.equal(item?.threadId, thread.threadId);
            assert.include(update?.nativeTaskRef?.nativeId ?? "", thread.providerThread.id);
          }
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });
});

describe("Prime tool mapping", () => {
  type AdapterEvent = ProviderAdapter.ProviderAdapterV2Event;
  type TurnItem = Extract<AdapterEvent, { type: "turn_item.updated" }>["turnItem"];

  const openToolTurn = Effect.fnUntraced(function* () {
    const fake = yield* makeFakePrime;
    const { runtime, takeEvent } = yield* openRuntime(fake);
    const providerThread = yield* runtime.ensureThread({
      threadId: THREAD_ID,
      modelSelection: modelSelection("default"),
      runtimePolicy,
    });
    yield* startTurn(runtime, providerThread);
    yield* fake.takeRequest("prompt");
    yield* fake.emit({ type: "response", command: "prompt", success: true });
    yield* fake.emit({ type: "agent_start" });
    const items: Array<TurnItem> = [];
    /** Waits for the next item update matching the predicate, keeping every item seen. */
    const nextItem = (predicate: (item: TurnItem) => boolean) =>
      Effect.map(
        takeEvent((event) => {
          if (event.type !== "turn_item.updated") return false;
          items.push(event.turnItem);
          return predicate(event.turnItem);
        }),
        (event) => (event as Extract<AdapterEvent, { type: "turn_item.updated" }>).turnItem,
      );
    const itemsFor = (nativeId: string) =>
      items.filter((item) => item.nativeItemRef?.nativeId === nativeId);
    return { fake, nextItem, itemsFor };
  });

  const ipythonCode = "for i in range(3):\n    print(i)\nedit('src/a.py')";

  it.effect(
    "[VAL-TOOL-001][VAL-TOOL-002] ipython streams as one command execution, the heartbeat adds no output, and its diffs add a file change",
    () =>
      Effect.gen(function* () {
        const { fake, nextItem, itemsFor } = yield* openToolTurn();
        yield* fake.emit({
          type: "tool_execution_start",
          toolCallId: "cell-1",
          toolName: "ipython",
          args: { code: ipythonCode },
        });
        const running = yield* nextItem((item) => item.nativeItemRef?.nativeId === "cell-1");
        assert.isTrue(
          running.type === "command_execution" &&
            running.status === "running" &&
            running.input === ipythonCode,
        );
        for (const partialResult of [
          {
            content: [{ type: "text", text: "Starting Python kernel..." }],
            details: { status: "starting" },
          },
          { content: [{ type: "text", text: "0\n" }], details: { status: "ok" } },
          {
            content: [{ type: "text", text: "0\n[ipython cell still running (30s elapsed)]" }],
            details: { status: "ok" },
          },
          { content: [{ type: "text", text: "1\n" }], details: { status: "ok" } },
        ]) {
          yield* fake.emit({
            type: "tool_execution_update",
            toolCallId: "cell-1",
            toolName: "ipython",
            args: { code: ipythonCode },
            partialResult,
          });
        }
        const streamed = yield* nextItem(
          (item) =>
            item.nativeItemRef?.nativeId === "cell-1" &&
            item.type === "command_execution" &&
            item.output === "0\n1\n",
        );
        assert.equal(streamed.status, "running");

        yield* fake.emit({
          type: "tool_execution_end",
          toolCallId: "cell-1",
          toolName: "ipython",
          isError: false,
          result: {
            content: [{ type: "text", text: "0\n1\n2\n" }],
            details: {
              status: "ok",
              stdout: "0\n1\n2\n",
              stderr: "",
              diffs: [
                { path: "src/a.py", oldStr: "x = 1\n", newStr: "x = 2\n", startLine: 4 },
                { path: "src/b.py", oldStr: "y = 1\n", newStr: "y = 2\n", startLine: 1 },
              ],
            },
          },
        });
        const completed = yield* nextItem(
          (item) => item.nativeItemRef?.nativeId === "cell-1" && item.status === "completed",
        );
        assert.isTrue(
          completed.type === "command_execution" &&
            completed.input === ipythonCode &&
            completed.output === "0\n1\n2\n",
        );
        const fileChange = yield* nextItem((item) => item.type === "file_change");
        assert.isTrue(fileChange.type === "file_change" && fileChange.status === "completed");
        if (fileChange.type !== "file_change") return;
        assert.deepEqual(
          fileChange.changes?.map((change) => change.path),
          ["src/a.py", "src/b.py"],
        );
        assert.include(fileChange.diffStr, "@@ -4,1 +4,1 @@\n-x = 1\n+x = 2");
        assert.include(fileChange.diffStr, "+++ src/b.py");
        assert.notEqual(fileChange.id, completed.id);

        const cellUpdates = itemsFor("cell-1");
        assert.deepEqual(
          cellUpdates.map((item) => [
            item.status,
            item.type === "command_execution" ? item.output : null,
          ]),
          [
            ["running", undefined],
            ["running", "0\n"],
            ["running", "0\n1\n"],
            ["completed", "0\n1\n2\n"],
          ],
          "start-up and heartbeat updates emit nothing",
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-TOOL-003] a failed ipython cell ends as a failed command execution with the error text",
    () =>
      Effect.gen(function* () {
        const { fake, nextItem } = yield* openToolTurn();
        yield* fake.emit({
          type: "tool_execution_start",
          toolCallId: "cell-err",
          toolName: "ipython",
          args: { code: "1 / 0" },
        });
        yield* fake.emit({
          type: "tool_execution_end",
          toolCallId: "cell-err",
          toolName: "ipython",
          isError: true,
          result: {
            content: [{ type: "text", text: "ZeroDivisionError: division by zero" }],
            details: {
              status: "error",
              errorEname: "ZeroDivisionError",
              error: {
                ename: "ZeroDivisionError",
                evalue: "division by zero",
                traceback: ["ZeroDivisionError: division by zero"],
              },
            },
          },
        });
        const failed = yield* nextItem(
          (item) => item.nativeItemRef?.nativeId === "cell-err" && item.status !== "running",
        );
        assert.isTrue(
          failed.type === "command_execution" &&
            failed.status === "failed" &&
            failed.input === "1 / 0" &&
            failed.output === "ZeroDivisionError: division by zero",
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    {
      ids: "[VAL-TOOL-004]",
      toolName: "bash",
      args: { command: "ls" },
      result: { content: [{ type: "text", text: "README.md" }], details: { exitCode: 0 } },
      expected: { type: "command_execution", input: "ls", output: "README.md", exitCode: 0 },
    },
    {
      ids: "[VAL-TOOL-005]",
      toolName: "edit",
      args: { path: "a.ts" },
      result: { details: { diff: "-1 old\n+1 new", firstChangedLine: 1 } },
      expected: {
        type: "file_change",
        fileName: "a.ts",
        diffStr: "--- a.ts\n+++ a.ts\n@@ -1,1 +1,1 @@\n-old\n+new\n",
      },
    },
    {
      ids: "[VAL-TOOL-005]",
      toolName: "write",
      args: { path: "b.ts", content: "export {};\n" },
      result: { content: [{ type: "text", text: "Successfully wrote to b.ts" }] },
      expected: { type: "file_change", fileName: "b.ts", newStr: "export {};\n" },
    },
    {
      ids: "[VAL-TOOL-006]",
      toolName: "rename",
      args: { oldPath: "old.ts", newPath: "new.ts" },
      result: { content: [{ type: "text", text: "Renamed" }] },
      expected: {
        type: "file_change",
        fileName: "new.ts",
        changes: [{ operation: "rename", path: "new.ts", oldPath: "old.ts" }],
      },
    },
    {
      ids: "[VAL-TOOL-006]",
      toolName: "rename",
      args: { name: "renamed session" },
      result: { content: [{ type: "text", text: "Renamed" }] },
      expected: {
        type: "dynamic_tool",
        toolName: "rename",
        input: { name: "renamed session" },
        output: "Renamed",
      },
    },
    {
      ids: "[VAL-TOOL-007]",
      toolName: "stdout",
      args: { text: "hello" },
      result: { content: [{ type: "text", text: "hello" }] },
      expected: {
        type: "dynamic_tool",
        toolName: "stdout",
        input: { text: "hello" },
        output: "hello",
      },
    },
    {
      ids: "[VAL-TOOL-007]",
      toolName: "custom_lookup",
      args: { query: "q" },
      result: { content: [{ type: "text", text: "answer" }] },
      expected: {
        type: "dynamic_tool",
        toolName: "custom_lookup",
        input: { query: "q" },
        output: "answer",
      },
    },
  ])(
    "$ids maps $toolName with args $args to $expected.type",
    ({ toolName, args, result, expected }) =>
      Effect.gen(function* () {
        const { fake, nextItem } = yield* openToolTurn();
        yield* fake.emit({ type: "tool_execution_start", toolCallId: "tool-1", toolName, args });
        yield* fake.emit({
          type: "tool_execution_end",
          toolCallId: "tool-1",
          toolName,
          isError: false,
          result,
        });
        const completed = yield* nextItem(
          (item) => item.nativeItemRef?.nativeId === "tool-1" && item.status === "completed",
        );
        const projected = Object.fromEntries(
          Object.keys(expected).map((key) => [key, Reflect.get(completed, key)]),
        );
        assert.deepEqual(projected, expected);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
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
