import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { vi } from "vite-plus/test";

import { signalProcessGroup } from "@t3tools/provider-core/server/processGroup";
import * as HostProcess from "@t3tools/shared/HostProcess";

import { DROID_PROTOCOL_VERSION, DROID_STDERR_TAIL_MAX, timeoutForMethod } from "./protocol.ts";
import {
  type DroidRpcEvent,
  type DroidRpcRequestError,
  type DroidRpcRuntimeOptions,
  DroidRpcTerminatedError,
  makeDroidRpcRuntime,
} from "./rpc.ts";
import { makeRpcMockAgent } from "./rpcMockAgent.testkit.ts";

const decodeLogLine = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const encodeJsonLine = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const encoder = new TextEncoder();

type LogEntry = Record<string, unknown>;

const asRecord = (value: unknown): Record<string, unknown> =>
  Predicate.isObject(value) ? value : assert.fail(`expected an object, got ${typeof value}`);

const asString = (value: unknown): string =>
  Predicate.isString(value) ? value : assert.fail(`expected a string, got ${typeof value}`);

const asNumber = (value: unknown): number =>
  Predicate.isNumber(value) ? value : assert.fail(`expected a number, got ${typeof value}`);

class HandlerFailure extends Data.TaggedError("HandlerFailure")<{ readonly message: string }> {}

/** Outside the valid PID range, so cleanup of a fake process can never signal a real one. */
const FAKE_PID = 999_999_999;
const SENTINEL_KEY = "not-a-real-factory-key";

const startMockRuntime = Effect.fn("startMockRuntime")(function* (
  env: Record<string, string> = {},
  options: Partial<DroidRpcRuntimeOptions> = {},
) {
  const mock = yield* makeRpcMockAgent(env);
  const runtime = yield* makeDroidRpcRuntime({
    binaryPath: mock.binaryPath,
    cwd: mock.projectDir,
    ...options,
  });
  return { mock, runtime };
});

type Runtime = Effect.Success<ReturnType<typeof makeDroidRpcRuntime>>;

type EventQueue = Queue.Queue<DroidRpcEvent, DroidRpcTerminatedError | Cause.Done>;

const collectEvents = Effect.fn("collectEvents")(function* (runtime: Runtime) {
  const queue: EventQueue = yield* Queue.unbounded<
    DroidRpcEvent,
    DroidRpcTerminatedError | Cause.Done
  >();
  yield* Stream.runIntoQueue(runtime.events, queue).pipe(Effect.forkScoped);
  return queue;
});

/** Takes events until one matches; returns the skipped events and the match. */
const takeUntil = Effect.fn("takeUntil")(function* (
  queue: EventQueue,
  predicate: (event: DroidRpcEvent) => boolean,
) {
  const skipped: Array<DroidRpcEvent> = [];
  while (true) {
    const event = yield* Queue.take(queue);
    if (predicate(event)) {
      return { skipped, event };
    }
    skipped.push(event);
  }
});

const isNotification = (type: string) => (event: DroidRpcEvent) =>
  event._tag === "Notification" && event.notification.type === type;

const notificationType = (event: DroidRpcEvent) =>
  event._tag === "Notification" ? event.notification.type : event._tag;

const initializeSession = (runtime: Runtime, cwd: string) =>
  runtime.request("droid.initialize_session", {
    machineId: "default",
    cwd,
    modelId: "claude-opus-5-5",
    reasoningEffort: "medium",
    interactionMode: "auto",
    autonomyLevel: "off",
  });

const mockResponses = (log: ReadonlyArray<LogEntry>, id: string) =>
  log.filter((entry) => entry.type === "response" && entry.id === id);

const receivedMethods = (log: ReadonlyArray<LogEntry>) =>
  log
    .filter((entry) => entry.type === "recv")
    .map((entry) => asRecord(entry.message).method)
    .filter(Predicate.isString);

const spawnedPid = (log: ReadonlyArray<LogEntry>) =>
  asNumber(log.find((entry) => entry.type === "argv")?.pid);

/** Real subprocess tests run on the live clock; the timeout only fails a hung test. */
const live = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.timeout("30 seconds"), TestClock.withLive);

const captureLogs = () => {
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

const stubEnv = (name: string, value: string | undefined) =>
  Effect.acquireRelease(
    Effect.sync(() => vi.stubEnv(name, value)),
    () => Effect.sync(() => vi.unstubAllEnvs()),
  );

const groupExists = (pid: number) =>
  Effect.sync(() => {
    try {
      signalProcessGroup(pid, 0);
      return true;
    } catch {
      return false;
    }
  });

/** `process.kill(pid, 0)` outcome: "alive" when it succeeds, else the errno code (`ESRCH` once gone). */
const probeCode = (pid: number) => {
  try {
    process.kill(pid, 0);
    return "alive";
  } catch (error) {
    return Predicate.isObject(error) ? String(error.code) : String(error);
  }
};

/** Cleanup for a test that captured the group of a mock it spawned; never matches by name. */
const killGroupIfStillThere = (pgid: number) =>
  Effect.sync(() => {
    if (probeCode(-pgid) === "alive") process.kill(-pgid, "SIGKILL");
  });

interface KillCall {
  readonly pid: number;
  readonly signal: string | number | undefined;
}

/**
 * Replaces `process.kill` so signals aimed at a fake pid can be observed and never delivered.
 * A probe answers ESRCH once `groupGone` holds, and EPERM while `lingering` holds, as macOS does
 * for a group whose only members are zombies awaiting reap.
 */
const recordProcessKill = (
  groupGone: (calls: ReadonlyArray<KillCall>) => boolean,
  lingering: (calls: ReadonlyArray<KillCall>) => boolean = () => false,
) =>
  Effect.acquireRelease(
    Effect.sync(() => {
      const calls: Array<KillCall> = [];
      const spy = vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
        calls.push({ pid, signal });
        if (signal === 0 && lingering(calls)) {
          throw Object.assign(new Error("kill EPERM"), { code: "EPERM" });
        }
        if (signal === 0 && groupGone(calls)) {
          throw Object.assign(new Error("kill ESRCH"), { code: "ESRCH" });
        }
        return true;
      });
      return { calls, spy };
    }),
    ({ spy }) => Effect.sync(() => spy.mockRestore()),
  ).pipe(Effect.map(({ calls }) => calls));

const standardCommand = (command: ChildProcess.Command): ChildProcess.StandardCommand =>
  ChildProcess.isStandardCommand(command) ? command : assert.fail("unexpected pipeline");

const makeFakeDroidProcess = Effect.fn("makeFakeDroidProcess")(function* (
  options: { readonly stderr?: string; readonly pid?: number } = {},
) {
  const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
  const written = yield* Queue.unbounded<string>();
  const exitCode = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
  const commands: Array<ChildProcess.StandardCommand> = [];
  const decoder = new TextDecoder();
  let pendingInput = "";
  const handle = ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(options.pid ?? FAKE_PID),
    exitCode: Deferred.await(exitCode),
    isRunning: Effect.map(Deferred.isDone(exitCode), (done) => !done),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.forEach((chunk: Uint8Array) =>
      Effect.suspend(() => {
        pendingInput += decoder.decode(chunk, { stream: true });
        const lines = pendingInput.split("\n");
        pendingInput = lines.pop() ?? "";
        return Effect.forEach(lines, (line) => Queue.offer(written, line), { discard: true });
      }),
    ),
    stdout: Stream.fromQueue(stdout),
    stderr:
      options.stderr === undefined ? Stream.empty : Stream.make(encoder.encode(options.stderr)),
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
  return {
    commands,
    spawner: ChildProcessSpawner.make((command) => {
      commands.push(standardCommand(command));
      return Effect.succeed(handle);
    }),
    takeWritten: Effect.map(Queue.take(written), (line): LogEntry => decodeLogLine(line)),
    emitBytes: (bytes: Uint8Array) => Queue.offer(stdout, bytes),
    emit: (value: unknown) => Queue.offer(stdout, encoder.encode(`${encodeJsonLine(value)}\n`)),
    endStdout: Queue.end(stdout),
    exit: (code: number) => Deferred.succeed(exitCode, ChildProcessSpawner.ExitCode(code)),
  };
});

const startFakeRuntime = Effect.fn("startFakeRuntime")(function* (
  options: {
    readonly stderr?: string;
    readonly pid?: number;
    readonly grace?: DroidRpcRuntimeOptions["grace"];
    readonly env?: DroidRpcRuntimeOptions["env"];
  } = {},
) {
  const fake = yield* makeFakeDroidProcess({
    ...(options.stderr === undefined ? {} : { stderr: options.stderr }),
    ...(options.pid === undefined ? {} : { pid: options.pid }),
  });
  const fs = yield* FileSystem.FileSystem;
  const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-droid-rpc-fake-" });
  // Closing it early lets a test drive termination; the test's own scope closes it otherwise.
  const scope = yield* Scope.make();
  yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
  const runtime = yield* makeDroidRpcRuntime({
    binaryPath: "droid",
    cwd,
    ...(options.grace === undefined ? {} : { grace: options.grace }),
    ...(options.env === undefined ? {} : { env: options.env }),
  }).pipe(
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner),
    Scope.provide(scope),
  );
  return { fake, runtime, scope, cwd };
});

const responseLine = (id: unknown, result: unknown, version: string = DROID_PROTOCOL_VERSION) => ({
  jsonrpc: "2.0",
  type: "response",
  factoryApiVersion: "1.0.0",
  factoryProtocolVersion: version,
  id,
  result,
});

const notificationLine = (notification: Record<string, unknown>, sessionId = "session-1") => ({
  jsonrpc: "2.0",
  type: "notification",
  factoryApiVersion: "1.0.0",
  factoryProtocolVersion: DROID_PROTOCOL_VERSION,
  method: "droid.session_notification",
  params: { sessionId, notification },
});

const errorResponseLine = (id: unknown, error: unknown) => ({
  ...responseLine(id, undefined),
  result: undefined,
  error,
});

type FakeRuntime = Effect.Success<ReturnType<typeof startFakeRuntime>>;

/** Once it returns, the runtime has routed every frame the fake emitted before the call. */
const roundTrip = Effect.fn("roundTrip")(function* ({ fake, runtime }: FakeRuntime) {
  const fiber = yield* runtime.request("droid.list_models").pipe(Effect.forkChild);
  const line = yield* fake.takeWritten;
  yield* fake.emit(responseLine(line.id, { models: [] }));
  yield* Fiber.join(fiber);
});

const inboundRequestLine = (id: string, method: string, params: unknown) => ({
  jsonrpc: "2.0",
  type: "request",
  factoryApiVersion: "1.0.0",
  factoryProtocolVersion: DROID_PROTOCOL_VERSION,
  id,
  method,
  params,
});

/** What the mock `droid` saw in its own environment; it reports names and presence, never values. */
const childEnvironment = Effect.fn("childEnvironment")(function* (
  options: Partial<DroidRpcRuntimeOptions> = {},
) {
  const { mock, runtime } = yield* startMockRuntime({}, options);
  yield* runtime.request("droid.list_models");
  const entry = asRecord((yield* mock.readLog).find((candidate) => candidate.type === "env"));
  yield* runtime.close;
  return {
    factoryApiKeySet: entry.factoryApiKeySet === true,
    names: Array.isArray(entry.factoryEnvNames) ? entry.factoryEnvNames.map(asString) : [],
  };
});

/** A fresh copy of `rpc.ts`, so its once-per-process warning flag starts unset. */
const loadFreshRpcModule = Effect.promise(async () => {
  vi.resetModules();
  return import("./rpc.ts");
});

const yieldSome = Effect.forEach(Array.from({ length: 16 }), () => Effect.yieldNow, {
  discard: true,
});

describe("timeoutForMethod", () => {
  it("[VAL-RPC-008] uses 30 s by default, 60 s for session loading and rewind, 240 s for compaction", () => {
    const seconds = (method: string) => Duration.toSeconds(timeoutForMethod(method));
    assert.strictEqual(seconds("droid.add_user_message"), 30);
    assert.strictEqual(seconds("droid.list_models"), 30);
    assert.strictEqual(seconds("droid.close_session"), 30);
    assert.strictEqual(seconds("droid.initialize_session"), 60);
    assert.strictEqual(seconds("droid.load_session"), 60);
    assert.strictEqual(seconds("droid.execute_rewind"), 60);
    assert.strictEqual(seconds("droid.compact_session"), 240);
  });
});

it.layer(NodeServices.layer)("DroidRpcRuntime", (it) => {
  it.effect(
    "[VAL-RPC-001][VAL-RPC-002] Spawn arguments include --settings, and the file disables hooks, and close removes it",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const home = yield* HostProcess.HomeDirectory;
        const { mock, runtime } = yield* startMockRuntime();
        yield* runtime.request("droid.list_models");

        const log = yield* mock.readLog;
        assert.lengthOf(
          log.filter((entry) => entry.type === "argv"),
          1,
        );
        const settings = log.find((entry) => entry.type === "settings_file");
        assert.isDefined(settings);
        const settingsPath = asString(settings.path);
        assert.strictEqual(settings.content, '{"hooks":{"hooksDisabled":true}}');
        assert.deepStrictEqual(decodeJson(settings.content), { hooks: { hooksDisabled: true } });
        assert.deepStrictEqual(log.find((entry) => entry.type === "argv")?.args, [
          "exec",
          "--input-format",
          "stream-jsonrpc",
          "--output-format",
          "stream-jsonrpc",
          "--cwd",
          mock.projectDir,
          "--settings",
          settingsPath,
        ]);
        assert.isTrue(
          path.relative(mock.projectDir, settingsPath).startsWith(".."),
          "settings file must not live inside the project",
        );
        assert.isTrue(
          path.relative(path.join(home, ".factory"), settingsPath).startsWith(".."),
          "settings file must not live under ~/.factory",
        );
        const settingsDir = path.dirname(settingsPath);
        assert.isTrue(path.basename(settingsDir).startsWith("t3-droid-settings-"));
        assert.strictEqual(
          path.dirname(settingsDir),
          path.dirname(path.dirname(mock.projectDir)),
          "settings directory is a sibling of the test's temp root, in the OS temp dir",
        );
        assert.isTrue(yield* fs.exists(settingsPath));

        yield* runtime.close;
        assert.isFalse(yield* fs.exists(settingsPath));
        assert.isFalse(yield* fs.exists(settingsDir));
      }).pipe(live),
  );

  it.effect("[VAL-RPC-001] every spawn gets its own settings file", () =>
    Effect.gen(function* () {
      const mock = yield* makeRpcMockAgent();
      const options = { binaryPath: mock.binaryPath, cwd: mock.projectDir };
      const first = yield* makeDroidRpcRuntime(options);
      const second = yield* makeDroidRpcRuntime(options);
      yield* first.request("droid.list_models");
      yield* second.request("droid.list_models");

      const paths = (yield* mock.readLog)
        .filter((entry) => entry.type === "settings_file")
        .map((entry) => asString(entry.path));
      assert.lengthOf(paths, 2);
      assert.notStrictEqual(paths[0], paths[1]);
    }).pipe(live),
  );

  it.effect(
    "[VAL-RPC-002] a runtime whose scope closes without close still removes its settings file and reaps the process",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const mock = yield* makeRpcMockAgent();
        const scope = yield* Scope.make();
        const runtime = yield* makeDroidRpcRuntime({
          binaryPath: mock.binaryPath,
          cwd: mock.projectDir,
        }).pipe(Scope.provide(scope));
        yield* runtime.request("droid.list_models");
        const settingsPath = asString(
          (yield* mock.readLog).find((entry) => entry.type === "settings_file")?.path,
        );
        assert.isTrue(yield* fs.exists(settingsPath));

        yield* Scope.close(scope, Exit.void);

        assert.isFalse(yield* fs.exists(settingsPath));
        assert.isFalse(yield* fs.exists(path.dirname(settingsPath)));
        const log = yield* mock.readLog;
        assert.isTrue(log.some((entry) => entry.type === "exit"));
      }).pipe(live),
  );

  it.effect("every outgoing request carries the Droid envelope with a string id and params", () =>
    Effect.gen(function* () {
      const { mock, runtime } = yield* startMockRuntime();
      yield* runtime.request("droid.list_models");
      yield* initializeSession(runtime, mock.projectDir);
      yield* runtime.request("droid.update_session_settings", { autonomyLevel: "low" });

      const received = (yield* mock.readLog)
        .filter((entry) => entry.type === "recv")
        .map((entry) => asRecord(entry.message));
      assert.lengthOf(received, 3);
      for (const message of received) {
        assert.strictEqual(message.type, "request");
        assert.strictEqual(message.jsonrpc, "2.0");
        assert.strictEqual(message.factoryApiVersion, "1.0.0");
        assert.strictEqual(message.factoryProtocolVersion, DROID_PROTOCOL_VERSION);
        assert.isString(message.id);
        assert.isObject(message.params);
      }
      assert.deepStrictEqual(
        received.map((message) => message.method),
        ["droid.list_models", "droid.initialize_session", "droid.update_session_settings"],
      );
      assert.deepStrictEqual(received[0]?.params, {});
      assert.strictEqual(new Set(received.map((message) => message.id)).size, 3);
    }).pipe(live),
  );

  it.effect("resolves out-of-order responses to the right callers", () =>
    Effect.gen(function* () {
      const { fake, runtime } = yield* startFakeRuntime();
      const first = yield* runtime.request("droid.list_models").pipe(Effect.forkChild);
      const firstLine = yield* fake.takeWritten;
      const second = yield* runtime.request("droid.get_context_stats").pipe(Effect.forkChild);
      const secondLine = yield* fake.takeWritten;
      assert.strictEqual(firstLine.method, "droid.list_models");
      assert.strictEqual(secondLine.method, "droid.get_context_stats");

      yield* fake.emit(responseLine(secondLine.id, { answer: "second" }));
      yield* fake.emit(responseLine(firstLine.id, { answer: "first" }));

      assert.deepStrictEqual(yield* Fiber.join(second), { answer: "second" });
      assert.deepStrictEqual(yield* Fiber.join(first), { answer: "first" });
    }),
  );

  it.effect("an error response fails only its own caller", () =>
    Effect.gen(function* () {
      const { fake, runtime } = yield* startFakeRuntime();
      const failing = yield* runtime
        .request("droid.update_session_settings", { autonomyLevel: "bogus" })
        .pipe(Effect.asVoid, Effect.flip, Effect.forkChild);
      const failingLine = yield* fake.takeWritten;
      const healthy = yield* runtime.request("droid.list_models").pipe(Effect.forkChild);
      const healthyLine = yield* fake.takeWritten;

      yield* fake.emit({
        ...responseLine(failingLine.id, undefined),
        result: undefined,
        error: {
          code: -32602,
          message: "Invalid autonomy level",
          data: { field: "autonomyLevel" },
        },
      });
      yield* fake.emit(responseLine(healthyLine.id, { models: [] }));

      const error = yield* Fiber.join(failing);
      assert.strictEqual(error._tag, "DroidRpcResponseError");
      if (error._tag === "DroidRpcResponseError") {
        assert.strictEqual(error.method, "droid.update_session_settings");
        assert.strictEqual(error.code, -32602);
        assert.strictEqual(error.message, "Invalid autonomy level");
        assert.deepStrictEqual(error.data, { field: "autonomyLevel" });
      }
      assert.deepStrictEqual(yield* Fiber.join(healthy), { models: [] });
    }),
  );

  it.effect("maps error responses to typed failures carrying the JSON-RPC code and message", () =>
    Effect.gen(function* () {
      const { runtime } = yield* startMockRuntime();
      const noSession = yield* runtime
        .request("droid.add_user_message", { text: "hello" })
        .pipe(Effect.asVoid, Effect.flip);
      assert.strictEqual(noSession._tag, "DroidRpcResponseError");
      if (noSession._tag === "DroidRpcResponseError") {
        assert.strictEqual(noSession.method, "droid.add_user_message");
        assert.strictEqual(noSession.code, -32603);
        assert.strictEqual(
          noSession.message,
          "No active session. Call initialize_session or load_session first.",
        );
      }

      const unknown = yield* runtime.request("droid.not_a_method").pipe(Effect.asVoid, Effect.flip);
      assert.strictEqual(unknown._tag, "DroidRpcResponseError");
      if (unknown._tag === "DroidRpcResponseError") {
        assert.strictEqual(unknown.code, -32601);
        assert.strictEqual(unknown.message, "Unknown method: droid.not_a_method");
      }

      assert.deepStrictEqual(Object.keys(asRecord(yield* runtime.request("droid.list_models"))), [
        "models",
      ]);
    }).pipe(live),
  );

  it.effect(
    "drops a response with an unknown id and a late response for a timed-out request without affecting other callers",
    () => {
      const logs = captureLogs();
      return Effect.gen(function* () {
        const { fake, runtime } = yield* startFakeRuntime();
        const timedOut = yield* runtime
          .request("droid.list_models", {}, { timeout: "1 second" })
          .pipe(Effect.asVoid, Effect.flip, Effect.forkChild);
        const timedOutLine = yield* fake.takeWritten;
        const other = yield* runtime.request("droid.get_context_stats").pipe(Effect.forkChild);
        const otherLine = yield* fake.takeWritten;

        yield* TestClock.adjust("1 second");
        const error = yield* Fiber.join(timedOut);
        assert.strictEqual(error._tag, "DroidRpcTimeoutError");

        yield* fake.emit(responseLine(timedOutLine.id, { late: true }));
        yield* fake.emit(responseLine("no-such-id", { stray: true }));
        yield* fake.emit(responseLine(otherLine.id, { answer: "other" }));

        assert.deepStrictEqual(yield* Fiber.join(other), { answer: "other" });
        const dropped = logs.entries.filter((entry) =>
          entry.text.includes("Dropping Droid response for an unknown or timed-out request"),
        );
        assert.lengthOf(dropped, 2);
        assert.isTrue(
          dropped.some((entry) => entry.text.includes(`id=${String(timedOutLine.id)}`)),
        );
        assert.isTrue(dropped.some((entry) => entry.text.includes("id=no-such-id")));
      }).pipe(logs.provide);
    },
  );

  it.effect(
    "delivers an inbound permission request without blocking notifications or other responses",
    () =>
      Effect.gen(function* () {
        const { mock, runtime } = yield* startMockRuntime();
        const events = yield* collectEvents(runtime);
        yield* initializeSession(runtime, mock.projectDir);
        yield* runtime.request("droid.add_user_message", {
          text: "APPROVE_ME",
          messageId: "turn-1",
        });

        const { skipped, event: permission } = yield* takeUntil(
          events,
          (event) => event._tag === "Request",
        );
        assert.isTrue(skipped.some(isNotification("tool_call")));
        assert.strictEqual(permission._tag, "Request");
        if (permission._tag !== "Request") return;
        assert.strictEqual(permission.method, "droid.request_permission");

        // While the request is unanswered, later notifications and responses keep flowing.
        yield* takeUntil(events, isNotification("session_token_usage_changed"));
        yield* takeUntil(events, isNotification("session_title_updated"));
        const models = asRecord(yield* runtime.request("droid.list_models"));
        assert.isArray(models.models);

        yield* runtime.respond(permission.id, { selectedOption: "proceed_once" });
        const { skipped: afterAnswer, event: completed } = yield* takeUntil(
          events,
          isNotification("agent_turn_completed"),
        );
        assert.isTrue(afterAnswer.some(isNotification("permission_resolved")));
        assert.strictEqual(
          completed._tag === "Notification" ? completed.notification.reason : undefined,
          "completed",
        );

        const log = yield* mock.readLog;
        assert.deepStrictEqual(
          mockResponses(log, permission.id).map((entry) => entry.result),
          [{ selectedOption: "proceed_once" }],
        );
        const answer = log
          .filter((entry) => entry.type === "recv")
          .map((entry) => asRecord(entry.message))
          .find((message) => message.id === permission.id);
        assert.deepStrictEqual(answer, {
          type: "response",
          jsonrpc: "2.0",
          factoryApiVersion: "1.0.0",
          factoryProtocolVersion: DROID_PROTOCOL_VERSION,
          id: permission.id,
          result: { selectedOption: "proceed_once" },
        });
      }).pipe(live),
  );

  it.effect(
    "surfaces permission, ask_user and pick_options requests in arrival order and answers each exactly once",
    () =>
      Effect.gen(function* () {
        const { fake, runtime } = yield* startFakeRuntime();
        const events = yield* collectEvents(runtime);
        yield* fake.emit(inboundRequestLine("perm-1", "droid.request_permission", { n: 1 }));
        yield* fake.emit(inboundRequestLine("ask-1", "droid.ask_user", { n: 2 }));
        yield* fake.emit(notificationLine({ type: "tool_call", toolUseId: "tool-1" }));
        yield* fake.emit(inboundRequestLine("pick-1", "droid.pick_options", { n: 3 }));

        const arrived = [
          yield* Queue.take(events),
          yield* Queue.take(events),
          yield* Queue.take(events),
          yield* Queue.take(events),
        ];
        assert.deepStrictEqual(
          arrived.map((event) => (event._tag === "Request" ? `${event.method}#${event.id}` : "n")),
          [
            "droid.request_permission#perm-1",
            "droid.ask_user#ask-1",
            "n",
            "droid.pick_options#pick-1",
          ],
        );

        yield* runtime.respond("perm-1", { selectedOption: "proceed_once" });
        yield* runtime.respondError("ask-1", { code: -32000, message: "declined" });
        yield* runtime.answer("pick-1", Effect.succeed({ selectedIds: ["a"], cancelled: false }));
        const lines = [yield* fake.takeWritten, yield* fake.takeWritten, yield* fake.takeWritten];
        assert.deepStrictEqual(lines, [
          {
            jsonrpc: "2.0",
            factoryApiVersion: "1.0.0",
            factoryProtocolVersion: DROID_PROTOCOL_VERSION,
            type: "response",
            id: "perm-1",
            result: { selectedOption: "proceed_once" },
          },
          {
            jsonrpc: "2.0",
            factoryApiVersion: "1.0.0",
            factoryProtocolVersion: DROID_PROTOCOL_VERSION,
            type: "response",
            id: "ask-1",
            error: { code: -32000, message: "declined" },
          },
          {
            jsonrpc: "2.0",
            factoryApiVersion: "1.0.0",
            factoryProtocolVersion: DROID_PROTOCOL_VERSION,
            type: "response",
            id: "pick-1",
            result: { selectedIds: ["a"], cancelled: false },
          },
        ]);

        for (const id of ["perm-1", "ask-1", "pick-1"]) {
          const second = yield* runtime.respond(id, { again: true }).pipe(Effect.flip);
          assert.strictEqual(second._tag, "DroidRpcUnknownInboundRequestError");
          const secondError = yield* runtime
            .respondError(id, { code: -32000, message: "again" })
            .pipe(Effect.flip);
          assert.strictEqual(secondError._tag, "DroidRpcUnknownInboundRequestError");
        }
        // The next line written is the next request, so the rejected answers wrote nothing.
        yield* runtime.request("droid.list_models").pipe(Effect.forkChild);
        assert.strictEqual((yield* fake.takeWritten).method, "droid.list_models");
      }),
  );

  it.effect("answers unknown inbound methods with -32601 without surfacing them", () =>
    Effect.gen(function* () {
      const { mock, runtime } = yield* startMockRuntime();
      const events = yield* collectEvents(runtime);
      yield* initializeSession(runtime, mock.projectDir);
      yield* runtime.request("droid.add_user_message", { text: "UNKNOWN_INBOUND" });

      const { skipped, event } = yield* takeUntil(events, isNotification("agent_turn_completed"));
      assert.strictEqual(
        event._tag === "Notification" ? event.notification.reason : "",
        "completed",
      );
      assert.isFalse(skipped.some((entry) => entry._tag === "Request"));

      const answers = (yield* mock.readLog).filter((entry) => entry.type === "response");
      assert.lengthOf(answers, 1);
      assert.strictEqual(asRecord(answers[0]?.error).code, -32601);
      assert.strictEqual(
        asRecord(answers[0]?.error).message,
        "Unknown method: droid.future_request",
      );
    }).pipe(live),
  );

  it.effect(
    "answers a failing handler with a JSON-RPC error and never answers an inbound id twice",
    () =>
      Effect.gen(function* () {
        const { mock, runtime } = yield* startMockRuntime();
        const events = yield* collectEvents(runtime);
        yield* initializeSession(runtime, mock.projectDir);
        yield* runtime.request("droid.add_user_message", { text: "APPROVE_ME" });
        const { event: permission } = yield* takeUntil(events, (event) => event._tag === "Request");
        if (permission._tag !== "Request") return assert.fail("expected an inbound request");

        yield* runtime.answer(
          permission.id,
          Effect.fail(new HandlerFailure({ message: "approval store unavailable" })),
        );
        const { skipped } = yield* takeUntil(events, isNotification("agent_turn_completed"));
        const toolResult = skipped.find(isNotification("tool_result"));
        assert.strictEqual(
          toolResult?._tag === "Notification" ? toolResult.notification.isError : undefined,
          true,
        );

        const again = yield* runtime
          .respond(permission.id, { selectedOption: "proceed_once" })
          .pipe(Effect.flip);
        assert.strictEqual(again._tag, "DroidRpcUnknownInboundRequestError");
        const errorAgain = yield* runtime
          .respondError(permission.id, { code: -32000, message: "again" })
          .pipe(Effect.flip);
        assert.strictEqual(errorAgain._tag, "DroidRpcUnknownInboundRequestError");
        const handlerAgain = yield* runtime
          .answer(permission.id, Effect.succeed({ selectedOption: "proceed_once" }))
          .pipe(Effect.flip);
        assert.strictEqual(handlerAgain._tag, "DroidRpcUnknownInboundRequestError");

        // A later round trip proves nothing else was written for that id: the mock logs every
        // received line before it replies.
        yield* runtime.request("droid.list_models");
        const answers = mockResponses(yield* mock.readLog, permission.id);
        assert.lengthOf(answers, 1);
        const error = asRecord(answers[0]?.error);
        assert.strictEqual(error.code, -32603);
        assert.include(String(error.message), "approval store unavailable");
      }).pipe(live),
  );

  it.effect("decodes a stdout line over 10 MB with U+2028 and U+2029 inside a string", () =>
    Effect.gen(function* () {
      const { mock, runtime } = yield* startMockRuntime();
      const events = yield* collectEvents(runtime);
      yield* initializeSession(runtime, mock.projectDir);
      yield* runtime.request("droid.add_user_message", { text: "HUGE_RESULT" });

      const { event } = yield* takeUntil(events, isNotification("tool_result"));
      const content = asString(
        event._tag === "Notification" ? event.notification.content : undefined,
      );
      assert.isAbove(content.length, 10 * 1024 * 1024);
      assert.isTrue(content.startsWith("big:"));
      assert.isTrue(content.endsWith("\u2028\u2029end"));
      yield* takeUntil(events, isNotification("agent_turn_completed"));
    }).pipe(live),
  );

  it.effect("reassembles lines split into 1-byte chunks without splitting on U+2028/U+2029", () =>
    Effect.gen(function* () {
      const { fake, runtime } = yield* startFakeRuntime();
      const events = yield* collectEvents(runtime);
      const text = "caf\u00e9 \u2713 line\u2028separator\u2029paragraph";
      const bytes = encoder.encode(
        `${encodeJsonLine(notificationLine({ type: "assistant_text_delta", text }))}\n${encodeJsonLine(notificationLine({ type: "thinking_text_delta", text }))}\n`,
      );
      for (const byte of bytes) {
        yield* fake.emitBytes(Uint8Array.of(byte));
      }

      const first = yield* Queue.take(events);
      const second = yield* Queue.take(events);
      assert.deepStrictEqual(first, {
        _tag: "Notification",
        sessionId: "session-1",
        notification: { type: "assistant_text_delta", text },
      });
      assert.deepStrictEqual(second, {
        _tag: "Notification",
        sessionId: "session-1",
        notification: { type: "thinking_text_delta", text },
      });
    }),
  );

  it.effect(
    "a malformed stdout line fails pending requests with the stderr tail, ends the event stream, and stops the process",
    () =>
      Effect.gen(function* () {
        const { mock, runtime } = yield* startMockRuntime();
        const events = yield* collectEvents(runtime);
        yield* initializeSession(runtime, mock.projectDir);

        const error = yield* runtime
          .request("droid.add_user_message", { text: "MALFORMED_LINE" })
          .pipe(Effect.asVoid, Effect.flip);
        assert.strictEqual(error._tag, "DroidRpcTerminatedError");
        if (error._tag !== "DroidRpcTerminatedError") return;
        assert.strictEqual(error.reason, "malformed_output");
        assert.include(error.stderrTail, "mock: writing a malformed stdout line");
        assert.include(error.message, "mock: writing a malformed stdout line");

        const streamEnd = yield* takeUntil(events, () => false).pipe(Effect.flip);
        assert.instanceOf(streamEnd, DroidRpcTerminatedError);

        const exit = yield* runtime.awaitExit;
        assert.isFalse(exit.code === null && exit.signal === null);
        const log = yield* mock.readLog;
        assert.isTrue(log.some((entry) => entry.type === "exit"));
        // The runtime closes stdin and sends SIGTERM together; whichever reaches the mock first
        // ends it.
        assert.isTrue(log.some((entry) => entry.type === "sigterm" || entry.type === "stdin_eof"));
        const later = yield* runtime.request("droid.list_models").pipe(Effect.asVoid, Effect.flip);
        assert.strictEqual(later._tag, "DroidRpcTerminatedError");
      }).pipe(live),
  );

  it.effect("process exit mid-request fails pending requests with a bounded stderr tail", () =>
    Effect.gen(function* () {
      const { mock, runtime } = yield* startMockRuntime();
      const events = yield* collectEvents(runtime);
      yield* initializeSession(runtime, mock.projectDir);

      const error = yield* runtime
        .request("droid.add_user_message", { text: "CRASH_ME" })
        .pipe(Effect.asVoid, Effect.flip);
      assert.strictEqual(error._tag, "DroidRpcTerminatedError");
      if (error._tag !== "DroidRpcTerminatedError") return;
      assert.strictEqual(error.reason, "process_exit");
      assert.strictEqual(error.exitCode, 1);
      assert.include(error.message, "fatal: mock crash");
      assert.isAtMost(error.stderrTail.length, DROID_STDERR_TAIL_MAX);
      assert.isTrue(error.stderrTail.endsWith("fatal: mock crash\n"));
      const tail = yield* runtime.stderrTail;
      assert.isAtMost(tail.length, DROID_STDERR_TAIL_MAX);
      assert.isTrue(tail.endsWith("fatal: mock crash\n"));

      const streamEnd = yield* takeUntil(events, () => false).pipe(Effect.flip);
      assert.instanceOf(streamEnd, DroidRpcTerminatedError);
      assert.deepStrictEqual(yield* runtime.awaitExit, { code: 1, signal: null });
    }).pipe(live),
  );

  it.effect(
    "a process exit seen before the last stdout frames still resolves the final response and delivers the final notification",
    () =>
      Effect.gen(function* () {
        const { fake, runtime } = yield* startFakeRuntime();
        const events = yield* collectEvents(runtime);
        const fiber = yield* runtime.request("droid.list_models").pipe(Effect.forkChild);
        const line = yield* fake.takeWritten;

        yield* fake.exit(0);
        assert.deepStrictEqual(yield* runtime.awaitExit, { code: 0, signal: null });
        // Give the exit watcher every chance to act before the final frames arrive.
        yield* yieldSome;

        const turnCompleted = {
          type: "agent_turn_completed",
          turnId: "turn-1",
          reason: "process_exit",
        };
        yield* fake.emit(responseLine(line.id, { finalResponse: true }));
        yield* fake.emit(notificationLine(turnCompleted));
        yield* fake.endStdout;

        assert.deepStrictEqual(yield* Fiber.join(fiber), { finalResponse: true });
        assert.deepStrictEqual(yield* Queue.take(events), {
          _tag: "Notification",
          sessionId: "session-1",
          notification: turnCompleted,
        });
        const streamEnd = yield* takeUntil(events, () => false).pipe(Effect.flip);
        assert.instanceOf(streamEnd, DroidRpcTerminatedError);
        if (Cause.isDone(streamEnd)) return;
        assert.strictEqual(streamEnd.reason, "process_exit");
        assert.strictEqual(streamEnd.exitCode, 0);
      }),
  );

  it.effect(
    "a process exit without stdout EOF fails pending requests with the stderr tail once the drain grace elapses, not before",
    () =>
      Effect.gen(function* () {
        const { fake, runtime } = yield* startFakeRuntime({
          stderr: "fatal: stuck pipe\n",
          grace: { drain: "500 millis" },
        });
        const events = yield* collectEvents(runtime);
        const fiber = yield* runtime
          .request("droid.list_models")
          .pipe(Effect.asVoid, Effect.flip, Effect.forkChild);
        yield* fake.takeWritten;

        yield* fake.exit(1);
        assert.deepStrictEqual(yield* runtime.awaitExit, { code: 1, signal: null });
        yield* yieldSome;
        assert.isUndefined(fiber.pollUnsafe());

        yield* TestClock.adjust("499 millis");
        yield* yieldSome;
        assert.isUndefined(fiber.pollUnsafe());

        yield* TestClock.adjust("1 millis");
        const error = yield* Fiber.join(fiber);
        assert.strictEqual(error._tag, "DroidRpcTerminatedError");
        if (error._tag !== "DroidRpcTerminatedError") return;
        assert.strictEqual(error.reason, "process_exit");
        assert.strictEqual(error.exitCode, 1);
        assert.strictEqual(error.stderrTail, "fatal: stuck pipe\n");
        const streamEnd = yield* takeUntil(events, () => false).pipe(Effect.flip);
        assert.instanceOf(streamEnd, DroidRpcTerminatedError);
      }),
  );

  it.effect(
    "[VAL-RPC-010] drops notifications of an unknown type, method, or shape, and keeps delivering known ones",
    () => {
      const logs = captureLogs();
      return Effect.gen(function* () {
        const { mock, runtime } = yield* startMockRuntime();
        const events = yield* collectEvents(runtime);
        yield* initializeSession(runtime, mock.projectDir);
        yield* runtime.request("droid.add_user_message", { text: "UNKNOWN_EVENT" });

        const { skipped, event } = yield* takeUntil(events, isNotification("agent_turn_completed"));
        assert.strictEqual(
          event._tag === "Notification" ? event.notification.reason : "",
          "completed",
        );
        const delivered = new Set(skipped.map(notificationType));
        for (const dropped of [
          "mission_heartbeat",
          "tool_execution_heartbeat",
          "some_future_event_v2",
          "future",
        ]) {
          assert.isFalse(delivered.has(dropped), `${dropped} must not reach the event stream`);
        }
        assert.isTrue(delivered.has("create_message"));

        const warned = (type: string) =>
          logs.entries.some(
            (entry) => entry.level === "Debug" && entry.text.includes(`type=${type}`),
          );
        assert.isTrue(warned("mission_heartbeat"));
        assert.isTrue(warned("some_future_event_v2"));
        assert.isTrue(
          logs.entries.some(
            (entry) =>
              entry.level === "Warn" && entry.text.includes("method=droid.future_notification"),
          ),
        );
        assert.isNotEmpty(
          logs.entries.filter(
            (entry) =>
              entry.level === "Warn" && entry.text.includes("method=droid.session_notification"),
          ),
        );

        // The process, the stream, and later requests are all unaffected.
        assert.isArray(asRecord(yield* runtime.request("droid.list_models")).models);
        assert.deepStrictEqual(yield* runtime.close, { code: 0, signal: null });
        const end = yield* takeUntil(events, () => false).pipe(Effect.flip);
        assert.isTrue(Cause.isDone(end));
      }).pipe(live, logs.provide);
    },
  );

  it.effect(
    "[VAL-RPC-010] an unknown notification type between a request and its response changes nothing for either",
    () => {
      const logs = captureLogs();
      return Effect.gen(function* () {
        const { fake, runtime } = yield* startFakeRuntime();
        const events = yield* collectEvents(runtime);
        const pending = yield* runtime.request("droid.list_models").pipe(Effect.forkChild);
        const line = yield* fake.takeWritten;

        yield* fake.emit(notificationLine({ type: "mission_state_changed", state: "running" }));
        yield* fake.emit(notificationLine({ type: "tool_call", toolUseId: "tool-1" }));
        yield* fake.emit(responseLine(line.id, { models: [] }));

        const first = yield* Queue.take(events);
        assert.deepStrictEqual(first, {
          _tag: "Notification",
          sessionId: "session-1",
          notification: { type: "tool_call", toolUseId: "tool-1" },
        });
        assert.deepStrictEqual(yield* Fiber.join(pending), { models: [] });
        assert.isTrue(
          logs.entries.some(
            (entry) => entry.level === "Debug" && entry.text.includes("type=mission_state_changed"),
          ),
        );
      }).pipe(logs.provide);
    },
  );

  it.effect("records the peer protocol version without warning when it matches", () => {
    const logs = captureLogs();
    return Effect.gen(function* () {
      const { runtime } = yield* startMockRuntime();
      assert.isTrue(Option.isNone(yield* runtime.peerProtocolVersion));
      yield* runtime.request("droid.list_models");
      assert.deepStrictEqual(
        yield* runtime.peerProtocolVersion,
        Option.some(DROID_PROTOCOL_VERSION),
      );
      assert.strictEqual(DROID_PROTOCOL_VERSION, "1.269.0");
      assert.lengthOf(
        logs.entries.filter((entry) => entry.level === "Warn"),
        0,
      );
    }).pipe(live, logs.provide);
  });

  it.effect("keeps the first peer protocol version it sees", () =>
    Effect.gen(function* () {
      const { fake, runtime } = yield* startFakeRuntime();
      const first = yield* runtime.request("droid.list_models").pipe(Effect.forkChild);
      const firstLine = yield* fake.takeWritten;
      yield* fake.emit(responseLine(firstLine.id, {}, "1.100.0"));
      yield* Fiber.join(first);
      const second = yield* runtime.request("droid.list_models").pipe(Effect.forkChild);
      const secondLine = yield* fake.takeWritten;
      yield* fake.emit(responseLine(secondLine.id, {}, "1.200.0"));
      yield* Fiber.join(second);

      assert.deepStrictEqual(yield* runtime.peerProtocolVersion, Option.some("1.100.0"));
    }),
  );

  it.effect(
    "[VAL-RPC-011] warns once on a protocol version mismatch and keeps working, across runtimes in one process",
    () => {
      const logs = captureLogs();
      return Effect.gen(function* () {
        const rpc = yield* loadFreshRpcModule;
        const first = yield* makeRpcMockAgent({ DROID_MOCK_PROTOCOL_VERSION: "9.9.9" });
        const second = yield* makeRpcMockAgent({ DROID_MOCK_PROTOCOL_VERSION: "9.9.9" });
        const runtimeA = yield* rpc.makeDroidRpcRuntime({
          binaryPath: first.binaryPath,
          cwd: first.projectDir,
        });
        const runtimeB = yield* rpc.makeDroidRpcRuntime({
          binaryPath: second.binaryPath,
          cwd: second.projectDir,
        });

        yield* runtimeA.request("droid.list_models");
        assert.deepStrictEqual(yield* runtimeA.peerProtocolVersion, Option.some("9.9.9"));
        assert.isArray(asRecord(yield* runtimeA.request("droid.list_models")).models);
        yield* runtimeB.request("droid.list_models");
        assert.deepStrictEqual(yield* runtimeB.peerProtocolVersion, Option.some("9.9.9"));

        const warnings = logs.entries.filter((entry) => entry.level === "Warn");
        assert.lengthOf(warnings, 1);
        assert.include(warnings[0]?.text, "9.9.9");
        assert.include(warnings[0]?.text, DROID_PROTOCOL_VERSION);
      }).pipe(live, logs.provide);
    },
  );

  it.effect("a matching peer never uses up the once-per-process mismatch warning", () => {
    const logs = captureLogs();
    return Effect.gen(function* () {
      const rpc = yield* loadFreshRpcModule;
      const matching = yield* makeRpcMockAgent();
      const mismatching = yield* makeRpcMockAgent({ DROID_MOCK_PROTOCOL_VERSION: "2.0.0" });
      const matchingRuntime = yield* rpc.makeDroidRpcRuntime({
        binaryPath: matching.binaryPath,
        cwd: matching.projectDir,
      });
      yield* matchingRuntime.request("droid.list_models");
      assert.lengthOf(
        logs.entries.filter((entry) => entry.level === "Warn"),
        0,
      );

      const mismatchingRuntime = yield* rpc.makeDroidRpcRuntime({
        binaryPath: mismatching.binaryPath,
        cwd: mismatching.projectDir,
      });
      yield* mismatchingRuntime.request("droid.list_models");
      yield* mismatchingRuntime.request("droid.list_models");
      assert.lengthOf(
        logs.entries.filter((entry) => entry.level === "Warn"),
        1,
      );
    }).pipe(live, logs.provide);
  });

  it.effect("[VAL-RPC-008] an unanswered request times out at its limit and not before", () =>
    Effect.gen(function* () {
      const { fake, runtime } = yield* startFakeRuntime();
      const fiber = yield* runtime
        .request("droid.add_user_message", { text: "hello" })
        .pipe(Effect.asVoid, Effect.flip, Effect.forkChild);
      const line = yield* fake.takeWritten;
      assert.strictEqual(line.method, "droid.add_user_message");
      yield* yieldSome;

      yield* TestClock.adjust("29999 millis");
      yield* yieldSome;
      assert.isUndefined(fiber.pollUnsafe());

      yield* TestClock.adjust("1 millis");
      const error = yield* Fiber.join(fiber);
      assert.strictEqual(error._tag, "DroidRpcTimeoutError");
      if (error._tag === "DroidRpcTimeoutError") {
        assert.strictEqual(error.method, "droid.add_user_message");
        assert.strictEqual(error.timeoutMs, 30_000);
      }
    }),
  );

  it.effect(
    "[VAL-RPC-008] session loading and rewind time out at 60 s and compaction at 240 s, each not before",
    () =>
      Effect.gen(function* () {
        const { fake, runtime } = yield* startFakeRuntime();
        const methods = [
          "droid.initialize_session",
          "droid.load_session",
          "droid.execute_rewind",
          "droid.compact_session",
        ];
        const fibers = new Map<string, Fiber.Fiber<DroidRpcRequestError, void>>();
        for (const method of methods) {
          fibers.set(
            method,
            yield* runtime.request(method).pipe(Effect.asVoid, Effect.flip, Effect.forkChild),
          );
          yield* fake.takeWritten;
        }
        const settled = (method: string) => fibers.get(method)?.pollUnsafe() !== undefined;
        const assertTimedOut = Effect.fn("assertTimedOut")(function* (
          method: string,
          timeoutMs: number,
        ) {
          const error = yield* Fiber.join(fibers.get(method) ?? assert.fail("no fiber"));
          assert.strictEqual(error._tag, "DroidRpcTimeoutError");
          if (error._tag === "DroidRpcTimeoutError") {
            assert.strictEqual(error.method, method);
            assert.strictEqual(error.timeoutMs, timeoutMs);
          }
        });

        yield* TestClock.adjust("59999 millis");
        yield* yieldSome;
        assert.deepStrictEqual(methods.filter(settled), []);

        yield* TestClock.adjust("1 millis");
        yield* yieldSome;
        assert.deepStrictEqual(methods.filter(settled), methods.slice(0, 3));
        for (const method of methods.slice(0, 3)) {
          yield* assertTimedOut(method, 60_000);
        }

        yield* TestClock.adjust("179999 millis");
        yield* yieldSome;
        assert.isFalse(settled("droid.compact_session"));

        yield* TestClock.adjust("1 millis");
        yield* assertTimedOut("droid.compact_session", 240_000);
      }),
  );

  it.effect("[VAL-RPC-008] no timeout covers a whole turn", () =>
    Effect.gen(function* () {
      const { fake, runtime } = yield* startFakeRuntime();
      const events = yield* collectEvents(runtime);
      const ack = yield* runtime
        .request("droid.add_user_message", { text: "long turn" })
        .pipe(Effect.forkChild);
      const line = yield* fake.takeWritten;
      yield* fake.emit(responseLine(line.id, {}));
      yield* Fiber.join(ack);

      yield* TestClock.adjust("1 hour");
      yield* fake.emit(notificationLine({ type: "agent_turn_completed", reason: "completed" }));
      const event = yield* Queue.take(events);
      assert.strictEqual(notificationType(event), "agent_turn_completed");
    }),
  );

  it.effect(
    "close on an idle runtime sends close_session, the process exits 0, and no signal is sent",
    () =>
      Effect.gen(function* () {
        const { mock, runtime } = yield* startMockRuntime();
        const events = yield* collectEvents(runtime);
        yield* runtime.request("droid.list_models");

        const signals = vi.spyOn(process, "kill");
        const exit = yield* runtime.close.pipe(
          Effect.ensuring(Effect.sync(() => signals.mockRestore())),
        );
        assert.deepStrictEqual(exit, { code: 0, signal: null });
        assert.lengthOf(signals.mock.calls, 0);

        const log = yield* mock.readLog;
        assert.deepStrictEqual(receivedMethods(log), ["droid.list_models", "droid.close_session"]);
        assert.isFalse(log.some((entry) => entry.type === "sigterm"));
        assert.isTrue(log.some((entry) => entry.type === "stdin_eof"));
        assert.deepStrictEqual(log.at(-1), { type: "exit", code: 0 });

        const end = yield* takeUntil(events, () => false).pipe(Effect.flip);
        assert.isTrue(Cause.isDone(end));
      }).pipe(live),
  );

  it.effect("close after a completed turn is as graceful as close on a fresh runtime", () =>
    Effect.gen(function* () {
      const { mock, runtime } = yield* startMockRuntime();
      const events = yield* collectEvents(runtime);
      yield* initializeSession(runtime, mock.projectDir);
      yield* runtime.request("droid.add_user_message", { text: "hello" });
      yield* takeUntil(events, isNotification("agent_turn_completed"));

      assert.deepStrictEqual(yield* runtime.close, { code: 0, signal: null });
      const log = yield* mock.readLog;
      assert.deepStrictEqual(receivedMethods(log), [
        "droid.initialize_session",
        "droid.add_user_message",
        "droid.close_session",
      ]);
      assert.isFalse(log.some((entry) => entry.type === "sigterm"));
    }).pipe(live),
  );

  it.effect("close interrupts a running turn before it closes the session", () =>
    Effect.gen(function* () {
      const { mock, runtime } = yield* startMockRuntime();
      const events = yield* collectEvents(runtime);
      yield* initializeSession(runtime, mock.projectDir);
      yield* runtime.request("droid.add_user_message", { text: "INTERRUPT_ME" });
      yield* takeUntil(events, isNotification("assistant_text_delta"));

      assert.deepStrictEqual(yield* runtime.close, { code: 0, signal: null });
      const log = yield* mock.readLog;
      assert.deepStrictEqual(receivedMethods(log), [
        "droid.initialize_session",
        "droid.add_user_message",
        "droid.interrupt_session",
        "droid.close_session",
      ]);
      assert.isFalse(log.some((entry) => entry.type === "sigterm"));
    }).pipe(live),
  );

  it.effect(
    "[VAL-RPC-004][VAL-RPC-005] close escalates to SIGTERM then SIGKILL when the process ignores close_session, stdin EOF and SIGTERM, and reports the SIGKILL it sent",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const posix = (yield* HostProcess.Platform) !== "win32";
        const { mock, runtime } = yield* startMockRuntime(
          { DROID_MOCK_STUBBORN: "1" },
          { grace: { closeSession: "5 seconds", exit: "100 millis", terminate: "100 millis" } },
        );
        // The first response proves the mock installed its SIGTERM handler.
        yield* runtime.request("droid.list_models");
        const pid = spawnedPid(yield* mock.readLog);
        if (posix) {
          assert.isTrue(yield* groupExists(pid), "the child leads a process group of its own");
        }

        const exit = yield* runtime.close;
        const log = yield* mock.readLog;
        assert.isTrue(receivedMethods(log).includes("droid.close_session"));
        assert.isTrue(log.some((entry) => entry.type === "stdin_eof"));
        if (posix) {
          assert.deepStrictEqual(exit, { code: null, signal: "SIGKILL" });
          assert.deepStrictEqual(yield* runtime.awaitExit, { code: null, signal: "SIGKILL" });
          assert.isTrue(log.some((entry) => entry.type === "sigterm"));
          assert.isFalse(yield* groupExists(pid), "the whole group is gone after close");
        }
        const settingsPath = asString(log.find((entry) => entry.type === "settings_file")?.path);
        assert.isFalse(yield* fs.exists(settingsPath));
      }).pipe(live),
  );

  it.effect(
    "[VAL-RPC-005] reports SIGTERM when the process dies from the SIGTERM the runtime sent",
    () =>
      Effect.gen(function* () {
        const posix = (yield* HostProcess.Platform) !== "win32";
        const { mock, runtime } = yield* startMockRuntime(
          { DROID_MOCK_STUBBORN: "1", DROID_MOCK_SIGTERM: "die" },
          { grace: { closeSession: "5 seconds", exit: "100 millis", terminate: "5 seconds" } },
        );
        yield* runtime.request("droid.list_models");

        const exit = yield* runtime.close;
        const log = yield* mock.readLog;
        assert.isTrue(log.some((entry) => entry.type === "stdin_eof"));
        if (posix) {
          assert.deepStrictEqual(exit, { code: null, signal: "SIGTERM" });
          assert.deepStrictEqual(yield* runtime.awaitExit, { code: null, signal: "SIGTERM" });
        }
      }).pipe(live),
  );

  it.effect(
    "[VAL-RPC-005] a DroidRpcTerminatedError after a forced stop names the signal the runtime sent",
    () =>
      Effect.gen(function* () {
        const posix = (yield* HostProcess.Platform) !== "win32";
        const cases = [
          { env: { DROID_MOCK_STUBBORN: "1" }, signal: "SIGKILL" },
          { env: { DROID_MOCK_STUBBORN: "1", DROID_MOCK_SIGTERM: "die" }, signal: "SIGTERM" },
        ];
        for (const { env, signal } of cases) {
          const { mock, runtime } = yield* startMockRuntime(env, {
            grace: { terminate: "100 millis" },
          });
          yield* initializeSession(runtime, mock.projectDir);
          const error = yield* runtime
            .request("droid.add_user_message", { text: "MALFORMED_LINE" })
            .pipe(Effect.asVoid, Effect.flip);
          assert.strictEqual(error._tag, "DroidRpcTerminatedError");
          if (error._tag !== "DroidRpcTerminatedError") return;
          assert.strictEqual(error.reason, "malformed_output");
          if (posix) {
            assert.strictEqual(error.exitCode, null);
            assert.strictEqual(error.signal, signal);
            assert.include(error.detail, `signal ${signal}`);
            assert.deepStrictEqual(yield* runtime.awaitExit, { code: null, signal });
          }
        }
      }).pipe(live),
  );

  it.effect(
    "[VAL-RPC-004] spawns detached with the inherited environment, the Droid argv, and a SIGTERM kill signal",
    () =>
      Effect.gen(function* () {
        const platform = yield* HostProcess.Platform;
        const { fake, cwd } = yield* startFakeRuntime({ env: { FACTORY_API_KEY: SENTINEL_KEY } });

        assert.lengthOf(fake.commands, 1);
        const command = fake.commands[0] ?? assert.fail("no spawn");
        assert.strictEqual(command.options.detached, platform !== "win32");
        assert.strictEqual(command.options.extendEnv, true);
        assert.strictEqual(command.options.killSignal, "SIGTERM");
        assert.strictEqual(command.options.cwd, cwd);
        assert.deepStrictEqual(command.options.env, { FACTORY_API_KEY: SENTINEL_KEY });
        assert.deepStrictEqual(command.args.slice(0, 7), [
          "exec",
          "--input-format",
          "stream-jsonrpc",
          "--output-format",
          "stream-jsonrpc",
          "--cwd",
          cwd,
        ]);
        assert.strictEqual(command.args[7], "--settings");
        assert.lengthOf(command.args, 9);
      }),
  );

  it.effect(
    "[VAL-RPC-004] stops the process group with SIGTERM, waits the bounded grace, re-checks it, then SIGKILLs",
    () =>
      Effect.gen(function* () {
        if ((yield* HostProcess.Platform) === "win32") return;
        const calls = yield* recordProcessKill((seen) =>
          seen.some((call) => call.signal === "SIGKILL"),
        );
        const { scope } = yield* startFakeRuntime({ grace: { terminate: "5 seconds" } });
        const closing = yield* Scope.close(scope, Exit.void).pipe(Effect.forkChild);
        yield* yieldSome;

        const signals = () => calls.filter((call) => call.signal !== 0).map((call) => call.signal);
        assert.deepStrictEqual(signals(), ["SIGTERM"]);
        yield* TestClock.adjust("4999 millis");
        yield* yieldSome;
        assert.deepStrictEqual(signals(), ["SIGTERM"]);

        yield* TestClock.adjust("1 millis");
        yield* Fiber.join(closing);
        assert.deepStrictEqual(calls, [
          { pid: -FAKE_PID, signal: 0 },
          { pid: -FAKE_PID, signal: "SIGTERM" },
          { pid: -FAKE_PID, signal: 0 },
          { pid: -FAKE_PID, signal: "SIGKILL" },
          { pid: -FAKE_PID, signal: 0 },
        ]);
      }),
  );

  it.effect("[VAL-RPC-004] sends no SIGKILL when the group is gone after SIGTERM", () =>
    Effect.gen(function* () {
      if ((yield* HostProcess.Platform) === "win32") return;
      const calls = yield* recordProcessKill((seen) =>
        seen.some((call) => call.signal === "SIGTERM"),
      );
      const { scope } = yield* startFakeRuntime({ grace: { terminate: "5 seconds" } });
      const closing = yield* Scope.close(scope, Exit.void).pipe(Effect.forkChild);
      yield* yieldSome;
      yield* TestClock.adjust("5 seconds");
      yield* Fiber.join(closing);

      assert.deepStrictEqual(calls, [
        { pid: -FAKE_PID, signal: 0 },
        { pid: -FAKE_PID, signal: "SIGTERM" },
        { pid: -FAKE_PID, signal: 0 },
      ]);
    }),
  );

  it.effect("[VAL-RPC-004] never signals a process that has already exited", () =>
    Effect.gen(function* () {
      if ((yield* HostProcess.Platform) === "win32") return;
      const calls = yield* recordProcessKill(() => false);
      const { fake, scope } = yield* startFakeRuntime();
      yield* fake.exit(0);
      yield* yieldSome;

      yield* Scope.close(scope, Exit.void);
      assert.deepStrictEqual(calls, []);
    }),
  );

  it.effect(
    "[VAL-RPC-004][M2-RPC-GROUP-EXIT] close leaves no descendant behind when SIGTERM kills the leader and a descendant ignores it",
    () =>
      Effect.gen(function* () {
        if ((yield* HostProcess.Platform) === "win32") return;
        const { mock, runtime } = yield* startMockRuntime(
          {
            DROID_MOCK_STUBBORN: "1",
            DROID_MOCK_SIGTERM: "die",
            DROID_MOCK_GRANDCHILD: "ignore-sigterm",
          },
          { grace: { closeSession: "5 seconds", exit: "100 millis", terminate: "500 millis" } },
        );
        // The mock reads stdin only once its grandchild ignores SIGTERM, so a reply proves that.
        yield* runtime.request("droid.list_models");
        const log = yield* mock.readLog;
        const pgid = spawnedPid(log);
        const grandchildPid = asNumber(log.find((entry) => entry.type === "grandchild")?.pid);
        yield* Effect.addFinalizer(() => killGroupIfStillThere(pgid));
        assert.strictEqual(probeCode(grandchildPid), "alive");
        assert.strictEqual(probeCode(-pgid), "alive");

        const exit = yield* runtime.close;

        assert.deepStrictEqual(exit, { code: null, signal: "SIGTERM" });
        assert.strictEqual(probeCode(grandchildPid), "ESRCH");
        assert.strictEqual(probeCode(-pgid), "ESRCH");
      }).pipe(live),
  );

  it.effect(
    "[VAL-RPC-004][M2-RPC-GROUP-EXIT] SIGKILLs a group that outlives its leader once the grace is spent, not before",
    () =>
      Effect.gen(function* () {
        if ((yield* HostProcess.Platform) === "win32") return;
        const calls = yield* recordProcessKill((seen) =>
          seen.some((call) => call.signal === "SIGKILL"),
        );
        const { fake, scope } = yield* startFakeRuntime({ grace: { terminate: "5 seconds" } });
        const closing = yield* Scope.close(scope, Exit.void).pipe(Effect.forkChild);
        yield* yieldSome;
        // The leader dies from the SIGTERM; a descendant that ignores it keeps the group alive.
        yield* fake.exit(0);
        yield* yieldSome;

        const signals = () => calls.filter((call) => call.signal !== 0).map((call) => call.signal);
        assert.deepStrictEqual(signals(), ["SIGTERM"]);
        yield* TestClock.adjust("4999 millis");
        yield* yieldSome;
        assert.deepStrictEqual(signals(), ["SIGTERM"]);

        yield* TestClock.adjust("1 millis");
        yield* Fiber.join(closing);
        assert.deepStrictEqual(signals(), ["SIGTERM", "SIGKILL"]);
        assert.isTrue(calls.every((call) => call.pid === -FAKE_PID));
      }),
  );

  it.effect(
    "[VAL-RPC-004][M2-RPC-GROUP-EXIT] sends no SIGKILL when the group drains after its leader exited",
    () =>
      Effect.gen(function* () {
        if ((yield* HostProcess.Platform) === "win32") return;
        let drained = false;
        const calls = yield* recordProcessKill(() => drained);
        const { fake, scope } = yield* startFakeRuntime({ grace: { terminate: "5 seconds" } });
        const closing = yield* Scope.close(scope, Exit.void).pipe(Effect.forkChild);
        yield* yieldSome;
        yield* fake.exit(0);
        yield* yieldSome;
        assert.isUndefined(closing.pollUnsafe());

        drained = true;
        yield* TestClock.adjust("1 second");
        yield* Fiber.join(closing);
        assert.deepStrictEqual(
          calls.filter((call) => call.signal !== 0).map((call) => call.signal),
          ["SIGTERM"],
        );
      }),
  );

  it.effect(
    "[VAL-RPC-004][M2-RPC-GROUP-EXIT] sends no SIGKILL when only zombies are left in the group after SIGTERM",
    () =>
      Effect.gen(function* () {
        if ((yield* HostProcess.Platform) === "win32") return;
        const calls = yield* recordProcessKill(
          () => false,
          (seen) => seen.some((call) => call.signal === "SIGTERM"),
        );
        const { scope } = yield* startFakeRuntime({ grace: { terminate: "5 seconds" } });
        const closing = yield* Scope.close(scope, Exit.void).pipe(Effect.forkChild);
        yield* yieldSome;
        yield* TestClock.adjust("5 seconds");
        yield* Fiber.join(closing);

        assert.deepStrictEqual(
          calls.filter((call) => call.signal !== 0).map((call) => call.signal),
          ["SIGTERM"],
        );
      }),
  );

  it.effect(
    "[VAL-RPC-004][M2-RPC-GROUP-EXIT] returns only once a SIGKILLed group is fully reaped",
    () =>
      Effect.gen(function* () {
        if ((yield* HostProcess.Platform) === "win32") return;
        let reaped = false;
        const calls = yield* recordProcessKill(
          () => reaped,
          (seen) => !reaped && seen.some((call) => call.signal === "SIGKILL"),
        );
        const { scope } = yield* startFakeRuntime({ grace: { terminate: "5 seconds" } });
        const closing = yield* Scope.close(scope, Exit.void).pipe(Effect.forkChild);
        yield* yieldSome;
        yield* TestClock.adjust("5 seconds");
        yield* yieldSome;

        const signals = () => calls.filter((call) => call.signal !== 0).map((call) => call.signal);
        assert.deepStrictEqual(signals(), ["SIGTERM", "SIGKILL"]);
        assert.isUndefined(closing.pollUnsafe(), "the SIGKILLed group has not been reaped yet");

        reaped = true;
        yield* TestClock.adjust("1 second");
        yield* Fiber.join(closing);
        assert.deepStrictEqual(signals(), ["SIGTERM", "SIGKILL"]);
      }),
  );

  it.effect(
    "[VAL-RPC-004][M2-RPC-GROUP-EXIT] gives up on a SIGKILLed group that never goes away after a bounded wait",
    () =>
      Effect.gen(function* () {
        if ((yield* HostProcess.Platform) === "win32") return;
        const calls = yield* recordProcessKill(
          () => false,
          (seen) => seen.some((call) => call.signal === "SIGKILL"),
        );
        const { scope } = yield* startFakeRuntime({ grace: { terminate: "5 seconds" } });
        const closing = yield* Scope.close(scope, Exit.void).pipe(Effect.forkChild);
        yield* yieldSome;
        yield* TestClock.adjust("5 seconds");
        yield* TestClock.adjust("1 second");
        yield* Fiber.join(closing);

        assert.deepStrictEqual(
          calls.filter((call) => call.signal !== 0).map((call) => call.signal),
          ["SIGTERM", "SIGKILL"],
        );
      }),
  );

  it.effect(
    "[VAL-HIST-017] kill signals the process group once, ends the event stream instead of failing it, and rejects later requests as closed",
    () =>
      Effect.gen(function* () {
        if ((yield* HostProcess.Platform) === "win32") return;
        const calls = yield* recordProcessKill((seen) =>
          seen.some((call) => call.signal === "SIGTERM"),
        );
        const { fake, runtime } = yield* startFakeRuntime();
        const ended = yield* Stream.runDrain(runtime.events).pipe(Effect.exit, Effect.forkChild);
        const killing = yield* runtime.kill.pipe(Effect.forkChild);
        yield* yieldSome;
        yield* fake.exit(143);

        const exit = yield* Fiber.join(killing);
        assert.strictEqual(exit.code, 143);
        assert.deepStrictEqual(
          calls.filter((call) => call.signal !== 0).map((call) => call.signal),
          ["SIGTERM"],
        );
        assert.isTrue(calls.every((call) => call.pid === -FAKE_PID));
        assert.isTrue(Exit.isSuccess(yield* Fiber.join(ended)));
        const error = yield* runtime.request("droid.list_models").pipe(Effect.asVoid, Effect.flip);
        if (error._tag !== "DroidRpcTerminatedError") return assert.fail(error._tag);
        assert.strictEqual(error.reason, "closed");
        assert.deepStrictEqual(yield* runtime.kill, exit, "a second kill changes nothing");
      }),
  );

  it.effect(
    "[VAL-HIST-017] kill escalates to SIGKILL for a Droid that ignores SIGTERM and leaves nothing running",
    () =>
      Effect.gen(function* () {
        if ((yield* HostProcess.Platform) === "win32") return;
        const { mock, runtime } = yield* startMockRuntime(
          { DROID_MOCK_STUBBORN: "1" },
          { grace: { terminate: "100 millis" } },
        );
        yield* runtime.request("droid.list_models");
        const pgid = spawnedPid(yield* mock.readLog);
        yield* Effect.addFinalizer(() => killGroupIfStillThere(pgid));
        const ended = yield* Stream.runDrain(runtime.events).pipe(Effect.exit, Effect.forkChild);

        const exit = yield* runtime.kill;

        assert.deepStrictEqual(exit, { code: null, signal: "SIGKILL" });
        assert.strictEqual(probeCode(-pgid), "ESRCH");
        assert.isTrue(Exit.isSuccess(yield* Fiber.join(ended)));
      }).pipe(live),
  );

  it.effect("[VAL-RPC-014] FACTORY_API_KEY passed in options.env reaches the session process", () =>
    Effect.gen(function* () {
      yield* stubEnv("FACTORY_API_KEY", undefined);
      const without = yield* childEnvironment();
      assert.isFalse(without.factoryApiKeySet);
      const withKey = yield* childEnvironment({ env: { FACTORY_API_KEY: SENTINEL_KEY } });
      assert.isTrue(withKey.factoryApiKeySet);
    }).pipe(live),
  );

  it.effect(
    "[VAL-RPC-014] FACTORY_API_KEY set in the host environment reaches the session process",
    () =>
      Effect.gen(function* () {
        yield* stubEnv("FACTORY_API_KEY", SENTINEL_KEY);
        const inherited = yield* childEnvironment();
        assert.isTrue(inherited.factoryApiKeySet);
      }).pipe(live),
  );

  it.effect(
    "[VAL-RPC-014][VAL-RPC-015] the session process sees no FACTORY_ variable the host and the caller did not provide",
    () =>
      Effect.gen(function* () {
        const hostEnvironment = yield* HostProcess.Environment;
        const hostNames = Object.keys(hostEnvironment).filter((name) =>
          name.startsWith("FACTORY_"),
        );
        const child = yield* childEnvironment({ env: { FACTORY_API_KEY: SENTINEL_KEY } });
        for (const name of child.names) {
          assert.isTrue(
            name === "FACTORY_API_KEY" || hostNames.includes(name),
            `the runtime added ${name} to the child's environment`,
          );
        }
      }).pipe(live),
  );

  it.effect(
    "[VAL-RPC-014] never logs or reports the key, even when Droid echoes it on stderr",
    () => {
      const logs = captureLogs();
      return Effect.gen(function* () {
        const crashed = Effect.fn("crashed")(function* (options: Partial<DroidRpcRuntimeOptions>) {
          const { mock, runtime } = yield* startMockRuntime({}, options);
          yield* initializeSession(runtime, mock.projectDir);
          const error = yield* runtime
            .request("droid.add_user_message", { text: "CRASH_ME" })
            .pipe(Effect.asVoid, Effect.flip);
          assert.strictEqual(error._tag, "DroidRpcTerminatedError");
          if (error._tag !== "DroidRpcTerminatedError") return assert.fail("expected termination");
          return { error, tail: yield* runtime.stderrTail };
        });

        yield* stubEnv("FACTORY_API_KEY", undefined);
        const viaOption = yield* crashed({ env: { FACTORY_API_KEY: SENTINEL_KEY } });
        yield* stubEnv("FACTORY_API_KEY", SENTINEL_KEY);
        const viaHost = yield* crashed({});

        for (const { error, tail } of [viaOption, viaHost]) {
          assert.include(tail, "authentication failed for key [redacted]");
          assert.include(error.stderrTail, "authentication failed for key [redacted]");
          for (const text of [
            tail,
            error.stderrTail,
            error.message,
            error.detail,
            Cause.pretty(Cause.fail(error)),
          ]) {
            assert.notInclude(text, SENTINEL_KEY);
          }
        }
        assert.isNotEmpty(logs.entries);
        for (const entry of logs.entries) {
          assert.notInclude(entry.text, SENTINEL_KEY);
        }
      }).pipe(live, logs.provide);
    },
  );

  it.effect(
    "[VAL-RPC-014][M2-RPC-DIAGNOSTIC-REDACTION] a JSON-RPC error that echoes the key reaches the caller redacted, in message and data",
    () => {
      const logs = captureLogs();
      return Effect.gen(function* () {
        const rejected = Effect.fn("rejected")(function* (options: {
          readonly env?: Record<string, string>;
        }) {
          const { fake, runtime } = yield* startFakeRuntime(options);
          const fiber = yield* runtime
            .request("droid.add_user_message", { text: "hello" })
            .pipe(Effect.asVoid, Effect.flip, Effect.forkChild);
          const line = yield* fake.takeWritten;
          yield* fake.emit(
            errorResponseLine(line.id, {
              code: -32603,
              message: `authentication failed for key ${SENTINEL_KEY}`,
              data: {
                detail: `key ${SENTINEL_KEY}`,
                nested: [{ [SENTINEL_KEY]: "kept", note: SENTINEL_KEY }],
                attempts: 2,
              },
            }),
          );
          const error = yield* Fiber.join(fiber);
          return error._tag === "DroidRpcResponseError"
            ? error
            : assert.fail(`expected a response error, got ${error._tag}`);
        });

        yield* stubEnv("FACTORY_API_KEY", undefined);
        const viaOption = yield* rejected({ env: { FACTORY_API_KEY: SENTINEL_KEY } });
        yield* stubEnv("FACTORY_API_KEY", SENTINEL_KEY);
        const viaHost = yield* rejected({});

        for (const error of [viaOption, viaHost]) {
          assert.strictEqual(error.method, "droid.add_user_message");
          assert.strictEqual(error.code, -32603);
          assert.strictEqual(error.message, "authentication failed for key [redacted]");
          assert.deepStrictEqual(error.data, {
            detail: "key [redacted]",
            nested: [{ "[redacted]": "kept", note: "[redacted]" }],
            attempts: 2,
          });
          for (const text of [
            error.message,
            JSON.stringify(error.data),
            Cause.pretty(Cause.fail(error)),
          ]) {
            assert.notInclude(text, SENTINEL_KEY);
          }
        }
        for (const entry of logs.entries) {
          assert.notInclude(entry.text, SENTINEL_KEY);
        }
      }).pipe(logs.provide);
    },
  );

  it.effect("[VAL-RPC-014] an empty or whitespace-only FACTORY_API_KEY redacts nothing", () =>
    Effect.gen(function* () {
      for (const key of ["", "   "]) {
        const { fake, runtime } = yield* startFakeRuntime({ env: { FACTORY_API_KEY: key } });
        const fiber = yield* runtime
          .request("droid.update_session_settings", { autonomyLevel: "bogus" })
          .pipe(Effect.asVoid, Effect.flip, Effect.forkChild);
        const line = yield* fake.takeWritten;
        yield* fake.emit(
          errorResponseLine(line.id, {
            code: -32602,
            message: "Invalid autonomy level",
            data: { field: "autonomyLevel" },
          }),
        );
        const error = yield* Fiber.join(fiber);
        assert.strictEqual(error._tag, "DroidRpcResponseError");
        if (error._tag !== "DroidRpcResponseError") return;
        assert.strictEqual(error.message, "Invalid autonomy level");
        assert.deepStrictEqual(error.data, { field: "autonomyLevel" });
      }
    }),
  );

  it.effect(
    "[VAL-RPC-014][M2-RPC-DIAGNOSTIC-REDACTION] a non-JSON stdout line that carries the key fails the pending request and the event stream, and is logged, without it",
    () => {
      const logs = captureLogs();
      return Effect.gen(function* () {
        // V8 quotes a bad line in full only when it is at most 21 characters, so this test
        // uses a shorter fake key than SENTINEL_KEY for the parser's message to hold the key.
        const key = "not-a-real-key";
        const { fake, runtime } = yield* startFakeRuntime({ env: { FACTORY_API_KEY: key } });
        const events = yield* collectEvents(runtime);
        const pending = yield* runtime
          .request("droid.list_models")
          .pipe(Effect.asVoid, Effect.flip, Effect.forkChild);
        yield* fake.takeWritten;
        yield* fake.emitBytes(encoder.encode(`${key} {\n`));
        yield* fake.exit(1);

        const requestError = yield* Fiber.join(pending);
        const streamError = yield* takeUntil(events, () => false).pipe(Effect.flip);
        for (const error of [requestError, streamError]) {
          if (error._tag !== "DroidRpcTerminatedError") return assert.fail("expected a stop");
          assert.strictEqual(error.reason, "malformed_output");
          assert.include(error.detail, `"[redacted] {"`);
          for (const text of [
            error.detail,
            error.message,
            error.stderrTail,
            Cause.pretty(Cause.fail(error)),
          ]) {
            assert.notInclude(text, key);
          }
        }
        const warning = logs.entries.find((entry) =>
          entry.text.includes("Droid process terminated"),
        );
        assert.include(warning?.text, `"[redacted] {"`);
        for (const entry of logs.entries) {
          assert.notInclude(entry.text, key);
        }
      }).pipe(logs.provide);
    },
  );

  const logsForFrame = Effect.fn("logsForFrame")(function* (frame: unknown) {
    const logs = captureLogs();
    yield* Effect.gen(function* () {
      const started = yield* startFakeRuntime({ env: { FACTORY_API_KEY: SENTINEL_KEY } });
      yield* started.fake.emit(frame);
      yield* roundTrip(started);
    }).pipe(logs.provide);
    for (const entry of logs.entries) {
      assert.notInclude(entry.text, SENTINEL_KEY);
    }
    return (level: string, message: string) => {
      const entry = logs.entries.find(
        (candidate) => candidate.level === level && candidate.text.includes(message),
      );
      return entry?.text ?? assert.fail(`no ${level} log containing "${message}"`);
    };
  });

  it.effect(
    "[VAL-RPC-014][M2-RPC-DIAGNOSTIC-REDACTION] an error frame without an id that echoes the key is logged without it",
    () =>
      Effect.gen(function* () {
        const logged = yield* logsForFrame(
          errorResponseLine(null, {
            code: -32700,
            message: `Invalid JSON-RPC message for key ${SENTINEL_KEY}`,
          }),
        );
        assert.include(
          logged("Warn", "Droid rejected a message"),
          "Invalid JSON-RPC message for key [redacted]",
        );
      }),
  );

  it.effect(
    "[VAL-RPC-014][M2-RPC-DIAGNOSTIC-REDACTION] a notification method that carries the key is logged without it",
    () =>
      Effect.gen(function* () {
        const logged = yield* logsForFrame({
          ...notificationLine({ type: "tool_call" }),
          method: `droid.${SENTINEL_KEY}`,
        });
        assert.include(
          logged("Warn", "Dropping an unsupported Droid notification"),
          "method=droid.[redacted]",
        );
      }),
  );

  it.effect(
    "[VAL-RPC-014][M2-RPC-DIAGNOSTIC-REDACTION] a notification type that carries the key is logged without it",
    () =>
      Effect.gen(function* () {
        const logged = yield* logsForFrame(notificationLine({ type: `future_${SENTINEL_KEY}` }));
        assert.include(
          logged("Debug", "Dropping a Droid notification of an unknown type"),
          "type=future_[redacted]",
        );
      }),
  );

  it.effect(
    "[VAL-RPC-014][M2-RPC-DIAGNOSTIC-REDACTION] a frame type that carries the key is logged without it",
    () =>
      Effect.gen(function* () {
        const logged = yield* logsForFrame({ type: `frame_${SENTINEL_KEY}` });
        assert.include(
          logged("Warn", "Dropping a Droid message that is not a valid frame"),
          "type=frame_[redacted]",
        );
      }),
  );

  it.effect(
    "[VAL-RPC-014][M2-RPC-DIAGNOSTIC-REDACTION] a response id that carries the key is logged without it",
    () =>
      Effect.gen(function* () {
        const logged = yield* logsForFrame(responseLine(`stray-${SENTINEL_KEY}`, {}));
        assert.include(
          logged("Debug", "Dropping Droid response for an unknown or timed-out request"),
          "id=stray-[redacted]",
        );
      }),
  );

  it.effect(
    "[VAL-RPC-014][M2-RPC-DIAGNOSTIC-REDACTION] a protocol version that carries the key is warned about and reported without it",
    () => {
      const logs = captureLogs();
      return Effect.gen(function* () {
        const rpc = yield* loadFreshRpcModule;
        const agent = yield* makeRpcMockAgent({ DROID_MOCK_PROTOCOL_VERSION: SENTINEL_KEY });
        const runtime = yield* rpc.makeDroidRpcRuntime({
          binaryPath: agent.binaryPath,
          cwd: agent.projectDir,
          env: { FACTORY_API_KEY: SENTINEL_KEY },
        });
        yield* runtime.request("droid.list_models");

        const warnings = logs.entries.filter((entry) => entry.level === "Warn");
        assert.lengthOf(warnings, 1);
        assert.include(warnings[0]?.text, `factoryProtocolVersion [redacted];`);
        assert.deepStrictEqual(yield* runtime.peerProtocolVersion, Option.some("[redacted]"));
        for (const entry of logs.entries) {
          assert.notInclude(entry.text, SENTINEL_KEY);
        }
      }).pipe(live, logs.provide);
    },
  );
});
