/**
 * DroidRpcRuntime - one `droid exec` process speaking Droid's stream-jsonrpc protocol.
 *
 * Transport only: it spawns the process, frames NDJSON, correlates responses by id, delivers
 * notifications and inbound requests in arrival order, and shuts the process down. It knows
 * nothing about sessions or turns; the adapter owns those.
 */
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Ndjson from "effect/encoding/Ndjson";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { signalProcessGroup } from "@t3tools/provider-core/server/processGroup";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import { DROID_HOOKS_OFF_SETTINGS } from "./hooksOffSettings.ts";
import {
  DROID_API_VERSION,
  DROID_NOTIFICATION_METHOD,
  DROID_PROTOCOL_VERSION,
  DROID_STDERR_TAIL_MAX,
  type DroidInboundRequestMethod,
  JSON_RPC_INTERNAL_ERROR,
  JSON_RPC_METHOD_NOT_FOUND,
  decodeDroidFrame,
  decodeDroidNotificationParams,
  isDroidInboundRequestMethod,
  isDroidNotificationType,
  timeoutForMethod,
} from "./protocol.ts";

const STDERR_DRAIN_GRACE = Duration.seconds(1);
const GROUP_POLL_INTERVAL = Duration.millis(25);
// SIGKILL cannot be refused, but a killed process still shows in the group until it is reaped.
const GROUP_REAP_GRACE = Duration.seconds(1);
const REDACTED = "[redacted]";

const DEFAULT_GRACE: DroidRpcGrace = {
  drain: Duration.seconds(2),
  closeSession: Duration.seconds(5),
  exit: Duration.seconds(5),
  terminate: Duration.seconds(5),
};

export interface DroidNotification {
  readonly type: string;
  readonly [key: string]: unknown;
}

/** Notifications and inbound requests, in the order Droid wrote them. */
export type DroidRpcEvent =
  | {
      readonly _tag: "Notification";
      /** `params.sessionId`; Droid omits it on several notification types. */
      readonly sessionId: string | undefined;
      readonly notification: DroidNotification;
    }
  | {
      readonly _tag: "Request";
      readonly id: string;
      readonly method: DroidInboundRequestMethod;
      readonly params: unknown;
    };

export interface DroidRpcExit {
  readonly code: number | null;
  /** The signal this runtime sent before the process died without an exit code. */
  readonly signal: string | null;
}

export interface DroidRpcGrace {
  /** Wait for stdout EOF after the process exits, so its last frames are still delivered. */
  readonly drain: Duration.Input;
  /** Wait for each of the `interrupt_session` and `close_session` replies. */
  readonly closeSession: Duration.Input;
  /** Wait for the process to exit on its own after close/EOF. */
  readonly exit: Duration.Input;
  /** Wait after SIGTERM before SIGKILL. */
  readonly terminate: Duration.Input;
}

export interface DroidRpcRuntimeOptions {
  readonly binaryPath: string;
  readonly cwd: string;
  /** Layered over the inherited environment, so `FACTORY_API_KEY` reaches Droid when it is set. */
  readonly env?: Readonly<Record<string, string>>;
  readonly grace?: Partial<DroidRpcGrace>;
}

export class DroidRpcResponseError extends Schema.TaggedError<DroidRpcResponseError>()(
  "DroidRpcResponseError",
  {
    method: Schema.String,
    code: Schema.Finite,
    message: Schema.String,
    data: Schema.optional(Schema.Unknown),
  },
) {}

export class DroidRpcTimeoutError extends Schema.TaggedError<DroidRpcTimeoutError>()(
  "DroidRpcTimeoutError",
  {
    method: Schema.String,
    timeoutMs: Schema.Finite,
  },
) {
  override get message(): string {
    return `Droid did not answer ${this.method} within ${this.timeoutMs} ms`;
  }
}

export class DroidRpcTerminatedError extends Schema.TaggedError<DroidRpcTerminatedError>()(
  "DroidRpcTerminatedError",
  {
    reason: Schema.Literals(["process_exit", "malformed_output", "transport_error", "closed"]),
    detail: Schema.String,
    exitCode: Schema.NullOr(Schema.Finite),
    signal: Schema.NullOr(Schema.String),
    stderrTail: Schema.String,
  },
) {
  override get message(): string {
    const tail = this.stderrTail.trim();
    return tail.length === 0 ? this.detail : `${this.detail}\nstderr:\n${tail}`;
  }
}

export class DroidRpcUnknownInboundRequestError extends Schema.TaggedError<DroidRpcUnknownInboundRequestError>()(
  "DroidRpcUnknownInboundRequestError",
  { id: Schema.String },
) {
  override get message(): string {
    return `No open Droid inbound request with id ${this.id}`;
  }
}

export class DroidRpcSpawnError extends Schema.TaggedError<DroidRpcSpawnError>()(
  "DroidRpcSpawnError",
  {
    command: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to start ${this.command}: ${String(this.cause)}`;
  }
}

export type DroidRpcRequestError =
  | DroidRpcResponseError
  | DroidRpcTimeoutError
  | DroidRpcTerminatedError;

export type DroidRpcRespondError = DroidRpcUnknownInboundRequestError | DroidRpcTerminatedError;

export class DroidRpcRuntime extends Context.Service<
  DroidRpcRuntime,
  {
    /** Single consumer. Fails with `DroidRpcTerminatedError` when the process dies unexpectedly; ends after `close`. */
    readonly events: Stream.Stream<DroidRpcEvent, DroidRpcTerminatedError>;
    readonly request: (
      method: string,
      params?: object,
      options?: { readonly timeout?: Duration.Input },
    ) => Effect.Effect<unknown, DroidRpcRequestError>;
    /** Answers an open inbound request; each id can be answered once. */
    readonly respond: (id: string, result: unknown) => Effect.Effect<void, DroidRpcRespondError>;
    readonly respondError: (
      id: string,
      error: { readonly code: number; readonly message: string },
    ) => Effect.Effect<void, DroidRpcRespondError>;
    /** Runs `handler` and answers with its result, or with a JSON-RPC error when it fails. */
    readonly answer: <E>(
      id: string,
      handler: Effect.Effect<unknown, E>,
    ) => Effect.Effect<void, DroidRpcRespondError>;
    /** `factoryProtocolVersion` of the first response. */
    readonly peerProtocolVersion: Effect.Effect<Option.Option<string>>;
    readonly stderrTail: Effect.Effect<string>;
    readonly awaitExit: Effect.Effect<DroidRpcExit>;
    /**
     * `interrupt_session` when a turn may be running, `close_session`, stdin EOF, wait for
     * exit, then SIGTERM and SIGKILL to the process group. Removes the settings file.
     */
    readonly close: Effect.Effect<DroidRpcExit>;
  }
>()("@t3tools/provider-droid/server/rpc/DroidRpcRuntime") {}

const encodeJson = Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

const envelope = (fields: Record<string, unknown>) => ({
  jsonrpc: "2.0",
  factoryApiVersion: DROID_API_VERSION,
  factoryProtocolVersion: DROID_PROTOCOL_VERSION,
  ...fields,
});

const describeError = (error: unknown): string => {
  if (Predicate.isError(error)) return error.message;
  if (Predicate.isObject(error) && Predicate.isString(error.message)) return error.message;
  return String(error);
};

/**
 * Droid inherits `FACTORY_API_KEY` and can echo it in anything it writes, from stderr to a
 * JSON-RPC error. Everything Droid wrote that reaches an error or a log goes through here.
 * An empty or whitespace-only key redacts nothing: replacing "" would mangle every character.
 */
export const makeRedactor = (apiKey: string | undefined) => {
  const key = apiKey?.trim();
  const text = (value: string): string => (key ? value.replaceAll(key, REDACTED) : value);
  // Keys and string leaves only: the input is parsed JSON, so there are no other carriers.
  const json = (value: unknown): unknown => {
    if (Predicate.isString(value)) return text(value);
    if (Array.isArray(value)) return value.map(json);
    if (Predicate.isObject(value)) {
      return Object.fromEntries(
        Object.entries(value).map(([name, item]) => [text(name), json(item)]),
      );
    }
    return value;
  };
  return { text, json };
};

interface PendingRequest {
  readonly method: string;
  readonly deferred: Deferred.Deferred<unknown, DroidRpcRequestError>;
}

interface RuntimeState {
  readonly terminating: boolean;
  readonly pending: ReadonlyMap<string, PendingRequest>;
}

type TerminationReason = "process_exit" | "malformed_output" | "transport_error";

// One warning per process: every thread spawns its own runtime, and they all see the same Droid.
let protocolMismatchWarned = false;

export const makeDroidRpcRuntime = Effect.fn("makeDroidRpcRuntime")(function* (
  options: DroidRpcRuntimeOptions,
): Effect.fn.Return<
  DroidRpcRuntime["Service"],
  DroidRpcSpawnError,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path | Scope.Scope
> {
  const scope = yield* Scope.Scope;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const platform = yield* HostProcess.Platform;
  const hostEnvironment = yield* HostProcess.Environment;
  const grace = { ...DEFAULT_GRACE, ...options.grace };
  const spawnError = (cause: unknown) =>
    new DroidRpcSpawnError({ command: options.binaryPath, cause });

  const redactor = makeRedactor(
    options.env?.["FACTORY_API_KEY"] ?? hostEnvironment["FACTORY_API_KEY"],
  );
  const redact = redactor.text;
  // The one way this module logs, so text from Droid cannot reach a log unredacted.
  const logDroid =
    (log: typeof Effect.logWarning) =>
    (message: string, fields: Readonly<Record<string, string>> = {}) =>
      log(redact(message)).pipe(
        Effect.annotateLogs(
          Object.fromEntries(Object.entries(fields).map(([name, value]) => [name, redact(value)])),
        ),
      );
  const logWarning = logDroid(Effect.logWarning);
  const logDebug = logDroid(Effect.logDebug);

  // Disables the user's hooks for this process only; never inside the project or ~/.factory.
  const settingsDir = yield* fs
    .makeTempDirectory({ prefix: "t3-droid-settings-" })
    .pipe(Effect.mapError(spawnError));
  const removeSettings = fs.remove(settingsDir, { recursive: true }).pipe(Effect.ignore);
  yield* Effect.addFinalizer(() => removeSettings);
  const settingsPath = path.join(settingsDir, "settings.json");
  yield* fs
    .writeFileString(settingsPath, DROID_HOOKS_OFF_SETTINGS)
    .pipe(Effect.mapError(spawnError));

  const spawnCommand = yield* resolveSpawnCommand(
    options.binaryPath,
    [
      "exec",
      "--input-format",
      "stream-jsonrpc",
      "--output-format",
      "stream-jsonrpc",
      "--cwd",
      options.cwd,
      "--settings",
      settingsPath,
    ],
    { ...(options.env ? { env: options.env } : {}), extendEnv: true },
  );
  const child = yield* spawner
    .spawn(
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        cwd: options.cwd,
        ...(options.env ? { env: options.env } : {}),
        extendEnv: true,
        shell: spawnCommand.shell,
        // Droid starts tool subprocesses and MCP servers; a group of their own lets one signal
        // reach all of them.
        detached: platform !== "win32",
        killSignal: "SIGTERM",
        forceKillAfter: grace.terminate,
      }),
    )
    .pipe(Effect.mapError(spawnError));

  // The spawner reports a signal death only as text on a failed `exitCode`, so the signal
  // reported to callers is the last one this runtime sent.
  let sentSignal: NodeJS.Signals | null = null;

  const signalGroup = (signal: NodeJS.Signals): boolean => {
    try {
      signalProcessGroup(Number(child.pid), signal);
      sentSignal = signal;
      return true;
    } catch {
      return false;
    }
  };

  /**
   * A dead leader cannot tell us whether its descendants are alive, and once nothing is left
   * in the group the OS may recycle the pid, so the first check treats a dead leader as
   * "nothing to stop". After our own SIGTERM that shortcut is wrong: a descendant can ignore
   * it and outlive the leader. POSIX never reuses a pid as a process or group id while any
   * process is still in that group, so a successful group probe still means our group, and
   * after the leader died it is the only evidence of a survivor.
   */
  const probeGroup = (): "alive" | "lingering" | "gone" => {
    try {
      signalProcessGroup(Number(child.pid), 0);
      return "alive";
    } catch (error) {
      // macOS answers EPERM, not ESRCH, while a group's only members are zombies awaiting reap.
      return Predicate.isObject(error) && error.code === "EPERM" ? "lingering" : "gone";
    }
  };
  const probeExists = (): boolean => {
    if (platform !== "win32") return probeGroup() === "alive";
    try {
      process.kill(Number(child.pid), 0);
      return true;
    } catch {
      return false;
    }
  };
  const hasExited = child.isRunning.pipe(
    Effect.orElseSucceed(() => true),
    Effect.map((running) => !running || !probeExists()),
  );

  const awaitGroupGone = Effect.gen(function* () {
    while (probeExists()) yield* Effect.sleep(GROUP_POLL_INTERVAL);
  });
  const awaitGroupReaped = Effect.gen(function* () {
    while (probeGroup() !== "gone") yield* Effect.sleep(GROUP_POLL_INTERVAL);
  });

  const terminatePosixGroup = Effect.gen(function* () {
    if (yield* hasExited) return;
    if (!signalGroup("SIGTERM")) return;
    // The leader can exit first; its descendants then get the rest of the same grace to follow.
    yield* child.exitCode.pipe(
      Effect.ignore,
      Effect.andThen(awaitGroupGone),
      Effect.timeoutOption(grace.terminate),
    );
    if (!probeExists()) return;
    signalGroup("SIGKILL");
    // Returning before the group is reaped would leave `close` unable to say it left nothing behind.
    yield* awaitGroupReaped.pipe(Effect.timeoutOption(GROUP_REAP_GRACE));
  });

  // Windows has no process groups; `taskkill /T` reaps the whole tree.
  const terminateWindowsTree = Effect.gen(function* () {
    if (yield* hasExited) return;
    const taskkill = yield* spawner.spawn(
      ChildProcess.make("taskkill", ["/PID", String(child.pid), "/T", "/F"]),
    );
    yield* taskkill.exitCode;
  }).pipe(Effect.scoped, Effect.ignore);

  const stopProcess = platform === "win32" ? terminateWindowsTree : terminatePosixGroup;

  // Registered before any further setup: an interrupt or failure between the spawn and the
  // rest of this constructor would otherwise leak a detached Droid with nothing to reap it.
  yield* Scope.addFinalizer(scope, stopProcess.pipe(Effect.ignore, Effect.uninterruptible));

  const outgoing = yield* Queue.unbounded<string, Cause.Done>();
  const events = yield* Queue.unbounded<DroidRpcEvent, DroidRpcTerminatedError | Cause.Done>();
  const state = yield* Ref.make<RuntimeState>({ terminating: false, pending: new Map() });
  const openInbound = yield* Ref.make<ReadonlySet<string>>(new Set());
  const nextId = yield* Ref.make(1);
  const closing = yield* Ref.make(false);
  // Closing stdin does not cancel a running turn, so close has to know whether to interrupt.
  // It errs towards interrupting: a spurious `interrupt_session` on an idle session is harmless.
  const turnMayBeRunning = yield* Ref.make(false);
  const peerVersion = yield* Ref.make(Option.none<string>());
  const stderrTail = yield* Ref.make("");
  const stderrDrained = yield* Deferred.make<void>();
  const stdoutEnded = yield* Deferred.make<void>();
  const exited = yield* Deferred.make<DroidRpcExit>();
  const terminated = yield* Deferred.make<DroidRpcTerminatedError>();

  const readStderrTail = Ref.get(stderrTail).pipe(Effect.map(redact));

  const terminate = Effect.fnUntraced(function* (reason: TerminationReason, detail: string) {
    const first = yield* Ref.modify(state, (current) =>
      current.terminating
        ? ([false, current] as const)
        : ([true, { ...current, terminating: true }] as const),
    );
    if (!first) return;
    yield* Queue.end(outgoing);
    if (reason !== "process_exit") {
      yield* stopProcess;
    }
    let exit = yield* Deferred.await(exited).pipe(Effect.timeoutOption(grace.exit));
    if (Option.isNone(exit)) {
      yield* stopProcess;
      exit = yield* Deferred.await(exited).pipe(Effect.timeoutOption(grace.exit));
    }
    // A dying process's last stderr lines can arrive after its stdout ends.
    yield* Deferred.await(stderrDrained).pipe(Effect.timeoutOption(STDERR_DRAIN_GRACE));
    const isClosing = yield* Ref.get(closing);
    const exitInfo = Option.getOrElse(exit, (): DroidRpcExit => ({ code: null, signal: null }));
    const exitText =
      exitInfo.code !== null
        ? `exit code ${exitInfo.code}`
        : exitInfo.signal !== null
          ? `signal ${exitInfo.signal}`
          : "unknown exit status";
    const error = new DroidRpcTerminatedError({
      reason: isClosing ? "closed" : reason,
      detail: redact(isClosing ? `Droid session closed (${exitText})` : `${detail} (${exitText})`),
      exitCode: exitInfo.code,
      signal: exitInfo.signal,
      stderrTail: yield* readStderrTail,
    });
    const pending = yield* Ref.modify(state, (current) => [
      current.pending,
      { ...current, pending: new Map() },
    ]);
    yield* Effect.forEach(pending.values(), ({ deferred }) => Deferred.fail(deferred, error), {
      discard: true,
    });
    yield* Ref.set(openInbound, new Set());
    yield* Deferred.succeed(terminated, error);
    if (isClosing) {
      yield* Queue.end(events);
    } else {
      yield* logWarning("Droid process terminated", { reason, detail: error.detail });
      yield* Queue.fail(events, error);
    }
  });

  const write = Effect.fnUntraced(function* (message: Record<string, unknown>) {
    const line = yield* encodeJson(message).pipe(Effect.orDie);
    const accepted = yield* Queue.offer(outgoing, `${line}\n`);
    if (!accepted) {
      return yield* Deferred.await(terminated).pipe(Effect.flatMap(Effect.fail));
    }
  });

  const removePending = (id: string) =>
    Ref.update(state, (current) => {
      if (!current.pending.has(id)) return current;
      const pending = new Map(current.pending);
      pending.delete(id);
      return { ...current, pending };
    });

  const takePending = (id: string) =>
    Ref.modify(state, (current) => {
      const entry = current.pending.get(id);
      if (entry === undefined) return [Option.none<PendingRequest>(), current] as const;
      const pending = new Map(current.pending);
      pending.delete(id);
      return [Option.some(entry), { ...current, pending }] as const;
    });

  const recordPeerVersion = Effect.fnUntraced(function* (version: string) {
    const recorded = yield* Ref.modify(peerVersion, (current) =>
      Option.isNone(current)
        ? ([true, Option.some(redact(version))] as const)
        : ([false, current] as const),
    );
    if (recorded && version !== DROID_PROTOCOL_VERSION && !protocolMismatchWarned) {
      protocolMismatchWarned = true;
      yield* logWarning(
        `Droid speaks factoryProtocolVersion ${version}; t3 was built against ${DROID_PROTOCOL_VERSION}`,
      );
    }
  });

  const handleResponse = Effect.fnUntraced(function* (frame: {
    readonly id?: string | null | undefined;
    readonly factoryProtocolVersion?: string | undefined;
    readonly result?: unknown;
    readonly error?: unknown;
  }) {
    if (frame.factoryProtocolVersion !== undefined) {
      yield* recordPeerVersion(frame.factoryProtocolVersion);
    }
    const id = frame.id;
    if (!Predicate.isString(id)) {
      // Droid answers an envelope it could not parse with `id: null`.
      yield* logWarning("Droid rejected a message", { error: describeError(frame.error) });
      return;
    }
    const entry = yield* takePending(id);
    if (Option.isNone(entry)) {
      yield* logDebug("Dropping Droid response for an unknown or timed-out request", { id });
      return;
    }
    const { deferred, method } = entry.value;
    const error = frame.error;
    if (error !== undefined && error !== null) {
      const fields = Predicate.isObject(error) ? error : {};
      yield* Deferred.fail(
        deferred,
        new DroidRpcResponseError({
          method,
          code: Predicate.isNumber(fields.code) ? fields.code : JSON_RPC_INTERNAL_ERROR,
          message: redact(
            Predicate.isString(fields.message) ? fields.message : describeError(error),
          ),
          ...(fields.data === undefined ? {} : { data: redactor.json(fields.data) }),
        }),
      );
      return;
    }
    yield* Deferred.succeed(deferred, frame.result);
  });

  const writeResponse = (id: string, body: { result: unknown } | { error: unknown }) =>
    write(envelope({ type: "response", id, ...body }));

  const handleInboundRequest = Effect.fnUntraced(function* (frame: {
    readonly id: string;
    readonly method: string;
    readonly params?: unknown;
  }) {
    const { id, method } = frame;
    if (!isDroidInboundRequestMethod(method)) {
      yield* writeResponse(id, {
        error: { code: JSON_RPC_METHOD_NOT_FOUND, message: `Unknown method: ${method}` },
      }).pipe(Effect.ignore);
      return;
    }
    yield* Ref.update(openInbound, (current) => new Set(current).add(id));
    yield* Queue.offer(events, { _tag: "Request", id, method, params: frame.params });
  });

  const handleNotification = Effect.fnUntraced(function* (frame: {
    readonly method: string;
    readonly params?: unknown;
  }) {
    const params =
      frame.method === DROID_NOTIFICATION_METHOD
        ? decodeDroidNotificationParams(frame.params)
        : Option.none();
    if (Option.isNone(params)) {
      yield* logWarning("Dropping an unsupported Droid notification", { method: frame.method });
      return;
    }
    const { notification } = params.value;
    if (!isDroidNotificationType(notification.type)) {
      yield* logDebug("Dropping a Droid notification of an unknown type", {
        type: notification.type,
      });
      return;
    }
    if (notification.type === "agent_turn_completed") {
      yield* Ref.set(turnMayBeRunning, false);
    } else if (
      notification.type === "droid_working_state_changed" &&
      notification["newState"] !== "idle"
    ) {
      yield* Ref.set(turnMayBeRunning, true);
    }
    yield* Queue.offer(events, {
      _tag: "Notification",
      sessionId: Predicate.isString(params.value["sessionId"])
        ? params.value["sessionId"]
        : undefined,
      notification,
    });
  });

  const route = Effect.fnUntraced(function* (message: unknown) {
    const frame = decodeDroidFrame(message);
    if (Option.isNone(frame)) {
      yield* logWarning("Dropping a Droid message that is not a valid frame", {
        type: Predicate.isObject(message) ? String(message.type) : typeof message,
      });
      return;
    }
    switch (frame.value.type) {
      case "response":
        return yield* handleResponse(frame.value);
      case "request":
        return yield* handleInboundRequest(frame.value);
      case "notification":
        return yield* handleNotification(frame.value);
    }
  });

  const onFiberFailure =
    (reason: TerminationReason, describe: (cause: Cause.Cause<unknown>) => string) =>
    (cause: Cause.Cause<unknown>) =>
      Cause.hasInterruptsOnly(cause) ? Effect.void : terminate(reason, describe(cause));

  yield* child.stderr.pipe(
    Stream.decodeText(),
    Stream.runForEach((chunk) =>
      Ref.update(stderrTail, (current) => (current + chunk).slice(-DROID_STDERR_TAIL_MAX)),
    ),
    Effect.catchCause((cause) =>
      logDebug("Droid stderr reader stopped", { cause: Cause.pretty(cause) }),
    ),
    Effect.ensuring(Deferred.succeed(stderrDrained, undefined)),
    Effect.forkIn(scope),
  );

  yield* child.stdout.pipe(
    Stream.pipeThroughChannel(Ndjson.decode({ ignoreEmptyLines: true })),
    Stream.runForEach(route),
    Effect.matchCauseEffect({
      onSuccess: () => terminate("process_exit", "Droid closed its stdout"),
      onFailure: (cause) => {
        if (Cause.hasInterruptsOnly(cause)) return Effect.void;
        const error = Cause.squash(cause);
        return error instanceof Ndjson.NdjsonError
          ? terminate(
              "malformed_output",
              `Droid wrote a malformed stdout line: ${describeError(error.cause)}`,
            )
          : terminate("transport_error", `Reading Droid stdout failed: ${describeError(error)}`);
      },
    }),
    Effect.ensuring(Deferred.succeed(stdoutEnded, undefined)),
    Effect.forkIn(scope),
  );

  yield* Stream.fromQueue(outgoing).pipe(
    Stream.encodeText,
    Stream.run(child.stdin),
    Effect.catchCause(
      onFiberFailure(
        "transport_error",
        (cause) => `Writing to Droid stdin failed: ${describeError(Cause.squash(cause))}`,
      ),
    ),
    Effect.forkIn(scope),
  );

  yield* child.exitCode.pipe(
    Effect.exit,
    Effect.flatMap((exit) =>
      Deferred.succeed(
        exited,
        Exit.isSuccess(exit)
          ? { code: exit.value, signal: null }
          : { code: null, signal: sentSignal },
      ),
    ),
    // Node can report the exit before stdout is fully read; the reader terminates on EOF itself.
    Effect.andThen(Deferred.await(stdoutEnded).pipe(Effect.timeoutOption(grace.drain))),
    Effect.andThen(terminate("process_exit", "Droid exited")),
    Effect.forkIn(scope),
  );

  const request = (
    method: string,
    params: object = {},
    requestOptions?: { readonly timeout?: Duration.Input },
  ): Effect.Effect<unknown, DroidRpcRequestError> =>
    Effect.gen(function* () {
      const id = String(yield* Ref.getAndUpdate(nextId, (current) => current + 1));
      const deferred = yield* Deferred.make<unknown, DroidRpcRequestError>();
      const registered = yield* Ref.modify(state, (current) => {
        if (current.terminating) return [false, current] as const;
        const pending = new Map(current.pending).set(id, { method, deferred });
        return [true, { ...current, pending }] as const;
      });
      if (!registered) {
        return yield* Deferred.await(terminated).pipe(Effect.flatMap(Effect.fail));
      }
      if (method === "droid.add_user_message") {
        yield* Ref.set(turnMayBeRunning, true);
      }
      yield* write(envelope({ type: "request", id, method, params })).pipe(
        Effect.tapError(() => removePending(id)),
      );
      const timeout = Duration.fromInputUnsafe(requestOptions?.timeout ?? timeoutForMethod(method));
      const result = yield* Deferred.await(deferred).pipe(
        Effect.timeoutOption(timeout),
        Effect.onInterrupt(() => removePending(id)),
      );
      if (Option.isNone(result)) {
        yield* removePending(id);
        return yield* new DroidRpcTimeoutError({ method, timeoutMs: Duration.toMillis(timeout) });
      }
      return result.value;
    });

  const takeOpenInbound = (id: string) =>
    Ref.modify(openInbound, (current) => {
      if (!current.has(id)) return [false, current] as const;
      const next = new Set(current);
      next.delete(id);
      return [true, next] as const;
    });

  const respondWith = Effect.fnUntraced(function* (
    id: string,
    body: { result: unknown } | { error: unknown },
  ) {
    if (!(yield* takeOpenInbound(id))) {
      return yield* new DroidRpcUnknownInboundRequestError({ id });
    }
    yield* writeResponse(id, body);
  });

  const answer = <E>(id: string, handler: Effect.Effect<unknown, E>) =>
    Effect.gen(function* () {
      if (!(yield* Ref.get(openInbound)).has(id)) {
        return yield* new DroidRpcUnknownInboundRequestError({ id });
      }
      const exit = yield* Effect.exit(handler);
      yield* Exit.isSuccess(exit)
        ? respondWith(id, { result: exit.value })
        : respondWith(id, {
            error: {
              code: JSON_RPC_INTERNAL_ERROR,
              message: describeError(Cause.squash(exit.cause)),
            },
          });
    });

  const close = Effect.gen(function* () {
    const alreadyClosing = yield* Ref.getAndSet(closing, true);
    if (!alreadyClosing) {
      if (!(yield* Ref.get(state)).terminating) {
        if (yield* Ref.get(turnMayBeRunning)) {
          yield* request("droid.interrupt_session", {}, { timeout: grace.closeSession }).pipe(
            Effect.ignore,
          );
        }
        yield* request("droid.close_session", {}, { timeout: grace.closeSession }).pipe(
          Effect.ignore,
        );
        yield* Queue.end(outgoing);
      }
      const exit = yield* Deferred.await(exited).pipe(Effect.timeoutOption(grace.exit));
      if (Option.isNone(exit)) {
        yield* stopProcess;
      }
      yield* removeSettings;
    }
    return yield* Deferred.await(exited);
  });

  return DroidRpcRuntime.of({
    events: Stream.fromQueue(events),
    request,
    respond: (id, result) => respondWith(id, { result }),
    respondError: (id, error) => respondWith(id, { error }),
    answer,
    peerProtocolVersion: Ref.get(peerVersion),
    stderrTail: readStderrTail,
    awaitExit: Deferred.await(exited),
    close,
  });
});
