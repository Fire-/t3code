/**
 * Framing follows Prime's spec: LF-delimited only, with a trailing `\r` stripped.
 * Lines are split manually (never with `readline`, which also splits on
 * U+2028/U+2029 and would corrupt frames).
 */
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import {
  MAX_TOOL_OUTPUT_IMAGES,
  MAX_TOOL_OUTPUT_IMAGE_BASE64_LENGTH,
} from "@t3tools/shared/toolOutput";

import { signalProcessGroup } from "@t3tools/provider-core/server/processGroup";

export class PrimeRpcError extends Schema.TaggedError<PrimeRpcError>()("PrimeRpcError", {
  operation: Schema.String,
  detail: Schema.optional(Schema.String),
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `Prime RPC ${this.operation} failed${this.detail === undefined ? "" : `: ${this.detail}`}.`;
  }
}

export class PrimeRpcTimeoutError extends Schema.TaggedError<PrimeRpcTimeoutError>()(
  "PrimeRpcTimeoutError",
  {
    operation: Schema.String,
    timeoutMs: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
  },
) {
  override get message(): string {
    return `Prime RPC ${this.operation} failed: timed out after ${this.timeoutMs}ms.`;
  }
}

export type PrimeRpcRecord = Record<string, unknown>;

export function primeRecordField(input: unknown, key: string): unknown {
  return Predicate.isObject(input) ? input[key] : undefined;
}

export function primeRecordString(input: unknown, key: string): string | undefined {
  const value = primeRecordField(input, key);
  return Predicate.isString(value) ? value : undefined;
}

export function primeRecordNumber(input: unknown, key: string): number | undefined {
  const value = primeRecordField(input, key);
  return Predicate.isNumber(value) && Number.isFinite(value) ? value : undefined;
}

export function parsePrimeModelSlug(slug: string): { provider: string; modelId: string } | null {
  const separator = slug.indexOf("/");
  if (separator <= 0 || separator === slug.length - 1) return null;
  return { provider: slug.slice(0, separator), modelId: slug.slice(separator + 1) };
}

// Prime-only shapes, from packages/coding-agent/src/core/agent-session.ts
// (AgentSessionEvent), core/session-action-store.ts and core/goals.ts. Enum-like
// fields stay plain strings so a new Prime value never turns a known frame into
// an unknown one.
export const PrimeSessionActionSnapshot = Schema.Struct({
  queuedCount: Schema.Number,
  steering: Schema.Array(Schema.Unknown),
  followUps: Schema.Array(Schema.Unknown),
  active: Schema.optionalKey(
    Schema.Struct({
      kind: Schema.String,
      phase: Schema.String,
      label: Schema.optionalKey(Schema.String),
    }),
  ),
});
export type PrimeSessionActionSnapshot = typeof PrimeSessionActionSnapshot.Type;

export const PrimeGoalState = Schema.Struct({
  active: Schema.Boolean,
  status: Schema.String,
  goalId: Schema.optionalKey(Schema.String),
  objective: Schema.optionalKey(Schema.String),
  tokenBudget: Schema.optionalKey(Schema.Number),
  tokensUsed: Schema.Number,
  timeUsedSeconds: Schema.Number,
  continuationsUsed: Schema.Number,
  lastReason: Schema.optionalKey(Schema.String),
  lastError: Schema.optionalKey(Schema.String),
});
export type PrimeGoalState = typeof PrimeGoalState.Type;

/** `get_state` data (modes/rpc/rpc-types.ts). Prime has no `pendingMessageCount`. */
export const PrimeState = Schema.Struct({
  model: Schema.optionalKey(Schema.Unknown),
  thinkingLevel: Schema.optionalKey(Schema.String),
  isStreaming: Schema.Boolean,
  isCompacting: Schema.Boolean,
  steeringMode: Schema.optionalKey(Schema.String),
  followUpMode: Schema.optionalKey(Schema.String),
  sessionFile: Schema.optionalKey(Schema.String),
  sessionId: Schema.optionalKey(Schema.String),
  sessionName: Schema.optionalKey(Schema.String),
  autoCompactionEnabled: Schema.optionalKey(Schema.Boolean),
  messageCount: Schema.optionalKey(Schema.Number),
  sessionActions: Schema.optionalKey(PrimeSessionActionSnapshot),
  goal: Schema.optionalKey(PrimeGoalState),
});
export type PrimeState = typeof PrimeState.Type;

export const decodePrimeState = Schema.decodeUnknownOption(PrimeState);

export function isPrimeStateIdle(state: PrimeState): boolean {
  return (
    !state.isStreaming &&
    !state.isCompacting &&
    state.sessionActions?.active === undefined &&
    (state.sessionActions?.queuedCount ?? 0) === 0
  );
}

const PrimeOnlyFrame = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("session_action_update"),
    actions: PrimeSessionActionSnapshot,
  }),
  Schema.Struct({ type: Schema.Literal("goal_update"), goal: PrimeGoalState }),
  Schema.Struct({
    type: Schema.Literal("rlm_child_update"),
    child: Schema.Struct({
      id: Schema.String,
      parentId: Schema.optionalKey(Schema.String),
      activeSessionId: Schema.optionalKey(Schema.String),
      sessionName: Schema.optionalKey(Schema.String),
      label: Schema.String,
      status: Schema.String,
      sessionDir: Schema.optionalKey(Schema.String),
      error: Schema.optionalKey(Schema.String),
    }),
  }),
  Schema.Struct({
    type: Schema.Literal("compaction_start"),
    reason: Schema.String,
    customInstructions: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("compaction_end"),
    reason: Schema.String,
    result: Schema.optionalKey(Schema.Unknown),
    aborted: Schema.Boolean,
    willRetry: Schema.Boolean,
    errorMessage: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("auto_retry_start"),
    attempt: Schema.Number,
    maxAttempts: Schema.Number,
    delayMs: Schema.Number,
    errorMessage: Schema.String,
  }),
  Schema.Struct({
    type: Schema.Literal("auto_retry_end"),
    success: Schema.Boolean,
    attempt: Schema.Number,
    finalError: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("extension_error"),
    extensionPath: Schema.optionalKey(Schema.String),
    event: Schema.optionalKey(Schema.String),
    error: Schema.String,
  }),
  Schema.Struct({ type: Schema.Literal("refine_complete"), result: Schema.Unknown }),
  Schema.Struct({ type: Schema.Literal("refine_failed"), error: Schema.String }),
  Schema.Struct({
    type: Schema.Literals(["message_start", "message_end"]),
    message: Schema.Struct({
      role: Schema.Literal("custom"),
      customType: Schema.String,
      content: Schema.Unknown,
      display: Schema.Boolean,
      details: Schema.optionalKey(Schema.Unknown),
    }),
  }),
]);
export type PrimeOnlyFrame = typeof PrimeOnlyFrame.Type;
const decodePrimeOnlyFrame = Schema.decodeUnknownOption(PrimeOnlyFrame);

// Events Prime shares with Pi, plus Prime notices T3 does not map.
const PRIME_PASSTHROUGH_EVENT_TYPES: ReadonlySet<string> = new Set([
  "agent_start",
  "agent_end",
  "turn_start",
  "turn_end",
  "message_start",
  "message_update",
  "message_end",
  "tool_execution_start",
  "tool_execution_update",
  "tool_execution_end",
  "response",
  "extension_ui_request",
  "session_info_changed",
  "thinking_level_changed",
  "service_tier_changed",
  "auth_stale",
  "rlm_progress_note",
  "recap_update",
  "ipython_sent_agent_message",
  "bash_start",
  "bash_output",
  "bash_end",
  "observed_session_event",
  "observed_session_closed",
]);

export type PrimeEventFrame =
  | { readonly _tag: "Prime"; readonly frame: PrimeOnlyFrame }
  /**
   * Prime answers unknown commands and reports post-acceptance prompt failures
   * on the daemon path with a response that has no id (rpc-mode.ts,
   * daemon-mode.ts), so it cannot be routed to a pending request.
   */
  | {
      readonly _tag: "OrphanErrorResponse";
      readonly command: string | undefined;
      readonly error: string;
    }
  | { readonly _tag: "Passthrough"; readonly type: string }
  | { readonly _tag: "Unknown"; readonly type: string | undefined };

export function classifyPrimeEvent(record: PrimeRpcRecord): PrimeEventFrame {
  const type = record["type"];
  if (type === "response" && record["id"] === undefined && record["success"] === false) {
    return {
      _tag: "OrphanErrorResponse",
      command: primeRecordString(record, "command"),
      error: Predicate.isString(record["error"])
        ? record["error"]
        : summarizePrimeError(record["error"]),
    };
  }
  const prime = decodePrimeOnlyFrame(record);
  if (Option.isSome(prime)) return { _tag: "Prime", frame: prime.value };
  if (Predicate.isString(type) && PRIME_PASSTHROUGH_EVENT_TYPES.has(type)) {
    return { _tag: "Passthrough", type };
  }
  return { _tag: "Unknown", type: Predicate.isString(type) ? type : undefined };
}
export interface PrimeRpcSpawnOptions {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string | undefined;
  readonly env: NodeJS.ProcessEnv;
}

export interface PrimeRpcConnection {
  readonly send: (record: PrimeRpcRecord) => Effect.Effect<void, PrimeRpcError>;
  readonly request: (
    record: PrimeRpcRecord,
    timeoutMs?: number,
  ) => Effect.Effect<unknown, PrimeRpcError | PrimeRpcTimeoutError>;
  readonly events: Queue.Queue<PrimeRpcRecord, PrimeRpcError>;
  readonly exited: Effect.Effect<number, PrimeRpcError>;
  readonly terminate: Effect.Effect<void>;
}

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
/**
 * SIGTERM runs Prime's own shutdown (modes/rpc/rpc-mode.ts `shutdown`), whose
 * dispose sends `complete_owned_session` and waits while the daemon stops the
 * worker (up to 5 s for the worker's shutdown request). A SIGKILL before that
 * leaves the worker in its 30 s disconnect grace, and a resume of the same
 * session then fails with SessionAlreadyActiveError until the grace ends.
 */
export const PRIME_TERMINATION_GRACE = Duration.seconds(10);

interface PendingPrimeRequest {
  readonly command: string;
  readonly deferred: Deferred.Deferred<unknown, PrimeRpcError>;
}

// MCP completion events carry images in both model content and the script
// CallToolResult. Allow the shared image budget twice, plus bounded JSON/text
// overhead, before dropping an extension's oversized record.
const MAX_PRIME_RECORD_CHARS =
  2 * MAX_TOOL_OUTPUT_IMAGES * MAX_TOOL_OUTPUT_IMAGE_BASE64_LENGTH + 8 * 1024 * 1024;

function makeJsonlFramer() {
  let buffer = "";
  let dropping = false;
  return (chunk: string): ReadonlyArray<string> => {
    const lines: string[] = [];
    let start = 0;
    while (start < chunk.length) {
      const newline = chunk.indexOf("\n", start);
      const end = newline < 0 ? chunk.length : newline;
      if (!dropping) {
        if (buffer.length + end - start > MAX_PRIME_RECORD_CHARS) {
          buffer = "";
          dropping = true;
        } else {
          buffer += chunk.slice(start, end);
        }
      }
      if (newline < 0) break;
      if (!dropping && buffer.length > 0) {
        lines.push(buffer.endsWith("\r") ? buffer.slice(0, -1) : buffer);
      }
      buffer = "";
      dropping = false;
      start = newline + 1;
    }
    return lines;
  };
}

const UnknownFromJsonString = Schema.fromJsonString(Schema.Unknown);
const decodeJsonLine = Schema.decodeSync(UnknownFromJsonString);
const encodeJsonLine = Schema.encodeSync(UnknownFromJsonString);

const PRIME_ERROR_DETAIL_MAX_CHARS = 200;

function summarizePrimeError(error: unknown): string {
  const text = typeof error === "string" ? error : JSON.stringify(error);
  if (text === undefined) return "unknown error";
  return text.length > PRIME_ERROR_DETAIL_MAX_CHARS
    ? `${text.slice(0, PRIME_ERROR_DETAIL_MAX_CHARS)}…`
    : text;
}

/**
 * Stdout closure is not enough to diagnose an early crash: Pi 0.84+ (and so Prime) can exit 1
 * on import before writing any protocol line.
 */
function describePrimeStdoutClosure(exitCode: number | undefined): string {
  if (exitCode === undefined || !Number.isFinite(exitCode)) {
    return "prime-agent process closed stdout";
  }
  return `prime-agent process exited with code ${exitCode}`;
}

function parsePrimeRecord(line: string): PrimeRpcRecord | undefined {
  try {
    const parsed: unknown = decodeJsonLine(line);
    return Predicate.isObject(parsed) ? parsed : undefined;
  } catch {
    return undefined;
  }
}

/**
 * `hasExited` is consulted before each signal. Once the original child is
 * gone its pid/pgid can be recycled by the OS, so escalating blindly could
 * deliver SIGKILL to an unrelated process.
 */
export const terminatePrimeProcess = (
  kill: (signal: NodeJS.Signals) => boolean,
  hasExited: () => boolean,
  awaitExit: Effect.Effect<void>,
) =>
  Effect.gen(function* () {
    if (hasExited()) return;
    if (!kill("SIGTERM")) return;
    const exited = yield* awaitExit.pipe(Effect.timeoutOption(PRIME_TERMINATION_GRACE));
    if (Option.isSome(exited) || hasExited()) return;
    kill("SIGKILL");
  });

export const makePrimeRpcConnection = Effect.fnUntraced(function* (options: PrimeRpcSpawnOptions) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const platform = yield* HostProcess.Platform;
  const scope = yield* Effect.scope;

  const spawnCommand = yield* resolveSpawnCommand(options.command, [...options.args], {
    env: options.env,
  }).pipe(Effect.mapError((cause) => new PrimeRpcError({ operation: "spawn", cause })));
  const child = yield* spawner
    .spawn(
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        ...(options.cwd === undefined ? {} : { cwd: options.cwd }),
        env: options.env,
        extendEnv: false,
        shell: spawnCommand.shell,
        detached: platform !== "win32",
      }),
    )
    .pipe(Effect.mapError((cause) => new PrimeRpcError({ operation: "spawn", cause })));

  let childExited = false;
  let diagnosingStdoutClose = false;
  const exitDeferred = yield* Deferred.make<number, PrimeRpcError>();

  // Forked first. Before the reader, so an immediate crash populates
  // `exitDeferred` before stdout-close diagnosis runs. Before the terminate
  // finalizer, because finalizers run in reverse order: the watcher is then
  // still running while terminate waits for the exit, instead of being
  // interrupted first and making every close wait out the whole grace.
  yield* child.exitCode.pipe(
    Effect.matchEffect({
      onFailure: (cause) =>
        Deferred.fail(exitDeferred, new PrimeRpcError({ operation: "exit", cause })),
      onSuccess: (code) =>
        Effect.suspend(() => {
          childExited = true;
          return Deferred.succeed(exitDeferred, Number(code));
        }),
    }),
    Effect.forkIn(scope),
  );

  const killProcessGroup = (signal: NodeJS.Signals): boolean => {
    try {
      if (platform === "win32") {
        process.kill(Number(child.pid), signal);
      } else {
        signalProcessGroup(Number(child.pid), signal);
      }
      return true;
    } catch {
      return false;
    }
  };

  const hasExited = (): boolean => {
    if (childExited) return true;
    try {
      if (platform === "win32") process.kill(Number(child.pid), 0);
      else signalProcessGroup(Number(child.pid), 0);
      return false;
    } catch {
      return true;
    }
  };

  /**
   * Windows has no process groups, so `process.kill` reaches only prime-agent itself
   * and leaves extension subprocesses running with inherited stdio handles.
   * `taskkill /T` reaps the whole tree.
   */
  const terminateWindowsTree = Effect.gen(function* () {
    if (hasExited()) return;
    const taskkill = yield* spawner.spawn(
      ChildProcess.make("taskkill", ["/PID", String(child.pid), "/T", "/F"]),
    );
    yield* taskkill.exitCode;
  }).pipe(Effect.scoped, Effect.ignore);

  const terminateProcess =
    platform === "win32"
      ? terminateWindowsTree
      : terminatePrimeProcess(
          killProcessGroup,
          hasExited,
          Deferred.await(exitDeferred).pipe(Effect.ignore),
        ).pipe(
          // The grace bounds a real process's shutdown, so it runs on wall
          // time: under a TestClock, a large pending adjustment would expire
          // it at once and SIGKILL Prime before it releases its session lease.
          Effect.provideService(Clock.Clock, Clock.Clock.defaultValue()),
        );

  // Registered before any further setup: an interrupt or failure between the
  // spawn and the rest of this constructor would otherwise leak a detached
  // prime-agent process with no finalizer to reap it.
  yield* Scope.addFinalizer(scope, terminateProcess.pipe(Effect.ignore, Effect.uninterruptible));

  const pendingRequests = new Map<string, PendingPrimeRequest>();
  const events = yield* Queue.unbounded<PrimeRpcRecord, PrimeRpcError>();
  const outgoing = yield* Queue.unbounded<Uint8Array, PrimeRpcError>();
  const transportDown = yield* Deferred.make<never, PrimeRpcError>();
  let nextRequestId = 0;

  const failTransport = (error: PrimeRpcError) =>
    Effect.gen(function* () {
      const claimed = yield* Deferred.fail(transportDown, error);
      if (!claimed) return;
      for (const [key, pending] of pendingRequests) {
        pendingRequests.delete(key);
        yield* Deferred.fail(pending.deferred, error);
      }
      yield* Queue.fail(outgoing, error);
      yield* Queue.fail(events, error);
    });

  const routeRecord = (record: PrimeRpcRecord) =>
    Effect.gen(function* () {
      if (record["type"] === "response" && typeof record["id"] === "string") {
        const pending = pendingRequests.get(record["id"]);
        if (pending !== undefined) {
          pendingRequests.delete(record["id"]);
          if (record["success"] === true) {
            yield* Deferred.succeed(pending.deferred, record["data"]);
          } else {
            yield* Deferred.fail(
              pending.deferred,
              new PrimeRpcError({
                operation: String(record["command"] ?? "request"),
                detail: summarizePrimeError(record["error"]),
                ...(record["error"] === undefined ? {} : { cause: record["error"] }),
              }),
            );
          }
          return;
        }
      }
      // Prime answers a command it lacks (`get_entries` in 1.0.0) without the
      // request id (modes/rpc/rpc-mode.ts default branch). Matching it by
      // command fails that request now instead of at its timeout, and keeps it
      // from reaching the event path, where an id-less error ends the open turn.
      const command = record["command"];
      if (
        record["type"] === "response" &&
        record["id"] === undefined &&
        Predicate.isString(command) &&
        record["error"] === `Unknown command: ${command}`
      ) {
        for (const [key, pending] of pendingRequests) {
          if (pending.command !== command) continue;
          pendingRequests.delete(key);
          yield* Deferred.fail(
            pending.deferred,
            new PrimeRpcError({ operation: command, detail: record["error"] }),
          );
          return;
        }
      }
      yield* Queue.offer(events, record);
    });

  yield* Effect.gen(function* () {
    const frame = makeJsonlFramer();
    yield* child.stdout.pipe(
      Stream.decodeText(),
      Stream.runForEach((chunk) =>
        Effect.gen(function* () {
          const lines = frame(chunk);
          for (const line of lines) {
            const record = parsePrimeRecord(line);
            if (record === undefined) {
              yield* Effect.logDebug("Dropping non-JSON prime-agent stdout line.", {
                lineLength: line.length,
              });
              continue;
            }
            yield* routeRecord(record);
          }
        }),
      ),
    );
    for (const line of frame("\n")) {
      const trailing = parsePrimeRecord(line);
      if (trailing !== undefined) yield* routeRecord(trailing);
    }
  }).pipe(
    Effect.matchCauseEffect({
      onFailure: (cause) => failTransport(new PrimeRpcError({ operation: "read", cause })),
      onSuccess: () =>
        Effect.gen(function* () {
          // Claim the stdout-close path before waiting so a broken stdin
          // writer cannot replace the exit diagnosis with a write error.
          diagnosingStdoutClose = true;
          const polled = yield* Deferred.poll(exitDeferred);
          const maybeExitCode = Option.isSome(polled)
            ? yield* polled.value.pipe(
                Effect.map(Option.some),
                Effect.orElseSucceed(() => Option.none<number>()),
              )
            : yield* Deferred.await(exitDeferred).pipe(
                Effect.timeoutOption(Duration.millis(250)),
                // Adapter tests run under TestClock. A planned stdout close
                // without an exit, for example Stop-with-restart, must not wait
                // on that clock or the transport never fails.
                Effect.provideService(Clock.Clock, Clock.Clock.defaultValue()),
                Effect.orElseSucceed(() => Option.none<number>()),
              );
          return yield* failTransport(
            new PrimeRpcError({
              operation: "read",
              detail: describePrimeStdoutClosure(
                Option.isSome(maybeExitCode) ? maybeExitCode.value : undefined,
              ),
            }),
          );
        }),
    }),
    Effect.forkIn(scope),
  );

  yield* child.stderr.pipe(
    Stream.decodeText(),
    Stream.runForEach((chunk) =>
      chunk.trim().length === 0
        ? Effect.void
        : // Length only: Prime's stderr is unbounded remote output and can carry
          // credentials or prompt text, so it never enters a log annotation.
          Effect.logDebug("prime-agent stderr", { stderrLength: chunk.length }),
    ),
    Effect.ignore,
    Effect.forkIn(scope),
  );

  // Writer starts after the reader so an already-closed stdout can mark
  // diagnosis before a broken-pipe stdin claims the transport.
  yield* Stream.fromQueue(outgoing).pipe(
    Stream.run(child.stdin),
    Effect.catchCause((cause) =>
      diagnosingStdoutClose
        ? Effect.void
        : failTransport(new PrimeRpcError({ operation: "write", cause })),
    ),
    Effect.forkIn(scope),
  );

  const send = (record: PrimeRpcRecord): Effect.Effect<void, PrimeRpcError> =>
    Effect.gen(function* () {
      const accepted = yield* Queue.offer(
        outgoing,
        new TextEncoder().encode(`${encodeJsonLine(record)}\n`),
      );
      if (!accepted) {
        return yield* Deferred.await(transportDown);
      }
    });

  const request = (
    record: PrimeRpcRecord,
    timeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
  ): Effect.Effect<unknown, PrimeRpcError | PrimeRpcTimeoutError> =>
    Effect.gen(function* () {
      const id = `t3-${nextRequestId++}`;
      const deferred = yield* Deferred.make<unknown, PrimeRpcError>();
      pendingRequests.set(id, { command: String(record["type"]), deferred });
      yield* send({ ...record, id }).pipe(
        Effect.tapError(() => Effect.sync(() => pendingRequests.delete(id))),
      );
      return yield* Effect.raceFirst(Deferred.await(deferred), Deferred.await(transportDown)).pipe(
        Effect.timeoutOrElse({
          duration: Duration.millis(timeoutMs),
          orElse: () =>
            Effect.fail(
              new PrimeRpcTimeoutError({
                operation: String(record["type"] ?? "request"),
                timeoutMs,
              }),
            ),
        }),
        Effect.onInterrupt(() => Effect.sync(() => pendingRequests.delete(id))),
        Effect.onError(() => Effect.sync(() => pendingRequests.delete(id))),
      );
    });

  return {
    send,
    request,
    events,
    exited: Deferred.await(exitDeferred),
    terminate: failTransport(
      new PrimeRpcError({ operation: "terminate", detail: "prime-agent process was stopped" }),
    ).pipe(Effect.andThen(terminateProcess), Effect.ignore, Effect.uninterruptible),
  } satisfies PrimeRpcConnection;
});
