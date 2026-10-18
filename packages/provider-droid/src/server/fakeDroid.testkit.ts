/**
 * A scripted in-process `droid exec` for adapter tests: the spawner hands out one fake process
 * per spawn, each answering Droid's session methods from a shared session store and recording
 * every frame the adapter writes. Tests drive the rest (turn notifications, inbound requests,
 * exits) with `emit`, `emitInbound`, and `exit`. It never starts a real process.
 */
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { DROID_PROTOCOL_VERSION } from "./protocol.ts";

/** Outside the valid PID range, so cleanup of a fake process can never signal a real one. */
const FAKE_PID = 999_999_999;

const decodeFrame = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);
const encodeLine = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const encoder = new TextEncoder();

export type FakeDroidFrame = Readonly<Record<string, unknown>>;
export type FakeDroidSettings = Readonly<Record<string, unknown>>;

export type FakeDroidReply =
  | { readonly result: unknown }
  | { readonly error: { readonly code: number; readonly message: string } }
  /** The request is never answered. */
  | "silent";

export interface FakeDroidSession {
  readonly settings: FakeDroidSettings;
  readonly messages: ReadonlyArray<unknown>;
  /** What `fork_session` and `execute_rewind` named the session; `load_session` reports it. */
  readonly title?: string;
}

export interface FakeDroidOptions {
  /** Replaces the default answer to a method; `undefined` falls through to the default. */
  readonly overrides?: Readonly<
    Record<string, (params: Record<string, unknown>) => FakeDroidReply | undefined>
  >;
  /**
   * The `settings_updated` echoes sent for an `update_session_settings`, in order. Droid
   * sends several and the first can still show the previous level.
   */
  readonly settingsEchoes?: (
    next: FakeDroidSettings,
    previous: FakeDroidSettings,
  ) => ReadonlyArray<FakeDroidSettings>;
  /** Sessions `load_session` can find, keyed by session id. */
  readonly sessions?: Readonly<Record<string, FakeDroidSession>>;
  /** The model list `initialize_session` and `load_session` report (Droid's `availableModels`). */
  readonly availableModels?: ReadonlyArray<Readonly<Record<string, unknown>>>;
}

export interface FakeDroidRequest {
  readonly id: string;
  readonly method: string;
  readonly params: Record<string, unknown>;
}

export interface FakeDroidProcess {
  /** Everything the adapter wrote to stdin, in order. */
  readonly frames: () => ReadonlyArray<FakeDroidFrame>;
  readonly requests: () => ReadonlyArray<FakeDroidRequest>;
  /** Methods of the requests the adapter sent, in order. */
  readonly methods: () => ReadonlyArray<string>;
  /** Answers the adapter wrote to inbound requests, by request id. */
  readonly answers: () => ReadonlyArray<{ readonly id: string; readonly result: unknown }>;
  /**
   * Resolves once the adapter has handled everything the process emitted before the call: it
   * sends an inbound request and waits for the adapter's answer, and the adapter handles
   * Droid's frames in order.
   */
  readonly settle: Effect.Effect<void>;
  readonly sessionId: () => string | undefined;
  readonly settings: () => FakeDroidSettings | undefined;
  /** `sessionId` defaults to the process's current session; `null` sends an unscoped one. */
  readonly emit: (
    notification: Record<string, unknown>,
    sessionId?: string | null,
  ) => Effect.Effect<void>;
  readonly emitInbound: (id: string, method: string, params: unknown) => Effect.Effect<void>;
  readonly exit: (code?: number) => Effect.Effect<void>;
}

export interface FakeDroid {
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly sessions: Map<string, FakeDroidSession>;
  readonly processes: () => ReadonlyArray<FakeDroidProcess>;
  readonly current: () => FakeDroidProcess;
}

export const FAKE_DROID_DEFAULT_SETTINGS: FakeDroidSettings = {
  modelId: "claude-opus-5-5",
  reasoningEffort: "medium",
  interactionMode: "auto",
  autonomyLevel: "off",
  specModeModelId: "claude-opus-5-5",
  specModeReasoningEffort: "medium",
};

const SETTINGS_KEYS = [
  "modelId",
  "reasoningEffort",
  "interactionMode",
  "autonomyLevel",
  "specModeModelId",
  "specModeReasoningEffort",
] as const;

const mergeSettings = (
  settings: FakeDroidSettings,
  params: Record<string, unknown>,
): FakeDroidSettings => {
  const merged: Record<string, unknown> = { ...settings };
  for (const key of SETTINGS_KEYS) {
    if (params[key] !== undefined) merged[key] = params[key];
  }
  return merged;
};

const envelope = (fields: Record<string, unknown>) => ({
  jsonrpc: "2.0",
  factoryApiVersion: "1.0.0",
  factoryProtocolVersion: DROID_PROTOCOL_VERSION,
  ...fields,
});

const asRecord = (value: unknown): Record<string, unknown> =>
  Predicate.isObject(value) ? value : {};

const makeFakeProcess = Effect.fnUntraced(function* (
  store: Map<string, FakeDroidSession>,
  nextSessionId: () => string,
  options: FakeDroidOptions,
) {
  const scope = yield* Scope.Scope;
  const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
  const incoming = yield* Queue.unbounded<string>();
  const exitCode = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
  const decoder = new TextDecoder();
  let pendingInput = "";
  const frames: Array<FakeDroidFrame> = [];
  const answered = new Map<string, Deferred.Deferred<void>>();
  let settleCount = 0;
  let sessionId: string | undefined;

  const send = (value: unknown) => Queue.offer(stdout, encoder.encode(`${encodeLine(value)}\n`));
  const emit = (notification: Record<string, unknown>, scopedTo?: string | null) => {
    const target = scopedTo === undefined ? sessionId : (scopedTo ?? undefined);
    return send(
      envelope({
        type: "notification",
        method: "droid.session_notification",
        params: { ...(target === undefined ? {} : { sessionId: target }), notification },
      }),
    );
  };
  const exit = (code = 0) =>
    Queue.end(stdout).pipe(
      Effect.andThen(Deferred.succeed(exitCode, ChildProcessSpawner.ExitCode(code))),
      Effect.asVoid,
    );

  const respond = (id: string, reply: FakeDroidReply) => {
    if (reply === "silent") return Effect.void;
    return send(
      envelope({
        type: "response",
        id,
        ...("result" in reply ? { result: reply.result } : { error: reply.error }),
      }),
    );
  };

  const answerSignal = (id: string) => {
    const existing = answered.get(id);
    if (existing !== undefined) return existing;
    const created = Deferred.makeUnsafe<void>();
    answered.set(id, created);
    return created;
  };

  const currentSettings = () =>
    sessionId === undefined ? undefined : store.get(sessionId)?.settings;

  const save = (id: string, settings: FakeDroidSettings) =>
    store.set(id, { messages: [], ...store.get(id), settings });

  const handle = Effect.fnUntraced(function* (request: FakeDroidRequest) {
    const { id, method, params } = request;
    const override = options.overrides?.[method]?.(params);
    if (override !== undefined) {
      yield* respond(id, override);
      return;
    }
    switch (method) {
      case "droid.list_models":
        yield* respond(id, { result: { models: [] } });
        return;
      case "droid.initialize_session": {
        const created = nextSessionId();
        const settings = mergeSettings(FAKE_DROID_DEFAULT_SETTINGS, params);
        store.set(created, { settings, messages: [] });
        sessionId = created;
        yield* emit({ type: "settings_updated", settings });
        yield* respond(id, {
          result: {
            sessionId: created,
            hostId: "fake-host",
            session: { messages: [] },
            settings,
            availableModels: options.availableModels ?? [],
          },
        });
        return;
      }
      case "droid.load_session": {
        const wanted = params["sessionId"];
        const found = Predicate.isString(wanted) ? store.get(wanted) : undefined;
        if (found === undefined || !Predicate.isString(wanted)) {
          yield* respond(id, { error: { code: -32004, message: "Session not found" } });
          return;
        }
        sessionId = wanted;
        yield* emit({ type: "settings_updated", settings: found.settings }, null);
        yield* respond(id, {
          result: {
            session: { messages: found.messages, title: found.title ?? "Fake title" },
            hasOlderMessages: false,
            hostId: "fake-host",
            settings: found.settings,
            availableModels: options.availableModels ?? [],
            workingState: "idle",
          },
        });
        return;
      }
      case "droid.update_session_settings": {
        if (sessionId === undefined) {
          yield* respond(id, { error: { code: -32603, message: "No active session." } });
          return;
        }
        const previous = currentSettings() ?? FAKE_DROID_DEFAULT_SETTINGS;
        const next = mergeSettings(previous, params);
        save(sessionId, next);
        for (const echo of options.settingsEchoes?.(next, previous) ?? [next]) {
          yield* emit({ type: "settings_updated", requestId: id, settings: echo });
        }
        yield* respond(id, { result: {} });
        return;
      }
      case "droid.add_user_message": {
        const session = sessionId === undefined ? undefined : store.get(sessionId);
        if (sessionId !== undefined && session !== undefined) {
          store.set(sessionId, {
            ...session,
            messages: [
              ...session.messages,
              {
                id: params["messageId"],
                role: "user",
                content: [{ type: "text", text: params["text"] }],
                createdAt: 1_700_000_000_000,
              },
            ],
          });
        }
        yield* respond(id, { result: {} });
        return;
      }
      case "droid.execute_rewind": {
        const source = Predicate.isString(params["sessionId"])
          ? store.get(params["sessionId"])
          : undefined;
        const index =
          source?.messages.findIndex(
            (message) => asRecord(message)["id"] === params["messageId"],
          ) ?? -1;
        if (source === undefined || index < 0) {
          yield* respond(id, { error: { code: -32603, message: "Message not found" } });
          return;
        }
        // Like Droid, the rewind forks a new session and leaves the process on the one it holds.
        const created = nextSessionId();
        const forkTitle = params["forkTitle"];
        store.set(created, {
          settings: source.settings,
          messages: source.messages.slice(0, index),
          ...(Predicate.isString(forkTitle) ? { title: forkTitle } : {}),
        });
        yield* respond(id, {
          result: {
            newSessionId: created,
            restoredCount: 0,
            deletedCount: 0,
            failedRestoreCount: 0,
            failedDeleteCount: 0,
          },
        });
        return;
      }
      case "droid.fork_session": {
        // Like Droid, the session id param is ignored: the process's current session is forked, and
        // the process stays on it.
        const source = sessionId === undefined ? undefined : store.get(sessionId);
        if (source === undefined) {
          yield* respond(id, { error: { code: -32603, message: "No active session." } });
          return;
        }
        const created = nextSessionId();
        const title = params["title"];
        store.set(created, {
          settings: source.settings,
          messages: source.messages,
          ...(Predicate.isString(title) ? { title } : {}),
        });
        yield* respond(id, { result: { newSessionId: created } });
        return;
      }
      case "droid.compact_session": {
        const session = sessionId === undefined ? undefined : store.get(sessionId);
        if (sessionId === undefined || session === undefined || session.messages.length === 0) {
          yield* respond(id, {
            error: { code: -32603, message: "Failed to generate summary for compaction." },
          });
          return;
        }
        // Default-mode compaction keeps the session id; the divider notification follows the reply.
        yield* respond(id, {
          result: { newSessionId: sessionId, removedCount: session.messages.length },
        });
        yield* emit({
          type: "session_compacted",
          summaryId: "summary-1",
          removedCount: session.messages.length,
          visibleBoundaryMessageId: null,
        });
        return;
      }
      case "droid.close_session":
        yield* respond(id, { result: {} });
        yield* exit(0);
        return;
      default:
        yield* respond(id, { result: {} });
    }
  });

  yield* Effect.forever(
    Queue.take(incoming).pipe(
      Effect.flatMap((line) => {
        const frame = decodeFrame(line);
        frames.push(frame);
        if (frame["type"] === "response" && Predicate.isString(frame["id"])) {
          return Deferred.succeed(answerSignal(frame["id"]), undefined);
        }
        return frame["type"] === "request" && Predicate.isString(frame["id"])
          ? handle({
              id: frame["id"],
              method: String(frame["method"]),
              params: asRecord(frame["params"]),
            })
          : Effect.void;
      }),
    ),
  ).pipe(Effect.forkIn(scope));

  const handleProcess = ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(FAKE_PID),
    exitCode: Deferred.await(exitCode),
    isRunning: Effect.map(Deferred.isDone(exitCode), (done) => !done),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.forEach((chunk: Uint8Array) =>
      Effect.suspend(() => {
        pendingInput += decoder.decode(chunk, { stream: true });
        const lines = pendingInput.split("\n");
        pendingInput = lines.pop() ?? "";
        return Effect.forEach(lines, (line) => Queue.offer(incoming, line), { discard: true });
      }),
    ),
    stdout: Stream.fromQueue(stdout),
    stderr: Stream.empty,
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });

  const process: FakeDroidProcess = {
    frames: () => frames,
    requests: () =>
      frames.flatMap((frame) =>
        frame["type"] === "request" && Predicate.isString(frame["id"])
          ? [
              {
                id: frame["id"],
                method: String(frame["method"]),
                params: asRecord(frame["params"]),
              },
            ]
          : [],
      ),
    methods: () => process.requests().map((request) => request.method),
    answers: () =>
      frames.flatMap((frame) =>
        frame["type"] === "response" && Predicate.isString(frame["id"])
          ? [{ id: frame["id"], result: frame["result"] }]
          : [],
      ),
    settle: Effect.suspend(() => {
      const id = `fake-settle-${++settleCount}`;
      return send(
        envelope({
          type: "request",
          id,
          method: "droid.ask_user",
          params: { toolCallId: id, questions: [] },
        }),
      ).pipe(Effect.andThen(Deferred.await(answerSignal(id))));
    }),
    sessionId: () => sessionId,
    settings: currentSettings,
    emit,
    emitInbound: (id, method, params) =>
      send(envelope({ type: "request", id, method, params })).pipe(Effect.asVoid),
    exit,
  };
  return { handle: handleProcess, process };
});

export const makeFakeDroid = (options: FakeDroidOptions = {}) =>
  Effect.sync(() => {
    const sessions = new Map<string, FakeDroidSession>(Object.entries(options.sessions ?? {}));
    const processes: Array<FakeDroidProcess> = [];
    let sessionCounter = 0;
    const nextSessionId = () => `droid-session-${++sessionCounter}`;
    const spawner = ChildProcessSpawner.make(() =>
      makeFakeProcess(sessions, nextSessionId, options).pipe(
        Effect.map(({ handle, process }) => {
          processes.push(process);
          return handle;
        }),
      ),
    );
    const fake: FakeDroid = {
      spawner,
      sessions,
      processes: () => processes,
      current: () => {
        const latest = processes.at(-1);
        if (latest === undefined) throw new Error("the fake droid was never spawned");
        return latest;
      },
    };
    return fake;
  });
