import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ProviderReplayEntry,
  type ProviderReplayTranscript,
  type ProviderReplayEntry as ProviderReplayEntryType,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { ProviderAdapterDriverCreateError } from "@t3tools/provider-core/server/adapterDriver";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import type { OrchestratorV2ProviderReplayHarness } from "../testkit/ProviderReplayHarness.ts";
import { DROID_PROVIDER, DroidAdapterV2Driver } from "@t3tools/provider-droid/server";

export const DROID_RPC_REPLAY_PROTOCOL = "droid.stream-jsonrpc";
const DROID_REPLAY_ANY = "<any>";

const DroidRpcReplayTranscript = Schema.Struct({
  provider: Schema.Literal(DROID_PROVIDER),
  protocol: Schema.Literal(DROID_RPC_REPLAY_PROTOCOL),
  version: Schema.String,
  scenario: Schema.String,
  metadata: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
  entries: Schema.Array(ProviderReplayEntry),
});
type DroidRpcReplayTranscript = typeof DroidRpcReplayTranscript.Type;
const decodeDroidRpcReplayTranscript = Schema.decodeUnknownEffect(DroidRpcReplayTranscript);

class DroidReplayTranscriptDecodeError extends Schema.TaggedError<DroidReplayTranscriptDecodeError>()(
  "DroidReplayTranscriptDecodeError",
  {
    driver: Schema.optional(Schema.String),
    protocol: Schema.optional(Schema.String),
    scenario: Schema.optional(Schema.String),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to decode Droid RPC replay transcript for scenario ${this.scenario ?? "<unknown>"}.`;
  }
}

class DroidReplayMismatchError extends Schema.TaggedError<DroidReplayMismatchError>()(
  "DroidReplayMismatchError",
  {
    scenario: Schema.String,
    cursor: Schema.Number,
    process: Schema.Number,
    label: Schema.optional(Schema.String),
    actualType: Schema.optional(Schema.String),
    expected: Schema.Unknown,
    actual: Schema.Unknown,
  },
) {
  override get message(): string {
    return `Droid replay frame mismatch at cursor ${this.cursor} (expected ${this.label ?? "<none>"}, received ${this.actualType ?? "<untyped>"} from process ${this.process}) in scenario ${this.scenario}.`;
  }
}

class DroidReplayIncompleteError extends Schema.TaggedError<DroidReplayIncompleteError>()(
  "DroidReplayIncompleteError",
  {
    scenario: Schema.String,
    cursor: Schema.Number,
    remaining: Schema.Number,
    nextLabel: Schema.optional(Schema.String),
  },
) {
  override get message(): string {
    return `Droid replay ended with ${this.remaining} unconsumed entries at cursor ${this.cursor} (next ${this.nextLabel ?? "<none>"}) in scenario ${this.scenario}.`;
  }
}

function entryLabel(entry: ProviderReplayEntryType | undefined): string | undefined {
  if (entry === undefined) return undefined;
  return entry.type === "runtime_exit" ? "runtime_exit" : (entry.label ?? entry.type);
}

const DroidOrchestratorReplayHarnessError = Schema.Union([
  DroidReplayTranscriptDecodeError,
  ProviderAdapterDriverCreateError,
]);
type DroidOrchestratorReplayHarnessError = typeof DroidOrchestratorReplayHarnessError.Type;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function replayValueMatches(expected: unknown, actual: unknown): boolean {
  if (expected === DROID_REPLAY_ANY) return true;
  if (Array.isArray(expected)) {
    return (
      Array.isArray(actual) &&
      expected.length === actual.length &&
      expected.every((entry, index) => replayValueMatches(entry, actual[index]))
    );
  }
  if (isRecord(expected)) {
    if (!isRecord(actual)) return false;
    const keys = new Set([...Object.keys(expected), ...Object.keys(actual)]);
    return [...keys].every((key) => replayValueMatches(expected[key], actual[key]));
  }
  return Object.is(expected, actual);
}

const ADAPTER_REQUEST_ID = /^\d+$/u;

/** Droid's own requests carry ids Droid chose, so only an outbound `request` frame has an adapter id. */
function adapterRequestId(frame: unknown): string | undefined {
  if (!isRecord(frame) || frame.type !== "request") return undefined;
  return typeof frame.id === "string" && ADAPTER_REQUEST_ID.test(frame.id) ? frame.id : undefined;
}

/** The id the adapter gives a turn's `add_user_message`; it embeds uuids minted per run. */
function turnMessageId(frame: unknown): string | undefined {
  if (!isRecord(frame) || frame.type !== "request" || !isRecord(frame.params)) return undefined;
  return typeof frame.params.messageId === "string" ? frame.params.messageId : undefined;
}

/**
 * Compares a recorded request against the ids the adapter minted for this run, so the
 * correlation Droid recorded holds without pinning counter values or per-run uuids. The id of a
 * `response` the adapter writes back to a Droid request stays exact.
 *
 * A recorded message id the replay has already bound names an existing turn (the target of a
 * rewind), so the adapter must send the id it chose for that turn. An unbound one is a new turn.
 */
function withAdapterIds(
  expected: unknown,
  actual: unknown,
  boundMessageIds: ReadonlyMap<string, string>,
): unknown {
  if (!isRecord(expected)) return expected;
  const actualId = adapterRequestId(actual);
  const actualMessageId = turnMessageId(actual);
  const expectedMessageId = turnMessageId(expected);
  const withId =
    adapterRequestId(expected) !== undefined && actualId !== undefined
      ? { ...expected, id: actualId }
      : expected;
  return isRecord(withId.params) && expectedMessageId !== undefined && actualMessageId !== undefined
    ? {
        ...withId,
        params: {
          ...withId.params,
          messageId: boundMessageIds.get(expectedMessageId) ?? actualMessageId,
        },
      }
    : withId;
}

function replaceMessageIds(value: unknown, ids: ReadonlyMap<string, string>): unknown {
  if (ids.size === 0) return value;
  if (typeof value === "string") {
    let replaced = value;
    for (const [recorded, actual] of ids) replaced = replaced.replaceAll(recorded, actual);
    return replaced;
  }
  if (Array.isArray(value)) return value.map((entry) => replaceMessageIds(entry, ids));
  if (!isRecord(value)) return value;
  return Object.fromEntries(
    Object.entries(value).map(([key, entry]) => [key, replaceMessageIds(entry, ids)]),
  );
}

const TURN_STARTING_METHODS = new Set([
  "droid.initialize_session",
  "droid.load_session",
  "droid.add_user_message",
]);

function startsTurn(entry: ProviderReplayEntryType): boolean {
  if (entry.type !== "expect_outbound" || !isRecord(entry.frame)) return false;
  return (
    entry.frame.type === "process_start" ||
    (entry.frame.type === "request" &&
      typeof entry.frame.method === "string" &&
      TURN_STARTING_METHODS.has(entry.frame.method))
  );
}

function processStartFrame(command: ChildProcess.Command) {
  const args = ChildProcess.isStandardCommand(command) ? command.args : [];
  return { type: "process_start", args: [...args] };
}

function entryProcess(entry: ProviderReplayEntryType): number {
  const label = entry.type === "runtime_exit" ? undefined : entry.label;
  const match = label === undefined ? null : /@p(\d+)$/u.exec(label);
  return match === null ? 1 : Number(match[1]);
}

const decodeJsonLine = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const encodeJsonLine = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

function makeLineSplitter() {
  let buffer = "";
  const decoder = new TextDecoder();
  return (chunk: Uint8Array): ReadonlyArray<string> => {
    buffer += decoder.decode(chunk, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";
    return lines.map((line) => line.replace(/\r$/u, "")).filter((line) => line.length > 0);
  };
}

interface DroidReplayProcess {
  readonly ordinal: number;
  readonly stdout: Queue.Queue<Uint8Array, Cause.Done>;
  readonly exited: Deferred.Deferred<ChildProcessSpawner.ExitCode>;
  readonly ids: Map<string, string>;
  open: boolean;
}

/**
 * Independent adapter fibers can reach stdin in a different order than recorded, so a write
 * that matches a later outbound record of the same turn is held until the transcript reaches
 * it. A write that matches nothing before the next turn-starting record fails immediately.
 */
class DroidReplayController {
  private cursor = 0;
  private failure: unknown = null;
  private readonly consumed = new Set<number>();
  private readonly held: Array<{
    readonly process: DroidReplayProcess;
    readonly actual: unknown;
  }> = [];
  private readonly processes: Array<DroidReplayProcess> = [];
  /** Recorded message id to the one the adapter chose; Droid's later frames and history echo it. */
  private readonly messageIds = new Map<string, string>();
  private readonly transcript: DroidRpcReplayTranscript;

  constructor(transcript: DroidRpcReplayTranscript) {
    this.transcript = transcript;
  }

  spawn(
    command: ChildProcess.Command,
    stdout: Queue.Queue<Uint8Array, Cause.Done>,
  ): DroidReplayProcess {
    const process: DroidReplayProcess = {
      ordinal: this.processes.length + 1,
      stdout,
      exited: Deferred.makeUnsafe(),
      ids: new Map(),
      open: true,
    };
    this.processes.push(process);
    this.receive(process, processStartFrame(command));
    return process;
  }

  receive(process: DroidReplayProcess, actual: unknown): void {
    if (this.failure !== null) return;
    const entries = this.transcript.entries;
    let possible = false;
    for (let index = this.cursor; index < entries.length && !possible; index += 1) {
      const entry = entries[index]!;
      const pending = entry.type === "expect_outbound" && !this.consumed.has(index);
      possible =
        pending &&
        entryProcess(entry) === process.ordinal &&
        replayValueMatches(withAdapterIds(entry.frame, actual, this.messageIds), actual);
      if (!possible && pending && startsTurn(entry)) break;
    }
    if (!possible) {
      const expected = entries[this.cursor];
      const label = entryLabel(expected);
      const actualType = isRecord(actual) ? (actual.method ?? actual.type) : undefined;
      this.fail(
        new DroidReplayMismatchError({
          scenario: this.transcript.scenario,
          cursor: this.cursor,
          process: process.ordinal,
          ...(label === undefined ? {} : { label }),
          ...(typeof actualType === "string" ? { actualType } : {}),
          expected: expected ?? null,
          actual,
        }),
      );
      return;
    }
    this.held.push({ process, actual });
    this.advance();
  }

  /** Droid exits once its stdin closes after `close_session`, so the replayed process does too. */
  close(process: DroidReplayProcess): void {
    process.open = false;
    Queue.endUnsafe(process.stdout);
    Deferred.doneUnsafe(process.exited, Effect.succeed(ChildProcessSpawner.ExitCode(0)));
  }

  assertComplete(): void {
    if (this.failure !== null) throw this.failure;
    if (this.cursor !== this.transcript.entries.length || this.held.length > 0) {
      const nextLabel = entryLabel(this.transcript.entries[this.cursor]);
      throw new DroidReplayIncompleteError({
        scenario: this.transcript.scenario,
        cursor: this.cursor,
        remaining: this.transcript.entries.length - this.cursor,
        ...(nextLabel === undefined ? {} : { nextLabel }),
      });
    }
  }

  private advance(): void {
    let progressed = true;
    while (progressed && this.failure === null) {
      progressed = this.drainInbound();
      for (const [heldIndex, write] of this.held.entries()) {
        if (this.consumePending(write.process, write.actual)) {
          this.held.splice(heldIndex, 1);
          progressed = true;
          break;
        }
      }
    }
  }

  private consumePending(process: DroidReplayProcess, actual: unknown): boolean {
    const entries = this.transcript.entries;
    for (let index = this.cursor; index < entries.length; index += 1) {
      const entry = entries[index]!;
      if (entry.type !== "expect_outbound") return false;
      if (this.consumed.has(index) || entryProcess(entry) !== process.ordinal) continue;
      if (!replayValueMatches(withAdapterIds(entry.frame, actual, this.messageIds), actual))
        continue;
      const recordedId = adapterRequestId(entry.frame);
      const actualId = adapterRequestId(actual);
      if (recordedId !== undefined && actualId !== undefined) {
        process.ids.set(recordedId, actualId);
      }
      const recordedMessageId = turnMessageId(entry.frame);
      const actualMessageId = turnMessageId(actual);
      if (recordedMessageId !== undefined && actualMessageId !== undefined) {
        this.messageIds.set(recordedMessageId, actualMessageId);
      }
      this.consumed.add(index);
      while (this.consumed.has(this.cursor)) this.cursor += 1;
      return true;
    }
    return false;
  }

  private drainInbound(): boolean {
    let emitted = false;
    while (this.failure === null) {
      const entry = this.transcript.entries[this.cursor];
      if (entry?.type === "runtime_exit") {
        this.cursor += 1;
        emitted = true;
        const process = this.processes.at(-1);
        if (process !== undefined) this.close(process);
        continue;
      }
      if (entry?.type !== "emit_inbound") return emitted;
      const process = this.processes[entryProcess(entry) - 1];
      if (process === undefined) return emitted;
      this.cursor += 1;
      emitted = true;
      if (!process.open) continue;
      const frame = entry.frame;
      const recordedId = isRecord(frame) && frame.type === "response" ? frame.id : undefined;
      const reboundId = typeof recordedId === "string" ? process.ids.get(recordedId) : undefined;
      const rebound = replaceMessageIds(
        isRecord(frame) && reboundId !== undefined ? { ...frame, id: reboundId } : frame,
        this.messageIds,
      );
      Queue.offerUnsafe(process.stdout, new TextEncoder().encode(`${encodeJsonLine(rebound)}\n`));
    }
    return emitted;
  }

  private fail(cause: unknown): void {
    if (this.failure !== null) return;
    this.failure = cause;
    for (const process of this.processes) this.close(process);
  }
}

/** Deliberately outside the valid pid range so the runtime's group kill never lands. */
const REPLAY_PID = 999_999_999;

function makeDroidReplaySpawner(
  controller: DroidReplayController,
): ChildProcessSpawner.ChildProcessSpawner["Service"] {
  return ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      const process = controller.spawn(command, yield* Queue.unbounded<Uint8Array, Cause.Done>());
      yield* Effect.addFinalizer(() => Effect.sync(() => controller.close(process)));
      const split = makeLineSplitter();
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(REPLAY_PID),
        exitCode: Deferred.await(process.exited),
        isRunning: Effect.map(Deferred.isDone(process.exited), (done) => !done),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach((chunk: Uint8Array) =>
          Effect.sync(() => {
            for (const line of split(chunk)) controller.receive(process, decodeJsonLine(line));
          }),
        ).pipe(Sink.ensuring(Effect.sync(() => controller.close(process)))),
        stdout: Stream.fromQueue(process.stdout),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
    }),
  );
}

export function layer<E, R>(input: {
  readonly scenario: string;
  readonly spawner: Layer.Layer<ChildProcessSpawner.ChildProcessSpawner, E, R>;
  readonly binaryPath: string;
}) {
  return ProviderAdapterRegistry.layerFromDrivers({
    drivers: [DroidAdapterV2Driver],
    configMap: {
      [DROID_PROVIDER]: {
        driver: DROID_PROVIDER,
        enabled: true,
        config: { binaryPath: input.binaryPath },
      },
    },
  }).pipe(
    Layer.provide(input.spawner),
    Layer.provide(
      Layer.mergeAll(
        TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
        NodeServices.layer,
        IdAllocator.layer,
      ),
    ),
  );
}

export const DroidOrchestratorReplayHarness: OrchestratorV2ProviderReplayHarness<
  DroidRpcReplayTranscript,
  DroidOrchestratorReplayHarnessError
> = {
  driver: DROID_PROVIDER,
  decodeTranscript: (transcript: ProviderReplayTranscript) =>
    decodeDroidRpcReplayTranscript(transcript).pipe(
      Effect.mapError(
        (cause) =>
          new DroidReplayTranscriptDecodeError({
            driver: transcript.provider,
            protocol: transcript.protocol,
            scenario: transcript.scenario,
            cause,
          }),
      ),
    ),
  makeProviderAdapterRegistryLayer: (transcript) =>
    layer({
      scenario: transcript.scenario,
      binaryPath: "droid",
      spawner: Layer.effect(
        ChildProcessSpawner.ChildProcessSpawner,
        Effect.gen(function* () {
          const controller = new DroidReplayController(transcript);
          yield* Effect.addFinalizer(() => Effect.sync(() => controller.assertComplete()));
          return makeDroidReplaySpawner(controller);
        }),
      ),
    }),
};

/** The label a recorded frame gets: what it is, and for a response, which request it answers. */
function recordLabel(
  frame: unknown,
  direction: "expect_outbound" | "emit_inbound",
  process: number,
  methods: {
    readonly outbound: Map<string, string>;
    readonly inbound: Map<string, string>;
  },
): string | undefined {
  if (!isRecord(frame)) return undefined;
  const id = typeof frame.id === "string" ? frame.id : undefined;
  const method = typeof frame.method === "string" ? frame.method : undefined;
  let base: string | undefined;
  if (frame.type === "request" && method !== undefined) {
    (direction === "expect_outbound" ? methods.outbound : methods.inbound).set(id ?? "", method);
    base = `request:${method}`;
  } else if (frame.type === "response") {
    const answered = (direction === "emit_inbound" ? methods.outbound : methods.inbound).get(
      id ?? "",
    );
    base = answered === undefined ? "response" : `response:${answered}`;
  } else if (frame.type === "notification") {
    const type = isRecord(frame.params) ? frame.params.notification : undefined;
    const notificationType = isRecord(type) ? type.type : undefined;
    base =
      typeof notificationType === "string" ? `notification:${notificationType}` : "notification";
  } else if (typeof frame.type === "string") {
    base = frame.type;
  }
  if (base === undefined || process === 1) return base;
  return `${base}@p${process}`;
}

export function makeDroidRecordingSpawner(
  live: ChildProcessSpawner.ChildProcessSpawner["Service"],
  entries: Array<ProviderReplayEntryType>,
): ChildProcessSpawner.ChildProcessSpawner["Service"] {
  let spawned = 0;
  return ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      const handle = yield* live.spawn(command);
      const process = ++spawned;
      const methods = { outbound: new Map<string, string>(), inbound: new Map<string, string>() };
      const record = (type: "expect_outbound" | "emit_inbound", frame: unknown) => {
        const label = recordLabel(frame, type, process, methods);
        entries.push({ type, ...(label === undefined ? {} : { label }), frame });
      };
      const recordLines = (
        type: "expect_outbound" | "emit_inbound",
        lines: ReadonlyArray<string>,
      ) => {
        for (const line of lines) {
          try {
            record(type, decodeJsonLine(line));
          } catch {
            // The runtime drops non-JSON stdout lines, so the transcript does too.
          }
        }
      };
      record("expect_outbound", processStartFrame(command));
      const splitIn = makeLineSplitter();
      const splitOut = makeLineSplitter();
      return ChildProcessSpawner.makeHandle({
        ...handle,
        stdin: handle.stdin.pipe(
          Sink.mapInput((chunk: Uint8Array) => {
            recordLines("expect_outbound", splitIn(chunk));
            return chunk;
          }),
        ),
        stdout: handle.stdout.pipe(
          Stream.tap((chunk) => Effect.sync(() => recordLines("emit_inbound", splitOut(chunk)))),
        ),
      });
    }),
  );
}
