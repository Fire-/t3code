import { assert } from "@effect/vitest";
import {
  ProviderDriverKind,
  type OrchestrationV2ThreadProjection,
  type ProviderReplayTranscript,
} from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import { DROID_MODEL_SELECTION, projectionFor } from "../shared.ts";
import { assertSimpleOutput } from "./codex_output.ts";

const DROID_DRIVER = ProviderDriverKind.make("droid");
const AUTONOMY_LEVELS = ["off", "low", "medium", "high"];

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

export interface DroidRecordedRequest {
  /** Position in the transcript, so two requests can be ordered across processes. */
  readonly index: number;
  readonly id: unknown;
  readonly method: string;
  readonly params: unknown;
}

/** The requests the adapter wrote, in order, across every Droid process of the recording. */
export function droidRequests(
  transcript: ProviderReplayTranscript,
  name?: string,
): ReadonlyArray<DroidRecordedRequest> {
  return transcript.entries.flatMap((entry, index) => {
    if (entry.type !== "expect_outbound" || field(entry.frame, "type") !== "request") return [];
    const method = field(entry.frame, "method");
    if (typeof method !== "string" || (name !== undefined && method !== `droid.${name}`)) return [];
    return [{ index, id: field(entry.frame, "id"), method, params: field(entry.frame, "params") }];
  });
}

/** The requests Droid itself sent the adapter (`ask_user`, `request_permission`), in order. */
export function droidInboundRequests(
  transcript: ProviderReplayTranscript,
  name: string,
): ReadonlyArray<DroidRecordedRequest> {
  return transcript.entries.flatMap((entry, index) => {
    if (entry.type !== "emit_inbound" || field(entry.frame, "type") !== "request") return [];
    if (field(entry.frame, "method") !== `droid.${name}`) return [];
    return [
      {
        index,
        id: field(entry.frame, "id"),
        method: `droid.${name}`,
        params: field(entry.frame, "params"),
      },
    ];
  });
}

/** Droid's requests to leave spec mode: `request_permission` with confirmationType `exit_spec_mode`. */
export function droidExitSpecModeRequests(
  transcript: ProviderReplayTranscript,
): ReadonlyArray<DroidRecordedRequest> {
  return droidInboundRequests(transcript, "request_permission").filter((request) => {
    const toolUses = field(request.params, "toolUses");
    return Array.isArray(toolUses) && field(toolUses[0], "confirmationType") === "exit_spec_mode";
  });
}

/** The `result` the adapter wrote back to one of Droid's own requests. */
export function adapterReplyTo(
  transcript: ProviderReplayTranscript,
  inbound: DroidRecordedRequest,
): unknown {
  const replies = transcript.entries.flatMap((entry, index) =>
    index > inbound.index &&
    entry.type === "expect_outbound" &&
    field(entry.frame, "type") === "response" &&
    field(entry.frame, "id") === inbound.id
      ? [entry.frame]
      : [],
  );
  assert.lengthOf(replies, 1, `the adapter answers ${inbound.method} exactly once`);
  return field(replies[0], "result");
}

export interface DroidRecordedNotification {
  readonly index: number;
  readonly notification: unknown;
}

/** The session notifications Droid sent, in order, optionally of one `type`. */
export function droidNotifications(
  transcript: ProviderReplayTranscript,
  type?: string,
): ReadonlyArray<DroidRecordedNotification> {
  return transcript.entries.flatMap((entry, index) => {
    if (entry.type !== "emit_inbound") return [];
    if (field(entry.frame, "method") !== "droid.session_notification") return [];
    const notification = field(field(entry.frame, "params"), "notification");
    return type === undefined || field(notification, "type") === type
      ? [{ index, notification }]
      : [];
  });
}

/** Tools that write files, which Droid never runs in spec mode. */
export const DROID_FILE_WRITE_TOOLS: ReadonlyArray<string> = [
  "Create",
  "Edit",
  "ApplyPatch",
  "MultiEdit",
];

/** Every tool that can change the workspace. Spec mode may still run `Execute` for read-only commands. */
export const DROID_WRITE_TOOLS: ReadonlyArray<string> = [...DROID_FILE_WRITE_TOOLS, "Execute"];

/** The tool calls Droid announced, in order; repeated frames for one call are kept. */
export function droidToolCalls(
  transcript: ProviderReplayTranscript,
): ReadonlyArray<{ readonly index: number; readonly name: string }> {
  return droidNotifications(transcript, "tool_call").flatMap(({ index, notification }) => {
    const name = field(field(notification, "toolUse"), "name");
    return typeof name === "string" ? [{ index, name }] : [];
  });
}

/**
 * The settings the session had when the request at `requestIndex` went out: the params of the
 * latest settings-bearing frame (`initialize_session`, or `update_session_settings` on a resumed
 * or changed session) written before it.
 */
export function droidSettingsFrameBefore(
  transcript: ProviderReplayTranscript,
  requestIndex: number,
) {
  const frames = droidRequests(transcript).filter(
    (request) =>
      request.index < requestIndex &&
      (request.method === "droid.initialize_session" ||
        request.method === "droid.update_session_settings"),
  );
  const latest = frames.at(-1);
  assert.isDefined(latest, "a settings frame precedes the message");
  return latest?.params;
}

export function droidProcessStartIndexes(transcript: ProviderReplayTranscript) {
  return transcript.entries.flatMap((entry, index) =>
    entry.type === "expect_outbound" && field(entry.frame, "type") === "process_start"
      ? [index]
      : [],
  );
}

/** What Droid answered to one of the adapter's requests: the first response after it with its id. */
export function droidResponseTo(
  transcript: ProviderReplayTranscript,
  request: DroidRecordedRequest,
): unknown {
  const entry = transcript.entries.find(
    (candidate, index) =>
      index > request.index &&
      candidate.type === "emit_inbound" &&
      field(candidate.frame, "type") === "response" &&
      field(candidate.frame, "id") === request.id,
  );
  assert.isDefined(entry, `Droid must answer ${request.method} (request ${String(request.id)})`);
  return entry?.type === "runtime_exit" ? undefined : field(entry?.frame, "result");
}

/** The session id Droid returned for the one `initialize_session` of a recording. */
export function droidInitializedSessionId(transcript: ProviderReplayTranscript): string {
  const initializes = droidRequests(transcript, "initialize_session");
  assert.lengthOf(initializes, 1, "a new thread opens exactly one Droid session");
  const sessionId = field(droidResponseTo(transcript, initializes[0]!), "sessionId");
  assert.isString(sessionId);
  return sessionId as string;
}

/** One Droid session behind one provider thread whose native ref is that session's id. */
export function assertDroidSessionThread(
  projection: OrchestrationV2ThreadProjection,
  sessionId: string,
) {
  assert.lengthOf(projection.providerThreads, 1);
  const [providerThread] = projection.providerThreads;
  assert.deepEqual(providerThread?.nativeThreadRef, {
    driver: DROID_DRIVER,
    nativeId: sessionId,
    strength: "strong",
  });
}

/**
 * Each turn's native ref is the message id the adapter sent for it. The recorded ids are
 * placeholders (the orchestrator mints new ones each run), so the check reads the live
 * `user_message` items, whose ids are the ones the replay saw in `add_user_message`.
 */
export function assertDroidTurnIdentity(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const sends = droidRequests(transcript, "add_user_message");
  const projection = projectionFor(result, transcript.scenario);
  const turns = projection.providerTurns.toSorted((left, right) => left.ordinal - right.ordinal);
  const messageIds = projection.turnItems
    .flatMap((item) => (item.type === "user_message" ? [item] : []))
    .toSorted((left, right) => left.ordinal - right.ordinal)
    .map((item) => String(item.messageId));
  assert.lengthOf(turns, sends.length, "every add_user_message starts one provider turn");
  assert.lengthOf(messageIds, sends.length, "every turn answers one user message");
  assert.lengthOf(new Set(messageIds), sends.length, "each turn sends its own messageId");
  for (const [index, turn] of turns.entries()) {
    assert.deepEqual(turn.nativeTurnRef, {
      driver: DROID_DRIVER,
      nativeId: String(messageIds[index]),
      strength: "strong",
    });
  }
}

/** Synthetic item ids carry the provider turn id so two turns never collide. */
function assertDroidItemIdsIncludeProviderTurn(projection: OrchestrationV2ThreadProjection) {
  const items = projection.turnItems.filter(
    (item) =>
      item.type === "assistant_message" ||
      item.type === "reasoning" ||
      item.type === "command_execution",
  );
  assert.isAbove(items.length, 0);
  for (const item of items) {
    assert.isNotNull(item.providerTurnId, `${item.type} item must belong to a provider turn`);
    assert.include(item.nativeItemRef?.nativeId ?? "", item.providerTurnId ?? "");
  }
}

export function assertDroidSimpleOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertSimpleOutput(result, transcript);
  const projection = projectionFor(result, transcript.scenario);

  const sessionId = droidInitializedSessionId(transcript);
  const [initialize] = droidRequests(transcript, "initialize_session");
  assert.lengthOf(droidRequests(transcript, "load_session"), 0);
  assert.equal(field(initialize?.params, "interactionMode"), "auto");
  assert.include(AUTONOMY_LEVELS, field(initialize?.params, "autonomyLevel"));
  assert.equal(field(initialize?.params, "modelId"), DROID_MODEL_SELECTION.model);
  assert.isString(field(initialize?.params, "cwd"));
  assertDroidSessionThread(projection, sessionId);

  assertDroidTurnIdentity(result, transcript);
  assertDroidItemIdsIncludeProviderTurn(projection);

  const usage = projection.providerThreads[0]?.contextUsage;
  assert.isAbove(usage?.usedTokens ?? 0, 0, "the turn must report the context size it used");
}
