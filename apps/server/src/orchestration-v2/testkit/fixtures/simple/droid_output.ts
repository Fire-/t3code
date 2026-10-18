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

export function droidProcessStartIndexes(transcript: ProviderReplayTranscript) {
  return transcript.entries.flatMap((entry, index) =>
    entry.type === "expect_outbound" && field(entry.frame, "type") === "process_start"
      ? [index]
      : [],
  );
}

/** What Droid answered to one of the adapter's requests: the first response after it with its id. */
function droidResponseTo(
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
