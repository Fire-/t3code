import { assert } from "@effect/vitest";
import { ProviderDriverKind, type ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertAssistantTextIncludes,
  assertBaseProjection,
  assertRuntimeRequestCounts,
  assertSemanticProjectionIntegrity,
  assertTurnItemTypes,
  assertUserMessageInputIntents,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  MESSAGE_STEERING_INITIAL_PROMPT,
  MESSAGE_STEERING_STEER_PROMPT,
  projectionFor,
} from "../shared.ts";
import { droidRequests } from "../simple/droid_output.ts";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

/**
 * Droid takes a steer as `add_user_message` with `queuePlacement: "end_of_turn"` on the running
 * session. Whether it folds the steer into the native turn or runs it as the next native turn,
 * the t3 turn stays one run, and the steer's `user_message` item is the orchestrator's.
 */
export function assertDroidMessageSteeringOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const sends = droidRequests(transcript, "add_user_message");
  assert.deepEqual(
    sends.map((send) => [field(send.params, "text"), field(send.params, "queuePlacement")]),
    [
      [MESSAGE_STEERING_INITIAL_PROMPT, undefined],
      [MESSAGE_STEERING_STEER_PROMPT, "end_of_turn"],
    ],
    "the first message starts the turn; the steer queues at the end of the running turn",
  );
  assert.lengthOf(
    droidRequests(transcript, "interrupt_session"),
    0,
    "steering must not interrupt the running Droid turn",
  );

  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["completed"] });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assertTurnItemTypes(projection, ["user_message", "assistant_message"]);
  assertRuntimeRequestCounts(projection, { total: 0 });
  assertUserMessageInputIntents(projection, ["turn_start", "steer"]);

  const steers = projection.turnItems.flatMap((item) =>
    item.type === "user_message" && item.inputIntent === "steer" ? [item] : [],
  );
  assert.lengthOf(
    steers,
    1,
    "the orchestrator writes exactly one steer item; the adapter adds none",
  );
  assert.equal(steers[0]?.text, MESSAGE_STEERING_STEER_PROMPT);

  const [run] = projection.runs;
  assert.equal(steers[0]?.runId, run?.id, "the steer belongs to the run it steered");
  assertAssistantTextIncludes(projection, "steering fixture observed");
  const reply = projection.turnItems.find(
    (item) => item.type === "assistant_message" && item.text.includes("steering fixture observed"),
  );
  assert.equal(reply?.runId, run?.id, "the steer's reply lands in the same run");

  assert.lengthOf(projection.providerTurns, 1, "a steer never opens a second provider turn");
  const [providerTurn] = projection.providerTurns;
  assert.equal(providerTurn?.status, "completed");
  const turnStart = projection.turnItems.find(
    (item) => item.type === "user_message" && item.inputIntent === "turn_start",
  );
  assert.deepEqual(providerTurn?.nativeTurnRef, {
    driver: ProviderDriverKind.make("droid"),
    nativeId: String(turnStart?.type === "user_message" ? turnStart.messageId : ""),
    strength: "strong",
  });
  assert.isFalse(
    projection.turnItems.some((item) => item.status === "failed" || item.type === "error"),
    "a steered turn has no failed item",
  );
}
