import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertRunOrdinals,
  assertSemanticProjectionIntegrity,
  assertVisibleUserMessagesExclude,
  assertVisibleUserMessagesInclude,
  projectionFor,
  THREAD_ROLLBACK_FIRST_PROMPT,
  THREAD_ROLLBACK_SECOND_PROMPT,
  TURN_INTERRUPT_MID_TOOL_PROMPT,
} from "../shared.ts";
import { droidRequests } from "../simple/droid_output.ts";
import { DROID_ASSISTANT_REPLIES_PROMPT } from "../thread_rollback/droid_input.ts";
import { assertDroidRewind } from "../thread_rollback/droid_output.ts";

/**
 * The stopped turn is the first one the rollback discards. Its rewind target is the message id the
 * adapter sent for it, so the Stop that ends the turn cannot lose it.
 */
export function assertDroidThreadRollbackAfterStopOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({ result, transcript, runCount: 4 });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertRunOrdinals(projection, [1, 2, 3, 4]);
  assert.deepEqual(
    projection.runs.map((run) => run.status),
    ["completed", "rolled_back", "rolled_back", "completed"],
  );

  const stoppedTurn = projection.providerTurns.find((turn) => turn.ordinal === 2);
  assert.equal(stoppedTurn?.status, "interrupted");
  assert.equal(stoppedTurn?.nativeTurnRef?.strength, "strong");

  const interrupts = droidRequests(transcript, "interrupt_session");
  assert.lengthOf(interrupts, 1, "Stop sends one interrupt_session");
  const rewinds = droidRequests(transcript, "execute_rewind");
  assert.lengthOf(rewinds, 1);
  assert.isAbove(
    rewinds[0]!.index,
    interrupts[0]!.index,
    "the rollback comes after the turn was stopped",
  );
  assertDroidRewind({ result, transcript, projection, discardedOrdinal: 2 });

  assertVisibleUserMessagesInclude(projection, [
    THREAD_ROLLBACK_FIRST_PROMPT,
    DROID_ASSISTANT_REPLIES_PROMPT,
  ]);
  assertVisibleUserMessagesExclude(projection, [
    TURN_INTERRUPT_MID_TOOL_PROMPT,
    THREAD_ROLLBACK_SECOND_PROMPT,
  ]);

  const finalAnswer = projection.turnItems.findLast((item) => item.type === "assistant_message");
  assert.include(finalAnswer?.text, "rollback fixture first turn complete");
  assert.notInclude(finalAnswer?.text, "second turn");
  assert.notInclude(finalAnswer?.text, "interrupt fixture");
}
