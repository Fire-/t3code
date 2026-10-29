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
import { PRIME_THREAD_ROLLBACK_REPLIES_PROMPT } from "../prime_thread_rollback/input.ts";
import { assertPrimeRollbackForkedBefore } from "../prime_thread_rollback/prime_output.ts";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

/**
 * Stop retires the Prime process, so the rollback runs in the process the next
 * message started. Prime reads the stopped turn's entry ids at rollback time
 * from `get_fork_messages`; nothing has to be captured before the stop.
 */
export function assertPrimeThreadRollbackAfterStopOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({ result, transcript, runCount: 4 });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertRunOrdinals(projection, [1, 2, 3, 4]);
  const runStatus = (ordinal: number) =>
    projection.runs.find((run) => run.ordinal === ordinal)?.status;
  assert.equal(runStatus(1), "completed");
  assert.equal(runStatus(2), "rolled_back");
  assert.equal(runStatus(3), "rolled_back");
  assert.equal(runStatus(4), "completed");
  const stoppedTurn = projection.providerTurns.find((providerTurn) => providerTurn.ordinal === 2);
  assert.equal(stoppedTurn?.status, "interrupted");

  const outboundIndexes = (type: string) =>
    transcript.entries.flatMap((entry, index) =>
      entry.type === "expect_outbound" && field(entry.frame, "type") === type ? [index] : [],
    );
  const [abortIndex] = outboundIndexes("abort");
  const [forkIndex] = outboundIndexes("fork");
  assert.isDefined(abortIndex);
  assert.isAbove(
    outboundIndexes("process_start").filter((index) => index > abortIndex!).length,
    0,
    "a new Prime process serves the turns after Stop",
  );
  assert.isAbove(forkIndex ?? -1, abortIndex!);
  assertPrimeRollbackForkedBefore(result, transcript, TURN_INTERRUPT_MID_TOOL_PROMPT);

  assertVisibleUserMessagesInclude(projection, [
    THREAD_ROLLBACK_FIRST_PROMPT,
    PRIME_THREAD_ROLLBACK_REPLIES_PROMPT,
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
