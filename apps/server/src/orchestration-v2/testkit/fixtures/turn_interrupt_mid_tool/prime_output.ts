import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  projectionFor,
  TURN_INTERRUPT_MID_TOOL_PROMPT,
} from "../shared.ts";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

/**
 * Prime's only built-in tool is `ipython`, so the shell command runs inside a
 * Python cell. Stop aborts while that cell runs: the turn ends once, as
 * interrupted, and the cell's command item is closed rather than left running.
 */
export function assertTurnInterruptMidToolPrimeOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const toolStartIndex = transcript.entries.findIndex(
    (entry) =>
      entry.type === "emit_inbound" &&
      field(entry.frame, "type") === "tool_execution_start" &&
      field(entry.frame, "toolName") === "ipython",
  );
  const abortIndexes = transcript.entries.flatMap((entry, index) =>
    entry.type === "expect_outbound" && field(entry.frame, "type") === "abort" ? [index] : [],
  );
  assert.isAtLeast(toolStartIndex, 0, "Prime must record the ipython cell starting");
  assert.lengthOf(abortIndexes, 1, "Stop sends one abort");
  assert.isAbove(abortIndexes[0] ?? -1, toolStartIndex, "Stop must abort after the cell started");

  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["interrupted"] });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assertUserMessagesInclude(projection, [TURN_INTERRUPT_MID_TOOL_PROMPT]);

  const commandItems = projection.turnItems.filter((item) => item.type === "command_execution");
  assert.lengthOf(commandItems, 1, "the ipython cell is one command item");
  const commandItem = commandItems[0];
  assert.isDefined(commandItem);
  assert.include(commandItem.input, "node -e", "the cell's code is the command");
  assert.equal(commandItem.status, "interrupted", "the interrupted cell's item is closed");
  assert.isNotNull(commandItem.completedAt);
  assert.deepEqual(
    projection.turnItems.filter((item) => item.status === "running").map((item) => item.type),
    [],
    "no item is left running after the terminal",
  );
  const interruptResult = projection.turnItems.find((item) => item.type === "run_interrupt_result");
  assert.equal(interruptResult?.status, "interrupted");
  assert.deepEqual(
    projection.providerTurns.map((turn) => turn.status),
    ["interrupted"],
    "exactly one interrupted terminal",
  );
  assert.deepEqual(
    projection.providerSessions.map((session) => [session.status, session.lastError]),
    [["stopped", null]],
    "Stop-with-restart retires the Prime process without reporting an error",
  );
}
