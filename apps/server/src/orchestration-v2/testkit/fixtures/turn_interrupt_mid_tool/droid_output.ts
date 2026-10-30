import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertTurnItemTypes,
  assertUserMessagesInclude,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  projectionFor,
  TURN_INTERRUPT_MID_TOOL_PROMPT,
} from "../shared.ts";
import {
  assertDroidTurnIdentity,
  droidProcessStartIndexes,
  droidRequests,
} from "../simple/droid_output.ts";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

export function assertTurnInterruptMidToolDroidOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const toolStartIndex = transcript.entries.findIndex(
    (entry) =>
      entry.type === "emit_inbound" &&
      field(field(field(entry.frame, "params"), "notification"), "type") === "tool_call",
  );
  assert.isAtLeast(toolStartIndex, 0, "Droid must record the shell tool starting");
  const interrupts = droidRequests(transcript, "interrupt_session");
  assert.lengthOf(interrupts, 1, "Stop sends one interrupt_session");
  assert.isAbove(interrupts[0]!.index, toolStartIndex, "Stop interrupts after the tool started");

  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["interrupted"] });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assertTurnItemTypes(projection, [
    "user_message",
    "command_execution",
    "run_interrupt_request",
    "run_interrupt_result",
  ]);
  assertUserMessagesInclude(projection, [TURN_INTERRUPT_MID_TOOL_PROMPT]);

  const commandItems = projection.turnItems.filter((item) => item.type === "command_execution");
  assert.lengthOf(commandItems, 1, "Droid sends tool_call twice, but the tool is one item");
  // The fixture interrupts as soon as the command item exists, which can be before Droid's second
  // tool_call carries the command line.
  const commandInput = commandItems[0]?.input ?? "";
  assert.isTrue(
    commandInput === "" || commandInput.includes("node -e"),
    "a command that has an input shows the command line the model asked for",
  );
  // Droid answers a stopped tool with an error `tool_result` ("Tool execution cancelled by
  // user") after the interrupt request; that is a stop, so the item settles `interrupted` like
  // every other provider's stopped command, never `failed`.
  assert.equal(
    commandItems[0]?.status,
    "interrupted",
    "the stopped command settles interrupted, not running or failed",
  );
  const interruptResult = projection.turnItems.find((item) => item.type === "run_interrupt_result");
  assert.equal(interruptResult?.status, "interrupted");

  for (const item of projection.turnItems) {
    if (item.type === "assistant_message" || item.type === "reasoning") {
      assert.isFalse(
        item.streaming,
        `${item.type} that streamed before the interrupt is finalized`,
      );
    }
  }
  assert.deepEqual(
    projection.providerTurns.map((turn) => turn.status),
    ["interrupted"],
  );
  assertDroidTurnIdentity(result, transcript);
  assert.lengthOf(
    droidProcessStartIndexes(transcript),
    1,
    "interrupt_session ends the turn inside the running process, so nothing restarts",
  );
  assert.deepEqual(
    projection.providerSessions.map((session) => [session.status, session.lastError]),
    [["ready", null]],
    "an interrupted Droid turn leaves the session usable and reports no error",
  );
}
