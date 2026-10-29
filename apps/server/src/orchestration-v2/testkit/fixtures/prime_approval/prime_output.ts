import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import { PRIME_REPLAY_ANY } from "../../../Adapters/PrimeAdapterV2.testkit.ts";
import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  PRIME_APPROVAL_PROMPT,
  projectionFor,
} from "../shared.ts";
import { assertPrimeTurnsSettleOnce } from "../simple/prime_output.ts";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

function frameOf(entry: ProviderReplayTranscript["entries"][number] | undefined): unknown {
  return entry === undefined || entry.type === "runtime_exit" ? undefined : entry.frame;
}

const NORMALIZED_UUID = /^00000000-0000-4000-8000-\d{12}$/u;

/**
 * T3's bridge extension gates the `ipython` cell with `ctx.ui.confirm` under
 * approval-required. The confirm becomes one pending command request; the
 * accept goes back as an `extension_ui_response` carrying Prime's own dialog
 * id, and only then does the cell run. The turn completes once and no request
 * is left pending.
 */
export function assertPrimeApprovalOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const entries = transcript.entries;
  const processStart = entries.find(
    (entry) => entry.type === "expect_outbound" && field(entry.frame, "type") === "process_start",
  );
  const launchArgs = field(frameOf(processStart), "args");
  const args: ReadonlyArray<unknown> = Array.isArray(launchArgs) ? launchArgs : [];
  assert.include(args, "--no-extensions", "the recording loads no discovered extensions");
  assert.equal(args[args.indexOf("-e") + 1], PRIME_REPLAY_ANY, "the bridge path is normalised");

  const confirmIndexes = entries.flatMap((entry, index) =>
    entry.type === "emit_inbound" &&
    field(entry.frame, "type") === "extension_ui_request" &&
    field(entry.frame, "method") === "confirm"
      ? [index]
      : [],
  );
  assert.lengthOf(confirmIndexes, 1, "the bridge confirms the one ipython cell");
  const confirmIndex = confirmIndexes[0]!;
  const dialogId = field(frameOf(entries[confirmIndex]), "id");
  assert.match(String(dialogId), NORMALIZED_UUID, "Prime's dialog id is normalised");

  const responses = entries.flatMap((entry, index) =>
    entry.type === "expect_outbound" && field(entry.frame, "type") === "extension_ui_response"
      ? [{ index, frame: entry.frame }]
      : [],
  );
  assert.lengthOf(responses, 1);
  const response = responses[0]!;
  assert.deepEqual(response.frame, {
    type: "extension_ui_response",
    id: dialogId,
    confirmed: true,
  });
  assert.isAbove(response.index, confirmIndex);
  const cellEndIndex = entries.findIndex(
    (entry) =>
      entry.type === "emit_inbound" &&
      field(entry.frame, "type") === "tool_execution_end" &&
      field(entry.frame, "toolName") === "ipython",
  );
  assert.isAbove(cellEndIndex, response.index, "the cell finishes only after the approval");

  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["completed"] });
  assertPrimeTurnsSettleOnce(result, transcript, 1);
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assertUserMessagesInclude(projection, [PRIME_APPROVAL_PROMPT]);

  assert.lengthOf(projection.runtimeRequests, 1, "the cell asks for approval exactly once");
  const request = projection.runtimeRequests[0]!;
  assert.equal(request.kind, "command");
  assert.equal(request.status, "resolved", "no approval is left pending");
  assert.equal(request.decision, "accept");
  assert.equal(request.nativeRequestRef?.nativeId, dialogId, "keyed by Prime's dialog id");
  const commandItems = projection.turnItems.filter((item) => item.type === "command_execution");
  assert.lengthOf(commandItems, 1, "the ipython cell is one command item");
  assert.include(commandItems[0]?.input, "prime approval fixture ran");
  assert.equal(commandItems[0]?.status, "completed");
  assert.deepEqual(
    projection.turnItems.filter((item) => item.status === "running").map((item) => item.type),
    [],
    "no item is left running after the terminal",
  );
}
