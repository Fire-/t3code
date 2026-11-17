import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertRuntimeRequestCounts,
  assertSemanticProjectionIntegrity,
  assertTurnItemTypes,
  assertUserMessagesInclude,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  DROID_MODEL_SELECTION,
  projectionFor,
  PROPOSED_PLAN_PROMPT,
} from "../shared.ts";
import {
  adapterReplyTo,
  DROID_WRITE_TOOLS,
  droidExitSpecModeRequests,
  droidNotifications,
  droidRequests,
  droidSettingsFrameBefore,
  droidToolCalls,
} from "../simple/droid_output.ts";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

/**
 * Droid ends a spec-mode plan with an `exit_spec_mode` permission request. t3 keeps the plan as a
 * proposal and answers `cancel`, which leaves Droid in spec mode: the turn completes, nothing is
 * implemented, and implementing is a later, ordinary turn.
 */
export function assertDroidProposedPlanOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const sends = droidRequests(transcript, "add_user_message");
  assert.lengthOf(sends, 1);
  const settings = droidSettingsFrameBefore(transcript, sends[0]!.index);
  assert.equal(field(settings, "interactionMode"), "spec", "plan mode runs Droid in spec mode");
  assert.equal(field(settings, "specModeModelId"), DROID_MODEL_SELECTION.model);

  const exits = droidExitSpecModeRequests(transcript);
  assert.isAtLeast(exits.length, 1, "Droid asks to leave spec mode once its plan is written");
  for (const exit of exits) {
    assert.deepEqual(
      adapterReplyTo(transcript, exit),
      { selectedOption: "cancel" },
      "the plan is held for the user; no proceed_* option is ever sent",
    );
  }
  assert.isFalse(
    droidRequests(transcript, "update_session_settings").some(
      (request) => field(request.params, "interactionMode") === "auto",
    ),
    "the adapter never switches the plan session to auto",
  );

  // Droid echoes settings after the cancel; none of them may report that spec mode ended.
  for (const { notification } of droidNotifications(transcript, "settings_updated")) {
    const mode = field(field(notification, "settings"), "interactionMode");
    assert.isTrue(
      mode === undefined || mode === "spec",
      `a settings echo reports ${String(mode)}, so spec mode ended`,
    );
  }
  const afterPlanRequest = droidToolCalls(transcript).filter(
    (call) => call.index > exits[0]!.index,
  );
  assert.deepEqual(
    afterPlanRequest.filter((call) => DROID_WRITE_TOOLS.includes(call.name)),
    [],
    "no implementation tool runs after the plan",
  );

  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["completed"] });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assertTurnItemTypes(projection, ["user_message", "proposed_plan"]);
  assert.isFalse(
    projection.turnItems.some((item) => item.type === "approval_request"),
    "exit_spec_mode never asks the user for approval",
  );
  assertRuntimeRequestCounts(projection, { total: 0 });
  assertUserMessagesInclude(projection, [PROPOSED_PLAN_PROMPT]);

  const proposedPlans = projection.plans.filter((plan) => plan.kind === "proposed_plan");
  assert.isAtLeast(proposedPlans.length, 1);
  assert.equal(proposedPlans.at(-1)?.status, "active");
  assert.isAbove((proposedPlans.at(-1)?.markdown ?? "").trim().length, 0);

  assert.isFalse(
    projection.turnItems.some((item) => item.status === "failed" || item.type === "error"),
    "a held plan leaves no failed item, including the cancelled ExitSpecMode tool",
  );
  const items = projection.turnItems.toSorted((left, right) => left.ordinal - right.ordinal);
  const lastPlanIndex = items.findLastIndex((item) => item.type === "proposed_plan");
  assert.isAtLeast(lastPlanIndex, 0);
  assert.deepEqual(
    items
      .slice(lastPlanIndex + 1)
      .filter((item) => item.type === "file_change" || item.type === "command_execution"),
    [],
    "no file change or command comes after the plan",
  );
}
