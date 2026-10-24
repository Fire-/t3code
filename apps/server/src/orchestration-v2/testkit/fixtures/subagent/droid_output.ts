import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertAssistantTextIncludes,
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  projectionFor,
} from "../shared.ts";
import { droidNotifications, droidRequests } from "../simple/droid_output.ts";
import {
  DROID_SUBAGENT_ALPHA_CONTENT,
  DROID_SUBAGENT_BETA_CONTENT,
  DROID_SUBAGENT_PROMPT,
} from "./droid_input.ts";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

/**
 * Two awaited Task calls become two provider-native subagents on the parent thread. Droid sends
 * no child thread content (MISSION decision 15), so each subagent is known by its child session
 * id and the report its Task call returned.
 */
export function assertDroidSubagentOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["completed"] });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertUserMessagesInclude(projection, [DROID_SUBAGENT_PROMPT]);
  assert.lengthOf(droidRequests(transcript, "add_user_message"), 1);

  const childSessionIds = droidNotifications(transcript, "child_session_available").map(
    ({ notification }) => String(field(notification, "childSessionId")),
  );
  assert.lengthOf(childSessionIds, 2, "Droid starts one child session per Task call");
  assert.lengthOf(new Set(childSessionIds), 2);

  assert.lengthOf(projection.subagents, 2);
  for (const subagent of projection.subagents) {
    assert.equal(subagent.origin, "provider_native");
    assert.equal(subagent.driver, "droid");
    assert.equal(subagent.status, "completed");
    assert.isNotNull(subagent.nativeTaskRef);
    assert.include(childSessionIds, subagent.nativeTaskRef?.nativeId ?? "");
    assert.isNotNull(subagent.completedAt);
    assert.equal(subagent.runId, projection.runs[0]?.id);
  }
  assert.lengthOf(
    new Set(projection.subagents.map((subagent) => subagent.nativeTaskRef?.nativeId)),
    2,
  );
  assert.isTrue(
    projection.subagents.some((subagent) =>
      subagent.result?.includes(DROID_SUBAGENT_ALPHA_CONTENT),
    ),
    "one subagent reports the first file",
  );
  assert.isTrue(
    projection.subagents.some((subagent) => subagent.result?.includes(DROID_SUBAGENT_BETA_CONTENT)),
    "one subagent reports the second file",
  );

  const subagentItems = projection.turnItems.filter((item) => item.type === "subagent");
  assert.lengthOf(subagentItems, 2, "one subagent item per Task call");
  assert.isTrue(subagentItems.every((item) => item.status === "completed"));
  assert.lengthOf(
    projection.turnItems.filter((item) => item.type === "dynamic_tool"),
    0,
    "the Task calls are subagents, not tool items",
  );
  assert.deepEqual(projection.providerThreads[0]?.pendingBackgroundTasks ?? [], []);

  assertAssistantTextIncludes(projection, DROID_SUBAGENT_ALPHA_CONTENT);
  assertAssistantTextIncludes(projection, DROID_SUBAGENT_BETA_CONTENT);
}
