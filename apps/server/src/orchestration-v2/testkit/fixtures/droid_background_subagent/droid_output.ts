import { assert } from "@effect/vitest";
import type { OrchestrationV2ThreadProjection, ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  projectionFor,
} from "../shared.ts";
import {
  assertDroidSessionThread,
  droidInitializedSessionId,
  droidNotifications,
  droidRequests,
} from "../simple/droid_output.ts";
import {
  DROID_BACKGROUND_SUBAGENT_CHILD_REPLY,
  DROID_BACKGROUND_SUBAGENT_PROMPT,
  DROID_BACKGROUND_SUBAGENT_ROOT_REPLY,
} from "./input.ts";

const TASK_COMPLETION_PREFIX = "task-completion:";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

function runAssistantTexts(
  projection: OrchestrationV2ThreadProjection,
  runId: string | undefined,
): ReadonlyArray<string> {
  return projection.turnItems.flatMap((item) =>
    item.runId === runId && item.type === "assistant_message" ? [item.text.trim()] : [],
  );
}

/**
 * A background Task outlives the turn that launched it. Droid reports it later, while idle, as a
 * `task-completion:<id>` message and answers it in a turn it starts itself: the adapter offers
 * that turn as a continuation and the orchestrator runs it as a second run, with no message sent
 * to Droid.
 */
export function assertDroidBackgroundSubagentOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({
    result,
    transcript,
    runCount: 2,
    runStatuses: ["completed", "completed"],
  });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertUserMessagesInclude(projection, [DROID_BACKGROUND_SUBAGENT_PROMPT]);
  const [rootRun, wakeRun] = projection.runs;
  assertDroidSessionThread(projection, droidInitializedSessionId(transcript));

  // What Droid and the adapter exchanged: one user message, one child session, and one report
  // that Droid answered with a turn of its own.
  assert.lengthOf(
    droidRequests(transcript, "add_user_message"),
    1,
    "Droid starts the second turn itself, so the adapter sends no message for it",
  );
  const children = droidNotifications(transcript, "child_session_available");
  assert.lengthOf(children, 1, "one background subagent");
  const childSessionId = String(field(children[0]!.notification, "childSessionId"));
  const reports = droidNotifications(transcript, "create_message").filter(({ notification }) =>
    String(field(field(notification, "message"), "id")).startsWith(TASK_COMPLETION_PREFIX),
  );
  assert.lengthOf(reports, 1, "Droid reports the finished task once");
  const wakeTurnId = String(field(field(reports[0]!.notification, "message"), "id"));
  const completions = droidNotifications(transcript, "agent_turn_completed");
  assert.lengthOf(completions, 2, "the user's turn, then the turn Droid started for the report");
  assert.equal(field(completions[1]!.notification, "turnId"), wakeTurnId);
  assert.isAbove(
    reports[0]!.index,
    completions[0]!.index,
    "the report arrives after the user's turn ended, while Droid is idle",
  );

  assert.lengthOf(projection.subagents, 1);
  const subagent = projection.subagents[0]!;
  assert.equal(subagent.origin, "provider_native");
  assert.equal(subagent.driver, "droid");
  assert.equal(subagent.status, "completed");
  assert.equal(subagent.runId, rootRun?.id, "the subagent was spawned in run 1");
  assert.equal(subagent.nativeTaskRef?.nativeId, childSessionId);
  assert.isNotNull(subagent.completedAt);
  assert.include(subagent.result ?? "", DROID_BACKGROUND_SUBAGENT_CHILD_REPLY);
  const subagentItems = projection.turnItems.filter((item) => item.type === "subagent");
  assert.lengthOf(subagentItems, 1);
  assert.equal(subagentItems[0]?.status, "completed");
  assert.equal(subagentItems[0]?.runId, rootRun?.id);
  assert.isFalse(
    projection.turnItems.some((item) => item.type === "dynamic_tool"),
    "the Task call is a subagent, not a tool item",
  );

  assert.include(
    runAssistantTexts(projection, rootRun?.id).join("\n"),
    DROID_BACKGROUND_SUBAGENT_ROOT_REPLY,
  );
  assert.isNotEmpty(
    runAssistantTexts(projection, wakeRun?.id).join("").trim(),
    "Droid's answer to the report lands in the continuation run",
  );
  const wakeMessage = projection.messages.find((message) => message.id === wakeRun?.userMessageId);
  assert.equal(`${wakeMessage?.createdBy}:${wakeMessage?.creationSource}`, "agent:provider");
  const wakeTurn = projection.providerTurns.find(
    (turn) => turn.runAttemptId === wakeRun?.activeAttemptId,
  );
  assert.equal(wakeTurn?.nativeTurnRef?.nativeId, wakeTurnId);

  // The roster: the task is pending when run 1 completes, and gone once the report settled it.
  const rootCompletedAt = result.domainEvents.findIndex(
    (event) =>
      event.type === "run.updated" &&
      event.payload.id === rootRun?.id &&
      event.payload.status === "completed",
  );
  assert.isAtLeast(rootCompletedAt, 0);
  const rosterBefore = result.domainEvents
    .slice(0, rootCompletedAt)
    .findLast((event) => event.type === "provider-thread.updated");
  assert.deepEqual(
    rosterBefore?.type === "provider-thread.updated"
      ? (rosterBefore.payload.pendingBackgroundTasks ?? []).map((task) => [task.taskId, task.kind])
      : [],
    [[childSessionId, "subagent"]],
    "the provider thread lists the running subagent when run 1 completes",
  );
  const subagentCompletedAt = result.domainEvents.findIndex(
    (event) =>
      event.type === "turn-item.updated" &&
      event.payload.type === "subagent" &&
      event.payload.status === "completed",
  );
  assert.isAbove(subagentCompletedAt, rootCompletedAt, "run 1 completed before the subagent");
  const clearedAt = result.domainEvents.findIndex(
    (event, index) =>
      index > rootCompletedAt &&
      event.type === "provider-thread.updated" &&
      (event.payload.pendingBackgroundTasks?.length ?? 0) === 0,
  );
  assert.isAbove(clearedAt, rootCompletedAt, "the report clears the roster");
  assert.deepEqual(projection.providerThreads[0]?.pendingBackgroundTasks ?? [], []);
}
