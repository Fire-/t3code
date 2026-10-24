import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

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
  droidResponseTo,
} from "../simple/droid_output.ts";
import {
  DROID_COMPACTION_FILLER_PROMPTS,
  DROID_COMPACTION_MARKER,
  DROID_COMPACTION_RECALL_PROMPT,
  DROID_COMPACTION_STORE_PROMPT,
} from "./input.ts";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

export function assertDroidCompactionOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const compactions = droidRequests(transcript, "compact_session");
  assert.lengthOf(compactions, 1, "`/compact` is one compact_session request");
  const sends = droidRequests(transcript, "add_user_message");
  assert.deepEqual(
    sends.map((send) => field(send.params, "text")),
    [
      DROID_COMPACTION_STORE_PROMPT,
      ...DROID_COMPACTION_FILLER_PROMPTS,
      DROID_COMPACTION_RECALL_PROMPT,
    ],
    "`/compact` is not sent to Droid as a user message",
  );
  assert.isAbove(compactions[0]!.index, sends[2]!.index);
  assert.isBelow(compactions[0]!.index, sends[3]!.index);

  assertBaseProjection({
    result,
    transcript,
    runCount: 5,
    runStatuses: ["completed", "completed", "completed", "completed", "completed"],
  });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertUserMessagesInclude(projection, [
    DROID_COMPACTION_STORE_PROMPT,
    DROID_COMPACTION_RECALL_PROMPT,
  ]);

  const compactionItems = projection.turnItems.filter((item) => item.type === "compaction");
  assert.lengthOf(compactionItems, 1);
  assert.equal(compactionItems[0]?.status, "completed");
  assert.equal(compactionItems[0]?.title, "Context compacted");
  const compactionRun = projection.runs[3];
  assert.equal(compactionItems[0]?.runId, compactionRun?.id);
  assert.isFalse(
    projection.turnItems.some(
      (item) => item.runId === compactionRun?.id && item.type === "assistant_message",
    ),
    "compaction runs no model turn",
  );

  // Droid keeps the session id through a compaction, so the thread never moves.
  const sessionId = droidInitializedSessionId(transcript);
  const answer = droidResponseTo(transcript, compactions[0]!);
  assert.equal(field(answer, "newSessionId"), sessionId);
  assert.lengthOf(droidRequests(transcript, "load_session"), 0);
  assertDroidSessionThread(projection, sessionId);

  // Droid runs no native turn for a compaction: the answer to `compact_session` is the only
  // completion signal the adapter gets, so it must settle the turn on the answer.
  const answerIndex = transcript.entries.findIndex(
    (entry, index) =>
      index > compactions[0]!.index &&
      entry.type === "emit_inbound" &&
      field(entry.frame, "type") === "response" &&
      field(entry.frame, "id") === compactions[0]!.id,
  );
  assert.isAbove(answerIndex, compactions[0]!.index);
  const whileCompacting = droidNotifications(transcript)
    .filter(({ index }) => index > compactions[0]!.index && index < answerIndex)
    .map(({ notification }) => field(notification, "type"));
  assert.isTrue(
    whileCompacting.every(
      (type) => type === "settings_updated" || type === "session_token_usage_changed",
    ),
    `Droid sends no message, working state or turn completion while it compacts: ${whileCompacting.join(", ")}`,
  );

  // `session_compacted` and the usage of the summary call come after the answer, when the
  // adapter has no active turn, so they must be dropped without opening an item or a turn.
  const [compacted, ...otherCompacted] = droidNotifications(transcript, "session_compacted");
  assert.isDefined(compacted);
  assert.lengthOf(otherCompacted, 0);
  assert.isAbove(compacted!.index, answerIndex);
  assert.equal(field(compacted!.notification, "removedCount"), field(answer, "removedCount"));
  assert.isBelow(compacted!.index, sends[3]!.index);

  // The session id and its settings survive, so the next turn needs no settings frame.
  assert.lengthOf(
    droidRequests(transcript, "update_session_settings").filter(
      (request) => request.index > answerIndex,
    ),
    0,
  );
  const compactionTurn = projection.providerTurns.find(
    (turn) => turn.runAttemptId === compactionRun?.activeAttemptId,
  );
  assert.equal(compactionTurn?.status, "completed");
  assert.isNull(compactionTurn?.nativeTurnRef, "a compaction has no message id to rewind to");

  const recall = projection.turnItems.findLast((item) => item.type === "assistant_message");
  assert.include(recall?.text, DROID_COMPACTION_MARKER);
}
