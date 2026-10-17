import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertRunOrdinals,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  assertVisibleUserMessagesExclude,
  assertVisibleUserMessagesInclude,
  projectionFor,
  THREAD_ROLLBACK_AFTER_PROMPT,
  THREAD_ROLLBACK_FIRST_PROMPT,
  THREAD_ROLLBACK_SECOND_PROMPT,
} from "../shared.ts";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

/**
 * Judges the rollback from Prime's own answers rather than the model's words:
 * one `get_fork_messages`, one `fork` at the user entry Prime lists for the
 * first discarded prompt (Prime echoes that prompt back as the fork's text),
 * and the provider thread moved to the session file `fork` switched to.
 */
function assertPrimeRollbackForkedBefore(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
  discardedPrompt: string,
) {
  const outbound = transcript.entries.flatMap((entry) =>
    entry.type === "expect_outbound" ? [entry.frame] : [],
  );
  const responses = transcript.entries.flatMap((entry) =>
    entry.type === "emit_inbound" && field(entry.frame, "type") === "response" ? [entry.frame] : [],
  );
  const outboundTypes = outbound.map((frame) => field(frame, "type"));
  assert.notInclude(outboundTypes, "get_entries");
  assert.lengthOf(
    outboundTypes.filter((type) => type === "get_fork_messages"),
    1,
    "rollback lists Prime's user entries once",
  );
  const forks = outbound.filter((frame) => field(frame, "type") === "fork");
  assert.lengthOf(forks, 1, "rollback forks the Prime session exactly once");

  const listed = field(
    field(
      responses.find((frame) => field(frame, "command") === "get_fork_messages"),
      "data",
    ),
    "messages",
  );
  assert.isTrue(Array.isArray(listed));
  const entries = Array.isArray(listed) ? listed : [];
  const discardedEntry = entries.find((entry) => field(entry, "text") === discardedPrompt);
  assert.isDefined(discardedEntry, "Prime lists the discarded prompt as a user entry");
  assert.equal(field(forks[0], "entryId"), field(discardedEntry, "entryId"));
  const forkIndex = transcript.entries.findIndex(
    (entry) =>
      entry.type === "emit_inbound" &&
      field(entry.frame, "type") === "response" &&
      field(entry.frame, "command") === "fork",
  );
  const forkResponse = transcript.entries[forkIndex];
  assert.deepEqual(
    field(forkResponse?.type === "emit_inbound" ? forkResponse.frame : null, "data"),
    {
      text: discardedPrompt,
      cancelled: false,
    },
  );

  const sessionFiles = transcript.entries.flatMap((entry, index) =>
    entry.type === "emit_inbound" &&
    field(entry.frame, "type") === "response" &&
    field(entry.frame, "command") === "get_state"
      ? [{ index, file: field(field(entry.frame, "data"), "sessionFile") }]
      : [],
  );
  const before = sessionFiles.findLast((state) => state.index < forkIndex)?.file;
  const after = sessionFiles.find((state) => state.index > forkIndex)?.file;
  assert.isString(after);
  assert.notEqual(after, before, "fork moves Prime to a new session file");
  const projection = projectionFor(result, transcript.scenario);
  assert.deepEqual(
    projection.providerThreads.map((thread) => thread.nativeThreadRef?.nativeId),
    [after],
  );
}

export function assertPrimeThreadRollbackOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({
    result,
    transcript,
    runCount: 3,
    runStatuses: ["completed", "rolled_back", "completed"],
  });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertRunOrdinals(projection, [1, 2, 3]);
  assertUserMessagesInclude(projection, [
    THREAD_ROLLBACK_FIRST_PROMPT,
    THREAD_ROLLBACK_SECOND_PROMPT,
    THREAD_ROLLBACK_AFTER_PROMPT,
  ]);
  assertVisibleUserMessagesInclude(projection, [
    THREAD_ROLLBACK_FIRST_PROMPT,
    THREAD_ROLLBACK_AFTER_PROMPT,
  ]);
  assertVisibleUserMessagesExclude(projection, [THREAD_ROLLBACK_SECOND_PROMPT]);
  assertPrimeRollbackForkedBefore(result, transcript, THREAD_ROLLBACK_SECOND_PROMPT);

  const finalAnswer = projection.turnItems.findLast((item) => item.type === "assistant_message");
  assert.include(finalAnswer?.text, "rollback fixture first turn complete");
  assert.notInclude(finalAnswer?.text, "second turn");
}
