import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  projectionFor,
  PROVIDER_THREAD_RESUME_FIRST_PROMPT,
  PROVIDER_THREAD_RESUME_SECOND_PROMPT,
} from "../shared.ts";
import { assertPrimeTurnsSettleOnce } from "../simple/prime_output.ts";

const FIRST_FINAL = "provider thread resume fixture first turn complete";
const SECOND_FINAL = "provider thread resume fixture second turn complete";
const RESUMED_SESSION = "/sessions/session-1.jsonl";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

/**
 * The idle release stops the first Prime process; the second resumes the same
 * session file through `switch_session`. Stopping the first process completes
 * its owned worker, so the resume must succeed on the first try instead of
 * hitting `SessionAlreadyActiveError` during Prime's disconnect grace.
 */
export function assertPrimeProviderThreadResumeOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const processStarts = transcript.entries.filter(
    (entry) => entry.type === "expect_outbound" && field(entry.frame, "type") === "process_start",
  );
  assert.lengthOf(processStarts, 2, "the idle release must retire the first Prime process");
  const switches = transcript.entries.flatMap((entry) =>
    entry.type === "expect_outbound" && field(entry.frame, "type") === "switch_session"
      ? [entry]
      : [],
  );
  assert.deepEqual(
    switches.map((entry) => [entry.label, field(entry.frame, "sessionPath")]),
    [["switch_session@p2", RESUMED_SESSION]],
    "the respawned process must resume the first session file once",
  );
  const switchAnswers = transcript.entries.flatMap((entry) =>
    entry.type === "emit_inbound" &&
    field(entry.frame, "type") === "response" &&
    field(entry.frame, "command") === "switch_session"
      ? [entry.frame]
      : [],
  );
  assert.deepEqual(
    switchAnswers.map((frame) => field(frame, "success")),
    [true],
    "the resume must not hit SessionAlreadyActiveError",
  );

  assertBaseProjection({
    result,
    transcript,
    runCount: 2,
    runStatuses: ["completed", "completed"],
  });
  assertPrimeTurnsSettleOnce(result, transcript, 2);
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertUserMessagesInclude(projection, [
    PROVIDER_THREAD_RESUME_FIRST_PROMPT,
    PROVIDER_THREAD_RESUME_SECOND_PROMPT,
  ]);
  assert.lengthOf(projection.providerThreads, 1, "resume must keep one provider thread");
  assert.equal(projection.providerThreads[0]?.nativeThreadRef?.nativeId, RESUMED_SESSION);
  const answers = projection.turnItems.flatMap((item) =>
    item.type === "assistant_message" ? [item.text] : [],
  );
  assert.lengthOf(answers, 2);
  assert.include(answers[1], FIRST_FINAL, "the resumed session must remember the first answer");
  assert.include(answers[1], SECOND_FINAL);
  // The respawned process could not revive the first process's Python kernel.
  assert.deepEqual(
    projection.turnItems.flatMap((item) => (item.type === "system_notice" ? [item.message] : [])),
    ["Python kernel state could not be revived — the kernel is starting fresh."],
    "the kernel restore row is one line",
  );
  assert.isFalse(
    projection.turnItems.some((item) => JSON.stringify(item).includes("[python-state-restored]")),
    "the raw kernel restore text never reaches an item",
  );
}
