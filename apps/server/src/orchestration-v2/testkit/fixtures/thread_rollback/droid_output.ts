import { assert } from "@effect/vitest";
import type { OrchestrationV2ThreadProjection, ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertRunOrdinals,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  assertVisibleUserMessagesExclude,
  assertVisibleUserMessagesInclude,
  projectionFor,
  THREAD_ROLLBACK_FIRST_PROMPT,
  THREAD_ROLLBACK_SECOND_PROMPT,
} from "../shared.ts";
import {
  assertDroidFrameCarriesCatalogEffort,
  assertDroidSessionThread,
  assertDroidTurnIdentity,
  droidInitializedSessionId,
  droidRequests,
  droidResponseTo,
} from "../simple/droid_output.ts";
import { DROID_ASSISTANT_REPLIES_PROMPT } from "./droid_input.ts";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

/**
 * What a Droid rollback must leave behind, shared by the plain and the after-stop fixtures: one
 * `execute_rewind` at the first discarded turn's user message, a `load_session` of the id it
 * returned, the settings Droid does not carry over, one provider thread on the new id, and a final
 * answer that lists only the replies the rewound session still holds.
 */
export function assertDroidRewind(input: {
  readonly result: OrchestratorV2ScenarioResult;
  readonly transcript: ProviderReplayTranscript;
  readonly projection: OrchestrationV2ThreadProjection;
  /** Ordinal of the first provider turn the rollback discards. */
  readonly discardedOrdinal: number;
}) {
  const { result, transcript, projection } = input;
  assertDroidTurnIdentity(result, transcript);
  const sends = droidRequests(transcript, "add_user_message");
  const discarded = sends[input.discardedOrdinal - 1];
  assert.isDefined(discarded, "the discarded turn reached Droid");

  const rewinds = droidRequests(transcript, "execute_rewind");
  assert.lengthOf(rewinds, 1, "a rollback rewinds the Droid session exactly once");
  const [rewind] = rewinds;
  // The testkit holds the adapter to the id it chose for the discarded turn, which
  // `assertDroidTurnIdentity` ties to that turn's strong `nativeTurnRef`; the recording ties
  // the rewind to the same message.
  assert.equal(field(rewind!.params, "messageId"), field(discarded!.params, "messageId"));
  assert.deepEqual(field(rewind!.params, "filesToRestore"), []);
  assert.deepEqual(field(rewind!.params, "filesToDelete"), []);
  const discardedTurn = projection.providerTurns.find(
    (turn) => turn.ordinal === input.discardedOrdinal,
  );
  assert.equal(discardedTurn?.nativeTurnRef?.strength, "strong");
  assert.isTrue(
    rewind!.index > discarded!.index,
    "the rewind targets a message that was already sent",
  );

  const rewoundSessionId = field(droidResponseTo(transcript, rewind!), "newSessionId");
  assert.isString(rewoundSessionId);
  const loads = droidRequests(transcript, "load_session").filter(
    (request) => request.index > rewind!.index,
  );
  assert.lengthOf(loads, 1, "the process moves to the rewound session once");
  assert.equal(field(loads[0]!.params, "sessionId"), rewoundSessionId);
  const sourceSessionId = droidInitializedSessionId(transcript);
  assert.equal(field(rewind!.params, "sessionId"), sourceSessionId);
  assert.notEqual(rewoundSessionId, sourceSessionId, "a rewind moves the thread to a new session");

  // `load_session` takes no settings, so the adapter re-applies them before the next turn.
  const finalSend = sends.at(-1)!;
  const settingsFrames = droidRequests(transcript, "update_session_settings").filter(
    (request) => request.index > loads[0]!.index && request.index < finalSend.index,
  );
  assert.lengthOf(settingsFrames, 1, "the rewound session gets its settings before the next turn");
  assertDroidFrameCarriesCatalogEffort(transcript, loads[0]!, settingsFrames[0]!);

  assertDroidSessionThread(projection, String(rewoundSessionId));
  assert.equal(field(finalSend.params, "text"), DROID_ASSISTANT_REPLIES_PROMPT);
}

export function assertDroidThreadRollbackOutput(
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
    DROID_ASSISTANT_REPLIES_PROMPT,
  ]);
  assertVisibleUserMessagesInclude(projection, [
    THREAD_ROLLBACK_FIRST_PROMPT,
    DROID_ASSISTANT_REPLIES_PROMPT,
  ]);
  assertVisibleUserMessagesExclude(projection, [THREAD_ROLLBACK_SECOND_PROMPT]);

  assertDroidRewind({ result, transcript, projection, discardedOrdinal: 2 });

  const finalAnswer = projection.turnItems.findLast((item) => item.type === "assistant_message");
  assert.include(finalAnswer?.text, "rollback fixture first turn complete");
  assert.notInclude(finalAnswer?.text, "second turn");
}
