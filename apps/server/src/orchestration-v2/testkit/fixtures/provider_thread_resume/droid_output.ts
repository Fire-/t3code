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
import {
  assertDroidSessionThread,
  assertDroidTurnIdentity,
  droidInitializedSessionId,
  droidProcessStartIndexes,
  droidRequests,
} from "../simple/droid_output.ts";

const FIRST_FINAL = "provider thread resume fixture first turn complete";
const SECOND_FINAL = "provider thread resume fixture second turn complete";
const HIDDEN_CONTEXT_PLACEHOLDER = "<hidden-context>";
const HIDDEN_MESSAGE_ID = /^(?:context-|__internal__system-reminder__)/u;

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

/** Every message Droid injects for the model alone, wherever the recording carries it. */
function hiddenContextMessages(value: unknown, found: Array<unknown> = []): Array<unknown> {
  if (Array.isArray(value)) {
    for (const item of value) hiddenContextMessages(item, found);
  } else if (typeof value === "object" && value !== null) {
    const id = field(value, "id");
    if (typeof id === "string" && HIDDEN_MESSAGE_ID.test(id)) found.push(value);
    for (const item of Object.values(value)) hiddenContextMessages(item, found);
  }
  return found;
}

export function assertDroidProviderThreadResumeOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const processStarts = droidProcessStartIndexes(transcript);
  assert.lengthOf(processStarts, 2, "the idle release must retire the first Droid process");

  const sessionId = droidInitializedSessionId(transcript);
  const closes = droidRequests(transcript, "close_session");
  assert.isAtLeast(closes.length, 1, "the adapter closes the first session before releasing it");
  assert.isBelow(
    closes[0]!.index,
    processStarts[1]!,
    "the first process closes before the respawn",
  );

  const loads = droidRequests(transcript, "load_session");
  assert.lengthOf(loads, 1, "the respawned process resumes the session instead of starting one");
  assert.isAbove(loads[0]!.index, processStarts[1]!);
  assert.equal(field(loads[0]!.params, "sessionId"), sessionId);

  const sends = droidRequests(transcript, "add_user_message");
  assert.lengthOf(sends, 2);
  const resumeFrames = droidRequests(transcript)
    .filter((request) => request.index > loads[0]!.index)
    .map((request) => request.method)
    .filter((method) =>
      ["droid.load_session", "droid.update_session_settings", "droid.add_user_message"].includes(
        method,
      ),
    );
  assert.deepEqual(
    resumeFrames,
    ["droid.update_session_settings", "droid.add_user_message"],
    "load_session takes no settings, so they are re-sent before the second turn",
  );
  const [settings] = droidRequests(transcript, "update_session_settings");
  assert.isAbove(settings!.index, loads[0]!.index);
  assert.isBelow(settings!.index, sends[1]!.index);
  assert.equal(field(settings!.params, "interactionMode"), "auto");
  assert.isString(field(settings!.params, "autonomyLevel"));

  assertBaseProjection({
    result,
    transcript,
    runCount: 2,
    runStatuses: ["completed", "completed"],
  });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertUserMessagesInclude(projection, [
    PROVIDER_THREAD_RESUME_FIRST_PROMPT,
    PROVIDER_THREAD_RESUME_SECOND_PROMPT,
  ]);
  assertDroidSessionThread(projection, sessionId);
  assertDroidTurnIdentity(result, transcript);

  const lastTurn = projection.providerTurns
    .toSorted((left, right) => left.ordinal - right.ordinal)
    .at(-1);
  const finalAnswer = projection.turnItems
    .flatMap((item) =>
      item.type === "assistant_message" && item.providerTurnId === lastTurn?.id ? [item.text] : [],
    )
    .join("\n");
  assert.include(finalAnswer, FIRST_FINAL, "the resumed session must remember the first answer");
  assert.include(finalAnswer, SECOND_FINAL);

  const hidden = transcript.entries.flatMap((entry) =>
    entry.type === "emit_inbound" ? hiddenContextMessages(entry.frame) : [],
  );
  assert.isAbove(hidden.length, 0, "load_session history carries Droid's injected messages");
  for (const message of hidden) {
    assert.include(
      [
        JSON.stringify(HIDDEN_CONTEXT_PLACEHOLDER),
        JSON.stringify([{ type: "text", text: HIDDEN_CONTEXT_PLACEHOLDER }]),
      ],
      JSON.stringify(field(message, "content")),
      "a committed transcript keeps no injected context text",
    );
  }
  const visibleText = JSON.stringify(
    projection.turnItems.flatMap((item) =>
      item.type === "user_message" || item.type === "assistant_message" ? [item.text] : [],
    ),
  );
  assert.notInclude(visibleText, HIDDEN_CONTEXT_PLACEHOLDER);
}
