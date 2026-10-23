import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import { projectionFor } from "../shared.ts";
import {
  assertDroidSessionThread,
  assertDroidTurnIdentity,
  droidInitializedSessionId,
  droidRequests,
} from "../simple/droid_output.ts";
import { assertMultiTurnOutput } from "./codex_output.ts";

export function assertDroidMultiTurnOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertMultiTurnOutput(result, transcript);
  const projection = projectionFor(result, transcript.scenario);

  const sessionId = droidInitializedSessionId(transcript);
  assert.lengthOf(
    droidRequests(transcript, "load_session"),
    0,
    "turn two stays on the live session",
  );
  assertDroidSessionThread(projection, sessionId);

  const sends = droidRequests(transcript, "add_user_message");
  assert.lengthOf(sends, 2);
  assertDroidTurnIdentity(result, transcript);
  const between = droidRequests(transcript)
    .filter((request) => request.index > sends[0]!.index && request.index < sends[1]!.index)
    .map((request) => request.method);
  assert.notInclude(
    between,
    "droid.update_session_settings",
    "nothing changed between the turns, so no settings frame is re-sent",
  );
}
