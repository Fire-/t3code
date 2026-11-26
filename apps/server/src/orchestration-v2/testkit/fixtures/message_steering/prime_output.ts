import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import { assertPrimeSettleProbesFollowAgentEnd } from "../simple/prime_output.ts";
import { assertPiMessageSteeringOutput } from "./pi_output.ts";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

/**
 * Prime steers exactly like Pi: both the turn and the steer go out as `prompt`
 * with `streamingBehavior: "steer"` and land in one provider turn. Prime may
 * refuse a message while busy; such a refusal must not fail the turn.
 */
export function assertPrimeMessageSteeringOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertPiMessageSteeringOutput(result, transcript);
  assertPrimeSettleProbesFollowAgentEnd(transcript);
  const outboundTypes = transcript.entries.flatMap((entry) =>
    entry.type === "expect_outbound" ? [field(entry.frame, "type")] : [],
  );
  assert.notInclude(outboundTypes, "steer", "Prime steers through prompt, never steer");
}
