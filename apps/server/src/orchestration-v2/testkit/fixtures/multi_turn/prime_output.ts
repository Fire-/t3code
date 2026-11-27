import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import { assertPrimeTurnsSettleOnce } from "../simple/prime_output.ts";
import { assertMultiTurnOutput } from "./codex_output.ts";

export function assertPrimeMultiTurnOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertMultiTurnOutput(result, transcript);
  assertPrimeTurnsSettleOnce(result, transcript, 2);
}
