import type { OrchestratorFixtureInput } from "../shared.ts";
import { withDroidFinalPrompt } from "../thread_rollback/droid_input.ts";
import { threadRollbackAfterStopInput } from "./input.ts";

export function droidThreadRollbackAfterStopInput(): OrchestratorFixtureInput {
  return withDroidFinalPrompt(threadRollbackAfterStopInput());
}
