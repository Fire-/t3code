import {
  THREAD_ROLLBACK_FIRST_PROMPT,
  THREAD_ROLLBACK_SECOND_PROMPT,
  TURN_INTERRUPT_MID_TOOL_PROMPT,
  type OrchestratorFixtureInput,
} from "../shared.ts";
import { PRIME_THREAD_ROLLBACK_REPLIES_PROMPT } from "../prime_thread_rollback/input.ts";

/**
 * The steps of `thread_rollback_after_stop`: Stop restarts Prime, so the
 * rollback runs in the process the next message started.
 */
export function primeThreadRollbackAfterStopInput(): OrchestratorFixtureInput {
  return {
    steps: [
      { type: "message", text: THREAD_ROLLBACK_FIRST_PROMPT },
      { type: "message", text: TURN_INTERRUPT_MID_TOOL_PROMPT },
      { type: "interrupt", targetRunIndex: 2, waitForTurnItemType: "command_execution" },
      { type: "message", text: THREAD_ROLLBACK_SECOND_PROMPT },
      {
        type: "rollback",
        checkpointScopeSuffix: "root",
        checkpointSuffix: "1",
      },
      { type: "message", text: PRIME_THREAD_ROLLBACK_REPLIES_PROMPT },
    ],
  };
}
