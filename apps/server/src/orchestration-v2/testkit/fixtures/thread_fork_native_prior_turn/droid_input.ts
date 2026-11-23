import {
  THREAD_FORK_NATIVE_PRIOR_TURN_ALPHA_PROMPT,
  THREAD_FORK_NATIVE_PRIOR_TURN_BETA_PROMPT,
  type OrchestratorFixtureInput,
} from "../shared.ts";
import { DROID_ASSISTANT_REPLIES_PROMPT } from "../thread_rollback/droid_input.ts";

/**
 * Two source turns, then a fork from the first. The fork drops the second turn, so Droid forks
 * the session and rewinds the copy, and the fork's answer can only list the first turn's reply.
 * The shared fixture ends with a prompt that repeats the conversation; see
 * `DROID_ASSISTANT_REPLIES_PROMPT` for why Droid asks for its own replies instead.
 */
export function droidThreadForkNativePriorTurnInput(): OrchestratorFixtureInput {
  return {
    steps: [
      { type: "message", text: THREAD_FORK_NATIVE_PRIOR_TURN_ALPHA_PROMPT },
      { type: "message", text: THREAD_FORK_NATIVE_PRIOR_TURN_BETA_PROMPT },
      { type: "fork_thread", sourceRunIndex: 1 },
      { type: "message", text: DROID_ASSISTANT_REPLIES_PROMPT },
    ],
  };
}
