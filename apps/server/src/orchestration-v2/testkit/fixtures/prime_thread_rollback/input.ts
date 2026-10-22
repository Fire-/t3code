import {
  THREAD_ROLLBACK_FIRST_PROMPT,
  THREAD_ROLLBACK_SECOND_PROMPT,
  type OrchestratorFixtureInput,
} from "../shared.ts";

/**
 * Prime injects the user's harness memories into every session as hidden
 * context, so a prompt that asks for "the conversation" makes the model
 * reproduce them. Asking only for its own replies keeps them out.
 */
export const PRIME_THREAD_ROLLBACK_REPLIES_PROMPT =
  "List your earlier replies in this thread, word for word, and nothing else.";

/** The steps of `thread_rollback`, ending with a question only Prime's own replies answer. */
export function primeThreadRollbackInput(): OrchestratorFixtureInput {
  return {
    steps: [
      { type: "message", text: THREAD_ROLLBACK_FIRST_PROMPT },
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
