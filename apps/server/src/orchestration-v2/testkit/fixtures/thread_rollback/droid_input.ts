import { THREAD_ROLLBACK_AFTER_PROMPT, type OrchestratorFixtureInput } from "../shared.ts";
import { threadRollbackInput } from "./input.ts";

/**
 * The shared fixtures end by asking the model to repeat the conversation. Droid loads the user's
 * personal instructions into every session, so that prompt can reproduce private context into a
 * recording. This one asks only for the assistant's own earlier replies.
 */
export const DROID_ASSISTANT_REPLIES_PROMPT =
  "List your earlier replies in this thread, word for word, and nothing else.";

export function withDroidFinalPrompt(input: OrchestratorFixtureInput): OrchestratorFixtureInput {
  return {
    ...input,
    steps: input.steps.map((step) =>
      step.type === "message" && step.text === THREAD_ROLLBACK_AFTER_PROMPT
        ? { ...step, text: DROID_ASSISTANT_REPLIES_PROMPT }
        : step,
    ),
  };
}

export function droidThreadRollbackInput(): OrchestratorFixtureInput {
  return withDroidFinalPrompt(threadRollbackInput());
}
