import type { OrchestratorFixtureInput } from "../shared.ts";

export const PRIME_SESSION_COMMAND_TEXT = "/goal status";

/** A built-in session command that starts no model turn (core/agent-session.ts _handleGoalSlashCommand). */
export function primeSessionCommandInput(): OrchestratorFixtureInput {
  return {
    steps: [{ type: "message", text: PRIME_SESSION_COMMAND_TEXT }],
  };
}
