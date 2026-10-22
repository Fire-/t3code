import type { OrchestratorFixtureInput } from "../shared.ts";

export const PRIME_GOAL_OBJECTIVE =
  "Reply with the single word goal-ok, then run `import goal; await goal.complete()` in the Python REPL.";
export const PRIME_GOAL_COMMAND = `/goal ${PRIME_GOAL_OBJECTIVE}`;
export const PRIME_AUTONOMOUS_COMMAND = '/autonomous on --gate "sh gate.sh" --max-turns 10';
export const PRIME_AUTONOMOUS_PROMPT = "Reply with exactly: ready";

/**
 * The gate fails until the model edits status.txt, which it only does after
 * Prime feeds the failure back as `[autonomous-continuation: gate-failed]`.
 * Prime reruns a failed gate only once the git worktree changed
 * (core/autonomous.ts runAutonomousQualityGates), so the second run passes.
 */
const GATE_SCRIPT = `if [ "$(cat status.txt)" = "done" ]; then
  echo "gate passed: status.txt is done"
  exit 0
fi
echo "status.txt must contain exactly: done"
echo "Fix it with: printf 'done\\n' > status.txt"
exit 1
`;

export function primeGoalAutonomousInput(): OrchestratorFixtureInput {
  return {
    workspaceFiles: { "gate.sh": GATE_SCRIPT, "status.txt": "pending\n" },
    steps: [
      { type: "message", text: PRIME_GOAL_COMMAND },
      { type: "message", text: PRIME_AUTONOMOUS_COMMAND },
      { type: "message", text: PRIME_AUTONOMOUS_PROMPT },
    ],
  };
}
