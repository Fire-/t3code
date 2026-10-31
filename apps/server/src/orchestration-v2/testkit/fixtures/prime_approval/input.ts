import { PRIME_APPROVAL_PROMPT, type OrchestratorFixtureInput } from "../shared.ts";

/** Prime's bridge confirms every `ipython` cell outside full access. */
export function primeApprovalInput(): OrchestratorFixtureInput {
  return {
    runtimeMode: "approval-required",
    steps: [
      { type: "message", text: PRIME_APPROVAL_PROMPT },
      { type: "approve_next_runtime_request" },
    ],
  };
}
