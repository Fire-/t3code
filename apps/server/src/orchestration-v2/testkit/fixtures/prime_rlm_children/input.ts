import type { OrchestratorFixtureInput } from "../shared.ts";

/**
 * `rlm.collect` blocks the cell until the child settles, so every child's terminal
 * `rlm_child_update` arrives inside the parent's turn, before the recording can close. A child
 * that ends without an explicit reply makes Prime start one more parent run with a
 * `[child-exited: ...]` notice (_deferRlmTerminalNotice); the last line keeps that run short.
 */
export const PRIME_RLM_CHILDREN_PROMPT = `Run this exact code in the Python REPL as one cell, then reply with exactly: parent-ok

h = await rlm.spawn('Reply with exactly: child-ok', name='scout')
print(await rlm.collect(h, timeout_ms=120000))

If a [child-exited] notice arrives afterwards, reply with exactly: notice-ok and run no code.`;

export function primeRlmChildrenInput(): OrchestratorFixtureInput {
  return { steps: [{ type: "message", text: PRIME_RLM_CHILDREN_PROMPT }] };
}
