import type { OrchestratorFixtureInput } from "../shared.ts";

export const DROID_SUBAGENT_ALPHA_CONTENT = "alpha-subagent-fixture";
export const DROID_SUBAGENT_BETA_CONTENT = "beta-subagent-fixture";

/**
 * The shared prompt reads files an empty recording workspace does not have, so Droid's variant
 * seeds two small ones. Each subagent is awaited: the Task call returns the subagent's report.
 */
export const DROID_SUBAGENT_PROMPT = [
  'Spawn 2 subagents with the Task tool (subagent_type "worker", not in the background): one reads alpha.txt and one reads beta.txt, and each replies with exactly the contents of its file.',
  "Do not read the files yourself and do not use any other tool.",
  `When both have replied, answer with exactly: ${DROID_SUBAGENT_ALPHA_CONTENT} ${DROID_SUBAGENT_BETA_CONTENT}`,
].join(" ");

export function droidSubagentInput(): OrchestratorFixtureInput {
  return {
    workspaceFiles: {
      "alpha.txt": `${DROID_SUBAGENT_ALPHA_CONTENT}\n`,
      "beta.txt": `${DROID_SUBAGENT_BETA_CONTENT}\n`,
    },
    steps: [{ type: "message", text: DROID_SUBAGENT_PROMPT }],
  };
}
