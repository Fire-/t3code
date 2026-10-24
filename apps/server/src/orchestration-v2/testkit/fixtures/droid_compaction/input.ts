import type { OrchestratorFixtureInput } from "../shared.ts";

export const DROID_COMPACTION_MARKER = "droid-marker-4Z7Q";
export const DROID_COMPACTION_STORE_PROMPT = `Remember the opaque marker ${DROID_COMPACTION_MARKER} for later in this conversation. Respond with exactly: compaction marker ${DROID_COMPACTION_MARKER} stored`;
export const DROID_COMPACTION_FILLER_PROMPTS = [
  "Respond with exactly: second compaction fixture turn",
  "Respond with exactly: third compaction fixture turn",
] as const;
export const DROID_COMPACTION_RECALL_PROMPT =
  "Which opaque marker did your first reply in this thread contain? Respond with only the marker.";

/** The turns before `/compact`, shared with the probe that stops a running compaction. */
export const droidCompactionLeadSteps = () =>
  [
    { type: "message", text: DROID_COMPACTION_STORE_PROMPT },
    ...DROID_COMPACTION_FILLER_PROMPTS.map((text) => ({ type: "message", text }) as const),
  ] as const satisfies OrchestratorFixtureInput["steps"];

export function droidCompactionInput(): OrchestratorFixtureInput {
  return {
    steps: [
      ...droidCompactionLeadSteps(),
      { type: "message", text: "/compact" },
      { type: "message", text: DROID_COMPACTION_RECALL_PROMPT },
    ],
  };
}
