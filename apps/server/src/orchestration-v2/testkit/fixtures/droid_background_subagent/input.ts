import { DROID_TASK_COMPLETION_LABEL } from "../../../Adapters/DroidAdapterV2.testkit.ts";
import type { OrchestratorFixtureInput } from "../shared.ts";

export const DROID_BACKGROUND_SUBAGENT_ROOT_REPLY = "ROOT_DONE";
export const DROID_BACKGROUND_SUBAGENT_CHILD_REPLY = "SUBAGENT_DONE";
export const DROID_BACKGROUND_SUBAGENT_WAKE_REPLY = "WAKE_DONE";

// Droid ends the root turn first and reports the finished task afterwards, in a turn of its own.
export const DROID_BACKGROUND_SUBAGENT_PROMPT = [
  'Call the Task tool exactly once with subagent_type "worker", description "sleeper", and await set to false so it runs in the background.',
  `Its prompt is: run the shell command 'sleep 10' and then reply exactly ${DROID_BACKGROUND_SUBAGENT_CHILD_REPLY}.`,
  `Do not wait for it and do not call any other tool. As soon as it is launched, reply exactly ${DROID_BACKGROUND_SUBAGENT_ROOT_REPLY} and stop.`,
  `When the subagent's completion is reported later, reply exactly ${DROID_BACKGROUND_SUBAGENT_WAKE_REPLY} and stop.`,
].join(" ");

/**
 * Replay emits Droid's idle report as soon as the cursor reaches it, before run 1 has settled, so
 * the report is held until the thread is idle. Live, Droid reports the task when it finishes and
 * the recorder drops the gate step: run 2 is the turn Droid starts on its own.
 */
export function droidBackgroundSubagentInput(): OrchestratorFixtureInput {
  return {
    steps: [
      { type: "message", text: DROID_BACKGROUND_SUBAGENT_PROMPT },
      { type: "release_replay_gate", label: DROID_TASK_COMPLETION_LABEL },
      { type: "await_run_status", targetRunIndex: 2, status: "completed" },
    ],
  };
}
