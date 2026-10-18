/**
 * One-off live probes the Droid recorder can run next to the registered fixtures. A probe has the
 * shape of a fixture (input, model, output assertions) but is never registered for CI replay: its
 * transcript is written under `PROBE_DIRECTORY`, outside the worktree.
 */
import { assert } from "@effect/vitest";
import { ProviderDriverKind, type ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../src/orchestration-v2/testkit/OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  DROID_MODEL_SELECTION,
  projectionFor,
  type OrchestratorReplayFixture,
} from "../src/orchestration-v2/testkit/fixtures/shared.ts";
import {
  adapterReplyTo,
  DROID_FILE_WRITE_TOOLS,
  DROID_WRITE_TOOLS,
  droidExitSpecModeRequests,
  droidNotifications,
  droidProcessStartIndexes,
  droidRequests,
  droidResponseTo,
  droidToolCalls,
} from "../src/orchestration-v2/testkit/fixtures/simple/droid_output.ts";

const PROBE_DIRECTORY = "/tmp/droid-v2-probes";

// Each turn names the one tool it may use: at autonomy off every other tool call would raise an
// approval nobody answers, and the scenario would wait for it forever.
const PLAN_PROMPT =
  "Plan the creation of a file hello.txt containing the word hi. Do not inspect the workspace and do not use any tool except ExitSpecMode. Keep the plan to one short line, then call ExitSpecMode with that plan.";
const IMPLEMENT_PROMPT =
  "The plan is approved. Create hello.txt containing the word hi with the Create tool, then stop. Do not use any other tool.";

type ProbeAutonomy = "off" | "high";

const RUNTIME_MODE_FOR_AUTONOMY = {
  off: "approval-required",
  high: "full-access",
} as const satisfies Record<ProbeAutonomy, string>;

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

function notificationText(notification: unknown): string {
  const content = field(notification, "content");
  return typeof content === "string" ? content : JSON.stringify(content);
}

/**
 * What the adapter's `exit_spec_mode` reply leaves behind, read from the frames of a plan turn at
 * one autonomy level, a process restart, and a default-mode turn that implements the plan.
 */
function assertExitSpecModeConfirmation(autonomy: ProbeAutonomy) {
  return (result: OrchestratorV2ScenarioResult, transcript: ProviderReplayTranscript) => {
    const processStarts = droidProcessStartIndexes(transcript);
    assert.lengthOf(processStarts, 2, "the idle release retires the plan session's process");
    const resumeStart = processStarts[1]!;
    const sends = droidRequests(transcript, "add_user_message");
    assert.lengthOf(sends, 2, "one plan turn, one implement turn");

    const exits = droidExitSpecModeRequests(transcript).filter(
      (request) => request.index < resumeStart,
    );
    assert.lengthOf(exits, 1, "Droid asks to leave spec mode exactly once");
    assert.deepEqual(adapterReplyTo(transcript, exits[0]!), { selectedOption: "cancel" });

    const planNotifications = (type: string) =>
      droidNotifications(transcript, type).filter(
        ({ index }) => index > exits[0]!.index && index < resumeStart,
      );
    const cancelledResults = planNotifications("tool_result").filter(
      ({ notification }) =>
        field(notification, "isError") === true &&
        notificationText(notification).includes("Plan not approved"),
    );
    assert.lengthOf(cancelledResults, 1, "Droid reports the plan as not approved, in spec mode");
    const [completion] = planNotifications("agent_turn_completed");
    assert.equal(field(completion?.notification, "reason"), "completed");

    assert.deepEqual(
      droidToolCalls(transcript).filter(
        (call) => call.index < resumeStart && DROID_FILE_WRITE_TOOLS.includes(call.name),
      ),
      [],
      "no write tool runs in the plan turn",
    );
    for (const { index, notification } of droidNotifications(transcript, "settings_updated")) {
      if (index >= resumeStart) continue;
      const mode = field(field(notification, "settings"), "interactionMode");
      assert.isTrue(
        mode === undefined || mode === "spec",
        `a settings echo reports ${String(mode)} before the restart`,
      );
    }
    const workingStates = droidNotifications(transcript, "droid_working_state_changed").filter(
      ({ index }) => index < resumeStart,
    );
    assert.equal(
      field(workingStates.at(-1)?.notification, "newState"),
      "idle",
      "the session is idle after the cancel",
    );

    const [load] = droidRequests(transcript, "load_session");
    assert.isDefined(load, "the restarted process resumes the session");
    assert.isAbove(load!.index, resumeStart);
    const loaded = droidResponseTo(transcript, load!);
    assert.equal(field(field(loaded, "settings"), "interactionMode"), "spec");
    assert.equal(field(field(loaded, "settings"), "autonomyLevel"), autonomy);
    assert.equal(field(loaded, "workingState"), "idle");

    const switchToAuto = droidRequests(transcript, "update_session_settings").filter(
      (request) => request.index > load!.index && request.index < sends[1]!.index,
    );
    assert.lengthOf(switchToAuto, 1, "the implement turn switches the resumed session to auto");
    assert.equal(field(switchToAuto[0]!.params, "interactionMode"), "auto");
    assert.equal(field(switchToAuto[0]!.params, "autonomyLevel"), autonomy);
    assert.equal(field(sends[1]!.params, "text"), IMPLEMENT_PROMPT);
    assert.isAtLeast(
      droidToolCalls(transcript).filter(
        (call) => call.index > sends[1]!.index && DROID_WRITE_TOOLS.includes(call.name),
      ).length,
      1,
      "the default-mode turn implements the plan",
    );
    const completions = droidNotifications(transcript, "agent_turn_completed");
    assert.deepEqual(
      completions.map(({ notification }) => field(notification, "reason")),
      ["completed", "completed"],
    );

    assertBaseProjection({
      result,
      transcript,
      runCount: 2,
      runStatuses: ["completed", "completed"],
    });
    const projection = projectionFor(result, transcript.scenario);
    assertSemanticProjectionIntegrity(projection);
    assert.deepEqual(
      projection.runtimeRequests.filter((request) => request.status !== "resolved"),
      [],
      "every approval the implement turn raised was answered",
    );
  };
}

function exitSpecModeProbe(autonomy: ProbeAutonomy): OrchestratorReplayFixture {
  const name = `exit_spec_mode_${autonomy}`;
  return {
    name,
    buildInput: () => ({
      interactionMode: "plan",
      runtimeMode: RUNTIME_MODE_FOR_AUTONOMY[autonomy],
      steps: [
        { type: "message", text: PLAN_PROMPT },
        // Outlasts the idle timeout, so the session's process exits and the next message resumes
        // the session in a fresh one.
        { type: "advance_clock", duration: "31 minutes" },
        { type: "set_interaction_mode", interactionMode: "default" },
        { type: "message", text: IMPLEMENT_PROMPT },
        ...(autonomy === "off" ? [{ type: "approve_next_runtime_request" } as const] : []),
      ],
    }),
    providers: [
      {
        driver: ProviderDriverKind.make("droid"),
        transcriptFile: new URL(`file://${PROBE_DIRECTORY}/${name}.ndjson`),
        modelSelection: DROID_MODEL_SELECTION,
        assertOutput: assertExitSpecModeConfirmation(autonomy),
      },
    ],
  };
}

export const DROID_PROBE_FIXTURES: ReadonlyArray<OrchestratorReplayFixture> = [
  exitSpecModeProbe("off"),
  exitSpecModeProbe("high"),
];
