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
  THREAD_ROLLBACK_FIRST_PROMPT,
  THREAD_ROLLBACK_SECOND_PROMPT,
  type OrchestratorReplayFixture,
} from "../src/orchestration-v2/testkit/fixtures/shared.ts";
import {
  DROID_COMPACTION_FILLER_PROMPTS,
  DROID_COMPACTION_MARKER,
  DROID_COMPACTION_RECALL_PROMPT,
  DROID_COMPACTION_STORE_PROMPT,
  droidCompactionLeadSteps,
} from "../src/orchestration-v2/testkit/fixtures/droid_compaction/input.ts";
import {
  DROID_BACKGROUND_SUBAGENT_ROOT_REPLY,
  DROID_BACKGROUND_SUBAGENT_WAKE_REPLY,
  droidBackgroundSubagentInput,
} from "../src/orchestration-v2/testkit/fixtures/droid_background_subagent/input.ts";
import { DROID_ASSISTANT_REPLIES_PROMPT } from "../src/orchestration-v2/testkit/fixtures/thread_rollback/droid_input.ts";
import {
  adapterReplyTo,
  assertDroidSessionThread,
  DROID_FILE_WRITE_TOOLS,
  DROID_WRITE_TOOLS,
  droidExitSpecModeRequests,
  droidInitializedSessionId,
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

/**
 * Rewinds a two-turn session to before its first user message, to learn whether Droid accepts
 * `execute_rewind` there. Either answer is a valid recording: an accepted rewind moves the thread
 * to the returned session, a refused one makes the adapter start a fresh session.
 */
function assertThreadStartRewind(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const sends = droidRequests(transcript, "add_user_message");
  assert.lengthOf(sends, 3, "two turns, then the question after the rewind");
  const rewinds = droidRequests(transcript, "execute_rewind");
  assert.lengthOf(rewinds, 1, "the rewind to the thread start is tried once");
  const [rewind] = rewinds;
  assert.equal(field(rewind!.params, "messageId"), field(sends[0]!.params, "messageId"));
  assert.equal(field(rewind!.params, "sessionId"), droidInitializedSessionId(transcript));

  const answer = droidResponseTo(transcript, rewind!);
  const rewoundSessionId = field(answer, "newSessionId");
  const loads = droidRequests(transcript, "load_session").filter(
    (request) => request.index > rewind!.index,
  );
  const initializes = droidRequests(transcript, "initialize_session");
  if (typeof rewoundSessionId === "string") {
    assert.lengthOf(initializes, 1, "an accepted rewind keeps the one session it opened");
    assert.lengthOf(loads, 1);
    assert.equal(field(loads[0]!.params, "sessionId"), rewoundSessionId);
  } else {
    assert.lengthOf(initializes, 2, "a refused rewind starts a fresh session");
    assert.lengthOf(loads, 0);
  }

  assertBaseProjection({
    result,
    transcript,
    runCount: 3,
    runStatuses: ["rolled_back", "rolled_back", "completed"],
  });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  const finalAnswer = projection.turnItems.findLast((item) => item.type === "assistant_message");
  assert.notInclude(finalAnswer?.text, "first turn complete");
  assert.notInclude(finalAnswer?.text, "second turn complete");
}

const threadStartRewindProbe: OrchestratorReplayFixture = {
  name: "thread_start_rewind",
  buildInput: () => ({
    steps: [
      { type: "message", text: THREAD_ROLLBACK_FIRST_PROMPT },
      { type: "message", text: THREAD_ROLLBACK_SECOND_PROMPT },
      { type: "rollback", checkpointScopeSuffix: "root", checkpointSuffix: "0" },
      { type: "message", text: DROID_ASSISTANT_REPLIES_PROMPT },
    ],
  }),
  providers: [
    {
      driver: ProviderDriverKind.make("droid"),
      transcriptFile: new URL(`file://${PROBE_DIRECTORY}/thread_start_rewind.ndjson`),
      modelSelection: DROID_MODEL_SELECTION,
      assertOutput: assertThreadStartRewind,
    },
  ],
};

const COMPACTION_SEEDED_PROMPTS = [
  DROID_COMPACTION_STORE_PROMPT,
  ...DROID_COMPACTION_FILLER_PROMPTS,
] as const;

function messageText(message: unknown): string {
  const content = field(message, "content");
  if (!Array.isArray(content)) return "";
  return content
    .map((part) => field(part, "text"))
    .filter((text) => typeof text === "string")
    .join("");
}

/**
 * Kills the Droid process group while `compact_session` is unanswered, then resumes the same
 * session on a new process. Droid ignores `interrupt_session` during a compaction, so a kill is
 * the only stop; the recording shows what the kill leaves on disk: a session that loads with its
 * pre-compaction history (no summary, no partial compaction) and answers the next turn.
 */
function assertCompactionStopKill(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const sessionId = droidInitializedSessionId(transcript);
  const compactions = droidRequests(transcript, "compact_session");
  assert.lengthOf(compactions, 1);
  const compactIndex = compactions[0]!.index;
  const [firstStart, secondStart, ...otherStarts] = droidProcessStartIndexes(transcript);
  assert.isDefined(firstStart);
  assert.isDefined(secondStart);
  assert.lengthOf(otherStarts, 0, "the kill ends one process and the next turn starts one more");
  assert.isAbove(compactIndex, firstStart!);
  assert.isBelow(compactIndex, secondStart!);

  const beforeKill = transcript.entries.slice(compactIndex + 1, secondStart);
  assert.isFalse(
    beforeKill.some(
      (entry) =>
        entry.type === "emit_inbound" &&
        field(entry.frame, "type") === "response" &&
        field(entry.frame, "id") === compactions[0]!.id,
    ),
    "the kill lands while compact_session is unanswered",
  );
  assert.lengthOf(droidNotifications(transcript, "session_compacted"), 0, "no compaction finished");
  assert.lengthOf(
    droidRequests(transcript).filter(
      (request) =>
        request.index > compactIndex &&
        request.index < secondStart! &&
        (request.method === "droid.interrupt_session" || request.method === "droid.close_session"),
    ),
    0,
    "a kill sends Droid nothing",
  );

  const loads = droidRequests(transcript, "load_session");
  assert.lengthOf(loads, 1, "the next turn resumes the session once");
  assert.isAbove(loads[0]!.index, secondStart!);
  assert.equal(field(loads[0]!.params, "sessionId"), sessionId);
  const loaded = droidResponseTo(transcript, loads[0]!);
  const session = field(loaded, "session");
  assert.isDefined(session, "load_session succeeds on the killed session");
  assert.isFalse(
    transcript.entries.some(
      (entry, index) =>
        index > secondStart! &&
        entry.type === "emit_inbound" &&
        field(entry.frame, "error") !== undefined,
    ),
    "the resumed process reports no error",
  );

  assert.isFalse(field(loaded, "hasOlderMessages"));
  const messages = field(session, "messages");
  assert.isTrue(Array.isArray(messages));
  const visible = (messages as ReadonlyArray<unknown>).filter(
    (message) => !field(message, "visibility"),
  );
  const hidden = (messages as ReadonlyArray<unknown>).filter((message) =>
    field(message, "visibility"),
  );
  assert.isTrue(
    hidden.every((message) => String(field(message, "id")).startsWith("context-message:")),
    "no summary or other hidden message was written",
  );
  assert.deepEqual(
    visible.map((message) => field(message, "role")),
    ["user", "assistant", "user", "assistant", "user", "assistant"],
    "every seeded message is still in the history",
  );
  assert.deepEqual(
    visible.filter((message) => field(message, "role") === "user").map(messageText),
    [...COMPACTION_SEEDED_PROMPTS],
  );
  assert.include(messageText(visible[1]), DROID_COMPACTION_MARKER);

  const sends = droidRequests(transcript, "add_user_message");
  assert.deepEqual(
    sends.map((send) => field(send.params, "text")),
    [...COMPACTION_SEEDED_PROMPTS, DROID_COMPACTION_RECALL_PROMPT],
  );
  assert.isAbove(sends[3]!.index, loads[0]!.index);

  assertBaseProjection({
    result,
    transcript,
    runCount: 5,
    runStatuses: ["completed", "completed", "completed", "interrupted", "completed"],
  });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assert.deepEqual(
    projection.turnItems.filter((item) => item.type === "compaction").map((item) => item.status),
    ["cancelled"],
  );
  assertDroidSessionThread(projection, sessionId);
  assert.include(
    projection.turnItems.findLast((item) => item.type === "assistant_message")?.text,
    DROID_COMPACTION_MARKER,
    "the resumed session recalls the marker",
  );
}

const compactionStopKillProbe: OrchestratorReplayFixture = {
  name: "compaction_stop_kill",
  buildInput: () => ({
    steps: [
      ...droidCompactionLeadSteps(),
      { type: "message", text: "/compact" },
      { type: "interrupt", targetRunIndex: 4, waitForTurnItemType: "compaction" },
      { type: "message", text: DROID_COMPACTION_RECALL_PROMPT },
    ],
  }),
  providers: [
    {
      driver: ProviderDriverKind.make("droid"),
      transcriptFile: new URL(`file://${PROBE_DIRECTORY}/compaction_stop_kill.ndjson`),
      modelSelection: DROID_MODEL_SELECTION,
      assertOutput: assertCompactionStopKill,
    },
  ],
};

const WAKE_ROLLBACK_EXTRA_REPLY = "EXTRA_DONE";
const WAKE_ROLLBACK_EXTRA_PROMPT = `Reply exactly ${WAKE_ROLLBACK_EXTRA_REPLY} and stop. Do not call any tool.`;

/**
 * Rolls a thread back across its wake turn: a background Task, the continuation turn Droid starts
 * when the task reports, one more user turn, then a rollback to the checkpoint before the wake
 * turn. The adapter rewinds to the wake turn's own ref (`task-completion:<id>`), so the recording
 * shows whether Droid keeps a message under that id and what a rewind to it leaves. Either answer
 * is a valid recording.
 */
function assertWakeRollback(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const sessionId = droidInitializedSessionId(transcript);
  const sends = droidRequests(transcript, "add_user_message");
  assert.lengthOf(sends, 3, "the Task turn, the extra turn, then the question after the rollback");
  const reports = droidNotifications(transcript, "create_message").filter(({ notification }) =>
    String(field(field(notification, "message"), "id")).startsWith("task-completion:"),
  );
  assert.lengthOf(reports, 1, "Droid reports the finished task once");
  const wakeId = String(field(field(reports[0]!.notification, "message"), "id"));

  const rewinds = droidRequests(transcript, "execute_rewind");
  assert.lengthOf(rewinds, 1, "the rollback rewinds once");
  const [rewind] = rewinds;
  assert.equal(field(rewind!.params, "sessionId"), sessionId);
  assert.equal(
    field(rewind!.params, "messageId"),
    wakeId,
    "the wake turn is rewound by its task-completion id",
  );

  const answer = droidResponseTo(transcript, rewind!);
  const rewoundSessionId = field(answer, "newSessionId");
  if (typeof rewoundSessionId !== "string") return;

  const loads = droidRequests(transcript, "load_session").filter(
    (request) => request.index > rewind!.index,
  );
  assert.lengthOf(loads, 1, "an accepted rewind loads the session it returned");
  assert.equal(field(loads[0]!.params, "sessionId"), rewoundSessionId);
  const messages = field(field(droidResponseTo(transcript, loads[0]!), "session"), "messages");
  assert.isTrue(Array.isArray(messages));
  // The user's own prompt names the wake reply, so only the assistant's messages are read.
  const assistantText = (messages as ReadonlyArray<unknown>)
    .filter((message) => field(message, "role") === "assistant")
    .map(messageText)
    .join("\n");
  assert.include(assistantText, DROID_BACKGROUND_SUBAGENT_ROOT_REPLY);
  assert.notInclude(assistantText, DROID_BACKGROUND_SUBAGENT_WAKE_REPLY);
  assert.notInclude(assistantText, WAKE_ROLLBACK_EXTRA_REPLY);
  assert.isFalse(
    droidMessageIdsOf(messages).includes(wakeId),
    "the rewound history no longer holds the wake message",
  );

  assertBaseProjection({
    result,
    transcript,
    runCount: 4,
    runStatuses: ["completed", "rolled_back", "rolled_back", "completed"],
  });
  assertSemanticProjectionIntegrity(projectionFor(result, transcript.scenario));
}

function droidMessageIdsOf(messages: unknown): ReadonlyArray<string> {
  return Array.isArray(messages)
    ? messages.flatMap((message) => {
        const id = field(message, "id");
        return typeof id === "string" ? [id] : [];
      })
    : [];
}

const wakeRollbackProbe: OrchestratorReplayFixture = {
  name: "wake_rollback",
  buildInput: () => ({
    steps: [
      ...droidBackgroundSubagentInput().steps,
      { type: "message", text: WAKE_ROLLBACK_EXTRA_PROMPT },
      { type: "rollback", checkpointScopeSuffix: "root", checkpointSuffix: "1" },
      { type: "message", text: DROID_ASSISTANT_REPLIES_PROMPT },
    ],
  }),
  providers: [
    {
      driver: ProviderDriverKind.make("droid"),
      transcriptFile: new URL(`file://${PROBE_DIRECTORY}/wake_rollback.ndjson`),
      modelSelection: DROID_MODEL_SELECTION,
      runContinuationWorker: true,
      assertOutput: assertWakeRollback,
    },
  ],
};

/**
 * Forks from the turn before a wake turn. The adapter cuts the copy at the first later turn the
 * source's `load_session` lists, so the recording shows whether that listing holds the
 * `task-completion:<id>` message: if it does the copy ends before the wake turn, if it did not the
 * cut would fall at the extra turn and the copy would keep the wake turn.
 */
function assertWakeFork(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const sends = droidRequests(transcript, "add_user_message");
  assert.lengthOf(sends, 3, "the Task turn, the extra turn, then the question on the fork");
  const reports = droidNotifications(transcript, "create_message").filter(({ notification }) =>
    String(field(field(notification, "message"), "id")).startsWith("task-completion:"),
  );
  assert.lengthOf(reports, 1, "Droid reports the finished task once");
  const wakeId = String(field(field(reports[0]!.notification, "message"), "id"));

  const loads = droidRequests(transcript, "load_session");
  const sourceMessages = field(
    field(droidResponseTo(transcript, loads[0]!), "session"),
    "messages",
  );
  const listsWake = droidMessageIdsOf(sourceMessages).includes(wakeId);
  const rewinds = droidRequests(transcript, "execute_rewind");
  assert.lengthOf(rewinds, 1, "the fork rewinds its copy once");
  assert.equal(
    field(rewinds[0]!.params, "messageId"),
    listsWake ? wakeId : field(sends[1]!.params, "messageId"),
    "the copy is cut at the first later turn the source's history lists",
  );

  const fork = [...result.projections.values()].find((projection) =>
    projection.providerThreads.some((thread) => thread.forkedFrom != null),
  );
  assert.isDefined(fork);
  assertSemanticProjectionIntegrity(fork!);
  const answer = fork!.turnItems.findLast((item) => item.type === "assistant_message")?.text;
  assert.include(answer, DROID_BACKGROUND_SUBAGENT_ROOT_REPLY);
  assert.notInclude(answer, WAKE_ROLLBACK_EXTRA_REPLY);
  if (listsWake) assert.notInclude(answer, DROID_BACKGROUND_SUBAGENT_WAKE_REPLY);
}

const wakeForkProbe: OrchestratorReplayFixture = {
  name: "wake_fork",
  buildInput: () => ({
    steps: [
      ...droidBackgroundSubagentInput().steps,
      { type: "message", text: WAKE_ROLLBACK_EXTRA_PROMPT },
      { type: "fork_thread", sourceRunIndex: 1 },
      { type: "message", text: DROID_ASSISTANT_REPLIES_PROMPT },
    ],
  }),
  providers: [
    {
      driver: ProviderDriverKind.make("droid"),
      transcriptFile: new URL(`file://${PROBE_DIRECTORY}/wake_fork.ndjson`),
      modelSelection: DROID_MODEL_SELECTION,
      runContinuationWorker: true,
      assertOutput: assertWakeFork,
    },
  ],
};

export const DROID_PROBE_FIXTURES: ReadonlyArray<OrchestratorReplayFixture> = [
  exitSpecModeProbe("off"),
  exitSpecModeProbe("high"),
  threadStartRewindProbe,
  compactionStopKillProbe,
  wakeRollbackProbe,
  wakeForkProbe,
];
