import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertSemanticProjectionIntegrity,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  projectionFor,
} from "../shared.ts";
import {
  PRIME_AUTONOMOUS_COMMAND,
  PRIME_AUTONOMOUS_PROMPT,
  PRIME_GOAL_COMMAND,
  PRIME_GOAL_OBJECTIVE,
} from "./input.ts";

const AUTONOMOUS_NOTICE_TYPES = [
  "autonomous_gate_started",
  "autonomous_gate_failed",
  "autonomous_gate_passed",
  "autonomous_stopped",
];
const GATE_FAILED_MARKER = "[autonomous-continuation: gate-failed]";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

function messageText(message: unknown): string {
  const content = field(message, "content");
  if (typeof content === "string") return content;
  return Array.isArray(content)
    ? content.map((block) => (field(block, "type") === "text" ? field(block, "text") : "")).join("")
    : "";
}

/**
 * `/goal <objective>` runs as one session_command action across its agent
 * runs, so its turn stays open over `agent_end` and settles once after the
 * action clears. `/autonomous on --gate ...` then makes the next prompt's
 * run check a gate that fails once: Prime posts gate rows as custom messages
 * and feeds the failure back as a user-role `[autonomous-continuation:
 * gate-failed]` message, which must be a provider-created item, never a user
 * bubble.
 */
export function assertPrimeGoalAutonomousOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const frames = transcript.entries.flatMap((entry, index) =>
    entry.type === "runtime_exit"
      ? []
      : [{ index, direction: entry.type === "expect_outbound" ? "out" : "in", frame: entry.frame }],
  );
  const typeOf = (frame: unknown) => field(frame, "type");
  const prompts = frames.filter(
    (entry) => entry.direction === "out" && typeOf(entry.frame) === "prompt",
  );
  assert.deepEqual(
    prompts.map((entry) => field(entry.frame, "message")),
    [PRIME_GOAL_COMMAND, PRIME_AUTONOMOUS_COMMAND, PRIME_AUTONOMOUS_PROMPT],
  );
  const [goalPrompt, , autonomousPrompt] = prompts.map((entry) => entry.index);
  const inbound = (from: number, to = Number.POSITIVE_INFINITY) =>
    frames.filter((entry) => entry.direction === "in" && entry.index > from! && entry.index < to);

  const goalFrames = inbound(goalPrompt!, prompts[1]!.index);
  const goalAgentEnds = goalFrames.filter((entry) => typeOf(entry.frame) === "agent_end");
  assert.isNotEmpty(goalAgentEnds, "the goal ran the agent");
  const goalActionUpdates = goalFrames.filter(
    (entry) => typeOf(entry.frame) === "session_action_update",
  );
  const goalClearing = goalActionUpdates.at(-1);
  assert.isDefined(goalClearing);
  assert.isUndefined(field(field(goalClearing!.frame, "actions"), "active"));
  assert.isTrue(
    goalActionUpdates.some(
      (entry) =>
        entry.index < goalAgentEnds[0]!.index &&
        field(field(field(entry.frame, "actions"), "active"), "kind") === "session_command",
    ),
    "the goal action is active when its first run ends",
  );
  assert.isAbove(goalClearing!.index, goalAgentEnds.at(-1)!.index);
  assert.isTrue(
    goalFrames.some((entry) => typeOf(entry.frame) === "goal_update"),
    "Prime reports the goal",
  );

  const autonomousFrames = inbound(autonomousPrompt!);
  const ended = autonomousFrames.filter((entry) => typeOf(entry.frame) === "message_end");
  const continuation = ended.find(
    (entry) =>
      field(field(entry.frame, "message"), "role") === "user" &&
      messageText(field(entry.frame, "message")).startsWith(GATE_FAILED_MARKER),
  );
  assert.isDefined(continuation, "Prime fed the failed gate back as a continuation");
  const notices = ended.flatMap((entry) => {
    const message = field(entry.frame, "message");
    const customType = field(message, "customType");
    return field(message, "role") === "custom" &&
      typeof customType === "string" &&
      AUTONOMOUS_NOTICE_TYPES.includes(customType)
      ? [{ index: entry.index, customType, content: messageText(message) }]
      : [];
  });
  const firstIndex = (customType: string) =>
    notices.find((notice) => notice.customType === customType)?.index ?? -1;
  for (const customType of AUTONOMOUS_NOTICE_TYPES) {
    assert.isAbove(firstIndex(customType), -1, `${customType} was recorded`);
  }
  assert.isBelow(firstIndex("autonomous_gate_failed"), continuation!.index);
  assert.isBelow(continuation!.index, firstIndex("autonomous_gate_passed"));
  assert.isBelow(firstIndex("autonomous_gate_passed"), firstIndex("autonomous_stopped"));

  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assert.isAtLeast(projection.runs.length, 3);
  assert.deepEqual(
    projection.runs.map((run) => run.status),
    projection.runs.map(() => "completed"),
  );
  for (const run of projection.runs) {
    const turns = projection.providerTurns.filter(
      (turn) => turn.runAttemptId === run.activeAttemptId,
    );
    assert.deepEqual(
      turns.map((turn) => turn.status),
      ["completed"],
      `run ${run.ordinal} settles exactly once`,
    );
  }

  const providerThread = projection.providerThreads.find(
    (thread) => thread.id === projection.thread.activeProviderThreadId,
  );
  assert.equal(providerThread?.goal?.objective, PRIME_GOAL_OBJECTIVE, "goal state is projected");

  const userAuthored = projection.messages.filter((message) => message.createdBy === "user");
  assert.deepEqual(
    userAuthored.map((message) => message.text),
    [PRIME_GOAL_COMMAND, PRIME_AUTONOMOUS_COMMAND, PRIME_AUTONOMOUS_PROMPT],
    "only the three prompts are user bubbles; echoes and continuations are not",
  );
  const continuationItems = projection.turnItems.filter(
    (item) => item.type === "user_message" && item.text.startsWith(GATE_FAILED_MARKER),
  );
  assert.isNotEmpty(continuationItems, "the continuation is a turn item");
  const autonomousRun = projection.runs.find((run) => run.userMessageId === userAuthored[2]!.id);
  assert.isDefined(autonomousRun);
  for (const item of continuationItems) {
    assert.isTrue(item.type === "user_message" && item.title === "Autonomous gate failed");
    assert.equal(item.runId, autonomousRun!.id);
    const message = projection.messages.find(
      (candidate) => item.type === "user_message" && candidate.id === item.messageId,
    );
    assert.equal(message?.createdBy, "agent");
    assert.equal(message?.creationSource, "provider");
  }

  const noticeMessages = projection.turnItems.flatMap((item) =>
    item.type === "system_notice" ? [item.message] : [],
  );
  for (const notice of notices) {
    assert.include(noticeMessages, notice.content, `${notice.customType} is a system notice`);
  }
  assert.isFalse(
    projection.turnItems.some(
      (item) => item.type === "assistant_message" && item.text.includes("[autonomous-continuation"),
    ),
    "the continuation never renders as assistant text",
  );

  const displayedRows = inbound(goalPrompt!).flatMap((entry) => {
    const message = field(entry.frame, "message");
    return typeOf(entry.frame) === "message_end" &&
      field(message, "role") === "custom" &&
      (field(message, "customType") === "session_slash_command_result" ||
        field(message, "customType") === "autonomous_status")
      ? [messageText(message)]
      : [];
  });
  assert.lengthOf(displayedRows, 2, "the /goal result row and the /autonomous status");
  for (const row of displayedRows) {
    assert.include(noticeMessages, row, "a displayed row is a system notice");
  }
  assert.isTrue(
    inbound(goalPrompt!).some(
      (entry) => field(field(entry.frame, "message"), "customType") === "goal_context",
    ),
    "Prime steered the goal with goal_context",
  );
  assert.isFalse(
    projection.turnItems.some(
      (item) =>
        item.type !== "user_message" &&
        [PRIME_GOAL_COMMAND, PRIME_AUTONOMOUS_COMMAND, "[goal: continuation]"].some((text) =>
          JSON.stringify(item).includes(text),
        ),
    ),
    "the slash echoes and goal_context produce no item",
  );
}
