import { assert, describe, it } from "@effect/vitest";
import {
  MessageId,
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import { historicalMessage } from "@t3tools/provider-core/server/handoffBudget";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { droidTranscriptEntries } from "@t3tools/provider-droid/server";
import * as DateTime from "effect/DateTime";

import { DROID_MODEL_SELECTION } from "../orchestration-v2/testkit/fixtures/shared.ts";
import { droidRunRecovery } from "./DroidRunRecovery.ts";

const droid = ProviderDriverKind.make("droid");
const threadId = ThreadId.make("thread-recover");
const runId = RunId.make("run-recover:2");
const rootNodeId = NodeId.make("node-recover:root");
const attemptId = RunAttemptId.make("attempt-recover:1");
const providerThreadId = ProviderThreadId.make("provider-thread-recover");
const providerTurnId = ProviderTurnId.make("provider-turn-recover");
const providerInstanceId = ProviderInstanceId.make("droid");
/** Seconds after 10:00:00. */
const at = (second: number) => DateTime.makeUnsafe(Date.UTC(2026, 9, 26, 10, 0, second));
const iso = (second: number) => DateTime.formatIso(at(second));

const run: OrchestrationV2Run = {
  id: runId,
  threadId,
  ordinal: 2,
  providerInstanceId,
  modelSelection: DROID_MODEL_SELECTION,
  providerThreadId,
  userMessageId: MessageId.make("u1"),
  rootNodeId,
  activeAttemptId: attemptId,
  status: "running",
  queuePosition: null,
  requestedAt: at(0),
  startedAt: at(0),
  completedAt: null,
  checkpointId: null,
  contextHandoffId: null,
};

const attempt: OrchestrationV2RunAttempt = {
  id: attemptId,
  runId,
  attemptOrdinal: 1,
  rootNodeId,
  providerInstanceId,
  providerThreadId,
  providerTurnId: null,
  reason: "initial",
  status: "running",
  startedAt: at(0),
  completedAt: null,
};

const providerTurn: OrchestrationV2ProviderTurn = {
  id: providerTurnId,
  providerThreadId,
  nodeId: rootNodeId,
  runAttemptId: attemptId,
  nativeTurnRef: { driver: droid, nativeId: "turn-1", strength: "strong" },
  ordinal: 2,
  status: "running",
  startedAt: at(0),
  completedAt: null,
};

const node = (
  id: NodeId,
  kind: OrchestrationV2ExecutionNode["kind"],
  startedAt: number,
): OrchestrationV2ExecutionNode => ({
  id,
  threadId,
  runId,
  parentNodeId: id === rootNodeId ? null : rootNodeId,
  rootNodeId,
  kind,
  status: "running",
  countsForRun: id === rootNodeId,
  providerThreadId,
  providerTurnId: id === rootNodeId ? null : providerTurnId,
  nativeItemRef: null,
  runtimeRequestId: null,
  checkpointScopeId: null,
  startedAt: at(startedAt),
  completedAt: null,
});

const base = (id: string, ordinal: number, second: number) => ({
  id: TurnItemId.make(id),
  threadId,
  runId,
  nodeId: rootNodeId,
  providerThreadId,
  providerTurnId,
  nativeItemRef: null,
  parentItemId: null,
  ordinal: 2_000_000 + ordinal,
  title: null,
  startedAt: at(second),
  completedAt: at(second),
  updatedAt: at(second),
});

const userMessage = (
  id: string,
  ordinal: number,
  second: number,
  text: string,
): OrchestrationV2TurnItem => ({
  ...base(id, ordinal, second),
  status: "completed",
  type: "user_message",
  createdBy: "user",
  creationSource: "web",
  messageId: MessageId.make(id),
  inputIntent: ordinal === 1 ? "turn_start" : "steer",
  text,
  attachments: [],
});

/** A tool row as the live turn left it: started, never settled. */
const liveTool = (toolUseId: string, ordinal: number, second: number): OrchestrationV2TurnItem => ({
  ...base(`live:${toolUseId}`, ordinal, second),
  nodeId: NodeId.make(`node:${toolUseId}`),
  nativeItemRef: {
    driver: droid,
    nativeId: `${providerTurnId}:tool:${toolUseId}`,
    strength: "strong",
  },
  status: "running",
  title: "Grep",
  completedAt: null,
  type: "dynamic_tool",
  toolName: "Grep",
  input: {},
});

const record = (
  id: string,
  second: number,
  role: "user" | "assistant",
  content: ReadonlyArray<Record<string, unknown>>,
  visibility?: string,
) => ({
  type: "message",
  id,
  timestamp: iso(second),
  message: { role, content, ...(visibility === undefined ? {} : { visibility }) },
});
const text = (value: string) => ({ type: "text", text: value });
const thinking = (value: string) => ({ type: "thinking", thinking: value, signature: "sig" });
const toolUse = (id: string, name: string, input: unknown = {}) => ({
  type: "tool_use",
  id,
  name,
  input,
});
const toolResult = (id: string, content: string, isError = false) => ({
  type: "tool_result",
  tool_use_id: id,
  content,
  ...(isError ? { is_error: true } : {}),
});

describe("droidRunRecovery", () => {
  it("settles a run's tool rows from the transcript and puts its thinking and replies between them", () => {
    const entries = droidTranscriptEntries([
      record("before", -5, "assistant", [text("From an earlier run.")]),
      record("u1", 0, "user", [text("Find the bug")]),
      record("a1", 1, "assistant", [thinking("Search first."), toolUse("t-a", "Grep")]),
      record("r1", 3, "user", [toolResult("t-a", "found in a.ts")]),
      record("a2", 4, "assistant", [text("It's in a.ts.")]),
      record("u2", 5, "user", [text("Also check b.ts")]),
      record("a3", 6, "assistant", [thinking("Now b."), toolUse("t-b", "Grep")]),
      record("r2", 7, "user", [toolResult("t-b", "no such file", true)]),
      record("a4", 8, "assistant", [toolUse("t-c", "Grep")]),
      record("a5", 9, "assistant", [toolUse("t-d", "Script", { script: "await tools.LS({})" })]),
      record(
        "repl-tool-call:1",
        9,
        "assistant",
        [{ ...toolUse("t-inner", "LS"), script_execution: { outer_tool_use_id: "t-d" } }],
        "user_only",
      ),
      record("r3", 10, "user", [toolResult("t-d", "a.ts b.ts")]),
      record("a6", 11, "assistant", [text("Done.")]),
    ]);
    const items = [
      userMessage("u1", 1, 0, "Find the bug"),
      liveTool("t-a", 2, 2),
      userMessage("u2", 3, 5, "Also check b.ts"),
      liveTool("t-b", 4, 6),
      liveTool("t-c", 5, 8),
    ];
    const nodes = [
      node(rootNodeId, "root_turn", 0),
      ...["t-a", "t-b", "t-c"].map((id, index) =>
        node(NodeId.make(`node:${id}`), "tool_call", 2 + index * 3),
      ),
    ];

    const recovery = droidRunRecovery({
      run,
      projection: {
        attempts: [attempt],
        nodes,
        providerTurns: [providerTurn],
        turnItems: items,
        messages: [],
      },
      entries,
      derive: IdAllocator.derive,
    });

    assert.deepStrictEqual(
      recovery.items.map((item) => [
        item.type,
        item.status,
        item.type === "dynamic_tool"
          ? (item.output ?? null)
          : item.type === "user_message" ||
              item.type === "assistant_message" ||
              item.type === "reasoning"
            ? item.text
            : null,
      ]),
      [
        ["user_message", "completed", "Find the bug"],
        ["reasoning", "completed", "Search first."],
        ["dynamic_tool", "completed", "found in a.ts"],
        ["assistant_message", "completed", "It's in a.ts."],
        ["user_message", "completed", "Also check b.ts"],
        ["reasoning", "completed", "Now b."],
        ["dynamic_tool", "failed", "no such file"],
        ["dynamic_tool", "interrupted", null],
        ["dynamic_tool", "completed", "a.ts b.ts"],
        ["assistant_message", "completed", "Done."],
      ],
    );
    // Live rows keep their ids; a call the live turn never showed gets the id it would have had.
    assert.deepStrictEqual(
      recovery.items.filter((item) => item.type === "dynamic_tool").map((item) => item.id),
      [
        "live:t-a",
        "live:t-b",
        "live:t-c",
        IdAllocator.derive.turnItemFromProviderItem({
          driver: droid,
          nativeItemId: `${providerTurnId}:tool:t-d`,
        }),
      ],
    );
    assert.deepStrictEqual(
      recovery.messages.map((message) => [message.role, message.text, message.runId]),
      [
        ["assistant", "It's in a.ts.", runId],
        ["assistant", "Done.", runId],
      ],
    );
    assert.deepStrictEqual(
      [recovery.run.status, recovery.run.completedAt, recovery.attempts.map((a) => a.status)],
      ["interrupted", at(11), ["interrupted"]],
    );
    assert.deepStrictEqual(
      recovery.providerTurns.map((turn) => turn.status),
      ["interrupted"],
    );
    assert.deepStrictEqual(
      recovery.nodes.map((settled) => [settled.id, settled.status]),
      [
        [rootNodeId, "interrupted"],
        ["node:t-a", "completed"],
        ["node:t-b", "failed"],
        ["node:t-c", "interrupted"],
      ],
    );
  });

  it("hands the next turn each AskUser exchange as a question and an answer, without adding them to the thread", () => {
    const questionnaire = "1. [question] Which file?\n[option] a.ts\n[option] b.ts";
    const entries = droidTranscriptEntries([
      record("u1", 0, "user", [text("Fix it")]),
      record("a1", 1, "assistant", [toolUse("t-ask", "AskUser", { questionnaire })]),
      record("r1", 3, "user", [toolResult("t-ask", "1. [question] Which file?\n[answer] a.ts")]),
      record("a2", 4, "assistant", [text("Fixing a.ts.")]),
      record("a3", 5, "assistant", [toolUse("t-ask-2", "AskUser", { questionnaire })]),
    ]);

    const recovery = droidRunRecovery({
      run,
      projection: {
        attempts: [attempt],
        nodes: [node(rootNodeId, "root_turn", 0)],
        providerTurns: [providerTurn],
        turnItems: [userMessage("u1", 1, 0, "Fix it")],
        messages: [],
      },
      entries,
      derive: IdAllocator.derive,
    });

    assert.deepStrictEqual(
      recovery.handoffItems.flatMap((item) => {
        const message = historicalMessage(item);
        return message === null ? [] : [[message.role, message.status, message.text]];
      }),
      [
        ["user", "completed", "Fix it"],
        ["assistant", "completed", `<ask_user>\n${questionnaire}\n</ask_user>`],
        [
          "user",
          "completed",
          "<ask_user_response>\n1. [question] Which file?\n[answer] a.ts\n</ask_user_response>",
        ],
        ["assistant", "completed", "Fixing a.ts."],
        ["assistant", "interrupted", `<ask_user>\n${questionnaire}\n</ask_user>`],
      ],
    );
    assert.deepStrictEqual(
      recovery.items.map((item) => item.type),
      ["user_message", "dynamic_tool", "assistant_message", "dynamic_tool"],
    );
  });
});
