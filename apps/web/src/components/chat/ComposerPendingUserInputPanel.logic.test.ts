import {
  MessageId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2ProjectedTurnItem,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";
import { questionPreamble } from "./ComposerPendingUserInputPanel.logic";

const threadId = ThreadId.make("thread");
const at = DateTime.makeUnsafe("2026-01-01T00:00:00.000Z");
const runA = RunId.make("run-a");
const runB = RunId.make("run-b");
const requestId = RuntimeRequestId.make("request");

function item(runId: RunId, id: string, ordinal: number) {
  return {
    id: TurnItemId.make(id),
    threadId,
    runId,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal,
    status: "completed" as const,
    title: null,
    startedAt: at,
    completedAt: at,
    updatedAt: at,
  };
}

function userItem(
  runId: RunId,
  id: string,
  ordinal: number,
  text: string,
): OrchestrationV2TurnItem {
  return {
    ...item(runId, id, ordinal),
    type: "user_message",
    messageId: MessageId.make(id),
    text,
    attachments: [],
    inputIntent: "turn_start",
    createdBy: "user",
    creationSource: "web",
  } satisfies OrchestrationV2TurnItem;
}

function reasoningItem(
  runId: RunId,
  id: string,
  ordinal: number,
  text: string,
): OrchestrationV2TurnItem {
  return {
    ...item(runId, id, ordinal),
    type: "reasoning",
    text,
    streaming: false,
  } satisfies OrchestrationV2TurnItem;
}

function assistantItem(
  runId: RunId,
  id: string,
  ordinal: number,
  text: string,
): OrchestrationV2TurnItem {
  return {
    ...item(runId, id, ordinal),
    type: "assistant_message",
    messageId: MessageId.make(id),
    text,
    streaming: false,
  } satisfies OrchestrationV2TurnItem;
}

function questionItem(runId: RunId, ordinal: number): OrchestrationV2TurnItem {
  return {
    ...item(runId, "question", ordinal),
    type: "user_input_request",
    requestId,
    questions: [],
  } satisfies OrchestrationV2TurnItem;
}

function rows(items: ReadonlyArray<OrchestrationV2TurnItem>): OrchestrationV2ProjectedTurnItem[] {
  return items.map((entry, position) => ({
    position,
    visibility: "local",
    sourceThreadId: threadId,
    sourceItemId: entry.id,
    item: entry,
  }));
}

describe("questionPreamble", () => {
  it("returns the trimmed reasoning that explains a question with no visible text", () => {
    const preamble = questionPreamble(
      rows([
        userItem(runA, "user", 0, "Ship the migration."),
        reasoningItem(runA, "reasoning", 1, "  I need to know which database to target first.  "),
        questionItem(runA, 2),
      ]),
      requestId,
    );
    expect(preamble).toBe("I need to know which database to target first.");
  });

  it("returns null when visible assistant text sits between the reasoning and the question", () => {
    const preamble = questionPreamble(
      rows([
        userItem(runA, "user", 0, "Ship the migration."),
        reasoningItem(runA, "reasoning", 1, "I need to know which database to target first."),
        assistantItem(runA, "assistant", 2, "Which database should the migration target?"),
        questionItem(runA, 3),
      ]),
      requestId,
    );
    expect(preamble).toBeNull();
  });

  it("returns null when the run raised the question without any reasoning", () => {
    const preamble = questionPreamble(
      rows([userItem(runA, "user", 0, "Ship the migration."), questionItem(runA, 1)]),
      requestId,
    );
    expect(preamble).toBeNull();
  });

  it("uses only the nearest of several reasoning items", () => {
    const preamble = questionPreamble(
      rows([
        userItem(runA, "user", 0, "Ship the migration."),
        reasoningItem(runA, "reasoning-1", 1, "First I will list the migration files."),
        reasoningItem(runA, "reasoning-2", 2, "I should ask which database to target first."),
        questionItem(runA, 3),
      ]),
      requestId,
    );
    expect(preamble).toBe("I should ask which database to target first.");
  });

  it("returns null for reasoning from an earlier run", () => {
    const preamble = questionPreamble(
      rows([
        userItem(runA, "user-a", 0, "Ship the migration."),
        reasoningItem(runA, "reasoning-a", 1, "I need to know which database to target first."),
        questionItem(runB, 2),
      ]),
      requestId,
    );
    expect(preamble).toBeNull();
  });

  it("returns null for reasoning that precedes an earlier user message of the run", () => {
    const preamble = questionPreamble(
      rows([
        userItem(runA, "user", 0, "Ship the migration."),
        reasoningItem(runA, "reasoning", 1, "I need to know which database to target first."),
        userItem(runA, "steer", 2, "Also update the docs."),
        questionItem(runA, 3),
      ]),
      requestId,
    );
    expect(preamble).toBeNull();
  });

  it("returns null for whitespace-only reasoning", () => {
    const preamble = questionPreamble(
      rows([
        userItem(runA, "user", 0, "Ship the migration."),
        reasoningItem(runA, "reasoning", 1, " \n\t "),
        questionItem(runA, 2),
      ]),
      requestId,
    );
    expect(preamble).toBeNull();
  });

  it("returns null when no question item matches the request", () => {
    const preamble = questionPreamble(
      rows([userItem(runA, "user", 0, "Ship the migration.")]),
      requestId,
    );
    expect(preamble).toBeNull();
  });
});
