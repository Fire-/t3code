import { assert, describe, it } from "@effect/vitest";

import { droidTranscriptEntries, type DroidTranscriptRecord } from "./sessionTranscripts.ts";

const text = (value: string) => [{ type: "text", text: value }];

/** Records one second apart, starting at 10:00:00. */
const recorder = () => {
  let second = 0;
  return (
    id: string,
    role: "user" | "assistant",
    content: ReadonlyArray<Record<string, unknown>>,
    visibility?: string,
  ): DroidTranscriptRecord => ({
    type: "message",
    id,
    timestamp: `2026-08-24T10:00:${String(second++).padStart(2, "0")}.000Z`,
    message: { role, content, ...(visibility === undefined ? {} : { visibility }) },
  });
};

const toolUse = (id: string, name: string, input: unknown) => ({
  type: "tool_use",
  id,
  name,
  input,
});
const toolResult = (id: string, content: unknown, isError = false) => ({
  type: "tool_result",
  tool_use_id: id,
  content,
  ...(isError ? { is_error: true } : {}),
});

describe("droidTranscriptEntries", () => {
  it("pairs tool calls with their results and Tasks with their reports, in transcript order", () => {
    const record = recorder();
    const entries = droidTranscriptEntries([
      record("u1", "user", text("Why does CI fail?")),
      record("a1", "assistant", [
        { type: "thinking", thinking: "hidden" },
        ...text("Checking."),
        toolUse("t-exec", "Execute", { command: "vp test" }),
        toolUse("t-edit", "Edit", { file_path: "/w/a.ts", old_str: "a", new_str: "b" }),
      ]),
      record("r1", "user", [
        toolResult("t-exec", "1 failed\n\n[Process exited with code 1]"),
        toolResult("t-edit", "old_str not found", true),
      ]),
      record("a2", "assistant", [
        toolUse("t-awaited", "Task", { description: "Read logs", prompt: "Read the CI logs" }),
        toolUse("t-background", "Task", { description: "Find flake", prompt: "Find the flake" }),
        toolUse("t-lost", "Task", { description: "Never reported", prompt: "Lost" }),
        toolUse("t-unanswered", "Grep", { pattern: "race" }),
      ]),
      record("r2", "user", [
        toolResult("t-awaited", "session_id: s1\nThe logs show a timeout."),
        toolResult(
          "t-background",
          "task_id: task_9\nsession_id: s2\nsubagent_type: worker\ndescription: Find flake",
        ),
        toolResult("t-lost", "task_id: task_lost\nsession_id: s3"),
      ]),
      record(
        "task-completion:1",
        "user",
        text("Background task completed.\ntask_id: task_9\noutput: setup races the server"),
        "llm_only",
      ),
      record("a3", "assistant", text("A race in setup.")),
    ]);

    assert.deepStrictEqual(
      entries.map((entry) =>
        entry.type === "message"
          ? [entry.role, entry.text]
          : entry.activity.kind === "tool"
            ? [entry.activity.item.type, entry.activity.item.status]
            : entry.activity.kind === "subagent"
              ? ["subagent", entry.activity.title, entry.activity.status, entry.activity.result]
              : ["notice", entry.activity.message],
      ),
      [
        ["user", "Why does CI fail?"],
        ["assistant", "Checking."],
        ["command_execution", "completed"],
        ["file_change", "failed"],
        ["subagent", "Read logs", "completed", "The logs show a timeout."],
        ["subagent", "Find flake", "completed", "setup races the server"],
        ["subagent", "Never reported", "interrupted", null],
        ["dynamic_tool", "interrupted"],
        ["assistant", "A race in setup."],
      ],
    );
    const execute = entries[2];
    assert.deepStrictEqual(execute?.type === "activity" ? execute.activity : undefined, {
      kind: "tool",
      item: {
        status: "completed",
        title: "Execute",
        type: "command_execution",
        input: "vp test",
        output: "1 failed",
        exitCode: 1,
      },
    });
    assert.deepStrictEqual(
      execute?.type === "activity" ? [execute.startedAt, execute.completedAt] : undefined,
      ["2026-08-24T10:00:01.000Z", "2026-08-24T10:00:02.000Z"],
    );
    const background = entries[5];
    assert.strictEqual(
      background?.type === "activity" ? background.completedAt : undefined,
      "2026-08-24T10:00:05.000Z",
    );
  });

  it("keeps Droid's own notices out of the user's messages", () => {
    const record = recorder();
    const entries = droidTranscriptEntries([
      record("u1", "user", text("login")),
      record("n1", "user", text("BYOK Error: 405"), "user_only"),
      record("hook", "user", [], "user_only"),
      record("u2", "user", text("Try again")),
    ]);

    assert.deepStrictEqual(
      entries.map((entry) =>
        entry.type === "message" ? [entry.role, entry.text] : [entry.activity.kind],
      ),
      [["user", "login"], ["notice"], ["user", "Try again"]],
    );
    const notice = entries[1];
    assert.deepStrictEqual(notice?.type === "activity" ? notice.activity : undefined, {
      kind: "notice",
      message: "BYOK Error: 405",
    });
  });
});
