import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import {
  droidTranscriptEntries,
  droidTranscriptRecords,
  findDroidSessionTranscript,
  isDroidTranscriptHistoryRecord,
  type DroidTranscriptEntry,
  type DroidTranscriptRecord,
} from "./sessionTranscripts.ts";

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
const thinking = (value: string) => ({ type: "thinking", thinking: value, signature: "sig" });

const summary = (entry: DroidTranscriptEntry) =>
  entry.type === "message"
    ? [entry.role, entry.text]
    : entry.activity.kind === "tool"
      ? [entry.activity.item.type, entry.activity.item.status]
      : entry.activity.kind === "subagent"
        ? ["subagent", entry.activity.title, entry.activity.status, entry.activity.result]
        : entry.activity.kind === "reasoning"
          ? ["reasoning", entry.activity.text]
          : ["notice", entry.activity.message];

describe("droidTranscriptEntries", () => {
  it("pairs tool calls with their results and Tasks with their reports, in transcript order", () => {
    const record = recorder();
    const entries = droidTranscriptEntries([
      record("u1", "user", text("Why does CI fail?")),
      record("a1", "assistant", [
        thinking("hidden"),
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

    assert.deepStrictEqual(entries.map(summary), [
      ["user", "Why does CI fail?"],
      ["reasoning", "hidden"],
      ["assistant", "Checking."],
      ["command_execution", "completed"],
      ["file_change", "failed"],
      ["subagent", "Read logs", "completed", "The logs show a timeout."],
      ["subagent", "Find flake", "completed", "setup races the server"],
      ["subagent", "Never reported", "interrupted", null],
      ["dynamic_tool", "interrupted"],
      ["assistant", "A race in setup."],
    ]);
    const execute = entries[3];
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
    const background = entries[6];
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

  it("keeps the model's thinking ahead of its reply and skips signature-only blocks", () => {
    const record = recorder();
    const records = [
      record("u1", "user", text("Plan it")),
      record("a1", "assistant", [thinking("First idea."), thinking("Second idea.")]),
      record("a2", "assistant", [thinking("  "), ...text("Done.")]),
      record("a3", "assistant", [thinking("")]),
    ];

    assert.deepStrictEqual(
      records.map((entry) => isDroidTranscriptHistoryRecord(entry)),
      [true, true, true, false],
    );
    assert.deepStrictEqual(droidTranscriptEntries(records).map(summary), [
      ["user", "Plan it"],
      ["reasoning", "First idea."],
      ["reasoning", "Second idea."],
      ["assistant", "Done."],
    ]);
  });
});

describe("Droid transcript files", () => {
  it("reads a transcript's records and skips lines that are not one", () => {
    const records = droidTranscriptRecords(
      [
        JSON.stringify({ type: "session_start", id: "s-1", cwd: "/repo" }),
        "not json",
        "",
        JSON.stringify(recorder()("m-1", "user", text("Hi"))),
      ].join("\n"),
    );
    assert.deepStrictEqual(
      records.map((record) => [record.type, record.id]),
      [
        ["session_start", "s-1"],
        ["message", "m-1"],
      ],
    );
  });

  it.effect("finds a session in any folder or at the top, and nothing outside them", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const home = yield* fs.makeTempDirectoryScoped({ prefix: "t3-droid-home-" });
      const sessions = path.join(home, ".factory", "sessions");
      yield* fs.makeDirectory(path.join(sessions, "-private-tmp"), { recursive: true });
      yield* fs.writeFileString(path.join(sessions, "-private-tmp", "child-1.jsonl"), "");
      yield* fs.writeFileString(path.join(sessions, "top-1.jsonl"), "");
      yield* fs.writeFileString(path.join(home, "outside.jsonl"), "");

      assert.strictEqual(
        yield* findDroidSessionTranscript(home, "child-1"),
        path.join(sessions, "-private-tmp", "child-1.jsonl"),
      );
      assert.strictEqual(
        yield* findDroidSessionTranscript(home, "top-1"),
        path.join(sessions, "top-1.jsonl"),
      );
      assert.isUndefined(yield* findDroidSessionTranscript(home, "missing"));
      assert.isUndefined(yield* findDroidSessionTranscript(home, "../../outside"));
      assert.isUndefined(yield* findDroidSessionTranscript(path.join(home, "nobody"), "child-1"));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
