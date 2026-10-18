import { assert, describe, it } from "@effect/vitest";
import * as Option from "effect/Option";

import {
  DROID_API_VERSION,
  DROID_ASK_USER_CANCELLED,
  DROID_NOTIFICATION_TYPES,
  DROID_PICK_OPTIONS_CANCELLED,
  DROID_PROTOCOL_VERSION,
  DROID_TOOL_OUTPUT_MAX_CHARS,
  awaitedTaskReport,
  boundDroidToolOutput,
  decodeDroidFrame,
  decodeDroidNotificationParams,
  droidAskUserQuestionsFromParams,
  droidEditDiffCounts,
  droidMessageText,
  droidTaskCompletionOf,
  droidTaskResultReport,
  droidTokenCounts,
  droidToolKind,
  droidToolPath,
  droidToolResultOutput,
  droidToolSummary,
  isDroidInboundRequestMethod,
  isDroidNotificationType,
  isZeroDroidUsage,
  parseDroidTaskCompletion,
  splitDroidExecuteOutput,
  subtractDroidCounts,
} from "./protocol.ts";

describe("versions", () => {
  it("is built against factoryApiVersion 1.0.0 and factoryProtocolVersion 1.269.0", () => {
    assert.strictEqual(DROID_API_VERSION, "1.0.0");
    assert.strictEqual(DROID_PROTOCOL_VERSION, "1.269.0");
  });
});

describe("frame decoding", () => {
  it("decodes responses, inbound requests, and notifications by `type`", () => {
    const response = decodeDroidFrame({
      type: "response",
      id: "7",
      factoryProtocolVersion: "1.269.0",
      result: { ok: true },
    });
    assert.deepStrictEqual(
      Option.map(response, (frame) => frame.type),
      Option.some("response"),
    );
    const request = decodeDroidFrame({
      type: "request",
      id: "p-1",
      method: "droid.request_permission",
      params: {},
    });
    assert.deepStrictEqual(
      Option.map(request, (frame) => frame.type),
      Option.some("request"),
    );
    const notification = decodeDroidFrame({
      type: "notification",
      method: "droid.session_notification",
      params: { notification: { type: "tool_call" } },
    });
    assert.deepStrictEqual(
      Option.map(notification, (frame) => frame.type),
      Option.some("notification"),
    );
  });

  it("accepts a null response id (Droid's answer to an envelope it could not parse)", () => {
    assert.isTrue(
      Option.isSome(
        decodeDroidFrame({ type: "response", id: null, error: { code: -32700, message: "Parse" } }),
      ),
    );
  });

  it("rejects frames with an unknown type, a non-string request id, or no method", () => {
    assert.isTrue(Option.isNone(decodeDroidFrame({ type: "event" })));
    assert.isTrue(Option.isNone(decodeDroidFrame({ type: "request", id: 5, method: "m" })));
    assert.isTrue(Option.isNone(decodeDroidFrame({ type: "request", id: "5" })));
    assert.isTrue(Option.isNone(decodeDroidFrame({ type: "notification" })));
    assert.isTrue(Option.isNone(decodeDroidFrame("not an object")));
    assert.isTrue(Option.isNone(decodeDroidFrame(null)));
  });

  it("keeps every extra field of a notification body and of its params", () => {
    const decoded = decodeDroidNotificationParams({
      sessionId: "s-1",
      notification: { type: "assistant_text_delta", messageId: "m-1", textDelta: "hi" },
    });
    assert.isTrue(Option.isSome(decoded));
    if (Option.isNone(decoded)) return;
    assert.strictEqual(decoded.value["sessionId"], "s-1");
    assert.deepStrictEqual(decoded.value.notification, {
      type: "assistant_text_delta",
      messageId: "m-1",
      textDelta: "hi",
    });
  });

  it("rejects notification params without a typed notification", () => {
    assert.isTrue(Option.isNone(decodeDroidNotificationParams({ sessionId: "s-1" })));
    assert.isTrue(Option.isNone(decodeDroidNotificationParams({ notification: {} })));
    assert.isTrue(Option.isNone(decodeDroidNotificationParams({ notification: { type: 3 } })));
    assert.isTrue(Option.isNone(decodeDroidNotificationParams(undefined)));
  });
});

describe("allowlists", () => {
  it("treats the three inbound methods as known and everything else as unknown", () => {
    for (const method of ["droid.request_permission", "droid.ask_user", "droid.pick_options"]) {
      assert.isTrue(isDroidInboundRequestMethod(method));
    }
    assert.isFalse(isDroidInboundRequestMethod("droid.future_request"));
    assert.isFalse(isDroidInboundRequestMethod("droid.add_user_message"));
  });

  it("recognizes the consumed notification types and drops mission and future types", () => {
    for (const type of DROID_NOTIFICATION_TYPES) {
      assert.isTrue(isDroidNotificationType(type), type);
    }
    assert.isTrue(isDroidNotificationType("agent_turn_completed"));
    assert.isFalse(isDroidNotificationType("mission_state_changed"));
    assert.isFalse(isDroidNotificationType("hook_execution_started"));
    assert.isFalse(isDroidNotificationType("brand_new_event"));
    assert.isFalse(isDroidNotificationType(""));
  });
});

describe("tool parsers", () => {
  it("bounds tool output to one named constant, keeping the end", () => {
    assert.isAtMost(DROID_TOOL_OUTPUT_MAX_CHARS, 8_192);
    assert.strictEqual(boundDroidToolOutput("short"), "short");
    const bounded = boundDroidToolOutput(`HEAD${"x".repeat(20_000)}TAIL`);
    assert.isAtMost(bounded.length, DROID_TOOL_OUTPUT_MAX_CHARS);
    assert.isTrue(bounded.endsWith("TAIL"));
    assert.isFalse(bounded.includes("HEAD"));
  });

  it("maps Droid tool names to a kind", () => {
    const table: ReadonlyArray<readonly [string, string]> = [
      ["Execute", "execute"],
      ["Edit", "edit"],
      ["MultiEdit", "edit"],
      ["Create", "edit"],
      ["ApplyPatch", "edit"],
      ["Read", "read"],
      ["LS", "read"],
      ["Grep", "search"],
      ["Glob", "search"],
      ["ToolSearch", "search"],
      ["WebSearch", "search"],
      ["FetchUrl", "fetch"],
      ["Task", "task"],
      ["mini___slow_count", "unknown"],
    ];
    for (const [name, kind] of table) {
      assert.deepStrictEqual([name, droidToolKind(name)], [name, kind]);
    }
  });

  it("titles a tool row from the first meaningful input field and finds its path", () => {
    assert.strictEqual(droidToolSummary({ command: "  ls -la  ", path: "/x" }), "ls -la");
    assert.strictEqual(droidToolSummary({ command: "  ", file_path: "/a.txt" }), "/a.txt");
    assert.strictEqual(droidToolSummary({ unrelated: 1 }), undefined);
    assert.strictEqual(droidToolSummary("not an object"), undefined);
    assert.strictEqual(droidToolSummary({ command: "x".repeat(5_000) })?.length, 2_000);
    assert.strictEqual(droidToolPath({ file_path: "/a.txt", path: "/b.txt" }), "/a.txt");
    assert.strictEqual(droidToolPath({ path: "/b.txt" }), "/b.txt");
    assert.strictEqual(droidToolPath({}), undefined);
  });

  it("handles plain-string, JSON-string, and array tool_result content", () => {
    const cases: ReadonlyArray<{
      readonly content: unknown;
      readonly expected: { text?: string; stdout?: string; stderr?: string };
    }> = [
      { content: "autonomy-high-test\n", expected: { text: "autonomy-high-test\n" } },
      {
        content: '{"success":true,"file_path":"/workspace/out.txt"}',
        expected: { text: '{"success":true,"file_path":"/workspace/out.txt"}' },
      },
      {
        content: '{"stdout":"built","stderr":"warn"}',
        expected: { text: '{"stdout":"built","stderr":"warn"}', stdout: "built", stderr: "warn" },
      },
      { content: "{not json", expected: { text: "{not json" } },
      {
        content: [
          { type: "text", text: "first" },
          { type: "image", data: "…" },
          { type: "text", text: "second" },
        ],
        expected: { text: "first\nsecond" },
      },
      { content: [], expected: {} },
      { content: "   ", expected: {} },
      { content: undefined, expected: {} },
      { content: { structured: true }, expected: { text: '{"structured":true}' } },
    ];
    for (const { content, expected } of cases) {
      const output = droidToolResultOutput(content);
      assert.deepStrictEqual(
        { text: output.text, stdout: output.stdout, stderr: output.stderr },
        { text: expected.text, stdout: expected.stdout, stderr: expected.stderr },
      );
    }
  });

  it("joins the text blocks of a message and passes plain strings through", () => {
    assert.strictEqual(droidMessageText("plain"), "plain");
    assert.strictEqual(
      droidMessageText([
        { type: "text", text: "one" },
        { type: "tool_use", id: "t" },
        { type: "text", text: "two" },
      ]),
      "one\ntwo",
    );
    assert.strictEqual(droidMessageText(undefined), "");
  });

  it("splits the exit-code footer off an Execute result", () => {
    assert.deepStrictEqual(splitDroidExecuteOutput("hello\n\n\n[Process exited with code 0]"), {
      output: "hello",
      exitCode: 0,
    });
    assert.deepStrictEqual(splitDroidExecuteOutput("oops\n\n\n[Process exited with code 127]"), {
      output: "oops",
      exitCode: 127,
    });
    assert.deepStrictEqual(splitDroidExecuteOutput("\n\n\n[Process exited with code 1]"), {
      output: undefined,
      exitCode: 1,
    });
    assert.deepStrictEqual(splitDroidExecuteOutput("no footer here"), {
      output: "no footer here",
      exitCode: undefined,
    });
    assert.deepStrictEqual(
      splitDroidExecuteOutput("echo [Process exited with code 3] is literal text"),
      { output: "echo [Process exited with code 3] is literal text", exitCode: undefined },
    );
    assert.deepStrictEqual(splitDroidExecuteOutput(undefined), {
      output: undefined,
      exitCode: undefined,
    });
  });

  it("counts added and removed lines in an Edit result's diffLines", () => {
    const edit = JSON.stringify({
      success: true,
      diffLines: [
        { type: "unchanged", content: "keep", lineNumber: 1 },
        { type: "removed", content: "old", lineNumber: 2 },
        { type: "added", content: "new one", lineNumber: 2 },
        { type: "added", content: "new two", lineNumber: 3 },
      ],
    });
    assert.deepStrictEqual(droidEditDiffCounts(edit), { additions: 2, deletions: 1 });
    assert.isUndefined(droidEditDiffCounts("Error: Failed to edit file"));
    assert.isUndefined(droidEditDiffCounts(JSON.stringify({ success: true })));
    assert.isUndefined(droidEditDiffCounts(undefined));
  });

  it("strips Droid's framing from an awaited Task report", () => {
    assert.strictEqual(
      awaitedTaskReport(
        "session_id: child-1\nUse this report as the source of record for the delegated work.\n\nthe report\n",
      ),
      "the report",
    );
    assert.strictEqual(awaitedTaskReport("just a report"), "just a report");
    assert.strictEqual(awaitedTaskReport("   "), undefined);
    assert.strictEqual(awaitedTaskReport(undefined), undefined);
  });

  it("strips the framing from a Task result before bounding it, whatever shape the content has", () => {
    const framed = (report: string) =>
      `session_id: child-1\nUse this report as the source of record for the delegated work.\n\n${report}`;
    assert.strictEqual(droidTaskResultReport(framed("the report")), "the report");
    assert.strictEqual(
      droidTaskResultReport([{ type: "text", text: framed("block report") }]),
      "block report",
    );
    const bounded = droidTaskResultReport(framed("x".repeat(DROID_TOOL_OUTPUT_MAX_CHARS * 2)));
    assert.isDefined(bounded);
    assert.isFalse(bounded?.includes("session_id") ?? true);
    assert.isTrue((bounded?.length ?? Infinity) <= DROID_TOOL_OUTPUT_MAX_CHARS);
    assert.isUndefined(droidTaskResultReport(undefined));
    assert.isUndefined(droidTaskResultReport(framed("  ")));
  });
});

describe("background task completion", () => {
  const completion = (input: {
    readonly taskId: string;
    readonly word: "completed" | "stopped" | "error";
    readonly output: string;
    readonly failure?: string;
  }) =>
    [
      `Background task ${input.word}.`,
      `task_id: ${input.taskId}`,
      "type: probe-slow",
      "reason: process_exit",
      "description: bg",
      ...(input.failure ? [`failure: ${input.failure}`] : []),
      `output: ${input.output}`,
      "",
      `Background task "bg" (${input.taskId}) just ${input.word}. This is a new asynchronous completion event.`,
    ].join("\n");

  const message = (id: string, role: string, text: string) => ({
    message: { id, role, content: [{ type: "text", text }] },
  });

  it("reads the completion out of Droid's injected message, by its id prefix", () => {
    const text = completion({ taskId: "child-1", word: "completed", output: "sub-ok" });
    const expected = { taskId: "child-1", status: "completed" as const, summary: "sub-ok" };
    assert.deepStrictEqual(
      droidTaskCompletionOf(message("task-completion:abc", "system", text)),
      expected,
    );
    // A stored history reads the same message back as a user message.
    assert.deepStrictEqual(
      droidTaskCompletionOf(message("task-completion:abc", "user", text)),
      expected,
    );
    assert.isUndefined(droidTaskCompletionOf(message("a-user-message", "system", text)));
    assert.isUndefined(
      droidTaskCompletionOf(message("task-completion:abc", "system", "not a report")),
    );
    assert.isUndefined(droidTaskCompletionOf({}));
  });

  it("reads a completed report and drops the trailing instructions to the model", () => {
    assert.deepStrictEqual(
      parseDroidTaskCompletion(
        completion({ taskId: "child-1", word: "completed", output: "line one\nline two" }),
      ),
      { taskId: "child-1", status: "completed", summary: "line one\nline two" },
    );
  });

  it("reads failed and stopped reports from the CLI template", () => {
    assert.deepStrictEqual(
      parseDroidTaskCompletion(
        completion({
          taskId: "child-1",
          word: "error",
          failure: "The child process exited before completing the turn.",
          output: "No output available",
        }),
      ),
      {
        taskId: "child-1",
        status: "failed",
        summary: "The child process exited before completing the turn.",
      },
    );
    assert.deepStrictEqual(
      parseDroidTaskCompletion(
        completion({ taskId: "child-2", word: "stopped", output: "No output available" }),
      ),
      { taskId: "child-2", status: "stopped", summary: undefined },
    );
  });

  it("returns nothing for text that is not a completion report", () => {
    assert.isUndefined(parseDroidTaskCompletion("hello"));
    assert.isUndefined(parseDroidTaskCompletion("Background task completed.\nno id here"));
  });
});

describe("usage counts", () => {
  it("reads token counts, treating missing, negative, and non-finite values as 0", () => {
    assert.deepStrictEqual(
      droidTokenCounts({
        inputTokens: 10.9,
        outputTokens: -3,
        cacheCreationTokens: Number.POSITIVE_INFINITY,
        cacheReadTokens: "7",
      }),
      {
        inputTokens: 10,
        outputTokens: 0,
        cacheCreationTokens: 0,
        cacheReadTokens: 0,
        thinkingTokens: 0,
      },
    );
    assert.isUndefined(droidTokenCounts("nope"));
  });

  it("detects all-zero usage and subtracts per field, clamping at 0", () => {
    const zero = droidTokenCounts({});
    const some = droidTokenCounts({ inputTokens: 5, outputTokens: 2 });
    assert.isDefined(zero);
    assert.isDefined(some);
    if (zero === undefined || some === undefined) return;
    assert.isTrue(isZeroDroidUsage(zero));
    assert.isFalse(isZeroDroidUsage(some));

    const exact = subtractDroidCounts(some, droidTokenCounts({ inputTokens: 2 }) ?? zero);
    assert.deepStrictEqual(exact, {
      counts: { ...zero, inputTokens: 3, outputTokens: 2 },
      clamped: false,
    });
    const clamped = subtractDroidCounts(some, droidTokenCounts({ inputTokens: 9 }) ?? zero);
    assert.deepStrictEqual(clamped, {
      counts: { ...zero, inputTokens: 0, outputTokens: 2 },
      clamped: true,
    });
  });
});

describe("ask_user questions", () => {
  const params = {
    toolCallId: "tool-ask-1",
    questions: [
      {
        index: 1,
        topic: "Favorite-Color",
        question: "What is your favorite color?",
        options: ["Red", "Blue"],
      },
      {
        index: 2,
        topic: "Letters",
        question: "Which letters do you like?",
        options: ["a", "b", "c"],
        multiSelect: true,
      },
    ],
  };

  it("reads questions keyed by their wire index, with options and multi-select", () => {
    assert.deepStrictEqual(droidAskUserQuestionsFromParams(params), [
      {
        index: 1,
        topic: "Favorite-Color",
        question: "What is your favorite color?",
        options: ["Red", "Blue"],
        multiSelect: false,
      },
      {
        index: 2,
        topic: "Letters",
        question: "Which letters do you like?",
        options: ["a", "b", "c"],
        multiSelect: true,
      },
    ]);
  });

  it("falls back to position for a missing index and to the topic for a missing question", () => {
    assert.deepStrictEqual(
      droidAskUserQuestionsFromParams({
        questions: [{ topic: "Only topic", options: [" x ", "", 3] }, "junk", { question: "  " }],
      }),
      [
        {
          index: 1,
          topic: "Only topic",
          question: "Only topic",
          options: ["x"],
          multiSelect: false,
        },
      ],
    );
    assert.deepStrictEqual(droidAskUserQuestionsFromParams({}), []);
    assert.deepStrictEqual(droidAskUserQuestionsFromParams(undefined), []);
  });

  it("has the cancel shapes Droid expects for ask_user and pick_options", () => {
    assert.deepStrictEqual(DROID_ASK_USER_CANCELLED, { cancelled: true, answers: [] });
    assert.deepStrictEqual(DROID_PICK_OPTIONS_CANCELLED, { selectedIds: [], cancelled: true });
  });
});
