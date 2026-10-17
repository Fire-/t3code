import { assert, describe, it } from "@effect/vitest";

import {
  primeAgentEndErrorMessage,
  primeAutonomousContinuation,
  primeAutonomousNotice,
  primeHeartbeatNotification,
  primeIpythonFileChange,
  primeProviderGoal,
  primeIpythonUpdate,
  primeToolItem,
} from "./events.ts";

describe("primeAgentEndErrorMessage", () => {
  it("[VAL-TURN-009] takes the first non-blank errorMessage among agent_end messages", () => {
    const errorMessage =
      'Mistral API error (400): {"message":"Invalid request payload: Expected at most one leading ThinkChunk, got 2","code":"3051"}';
    assert.equal(
      primeAgentEndErrorMessage({
        type: "agent_end",
        messages: [
          { role: "user", content: "did something happen?" },
          { role: "assistant", content: [], errorMessage: "  " },
          { role: "assistant", content: [], stopReason: "error", errorMessage },
          { role: "assistant", content: [], stopReason: "error", errorMessage: "later error" },
        ],
      }),
      errorMessage,
    );
  });

  it.each([
    { name: "a clean stop", messages: [{ role: "assistant", content: [], stopReason: "stop" }] },
    { name: "no messages", messages: [] },
    {
      name: "a whitespace-only errorMessage",
      messages: [{ role: "assistant", errorMessage: "  " }],
    },
    { name: "a missing messages array", messages: undefined },
  ])("[VAL-TURN-009] gives no detail for $name", ({ messages }) => {
    assert.isUndefined(primeAgentEndErrorMessage({ type: "agent_end", messages }));
  });
});

describe("primeToolItem", () => {
  const ipythonCode = "import pathlib\nprint(pathlib.Path('a.py').read_text())\n1 + 1";

  it("[VAL-TOOL-001] maps ipython to a command execution with the code as command and output from details", () => {
    assert.deepEqual(
      primeToolItem({
        toolName: "ipython",
        args: { code: ipythonCode },
        isError: false,
        result: {
          content: [{ type: "text", text: "content text is not preferred" }],
          details: { status: "ok", stdout: "hello\n", stderr: "warning: slow\n", result: "2" },
        },
      }),
      { type: "command_execution", input: ipythonCode, output: "hello\nwarning: slow\n2" },
    );
  });

  it("[VAL-TOOL-001] falls back to the result text when ipython details carry no output", () => {
    assert.deepEqual(
      primeToolItem({
        toolName: "ipython",
        args: { code: "print(h)" },
        isError: false,
        result: { content: [{ type: "text", text: "BashResult(exit_code=0)" }] },
      }),
      { type: "command_execution", input: "print(h)", output: "BashResult(exit_code=0)" },
    );
  });

  it("[VAL-TOOL-003] a failed ipython cell is a failed command execution that shows the error text", () => {
    const item = primeToolItem({
      toolName: "ipython",
      args: { code: "1 / 0" },
      isError: true,
      result: {
        content: [{ type: "text", text: "ignored" }],
        details: {
          status: "error",
          stdout: "before\n",
          errorEname: "ZeroDivisionError",
          error: {
            ename: "ZeroDivisionError",
            evalue: "division by zero",
            traceback: [
              "Traceback (most recent call last):",
              '  File "<cell>", line 1, in <module>',
              "ZeroDivisionError: division by zero",
            ],
          },
        },
      },
    });
    assert.equal(item?.type, "command_execution");
    if (item?.type !== "command_execution") return;
    assert.equal(item.status, "failed");
    assert.equal(
      item.output,
      'before\nTraceback (most recent call last):\n  File "<cell>", line 1, in <module>\nZeroDivisionError: division by zero',
    );
  });

  it("[VAL-TOOL-003] a failed ipython cell without a traceback shows ename and evalue", () => {
    const item = primeToolItem({
      toolName: "ipython",
      args: { code: "kernel()" },
      isError: true,
      result: {
        details: {
          status: "error",
          error: { ename: "KernelError", evalue: "kernel died", traceback: [] },
        },
      },
    });
    assert.deepEqual(item, {
      type: "command_execution",
      input: "kernel()",
      output: "KernelError: kernel died",
      status: "failed",
    });
  });

  it("[VAL-TOOL-001] an aborted ipython cell is interrupted", () => {
    const item = primeToolItem({
      toolName: "ipython",
      args: { code: "await asyncio.sleep(60)" },
      isError: true,
      result: { details: { status: "aborted", stdout: "started\n" } },
    });
    assert.deepEqual(item, {
      type: "command_execution",
      input: "await asyncio.sleep(60)",
      output: "started\n",
      status: "interrupted",
    });
  });

  it.each([
    {
      name: "structured exit code",
      result: {
        content: [{ type: "text", text: "hello" }],
        structuredContent: { exit_code: 2 },
        details: { exitCode: 99 },
      },
      exitCode: 2,
    },
    {
      name: "legacy details exit code",
      result: { content: [{ type: "text", text: "hello" }], details: { exitCode: 0 } },
      exitCode: 0,
    },
  ])("[VAL-TOOL-004] maps bash to a command execution ($name)", ({ result, exitCode }) => {
    assert.deepEqual(
      primeToolItem({ toolName: "bash", args: { command: "echo hello" }, isError: false, result }),
      { type: "command_execution", input: "echo hello", output: "hello", exitCode },
    );
  });

  it("[VAL-TOOL-005] converts an edit's numbered details.diff into a unified patch", () => {
    const diff = [
      "  1 import a",
      "- 2 old line",
      "+ 2 new line",
      "+ 3 added line",
      "  3 keep",
      "    ...",
      " 11 ctx",
      "-12 gone",
      " 13 tail",
    ].join("\n");
    assert.deepEqual(
      primeToolItem({
        toolName: "edit",
        args: { path: "src/a.ts", edits: [{ oldText: "old line", newText: "new line" }] },
        isError: false,
        result: {
          content: [{ type: "text", text: "Successfully replaced 1 block(s) in src/a.ts." }],
          details: { diff, firstChangedLine: 2 },
        },
      }),
      {
        type: "file_change",
        fileName: "src/a.ts",
        diffStr: [
          "--- src/a.ts",
          "+++ src/a.ts",
          "@@ -1,3 +1,4 @@",
          " import a",
          "-old line",
          "+new line",
          "+added line",
          " keep",
          "@@ -11,3 +12,2 @@",
          " ctx",
          "-gone",
          " tail",
          "",
        ].join("\n"),
      },
    );
  });

  it("[VAL-TOOL-005] keeps an edit's ready patch and a failed edit's error text", () => {
    const patch = "--- a.ts\n+++ a.ts\n@@ -1 +1 @@\n-old\n+new\n";
    assert.deepEqual(
      primeToolItem({
        toolName: "edit",
        args: { path: "a.ts" },
        isError: false,
        result: { details: { diff: "-1 old\n+1 new", patch } },
      }),
      { type: "file_change", fileName: "a.ts", diffStr: patch },
    );
    assert.deepEqual(
      primeToolItem({
        toolName: "edit",
        args: { path: "c.ts" },
        isError: true,
        result: { content: [{ type: "text", text: "Could not find the text in c.ts." }] },
      }),
      { type: "file_change", fileName: "c.ts", diffStr: "Could not find the text in c.ts." },
    );
  });

  it("[VAL-TOOL-005] maps write to a file change carrying the new content", () => {
    assert.deepEqual(
      primeToolItem({
        toolName: "write",
        args: { path: "b.ts", content: "export {};\n" },
        isError: false,
        result: { content: [{ type: "text", text: "Successfully wrote to b.ts" }] },
      }),
      { type: "file_change", fileName: "b.ts", newStr: "export {};\n" },
    );
  });

  it.each([
    { name: "camelCase", args: { oldPath: "old/name.ts", newPath: "new/name.ts" } },
    { name: "snake_case", args: { old_path: "old/name.ts", new_path: "new/name.ts" } },
    { name: "from/to", args: { from: "old/name.ts", to: "new/name.ts" } },
  ])("[VAL-TOOL-006] maps rename with both paths ($name) to a file change", ({ args }) => {
    assert.deepEqual(
      primeToolItem({ toolName: "rename", args, isError: false, result: undefined }),
      {
        type: "file_change",
        fileName: "new/name.ts",
        changes: [{ operation: "rename", path: "new/name.ts", oldPath: "old/name.ts" }],
      },
    );
  });

  it.each([
    { name: "no args", args: undefined },
    { name: "only the new path", args: { newPath: "new/name.ts" } },
    { name: "a session rename", args: { name: "my session" } },
  ])("[VAL-TOOL-006] leaves rename with $name to the dynamic tool mapping", ({ args }) => {
    assert.isUndefined(primeToolItem({ toolName: "rename", args, isError: false, result: {} }));
  });

  it.each(["stdout", "image_generate", "mcp__weather__get_weather"])(
    "[VAL-TOOL-007] leaves %s to the dynamic tool mapping",
    (toolName) => {
      assert.isUndefined(
        primeToolItem({
          toolName,
          args: { text: "hello" },
          isError: false,
          result: { content: [{ type: "text", text: "hello" }] },
        }),
      );
    },
  );
});

describe("primeIpythonFileChange", () => {
  it("[VAL-TOOL-002] turns details.diffs into one file change covering each path", () => {
    assert.deepEqual(
      primeIpythonFileChange({
        details: {
          status: "ok",
          diffs: [
            { path: "src/a.py", oldStr: "x = 1\ny = 2\n", newStr: "x = 10\n", startLine: 3 },
            { path: "src/b.py", oldStr: "", newStr: "print('new')\n" },
            { path: "src/a.py", oldStr: "z = 3", newStr: "z = 30", startLine: 9 },
          ],
        },
      }),
      {
        type: "file_change",
        fileName: "src/a.py",
        changes: [
          { operation: "update", path: "src/a.py" },
          { operation: "update", path: "src/b.py" },
        ],
        diffStr: [
          "--- src/a.py",
          "+++ src/a.py",
          "@@ -3,2 +3,1 @@",
          "-x = 1",
          "-y = 2",
          "+x = 10",
          "--- src/b.py",
          "+++ src/b.py",
          "@@ -0,0 +1,1 @@",
          "+print('new')",
          "--- src/a.py",
          "+++ src/a.py",
          "@@ -9,1 +9,1 @@",
          "-z = 3",
          "+z = 30",
          "",
        ].join("\n"),
      },
    );
  });

  it.each([
    { name: "no details", result: { content: [] } },
    { name: "empty diffs", result: { details: { status: "ok", diffs: [] } } },
    { name: "malformed diffs", result: { details: { diffs: [{ path: "a.py" }] } } },
  ])("[VAL-TOOL-002] produces no file change for $name", ({ result }) => {
    assert.isUndefined(primeIpythonFileChange(result));
  });
});

describe("primeIpythonUpdate", () => {
  it("[VAL-TOOL-001] treats the 30 s heartbeat and kernel start-up as progress, not output", () => {
    assert.deepEqual(
      primeIpythonUpdate({
        content: [{ type: "text", text: "partial\n[ipython cell still running (30s elapsed)]" }],
        details: { status: "ok" },
      }),
      { type: "progress" },
    );
    assert.deepEqual(
      primeIpythonUpdate({
        content: [{ type: "text", text: "Starting Python kernel..." }],
        details: { status: "starting" },
      }),
      { type: "progress" },
    );
  });

  it("[VAL-TOOL-001] treats a streamed chunk as new output", () => {
    assert.deepEqual(
      primeIpythonUpdate({
        content: [{ type: "text", text: "line 1\n" }],
        details: { status: "ok" },
      }),
      { type: "output", text: "line 1\n" },
    );
  });
});

describe("primeProviderGoal", () => {
  const goal = (status: string, active = status === "active") => ({
    active,
    status,
    objective: "  ship the release  ",
    tokensUsed: 1_200,
    tokenBudget: 50_000,
    timeUsedSeconds: 42,
    continuationsUsed: 1,
  });

  it.each([
    { status: "active", expected: "active" },
    { status: "paused", expected: "paused" },
    { status: "budget_limited", expected: "budget_limited" },
    { status: "complete", expected: "complete" },
    { status: "error", expected: "blocked" },
  ] as const)("[VAL-CONT-005] maps Prime status $status to $expected", ({ status, expected }) => {
    assert.deepEqual(primeProviderGoal(goal(status)), {
      objective: "ship the release",
      status: expected,
      tokensUsed: 1_200,
      tokenBudget: 50_000,
      timeUsedSeconds: 42,
    });
  });

  it.each([
    { name: "an idle goal", state: goal("idle") },
    {
      name: "an inactive goal without an objective",
      state: {
        active: false,
        status: "paused",
        tokensUsed: 0,
        timeUsedSeconds: 0,
        continuationsUsed: 0,
      },
    },
    { name: "a blank objective", state: { ...goal("active"), objective: "   " } },
  ])("[VAL-CONT-005] maps $name to no goal", ({ state }) => {
    assert.isNull(primeProviderGoal(state));
  });

  it("[VAL-CONT-005] reports a missing token budget as null", () => {
    const { tokenBudget: _tokenBudget, ...unbudgeted } = goal("active");
    assert.isNull(primeProviderGoal(unbudgeted)?.tokenBudget);
  });
});

describe("primeHeartbeatNotification", () => {
  it("[VAL-CONT-009] reads the schedule and prompt of Prime's heartbeat injection", () => {
    assert.deepEqual(
      primeHeartbeatNotification({
        role: "custom",
        customType: "heartbeat_prompt",
        content: "[heartbeat: */5 * * * * run#3]\n\nCheck the build",
        display: true,
        details: { jobId: "job-1", schedule: "*/5 * * * *", status: "active" },
      }),
      {
        source: { kind: "monitor" },
        outcome: "updated",
        summary: "Heartbeat */5 * * * *",
        detail: "Check the build",
      },
    );
  });

  it("[VAL-CONT-009] ignores other custom messages", () => {
    assert.isUndefined(
      primeHeartbeatNotification({ role: "custom", customType: "goal_context", content: "x" }),
    );
  });
});

describe("primeAutonomousContinuation", () => {
  const userText = (text: string, flagged: boolean) => ({
    role: "user",
    content: [{ type: "text", text }],
    ...(flagged ? { isAutonomousContinuation: true } : {}),
  });
  const gateFailed =
    "[autonomous-continuation: gate-failed]\n\nAutonomous quality gate failed (attempt 1/3): `npm test` exited 1.\n\nOutput:\nFAIL src/x.test.ts\n\nContinue working. Fix the failure, then produce terminal evidence.";

  it("[VAL-AUTO-002] reads the gate-failed reason from a flagged continuation", () => {
    assert.deepEqual(primeAutonomousContinuation(userText(gateFailed, true)), {
      reason: "gate-failed",
      title: "Autonomous gate failed",
      text: gateFailed,
    });
  });

  it("[VAL-AUTO-002] a plain marker is a generic continuation", () => {
    const text = "[autonomous-continuation]\n\nNo human input is available in autonomous mode.";
    assert.deepEqual(primeAutonomousContinuation(userText(text, true)), {
      reason: null,
      title: "Autonomous continuation",
      text,
    });
  });

  it("[VAL-AUTO-002] the text marker without the flag is still a continuation", () => {
    assert.equal(primeAutonomousContinuation(userText(gateFailed, false))?.reason, "gate-failed");
    assert.deepEqual(
      primeAutonomousContinuation({
        role: "user",
        content: "[autonomous-continuation: subagent-keep-alive]\n\nCheck on the subagents.",
      }),
      {
        reason: "subagent-keep-alive",
        title: "Autonomous continuation",
        text: "[autonomous-continuation: subagent-keep-alive]\n\nCheck on the subagents.",
      },
    );
  });

  it("[VAL-AUTO-002] the flag alone marks a continuation without a text marker", () => {
    assert.deepEqual(primeAutonomousContinuation(userText("Keep going.", true)), {
      reason: null,
      title: "Autonomous continuation",
      text: "Keep going.",
    });
  });

  it("[VAL-AUTO-002] ignores ordinary user messages and other roles", () => {
    assert.isUndefined(primeAutonomousContinuation(userText("Hello prime-agent", false)));
    assert.isUndefined(
      primeAutonomousContinuation(userText("see [autonomous-continuation] above", false)),
    );
    assert.isUndefined(
      primeAutonomousContinuation({
        role: "assistant",
        content: [{ type: "text", text: "[autonomous-continuation]" }],
      }),
    );
  });
});

describe("primeAutonomousNotice", () => {
  it.each([
    ["autonomous_gate_started", "Autonomous gate: running `npm test` (attempt 1/3)…"],
    ["autonomous_gate_failed", "Autonomous gate failed (attempt 1/3): `npm test` exited 1."],
    ["autonomous_gate_passed", "Autonomous gate passed: `npm test`."],
    ["autonomous_stopped", "Autonomous run complete: quality gates passed."],
    ["autonomous_status", "[autonomous-status: on]\n\nContinuations: 0/3."],
  ])("[VAL-AUTO-003] %s becomes a notice with its text", (customType, content) => {
    assert.equal(
      primeAutonomousNotice({ role: "custom", customType, content, display: true, details: {} }),
      content,
    );
  });

  it("[VAL-AUTO-003] ignores other custom messages and blank content", () => {
    assert.isUndefined(
      primeAutonomousNotice({ role: "custom", customType: "session_slash_command", content: "/x" }),
    );
    assert.isUndefined(
      primeAutonomousNotice({ role: "custom", customType: "autonomous_stopped", content: " " }),
    );
  });
});
