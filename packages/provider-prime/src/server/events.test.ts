import { assert, describe, it } from "@effect/vitest";

import {
  primeAgentEndErrorMessage,
  primeAutonomousContinuation,
  primeAutonomousNotice,
  primeCustomMessageHidden,
  primeCustomMessageNotice,
  primeDiscardedUserEntries,
  primeForkBoundary,
  primeForkMessages,
  primeHeartbeatNotification,
  primeIpythonFileChange,
  primeProviderGoal,
  primeIpythonUpdate,
  primeRlmChildSignature,
  primeRlmChildView,
  primeToolItem,
  primeUserEntriesFingerprint,
  primeUserEntryHash,
  primeUserEntryHashes,
} from "./events.ts";
import type { PrimeRlmChild } from "./rpc.ts";

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
describe("Prime custom messages", () => {
  const custom = (customType: string, content: unknown, display = true, details: unknown = {}) => ({
    role: "custom",
    customType,
    content,
    display,
    details,
  });

  it.each([
    { name: "the session-command echo", message: custom("session_slash_command", "/goal status") },
    {
      name: "goal steering",
      message: custom("goal_context", "[goal: continuation]\n\nContinue working."),
    },
    { name: "a display:false row", message: custom("prime_future_notice", "secret", false) },
    {
      name: "the compaction kernel note",
      message: custom("ipython_state", "[python-state]\n\nYour kernel persisted.", false),
    },
    { name: "a display:false gate row", message: custom("autonomous_status", "on", false) },
  ])("[VAL-MSG-001] hides $name", ({ message }) => {
    assert.isTrue(primeCustomMessageHidden(message));
    assert.isUndefined(primeCustomMessageNotice(message));
  });

  it("[VAL-MSG-002] RESTORE_KERNEL collapses the restored names into one line", () => {
    const content =
      "[python-state-restored]\n\nYour Python kernel state was revived from your previous session. These names are available again: df, model, helpers.";
    assert.equal(
      primeCustomMessageNotice(custom("ipython_state_restored", content, true, { restored: true })),
      "Python kernel state restored — 3 names revived.",
    );
  });

  it.each([
    {
      name: "one name",
      content:
        "[python-state-restored]\n\nYour Python kernel state was revived from your previous session. These names are available again: df.",
      notice: "Python kernel state restored — 1 name revived.",
    },
    {
      name: "a failed revive",
      content:
        "[python-state-restored]\n\nYour previous Python kernel state could not be revived; the kernel is starting fresh, so re-create any variables, imports, or loaded data you need.",
      notice: "Python kernel state could not be revived — the kernel is starting fresh.",
    },
    {
      name: "no name list",
      content: "[python-state-restored]\n\nSomething else.",
      notice: "Python kernel state restored.",
    },
  ])("[VAL-MSG-002] the kernel restore with $name is one line", ({ content, notice }) => {
    assert.equal(primeCustomMessageNotice(custom("ipython_state_restored", content)), notice);
  });

  it.each([
    { customType: "session_slash_command_result", content: "No active goal." },
    { customType: "prime_future_notice", content: "Something Prime wants shown." },
    { customType: "async_bash_completion", content: '[bash-done pid:1 exit:0]\n\nCommand: "ls"' },
  ])("[VAL-MSG-003] a displayed $customType becomes a notice with its text", (input) => {
    assert.equal(primeCustomMessageNotice(custom(input.customType, input.content)), input.content);
  });

  it.each([
    { name: "autonomous_status", message: custom("autonomous_status", "[autonomous-status: on]") },
    {
      name: "heartbeat_prompt",
      message: custom("heartbeat_prompt", "[heartbeat: */5 * * * * run#1]\n\nCheck"),
    },
    {
      name: "a failed session-command result",
      message: custom("session_slash_command_result", "Command failed: nope", true, {
        success: false,
        error: "nope",
      }),
    },
    { name: "a blank row", message: custom("prime_future_notice", "  ") },
  ])("[VAL-MSG-003] leaves $name to its own mapping", ({ message }) => {
    assert.isFalse(primeCustomMessageHidden(message));
    assert.isUndefined(primeCustomMessageNotice(message));
  });
});

describe("primeRlmChildView", () => {
  const child = (fields: Partial<PrimeRlmChild>): PrimeRlmChild => ({
    id: "child-1",
    label: "repo-scout",
    sessionDir: "/tmp/session/sub-1",
    status: "queued",
    ...fields,
  });

  it.each([
    { status: "queued", mapped: "waiting", terminal: false },
    { status: "running", mapped: "running", terminal: false },
    { status: "done", mapped: "completed", terminal: true },
    { status: "error", mapped: "failed", terminal: true },
    { status: "cancelled", mapped: "cancelled", terminal: true },
  ])("[VAL-RLM-001][VAL-RLM-003] maps $status to $mapped", ({ status, mapped, terminal }) => {
    const view = primeRlmChildView(child({ status }));
    assert.equal(view?.status, mapped);
    assert.equal(view?.terminal, terminal);
  });

  it("[VAL-RLM-001] ignores a status Prime does not define", () => {
    assert.isUndefined(primeRlmChildView(child({ status: "paused" })));
  });

  it("[VAL-RLM-001] titles the child by its label, falling back to the session name", () => {
    assert.equal(primeRlmChildView(child({ sessionName: "scout" }))?.title, "repo-scout");
    assert.equal(primeRlmChildView(child({ label: " ", sessionName: "scout" }))?.title, "scout");
  });

  it("[VAL-RLM-001] a running child reports its answer preview, else its activity, as progress", () => {
    assert.equal(
      primeRlmChildView(
        child({ status: "running", activity: { kind: "executing", toolName: "bash" } }),
      )?.progress,
      "Running bash",
    );
    assert.equal(
      primeRlmChildView(
        child({
          status: "running",
          answerPreview: "half done",
          activity: { kind: "writing" },
        }),
      )?.progress,
      "half done",
    );
    assert.isNull(primeRlmChildView(child({ status: "running" }))?.result);
  });

  it("[VAL-RLM-001] a done child's result is its answer preview", () => {
    const view = primeRlmChildView(
      child({ status: "done", answerPreview: "155 repos found", toolUseCount: 9 }),
    );
    assert.equal(view?.result, "155 repos found");
    assert.isUndefined(view?.progress);
  });

  it.each([
    { status: "error", error: "kernel died", result: "kernel died" },
    { status: "cancelled", error: "Cancelled by user", result: "Cancelled by user" },
    { status: "cancelled", error: undefined, result: null },
  ])("[VAL-RLM-003] a $status child carries its error text as the result", (input) => {
    assert.equal(
      primeRlmChildView(
        child({
          status: input.status,
          ...(input.error === undefined ? {} : { error: input.error }),
        }),
      )?.result,
      input.result,
    );
  });
});

describe("primeRlmChildSignature", () => {
  const base: PrimeRlmChild = {
    id: "child-1",
    label: "repo-scout",
    sessionDir: "/tmp/session/sub-1",
    status: "running",
    answerPreview: "half done",
    toolUseCount: 7,
  };

  it("[VAL-RLM-002] an identical snapshot has the same signature", () => {
    assert.equal(primeRlmChildSignature(base), primeRlmChildSignature({ ...base }));
  });

  it.each<{ name: string; change: Partial<PrimeRlmChild> }>([
    { name: "status", change: { status: "done" } },
    { name: "label", change: { label: "other" } },
    { name: "activity", change: { activity: { kind: "executing", toolName: "bash" } } },
    { name: "toolUseCount", change: { toolUseCount: 8 } },
    { name: "answerPreview", change: { answerPreview: "more" } },
    { name: "error", change: { error: "boom" } },
  ])("[VAL-RLM-002] changes when $name changes", ({ change }) => {
    assert.notEqual(primeRlmChildSignature(base), primeRlmChildSignature({ ...base, ...change }));
  });
});

describe("primeForkBoundary", () => {
  const turn = (ordinal: number, texts: ReadonlyArray<string> | undefined) => ({
    ordinal,
    nativeTurnRef:
      texts === undefined
        ? { fingerprint: undefined }
        : { fingerprint: primeUserEntriesFingerprint(texts.map(primeUserEntryHash)) },
  });
  const messages = (...texts: ReadonlyArray<string>) =>
    texts.map((text, index) => ({ entryId: `entry-${index}`, text }));

  it("round-trips the user entries fingerprint and ignores foreign refs", () => {
    const hashes = ["alpha", "bravo"].map(primeUserEntryHash);
    assert.deepEqual(primeUserEntryHashes(primeUserEntriesFingerprint(hashes)), hashes);
    assert.deepEqual(primeUserEntryHashes(primeUserEntriesFingerprint([])), []);
    assert.isUndefined(primeUserEntryHashes("u1"));
    assert.isUndefined(primeUserEntryHashes(undefined));
    assert.notEqual(primeUserEntryHash("alpha"), primeUserEntryHash("alphb"));
  });

  it("reads get_fork_messages and rejects a malformed reply", () => {
    assert.deepEqual(primeForkMessages({ messages: [{ entryId: "4c2b59bf", text: "alpha" }] }), [
      { entryId: "4c2b59bf", text: "alpha" },
    ]);
    assert.isUndefined(primeForkMessages({ messages: [{ entryId: "4c2b59bf" }] }));
    assert.isUndefined(primeForkMessages(undefined));
  });

  it("counts discarded user entries and flags a turn without a fingerprint", () => {
    const turns = [turn(1, ["a"]), turn(2, ["b", "steer"]), turn(3, [])];
    assert.equal(primeDiscardedUserEntries(turns, 1), 2);
    assert.equal(primeDiscardedUserEntries(turns, 2), 0);
    assert.isUndefined(primeDiscardedUserEntries([...turns, turn(4, undefined)], 1));
  });

  it("forks before the first discarded turn's prompt, counting steers and command-only turns", () => {
    const turns = [turn(1, ["a"]), turn(2, ["b", "steer"]), turn(3, []), turn(4, ["c"])];
    const history = messages("a", "b", "steer", "c");
    assert.deepEqual(
      primeForkBoundary({ messages: history, turns, boundaryOrdinal: 1, skipUnmatched: false }),
      { type: "fork", entryId: "entry-1" },
    );
    assert.deepEqual(
      primeForkBoundary({ messages: history, turns, boundaryOrdinal: 3, skipUnmatched: false }),
      { type: "fork", entryId: "entry-3" },
    );
    assert.deepEqual(
      primeForkBoundary({ messages: history, turns, boundaryOrdinal: 4, skipUnmatched: false }),
      { type: "keep_all" },
    );
  });

  it("keeps a forked session's inherited history when rolling back to thread start", () => {
    assert.deepEqual(
      primeForkBoundary({
        messages: messages("inherited", "a", "b"),
        turns: [turn(1, ["a"]), turn(2, ["b"])],
        boundaryOrdinal: 0,
        skipUnmatched: false,
      }),
      { type: "fork", entryId: "entry-1" },
    );
  });

  it("fails rollback when the session no longer matches or a turn has no fingerprint", () => {
    const history = messages("a", "b");
    assert.equal(
      primeForkBoundary({
        messages: history,
        turns: [turn(1, ["a"]), turn(2, ["changed"])],
        boundaryOrdinal: 1,
        skipUnmatched: false,
      }).type,
      "error",
    );
    assert.deepEqual(
      primeForkBoundary({
        messages: history,
        turns: [turn(1, ["a"]), turn(2, undefined)],
        boundaryOrdinal: 1,
        skipUnmatched: false,
      }),
      { type: "error", message: "Prime turn 2 has no recorded session entries" },
    );
  });

  it("skips a fork source's rolled-back turns but still checks the boundary turn", () => {
    const turns = [turn(1, ["a"]), turn(2, ["rolled back"]), turn(3, ["c"])];
    const history = messages("a", "c");
    assert.deepEqual(
      primeForkBoundary({ messages: history, turns, boundaryOrdinal: 1, skipUnmatched: true }),
      { type: "fork", entryId: "entry-1" },
    );
    assert.equal(
      primeForkBoundary({ messages: history, turns, boundaryOrdinal: 1, skipUnmatched: false })
        .type,
      "error",
    );
    // A rolled-back turn repeating the boundary turn's prompt would swallow it.
    assert.equal(
      primeForkBoundary({
        messages: messages("same", "same"),
        turns: [turn(1, ["same"]), turn(2, ["same"]), turn(3, ["same"])],
        boundaryOrdinal: 1,
        skipUnmatched: true,
      }).type,
      "error",
    );
  });
});
