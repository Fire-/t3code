import { assert, describe, it } from "@effect/vitest";

import {
  DROID_EXIT_SPEC_MODE,
  droidApprovalRequest,
  droidAskUserReply,
  droidExitSpecModePlan,
  droidPermissionReply,
  droidUserInputRequest,
} from "./requests.ts";

const EXEC_PARAMS = {
  toolUses: [
    {
      toolUse: { type: "tool_use", id: "tool-1", name: "Execute", input: { command: "ls -la" } },
      confirmationType: "exec",
      details: { type: "exec", fullCommand: "ls -la /tmp", command: "ls" },
    },
  ],
  options: [
    { label: "Yes, allow", value: "proceed_once" },
    { label: "Yes, and always allow (Medium)", value: "proceed_always" },
    { label: "No, cancel", value: "cancel" },
  ],
};

describe("droidApprovalRequest", () => {
  it("[VAL-REQ-005] an exec permission is a command request built from Droid's own options", () => {
    const request = droidApprovalRequest(EXEC_PARAMS);
    assert.strictEqual(request.requestKind, "command");
    assert.strictEqual(request.title, "Execute");
    assert.strictEqual(request.prompt, "ls -la /tmp");
    assert.deepStrictEqual(request.toolUseIds, ["tool-1"]);
    assert.deepStrictEqual(request.options, [
      { decision: "accept", label: "Yes, allow" },
      { decision: "acceptForSession", label: "Yes, and always allow (Medium)" },
      { decision: "decline", label: "No, cancel" },
    ]);
  });

  it("[VAL-REQ-005] create, edit, and apply_patch permissions are file-change requests", () => {
    for (const confirmationType of ["create", "edit", "apply_patch"]) {
      const request = droidApprovalRequest({
        toolUses: [
          {
            toolUse: { type: "tool_use", id: "tool-2", name: "Create", input: {} },
            confirmationType,
            details: { type: confirmationType, filePath: "/tmp/work/hello.txt" },
          },
        ],
        options: EXEC_PARAMS.options,
      });
      assert.strictEqual(request.requestKind, "file-change", confirmationType);
      assert.strictEqual(request.prompt, "/tmp/work/hello.txt", confirmationType);
    }
  });

  it("[VAL-REQ-005] any other confirmation type is a non-file permission request", () => {
    const request = droidApprovalRequest({
      toolUses: [
        {
          toolUse: { type: "tool_use", id: "tool-3", name: "mcp__linear__list", input: {} },
          confirmationType: "mcp_tool",
          details: { type: "mcp_tool", toolName: "mcp__linear__list", serverName: "linear" },
        },
      ],
      options: EXEC_PARAMS.options,
    });
    assert.strictEqual(request.requestKind, "permission");
    assert.strictEqual(request.confirmationType, "mcp_tool");
  });

  it("[VAL-REQ-005] options without a decision mapping are dropped and Droid's labels are kept", () => {
    const request = droidApprovalRequest({
      ...EXEC_PARAMS,
      options: [
        { label: "Proceed with implementation", value: "proceed_once" },
        { label: "Proceed, and allow Low", value: "proceed_auto_run_low" },
        { label: "Proceed in a new session", value: "proceed_new_session" },
        { label: "Proceed in a new session (Low)", value: "proceed_new_session_low" },
        { label: "Proceed in a new session (Medium)", value: "proceed_new_session_medium" },
        { label: "Proceed in a new session (High)", value: "proceed_new_session_high" },
        { label: "Edit the spec", value: "proceed_edit" },
        { label: "Report a false positive", value: "proceed_report_false_positive" },
        { label: "A brand new thing", value: "proceed_in_the_future" },
        { label: "No, keep iterating on spec", value: "cancel" },
      ],
    });
    assert.deepStrictEqual(
      request.options.map((option) => option.label),
      ["Proceed with implementation", "Proceed, and allow Low", "No, keep iterating on spec"],
    );
    const replies = request.options.map((option) => droidPermissionReply(request, option.decision));
    assert.deepStrictEqual(replies, [
      { selectedOption: "proceed_once" },
      { selectedOption: "proceed_auto_run_low" },
      { selectedOption: "cancel" },
    ]);
  });

  it("[VAL-REQ-006] the first offered option wins when two map to the same decision", () => {
    const request = droidApprovalRequest({
      ...EXEC_PARAMS,
      options: [
        { label: "Always allow this tool", value: "proceed_always_tools" },
        { label: "Always allow this server", value: "proceed_always_server" },
        { label: "No", value: "cancel" },
      ],
    });
    assert.deepStrictEqual(
      request.options.map((option) => option.label),
      ["Always allow this tool", "No"],
    );
    assert.deepStrictEqual(droidPermissionReply(request, "acceptForSession"), {
      selectedOption: "proceed_always_tools",
    });
  });
});

describe("droidPermissionReply", () => {
  const request = droidApprovalRequest({
    ...EXEC_PARAMS,
    options: [
      { label: "Yes", value: "proceed_once" },
      { label: "Always in this file", value: "proceed_always_file" },
      { label: "No", value: "cancel" },
    ],
  });

  it("[VAL-REQ-006] each decision answers with the option id Droid offered for it", () => {
    assert.deepStrictEqual(droidPermissionReply(request, "accept"), {
      selectedOption: "proceed_once",
    });
    assert.deepStrictEqual(droidPermissionReply(request, "acceptAlways"), {
      selectedOption: "proceed_always_file",
    });
  });

  it("[VAL-REQ-006] a decision Droid offered no option for has no reply", () => {
    assert.isUndefined(droidPermissionReply(request, "acceptForSession"));
  });

  it("[VAL-REQ-007] decline and cancel both answer with Droid's cancel option", () => {
    assert.deepStrictEqual(droidPermissionReply(request, "decline"), { selectedOption: "cancel" });
    assert.deepStrictEqual(droidPermissionReply(request, "cancel"), { selectedOption: "cancel" });
  });

  it("[VAL-REQ-007] without any offered options only the protocol's cancel is available", () => {
    const bare = droidApprovalRequest({ toolUses: [], options: [] });
    assert.deepStrictEqual(bare.options, []);
    assert.isUndefined(droidPermissionReply(bare, "accept"));
    assert.deepStrictEqual(droidPermissionReply(bare, "decline"), { selectedOption: "cancel" });
  });
});

describe("droidUserInputRequest", () => {
  it("[VAL-REQ-009] questions get stable ids from Droid's index and keep topic, text, and options", () => {
    const request = droidUserInputRequest({
      toolCallId: "call-1",
      questions: [
        { index: 1, topic: "Letter", question: "Pick a letter", options: ["A", "B"] },
        { index: 2, question: "Anything else?", options: [] },
      ],
    });
    assert.deepStrictEqual(request.questions, [
      {
        id: "question-1",
        header: "Letter",
        question: "Pick a letter",
        options: [
          { label: "A", description: "A", value: "A" },
          { label: "B", description: "B", value: "B" },
        ],
        multiSelect: false,
      },
      {
        id: "question-2",
        header: "Question 2",
        question: "Anything else?",
        options: [],
        multiSelect: false,
      },
    ]);
  });

  it("[VAL-REQ-009] a question marked multiSelect carries the setting over", () => {
    const request = droidUserInputRequest({
      questions: [
        { index: 3, topic: "Tools", question: "Which?", options: ["x"], multiSelect: true },
      ],
    });
    assert.strictEqual(request.questions[0]?.multiSelect, true);
  });

  it("[VAL-REQ-009] params without a usable question give no questions", () => {
    assert.deepStrictEqual(droidUserInputRequest({ questions: [] }).questions, []);
    assert.deepStrictEqual(droidUserInputRequest(undefined).questions, []);
  });
});

describe("droidAskUserReply", () => {
  const request = droidUserInputRequest({
    questions: [
      { index: 1, topic: "Letter", question: "Pick a letter", options: ["A", "B"] },
      {
        index: 2,
        topic: "Tools",
        question: "Which tools?",
        options: ["x", "y"],
        multiSelect: true,
      },
    ],
  });

  it("[VAL-REQ-009] answers are keyed by question id and sent as {index, question, answer}", () => {
    assert.deepStrictEqual(
      droidAskUserReply(request, { "question-1": "B", "question-2": ["x", "y"] }),
      {
        answers: [
          { index: 1, question: "Pick a letter", answer: "B" },
          { index: 2, question: "Which tools?", answer: "x, y" },
        ],
      },
    );
  });

  it("[VAL-REQ-009] a question without an answer is left out", () => {
    assert.deepStrictEqual(droidAskUserReply(request, { "question-2": "y" }), {
      answers: [{ index: 2, question: "Which tools?", answer: "y" }],
    });
  });

  it("[VAL-REQ-010] no usable answer at all is a dismissal, not an empty answer", () => {
    assert.isUndefined(droidAskUserReply(request, {}));
    assert.isUndefined(droidAskUserReply(request, { "question-1": 42 }));
    assert.isUndefined(droidAskUserReply(request, undefined));
  });
});

describe("droidExitSpecModePlan", () => {
  const exitParams = (entry: { readonly details?: unknown; readonly input?: unknown }) => ({
    toolUses: [
      {
        toolUse: { type: "tool_use", id: "tool-9", name: "ExitSpecMode", input: entry.input ?? {} },
        confirmationType: DROID_EXIT_SPEC_MODE,
        details: entry.details ?? { type: "exit_spec_mode" },
      },
    ],
    options: [{ label: "No, keep iterating on spec", value: "cancel" }],
  });

  it("[VAL-PLAN-002] an exit_spec_mode request is recognized by its confirmation type", () => {
    const request = droidApprovalRequest(exitParams({}));
    assert.strictEqual(request.confirmationType, DROID_EXIT_SPEC_MODE);
    assert.strictEqual(DROID_EXIT_SPEC_MODE, "exit_spec_mode");
    assert.deepStrictEqual(request.toolUseIds, ["tool-9"]);
  });

  it("[VAL-PLAN-003] the plan is details.plan, exactly as Droid wrote it", () => {
    const plan = "# Plan\n\n1. Create `hello.txt`.\n";
    assert.strictEqual(
      droidExitSpecModePlan(
        exitParams({ details: { type: "exit_spec_mode", plan }, input: { plan: "ignored" } }),
      ),
      plan,
    );
  });

  it("[VAL-PLAN-003] the ExitSpecMode tool input stands in when the details carry no plan", () => {
    assert.strictEqual(
      droidExitSpecModePlan(exitParams({ input: { plan: "From the input" } })),
      "From the input",
    );
    assert.strictEqual(
      droidExitSpecModePlan(
        exitParams({ details: { type: "exit_spec_mode", plan: "  " }, input: { plan: "Input" } }),
      ),
      "Input",
    );
  });

  it("[VAL-PLAN-003] no plan text anywhere is undefined, never an empty plan", () => {
    assert.isUndefined(droidExitSpecModePlan(exitParams({})));
    assert.isUndefined(
      droidExitSpecModePlan(exitParams({ details: { plan: 42 }, input: { plan: "" } })),
    );
    assert.isUndefined(droidExitSpecModePlan({ toolUses: [] }));
    assert.isUndefined(droidExitSpecModePlan(undefined));
  });
});
