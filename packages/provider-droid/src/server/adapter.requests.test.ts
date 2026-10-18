import { assert, describe, it } from "@effect/vitest";
import type { ProviderApprovalDecision, RuntimeRequestId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

import {
  beginTurn,
  captureLogs,
  completed,
  isTerminal,
  layerTest,
  runIdFor,
  SESSION_ID,
  terminals,
  type BegunTurn,
} from "./adapterHarness.testkit.ts";
import { makeFakeDroid } from "./fakeDroid.testkit.ts";

const isResponseError = Schema.is(ProviderAdapter.ProviderAdapterRuntimeRequestResponseError);

const EXEC_PERMISSION = {
  toolUses: [
    {
      toolUse: {
        type: "tool_use",
        id: "tool-1",
        name: "Execute",
        input: { command: "rm -rf build" },
      },
      confirmationType: "exec",
      details: { type: "exec", fullCommand: "rm -rf build", command: "rm" },
    },
  ],
  options: [
    { label: "Yes, allow", value: "proceed_once" },
    { label: "Yes, and always allow (Medium)", value: "proceed_always" },
    { label: "Always allow edits to this file", value: "proceed_always_file" },
    { label: "No, cancel", value: "cancel" },
  ],
};

const CREATE_PERMISSION = {
  toolUses: [
    {
      toolUse: {
        type: "tool_use",
        id: "tool-2",
        name: "Create",
        input: { file_path: "/tmp/w/a.txt" },
      },
      confirmationType: "create",
      details: { type: "create", filePath: "/tmp/w/a.txt" },
    },
  ],
  options: [
    { label: "Yes, allow", value: "proceed_once" },
    { label: "Proceed in a new session", value: "proceed_new_session" },
    { label: "Proceed in a new session (Low)", value: "proceed_new_session_low" },
    { label: "Edit the spec", value: "proceed_edit" },
    { label: "Report a false positive", value: "proceed_report_false_positive" },
    { label: "No, keep iterating", value: "cancel" },
  ],
};

const SINGLE_QUESTION = {
  toolCallId: "call-1",
  questions: [
    {
      index: 0,
      topic: "Language",
      question: "Which language should the script use?",
      options: ["TypeScript", "Rust"],
    },
  ],
};

const MULTI_QUESTION = {
  toolCallId: "call-2",
  questions: [
    {
      index: 1,
      topic: "Checks",
      question: "Which checks should run?",
      options: ["lint", "typecheck", "test"],
      multiSelect: true,
    },
  ],
};

const EXEC_CALL = {
  type: "tool_call",
  toolUse: { type: "tool_use", id: "tool-1", name: "Execute", input: { command: "rm -rf build" } },
};

const runtimeRequests = (begun: BegunTurn) =>
  begun.opened.all.flatMap((event) =>
    event.type === "runtime_request.updated" ? [event.runtimeRequest] : [],
  );

const turnItems = (begun: BegunTurn) =>
  begun.opened.all.flatMap((event) => (event.type === "turn_item.updated" ? [event.turnItem] : []));

const nodes = (begun: BegunTurn) =>
  begun.opened.all.flatMap((event) => (event.type === "node.updated" ? [event.node] : []));

const latestRequestStatuses = (begun: BegunTurn) => {
  const latest = new Map<string, string>();
  for (const request of runtimeRequests(begun)) latest.set(String(request.id), request.status);
  return latest;
};

/** The last published status of each approval and question item. */
const latestRequestItemStatuses = (begun: BegunTurn) => {
  const latest = new Map<string, string>();
  for (const item of turnItems(begun)) {
    if (item.type === "approval_request" || item.type === "user_input_request") {
      latest.set(String(item.id), item.status);
    }
  }
  return [...latest.values()];
};

const answersFor = (begun: BegunTurn, inboundId: string) =>
  begun.process
    .answers()
    .filter((answer) => answer.id === inboundId)
    .map((answer) => answer.result);

/** Sends a permission and waits until its item, the last event of the request, is published. */
const openPermission = (begun: BegunTurn, inboundId: string, params: unknown) =>
  Effect.gen(function* () {
    yield* begun.process.emitInbound(inboundId, "droid.request_permission", params);
    const event = yield* begun.opened.takeEvent(
      (candidate) =>
        candidate.type === "turn_item.updated" && candidate.turnItem.type === "approval_request",
    );
    if (event.type !== "turn_item.updated" || event.turnItem.type !== "approval_request") {
      return assert.fail("no approval item");
    }
    return event.turnItem.requestId;
  });

const openQuestion = (begun: BegunTurn, inboundId: string, params: unknown) =>
  Effect.gen(function* () {
    yield* begun.process.emitInbound(inboundId, "droid.ask_user", params);
    const event = yield* begun.opened.takeEvent(
      (candidate) =>
        candidate.type === "turn_item.updated" && candidate.turnItem.type === "user_input_request",
    );
    if (event.type !== "turn_item.updated" || event.turnItem.type !== "user_input_request") {
      return assert.fail("no question item");
    }
    return event.turnItem.requestId;
  });

const respond = (
  begun: BegunTurn,
  requestId: RuntimeRequestId,
  response: {
    readonly decision?: ProviderApprovalDecision;
    readonly answers?: Record<string, unknown>;
  },
) => begun.opened.runtime.respondToRuntimeRequest({ requestId, ...response });

const toolResult = (isError: boolean, content: string) => ({
  type: "tool_result",
  toolUseId: "tool-1",
  messageId: "tool-result-1",
  content,
  isError,
});

describe("DroidAdapterV2 permission requests", () => {
  it.effect(
    "[VAL-REQ-005] an exec permission becomes a live command approval built from Droid's options",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        yield* begun.process.emit(EXEC_CALL);
        const requestId = yield* openPermission(begun, "perm-1", EXEC_PERMISSION);

        const pending = runtimeRequests(begun).filter((request) => request.id === requestId);
        assert.strictEqual(pending.length, 1);
        assert.strictEqual(pending[0]?.status, "pending");
        assert.strictEqual(pending[0]?.kind, "command");
        assert.deepStrictEqual(pending[0]?.responseCapability, {
          type: "live",
          providerSessionId: SESSION_ID,
        });
        assert.strictEqual(pending[0]?.providerTurnId, begun.providerTurn.id);
        assert.strictEqual(pending[0]?.nativeRequestRef?.nativeId, "perm-1");

        const item = turnItems(begun).find((candidate) => candidate.type === "approval_request");
        assert.isTrue(item?.type === "approval_request");
        if (item?.type !== "approval_request") return;
        assert.strictEqual(item.requestId, requestId);
        assert.strictEqual(item.requestKind, "command");
        assert.strictEqual(item.status, "waiting");
        assert.strictEqual(item.prompt, "rm -rf build");
        assert.deepStrictEqual(item.options, [
          { decision: "accept", label: "Yes, allow" },
          { decision: "acceptForSession", label: "Yes, and always allow (Medium)" },
          { decision: "acceptAlways", label: "Always allow edits to this file" },
          { decision: "decline", label: "No, cancel" },
        ]);
        const node = nodes(begun).find((candidate) => candidate.runtimeRequestId === requestId);
        assert.strictEqual(node?.kind, "approval_request");
        assert.strictEqual(node?.status, "waiting");
        assert.strictEqual(node?.runId, runIdFor(1));

        yield* begun.process.settle;
        assert.deepStrictEqual(
          answersFor(begun, "perm-1"),
          [],
          "Droid waits for the user's answer",
        );
        assert.deepStrictEqual(terminals(begun.opened.all), []);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-REQ-005] a create permission is a file-change approval without the options t3 cannot honor",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        const requestId = yield* openPermission(begun, "perm-2", CREATE_PERMISSION);

        const request = runtimeRequests(begun).find((candidate) => candidate.id === requestId);
        assert.strictEqual(request?.kind, "file-change");
        const item = turnItems(begun).find((candidate) => candidate.type === "approval_request");
        assert.isTrue(item?.type === "approval_request");
        if (item?.type !== "approval_request") return;
        assert.strictEqual(item.requestKind, "file-change");
        assert.deepStrictEqual(item.options, [
          { decision: "accept", label: "Yes, allow" },
          { decision: "decline", label: "No, keep iterating" },
        ]);
        const labels = JSON.stringify(item.options);
        for (const dropped of ["new session", "Edit the spec", "false positive"]) {
          assert.notInclude(labels, dropped);
        }
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    { decision: "accept", option: "proceed_once" },
    { decision: "acceptForSession", option: "proceed_always" },
    { decision: "acceptAlways", option: "proceed_always_file" },
  ] as const)(
    "[VAL-REQ-006] $decision answers with the option id Droid offered for it and the turn continues",
    ({ decision, option }) =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        yield* begun.process.emit(EXEC_CALL);
        const requestId = yield* openPermission(begun, "perm-1", EXEC_PERMISSION);

        yield* respond(begun, requestId, { decision });
        yield* begun.process.settle;
        assert.deepStrictEqual(answersFor(begun, "perm-1"), [{ selectedOption: option }]);

        yield* begun.process.emit(toolResult(false, "removed"));
        yield* begun.process.emit(completed(1));
        const terminal = yield* begun.opened.takeEvent(isTerminal);
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");

        const resolved = runtimeRequests(begun).findLast((request) => request.id === requestId);
        assert.strictEqual(resolved?.status, "resolved");
        assert.strictEqual(resolved?.decision, decision);
        assert.isNotNull(resolved?.resolvedAt);
        const approval = turnItems(begun).findLast((item) => item.type === "approval_request");
        assert.strictEqual(approval?.status, "completed");
        const node = nodes(begun).findLast((candidate) => candidate.runtimeRequestId === requestId);
        assert.strictEqual(node?.status, "completed");
        const command = turnItems(begun).findLast((item) => item.type === "command_execution");
        assert.strictEqual(command?.status, "completed");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-REQ-006] a decision Droid offered no option for is refused without inventing an id",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        const requestId = yield* openPermission(begun, "perm-2", CREATE_PERMISSION);

        const failure = yield* respond(begun, requestId, { decision: "acceptAlways" }).pipe(
          Effect.flip,
        );
        assert.isTrue(isResponseError(failure));
        yield* begun.process.settle;
        assert.deepStrictEqual(answersFor(begun, "perm-2"), [], "nothing was sent to Droid");
        assert.strictEqual(latestRequestStatuses(begun).get(String(requestId)), "pending");

        yield* respond(begun, requestId, { decision: "accept" });
        yield* begun.process.settle;
        assert.deepStrictEqual(answersFor(begun, "perm-2"), [{ selectedOption: "proceed_once" }]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-REQ-006] answering a request that is not open fails typed and sends nothing",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        const requestId = yield* openPermission(begun, "perm-1", EXEC_PERMISSION);
        yield* respond(begun, requestId, { decision: "accept" });
        const again = yield* respond(begun, requestId, { decision: "decline" }).pipe(Effect.flip);
        assert.isTrue(isResponseError(again));
        const unknown = yield* respond(begun, "runtime-request:unknown" as RuntimeRequestId, {
          decision: "accept",
        }).pipe(Effect.flip);
        assert.isTrue(isResponseError(unknown));
        yield* begun.process.settle;
        assert.strictEqual(answersFor(begun, "perm-1").length, 1, "one answer per inbound id");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-REQ-007] declining sends Droid's cancel option and the turn still completes",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        yield* begun.process.emit(EXEC_CALL);
        const requestId = yield* openPermission(begun, "perm-1", EXEC_PERMISSION);

        yield* respond(begun, requestId, { decision: "decline" });
        yield* begun.process.settle;
        assert.deepStrictEqual(answersFor(begun, "perm-1"), [{ selectedOption: "cancel" }]);

        // Droid's verified cancel outcome: permission_resolved (unscoped), an error result, completion.
        yield* begun.process.emit(
          {
            type: "permission_resolved",
            requestId: "perm-1",
            toolUseIds: [],
            selectedOption: "cancel",
          },
          null,
        );
        yield* begun.process.emit(
          toolResult(true, "Error: Permission denied by the user. Continue without that command."),
        );
        yield* begun.process.emit(completed(1));
        const terminal = yield* begun.opened.takeEvent(isTerminal);
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");

        const resolved = runtimeRequests(begun).findLast((request) => request.id === requestId);
        assert.strictEqual(resolved?.status, "resolved");
        assert.strictEqual(resolved?.decision, "decline");
        const command = turnItems(begun).findLast((item) => item.type === "command_execution");
        assert.strictEqual(
          command?.status,
          "cancelled",
          "the declined tool is settled, not running",
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  /**
   * Precedent: Pi answers a cancelled prompt with `{cancelled: true}` (`piUiResponse`,
   * packages/provider-pi/src/server/adapter.ts:3212-3227), marks the request and its item
   * cancelled (`cancelPrompt`, :1162-1198), and the turn goes on to end `completed`; only a
   * failed Droid completion would fail it.
   */
  it.effect(
    "[VAL-REQ-008] Cancelling a permission request ends the turn the way upstream adapters treat a declined approval (Pi precedent)",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        yield* begun.process.emit(EXEC_CALL);
        const requestId = yield* openPermission(begun, "perm-1", EXEC_PERMISSION);

        yield* respond(begun, requestId, { decision: "cancel" });
        yield* begun.process.settle;
        assert.deepStrictEqual(answersFor(begun, "perm-1"), [{ selectedOption: "cancel" }]);

        yield* begun.process.emit(
          {
            type: "permission_resolved",
            requestId: "perm-1",
            toolUseIds: [],
            selectedOption: "cancel",
          },
          null,
        );
        yield* begun.process.emit(toolResult(true, "Error: Permission denied by the user."));
        yield* begun.process.emit(completed(1));
        const terminal = yield* begun.opened.takeEvent(isTerminal);

        assert.isTrue(terminal.type === "turn.terminal");
        if (terminal.type !== "turn.terminal") return;
        assert.strictEqual(terminal.status, "completed");
        assert.isNull(terminal.failure);
        const request = runtimeRequests(begun).findLast((candidate) => candidate.id === requestId);
        assert.strictEqual(request?.status, "cancelled");
        assert.strictEqual(request?.decision, "cancel");
        const approval = turnItems(begun).findLast((item) => item.type === "approval_request");
        assert.strictEqual(approval?.status, "cancelled");
        const node = nodes(begun).findLast((candidate) => candidate.runtimeRequestId === requestId);
        assert.strictEqual(node?.status, "cancelled");
        const command = turnItems(begun).findLast((item) => item.type === "command_execution");
        assert.strictEqual(command?.status, "cancelled");
        assert.strictEqual(terminals(begun.opened.all).length, 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "a permission Droid asks for in the shape of ask_user is cancelled at once and logged",
    () => {
      const logs = captureLogs();
      return Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        yield* begun.process.emitInbound("perm-ask", "droid.request_permission", {
          toolUses: [
            {
              toolUse: { type: "tool_use", id: "tool-ask", name: "AskUser", input: {} },
              confirmationType: "ask_user",
              details: {},
            },
          ],
          options: [{ label: "Cancel", value: "cancel" }],
        });
        yield* begun.process.settle;
        assert.deepStrictEqual(answersFor(begun, "perm-ask"), [{ selectedOption: "cancel" }]);
        assert.deepStrictEqual(runtimeRequests(begun), []);
        assert.isTrue(
          logs.entries.some(
            (entry) => entry.level === "Warn" && entry.text.includes("confirmationType ask_user"),
          ),
        );
      }).pipe(logs.provide, Effect.scoped, Effect.provide(layerTest));
    },
  );

  it.effect("a permission that arrives with no turn running is cancelled at once", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeDroid();
      const begun = yield* beginTurn(fake);
      yield* begun.process.emit(completed(1));
      yield* begun.opened.takeEvent(isTerminal);
      yield* begun.process.emitInbound("perm-late", "droid.request_permission", EXEC_PERMISSION);
      yield* begun.process.settle;
      assert.deepStrictEqual(answersFor(begun, "perm-late"), [{ selectedOption: "cancel" }]);
      assert.deepStrictEqual(runtimeRequests(begun), []);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});

describe("DroidAdapterV2 questions", () => {
  it.effect(
    "[VAL-REQ-009] a single-select ask_user becomes a question and the answer is sent in Droid's shape",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        const requestId = yield* openQuestion(begun, "ask-1", SINGLE_QUESTION);

        const request = runtimeRequests(begun).find((candidate) => candidate.id === requestId);
        assert.strictEqual(request?.kind, "user_input");
        assert.strictEqual(request?.status, "pending");
        assert.deepStrictEqual(request?.responseCapability, {
          type: "live",
          providerSessionId: SESSION_ID,
        });
        const item = turnItems(begun).find((candidate) => candidate.type === "user_input_request");
        assert.isTrue(item?.type === "user_input_request");
        if (item?.type !== "user_input_request") return;
        assert.strictEqual(item.requestId, requestId);
        assert.deepStrictEqual(item.questions, [
          {
            id: "question-0",
            header: "Language",
            question: "Which language should the script use?",
            options: [
              { label: "TypeScript", description: "TypeScript", value: "TypeScript" },
              { label: "Rust", description: "Rust", value: "Rust" },
            ],
            multiSelect: false,
          },
        ]);

        yield* respond(begun, requestId, { answers: { "question-0": "Rust" } });
        yield* begun.process.settle;
        assert.deepStrictEqual(answersFor(begun, "ask-1"), [
          {
            answers: [
              { index: 0, question: "Which language should the script use?", answer: "Rust" },
            ],
          },
        ]);

        yield* begun.process.emit(completed(1));
        const terminal = yield* begun.opened.takeEvent(isTerminal);
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
        const resolved = runtimeRequests(begun).findLast((candidate) => candidate.id === requestId);
        assert.strictEqual(resolved?.status, "resolved");
        assert.deepStrictEqual(resolved?.answers, { "question-0": "Rust" });
        const settledItem = turnItems(begun).findLast(
          (entry) => entry.type === "user_input_request",
        );
        assert.strictEqual(settledItem?.status, "completed");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("[VAL-REQ-009] a multi-select answer is joined with commas in the reply", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeDroid();
      const begun = yield* beginTurn(fake);
      const requestId = yield* openQuestion(begun, "ask-2", MULTI_QUESTION);

      const item = turnItems(begun).find((candidate) => candidate.type === "user_input_request");
      assert.isTrue(item?.type === "user_input_request");
      if (item?.type !== "user_input_request") return;
      assert.strictEqual(item.questions[0]?.id, "question-1");
      assert.isTrue(item.questions[0]?.multiSelect);

      yield* respond(begun, requestId, { answers: { "question-1": ["lint", "test"] } });
      yield* begun.process.settle;
      assert.deepStrictEqual(answersFor(begun, "ask-2"), [
        { answers: [{ index: 1, question: "Which checks should run?", answer: "lint, test" }] },
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-REQ-010] dismissing a question replies cancelled with no answers and the turn does not fail",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        const requestId = yield* openQuestion(begun, "ask-1", SINGLE_QUESTION);

        yield* respond(begun, requestId, { decision: "cancel" });
        yield* begun.process.settle;
        assert.deepStrictEqual(answersFor(begun, "ask-1"), [{ cancelled: true, answers: [] }]);

        // Droid then reports the unanswered question as an error result and completes the turn.
        yield* begun.process.emit({
          type: "tool_result",
          toolUseId: "call-1",
          messageId: "tool-result-ask",
          content: "Error: The user dismissed the question.",
          isError: true,
        });
        yield* begun.process.emit(completed(1));
        const terminal = yield* begun.opened.takeEvent(isTerminal);
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
        const request = runtimeRequests(begun).findLast((candidate) => candidate.id === requestId);
        assert.strictEqual(request?.status, "cancelled");
        const item = turnItems(begun).findLast((entry) => entry.type === "user_input_request");
        assert.strictEqual(item?.status, "cancelled");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("[VAL-REQ-010] an answer with nothing usable in it is a dismissal", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeDroid();
      const begun = yield* beginTurn(fake);
      const requestId = yield* openQuestion(begun, "ask-1", SINGLE_QUESTION);
      yield* respond(begun, requestId, { answers: { "question-0": "" } });
      yield* begun.process.settle;
      assert.deepStrictEqual(answersFor(begun, "ask-1"), [{ cancelled: true, answers: [] }]);
      assert.strictEqual(latestRequestStatuses(begun).get(String(requestId)), "cancelled");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("an ask_user with no questions is cancelled at once and opens no request", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeDroid();
      const begun = yield* beginTurn(fake);
      yield* begun.process.emitInbound("ask-empty", "droid.ask_user", {
        toolCallId: "call-0",
        questions: [],
      });
      yield* begun.process.settle;
      assert.deepStrictEqual(answersFor(begun, "ask-empty"), [{ cancelled: true, answers: [] }]);
      assert.deepStrictEqual(runtimeRequests(begun), []);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("[VAL-REQ-012] pick_options is cancelled at once with no request and no item", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeDroid();
      const begun = yield* beginTurn(fake);
      yield* begun.process.emitInbound("pick-1", "droid.pick_options", {
        options: [{ id: "a", label: "A" }],
      });
      yield* begun.process.settle;
      assert.deepStrictEqual(answersFor(begun, "pick-1"), [{ selectedIds: [], cancelled: true }]);

      yield* begun.process.emit(completed(1));
      const terminal = yield* begun.opened.takeEvent(isTerminal);
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      assert.deepStrictEqual(runtimeRequests(begun), []);
      assert.deepStrictEqual(
        turnItems(begun).filter(
          (item) => item.type === "approval_request" || item.type === "user_input_request",
        ),
        [],
      );
      assert.deepStrictEqual(
        nodes(begun).filter((node) => node.runtimeRequestId !== null),
        [],
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});

describe("DroidAdapterV2 requests and turn endings", () => {
  it.effect(
    "[VAL-REQ-013] interrupting cancels open requests before the interrupted terminal and a late answer is not sent",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        yield* begun.process.emit(EXEC_CALL);
        const permissionId = yield* openPermission(begun, "perm-1", EXEC_PERMISSION);
        const questionId = yield* openQuestion(begun, "ask-1", SINGLE_QUESTION);

        yield* begun.opened.runtime.interruptTurn({
          providerThread: begun.providerThread,
          providerTurnId: begun.providerTurn.id,
        });
        yield* begun.process.emit(completed(1, "cancelled"));
        const terminal = yield* begun.opened.takeEvent(isTerminal);
        yield* begun.process.settle;
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "interrupted");

        const frames = begun.process.frames();
        const interruptAt = frames.findIndex(
          (frame) => frame["type"] === "request" && frame["method"] === "droid.interrupt_session",
        );
        const answerAt = (id: string) =>
          frames.findIndex((frame) => frame["type"] === "response" && frame["id"] === id);
        assert.isAbove(interruptAt, -1, "interrupt_session was sent");
        assert.isAbove(answerAt("perm-1"), interruptAt);
        assert.isAbove(answerAt("ask-1"), interruptAt);
        assert.deepStrictEqual(answersFor(begun, "perm-1"), [{ selectedOption: "cancel" }]);
        assert.deepStrictEqual(answersFor(begun, "ask-1"), [{ cancelled: true, answers: [] }]);

        const terminalAt = begun.opened.all.indexOf(terminal);
        for (const requestId of [permissionId, questionId]) {
          const cancelledAt = begun.opened.all.findIndex(
            (event) =>
              event.type === "runtime_request.updated" &&
              event.runtimeRequest.id === requestId &&
              event.runtimeRequest.status === "cancelled",
          );
          assert.isAbove(cancelledAt, -1, `${requestId} is marked cancelled`);
          assert.isBelow(cancelledAt, terminalAt, "and before the terminal");
        }
        assert.deepStrictEqual(latestRequestItemStatuses(begun), ["cancelled", "cancelled"]);

        const framesBefore = frames.length;
        const eventsBefore = begun.opened.all.length;
        const lateAnswer = yield* respond(begun, permissionId, { decision: "accept" }).pipe(
          Effect.flip,
        );
        const lateQuestion = yield* respond(begun, questionId, {
          answers: { "question-0": "Rust" },
        }).pipe(Effect.flip);
        assert.isTrue(isResponseError(lateAnswer) && isResponseError(lateQuestion));
        assert.strictEqual(begun.process.frames().length, framesBefore, "nothing is sent again");
        assert.deepStrictEqual(begun.opened.all.slice(eventsBefore), [], "and nothing reopens");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    { path: "the user answers", answered: true },
    { path: "the turn is interrupted", answered: true },
    { path: "the turn ends while a request is open", answered: true },
    { path: "the process exits while a request is open", answered: false },
  ] as const)(
    "[VAL-REQ-014] $path: each inbound request is answered at most once and none stays pending",
    ({ path, answered }) =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        const permissionId = yield* openPermission(begun, "perm-1", EXEC_PERMISSION);
        const questionId = yield* openQuestion(begun, "ask-1", SINGLE_QUESTION);
        yield* begun.process.emitInbound("pick-1", "droid.pick_options", { options: [] });

        switch (path) {
          case "the user answers":
            yield* respond(begun, permissionId, { decision: "accept" });
            yield* respond(begun, questionId, { answers: { "question-0": "Rust" } });
            yield* begun.process.emit(completed(1));
            break;
          case "the turn is interrupted":
            yield* begun.opened.runtime.interruptTurn({
              providerThread: begun.providerThread,
              providerTurnId: begun.providerTurn.id,
            });
            yield* begun.process.emit(completed(1, "cancelled"));
            break;
          case "the turn ends while a request is open":
            yield* begun.process.emit(completed(1));
            break;
          case "the process exits while a request is open":
            yield* begun.process.exit(1);
            break;
        }
        yield* begun.opened.takeEvent(isTerminal);

        for (const status of latestRequestStatuses(begun).values()) {
          assert.notStrictEqual(status, "pending");
        }
        assert.strictEqual(latestRequestStatuses(begun).size, 2, "pick_options opened no request");
        if (answered) {
          yield* begun.process.settle;
          for (const id of ["perm-1", "ask-1", "pick-1"]) {
            assert.strictEqual(answersFor(begun, id).length, 1, `${id} is answered exactly once`);
          }
        } else {
          for (const id of ["perm-1", "ask-1"]) {
            assert.isAtMost(answersFor(begun, id).length, 1, `${id} is never answered twice`);
          }
        }
        assert.strictEqual(terminals(begun.opened.all).length, 1);
        assert.notInclude(latestRequestItemStatuses(begun), "waiting");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-REQ-014] a turn that ends with a request open answers it with Droid's cancel shape once",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        yield* openPermission(begun, "perm-1", EXEC_PERMISSION);
        yield* openQuestion(begun, "ask-1", SINGLE_QUESTION);
        yield* begun.process.emit(completed(1));
        const terminal = yield* begun.opened.takeEvent(isTerminal);
        yield* begun.process.settle;
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
        assert.deepStrictEqual(answersFor(begun, "perm-1"), [{ selectedOption: "cancel" }]);
        assert.deepStrictEqual(answersFor(begun, "ask-1"), [{ cancelled: true, answers: [] }]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
