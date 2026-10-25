import { assert, describe, it } from "@effect/vitest";
import type { RuntimeMode } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

import {
  beginTurn,
  captureLogs,
  completed,
  ensure,
  isTerminal,
  layerTest,
  makeProviderThread,
  messageIdFor,
  methodsOf,
  MODEL,
  openRuntime,
  requestsOf,
  runIdFor,
  runTurn,
  startTurn,
  terminals,
  THREAD_ID,
  type BegunTurn,
  type OpenRuntime,
  type TurnOptions,
} from "./adapterHarness.testkit.ts";
import { FAKE_DROID_DEFAULT_SETTINGS, makeFakeDroid, type FakeDroid } from "./fakeDroid.testkit.ts";

type Events = ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>;

const PLAN_MARKDOWN = "Create `hello.txt` in the working directory containing the word `hi`.";
const REQUEST_ID = "<uuid-1>";
const TOOL_USE_ID = "<toolu>";

/** The `exit_spec_mode` request frame of the spike's cancel probe (scrubbed), params only. */
const EXIT_SPEC_PERMISSION = {
  toolUses: [
    {
      toolUse: {
        type: "tool_use",
        id: TOOL_USE_ID,
        name: "ExitSpecMode",
        input: { plan: PLAN_MARKDOWN },
      },
      confirmationType: "exit_spec_mode",
      details: {
        type: "exit_spec_mode",
        plan: PLAN_MARKDOWN,
        contextSavingsPercent: 1,
        supportsClientStartedHandoff: true,
      },
    },
  ],
  options: [
    { label: "Proceed with implementation", value: "proceed_once", selectedColor: "#d78700" },
    {
      label: "Proceed, and allow file edits and read-only commands (Low)",
      value: "proceed_auto_run_low",
      selectedColor: "#d78700",
    },
    {
      label: "Proceed, and allow reversible commands (Medium)",
      value: "proceed_auto_run_medium",
      selectedColor: "#d78700",
    },
    {
      label: "Proceed, and allow all commands (High)",
      value: "proceed_auto_run_high",
      selectedColor: "#d75f5f",
    },
    {
      label: "Proceed in a new session (no autonomy)",
      value: "proceed_new_session",
      selectedColor: "#d78700",
    },
    {
      label: "Proceed in a new session (Low autonomy)",
      value: "proceed_new_session_low",
      selectedColor: "#d78700",
    },
    {
      label: "Proceed in a new session (Medium autonomy)",
      value: "proceed_new_session_medium",
      selectedColor: "#d78700",
    },
    {
      label: "Proceed in a new session (High autonomy)",
      value: "proceed_new_session_high",
      selectedColor: "#d75f5f",
    },
    {
      label: "No, keep iterating on spec",
      value: "cancel",
      selectedColor: "#d75f5f",
      selectedPrefix: "\u2715 ",
    },
  ],
};

const exitSpecPermission = (markdown: string, toolUseId = TOOL_USE_ID) => ({
  ...EXIT_SPEC_PERMISSION,
  toolUses: [
    {
      ...EXIT_SPEC_PERMISSION.toolUses[0],
      toolUse: {
        ...EXIT_SPEC_PERMISSION.toolUses[0]?.toolUse,
        id: toolUseId,
        input: { plan: markdown },
      },
      details: { ...EXIT_SPEC_PERMISSION.toolUses[0]?.details, plan: markdown },
    },
  ],
});

const exitSpecToolCall = (markdown = PLAN_MARKDOWN, toolUseId = TOOL_USE_ID) => ({
  type: "tool_call",
  toolUse: { type: "tool_use", id: toolUseId, name: "ExitSpecMode", input: { plan: markdown } },
});

const PLAN_NOT_APPROVED =
  "Error: Plan not approved - remaining in Spec Mode. Provide feedback to refine the spec.";

const exitSpecToolResult = (toolUseId = TOOL_USE_ID) => ({
  type: "tool_result",
  toolUseId,
  messageId: "<uuid-3>",
  content: PLAN_NOT_APPROVED,
  isError: true,
});

/**
 * The notifications Droid sent after the client's reply in the spike's cancel probe (frames rel 1
 * to 5, scrubbed). `scoped` is whether the frame carried a `sessionId`: `permission_resolved`
 * did not. The turn id is the only field the test rewrites, to the message id the adapter sent.
 */
const spikePostReplyFrames = (turnId: string) => [
  {
    scoped: true,
    notification: { type: "droid_working_state_changed", newState: "streaming_assistant_message" },
  },
  {
    scoped: false,
    notification: {
      type: "permission_resolved",
      requestId: REQUEST_ID,
      toolUseIds: [],
      selectedOption: "cancel",
    },
  },
  {
    scoped: true,
    notification: {
      type: "tool_result",
      toolUseId: TOOL_USE_ID,
      messageId: "<uuid-3>",
      content: PLAN_NOT_APPROVED,
      isError: true,
    },
  },
  {
    scoped: true,
    notification: {
      type: "agent_turn_completed",
      reason: "completed",
      turnId,
      tokenUsage: {
        inputTokens: 373,
        outputTokens: 137,
        cacheCreationTokens: 8535,
        cacheReadTokens: 14398,
        thinkingTokens: 49,
        factoryCredits: 682,
      },
      cumulativeTokenUsage: {
        inputTokens: 373,
        outputTokens: 137,
        cacheCreationTokens: 8535,
        cacheReadTokens: 14398,
        thinkingTokens: 49,
        factoryCredits: 682,
      },
      childTokenUsage: {
        inputTokens: 0,
        outputTokens: 0,
        cacheCreationTokens: 0,
        cacheReadTokens: 0,
        thinkingTokens: 0,
        factoryCredits: 0,
      },
      cumulativeChildTokenUsage: {
        inputTokens: 0,
        outputTokens: 0,
        cacheCreationTokens: 0,
        cacheReadTokens: 0,
        thinkingTokens: 0,
        factoryCredits: 0,
      },
      durationMs: 2133,
    },
  },
  { scoped: true, notification: { type: "droid_working_state_changed", newState: "idle" } },
];

const turnItemsOf = (events: Events) =>
  events.flatMap((event) => (event.type === "turn_item.updated" ? [event.turnItem] : []));
const plansOf = (events: Events) =>
  events.flatMap((event) => (event.type === "plan.updated" ? [event.plan] : []));
const nodesOf = (events: Events) =>
  events.flatMap((event) => (event.type === "node.updated" ? [event.node] : []));
const runtimeRequestsOf = (events: Events) =>
  events.flatMap((event) =>
    event.type === "runtime_request.updated" ? [event.runtimeRequest] : [],
  );

const answersFor = (process: BegunTurn["process"], inboundId: string) =>
  process
    .answers()
    .filter((answer) => answer.id === inboundId)
    .map((answer) => answer.result);

const settingsFrames = (fake: FakeDroid) =>
  requestsOf(fake, "droid.update_session_settings").map((request) => request.params);

/** A whole plan turn: the plan, Droid's cancelled tool result, and the completion. */
const proposePlan = Effect.fnUntraced(function* (
  opened: OpenRuntime,
  fake: FakeDroid,
  providerThread: Parameters<typeof startTurn>[1],
  options: TurnOptions & { readonly markdown?: string } = {},
) {
  const runOrdinal = options.runOrdinal ?? 1;
  const markdown = options.markdown ?? PLAN_MARKDOWN;
  const toolUseId = `tool-exit-${runOrdinal}`;
  yield* startTurn(opened.runtime, providerThread, { ...options, interactionMode: "plan" });
  const process = fake.current();
  yield* process.emit(exitSpecToolCall(markdown, toolUseId));
  yield* process.emitInbound(
    `perm-exit-${runOrdinal}`,
    "droid.request_permission",
    exitSpecPermission(markdown, toolUseId),
  );
  yield* process.emit(exitSpecToolResult(toolUseId));
  yield* process.emit(completed(runOrdinal));
  return yield* opened.takeEvent(isTerminal);
});

describe("DroidAdapterV2 plan mode", () => {
  describe("settings", () => {
    it.effect(
      "[VAL-PLAN-001] a plan turn sends spec mode with the thread's model and effort before the message",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          const thread = yield* ensure(opened);
          yield* startTurn(opened.runtime, thread, {
            interactionMode: "plan",
            model: "gpt-5-codex",
          });

          assert.deepStrictEqual(methodsOf(fake), [
            "droid.initialize_session",
            "droid.update_session_settings",
            "droid.add_user_message",
          ]);
          assert.deepStrictEqual(settingsFrames(fake), [
            {
              interactionMode: "spec",
              autonomyLevel: "off",
              modelId: "gpt-5-codex",
              reasoningEffort: "high",
              specModeModelId: "gpt-5-codex",
              specModeReasoningEffort: "high",
            },
          ]);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-PLAN-001] a model with no effort option sends no spec mode effort", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened);
        yield* startTurn(opened.runtime, thread, { interactionMode: "plan", effort: undefined });
        assert.deepStrictEqual(settingsFrames(fake), [
          {
            interactionMode: "spec",
            autonomyLevel: "off",
            modelId: MODEL,
            specModeModelId: MODEL,
          },
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-PLAN-001] a default turn sends auto and no spec model", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened);
        yield* runTurn(opened, fake, thread, { interactionMode: "plan" });
        yield* runTurn(opened, fake, thread, { runOrdinal: 2 });
        assert.deepStrictEqual(settingsFrames(fake).at(-1), {
          interactionMode: "auto",
          autonomyLevel: "off",
          modelId: MODEL,
          reasoningEffort: "high",
        });
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-PLAN-006] a plan turn after a default turn sends spec mode with the spec model again",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          const thread = yield* ensure(opened);
          yield* runTurn(opened, fake, thread, { interactionMode: "plan" });
          yield* runTurn(opened, fake, thread, { runOrdinal: 2 });
          yield* runTurn(opened, fake, thread, { runOrdinal: 3, interactionMode: "plan" });

          assert.deepStrictEqual(
            settingsFrames(fake).map((params) => [
              params["interactionMode"],
              params["specModeModelId"],
            ]),
            [
              ["spec", MODEL],
              ["auto", undefined],
              ["spec", MODEL],
            ],
          );
          assert.deepStrictEqual(methodsOf(fake).slice(-2), [
            "droid.update_session_settings",
            "droid.add_user_message",
          ]);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-PLAN-006] a plan turn on a resumed thread sends spec mode explicitly, whatever Droid restored",
      () =>
        Effect.gen(function* () {
          for (const stored of ["spec", "auto"]) {
            const fake = yield* makeFakeDroid({
              sessions: {
                "stored-session": {
                  settings: {
                    ...FAKE_DROID_DEFAULT_SETTINGS,
                    interactionMode: stored,
                    specModeModelId: "some-other-model",
                  },
                  messages: [],
                },
              },
            });
            yield* Effect.gen(function* () {
              const opened = yield* openRuntime(fake);
              const resumed = yield* opened.runtime.resumeThread({
                providerThread: yield* makeProviderThread("stored-session"),
              });
              yield* startTurn(opened.runtime, resumed, { interactionMode: "plan" });

              assert.deepStrictEqual(methodsOf(fake), [
                "droid.load_session",
                "droid.update_session_settings",
                "droid.add_user_message",
              ]);
              assert.deepStrictEqual(settingsFrames(fake), [
                {
                  interactionMode: "spec",
                  autonomyLevel: "off",
                  modelId: MODEL,
                  reasoningEffort: "high",
                  specModeModelId: MODEL,
                  specModeReasoningEffort: "high",
                },
              ]);
            }).pipe(Effect.scoped);
          }
        }).pipe(Effect.provide(layerTest)),
    );
  });

  describe("exit_spec_mode", () => {
    it.effect(
      "[VAL-PLAN-002] exit_spec_mode is answered at once with cancel, without a request, an approval item, or a failed tool",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const begun = yield* beginTurn(fake, { interactionMode: "plan" });
          const { process, opened } = begun;
          yield* process.emit(exitSpecToolCall());
          yield* process.emitInbound(REQUEST_ID, "droid.request_permission", EXIT_SPEC_PERMISSION);
          yield* process.settle;

          // The reply is on the wire before anything else of this turn arrives.
          assert.deepStrictEqual(answersFor(process, REQUEST_ID), [{ selectedOption: "cancel" }]);

          yield* process.emit(exitSpecToolResult());
          yield* process.emit(completed(1));
          yield* opened.takeEvent(isTerminal);

          assert.deepStrictEqual(runtimeRequestsOf(opened.all), []);
          assert.isFalse(nodesOf(opened.all).some((node) => node.kind === "approval_request"));
          const items = turnItemsOf(opened.all);
          assert.isFalse(items.some((item) => item.type === "approval_request"));
          assert.isFalse(items.some((item) => item.status === "failed"));

          const tool = items.findLast((item) => item.type === "dynamic_tool");
          assert.strictEqual(tool?.type, "dynamic_tool");
          if (tool?.type !== "dynamic_tool") return;
          assert.strictEqual(tool.toolName, "ExitSpecMode");
          assert.strictEqual(tool.status, "cancelled");
          assert.strictEqual(terminals(opened.all)[0]?.status, "completed");
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-PLAN-002] a Stop that lands first still settles the tool as interrupted, not failed",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const begun = yield* beginTurn(fake, { interactionMode: "plan" });
          const { process, opened } = begun;
          yield* process.emit(exitSpecToolCall());
          yield* process.emitInbound(REQUEST_ID, "droid.request_permission", EXIT_SPEC_PERMISSION);
          yield* process.settle;
          yield* opened.runtime.interruptTurn({
            providerThread: begun.providerThread,
            providerTurnId: begun.providerTurn.id,
          });
          yield* process.emit(exitSpecToolResult());
          yield* process.emit(completed(1, "cancelled"));
          yield* opened.takeEvent(isTerminal);

          const tool = turnItemsOf(opened.all).findLast((item) => item.type === "dynamic_tool");
          assert.strictEqual(tool?.status, "interrupted");
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-PLAN-002] exit_spec_mode arriving outside a turn is cancelled and projects nothing",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          yield* ensure(opened);
          const process = fake.current();
          yield* process.emitInbound(REQUEST_ID, "droid.request_permission", EXIT_SPEC_PERMISSION);
          yield* process.settle;
          assert.deepStrictEqual(answersFor(process, REQUEST_ID), [{ selectedOption: "cancel" }]);
          assert.deepStrictEqual(plansOf(opened.all), []);
          assert.deepStrictEqual(runtimeRequestsOf(opened.all), []);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-PLAN-002] a request without any plan text is still answered and logged", () => {
      const logs = captureLogs();
      return Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake, { interactionMode: "plan" });
        yield* begun.process.emitInbound("perm-empty", "droid.request_permission", {
          toolUses: [
            {
              toolUse: { type: "tool_use", id: "tool-empty", name: "ExitSpecMode", input: {} },
              confirmationType: "exit_spec_mode",
              details: { type: "exit_spec_mode", plan: "   " },
            },
          ],
          options: [{ label: "No, keep iterating on spec", value: "cancel" }],
        });
        yield* begun.process.settle;
        assert.deepStrictEqual(answersFor(begun.process, "perm-empty"), [
          { selectedOption: "cancel" },
        ]);
        assert.deepStrictEqual(plansOf(begun.opened.all), []);
        assert.isTrue(
          logs.entries.some(
            (entry) => entry.level === "Warn" && entry.text.includes("exit_spec_mode"),
          ),
        );
      }).pipe(logs.provide, Effect.scoped, Effect.provide(layerTest));
    });

    it.effect(
      "[VAL-PLAN-007] nothing is ever answered with a proceed option and the provider thread stays on its session",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const begun = yield* beginTurn(fake, { interactionMode: "plan" });
          const { process, opened } = begun;
          const nativeBefore = begun.providerThread.nativeThreadRef?.nativeId;
          yield* process.emit(exitSpecToolCall());
          yield* process.emitInbound(REQUEST_ID, "droid.request_permission", EXIT_SPEC_PERMISSION);
          yield* process.settle;
          yield* process.emit(exitSpecToolResult());
          yield* process.emit(completed(1));
          yield* opened.takeEvent(isTerminal);

          assert.deepStrictEqual(answersFor(process, REQUEST_ID), [{ selectedOption: "cancel" }]);
          for (const answer of process.answers()) {
            assert.notMatch(JSON.stringify(answer.result), /proceed_/);
          }
          assert.deepStrictEqual(methodsOf(fake), [
            "droid.initialize_session",
            "droid.update_session_settings",
            "droid.add_user_message",
          ]);
          const refs = opened.all.flatMap((event) =>
            event.type === "provider_thread.updated"
              ? [event.providerThread.nativeThreadRef?.nativeId]
              : [],
          );
          assert.isAbove(refs.length, 0);
          assert.isTrue(refs.every((nativeId) => nativeId === nativeBefore));
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-PLAN-007] a spec_handoff_session_created that arrives anyway is logged and the thread stays put",
      () => {
        const logs = captureLogs();
        return Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const begun = yield* beginTurn(fake, { interactionMode: "plan" });
          const { process, opened } = begun;
          const oldSession = process.sessionId() ?? "";
          assert.notStrictEqual(oldSession, "");
          yield* process.emit({
            type: "spec_handoff_session_created",
            newSessionId: "new-session-9",
            specHandoffId: "handoff-1",
            initialPrompt: `Implement the following plan:\n\n${PLAN_MARKDOWN}`,
            timestamp: 1_800_000_000_000,
          });
          yield* process.emit(completed(1, "spec_handoff"));
          const terminal = yield* opened.takeEvent(isTerminal);
          assert.strictEqual(terminal.type === "turn.terminal" && terminal.status, "completed");

          const warning = logs.entries.find(
            (entry) => entry.level === "Warn" && entry.text.includes("spec handoff"),
          );
          assert.isDefined(warning);
          assert.include(warning?.text, "new-session-9");
          assert.include(warning?.text, "handoff-1");
          assert.include(warning?.text, oldSession);

          const refs = opened.all.flatMap((event) =>
            event.type === "provider_thread.updated"
              ? [event.providerThread.nativeThreadRef?.nativeId]
              : [],
          );
          assert.isFalse(refs.includes("new-session-9"));

          yield* startTurn(opened.runtime, begun.providerThread, { runOrdinal: 2 });
          assert.strictEqual(process.sessionId(), oldSession);
          assert.deepStrictEqual(requestsOf(fake, "droid.load_session"), []);
          assert.strictEqual(requestsOf(fake, "droid.initialize_session").length, 1);
        }).pipe(logs.provide, Effect.scoped, Effect.provide(layerTest));
      },
    );
  });

  describe("proposed plans", () => {
    it.effect(
      "[VAL-PLAN-003][VAL-PLAN-004] exit_spec_mode becomes a proposed_plan item, and implementing it starts a turn with the chosen autonomy",
      () =>
        Effect.gen(function* () {
          const modes: ReadonlyArray<readonly [RuntimeMode, string]> = [
            ["approval-required", "off"],
            ["full-access", "high"],
          ];
          for (const [runtimeMode, level] of modes) {
            const fake = yield* makeFakeDroid();
            yield* Effect.gen(function* () {
              const opened = yield* openRuntime(fake, { runtimeMode });
              const thread = yield* ensure(opened, { runtimeMode });
              const planTerminal = yield* proposePlan(opened, fake, thread, { runtimeMode });
              assert.strictEqual(
                planTerminal.type === "turn.terminal" && planTerminal.status,
                "completed",
              );

              // The plan: one active artifact and one item that point at each other.
              const plans = plansOf(opened.all);
              assert.strictEqual(plans.length, 1);
              const plan = plans[0];
              assert.strictEqual(plan?.kind, "proposed_plan");
              if (plan?.kind !== "proposed_plan") return;
              assert.strictEqual(plan.status, "active");
              assert.strictEqual(plan.markdown, PLAN_MARKDOWN);
              assert.strictEqual(plan.threadId, THREAD_ID);
              assert.strictEqual(plan.runId, runIdFor(1));
              const item = turnItemsOf(opened.all).find(
                (candidate) => candidate.type === "proposed_plan",
              );
              assert.strictEqual(item?.type, "proposed_plan");
              if (item?.type !== "proposed_plan") return;
              assert.strictEqual(item.planId, plan.id);
              assert.strictEqual(item.markdown, PLAN_MARKDOWN);
              assert.strictEqual(item.streaming, false);
              assert.strictEqual(item.status, "completed");
              assert.strictEqual(item.runId, runIdFor(1));
              assert.strictEqual(item.nodeId, plan.nodeId);
              const planNode = nodesOf(opened.all).find((node) => node.id === plan.nodeId);
              assert.strictEqual(planNode?.kind, "plan");
              assert.strictEqual(planNode?.status, "completed");

              // Implementing: an ordinary turn, with one settings frame before its message.
              yield* runTurn(opened, fake, thread, {
                runOrdinal: 2,
                runtimeMode,
                text: "Implement the plan.",
              });
              const frames = fake
                .current()
                .requests()
                .map((request) => [request.method, request.params] as const);
              assert.deepStrictEqual(
                frames.map(([method]) => method),
                [
                  "droid.initialize_session",
                  "droid.update_session_settings",
                  "droid.add_user_message",
                  "droid.update_session_settings",
                  "droid.add_user_message",
                ],
              );
              assert.deepStrictEqual(frames[3]?.[1], {
                interactionMode: "auto",
                autonomyLevel: level,
                modelId: MODEL,
                reasoningEffort: "high",
              });
              assert.strictEqual(frames[4]?.[1]["text"], "Implement the plan.");
              assert.strictEqual(frames[4]?.[1]["messageId"], messageIdFor(2));
              assert.deepStrictEqual(answersFor(fake.current(), "perm-exit-1"), [
                { selectedOption: "cancel" },
              ]);
            }).pipe(Effect.scoped);
          }
        }).pipe(Effect.provide(layerTest)),
    );

    it.effect("[VAL-PLAN-003] the plan is taken from the tool input when the details lack it", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake, { interactionMode: "plan" });
        yield* begun.process.emitInbound("perm-input", "droid.request_permission", {
          toolUses: [
            {
              toolUse: {
                type: "tool_use",
                id: "tool-input",
                name: "ExitSpecMode",
                input: { plan: "# From the input" },
              },
              confirmationType: "exit_spec_mode",
              details: { type: "exit_spec_mode" },
            },
          ],
          options: [{ label: "No, keep iterating on spec", value: "cancel" }],
        });
        yield* begun.process.settle;
        const plan = plansOf(begun.opened.all)[0];
        assert.strictEqual(plan?.kind === "proposed_plan" && plan.markdown, "# From the input");
        assert.deepStrictEqual(answersFor(begun.process, "perm-input"), [
          { selectedOption: "cancel" },
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-PLAN-003] a newer plan supersedes the thread's previous one", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened);
        yield* proposePlan(opened, fake, thread, { markdown: "# First" });
        yield* proposePlan(opened, fake, thread, { runOrdinal: 2, markdown: "# Second" });

        const summary = plansOf(opened.all).map((plan) => [
          plan.kind === "proposed_plan" ? plan.markdown : "",
          plan.status,
        ]);
        assert.deepStrictEqual(summary, [
          ["# First", "active"],
          ["# First", "superseded"],
          ["# Second", "active"],
        ]);
        const [first, , second] = plansOf(opened.all);
        assert.notStrictEqual(first?.id, second?.id);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("todo lists", () => {
    const todoWrite = (toolUseId: string, todos: string) => ({
      type: "tool_call",
      toolUse: { type: "tool_use", id: toolUseId, name: "TodoWrite", input: { todos } },
    });

    it.effect("TodoWrite becomes the thread's todo list, superseded by the next turn's", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened);
        yield* startTurn(opened.runtime, thread);
        yield* fake.current().emit(todoWrite("todo-1", "1. [in_progress] Read\n2. [pending] Fix"));
        yield* fake.current().emit(todoWrite("todo-1", "1. [completed] Read\n2. [pending] Fix"));
        yield* fake.current().emit(completed(1));
        yield* opened.takeEvent(isTerminal);
        yield* startTurn(opened.runtime, thread, { runOrdinal: 2 });
        yield* fake.current().emit(todoWrite("todo-2", "1. [completed] Read\n2. [completed] Fix"));
        yield* fake.current().emit(completed(2));
        yield* opened.takeEvent(isTerminal);

        const plans = plansOf(opened.all);
        assert.deepStrictEqual(
          plans.map((plan) => [
            plan.kind,
            plan.status,
            plan.kind === "todo_list" ? plan.steps.map((step) => step.status) : [],
          ]),
          [
            ["todo_list", "active", ["running", "pending"]],
            ["todo_list", "active", ["completed", "pending"]],
            ["todo_list", "superseded", ["completed", "pending"]],
            ["todo_list", "completed", ["completed", "completed"]],
          ],
        );
        assert.strictEqual(plans[0]?.id, plans[1]?.id);
        assert.notStrictEqual(plans[1]?.id, plans[3]?.id);
        const todoItems = turnItemsOf(opened.all).filter((item) => item.type === "todo_list");
        assert.strictEqual(new Set(todoItems.map((item) => item.id)).size, 2);
        assert.isTrue(
          turnItemsOf(opened.all).some(
            (item) => item.type === "dynamic_tool" && item.toolName === "TodoWrite",
          ),
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("a TodoWrite made inside a Script also becomes the thread's todo list", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened);
        yield* startTurn(opened.runtime, thread);
        yield* fake.current().emit({
          type: "tool_call",
          toolUse: {
            type: "tool_use",
            id: "script-1",
            name: "Script",
            input: { script: "await tools.TodoWrite({ todos });" },
          },
        });
        yield* fake.current().emit({
          type: "create_message",
          message: {
            id: "message-inner-1",
            role: "assistant",
            visibility: "user_only",
            content: [
              {
                type: "tool_use",
                id: "inner-1",
                name: "TodoWrite",
                input: { todos: "1. [in_progress] Read\n2. [pending] Fix" },
                scriptExecution: { runId: "script-1", outerToolUseId: "script-1" },
              },
            ],
          },
        });
        yield* fake.current().emit(completed(1));
        yield* opened.takeEvent(isTerminal);

        assert.deepStrictEqual(
          plansOf(opened.all).map((plan) => [
            plan.kind,
            plan.kind === "todo_list" ? plan.steps.map((step) => step.status) : [],
          ]),
          [["todo_list", ["running", "pending"]]],
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("after the reply", () => {
    it.effect(
      "[VAL-PLAN-005] the spike's post-reply frames end the plan turn completed, and a settings_updated to auto is not applied",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const begun = yield* beginTurn(fake, { interactionMode: "plan" });
          const { process, opened } = begun;
          yield* process.emit(exitSpecToolCall());
          yield* process.emitInbound(REQUEST_ID, "droid.request_permission", EXIT_SPEC_PERMISSION);
          yield* process.settle;

          const postReply = spikePostReplyFrames(messageIdFor(1));
          const rest = postReply.slice(2);
          for (const frame of postReply.slice(0, 2)) {
            yield* process.emit(frame.notification, frame.scoped ? undefined : null);
          }
          // Droid does not do this after `cancel`; a proceed reply made it send one, and the
          // adapter must not read it as leaving spec mode.
          yield* process.emit({
            type: "settings_updated",
            settings: { interactionMode: "auto", autonomyLevel: "off" },
          });
          for (const frame of rest) {
            yield* process.emit(frame.notification, frame.scoped ? undefined : null);
          }
          const terminal = yield* opened.takeEvent(isTerminal);

          assert.strictEqual(terminal.type === "turn.terminal" && terminal.status, "completed");
          assert.strictEqual(terminal.type === "turn.terminal" && terminal.failure, null);
          assert.deepStrictEqual(
            [...new Set(turnItemsOf(opened.all).map((item) => item.type))].toSorted(),
            ["dynamic_tool", "proposed_plan"],
          );
          assert.isFalse(turnItemsOf(opened.all).some((item) => item.status === "failed"));
          assert.strictEqual(terminals(opened.all).length, 1);

          // The next default turn still has to switch Droid out of spec mode.
          yield* startTurn(opened.runtime, begun.providerThread, { runOrdinal: 2 });
          assert.deepStrictEqual(methodsOf(fake).slice(-2), [
            "droid.update_session_settings",
            "droid.add_user_message",
          ]);
          assert.deepStrictEqual(settingsFrames(fake).at(-1), {
            interactionMode: "auto",
            autonomyLevel: "off",
            modelId: MODEL,
            reasoningEffort: "high",
          });
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-PLAN-005] a plan turn ends without a command or file change and a second plan turn sends no switch",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          const thread = yield* ensure(opened);
          yield* proposePlan(opened, fake, thread);
          // Droid stays in spec mode after the cancel, so a second plan turn re-sends nothing.
          yield* proposePlan(opened, fake, thread, { runOrdinal: 2 });
          const types = new Set(turnItemsOf(opened.all).map((item) => item.type));
          assert.isFalse(types.has("command_execution"));
          assert.isFalse(types.has("file_change"));
          assert.strictEqual(settingsFrames(fake).length, 1);
          assert.deepStrictEqual(
            terminals(opened.all).map((terminal) => terminal.status),
            ["completed", "completed"],
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-PLAN-005] a default turn that is asked to leave spec mode still sends the switch next time",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const begun = yield* beginTurn(fake);
          yield* begun.process.emitInbound(
            REQUEST_ID,
            "droid.request_permission",
            EXIT_SPEC_PERMISSION,
          );
          yield* begun.process.settle;
          yield* begun.process.emit(completed(1));
          yield* begun.opened.takeEvent(isTerminal);

          yield* startTurn(begun.opened.runtime, begun.providerThread, { runOrdinal: 2 });
          assert.deepStrictEqual(
            settingsFrames(fake).map((params) => params["interactionMode"]),
            ["auto"],
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });
});
