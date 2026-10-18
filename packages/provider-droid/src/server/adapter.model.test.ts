import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import {
  completed,
  ensure,
  isTerminal,
  layerTest,
  methodsOf,
  MODEL,
  openRuntime,
  providerThreadUpdates,
  providerTurnsOf,
  refIds,
  requestsOf,
  runTurn,
  startTurn,
  type Events,
  type OpenRuntime,
} from "./adapterHarness.testkit.ts";
import { makeFakeDroid, type FakeDroid } from "./fakeDroid.testkit.ts";

const OTHER_MODEL = "gpt-5-codex";

const MODELS = [
  {
    id: "auto",
    modelId: "auto",
    kind: "router",
    supportedReasoningEfforts: ["none"],
    defaultReasoningEffort: "none",
  },
  {
    id: MODEL,
    modelId: MODEL,
    supportedReasoningEfforts: ["low", "medium", "high"],
    defaultReasoningEffort: "medium",
  },
  {
    id: OTHER_MODEL,
    modelId: OTHER_MODEL,
    supportedReasoningEfforts: ["low", "medium", "high"],
    defaultReasoningEffort: "low",
  },
];

const settingsFrames = (fake: FakeDroid) =>
  requestsOf(fake, "droid.update_session_settings").map((request) => request.params);

const noticesOf = (events: Events) =>
  events.flatMap((event) =>
    event.type === "turn_item.updated" && event.turnItem.type === "system_notice"
      ? [event.turnItem]
      : [],
  );

const assistantReply = (modelId: string | undefined, id: string) =>
  ({
    type: "create_message",
    message: {
      id,
      role: "assistant",
      ...(modelId === undefined ? {} : { modelId, apiProvider: "test-provider" }),
      content: [{ type: "text", text: `answer ${id}` }],
    },
  }) as const;

/** The explicit settings frame of a turn in `approval-required` mode. */
const frame = (modelId: string, reasoningEffort: string | undefined) => ({
  interactionMode: "auto",
  autonomyLevel: "off",
  modelId,
  ...(reasoningEffort === undefined ? {} : { reasoningEffort }),
});

describe("DroidAdapterV2 model switch", () => {
  it.effect(
    "[VAL-HIST-011] a different model sends one settings frame before the message and stays on the same process and session",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ availableModels: MODELS });
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened);
        yield* runTurn(opened, fake, thread);
        yield* runTurn(opened, fake, thread, { runOrdinal: 2, model: OTHER_MODEL });

        assert.deepStrictEqual(methodsOf(fake), [
          "droid.initialize_session",
          "droid.add_user_message",
          "droid.update_session_settings",
          "droid.add_user_message",
        ]);
        assert.deepStrictEqual(settingsFrames(fake), [frame(OTHER_MODEL, "high")]);
        assert.strictEqual(fake.processes().length, 1, "no new process");
        assert.strictEqual(fake.sessions.size, 1, "no new session");
        assert.strictEqual(requestsOf(fake, "droid.load_session").length, 0);
        assert.deepStrictEqual(
          [...new Set(refIds(providerThreadUpdates(opened.all)))],
          [thread.nativeThreadRef?.nativeId],
          "the thread never moves to another session",
        );
        const models = opened.all.flatMap((event) =>
          event.type === "provider_session.updated" ? [event.providerSession.model] : [],
        );
        assert.strictEqual(models.at(-1), OTHER_MODEL, "the session reports the model in use");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-011] an effort-only change sends one settings frame that changes only the effort",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ availableModels: MODELS });
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened);
        yield* runTurn(opened, fake, thread);
        yield* runTurn(opened, fake, thread, { runOrdinal: 2, effort: "low" });

        assert.deepStrictEqual(methodsOf(fake), [
          "droid.initialize_session",
          "droid.add_user_message",
          "droid.update_session_settings",
          "droid.add_user_message",
        ]);
        assert.deepStrictEqual(settingsFrames(fake), [frame(MODEL, "low")]);
        assert.strictEqual(fake.sessions.size, 1);
        assert.strictEqual(fake.processes().length, 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("[VAL-HIST-011] a turn that changes neither model nor effort sends no frame", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeDroid({ availableModels: MODELS });
      const opened = yield* openRuntime(fake);
      const thread = yield* ensure(opened);
      yield* runTurn(opened, fake, thread);
      yield* runTurn(opened, fake, thread, { runOrdinal: 2 });
      assert.deepStrictEqual(settingsFrames(fake), []);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});

describe("DroidAdapterV2 real model ids and the router", () => {
  it.effect(
    "[VAL-HIST-012] a default selection is sent as the configured default model id, on initialize and on update",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ availableModels: MODELS });
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened, { model: "default" });
        yield* runTurn(opened, fake, thread, { model: "default" });
        yield* runTurn(opened, fake, thread, { runOrdinal: 2, model: OTHER_MODEL });
        yield* runTurn(opened, fake, thread, { runOrdinal: 3, model: "default" });

        const initialize = requestsOf(fake, "droid.initialize_session")[0]?.params;
        assert.strictEqual(initialize?.["modelId"], MODEL);
        assert.deepStrictEqual(
          settingsFrames(fake).map((params) => params["modelId"]),
          [OTHER_MODEL, MODEL],
          "a default selection switches the session back to the default model",
        );
        const sent = fake
          .current()
          .requests()
          .map((request) => JSON.stringify(request.params))
          .join();
        assert.notInclude(sent, '"modelId":"default"');
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-012] the router gets no reasoning effort on initialize, on update, or for spec mode",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ availableModels: MODELS });
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened, { model: "auto", effort: "high" });
        yield* runTurn(opened, fake, thread, { model: "auto", effort: "high" });
        yield* runTurn(opened, fake, thread, {
          runOrdinal: 2,
          model: "auto",
          effort: "high",
          interactionMode: "plan",
        });

        const initialize = requestsOf(fake, "droid.initialize_session")[0]?.params;
        assert.strictEqual(initialize?.["modelId"], "auto");
        assert.notProperty(initialize, "reasoningEffort");
        assert.deepStrictEqual(settingsFrames(fake), [
          {
            interactionMode: "spec",
            autonomyLevel: "off",
            modelId: "auto",
            specModeModelId: "auto",
          },
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("[VAL-HIST-012] switching to the router drops the effort from the frame", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeDroid({ availableModels: MODELS });
      const opened = yield* openRuntime(fake);
      const thread = yield* ensure(opened);
      yield* runTurn(opened, fake, thread);
      yield* runTurn(opened, fake, thread, { runOrdinal: 2, model: "auto", effort: "high" });
      assert.deepStrictEqual(settingsFrames(fake), [frame("auto", undefined)]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-012] switching away from the router sends the chosen effort, or the model's default effort when none was chosen",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ availableModels: MODELS });
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened, { model: "auto", effort: undefined });
        yield* runTurn(opened, fake, thread, { model: "auto", effort: undefined });
        yield* runTurn(opened, fake, thread, { runOrdinal: 2, model: MODEL, effort: "high" });
        yield* runTurn(opened, fake, thread, { runOrdinal: 3, model: "auto", effort: undefined });
        yield* runTurn(opened, fake, thread, {
          runOrdinal: 4,
          model: OTHER_MODEL,
          effort: undefined,
        });

        assert.deepStrictEqual(settingsFrames(fake), [
          frame(MODEL, "high"),
          frame("auto", undefined),
          frame(OTHER_MODEL, "low"),
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-012] leaving the router on a session whose model list is unknown sends no invented effort",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened, { model: "auto", effort: undefined });
        yield* runTurn(opened, fake, thread, { model: "auto", effort: undefined });
        yield* runTurn(opened, fake, thread, { runOrdinal: 2, model: MODEL, effort: undefined });
        assert.deepStrictEqual(settingsFrames(fake), [frame(MODEL, undefined)]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});

describe("DroidAdapterV2 reroute notice", () => {
  const replyTurn = Effect.fnUntraced(function* (
    opened: OpenRuntime,
    fake: FakeDroid,
    thread: Parameters<typeof startTurn>[1],
    runOrdinal: number,
    models: ReadonlyArray<string | undefined>,
    options: { readonly model?: string } = {},
  ) {
    yield* startTurn(opened.runtime, thread, { runOrdinal, ...options });
    for (const [index, modelId] of models.entries()) {
      yield* fake.current().emit(assistantReply(modelId, `r${runOrdinal}-${index}`));
    }
    yield* fake.current().emit(completed(runOrdinal));
    yield* opened.takeEvent(isTerminal);
  });

  it.effect(
    "[VAL-HIST-014] one system_notice per rerouted model per turn; none when the model matches or the router was asked",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ availableModels: MODELS });
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened);

        yield* replyTurn(opened, fake, thread, 1, [OTHER_MODEL, OTHER_MODEL]);
        const first = noticesOf(opened.all);
        assert.strictEqual(first.length, 1, "a repeated reroute adds no notice");
        assert.include(first[0]?.message, OTHER_MODEL);
        assert.include(first[0]?.message, MODEL);
        assert.strictEqual(first[0]?.providerTurnId, providerTurnsOf(opened.all)[0]?.id);

        yield* replyTurn(opened, fake, thread, 2, [MODEL, undefined]);
        assert.strictEqual(noticesOf(opened.all).length, 1, "matching models add nothing");

        yield* replyTurn(opened, fake, thread, 3, [OTHER_MODEL]);
        assert.strictEqual(noticesOf(opened.all).length, 2, "a new turn notices a reroute again");

        yield* replyTurn(opened, fake, thread, 4, ["claude-sonnet-5"], { model: "auto" });
        assert.strictEqual(noticesOf(opened.all).length, 2, "the router picks its own model");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});

describe("DroidAdapterV2 model switch and spec mode", () => {
  it.effect(
    "[VAL-CROSS-006] a model switch carries into the plan turn's spec settings and the implement turn",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ availableModels: MODELS });
        const opened = yield* openRuntime(fake, { runtimeMode: "auto" });
        const thread = yield* ensure(opened, { runtimeMode: "auto" });
        const options = { runtimeMode: "auto" } as const;

        yield* runTurn(opened, fake, thread, { ...options, runOrdinal: 1 });
        yield* runTurn(opened, fake, thread, {
          ...options,
          runOrdinal: 2,
          model: OTHER_MODEL,
          effort: "low",
        });
        yield* runTurn(opened, fake, thread, {
          ...options,
          runOrdinal: 3,
          model: OTHER_MODEL,
          effort: "low",
          interactionMode: "plan",
        });
        yield* runTurn(opened, fake, thread, {
          ...options,
          runOrdinal: 4,
          model: OTHER_MODEL,
          effort: "low",
          interactionMode: "default",
        });

        assert.deepStrictEqual(settingsFrames(fake), [
          // The switch turn.
          {
            interactionMode: "auto",
            autonomyLevel: "medium",
            modelId: OTHER_MODEL,
            reasoningEffort: "low",
          },
          // The plan turn: spec mode on the new model and effort.
          {
            interactionMode: "spec",
            autonomyLevel: "medium",
            modelId: OTHER_MODEL,
            reasoningEffort: "low",
            specModeModelId: OTHER_MODEL,
            specModeReasoningEffort: "low",
          },
          // The implement turn: auto mode, the new model, the runtime mode's autonomy.
          {
            interactionMode: "auto",
            autonomyLevel: "medium",
            modelId: OTHER_MODEL,
            reasoningEffort: "low",
          },
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-CROSS-006] a plan turn that itself switches the model sets the spec model to it",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ availableModels: MODELS });
        const opened = yield* openRuntime(fake, { runtimeMode: "auto" });
        const thread = yield* ensure(opened, { runtimeMode: "auto" });
        yield* runTurn(opened, fake, thread, { runtimeMode: "auto" });
        yield* runTurn(opened, fake, thread, {
          runtimeMode: "auto",
          runOrdinal: 2,
          model: OTHER_MODEL,
          effort: "low",
          interactionMode: "plan",
        });
        const frames = settingsFrames(fake);
        assert.strictEqual(frames.length, 1);
        assert.strictEqual(frames[0]?.["specModeModelId"], OTHER_MODEL);
        assert.strictEqual(frames[0]?.["specModeReasoningEffort"], "low");
        assert.strictEqual(frames[0]?.["modelId"], OTHER_MODEL);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
