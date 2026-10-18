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

/** The explicit settings frame of a plan turn in `approval-required` mode. */
const specFrame = (modelId: string, reasoningEffort: string | undefined) => ({
  interactionMode: "spec",
  autonomyLevel: "off",
  modelId,
  ...(reasoningEffort === undefined ? {} : { reasoningEffort }),
  specModeModelId: modelId,
  ...(reasoningEffort === undefined ? {} : { specModeReasoningEffort: reasoningEffort }),
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

describe("DroidAdapterV2 effort stability after the router", () => {
  it.effect(
    "[VAL-HIST-012] leaving the router for a model's default effort sends one frame, then an identical turn sends none",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ availableModels: MODELS });
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened, { model: "auto", effort: undefined });
        yield* runTurn(opened, fake, thread, { model: "auto", effort: undefined });
        yield* runTurn(opened, fake, thread, {
          runOrdinal: 2,
          model: OTHER_MODEL,
          effort: undefined,
        });
        assert.deepStrictEqual(settingsFrames(fake), [frame(OTHER_MODEL, "low")]);

        yield* runTurn(opened, fake, thread, {
          runOrdinal: 3,
          model: OTHER_MODEL,
          effort: undefined,
        });
        yield* runTurn(opened, fake, thread, {
          runOrdinal: 4,
          model: OTHER_MODEL,
          effort: undefined,
        });
        assert.deepStrictEqual(
          settingsFrames(fake),
          [frame(OTHER_MODEL, "low")],
          "an unchanged selection and policy sends no settings frame",
        );
        assert.deepStrictEqual(methodsOf(fake), [
          "droid.initialize_session",
          "droid.add_user_message",
          "droid.update_session_settings",
          "droid.add_user_message",
          "droid.add_user_message",
          "droid.add_user_message",
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-012] spec mode and the implement turn after the restored default keep sending that effort",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ availableModels: MODELS });
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened, { model: "auto", effort: undefined });
        yield* runTurn(opened, fake, thread, { model: "auto", effort: undefined });
        const named = { model: OTHER_MODEL, effort: undefined };
        yield* runTurn(opened, fake, thread, { runOrdinal: 2, ...named });
        yield* runTurn(opened, fake, thread, { runOrdinal: 3, ...named });
        yield* runTurn(opened, fake, thread, {
          runOrdinal: 4,
          ...named,
          interactionMode: "plan",
        });
        yield* runTurn(opened, fake, thread, {
          runOrdinal: 5,
          ...named,
          interactionMode: "plan",
        });
        yield* runTurn(opened, fake, thread, {
          runOrdinal: 6,
          ...named,
          interactionMode: "default",
        });
        yield* runTurn(opened, fake, thread, {
          runOrdinal: 7,
          ...named,
          interactionMode: "default",
        });

        assert.deepStrictEqual(settingsFrames(fake), [
          frame(OTHER_MODEL, "low"),
          specFrame(OTHER_MODEL, "low"),
          frame(OTHER_MODEL, "low"),
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-012] an explicit effort or another model after the restored default behaves as before",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ availableModels: MODELS });
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened, { model: "auto", effort: undefined });
        yield* runTurn(opened, fake, thread, { model: "auto", effort: undefined });
        yield* runTurn(opened, fake, thread, {
          runOrdinal: 2,
          model: OTHER_MODEL,
          effort: undefined,
        });
        yield* runTurn(opened, fake, thread, { runOrdinal: 3, model: OTHER_MODEL, effort: "high" });
        yield* runTurn(opened, fake, thread, {
          runOrdinal: 4,
          model: OTHER_MODEL,
          effort: undefined,
        });
        yield* runTurn(opened, fake, thread, { runOrdinal: 5, model: MODEL, effort: undefined });

        assert.deepStrictEqual(settingsFrames(fake), [
          frame(OTHER_MODEL, "low"),
          frame(OTHER_MODEL, "high"),
          frame(OTHER_MODEL, undefined),
          frame(MODEL, undefined),
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-012] a fresh session and a non-router model with no chosen effort send no invented effort",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ availableModels: MODELS });
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened, { effort: undefined });
        yield* runTurn(opened, fake, thread, { effort: undefined });
        yield* runTurn(opened, fake, thread, { runOrdinal: 2, effort: undefined });
        yield* runTurn(opened, fake, thread, {
          runOrdinal: 3,
          effort: undefined,
          interactionMode: "plan",
        });
        yield* runTurn(opened, fake, thread, {
          runOrdinal: 4,
          model: OTHER_MODEL,
          effort: undefined,
        });
        yield* runTurn(opened, fake, thread, {
          runOrdinal: 5,
          model: OTHER_MODEL,
          effort: undefined,
        });

        const initialize = requestsOf(fake, "droid.initialize_session")[0]?.params;
        assert.notProperty(initialize, "reasoningEffort");
        assert.deepStrictEqual(settingsFrames(fake), [
          specFrame(MODEL, undefined),
          frame(OTHER_MODEL, undefined),
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});

describe("DroidAdapterV2 effort provenance", () => {
  interface Pick {
    readonly model: string;
    readonly effort: string | undefined;
    readonly interactionMode?: "default" | "plan";
  }

  /** `effort` left out is no chosen effort at all, which leaves it to the model's own default. */
  const on = (model: string, effort?: string, interactionMode?: "default" | "plan"): Pick => ({
    model,
    effort,
    ...(interactionMode === undefined ? {} : { interactionMode }),
  });
  const router = on("auto");
  const codex = (effort?: string, interactionMode?: "default" | "plan") =>
    on(OTHER_MODEL, effort, interactionMode);

  /** The settings frames each turn sent, first turn first. The thread starts on the first pick. */
  const framesPerTurn = Effect.fnUntraced(function* (first: Pick, later: ReadonlyArray<Pick>) {
    const fake = yield* makeFakeDroid({ availableModels: MODELS });
    const opened = yield* openRuntime(fake);
    const thread = yield* ensure(opened, { model: first.model, effort: first.effort });
    const perTurn: Array<ReturnType<typeof settingsFrames>> = [];
    let sent = 0;
    for (const [index, pick] of [first, ...later].entries()) {
      yield* runTurn(opened, fake, thread, { runOrdinal: index + 1, ...pick });
      const frames = settingsFrames(fake);
      perTurn.push(frames.slice(sent));
      sent = frames.length;
    }
    return { fake, perTurn };
  });

  it.effect(
    "[VAL-HIST-012] case 1: a fresh session with no chosen effort sends none, whatever the model's default",
    () =>
      Effect.gen(function* () {
        const { fake, perTurn } = yield* framesPerTurn(codex(), [
          codex(),
          codex(undefined, "plan"),
        ]);
        const initialize = requestsOf(fake, "droid.initialize_session")[0]?.params;
        assert.notProperty(initialize, "reasoningEffort");
        assert.deepStrictEqual(perTurn, [[], [], [specFrame(OTHER_MODEL, undefined)]]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-012] case 2: leaving the router with no chosen effort sends one frame carrying the model's default",
    () =>
      Effect.gen(function* () {
        const { perTurn } = yield* framesPerTurn(router, [codex()]);
        assert.deepStrictEqual(perTurn, [[], [frame(OTHER_MODEL, "low")]]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-012] case 3: an identical turn after the restored default sends no frame",
    () =>
      Effect.gen(function* () {
        const { perTurn } = yield* framesPerTurn(router, [codex(), codex(), codex()]);
        assert.deepStrictEqual(perTurn, [[], [frame(OTHER_MODEL, "low")], [], []]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-012] case 4: spec and implement turns after the restored default all carry it, and repeats send none",
    () =>
      Effect.gen(function* () {
        const { perTurn } = yield* framesPerTurn(router, [
          codex(),
          codex(undefined, "plan"),
          codex(undefined, "plan"),
          codex(undefined, "default"),
          codex(undefined, "default"),
        ]);
        assert.deepStrictEqual(perTurn, [
          [],
          [frame(OTHER_MODEL, "low")],
          [specFrame(OTHER_MODEL, "low")],
          [],
          [frame(OTHER_MODEL, "low")],
          [],
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-012] case 5: an explicit effort, one equal to the model's default included, is sent when the wire value changes",
    () =>
      Effect.gen(function* () {
        const fresh = yield* framesPerTurn(codex("low"), [
          codex("low"),
          codex("high"),
          codex("low"),
          codex("low"),
        ]);
        assert.strictEqual(
          requestsOf(fresh.fake, "droid.initialize_session")[0]?.params?.["reasoningEffort"],
          "low",
        );
        assert.deepStrictEqual(fresh.perTurn, [
          [],
          [],
          [frame(OTHER_MODEL, "high")],
          [frame(OTHER_MODEL, "low")],
          [],
        ]);

        const fromNone = yield* framesPerTurn(codex(), [codex("low"), codex("low")]);
        assert.deepStrictEqual(fromNone.perTurn, [[], [frame(OTHER_MODEL, "low")], []]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-012] case 6: dropping an explicit effort sends a frame without one, even when it equals the model's default",
    () =>
      Effect.gen(function* () {
        for (const effort of ["low", "high"]) {
          const { perTurn } = yield* framesPerTurn(codex(effort), [
            codex(),
            codex(undefined, "plan"),
            codex(undefined, "plan"),
            codex(undefined, "default"),
          ]);
          assert.deepStrictEqual(
            perTurn,
            [
              [],
              [frame(OTHER_MODEL, undefined)],
              [specFrame(OTHER_MODEL, undefined)],
              [],
              [frame(OTHER_MODEL, undefined)],
            ],
            `explicit ${effort} on a fresh session, then none`,
          );
        }

        const chosenLater = yield* framesPerTurn(codex(), [codex("low"), codex()]);
        assert.deepStrictEqual(
          chosenLater.perTurn,
          [[], [frame(OTHER_MODEL, "low")], [frame(OTHER_MODEL, undefined)]],
          "an explicit default reached by a selection change is explicit too",
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-012] case 7: selecting the restored default explicitly sends no frame, and dropping it afterwards sends one without an effort",
    () =>
      Effect.gen(function* () {
        const { perTurn } = yield* framesPerTurn(router, [
          codex(),
          codex("low"),
          codex(),
          codex(undefined, "plan"),
        ]);
        assert.deepStrictEqual(perTurn, [
          [],
          [frame(OTHER_MODEL, "low")],
          [],
          [frame(OTHER_MODEL, undefined)],
          [specFrame(OTHER_MODEL, undefined)],
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-012] case 8: a model change with no chosen effort sends none, and never inherits a restored effort",
    () =>
      Effect.gen(function* () {
        const fromModel = yield* framesPerTurn(on(MODEL), [codex(), codex()]);
        assert.deepStrictEqual(fromModel.perTurn, [[], [frame(OTHER_MODEL, undefined)], []]);

        const fromRestored = yield* framesPerTurn(router, [codex(), on(MODEL), codex()]);
        assert.deepStrictEqual(fromRestored.perTurn, [
          [],
          [frame(OTHER_MODEL, "low")],
          [frame(MODEL, undefined)],
          [frame(OTHER_MODEL, undefined)],
        ]);
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
