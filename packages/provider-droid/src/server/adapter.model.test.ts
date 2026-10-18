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
  resume,
  runTurn,
  startTurn,
  type Events,
  type OpenRuntime,
} from "./adapterHarness.testkit.ts";
import {
  FAKE_DROID_DEFAULT_SETTINGS,
  makeFakeDroid,
  type FakeDroid,
  type FakeDroidOptions,
} from "./fakeDroid.testkit.ts";

const OTHER_MODEL = "gpt-5-codex";
const NO_EFFORT_MODEL = "no-effort-model";
const UNLISTED_DEFAULT_MODEL = "unlisted-default-model";

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
  { id: NO_EFFORT_MODEL, modelId: NO_EFFORT_MODEL, supportedReasoningEfforts: [] },
  {
    id: UNLISTED_DEFAULT_MODEL,
    modelId: UNLISTED_DEFAULT_MODEL,
    supportedReasoningEfforts: ["minimal", "high"],
    defaultReasoningEffort: "medium",
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

describe("DroidAdapterV2 effort rule", () => {
  interface Pick {
    readonly model: string;
    readonly effort: string | undefined;
    readonly interactionMode?: "default" | "plan";
  }

  /** `effort` left out is no explicit effort at all, which leaves it to the model's catalog default. */
  const on = (model: string, effort?: string, interactionMode?: "default" | "plan"): Pick => ({
    model,
    effort,
    ...(interactionMode === undefined ? {} : { interactionMode }),
  });
  const router = on("auto");
  const codex = (effort?: string, interactionMode?: "default" | "plan") =>
    on(OTHER_MODEL, effort, interactionMode);

  /** The settings frames each turn sent, first turn first. The thread starts on the first pick. */
  const framesPerTurn = Effect.fnUntraced(function* (
    first: Pick,
    later: ReadonlyArray<Pick>,
    fakeOptions: FakeDroidOptions = { availableModels: MODELS },
  ) {
    const fake = yield* makeFakeDroid(fakeOptions);
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
    return { fake, opened, thread, perTurn };
  });

  it.effect(
    "[VAL-HIST-012] a fresh session whose echo already matches the computed settings sends no frame, and repeats send none",
    () =>
      Effect.gen(function* () {
        const explicit = yield* framesPerTurn(on(MODEL, "high"), [
          on(MODEL, "high"),
          on(MODEL, "high"),
        ]);
        assert.deepStrictEqual(explicit.perTurn, [[], [], []]);

        // No list is known when the first initialize_session goes out, so it carries no effort;
        // Droid's echo then shows the model's own default, which is what the catalog says.
        const unchosen = yield* framesPerTurn(on(MODEL), [on(MODEL), on(MODEL)]);
        assert.notProperty(
          requestsOf(unchosen.fake, "droid.initialize_session")[0]?.params,
          "reasoningEffort",
        );
        assert.deepStrictEqual(unchosen.perTurn, [[], [], []]);
        assert.deepStrictEqual(methodsOf(unchosen.fake), [
          "droid.initialize_session",
          "droid.add_user_message",
          "droid.add_user_message",
          "droid.add_user_message",
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-012] a fresh session whose echo differs from the computed settings sends exactly one frame before the first message",
    () =>
      Effect.gen(function* () {
        const { fake, perTurn } = yield* framesPerTurn(codex(), [codex(), codex()]);
        // The fake's default effort is medium; the catalog says this model starts at low.
        assert.notProperty(
          requestsOf(fake, "droid.initialize_session")[0]?.params,
          "reasoningEffort",
        );
        assert.deepStrictEqual(perTurn, [[frame(OTHER_MODEL, "low")], [], []]);
        assert.deepStrictEqual(methodsOf(fake), [
          "droid.initialize_session",
          "droid.update_session_settings",
          "droid.add_user_message",
          "droid.add_user_message",
          "droid.add_user_message",
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("[VAL-HIST-012] the first initialize_session carries the explicit effort", () =>
    Effect.gen(function* () {
      const { fake, perTurn } = yield* framesPerTurn(codex("high"), [codex("high")]);
      assert.strictEqual(
        requestsOf(fake, "droid.initialize_session")[0]?.params?.["reasoningEffort"],
        "high",
      );
      assert.deepStrictEqual(perTurn, [[], []]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-012] clearing an explicit effort sends the model's catalog default, or nothing when Droid already holds it",
    () =>
      Effect.gen(function* () {
        const cleared = yield* framesPerTurn(codex("high"), [codex(), codex(), codex()]);
        assert.deepStrictEqual(cleared.perTurn, [[], [frame(OTHER_MODEL, "low")], [], []]);

        const held = yield* framesPerTurn(codex("low"), [codex(), codex()]);
        assert.deepStrictEqual(held.perTurn, [[], [], []], "the record already holds the default");

        const chosenLater = yield* framesPerTurn(codex(), [codex("high"), codex()]);
        assert.deepStrictEqual(chosenLater.perTurn, [
          [frame(OTHER_MODEL, "low")],
          [frame(OTHER_MODEL, "high")],
          [frame(OTHER_MODEL, "low")],
        ]);

        const other = yield* framesPerTurn(on(MODEL, "high"), [on(MODEL)]);
        assert.deepStrictEqual(other.perTurn, [[], [frame(MODEL, "medium")]]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-012] leaving the router with no explicit effort sends the catalog default, and repeats send none",
    () =>
      Effect.gen(function* () {
        const toModel = yield* framesPerTurn(router, [codex(), codex(), codex()]);
        assert.deepStrictEqual(toModel.perTurn, [[], [frame(OTHER_MODEL, "low")], [], []]);

        const explicit = yield* framesPerTurn(router, [codex("high"), codex(), codex()]);
        assert.deepStrictEqual(explicit.perTurn, [
          [],
          [frame(OTHER_MODEL, "high")],
          [frame(OTHER_MODEL, "low")],
          [],
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-012] switching to the router sends no effort, and the model after it asks for its own again",
    () =>
      Effect.gen(function* () {
        const { perTurn } = yield* framesPerTurn(codex("high"), [
          router,
          router,
          codex(),
          on(MODEL),
        ]);
        assert.deepStrictEqual(perTurn, [
          [],
          [frame("auto", undefined)],
          [],
          [frame(OTHER_MODEL, "low")],
          [frame(MODEL, "medium")],
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-012] spec and implement turns carry the same effort as a plain turn, and repeats send none",
    () =>
      Effect.gen(function* () {
        const fromRouter = yield* framesPerTurn(router, [
          codex(),
          codex(undefined, "plan"),
          codex(undefined, "plan"),
          codex(undefined, "default"),
          codex(undefined, "default"),
        ]);
        assert.deepStrictEqual(fromRouter.perTurn, [
          [],
          [frame(OTHER_MODEL, "low")],
          [specFrame(OTHER_MODEL, "low")],
          [],
          [frame(OTHER_MODEL, "low")],
          [],
        ]);

        const explicit = yield* framesPerTurn(codex("high"), [
          codex("high", "plan"),
          codex("high", "default"),
        ]);
        assert.deepStrictEqual(explicit.perTurn, [
          [],
          [specFrame(OTHER_MODEL, "high")],
          [frame(OTHER_MODEL, "high")],
        ]);

        const onTheRouter = yield* framesPerTurn(router, [
          on("auto", undefined, "plan"),
          on("auto", undefined, "default"),
        ]);
        assert.deepStrictEqual(onTheRouter.perTurn, [
          [],
          [specFrame("auto", undefined)],
          [frame("auto", undefined)],
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("[VAL-HIST-012] a model that lists no efforts sends none, in spec mode too", () =>
    Effect.gen(function* () {
      const { perTurn } = yield* framesPerTurn(on(MODEL, "high"), [
        on(NO_EFFORT_MODEL),
        on(NO_EFFORT_MODEL),
        on(NO_EFFORT_MODEL, undefined, "plan"),
      ]);
      assert.deepStrictEqual(perTurn, [
        [],
        [frame(NO_EFFORT_MODEL, undefined)],
        [],
        [specFrame(NO_EFFORT_MODEL, undefined)],
      ]);

      const fresh = yield* framesPerTurn(on(NO_EFFORT_MODEL), [on(NO_EFFORT_MODEL)]);
      assert.notProperty(
        requestsOf(fresh.fake, "droid.initialize_session")[0]?.params,
        "reasoningEffort",
      );
      assert.deepStrictEqual(fresh.perTurn, [[], []]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-012] a default effort the model does not list falls back to its first listed effort, as the picker does",
    () =>
      Effect.gen(function* () {
        const { perTurn } = yield* framesPerTurn(on(MODEL, "high"), [on(UNLISTED_DEFAULT_MODEL)]);
        assert.deepStrictEqual(perTurn, [[], [frame(UNLISTED_DEFAULT_MODEL, "minimal")]]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-012] a model the session does not list, or no list at all, sends no invented effort",
    () =>
      Effect.gen(function* () {
        const unlisted = yield* framesPerTurn(on(MODEL, "high"), [on("custom-model")]);
        assert.deepStrictEqual(unlisted.perTurn, [[], [frame("custom-model", undefined)]]);

        const noList = yield* framesPerTurn(codex(), [codex(), codex("high")], {});
        assert.deepStrictEqual(noList.perTurn, [[], [], [frame(OTHER_MODEL, "high")]]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-AUTO-006] a settings_updated that raises autonomy or moves the effort is corrected on the next turn",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ availableModels: MODELS });
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened, { model: OTHER_MODEL, effort: undefined });
        const same = { model: OTHER_MODEL, effort: undefined };
        yield* runTurn(opened, fake, thread, { runOrdinal: 1, ...same });
        assert.deepStrictEqual(settingsFrames(fake), [frame(OTHER_MODEL, "low")]);

        const report = (changes: Record<string, string>) =>
          fake.current().emit(
            {
              type: "settings_updated",
              settings: {
                ...FAKE_DROID_DEFAULT_SETTINGS,
                modelId: OTHER_MODEL,
                reasoningEffort: "low",
                ...changes,
              },
            },
            null,
          );

        yield* report({});
        yield* fake.current().settle;
        yield* runTurn(opened, fake, thread, { runOrdinal: 2, ...same });
        assert.strictEqual(
          settingsFrames(fake).length,
          1,
          "a report of the same settings changes nothing",
        );

        yield* report({ autonomyLevel: "high" });
        yield* fake.current().settle;
        yield* runTurn(opened, fake, thread, { runOrdinal: 3, ...same });
        assert.deepStrictEqual(settingsFrames(fake).slice(1), [frame(OTHER_MODEL, "low")]);

        yield* report({ reasoningEffort: "high" });
        yield* fake.current().settle;
        yield* runTurn(opened, fake, thread, { runOrdinal: 4, ...same });
        assert.deepStrictEqual(settingsFrames(fake).slice(2), [frame(OTHER_MODEL, "low")]);

        yield* runTurn(opened, fake, thread, { runOrdinal: 5, ...same });
        assert.strictEqual(settingsFrames(fake).length, 3, "the correction holds");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  describe("after a load_session", () => {
    const stored = (settings: Record<string, string>) => ({
      "stored-session": {
        settings: { ...FAKE_DROID_DEFAULT_SETTINGS, ...settings },
        messages: [],
      },
    });

    const afterResume = Effect.fnUntraced(function* (
      settings: Record<string, string>,
      picks: ReadonlyArray<Pick>,
    ) {
      const fake = yield* makeFakeDroid({
        availableModels: MODELS,
        sessions: stored(settings),
      });
      const opened = yield* openRuntime(fake);
      const thread = yield* resume(opened, "stored-session");
      for (const [index, pick] of picks.entries()) {
        yield* runTurn(opened, fake, thread, { runOrdinal: index + 1, ...pick });
      }
      return fake;
    });

    it.effect(
      "[VAL-HIST-012] exactly one full frame carries the computed effort before the first message, whatever the stored session holds",
      () =>
        Effect.gen(function* () {
          const differing = yield* afterResume({ modelId: OTHER_MODEL, reasoningEffort: "high" }, [
            codex(),
            codex(),
          ]);
          assert.deepStrictEqual(methodsOf(differing), [
            "droid.load_session",
            "droid.update_session_settings",
            "droid.add_user_message",
            "droid.add_user_message",
          ]);
          assert.deepStrictEqual(settingsFrames(differing), [frame(OTHER_MODEL, "low")]);

          const matching = yield* afterResume({ modelId: OTHER_MODEL, reasoningEffort: "low" }, [
            codex(),
            codex(),
          ]);
          assert.deepStrictEqual(
            settingsFrames(matching),
            [frame(OTHER_MODEL, "low")],
            "the load took no settings, so the frame is sent although the stored ones match",
          );

          const explicit = yield* afterResume({ modelId: OTHER_MODEL, reasoningEffort: "low" }, [
            codex("high"),
          ]);
          assert.deepStrictEqual(settingsFrames(explicit), [frame(OTHER_MODEL, "high")]);

          const onRouter = yield* afterResume({ modelId: OTHER_MODEL, reasoningEffort: "low" }, [
            router,
            router,
          ]);
          assert.deepStrictEqual(settingsFrames(onRouter), [frame("auto", undefined)]);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-HIST-012] a model change after the load follows the same rule", () =>
      Effect.gen(function* () {
        const fake = yield* afterResume({ modelId: OTHER_MODEL, reasoningEffort: "low" }, [
          codex(),
          on(MODEL),
          on(MODEL),
        ]);
        assert.deepStrictEqual(settingsFrames(fake), [
          frame(OTHER_MODEL, "low"),
          frame(MODEL, "medium"),
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });
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
