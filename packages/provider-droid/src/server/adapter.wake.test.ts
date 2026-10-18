import { assert, describe, it } from "@effect/vitest";
import { ProviderThreadId, type OrchestrationV2ProviderThread } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";

import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

import {
  assistantItems,
  assistantSays,
  captureLogs,
  completed,
  compactTurn,
  ensure,
  isTerminal,
  layerTest,
  makeOfferQueue,
  makeProviderThread,
  messageIdFor,
  methodsOf,
  openItems,
  openRuntime,
  providerTurnsOf,
  requestsOf,
  runIdFor,
  sessionStatuses,
  startTurn,
  terminals,
  wakeCompleted,
  wakeMessage,
  type Events,
  type OpenRuntime,
} from "./adapterHarness.testkit.ts";
import { FAKE_DROID_DEFAULT_SETTINGS, makeFakeDroid } from "./fakeDroid.testkit.ts";

const isTurnStartError = Schema.is(ProviderAdapter.ProviderAdapterTurnStartError);
const isInterruptError = Schema.is(ProviderAdapter.ProviderAdapterInterruptError);
const isResumeError = Schema.is(ProviderAdapter.ProviderAdapterResumeThreadError);

const WAKE = "3a1b2c3d";

/** An idle runtime on a fake Droid whose continuation offers land in a queue the test reads. */
const idleRuntime = Effect.fnUntraced(function* (
  options: { readonly sessions?: Parameters<typeof makeFakeDroid>[0] } = {},
) {
  const fake = yield* makeFakeDroid(options.sessions);
  yield* fake.deliverGroupSignals;
  const { offers, continuationRequests } = yield* makeOfferQueue();
  const opened = yield* openRuntime(fake, { continuationRequests, runtimeMode: "auto" });
  const thread = yield* ensure(opened, { runtimeMode: "auto" });
  yield* opened.takeEvent((event) => event.type === "provider_thread.updated");
  return { fake, opened, thread, offers, process: fake.current() };
});

/** Droid starts the turn of its own that a background task's report opens, and it answers. */
const wakeStarts = Effect.fnUntraced(function* (
  { process }: Pick<Effect.Success<ReturnType<typeof idleRuntime>>, "process">,
  options: { readonly id?: string; readonly reply?: string; readonly complete?: boolean } = {},
) {
  const id = options.id ?? WAKE;
  yield* process.emit({
    type: "droid_working_state_changed",
    newState: "streaming_assistant_message",
  });
  yield* process.emit(wakeMessage(id));
  if (options.reply !== undefined) yield* assistantSays(process, `reply-${id}`, options.reply);
  if (options.complete === true) {
    yield* process.emit(wakeCompleted(id));
    yield* process.emit({ type: "droid_working_state_changed", newState: "idle" });
  }
  yield* process.settle;
});

const pending = (opened: OpenRuntime, thread: OrchestrationV2ProviderThread) =>
  Effect.gen(function* () {
    return [
      yield* opened.runtime.hasPendingBackgroundWork!,
      yield* opened.runtime.hasPendingBackgroundWorkForThread!(thread),
    ];
  });

const dispatch = (offer: { readonly dispatchIfCurrent?: unknown }) => {
  const run = offer.dispatchIfCurrent;
  return typeof run === "function"
    ? (run as <A>(effect: Effect.Effect<A>) => Effect.Effect<Option.Option<A>>)(
        Effect.succeed("dispatched"),
      )
    : Effect.die("the offer has no dispatchIfCurrent");
};

/** Waits for the runtime to publish the finished assistant message `text`. */
const assistantCompleted = (opened: OpenRuntime, text: string) =>
  opened.takeEvent(
    (event) =>
      event.type === "turn_item.updated" &&
      event.turnItem.type === "assistant_message" &&
      event.turnItem.status === "completed" &&
      event.turnItem.text === text,
  );

const sessionBecomes = (opened: OpenRuntime, status: string) =>
  opened.takeEvent(
    (event) => event.type === "provider_session.updated" && event.providerSession.status === status,
  );

const turnEvents = (events: Events) =>
  events.filter((event) =>
    ["provider_turn.updated", "turn_item.updated", "turn.terminal"].includes(event.type),
  );

const itemsOfRun = (events: Events, runOrdinal: number) =>
  events.filter(
    (event) => event.type === "turn_item.updated" && event.turnItem.runId === runIdFor(runOrdinal),
  );

const completedTexts = (events: Events) =>
  assistantItems(events)
    .filter((item) => item.status === "completed")
    .map((item) => (item.type === "assistant_message" ? item.text : ""));

describe("DroidAdapterV2 wake buffering", () => {
  it.effect(
    "[VAL-SUB-009] a wake while idle is offered once and buffered, then replayed in order into the continuation turn and continued live",
    () =>
      Effect.gen(function* () {
        const { fake, opened, thread, offers, process } = yield* idleRuntime();

        yield* wakeStarts({ process }, { reply: "The subagent found two bugs." });
        const offer = yield* Queue.take(offers);
        yield* sessionBecomes(opened, "running");

        assert.strictEqual(offer.threadId, thread.appThreadId);
        assert.strictEqual(offer.providerThreadId, thread.id);
        assert.strictEqual(offer.driver, "droid");
        assert.strictEqual(offer.delivery, "adapter_buffered");
        assert.strictEqual(yield* Queue.size(offers), 0, "one offer for the whole wake");
        assert.deepStrictEqual(yield* pending(opened, thread), [true, true]);

        // More of the same wake, while it is still waiting for the orchestrator.
        yield* assistantSays(process, "reply-more", "Both are in a.ts.");
        yield* process.settle;
        assert.strictEqual(yield* Queue.size(offers), 0);

        const dispatched = yield* dispatch(offer);
        assert.isTrue(Option.isSome(dispatched));
        assert.deepStrictEqual(yield* pending(opened, thread), [true, true]);

        yield* startTurn(opened.runtime, thread, {
          runOrdinal: 2,
          continuation: true,
          runtimeMode: "auto",
        });
        yield* assistantCompleted(opened, "Both are in a.ts.");
        const turn = providerTurnsOf(opened.all).at(-1);
        assert.strictEqual(turn?.nativeTurnRef?.nativeId, `task-completion:${WAKE}`);
        assert.deepStrictEqual(yield* pending(opened, thread), [false, false]);
        assert.deepStrictEqual(
          completedTexts(opened.all),
          ["The subagent found two bugs.", "Both are in a.ts."],
          "the buffered messages arrive in order inside the continuation turn",
        );
        assert.isTrue(
          assistantItems(opened.all).every((item) => item.providerTurnId === turn?.id),
          "every replayed item belongs to the continuation turn",
        );
        assert.strictEqual(
          turnEvents(opened.all)[0]?.type,
          "provider_turn.updated",
          "nothing reaches the orchestrator before a turn owns the wake",
        );
        assert.strictEqual(terminals(opened.all).length, 0, "the wake is still running");

        // Live from here on.
        yield* assistantSays(process, "reply-live", "Fixing them now.");
        yield* process.emit(wakeCompleted(WAKE));
        const terminal = yield* opened.takeEvent(isTerminal);

        assert.isTrue(
          terminal.type === "turn.terminal" &&
            terminal.status === "completed" &&
            terminal.runOrdinal === 2 &&
            terminal.threadDisposition === "reusable",
        );
        assert.deepStrictEqual(completedTexts(opened.all), [
          "The subagent found two bugs.",
          "Both are in a.ts.",
          "Fixing them now.",
        ]);
        assert.deepStrictEqual(openItems(opened.all), []);
        assert.strictEqual(sessionStatuses(opened.all).at(-1), "ready");
        assert.deepStrictEqual(
          methodsOf(fake),
          ["droid.initialize_session"],
          "no settings and no user message go out for a continuation",
        );
        assert.isTrue(Option.isNone(yield* dispatch(offer)), "the consumed wake is not current");
        assert.strictEqual(yield* Queue.size(offers), 0);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-SUB-009] a wake that already finished is replayed whole and the continuation turn ends with it",
    () =>
      Effect.gen(function* () {
        const { opened, thread, offers, process } = yield* idleRuntime();

        yield* wakeStarts({ process }, { reply: "All done.", complete: true });
        yield* Queue.take(offers);
        yield* sessionBecomes(opened, "running");
        yield* sessionBecomes(opened, "ready");
        assert.deepStrictEqual(yield* pending(opened, thread), [true, true]);

        yield* startTurn(opened.runtime, thread, {
          runOrdinal: 2,
          continuation: true,
          runtimeMode: "auto",
        });
        const terminal = yield* opened.takeEvent(isTerminal);

        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
        assert.deepStrictEqual(completedTexts(opened.all), ["All done."]);
        assert.deepStrictEqual(yield* pending(opened, thread), [false, false]);
        assert.strictEqual(terminals(opened.all).length, 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-SUB-009] a permission request the wake raises is held until a turn owns it, then shown and answered",
    () =>
      Effect.gen(function* () {
        const { opened, thread, offers, process } = yield* idleRuntime();
        yield* process.emit(wakeMessage(WAKE));
        yield* process.emit({
          type: "tool_call",
          toolUse: { type: "tool_use", id: "tool-w", name: "Execute", input: { command: "ls" } },
        });
        yield* process.emitInbound("perm-w", "droid.request_permission", {
          toolUses: [
            {
              toolUse: {
                type: "tool_use",
                id: "tool-w",
                name: "Execute",
                input: { command: "ls" },
              },
              confirmationType: "exec",
              details: { type: "exec", fullCommand: "ls", command: "ls" },
            },
          ],
          options: [
            { label: "Yes, allow", value: "proceed_once" },
            { label: "No, cancel", value: "cancel" },
          ],
        });
        yield* process.settle;
        yield* Queue.take(offers);
        assert.isFalse(
          process.answers().some((answer) => answer.id === "perm-w"),
          "Droid is left waiting for a turn to show the request",
        );
        assert.strictEqual(
          opened.all.filter((event) => event.type === "runtime_request.updated").length,
          0,
        );

        yield* startTurn(opened.runtime, thread, {
          runOrdinal: 2,
          continuation: true,
          runtimeMode: "approval-required",
        });
        const shown = yield* opened.takeEvent(
          (event) =>
            event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
        );
        if (shown.type !== "runtime_request.updated") return assert.fail("no request");
        yield* opened.runtime.respondToRuntimeRequest({
          requestId: shown.runtimeRequest.id,
          decision: "accept",
        } as never);
        yield* process.settle;
        assert.isTrue(process.answers().some((answer) => answer.id === "perm-w"));
        yield* process.emit({
          type: "tool_result",
          toolUseId: "tool-w",
          content: "a.ts",
          isError: false,
        });
        yield* process.emit(wakeCompleted(WAKE));
        const terminal = yield* opened.takeEvent(isTerminal);
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
        assert.deepStrictEqual(openItems(opened.all), []);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-SUB-009] a second report behind a finished wake opens a wake of its own once the first turn ends",
    () =>
      Effect.gen(function* () {
        const { opened, thread, offers, process } = yield* idleRuntime();

        yield* wakeStarts({ process }, { reply: "First.", complete: true });
        yield* process.emit(wakeMessage("second"));
        yield* assistantSays(process, "reply-second", "Second.");
        yield* process.settle;
        yield* Queue.take(offers);
        assert.strictEqual(yield* Queue.size(offers), 0, "the second report joins the first wake");

        yield* startTurn(opened.runtime, thread, {
          runOrdinal: 2,
          continuation: true,
          runtimeMode: "auto",
        });
        yield* opened.takeEvent(isTerminal);
        const second = yield* Queue.take(offers);

        assert.deepStrictEqual(completedTexts(opened.all), ["First."]);
        assert.strictEqual(second.delivery, "adapter_buffered");
        assert.deepStrictEqual(yield* pending(opened, thread), [true, true]);

        yield* startTurn(opened.runtime, thread, {
          runOrdinal: 3,
          continuation: true,
          runtimeMode: "auto",
        });
        yield* process.emit(wakeCompleted("second"));
        yield* opened.takeEvent(
          (event) => event.type === "turn.terminal" && event.runOrdinal === 3,
        );
        assert.deepStrictEqual(completedTexts(opened.all), ["First.", "Second."]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-SUB-010] a report during an active turn folds into it: no offer, and the turn ends once",
    () =>
      Effect.gen(function* () {
        const { opened, thread, offers, process } = yield* idleRuntime();
        yield* startTurn(opened.runtime, thread, { runtimeMode: "auto" });

        yield* process.emit(wakeMessage(WAKE));
        yield* assistantSays(process, "reply-in-turn", "Folded in.");
        yield* process.emit(completed(1));
        yield* opened.takeEvent(isTerminal);

        assert.strictEqual(yield* Queue.size(offers), 0, "no continuation is offered");
        assert.strictEqual(terminals(opened.all).length, 1);
        assert.deepStrictEqual(completedTexts(opened.all), ["Folded in."]);
        assert.deepStrictEqual(yield* pending(opened, thread), [false, false]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  describe("[VAL-SUB-011] a user turn takes over a buffered wake", () => {
    const userEcho = (text: string) => ({
      type: "create_message",
      message: { id: messageIdFor(1), role: "user", content: [{ type: "text", text }] },
    });

    it.effect(
      "queued behind the running wake: its events land in the user's turn, the message is sent, and the turn ends with the message",
      () =>
        Effect.gen(function* () {
          const { fake, opened, thread, offers, process } = yield* idleRuntime();
          yield* wakeStarts({ process }, { reply: "Wake output." });
          const offer = yield* Queue.take(offers);

          yield* startTurn(opened.runtime, thread, { runtimeMode: "auto", text: "hello" });
          yield* assistantCompleted(opened, "Wake output.");
          const turn = providerTurnsOf(opened.all).at(-1);

          assert.deepStrictEqual(
            requestsOf(fake, "droid.add_user_message").map((request) => request.params),
            [
              {
                text: "hello",
                messageId: messageIdFor(1),
                queuePlacement: "end_of_turn",
              },
            ],
            "the user's message still goes to Droid, queued behind the wake's turn",
          );
          assert.deepStrictEqual(completedTexts(opened.all), ["Wake output."]);
          assert.strictEqual(assistantItems(opened.all)[0]?.runId, runIdFor(1));
          assert.strictEqual(assistantItems(opened.all)[0]?.providerTurnId, turn?.id);
          assert.deepStrictEqual(yield* pending(opened, thread), [false, false]);
          assert.isTrue(Option.isNone(yield* dispatch(offer)), "the taken wake is not current");

          // The wake ends first. The user's message has not begun, so the turn stays open.
          yield* process.emit(wakeCompleted(WAKE));
          yield* process.emit(userEcho("hello"));
          yield* assistantSays(process, "reply-user", "Hi there.");
          yield* assistantCompleted(opened, "Hi there.");
          assert.strictEqual(terminals(opened.all).length, 0, "no premature terminal");

          yield* process.emit(completed(1));
          const terminal = yield* opened.takeEvent(isTerminal);

          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.deepStrictEqual(completedTexts(opened.all), ["Wake output.", "Hi there."]);
          assert.strictEqual(terminals(opened.all).length, 1);

          // The continuation the orchestrator had queued finds no wake and ends at once.
          yield* startTurn(opened.runtime, thread, {
            runOrdinal: 2,
            continuation: true,
            runtimeMode: "auto",
          });
          const second = yield* opened.takeEvent(
            (event) => event.type === "turn.terminal" && event.runOrdinal === 2,
          );
          assert.isTrue(
            second.type === "turn.terminal" &&
              second.status === "completed" &&
              second.failure === null,
          );
          assert.deepStrictEqual(itemsOfRun(opened.all, 2), [], "an empty turn: no items");
          assert.strictEqual(requestsOf(fake, "droid.add_user_message").length, 1);
          assert.strictEqual(terminals(opened.all).length, 2);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("joined into the running wake turn: the wake's completion ends the user's turn", () =>
      Effect.gen(function* () {
        const { opened, thread, offers, process } = yield* idleRuntime();
        yield* wakeStarts({ process }, { reply: "Wake output." });
        yield* Queue.take(offers);

        yield* startTurn(opened.runtime, thread, { runtimeMode: "auto", text: "hello" });
        // Droid took the message into the turn it was already running.
        yield* process.emit(userEcho("hello"));
        yield* assistantSays(process, "reply-joined", "Both handled.");
        yield* process.emit(wakeCompleted(WAKE));
        const terminal = yield* opened.takeEvent(isTerminal);

        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
        assert.deepStrictEqual(completedTexts(opened.all), ["Wake output.", "Both handled."]);
        assert.strictEqual(terminals(opened.all).length, 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "after the wake finished: the message goes out as an ordinary one and its own completion ends the turn",
      () =>
        Effect.gen(function* () {
          const { fake, opened, thread, offers, process } = yield* idleRuntime();
          yield* wakeStarts({ process }, { reply: "Wake output.", complete: true });
          yield* Queue.take(offers);

          yield* startTurn(opened.runtime, thread, { runtimeMode: "auto", text: "hello" });
          yield* assistantCompleted(opened, "Wake output.");

          assert.deepStrictEqual(
            requestsOf(fake, "droid.add_user_message").map((request) => request.params),
            [{ text: "hello", messageId: messageIdFor(1) }],
          );
          assert.deepStrictEqual(completedTexts(opened.all), ["Wake output."]);
          assert.strictEqual(
            terminals(opened.all).length,
            0,
            "the wake's completion is not the turn's",
          );

          yield* process.emit(userEcho("hello"));
          yield* assistantSays(process, "reply-user", "Hi there.");
          yield* process.emit(completed(1));
          yield* opened.takeEvent(isTerminal);

          assert.deepStrictEqual(completedTexts(opened.all), ["Wake output.", "Hi there."]);
          assert.strictEqual(terminals(opened.all).length, 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("a buffered wake is cleared when its process goes away", () => {
    it.effect(
      "[VAL-SUB-012] the process exiting clears it: nothing is pending, the offer is stale, and nothing stale is replayed",
      () =>
        Effect.gen(function* () {
          const { fake, opened, thread, offers, process } = yield* idleRuntime();
          yield* wakeStarts({ process }, { reply: "Stale output." });
          const offer = yield* Queue.take(offers);
          assert.deepStrictEqual(yield* pending(opened, thread), [true, true]);

          yield* process.exit(1);
          yield* Deferred.await(opened.streamEnded);

          assert.deepStrictEqual(yield* pending(opened, thread), [false, false]);
          assert.isTrue(Option.isNone(yield* dispatch(offer)), "the offer no longer dispatches");
          const failure = yield* startTurn(opened.runtime, thread, {
            runOrdinal: 2,
            continuation: true,
            runtimeMode: "auto",
          }).pipe(Effect.flip);
          assert.isTrue(isTurnStartError(failure));
          assert.deepStrictEqual(assistantItems(opened.all), [], "no stale event was replayed");
          assert.strictEqual(terminals(opened.all).length, 0);

          // The next session on a new process holds no wake: a continuation there ends at once.
          const next = yield* openRuntime(undefined, {
            spawner: fake.spawner,
            adapter: opened.adapter,
          });
          const resumed = yield* next.runtime.resumeThread({ providerThread: thread });
          yield* startTurn(next.runtime, resumed, {
            runOrdinal: 2,
            continuation: true,
            runtimeMode: "auto",
          });
          const terminal = yield* next.takeEvent(isTerminal);
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.deepStrictEqual(assistantItems(next.all), []);
          assert.strictEqual(
            requestsOf(fake, "droid.add_user_message").length,
            0,
            "no user message was sent anywhere",
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-SUB-012] the offer's clearIfCurrent stops the process and clears the wake",
      () =>
        Effect.gen(function* () {
          const { opened, thread, offers, process } = yield* idleRuntime();
          yield* wakeStarts({ process }, { reply: "Unwanted." });
          const offer = yield* Queue.take(offers);

          yield* offer.clearIfCurrent!();
          yield* Deferred.await(opened.streamEnded);

          assert.deepStrictEqual(process.signals(), ["SIGTERM"], "Droid would go on unsupervised");
          assert.deepStrictEqual(yield* pending(opened, thread), [false, false]);
          assert.strictEqual(sessionStatuses(opened.all).at(-1), "stopped");
          assert.isTrue(Option.isNone(yield* dispatch(offer)));
          assert.deepStrictEqual(assistantItems(opened.all), []);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-SUB-012] closing the session clears it", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { offers, continuationRequests } = yield* makeOfferQueue();
        const sessionScope = yield* Scope.make();
        const opened = yield* openRuntime(fake, {
          continuationRequests,
          sessionScope,
          runtimeMode: "auto",
        });
        const thread = yield* ensure(opened, { runtimeMode: "auto" });
        const process = fake.current();
        yield* wakeStarts({ process }, { reply: "Unwanted." });
        const offer = yield* Queue.take(offers);

        yield* Scope.close(sessionScope, Exit.void);
        yield* Deferred.await(opened.streamEnded);

        assert.deepStrictEqual(yield* pending(opened, thread), [false, false]);
        assert.isTrue(Option.isNone(yield* dispatch(offer)));
        assert.strictEqual(terminals(opened.all).length, 0);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    describe("a Stop that arrives with no turn running", () => {
      it.effect(
        "[VAL-SUB-012] with the restart flag it stops the pending wake's process, as every user Stop does",
        () =>
          Effect.gen(function* () {
            const { opened, thread, offers, process } = yield* idleRuntime();
            yield* wakeStarts({ process }, { reply: "Unwanted." });
            const offer = yield* Queue.take(offers);

            yield* opened.runtime.interruptTurn({
              providerThread: thread,
              providerTurnId: "settled-turn" as never,
              requestRuntimeRestart: true,
            });

            yield* Deferred.await(opened.streamEnded);

            assert.deepStrictEqual(process.signals(), ["SIGTERM"]);
            assert.deepStrictEqual(yield* pending(opened, thread), [false, false]);
            assert.strictEqual(sessionStatuses(opened.all).at(-1), "stopped");
            assert.isTrue(Option.isNone(yield* dispatch(offer)));
          }).pipe(Effect.scoped, Effect.provide(layerTest)),
      );

      it.effect("with the restart flag and no wake it succeeds and leaves the process alone", () =>
        Effect.gen(function* () {
          const { opened, thread, process } = yield* idleRuntime();

          yield* opened.runtime.interruptTurn({
            providerThread: thread,
            providerTurnId: "settled-turn" as never,
            requestRuntimeRestart: true,
          });

          assert.deepStrictEqual(process.signals(), []);
          assert.isFalse(Deferred.isDoneUnsafe(opened.streamEnded));
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
      );

      it.effect("without the flag it keeps the typed error and keeps the wake", () =>
        Effect.gen(function* () {
          const { opened, thread, offers, process } = yield* idleRuntime();
          yield* wakeStarts({ process }, { reply: "Kept." });
          yield* Queue.take(offers);

          const failure = yield* opened.runtime
            .interruptTurn({ providerThread: thread, providerTurnId: "settled-turn" as never })
            .pipe(Effect.flip);

          assert.isTrue(isInterruptError(failure));
          assert.deepStrictEqual(process.signals(), []);
          assert.deepStrictEqual(yield* pending(opened, thread), [true, true]);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
      );
    });
  });

  describe("[VAL-SUB-013] other unsolicited activity never becomes a turn", () => {
    it.effect(
      "an idle native turn that is not a task completion is logged and dropped: no offer, no turn event",
      () => {
        const logs = captureLogs();
        return Effect.gen(function* () {
          const { opened, thread, offers, process } = yield* idleRuntime();

          yield* process.emit({
            type: "create_message",
            message: {
              id: "late-steer",
              role: "user",
              content: [{ type: "text", text: "queued while the turn ran" }],
            },
          });
          yield* assistantSays(process, "reply-orphan", "Answering nobody.");
          yield* process.emit({
            type: "agent_turn_completed",
            reason: "completed",
            turnId: "late-steer",
          });
          yield* process.emit({
            type: "create_message",
            message: {
              id: "not-task-completion:abc",
              role: "system",
              content: [{ type: "text", text: "Background task completed." }],
              visibility: "llm_only",
            },
          });
          yield* process.settle;

          assert.strictEqual(yield* Queue.size(offers), 0, "no continuation is offered");
          assert.deepStrictEqual(turnEvents(opened.all), [], "no run, no item, no terminal");
          assert.notInclude(sessionStatuses(opened.all), "running");
          assert.deepStrictEqual(yield* pending(opened, thread), [false, false]);
          assert.isTrue(
            logs.entries.some(
              (entry) => entry.level === "Debug" && /outside a turn/u.test(entry.text),
            ),
            "the drop is logged",
          );
        }).pipe(logs.provide, Effect.scoped, Effect.provide(layerTest));
      },
    );

    it.effect("without a continuation queue a wake is dropped like any other activity", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake, { runtimeMode: "auto" });
        const thread = yield* ensure(opened, { runtimeMode: "auto" });

        yield* wakeStarts({ process: fake.current() }, { reply: "Nobody listens." });

        assert.deepStrictEqual(turnEvents(opened.all), []);
        assert.deepStrictEqual(yield* pending(opened, thread), [false, false]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("a compaction takes over a buffered wake", () => {
    it.effect(
      "[VAL-SUB-009] the wake's events land in the compaction turn and it ends with the compaction",
      () =>
        Effect.gen(function* () {
          const stored = {
            sessions: {
              "stored-session": {
                settings: FAKE_DROID_DEFAULT_SETTINGS,
                messages: [
                  { id: "u1", role: "user", content: [{ type: "text", text: "first" }] },
                  { id: "a1", role: "assistant", content: [{ type: "text", text: "ok" }] },
                ],
              },
            },
          };
          const fake = yield* makeFakeDroid(stored);
          const { offers, continuationRequests } = yield* makeOfferQueue();
          const opened = yield* openRuntime(fake, { continuationRequests, runtimeMode: "auto" });
          const thread = yield* opened.runtime.resumeThread({
            providerThread: yield* makeProviderThread("stored-session"),
          });
          const process = fake.current();
          yield* wakeStarts({ process }, { reply: "Wake output." });
          const offer = yield* Queue.take(offers);

          yield* compactTurn(opened.runtime, thread, { runOrdinal: 2, runtimeMode: "auto" });
          const terminal = yield* opened.takeEvent(isTerminal);

          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.deepStrictEqual(completedTexts(opened.all), ["Wake output."]);
          assert.strictEqual(
            assistantItems(opened.all)[0]?.providerTurnId,
            providerTurnsOf(opened.all)[0]?.id,
          );
          assert.deepStrictEqual(yield* pending(opened, thread), [false, false]);
          assert.isTrue(Option.isNone(yield* dispatch(offer)));
          assert.strictEqual(terminals(opened.all).length, 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-SUB-012] a stop that kills the process during that compaction leaves no wake behind",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid({
            sessions: {
              "stored-session": {
                settings: FAKE_DROID_DEFAULT_SETTINGS,
                messages: [
                  { id: "u1", role: "user", content: [{ type: "text", text: "first" }] },
                  { id: "a1", role: "assistant", content: [{ type: "text", text: "ok" }] },
                ],
              },
            },
            overrides: { "droid.compact_session": () => "silent" },
          });
          yield* fake.deliverGroupSignals;
          const { offers, continuationRequests } = yield* makeOfferQueue();
          const opened = yield* openRuntime(fake, { continuationRequests, runtimeMode: "auto" });
          const thread = yield* opened.runtime.resumeThread({
            providerThread: yield* makeProviderThread("stored-session"),
          });
          const process = fake.current();
          yield* wakeStarts({ process }, { reply: "Wake output." });
          const offer = yield* Queue.take(offers);

          yield* compactTurn(opened.runtime, thread, { runOrdinal: 2, runtimeMode: "auto" });
          const compaction = yield* opened.takeEvent(
            (event) => event.type === "provider_turn.updated",
          );
          if (compaction.type !== "provider_turn.updated") return assert.fail("no compaction turn");
          yield* assistantCompleted(opened, "Wake output.");

          yield* opened.runtime.interruptTurn({
            providerThread: thread,
            providerTurnId: compaction.providerTurn.id,
            requestRuntimeRestart: true,
          });
          yield* Deferred.await(opened.streamEnded);

          assert.deepStrictEqual(process.signals(), ["SIGTERM"]);
          assert.deepStrictEqual(yield* pending(opened, thread), [false, false]);
          assert.isTrue(Option.isNone(yield* dispatch(offer)), "the offer is stale");
          assert.strictEqual(yield* Queue.size(offers), 0);
          const all = terminals(opened.all);
          assert.deepStrictEqual(
            all.map((terminal) => terminal.status),
            ["interrupted"],
          );
          assert.strictEqual(all[0]?.threadDisposition, "reusable");
          assert.deepStrictEqual(openItems(opened.all), []);
          assert.strictEqual(sessionStatuses(opened.all).at(-1), "stopped");
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  it.effect(
    "a wake keeps its thread: another thread cannot replace it, and the wake stays buffered",
    () =>
      Effect.gen(function* () {
        const { opened, thread, offers, process } = yield* idleRuntime();
        yield* wakeStarts({ process }, { reply: "Mine." });
        yield* Queue.take(offers);
        const other = {
          ...(yield* makeProviderThread("some-other-session")),
          id: ProviderThreadId.make("provider-thread:other"),
        };

        const failure = yield* opened.runtime
          .resumeThread({ providerThread: other })
          .pipe(Effect.flip);

        assert.isTrue(isResumeError(failure));
        assert.deepStrictEqual(yield* pending(opened, thread), [true, true]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
