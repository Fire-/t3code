import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as TestClock from "effect/testing/TestClock";

import {
  assistantSays,
  beginTurn,
  captureLogs,
  completed,
  compactTurn,
  ensure,
  isTerminal,
  launchBackgroundTask,
  layerTest,
  openItems,
  openRuntime,
  providerThreadUpdates,
  resume,
  runTurn,
  sessionStatuses,
  terminals,
  type Events,
  type OpenRuntime,
} from "./adapterHarness.testkit.ts";
import {
  FAKE_DROID_DEFAULT_SETTINGS,
  makeFakeDroid,
  type FakeDroidProcess,
} from "./fakeDroid.testkit.ts";

const TEN_MINUTES = Duration.minutes(10);
const JUST_UNDER_TEN_MINUTES = Duration.millis(10 * 60_000 - 1);
const ONE_MILLI = Duration.millis(1);

const EXEC_CALL = {
  type: "tool_call",
  toolUse: { type: "tool_use", id: "tool-1", name: "Execute", input: { command: "sleep 3600" } },
};

const EXEC_RESULT = { type: "tool_result", toolUseId: "tool-1", content: "", isError: false };

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
    { label: "No, cancel", value: "cancel" },
  ],
};

const QUESTION = {
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

const silentTurn = Effect.fnUntraced(function* (
  runtimeMode: "auto" | "approval-required" = "auto",
) {
  const fake = yield* makeFakeDroid();
  yield* fake.deliverGroupSignals;
  const begun = yield* beginTurn(fake, { runtimeMode });
  return { fake, ...begun };
});

/**
 * Waits for the event the predicate names. Droid's events reach the test a step behind the
 * adapter, so an event that is observed shows the turn was still alive when it was handled; a
 * terminal that comes first fails the test instead of letting it wait.
 */
const observe = Effect.fnUntraced(function* (
  opened: OpenRuntime,
  predicate: (event: Events[number]) => boolean,
) {
  const event = yield* opened.takeEvent(
    (candidate) => isTerminal(candidate) || predicate(candidate),
  );
  if (isTerminal(event)) return assert.fail("the turn ended before the event it was waiting for");
  return event;
});

const isAssistantText = (text: string) => (event: Events[number]) =>
  event.type === "turn_item.updated" &&
  event.turnItem.type === "assistant_message" &&
  event.turnItem.status === "completed" &&
  event.turnItem.text === text;

const says = Effect.fnUntraced(function* (
  opened: OpenRuntime,
  process: FakeDroidProcess,
  id: string,
  text: string,
) {
  yield* assistantSays(process, id, text);
  yield* observe(opened, isAssistantText(text));
});

const isCommandItem = (status: string) => (event: Events[number]) =>
  event.type === "turn_item.updated" &&
  event.turnItem.type === "command_execution" &&
  event.turnItem.status === status;

const isRequestWith = (status: string) => (event: Events[number]) =>
  event.type === "runtime_request.updated" && event.runtimeRequest.status === status;

/** The terminal of a turn the watchdog ended, with everything that has to go with it. */
const assertWatchdogTerminal = (opened: OpenRuntime, terminal: Events[number]) => {
  assert.isTrue(terminal.type === "turn.terminal");
  if (terminal.type !== "turn.terminal") return;
  assert.strictEqual(terminal.status, "failed");
  assert.strictEqual(terminal.threadDisposition, "broken");
  assert.strictEqual(terminal.failure?.code, "inactivity_timeout");
  assert.strictEqual(terminal.failure?.class, "transport_error");
  assert.isTrue("failureItemOrdinal" in terminal);
  const errorAt = opened.all.findIndex(
    (event) =>
      event.type === "turn_item.updated" &&
      event.turnItem.type === "error" &&
      "failureItemOrdinal" in terminal &&
      event.turnItem.ordinal === terminal.failureItemOrdinal,
  );
  assert.isAbove(errorAt, -1, "an error item carries the failure ordinal");
  assert.isBelow(errorAt, opened.all.indexOf(terminal), "the error item comes first");
};

const assertTornDown = Effect.fnUntraced(function* (
  opened: OpenRuntime,
  process: FakeDroidProcess,
) {
  const streamExit = yield* Deferred.await(opened.streamEnded);
  assert.isTrue(Exit.isSuccess(streamExit), "the kill ends the event stream; it does not fail it");
  assert.strictEqual(
    process.methods().at(-1),
    "droid.interrupt_session",
    "Droid is asked to stop, and nothing follows the kill",
  );
  assert.deepStrictEqual(process.signals(), ["SIGTERM"], "the process group is killed once");
  assert.strictEqual(terminals(opened.all).length, 1, "exactly one terminal");
  const statuses = sessionStatuses(opened.all);
  assert.strictEqual(statuses.at(-1), "stopped");
  assert.include(statuses, "error", "the session reports the failure before it stops");
  assert.notInclude(statuses.slice(statuses.indexOf("error")), "ready", "never usable again");
  assert.strictEqual(providerThreadUpdates(opened.all).at(-1)?.status, "error");
});

describe("DroidAdapterV2 inactivity watchdog", () => {
  it.effect(
    "[VAL-WD-001] ten minutes without a Droid event end the turn failed and broken: the error item first, then the terminal",
    () => {
      const logs = captureLogs();
      return Effect.gen(function* () {
        const { opened, process } = yield* silentTurn();
        yield* says(opened, process, "reply-1", "Looking into it.");

        yield* TestClock.adjust(TEN_MINUTES);
        const terminal = yield* opened.takeEvent(isTerminal);

        assertWatchdogTerminal(opened, terminal);
        assert.include(
          terminal.type === "turn.terminal" ? (terminal.failure?.message ?? "") : "",
          "stopped responding",
        );
        assert.isTrue(
          logs.entries.some(
            (entry) => entry.level === "Warn" && /silent for 10 minutes/u.test(entry.text),
          ),
          "the silence is logged",
        );
        yield* assertTornDown(opened, process);
      }).pipe(logs.provide, Effect.scoped, Effect.provide(layerTest));
    },
  );

  it.effect(
    "[VAL-WD-002] it does not fire early, and every Droid event starts the ten minutes over",
    () =>
      Effect.gen(function* () {
        const { opened, process } = yield* silentTurn();

        // One millisecond short of the limit, an event arrives and the turn is still there.
        yield* TestClock.adjust(JUST_UNDER_TEN_MINUTES);
        assert.strictEqual(terminals(opened.all).length, 0);
        yield* says(opened, process, "reply-1", "One.");

        // Nine silent minutes later the first window has long passed. Only the event kept it open.
        yield* TestClock.adjust(Duration.minutes(9));
        assert.strictEqual(terminals(opened.all).length, 0);
        yield* says(opened, process, "reply-2", "Two.");

        // Short of ten minutes from the last event: nothing. At ten: the terminal.
        yield* TestClock.adjust(JUST_UNDER_TEN_MINUTES);
        assert.strictEqual(terminals(opened.all).length, 0, "not before ten minutes of silence");
        yield* TestClock.adjust(ONE_MILLI);
        const terminal = yield* opened.takeEvent(isTerminal);

        assertWatchdogTerminal(opened, terminal);
        yield* assertTornDown(opened, process);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-WD-003] it waits while a tool runs, however long, and fires ten silent minutes after the tool's result",
    () =>
      Effect.gen(function* () {
        const { opened, process } = yield* silentTurn();
        yield* process.emit(EXEC_CALL);
        yield* observe(opened, isCommandItem("running"));

        yield* TestClock.adjust(Duration.minutes(25));
        assert.strictEqual(terminals(opened.all).length, 0, "a running tool is not silence");

        yield* process.emit(EXEC_RESULT);
        yield* observe(opened, isCommandItem("completed"));

        yield* TestClock.adjust(JUST_UNDER_TEN_MINUTES);
        assert.strictEqual(terminals(opened.all).length, 0, "the window began at the result");
        yield* TestClock.adjust(ONE_MILLI);
        const terminal = yield* opened.takeEvent(isTerminal);

        assertWatchdogTerminal(opened, terminal);
        yield* assertTornDown(opened, process);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  describe("[VAL-WD-004] it waits while Droid's request is open", () => {
    const cases = [
      {
        name: "a permission request",
        method: "droid.request_permission",
        params: EXEC_PERMISSION,
        answer: { decision: "accept" } as const,
        opens: isRequestWith("pending"),
        settles: isRequestWith("resolved"),
      },
      {
        name: "a question",
        method: "droid.ask_user",
        params: QUESTION,
        answer: { answers: { "question-0": "Rust" } },
        opens: isRequestWith("pending"),
        settles: isRequestWith("resolved"),
      },
    ];

    for (const { name, method, params, answer, opens, settles } of cases) {
      it.effect(
        `${name} that nobody has answered is not silence, and the ten minutes begin when it is answered`,
        () =>
          Effect.gen(function* () {
            const { opened, process } = yield* silentTurn("approval-required");
            yield* process.emitInbound("inbound-1", method, params);
            const shown = yield* observe(opened, opens);
            if (shown.type !== "runtime_request.updated") return assert.fail("no request");

            yield* TestClock.adjust(Duration.minutes(25));
            assert.strictEqual(terminals(opened.all).length, 0, "an open request is not silence");

            yield* opened.runtime.respondToRuntimeRequest({
              requestId: shown.runtimeRequest.id,
              ...answer,
            });
            yield* observe(opened, settles);

            yield* TestClock.adjust(JUST_UNDER_TEN_MINUTES);
            assert.strictEqual(terminals(opened.all).length, 0, "the window began at the answer");
            yield* TestClock.adjust(ONE_MILLI);
            const terminal = yield* opened.takeEvent(isTerminal);

            assertWatchdogTerminal(opened, terminal);
            assert.isTrue(process.answers().some((reply) => reply.id === "inbound-1"));
            yield* assertTornDown(opened, process);
          }).pipe(Effect.scoped, Effect.provide(layerTest)),
      );
    }
  });

  it.effect(
    "[VAL-WD-005] firing settles what is open, clears the background tasks, interrupts Droid and then kills it",
    () =>
      Effect.gen(function* () {
        const { opened, process } = yield* silentTurn();
        yield* launchBackgroundTask(process);
        // A message Droid has started and not finished.
        yield* process.emit({
          type: "assistant_text_delta",
          messageId: "reply-open",
          blockIndex: 0,
          textDelta: "Half a thou",
        });
        yield* observe(
          opened,
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "assistant_message" &&
            event.turnItem.status === "running",
        );
        assert.deepStrictEqual(
          [
            yield* opened.runtime.hasPendingBackgroundWork!,
            providerThreadUpdates(opened.all).at(-1)?.pendingBackgroundTasks?.length,
          ],
          [true, 1],
          "a background task is listed before the watchdog fires",
        );

        yield* TestClock.adjust(TEN_MINUTES);
        const terminal = yield* opened.takeEvent(isTerminal);
        const terminalAt = opened.all.indexOf(terminal);

        assertWatchdogTerminal(opened, terminal);
        yield* assertTornDown(opened, process);
        assert.deepStrictEqual(openItems(opened.all), [], "no item is left running");
        const subagents = opened.all.flatMap((event) =>
          event.type === "subagent.updated" ? [event.subagent] : [],
        );
        assert.strictEqual(subagents.at(-1)?.status, "failed");
        assert.isNotNull(subagents.at(-1)?.completedAt);
        assert.deepStrictEqual(
          providerThreadUpdates(opened.all).at(-1)?.pendingBackgroundTasks,
          [],
        );
        const clearedAt = opened.all.findIndex(
          (event, index) =>
            index > 0 &&
            event.type === "provider_thread.updated" &&
            (event.providerThread.pendingBackgroundTasks?.length ?? 0) === 0 &&
            opened.all
              .slice(0, index)
              .some(
                (earlier) =>
                  earlier.type === "provider_thread.updated" &&
                  (earlier.providerThread.pendingBackgroundTasks?.length ?? 0) > 0,
              ),
        );
        assert.isBelow(clearedAt, terminalAt, "the roster is cleared before the terminal");
        assert.deepStrictEqual(
          [
            yield* opened.runtime.hasPendingBackgroundWork!,
            yield* opened.runtime.hasPendingBackgroundWorkForThread!(
              providerThreadUpdates(opened.all).at(-1)!,
            ),
          ],
          [false, false],
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  describe("[VAL-WD-006] it never fires outside a turn and never adds a second terminal", () => {
    it.effect("with no turn running, time passing does nothing", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        yield* fake.deliverGroupSignals;
        const opened = yield* openRuntime(fake, { runtimeMode: "auto" });
        const thread = yield* ensure(opened, { runtimeMode: "auto" });
        const process = fake.current();
        const statuses = sessionStatuses(opened.all).length;

        yield* TestClock.adjust(Duration.minutes(30));

        assert.strictEqual(terminals(opened.all).length, 0);
        assert.strictEqual(sessionStatuses(opened.all).length, statuses);
        assert.deepStrictEqual(process.signals(), []);
        assert.deepStrictEqual(process.methods(), ["droid.initialize_session"]);
        assert.isFalse(Deferred.isDoneUnsafe(opened.streamEnded));
        const terminal = yield* runTurn(opened, fake, thread, { runtimeMode: "auto" });
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("after the turn ended, time passing adds nothing and the session stays up", () =>
      Effect.gen(function* () {
        const { fake, opened, providerThread, process } = yield* silentTurn();
        yield* process.emit(completed(1));
        yield* opened.takeEvent(isTerminal);

        yield* TestClock.adjust(Duration.minutes(30));

        assert.strictEqual(terminals(opened.all).length, 1);
        assert.deepStrictEqual(process.signals(), []);
        assert.isFalse(Deferred.isDoneUnsafe(opened.streamEnded));
        assert.strictEqual(sessionStatuses(opened.all).at(-1), "ready");
        const next = yield* runTurn(opened, fake, providerThread, {
          runOrdinal: 2,
          runtimeMode: "auto",
        });
        assert.isTrue(next.type === "turn.terminal" && next.status === "completed");
        yield* TestClock.adjust(Duration.minutes(30));
        assert.strictEqual(terminals(opened.all).length, 2);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "a completion that arrives as the limit is about to pass is the turn's only terminal, and nothing is killed",
      () =>
        Effect.gen(function* () {
          const { opened, process } = yield* silentTurn();

          yield* TestClock.adjust(JUST_UNDER_TEN_MINUTES);
          yield* process.emit(completed(1));
          const terminal = yield* opened.takeEvent(isTerminal);
          yield* TestClock.adjust(TEN_MINUTES);

          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.strictEqual(terminals(opened.all).length, 1);
          assert.deepStrictEqual(process.signals(), []);
          assert.isFalse(Deferred.isDoneUnsafe(opened.streamEnded));
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("a completion that arrives after the watchdog fired adds no second terminal", () =>
      Effect.gen(function* () {
        const { opened, process } = yield* silentTurn();

        yield* TestClock.adjust(TEN_MINUTES);
        yield* opened.takeEvent(isTerminal);
        yield* Deferred.await(opened.streamEnded);
        yield* process.emit(completed(1));
        yield* TestClock.adjust(TEN_MINUTES);

        assert.strictEqual(terminals(opened.all).length, 1);
        assert.strictEqual(terminals(opened.all)[0]?.status, "failed");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("a completion and the limit landing together leave exactly one terminal", () =>
      Effect.gen(function* () {
        const { opened, process } = yield* silentTurn();

        yield* Effect.all([process.emit(completed(1)), TestClock.adjust(TEN_MINUTES)], {
          concurrency: "unbounded",
        });
        yield* opened.takeEvent(isTerminal);
        yield* TestClock.adjust(TEN_MINUTES);

        assert.strictEqual(terminals(opened.all).length, 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  it.effect(
    "a compaction is ended by compact_session's own timeout, long before the watchdog could fire, and the thread stays reusable",
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
        const opened = yield* openRuntime(fake, { runtimeMode: "auto" });
        const thread = yield* resume(opened, "stored-session");
        const process = fake.current();
        yield* compactTurn(opened.runtime, thread, { runOrdinal: 2, runtimeMode: "auto" });
        yield* observe(opened, (event) => event.type === "provider_turn.updated");

        // Droid says little while it compacts: a settings report is all that comes.
        yield* TestClock.adjust(Duration.seconds(200));
        yield* process.emit(
          { type: "settings_updated", settings: FAKE_DROID_DEFAULT_SETTINGS },
          null,
        );
        yield* process.settle;
        assert.strictEqual(terminals(opened.all).length, 0);

        yield* TestClock.adjust(Duration.seconds(40));
        const terminal = yield* opened.takeEvent(isTerminal);

        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "failed");
        assert.strictEqual(
          terminal.type === "turn.terminal" ? terminal.failure?.code : "",
          "compaction_failed",
          "the request's own timeout ended it, not the watchdog",
        );
        assert.strictEqual(
          terminal.type === "turn.terminal" ? terminal.threadDisposition : "",
          "reusable",
        );
        assert.deepStrictEqual(process.signals(), []);
        assert.isFalse(Deferred.isDoneUnsafe(opened.streamEnded));
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
