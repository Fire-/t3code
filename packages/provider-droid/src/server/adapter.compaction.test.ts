import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

import {
  compactTurn,
  completed,
  ensure,
  isTerminal,
  layerTest,
  messageIdFor,
  methodsOf,
  openRuntime,
  providerThreadUpdates,
  providerTurnsOf,
  refIds,
  requestsOf,
  resume,
  runTurn,
  sessionStatuses,
  startTurn,
  terminals,
  openItems,
  type Events,
  type OpenRuntime,
} from "./adapterHarness.testkit.ts";
import {
  FAKE_DROID_DEFAULT_SETTINGS,
  makeFakeDroid,
  type FakeDroid,
  type FakeDroidProcess,
} from "./fakeDroid.testkit.ts";

const isTurnStartError = Schema.is(ProviderAdapter.ProviderAdapterTurnStartError);

const itemsOfType = <Type extends string>(events: Events, type: Type) =>
  events.flatMap((event) =>
    event.type === "turn_item.updated" && event.turnItem.type === type ? [event.turnItem] : [],
  );

/** A fake session that already holds a finished turn, so Droid has something to compact. */
const withSession = {
  "stored-session": {
    settings: FAKE_DROID_DEFAULT_SETTINGS,
    messages: [
      { id: "u1", role: "user", content: [{ type: "text", text: "first" }] },
      { id: "a1", role: "assistant", content: [{ type: "text", text: "ok" }] },
    ],
  },
};

describe("DroidAdapterV2 compaction", () => {
  it.effect(
    "[VAL-HIST-009] compacts through compact_session with no user message, keeps the session id, and ends the turn completed with a compaction item",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake, { runtimeMode: "auto" });
        const thread = yield* ensure(opened, { runtimeMode: "auto" });
        yield* runTurn(opened, fake, thread, { runtimeMode: "auto" });
        const sessionId = thread.nativeThreadRef?.nativeId;

        yield* compactTurn(opened.runtime, thread, { runOrdinal: 2, runtimeMode: "auto" });
        const terminal = yield* opened.takeEvent(
          (event) => isTerminal(event) && event.runOrdinal === 2,
        );

        assert.deepStrictEqual(methodsOf(fake), [
          "droid.initialize_session",
          "droid.add_user_message",
          "droid.compact_session",
        ]);
        assert.deepStrictEqual(requestsOf(fake, "droid.compact_session")[0]?.params, {});
        assert.strictEqual(requestsOf(fake, "droid.add_user_message").length, 1);

        assert.strictEqual(terminal.type === "turn.terminal" ? terminal.status : "", "completed");
        assert.strictEqual(
          terminal.type === "turn.terminal" ? terminal.threadDisposition : "",
          "reusable",
        );
        const compactions = itemsOfType(opened.all, "compaction");
        assert.deepStrictEqual(
          compactions.map((item) => item.status),
          ["running", "completed"],
        );
        const [running, done] = compactions;
        assert.strictEqual(done?.id, running?.id);
        assert.strictEqual(done?.type === "compaction" ? done.driver : undefined, "droid");
        assert.isNotNull(done?.completedAt);
        assert.deepStrictEqual(openItems(opened.all), []);

        const compactionTurn = providerTurnsOf(opened.all).find((turn) => turn.ordinal === 2);
        assert.strictEqual(compactionTurn?.status, "completed");
        assert.isNull(
          compactionTurn?.nativeTurnRef,
          "no Droid user message began this turn, so there is no message id to rewind to",
        );
        assert.deepStrictEqual(
          sessionStatuses(opened.all).slice(-2),
          ["running", "ready"],
          "the session is busy while Droid compacts and ready after",
        );
        assert.deepStrictEqual(
          [...new Set(refIds(providerThreadUpdates(opened.all)))],
          [sessionId],
          "the equal session id leaves the ref alone",
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-009] a returned session id that differs is a forward switch: the session is loaded and the same provider thread moves to it before the terminal",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({
          sessions: {
            ...withSession,
            "compacted-session": {
              settings: FAKE_DROID_DEFAULT_SETTINGS,
              messages: [
                { id: "summary", role: "user", content: [{ type: "text", text: "summary" }] },
              ],
            },
          },
          overrides: {
            "droid.compact_session": () => ({
              result: { newSessionId: "compacted-session", removedCount: 2 },
            }),
          },
        });
        const opened = yield* openRuntime(fake, { runtimeMode: "auto" });
        const resumed = yield* resume(opened, "stored-session");
        const before = opened.all.length;

        yield* compactTurn(opened.runtime, resumed, { runOrdinal: 2, runtimeMode: "auto" });
        yield* opened.takeEvent(isTerminal);
        const afterSwitch = opened.all.slice(before);

        assert.deepStrictEqual(
          requestsOf(fake, "droid.load_session")
            .slice(-1)
            .map((request) => request.params),
          [{ sessionId: "compacted-session", loadAllMessages: true }],
        );
        assert.deepStrictEqual(fake.current().methods().slice(-2), [
          "droid.compact_session",
          "droid.load_session",
        ]);
        const moved = providerThreadUpdates(afterSwitch).find(
          (thread) => thread.nativeThreadRef?.nativeId === "compacted-session",
        );
        assert.strictEqual(moved?.id, resumed.id, "the provider thread id does not change");
        const switchIndex = afterSwitch.findIndex(
          (event) =>
            event.type === "provider_thread.updated" &&
            event.providerThread.nativeThreadRef?.nativeId === "compacted-session",
        );
        const terminalIndex = afterSwitch.findIndex(isTerminal);
        assert.isAbove(switchIndex, -1);
        assert.isBelow(switchIndex, terminalIndex, "the ref moves before the turn ends");
        assert.strictEqual(terminals(afterSwitch)[0]?.status, "completed");
        const refs = refIds(providerThreadUpdates(afterSwitch));
        assert.notInclude(
          refs.slice(refs.indexOf("compacted-session")),
          "stored-session",
          "the ref never goes back to the retired id",
        );

        // The next turn is a different session, so the settings are sent again before it.
        yield* startTurn(opened.runtime, moved ?? resumed, { runOrdinal: 3, runtimeMode: "auto" });
        assert.deepStrictEqual(fake.current().methods().slice(-2), [
          "droid.update_session_settings",
          "droid.add_user_message",
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-009] a compaction Droid refuses fails the turn once, settles its item failed, and leaves the session usable",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened);

        // Nothing has been said in this session yet: Droid answers -32603.
        yield* compactTurn(opened.runtime, thread);
        yield* opened.takeEvent(isTerminal);
        const all = terminals(opened.all);

        assert.strictEqual(all.length, 1);
        assert.strictEqual(all[0]?.status, "failed");
        assert.strictEqual(all[0]?.threadDisposition, "reusable");
        assert.include(all[0]?.failure?.message ?? "", "compaction");
        assert.deepStrictEqual(
          itemsOfType(opened.all, "compaction").map((item) => item.status),
          ["running", "failed"],
        );
        assert.strictEqual(sessionStatuses(opened.all).at(-1), "ready");

        yield* runTurn(opened, fake, thread, { runOrdinal: 2 });
        assert.strictEqual(terminals(opened.all).at(-1)?.status, "completed");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-009] compaction is refused, before anything is sent, while a turn is active",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened);
        yield* startTurn(opened.runtime, thread, { runOrdinal: 1 });

        const error = yield* compactTurn(opened.runtime, thread, { runOrdinal: 2 }).pipe(
          Effect.flip,
        );

        assert.isTrue(isTurnStartError(error));
        assert.deepStrictEqual(requestsOf(fake, "droid.compact_session"), []);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  describe("stopping a compaction", () => {
    /**
     * Droid answers `interrupt_session` during a compaction and carries on, so the only stop is
     * to kill the process. `compact_session` stays unanswered, as it does while Droid summarizes.
     */
    const compacting = Effect.gen(function* () {
      const fake = yield* makeFakeDroid({
        sessions: withSession,
        overrides: { "droid.compact_session": () => "silent" },
      });
      yield* fake.deliverGroupSignals;
      const opened = yield* openRuntime(fake);
      const thread = yield* resume(opened, "stored-session");
      yield* compactTurn(opened.runtime, thread, { runOrdinal: 2 });
      const published = yield* opened.takeEvent((event) => event.type === "provider_turn.updated");
      if (published.type !== "provider_turn.updated") return assert.fail("no provider turn");
      const stop = (options: { readonly requestRuntimeRestart?: boolean } = {}) =>
        opened.runtime.interruptTurn({
          providerThread: thread,
          providerTurnId: published.providerTurn.id,
          ...options,
        });
      return { fake, opened, process: fake.current(), stop };
    });

    const requestsTo = (process: FakeDroidProcess, method: string) =>
      process.requests().filter((request) => request.method === method);

    /** What the session manager does once the stopped session ends: a new session and process. */
    const nextTurnOnNewProcess = Effect.fnUntraced(function* (
      fake: FakeDroid,
      stopped: OpenRuntime,
    ) {
      yield* Deferred.await(stopped.streamEnded);
      const nativeId = refIds(providerThreadUpdates(stopped.all)).at(-1);
      assert.strictEqual(nativeId, "stored-session");
      const reopened = yield* openRuntime(fake);
      const thread = yield* resume(reopened, "stored-session");
      yield* startTurn(reopened.runtime, thread, { runOrdinal: 3, text: "What was the marker?" });
      return fake.current();
    });

    const assertKilledAndInterrupted = (
      opened: OpenRuntime,
      process: FakeDroidProcess,
      streamExit: Exit.Exit<void, unknown>,
    ) => {
      assert.isTrue(
        Exit.isSuccess(streamExit),
        "the kill ends the event stream; it does not fail it",
      );
      assert.deepStrictEqual(process.signals(), ["SIGTERM"]);
      assert.deepStrictEqual(requestsTo(process, "droid.interrupt_session"), []);
      assert.strictEqual(
        process.methods().at(-1),
        "droid.compact_session",
        "nothing follows the kill",
      );

      const all = terminals(opened.all);
      assert.strictEqual(all.length, 1, "exactly one terminal");
      assert.strictEqual(all[0]?.status, "interrupted");
      assert.isNull(all[0]?.failure);
      assert.strictEqual(
        all[0]?.threadDisposition,
        "reusable",
        "a deliberate kill does not break the thread",
      );
      assert.deepStrictEqual(
        itemsOfType(opened.all, "compaction").map((item) => item.status),
        ["running", "cancelled"],
      );
      assert.deepStrictEqual(openItems(opened.all), []);
      assert.strictEqual(
        providerTurnsOf(opened.all).find((turn) => turn.ordinal === 2)?.status,
        "interrupted",
      );

      const statuses = sessionStatuses(opened.all);
      assert.strictEqual(statuses.at(-1), "stopped");
      assert.notInclude(statuses, "error", "the exit is the stop, not an unexpected exit");
      assert.strictEqual(providerThreadUpdates(opened.all).at(-1)?.status, "idle");
      const stoppedAt = opened.all.findIndex(
        (event) =>
          event.type === "provider_session.updated" && event.providerSession.status === "stopped",
      );
      assert.isBelow(
        opened.all.findIndex(isTerminal),
        stoppedAt,
        "the turn ends before the session reports stopped",
      );
    };

    const assertResumedOnNewProcess = (previous: FakeDroidProcess, next: FakeDroidProcess) => {
      assert.notStrictEqual(next.pid, previous.pid);
      assert.deepStrictEqual(next.signals(), []);
      assert.deepStrictEqual(next.methods(), [
        "droid.load_session",
        "droid.update_session_settings",
        "droid.add_user_message",
      ]);
      assert.deepStrictEqual(requestsTo(next, "droid.load_session")[0]?.params, {
        sessionId: "stored-session",
        loadAllMessages: true,
      });
      const message = requestsTo(next, "droid.add_user_message")[0]?.params;
      assert.strictEqual(message?.["text"], "What was the marker?");
      assert.strictEqual(message?.["messageId"], messageIdFor(3));
    };

    it.effect(
      "[VAL-HIST-017] a stop kills the Droid process group, ends the turn interrupted with its item cancelled and one terminal, and does not break the thread",
      () =>
        Effect.gen(function* () {
          const { opened, process, stop } = yield* compacting;

          yield* stop();
          assert.deepStrictEqual(process.signals(), ["SIGTERM"], "the stop returns after the kill");
          const streamExit = yield* Deferred.await(opened.streamEnded);

          assertKilledAndInterrupted(opened, process, streamExit);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-HIST-017] the next turn after a stop starts a new Droid process that loads the same session before it delivers the message",
      () =>
        Effect.gen(function* () {
          const { fake, opened, process, stop } = yield* compacting;

          yield* stop();
          const next = yield* nextTurnOnNewProcess(fake, opened);

          assert.lengthOf(fake.processes(), 2);
          assertResumedOnNewProcess(process, next);
          assert.strictEqual(
            terminals(opened.all).length,
            1,
            "the new process adds nothing to the stopped session",
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-HIST-017] a restart-mode send during a compaction kills the process once, then resumes the same session on a new process",
      () =>
        Effect.gen(function* () {
          const { fake, opened, process, stop } = yield* compacting;

          yield* stop({ requestRuntimeRestart: true });
          const streamExit = yield* Deferred.await(opened.streamEnded);
          assertKilledAndInterrupted(opened, process, streamExit);

          const next = yield* nextTurnOnNewProcess(fake, opened);
          assertResumedOnNewProcess(process, next);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-HIST-017] a stop after Droid answered the compaction is refused and kills nothing",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid({ sessions: withSession });
          yield* fake.deliverGroupSignals;
          const opened = yield* openRuntime(fake);
          const thread = yield* resume(opened, "stored-session");
          yield* compactTurn(opened.runtime, thread, { runOrdinal: 2 });
          const published = yield* opened.takeEvent(
            (event) => event.type === "provider_turn.updated",
          );
          if (published.type !== "provider_turn.updated") return assert.fail("no provider turn");
          yield* opened.takeEvent(isTerminal);

          const error = yield* opened.runtime
            .interruptTurn({ providerThread: thread, providerTurnId: published.providerTurn.id })
            .pipe(Effect.flip);

          assert.include(JSON.stringify(error), "ProviderAdapterInterruptError");
          assert.deepStrictEqual(fake.current().signals(), []);
          assert.strictEqual(terminals(opened.all).length, 1);
          assert.isFalse(Deferred.isDoneUnsafe(opened.streamEnded), "the session stays up");
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    describe("outside a compaction", () => {
      for (const options of [{}, { requestRuntimeRestart: true }]) {
        it.effect(
          `[VAL-HIST-017] a stop of an ordinary turn sends interrupt_session and does not kill the process (${JSON.stringify(options)})`,
          () =>
            Effect.gen(function* () {
              const fake = yield* makeFakeDroid();
              yield* fake.deliverGroupSignals;
              const opened = yield* openRuntime(fake);
              const thread = yield* ensure(opened);
              yield* startTurn(opened.runtime, thread);
              const turn = yield* opened.takeEvent(
                (event) => event.type === "provider_turn.updated",
              );
              if (turn.type !== "provider_turn.updated") return assert.fail("no provider turn");
              const process = fake.current();

              yield* opened.runtime.interruptTurn({
                providerThread: thread,
                providerTurnId: turn.providerTurn.id,
                ...options,
              });
              yield* process.emit(completed(1, "cancelled"));
              const terminal = yield* opened.takeEvent(isTerminal);

              assert.deepStrictEqual(
                requestsTo(process, "droid.interrupt_session").map((request) => request.params),
                [{}],
              );
              assert.deepStrictEqual(process.signals(), []);
              assert.isTrue(
                terminal.type === "turn.terminal" &&
                  terminal.status === "interrupted" &&
                  terminal.threadDisposition === "reusable",
              );
              assert.strictEqual(sessionStatuses(opened.all).at(-1), "ready");
              assert.isFalse(Deferred.isDoneUnsafe(opened.streamEnded), "the session stays up");
            }).pipe(Effect.scoped, Effect.provide(layerTest)),
        );
      }
    });
  });

  it.effect(
    "[VAL-HIST-009] a Droid that exits mid-compaction fails the turn and its item once",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({
          sessions: withSession,
          overrides: { "droid.compact_session": () => "silent" },
        });
        const opened = yield* openRuntime(fake);
        const thread = yield* resume(opened, "stored-session");
        yield* compactTurn(opened.runtime, thread, { runOrdinal: 2 }).pipe(Effect.forkScoped);
        yield* opened.takeEvent(
          (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
        );

        yield* fake.current().exit(1);
        const terminal = yield* opened.takeEvent(isTerminal);

        assert.strictEqual(terminal.type === "turn.terminal" ? terminal.status : "", "failed");
        assert.strictEqual(
          terminal.type === "turn.terminal" ? terminal.threadDisposition : "",
          "broken",
        );
        assert.strictEqual(terminals(opened.all).length, 1);
        assert.deepStrictEqual(
          itemsOfType(opened.all, "compaction").map((item) => item.status),
          ["running", "failed"],
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
