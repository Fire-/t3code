import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";

import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

import {
  compactTurn,
  ensure,
  isTerminal,
  layerTest,
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
} from "./adapterHarness.testkit.ts";
import { FAKE_DROID_DEFAULT_SETTINGS, makeFakeDroid } from "./fakeDroid.testkit.ts";

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

  it.effect(
    "[VAL-HIST-009] stopping a compaction ends the turn interrupted and cancels its item; Droid's late answer changes nothing",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({
          sessions: withSession,
          overrides: { "droid.compact_session": () => "silent" },
        });
        const opened = yield* openRuntime(fake);
        const thread = yield* resume(opened, "stored-session");
        const compacting = yield* compactTurn(opened.runtime, thread, { runOrdinal: 2 }).pipe(
          Effect.forkScoped,
        );
        const running = yield* opened.takeEvent(
          (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
        );
        const providerTurnId =
          running.type === "turn_item.updated" ? running.turnItem.providerTurnId : undefined;
        assert.isDefined(providerTurnId);

        yield* opened.runtime.interruptTurn({
          providerThread: thread,
          providerTurnId: providerTurnId as never,
        });
        const terminal = yield* opened.takeEvent(isTerminal);

        assert.strictEqual(terminal.type === "turn.terminal" ? terminal.status : "", "interrupted");
        assert.deepStrictEqual(
          itemsOfType(opened.all, "compaction").map((item) => item.status),
          ["running", "cancelled"],
        );
        yield* Fiber.interrupt(compacting);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

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
        assert.strictEqual(terminals(opened.all).length, 1);
        assert.deepStrictEqual(
          itemsOfType(opened.all, "compaction").map((item) => item.status),
          ["running", "failed"],
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
