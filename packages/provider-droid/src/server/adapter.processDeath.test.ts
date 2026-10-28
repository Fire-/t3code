import { assert, describe, it } from "@effect/vitest";
import type { OrchestrationV2ProviderThread } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";

import {
  assistantSays,
  completed,
  compactTurn,
  ensure,
  isTerminal,
  launchBackgroundTask,
  layerTest,
  makeOfferQueue,
  methodsOf,
  openRuntime,
  providerThreadUpdates,
  refIds,
  requestsOf,
  resume,
  startTurn,
  terminals,
  wakeMessage,
  type Events,
  type OpenRuntime,
} from "./adapterHarness.testkit.ts";
import { FAKE_DROID_DEFAULT_SETTINGS, makeFakeDroid } from "./fakeDroid.testkit.ts";

const EXEC_PERMISSION = {
  toolUses: [
    {
      toolUse: { type: "tool_use", id: "tool-1", name: "Execute", input: { command: "ls" } },
      confirmationType: "exec",
      details: { type: "exec", fullCommand: "ls", command: "ls" },
    },
  ],
  options: [
    { label: "Yes, allow", value: "proceed_once" },
    { label: "No, cancel", value: "cancel" },
  ],
};

const STORED_SESSION = {
  settings: FAKE_DROID_DEFAULT_SETTINGS,
  messages: [
    { id: "u1", role: "user", content: [{ type: "text", text: "first" }] },
    { id: "a1", role: "assistant", content: [{ type: "text", text: "ok" }] },
  ],
};

const subagentsOf = (events: Events) =>
  events.flatMap((event) => (event.type === "subagent.updated" ? [event.subagent] : []));

const assistantTexts = (events: Events) =>
  events.flatMap((event) =>
    event.type === "turn_item.updated" &&
    event.turnItem.type === "assistant_message" &&
    event.turnItem.status === "completed"
      ? [event.turnItem.text]
      : [],
  );

const pending = (opened: OpenRuntime, thread: OrchestrationV2ProviderThread) =>
  Effect.gen(function* () {
    return [
      yield* opened.runtime.hasPendingBackgroundWork!,
      yield* opened.runtime.hasPendingBackgroundWorkForThread!(thread),
    ];
  });

describe("DroidAdapterV2 process death", () => {
  it.effect(
    "[VAL-CROSS-005] a death during a turn with an open request and a background task cancels the request, fails the turn broken, clears the roster, and the next turn loads the newest session",
    () =>
      Effect.gen(function* () {
        // The thread has moved to a newer session, so "newest" is not the one it began on.
        const fake = yield* makeFakeDroid({
          sessions: {
            "stored-session": STORED_SESSION,
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
        const opened = yield* openRuntime(fake, { runtimeMode: "approval-required" });
        const resumed = yield* resume(opened, "stored-session");
        yield* compactTurn(opened.runtime, resumed, { runOrdinal: 2 });
        yield* opened.takeEvent(isTerminal);
        const thread = providerThreadUpdates(opened.all).findLast(
          (candidate) => candidate.nativeThreadRef?.nativeId === "compacted-session",
        );
        if (thread === undefined) return assert.fail("the thread never moved to the new session");

        const process = fake.current();
        yield* startTurn(opened.runtime, thread, { runOrdinal: 3 });
        yield* opened.takeEvent(
          (event) => event.type === "provider_turn.updated" && event.providerTurn.ordinal === 3,
        );
        yield* launchBackgroundTask(process);
        yield* process.emitInbound("inbound-1", "droid.request_permission", EXEC_PERMISSION);
        const shown = yield* opened.takeEvent(
          (event) =>
            event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
        );
        if (shown.type !== "runtime_request.updated") return assert.fail("no request");
        assert.deepStrictEqual(yield* pending(opened, thread), [true, true]);

        yield* process.exit(1);
        const terminal = yield* opened.takeEvent(isTerminal);
        yield* Deferred.await(opened.streamEnded);

        assert.isTrue(
          terminal.type === "turn.terminal" &&
            terminal.status === "failed" &&
            terminal.threadDisposition === "broken",
        );
        const terminalAt = opened.all.indexOf(terminal);
        const cancelledAt = opened.all.findIndex(
          (event) =>
            event.type === "runtime_request.updated" &&
            event.runtimeRequest.id === shown.runtimeRequest.id &&
            event.runtimeRequest.status === "cancelled",
        );
        assert.isAbove(cancelledAt, -1, "the open request is cancelled");
        assert.isBelow(cancelledAt, terminalAt);
        assert.strictEqual(subagentsOf(opened.all).at(-1)?.status, "failed");
        assert.deepStrictEqual(
          providerThreadUpdates(opened.all).at(-1)?.pendingBackgroundTasks,
          [],
        );
        assert.deepStrictEqual(yield* pending(opened, thread), [false, false]);
        assert.strictEqual(
          terminals(opened.all).filter((entry) => entry.runOrdinal === 3).length,
          1,
        );

        // The next user turn: a new process resumes the newest id before it delivers the message.
        const newest = refIds(providerThreadUpdates(opened.all)).at(-1);
        assert.strictEqual(newest, "compacted-session");
        const reopened = yield* openRuntime(fake, { runtimeMode: "approval-required" });
        const next = yield* resume(reopened, newest ?? "");
        yield* startTurn(reopened.runtime, next, { runOrdinal: 4, text: "Carry on" });

        assert.lengthOf(fake.processes(), 2);
        assert.notStrictEqual(fake.current().pid, process.pid);
        assert.deepStrictEqual(
          requestsOf(fake, "droid.load_session").map((request) => request.params),
          [{ sessionId: "compacted-session", loadAllMessages: true }],
        );
        assert.deepStrictEqual(methodsOf(fake).slice(0, 1), ["droid.load_session"]);
        assert.strictEqual(methodsOf(fake).at(-1), "droid.add_user_message");
        assert.strictEqual(
          requestsOf(fake, "droid.add_user_message")[0]?.params["text"],
          "Carry on",
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-CROSS-005] a death while idle with a background task and a buffered wake clears the wake and the roster, and the next turn loads the newest session",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { offers, continuationRequests } = yield* makeOfferQueue();
        const opened = yield* openRuntime(fake, { continuationRequests, runtimeMode: "auto" });
        const thread = yield* ensure(opened, { runtimeMode: "auto" });
        const process = fake.current();
        yield* startTurn(opened.runtime, thread, { runtimeMode: "auto" });
        yield* opened.takeEvent((event) => event.type === "provider_turn.updated");
        yield* launchBackgroundTask(process);
        yield* process.emit(completed(1));
        yield* opened.takeEvent(isTerminal);

        // The task reports back while no turn is open: Droid starts a turn of its own.
        yield* process.emit(wakeMessage("3a1b2c3d"));
        yield* assistantSays(process, "reply-wake", "The task is done.");
        yield* process.settle;
        const offer = yield* Queue.take(offers);
        assert.deepStrictEqual(yield* pending(opened, thread), [true, true]);

        yield* process.exit(1);
        yield* Deferred.await(opened.streamEnded);

        assert.deepStrictEqual(yield* pending(opened, thread), [false, false]);
        assert.deepStrictEqual(
          providerThreadUpdates(opened.all).at(-1)?.pendingBackgroundTasks,
          [],
        );
        assert.strictEqual(subagentsOf(opened.all).at(-1)?.status, "failed");
        assert.isDefined(offer.dispatchIfCurrent);
        const dispatched = yield* offer.dispatchIfCurrent!(Effect.succeed("dispatched"));
        assert.isTrue(Option.isNone(dispatched), "the buffered wake is no longer current");
        assert.strictEqual(terminals(opened.all).length, 1, "no turn was open, so none ends");

        const newest = refIds(providerThreadUpdates(opened.all)).at(-1);
        assert.strictEqual(newest, "droid-session-1");
        const reopened = yield* openRuntime(fake, { runtimeMode: "auto" });
        const next = yield* resume(reopened, newest ?? "");
        yield* startTurn(reopened.runtime, next, { runOrdinal: 2, runtimeMode: "auto" });

        assert.deepStrictEqual(
          requestsOf(fake, "droid.load_session").map((request) => request.params),
          [{ sessionId: "droid-session-1", loadAllMessages: true }],
        );
        assert.strictEqual(methodsOf(fake).at(-1), "droid.add_user_message");
        assert.strictEqual(
          requestsOf(fake, "droid.add_user_message")[0]?.params["messageId"],
          `message:thread-droid-test:2`,
          "the user's message is the turn's, not the wake's",
        );
        assert.deepStrictEqual(
          assistantTexts(opened.all),
          [],
          "nothing of the stale wake was replayed anywhere",
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
