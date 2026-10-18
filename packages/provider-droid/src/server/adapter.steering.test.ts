import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

import {
  beginTurn,
  completed,
  isTerminal,
  layerTest,
  methodsOf,
  requestsOf,
  runIdFor,
  steerBegunTurn,
  steerMessageIdFor,
  terminals,
  type BegunTurn,
} from "./adapterHarness.testkit.ts";
import { makeFakeDroid, type FakeDroid } from "./fakeDroid.testkit.ts";

const isSteerError = Schema.is(ProviderAdapter.ProviderAdapterSteerRunError);

const steerCompleted = (ordinal = 1, reason = "completed") => ({
  ...completed(1, reason),
  turnId: steerMessageIdFor(ordinal),
});

/** Droid echoes a queued message as a user `create_message` when it picks it up. */
const userEcho = (ordinal: number, text: string) => ({
  type: "create_message",
  message: {
    id: steerMessageIdFor(ordinal),
    role: "user",
    content: [{ type: "text", text }],
  },
});

const assistantReply = (id: string, text: string) => ({
  type: "create_message",
  message: { id, role: "assistant", content: [{ type: "text", text }] },
});

const turnItems = (begun: BegunTurn) =>
  begun.opened.all.flatMap((event) => (event.type === "turn_item.updated" ? [event.turnItem] : []));

const addUserMessages = (fake: FakeDroid) => requestsOf(fake, "droid.add_user_message");

describe("DroidAdapterV2 steering", () => {
  it.effect(
    "[VAL-REQ-001] an active steer is sent end_of_turn, shown as a steer item, and never interrupts",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        yield* steerBegunTurn(begun, { text: "Also add tests" });
        yield* begun.process.settle;

        const sent = addUserMessages(fake);
        assert.strictEqual(sent.length, 2, "the turn's message and exactly one steer");
        assert.deepStrictEqual(sent[1]?.params, {
          text: "Also add tests",
          messageId: steerMessageIdFor(1),
          queuePlacement: "end_of_turn",
        });
        assert.notInclude(methodsOf(fake), "droid.interrupt_session");

        const steerItems = turnItems(begun).flatMap((item) =>
          item.type === "user_message" ? [item] : [],
        );
        assert.strictEqual(steerItems.length, 1);
        const item = steerItems[0];
        assert.strictEqual(item?.inputIntent, "steer");
        assert.strictEqual(item?.text, "Also add tests");
        assert.strictEqual(String(item?.messageId), steerMessageIdFor(1));
        assert.strictEqual(item?.runId, runIdFor(1));
        assert.strictEqual(item?.providerTurnId, begun.providerTurn.id);
        assert.strictEqual(item?.status, "completed");
        assert.deepStrictEqual(terminals(begun.opened.all), [], "the steer does not end the turn");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-REQ-001] a steer Droid rejects fails typed, shows no item, and leaves the turn alone",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({
          overrides: {
            "droid.add_user_message": (params) =>
              params["messageId"] === steerMessageIdFor(1)
                ? { error: { code: -32603, message: "queue full" } }
                : undefined,
          },
        });
        const begun = yield* beginTurn(fake);
        const failure = yield* steerBegunTurn(begun).pipe(Effect.flip);
        assert.isTrue(isSteerError(failure));
        assert.deepStrictEqual(
          turnItems(begun).filter((item) => item.type === "user_message"),
          [],
        );
        // The rejected steer is not waited for: the first completion still ends the turn.
        yield* begun.process.emit(completed(1));
        const terminal = yield* begun.opened.takeEvent(isTerminal);
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-REQ-002] a steer Droid folds into the running native turn ends with that turn's one completion",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        yield* steerBegunTurn(begun);
        yield* begun.process.emit(userEcho(1, "Also add tests"));
        yield* begun.process.emit(assistantReply("assistant-1", "Added the tests too"));
        yield* begun.process.emit(completed(1));
        const terminal = yield* begun.opened.takeEvent(isTerminal);
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
        yield* begun.process.settle;
        assert.strictEqual(terminals(begun.opened.all).length, 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-REQ-002] a steer Droid runs as its own native turn keeps the T3 turn open until that turn completes",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        yield* steerBegunTurn(begun);
        yield* begun.process.emit(assistantReply("assistant-1", "First reply"));
        yield* begun.process.emit(completed(1));
        yield* begun.process.settle;
        assert.deepStrictEqual(
          terminals(begun.opened.all),
          [],
          "the first native completion alone does not end the T3 turn",
        );

        yield* begun.process.emit(userEcho(1, "Also add tests"));
        yield* begun.process.emit(assistantReply("assistant-2", "Steer reply"));
        yield* begun.process.settle;
        assert.deepStrictEqual(terminals(begun.opened.all), []);

        yield* begun.process.emit(steerCompleted());
        const terminal = yield* begun.opened.takeEvent(isTerminal);
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
        yield* begun.process.settle;
        assert.strictEqual(terminals(begun.opened.all).length, 1, "exactly one terminal");

        const reply = turnItems(begun).findLast(
          (item) => item.type === "assistant_message" && item.text === "Steer reply",
        );
        assert.strictEqual(reply?.runId, runIdFor(1), "the steer's reply lands in the same run");
        assert.strictEqual(reply?.providerTurnId, begun.providerTurn.id);
        const providerTurns = new Set(
          begun.opened.all.flatMap((event) =>
            event.type === "provider_turn.updated" ? [event.providerTurn.id] : [],
          ),
        );
        assert.strictEqual(providerTurns.size, 1, "no extra provider turn is created");
        assert.strictEqual(addUserMessages(fake).length, 2, "Droid is sent nothing more");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-REQ-002] a steer sent while an earlier steer's own native turn runs joins the same T3 turn",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        yield* steerBegunTurn(begun, { ordinal: 1 });
        yield* begun.process.emit(completed(1));
        yield* begun.process.emit(userEcho(1, "Also add tests"));
        yield* begun.process.settle;

        yield* steerBegunTurn(begun, { ordinal: 2, text: "And a changelog" });
        yield* begun.process.emit(userEcho(2, "And a changelog"));
        yield* begun.process.emit(steerCompleted(1));
        const terminal = yield* begun.opened.takeEvent(isTerminal);
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
        yield* begun.process.settle;
        assert.strictEqual(terminals(begun.opened.all).length, 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-REQ-002] Stop while only a queued steer is outstanding ends the T3 turn interrupted",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        yield* steerBegunTurn(begun);
        yield* begun.process.emit(completed(1));
        yield* begun.process.settle;
        assert.deepStrictEqual(terminals(begun.opened.all), []);

        yield* begun.opened.runtime.interruptTurn({
          providerThread: begun.providerThread,
          providerTurnId: begun.providerTurn.id,
        });
        const terminal = yield* begun.opened.takeEvent(isTerminal);
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "interrupted");
        assert.deepStrictEqual(
          requestsOf(fake, "droid.interrupt_session").map((request) => request.params),
          [{}],
        );
        assert.strictEqual(terminals(begun.opened.all).length, 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-REQ-002] a cancelled first completion ends the turn even with a steer still queued",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        yield* steerBegunTurn(begun);
        yield* begun.process.emit(completed(1, "cancelled"));
        const terminal = yield* begun.opened.takeEvent(isTerminal);
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "interrupted");
        // Droid answered the interrupt by dropping the queue; the steer's late echo and
        // completion belong to no turn and must not produce a second terminal.
        yield* begun.process.emit(userEcho(1, "Also add tests"));
        yield* begun.process.emit(steerCompleted());
        yield* begun.process.settle;
        assert.strictEqual(terminals(begun.opened.all).length, 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-REQ-003] late steer behaves like Pi or Codex: it fails typed and sends nothing once the turn settled",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        yield* begun.process.emit(completed(1));
        yield* begun.opened.takeEvent(isTerminal);
        yield* begun.process.settle;
        const framesBefore = begun.process.frames().length;
        const eventsBefore = begun.opened.all.length;

        const failure = yield* steerBegunTurn(begun).pipe(Effect.flip);

        assert.isTrue(isSteerError(failure));
        assert.strictEqual(failure._tag, "ProviderAdapterSteerRunError");
        assert.strictEqual(
          begun.process.frames().length,
          framesBefore,
          "no frame is written for a late steer",
        );
        assert.strictEqual(addUserMessages(fake).length, 1, "only the turn's own message was sent");
        assert.deepStrictEqual(
          begun.opened.all.slice(eventsBefore),
          [],
          "no turn item, turn, or other event is emitted for it",
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-REQ-003] a steer for a turn that is not the active one fails typed and sends nothing",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake);
        const framesBefore = begun.process.frames().length;
        const failure = yield* steerBegunTurn(begun, {
          providerTurnId: "provider-turn:other" as never,
        }).pipe(Effect.flip);
        assert.isTrue(isSteerError(failure));
        assert.strictEqual(begun.process.frames().length, framesBefore);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
