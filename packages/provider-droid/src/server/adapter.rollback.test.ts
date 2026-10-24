import { assert, describe, it } from "@effect/vitest";
import {
  CheckpointId,
  NodeId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";

import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

import { DROID_PROVIDER } from "./adapter.ts";
import {
  adapterFromSavedBlock,
  awaitRef,
  completed,
  ensure,
  isTerminal,
  layerTest,
  makeOfferQueue,
  makeProviderThread,
  messageIdFor,
  methodsOf,
  MODEL,
  openRuntime,
  policy,
  PROVIDER_THREAD_ID,
  providerThreadUpdates,
  providerTurnsOf,
  refIds,
  requestsOf,
  runTurn,
  selection,
  startTurn,
  terminals,
  THREAD_ID,
  wakeCompleted,
  wakeMessage,
  type OpenRuntime,
  type TurnOptions,
} from "./adapterHarness.testkit.ts";
import { FAKE_DROID_DEFAULT_SETTINGS, makeFakeDroid, type FakeDroid } from "./fakeDroid.testkit.ts";

const isRollbackError = Schema.is(ProviderAdapter.ProviderAdapterRollbackThreadError);
const isTurnStartError = Schema.is(ProviderAdapter.ProviderAdapterTurnStartError);

type RollbackInput = Parameters<
  ProviderAdapter.ProviderAdapterV2SessionRuntime["rollbackThread"]
>[0];

const CHECKPOINT_ID = CheckpointId.make("checkpoint:droid-test:1");
const REWIND_TITLE = "T3 Code rewind";

const AUTO_SETTINGS = {
  interactionMode: "auto",
  autonomyLevel: "medium",
  modelId: MODEL,
  reasoningEffort: "high",
};

const toTurn = (turns: ReadonlyArray<OrchestrationV2ProviderTurn>, ordinal: number) => {
  const turn = turns.find((candidate) => candidate.ordinal === ordinal);
  if (turn === undefined) return assert.fail(`no provider turn ${ordinal}`);
  return turn;
};

const rollbackToTurn = (
  providerThread: OrchestrationV2ProviderThread,
  turns: ReadonlyArray<OrchestrationV2ProviderTurn>,
  ordinal: number,
): RollbackInput => ({
  providerThread,
  target: {
    type: "provider_turn",
    checkpointId: CHECKPOINT_ID,
    appRunOrdinal: ordinal,
    providerTurn: toTurn(turns, ordinal),
  },
  providerThreadTurns: turns,
});

const rollbackToStart = (
  providerThread: OrchestrationV2ProviderThread,
  turns: ReadonlyArray<OrchestrationV2ProviderTurn>,
): RollbackInput => ({
  providerThread,
  target: { type: "thread_start", checkpointId: CHECKPOINT_ID, appRunOrdinal: 0 },
  providerThreadTurns: turns,
});

const messageIdsOf = (fake: FakeDroid, sessionId: string | null | undefined) =>
  (fake.sessions.get(String(sessionId))?.messages ?? []).map((message) =>
    Schema.is(Schema.Struct({ id: Schema.String }))(message) ? message.id : undefined,
  );

const newRef = (nativeId: string) => ({
  driver: DROID_PROVIDER,
  nativeId,
  strength: "strong" as const,
});

/** One turn per message id of a stored session (`u1` and `u2` unless named), as the orchestrator would hold them. */
const storedTurns = (
  row: OrchestrationV2ProviderThread,
  nativeIds: ReadonlyArray<string> = ["u1", "u2"],
): ReadonlyArray<OrchestrationV2ProviderTurn> =>
  nativeIds.map((nativeId, index) => ({
    id: `provider-turn:${nativeId}` as OrchestrationV2ProviderTurn["id"],
    providerThreadId: row.id,
    nodeId: NodeId.make("node:stored-turn"),
    runAttemptId: null,
    nativeTurnRef: { driver: DROID_PROVIDER, nativeId, strength: "strong" as const },
    ordinal: index + 1,
    status: "completed" as const,
    startedAt: row.createdAt,
    completedAt: row.createdAt,
  }));

/** Opens a runtime on `fake` and completes `count` turns in auto runtime mode. */
const turnsOn = Effect.fnUntraced(function* (fake: FakeDroid, count: number) {
  const opened = yield* openRuntime(fake, { runtimeMode: "auto" });
  const thread = yield* ensure(opened, { runtimeMode: "auto" });
  for (let runOrdinal = 1; runOrdinal <= count; runOrdinal += 1) {
    yield* runTurn(opened, fake, thread, {
      runOrdinal,
      runtimeMode: "auto",
      text: `prompt ${runOrdinal}`,
    });
  }
  return { opened, thread, turns: providerTurnsOf(opened.all) };
});

const WAKE_ID = "4e5d6c7b";
const WAKE_MESSAGE_ID = `task-completion:${WAKE_ID}`;

/**
 * Turn 1, the continuation turn Droid starts when a background task ends (turn 2), then a user
 * turn (turn 3). A live `load_session` showed Droid keeps the report in the stored history, under
 * the same `task-completion:<id>` id, as an `llm_only` message between the turns it separates. The
 * fake does not record what Droid starts on its own, so it is given the message there.
 */
const turnsAcrossAWake = Effect.fnUntraced(function* (fake: FakeDroid) {
  const { offers, continuationRequests } = yield* makeOfferQueue();
  const opened = yield* openRuntime(fake, { runtimeMode: "auto", continuationRequests });
  const thread = yield* ensure(opened, { runtimeMode: "auto" });
  yield* runTurn(opened, fake, thread, { runOrdinal: 1, runtimeMode: "auto", text: "prompt 1" });

  const process = fake.current();
  yield* process.emit(wakeMessage(WAKE_ID));
  yield* process.settle;
  yield* Queue.take(offers);
  yield* startTurn(opened.runtime, thread, {
    runOrdinal: 2,
    continuation: true,
    runtimeMode: "auto",
  });
  yield* process.emit(wakeCompleted(WAKE_ID));
  yield* opened.takeEvent(isTerminal);

  const sessionId = String(process.sessionId());
  const stored = fake.sessions.get(sessionId);
  if (stored === undefined) return assert.fail("the fake holds no session");
  fake.sessions.set(sessionId, {
    ...stored,
    messages: [
      ...stored.messages,
      {
        id: WAKE_MESSAGE_ID,
        role: "user",
        visibility: "llm_only",
        content: [{ type: "text", text: "Background task completed." }],
      },
    ],
  });

  yield* runTurn(opened, fake, thread, { runOrdinal: 3, runtimeMode: "auto", text: "prompt 3" });
  return { opened, thread, turns: providerTurnsOf(opened.all) };
});

const PLAN_MARKDOWN = "Create `hello.txt` containing `hi`.";

/** A plan turn: Droid writes the plan, asks to leave spec mode, and ends the turn. */
const planTurn = Effect.fnUntraced(function* (
  opened: OpenRuntime,
  fake: FakeDroid,
  providerThread: OrchestrationV2ProviderThread,
  options: TurnOptions,
) {
  const runOrdinal = options.runOrdinal ?? 1;
  yield* startTurn(opened.runtime, providerThread, { ...options, interactionMode: "plan" });
  const process = fake.current();
  const toolUseId = `tool-exit-${runOrdinal}`;
  yield* process.emit({
    type: "tool_call",
    toolUse: {
      type: "tool_use",
      id: toolUseId,
      name: "ExitSpecMode",
      input: { plan: PLAN_MARKDOWN },
    },
  });
  yield* process.emitInbound(`perm-exit-${runOrdinal}`, "droid.request_permission", {
    toolUses: [
      {
        toolUse: {
          type: "tool_use",
          id: toolUseId,
          name: "ExitSpecMode",
          input: { plan: PLAN_MARKDOWN },
        },
        confirmationType: "exit_spec_mode",
        details: { type: "exit_spec_mode", plan: PLAN_MARKDOWN },
      },
    ],
    options: [{ label: "No, keep iterating on spec", value: "cancel" }],
  });
  yield* process.emit({
    type: "tool_result",
    toolUseId,
    messageId: `exit-result-${runOrdinal}`,
    content: "Error: Plan not approved - remaining in Spec Mode.",
    isError: true,
  });
  yield* process.emit(completed(runOrdinal));
  return yield* opened.takeEvent(isTerminal);
});

describe("DroidAdapterV2 rollback", () => {
  describe("to a provider turn", () => {
    it.effect(
      "[VAL-HIST-002] rewinds before the first later turn, loads the new session, re-sends the settings, and returns a snapshot with the new ref",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const { opened, thread, turns } = yield* turnsOn(fake, 2);
          const originalId = thread.nativeThreadRef?.nativeId;
          assert.isDefined(originalId);

          const snapshot = yield* opened.runtime.rollbackThread(rollbackToTurn(thread, turns, 1));

          const newId = snapshot.providerThread.nativeThreadRef?.nativeId;
          assert.isDefined(newId);
          assert.notStrictEqual(newId, originalId);
          assert.deepStrictEqual(methodsOf(fake), [
            "droid.initialize_session",
            "droid.add_user_message",
            "droid.add_user_message",
            "droid.execute_rewind",
            "droid.load_session",
            "droid.update_session_settings",
          ]);
          assert.deepStrictEqual(requestsOf(fake, "droid.execute_rewind")[0]?.params, {
            sessionId: originalId,
            messageId: messageIdFor(2),
            filesToRestore: [],
            filesToDelete: [],
            forkTitle: REWIND_TITLE,
          });
          assert.deepStrictEqual(requestsOf(fake, "droid.load_session")[0]?.params, {
            sessionId: newId,
            loadAllMessages: true,
          });
          assert.deepStrictEqual(
            requestsOf(fake, "droid.update_session_settings")[0]?.params,
            AUTO_SETTINGS,
          );
          assert.deepStrictEqual(messageIdsOf(fake, newId), [messageIdFor(1)]);
          assert.deepStrictEqual(
            messageIdsOf(fake, originalId),
            [messageIdFor(1), messageIdFor(2)],
            "the source session keeps both turns",
          );

          const updated = yield* awaitRef(opened, newId ?? "");
          assert.isTrue(updated.type === "provider_thread.updated");
          if (updated.type !== "provider_thread.updated") return;
          assert.strictEqual(updated.providerThread.id, PROVIDER_THREAD_ID);
          assert.deepStrictEqual(updated.providerThread.nativeThreadRef, newRef(newId ?? ""));
          assert.strictEqual(snapshot.providerThread.id, PROVIDER_THREAD_ID);
          assert.deepStrictEqual(snapshot.providerThread.nativeThreadRef, newRef(newId ?? ""));
          assert.deepStrictEqual(
            snapshot.messages.map((message) => [message.role, message.text]),
            [["user", "prompt 1"]],
          );
          assert.deepStrictEqual(snapshot.providerTurns, []);
          assert.deepStrictEqual(snapshot.runtimeRequests, []);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-HIST-002] the next turn on the rewound thread sends only its message when nothing changed",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const { opened, thread, turns } = yield* turnsOn(fake, 2);
          const snapshot = yield* opened.runtime.rollbackThread(rollbackToTurn(thread, turns, 1));
          const before = methodsOf(fake).length;

          yield* runTurn(opened, fake, snapshot.providerThread, {
            runOrdinal: 3,
            runtimeMode: "auto",
          });
          assert.deepStrictEqual(methodsOf(fake).slice(before), ["droid.add_user_message"]);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-HIST-012] the frame after the rewind carries the thread's last selection with the catalog default effort, and the next turn on it sends nothing",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid({
            availableModels: [
              {
                id: MODEL,
                modelId: MODEL,
                supportedReasoningEfforts: ["low", "medium", "high"],
                defaultReasoningEffort: "medium",
              },
              {
                id: "gpt-5-codex",
                modelId: "gpt-5-codex",
                supportedReasoningEfforts: ["low", "medium", "high"],
                defaultReasoningEffort: "low",
              },
            ],
          });
          const opened = yield* openRuntime(fake, { runtimeMode: "auto" });
          const thread = yield* ensure(opened, { runtimeMode: "auto" });
          const codex = { runtimeMode: "auto", model: "gpt-5-codex", effort: undefined } as const;
          yield* runTurn(opened, fake, thread, { runOrdinal: 1, runtimeMode: "auto" });
          yield* runTurn(opened, fake, thread, { runOrdinal: 2, ...codex });
          const turns = providerTurnsOf(opened.all);

          const snapshot = yield* opened.runtime.rollbackThread(rollbackToTurn(thread, turns, 1));
          assert.deepStrictEqual(requestsOf(fake, "droid.update_session_settings").at(-1)?.params, {
            interactionMode: "auto",
            autonomyLevel: "medium",
            modelId: "gpt-5-codex",
            reasoningEffort: "low",
          });

          const before = methodsOf(fake).length;
          yield* runTurn(opened, fake, snapshot.providerThread, { runOrdinal: 3, ...codex });
          assert.deepStrictEqual(methodsOf(fake).slice(before), ["droid.add_user_message"]);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("rolling back to the latest turn discards nothing and sends no frame", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { opened, thread, turns } = yield* turnsOn(fake, 2);
        const before = methodsOf(fake).length;
        const updatesBefore = providerThreadUpdates(opened.all).length;

        const snapshot = yield* opened.runtime.rollbackThread(rollbackToTurn(thread, turns, 2));

        assert.deepStrictEqual(methodsOf(fake).length, before);
        assert.deepStrictEqual(snapshot.providerThread.nativeThreadRef, thread.nativeThreadRef);
        assert.strictEqual(providerThreadUpdates(opened.all).length, updatesBefore);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "rolls back a thread no turn has run on yet, from a process that holds no session",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid({
            sessions: {
              "stored-session": {
                settings: FAKE_DROID_DEFAULT_SETTINGS,
                messages: [
                  { id: "u1", role: "user", content: [{ type: "text", text: "first" }] },
                  { id: "u2", role: "user", content: [{ type: "text", text: "second" }] },
                ],
              },
            },
          });
          const opened = yield* openRuntime(fake);
          const row = yield* makeProviderThread("stored-session");
          const turns = storedTurns(row);

          const snapshot = yield* opened.runtime.rollbackThread(rollbackToTurn(row, turns, 1));

          assert.deepStrictEqual(methodsOf(fake), [
            "droid.load_session",
            "droid.execute_rewind",
            "droid.load_session",
            "droid.update_session_settings",
          ]);
          assert.strictEqual(
            requestsOf(fake, "droid.load_session")[0]?.params["sessionId"],
            "stored-session",
          );
          assert.strictEqual(
            requestsOf(fake, "droid.execute_rewind")[0]?.params["sessionId"],
            "stored-session",
          );
          assert.strictEqual(
            requestsOf(fake, "droid.execute_rewind")[0]?.params["messageId"],
            "u2",
          );
          assert.deepStrictEqual(
            requestsOf(fake, "droid.update_session_settings")[0]?.params,
            {
              interactionMode: "auto",
              autonomyLevel: "off",
              modelId: MODEL,
              reasoningEffort: "high",
            },
            "the settings come from the thread's open input when no turn ran in this process",
          );
          assert.notStrictEqual(
            snapshot.providerThread.nativeThreadRef?.nativeId,
            "stored-session",
          );
          assert.deepStrictEqual(
            snapshot.messages.map((message) => message.text),
            ["first"],
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("to the thread start", () => {
    it.effect("[VAL-HIST-003] rewinds before the first turn and keeps the provider thread id", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { opened, thread, turns } = yield* turnsOn(fake, 2);
        const originalId = thread.nativeThreadRef?.nativeId;

        const snapshot = yield* opened.runtime.rollbackThread(rollbackToStart(thread, turns));

        const newId = snapshot.providerThread.nativeThreadRef?.nativeId;
        assert.isDefined(newId);
        assert.notStrictEqual(newId, originalId);
        assert.deepStrictEqual(requestsOf(fake, "droid.execute_rewind")[0]?.params, {
          sessionId: originalId,
          messageId: messageIdFor(1),
          filesToRestore: [],
          filesToDelete: [],
          forkTitle: REWIND_TITLE,
        });
        assert.deepStrictEqual(requestsOf(fake, "droid.load_session")[0]?.params, {
          sessionId: newId,
          loadAllMessages: true,
        });
        assert.deepStrictEqual(messageIdsOf(fake, newId), [], "no earlier turn survives");
        assert.strictEqual(snapshot.providerThread.id, PROVIDER_THREAD_ID);
        assert.deepStrictEqual(snapshot.messages, []);
        const updated = yield* awaitRef(opened, newId ?? "");
        assert.isTrue(
          updated.type === "provider_thread.updated" &&
            updated.providerThread.id === PROVIDER_THREAD_ID,
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-HIST-003] starts a fresh session when Droid refuses to rewind to the first message",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid({
            overrides: {
              "droid.execute_rewind": () => ({
                error: { code: -32603, message: "Cannot rewind to the first message" },
              }),
            },
          });
          const { opened, thread, turns } = yield* turnsOn(fake, 2);
          const originalId = thread.nativeThreadRef?.nativeId;

          const snapshot = yield* opened.runtime.rollbackThread(rollbackToStart(thread, turns));

          assert.deepStrictEqual(methodsOf(fake).slice(3), [
            "droid.execute_rewind",
            "droid.initialize_session",
          ]);
          const newId = snapshot.providerThread.nativeThreadRef?.nativeId;
          assert.isDefined(newId);
          assert.notStrictEqual(newId, originalId);
          assert.strictEqual(fake.current().sessionId(), newId);
          assert.deepStrictEqual(messageIdsOf(fake, newId), []);
          assert.strictEqual(snapshot.providerThread.id, PROVIDER_THREAD_ID);
          const first = requestsOf(fake, "droid.initialize_session")[0]?.params;
          assert.deepStrictEqual(requestsOf(fake, "droid.initialize_session")[1]?.params, {
            machineId: "default",
            cwd: first?.["cwd"],
            ...AUTO_SETTINGS,
            systemPrompt: first?.["systemPrompt"],
          });

          const before = methodsOf(fake).length;
          yield* runTurn(opened, fake, snapshot.providerThread, {
            runOrdinal: 3,
            runtimeMode: "auto",
          });
          assert.deepStrictEqual(
            methodsOf(fake).slice(before),
            ["droid.add_user_message"],
            "initialize_session carried the settings",
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "a refused rewind of a turn other than the first does not start a fresh session",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid({
            overrides: {
              "droid.execute_rewind": () => ({ error: { code: -32603, message: "boom" } }),
            },
          });
          const { opened, thread, turns } = yield* turnsOn(fake, 2);
          const failure = yield* opened.runtime
            .rollbackThread(rollbackToTurn(thread, turns, 1))
            .pipe(Effect.flip);
          assert.isTrue(isRollbackError(failure));
          assert.strictEqual(
            methodsOf(fake).filter((m) => m === "droid.initialize_session").length,
            1,
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("across a wake turn", () => {
    it.effect(
      "[VAL-HIST-002] rolling back to the turn before a wake turn rewinds to the wake's task-completion message and drops the wake and what followed",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const { opened, thread, turns } = yield* turnsAcrossAWake(fake);
          const originalId = thread.nativeThreadRef?.nativeId;
          assert.deepStrictEqual(
            turns.map((turn) => [turn.ordinal, turn.nativeTurnRef?.strength]),
            [
              [1, "strong"],
              [2, "strong"],
              [3, "strong"],
            ],
          );
          assert.strictEqual(turns[1]?.nativeTurnRef?.nativeId, WAKE_MESSAGE_ID);

          const snapshot = yield* opened.runtime.rollbackThread(rollbackToTurn(thread, turns, 1));

          assert.deepStrictEqual(requestsOf(fake, "droid.execute_rewind")[0]?.params, {
            sessionId: originalId,
            messageId: WAKE_MESSAGE_ID,
            filesToRestore: [],
            filesToDelete: [],
            forkTitle: REWIND_TITLE,
          });
          const newId = snapshot.providerThread.nativeThreadRef?.nativeId;
          assert.notStrictEqual(newId, originalId);
          assert.deepStrictEqual(messageIdsOf(fake, newId), [messageIdFor(1)]);
          assert.deepStrictEqual(
            messageIdsOf(fake, originalId),
            [messageIdFor(1), WAKE_MESSAGE_ID, messageIdFor(3)],
            "the source session keeps the wake and the turn after it",
          );
          assert.deepStrictEqual(
            snapshot.messages.map((message) => [message.role, message.text]),
            [["user", "prompt 1"]],
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-HIST-002] rolling back to a wake turn keeps its message and rewinds to the user turn after it",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const { opened, thread, turns } = yield* turnsAcrossAWake(fake);

          const snapshot = yield* opened.runtime.rollbackThread(rollbackToTurn(thread, turns, 2));

          assert.strictEqual(
            requestsOf(fake, "droid.execute_rewind")[0]?.params["messageId"],
            messageIdFor(3),
          );
          assert.deepStrictEqual(
            messageIdsOf(fake, snapshot.providerThread.nativeThreadRef?.nativeId),
            [messageIdFor(1), WAKE_MESSAGE_ID],
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-HIST-003] rolling back to the thread start rewinds to the first turn, not the wake that came later",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const { opened, thread, turns } = yield* turnsAcrossAWake(fake);

          const snapshot = yield* opened.runtime.rollbackThread(rollbackToStart(thread, turns));

          assert.strictEqual(
            requestsOf(fake, "droid.execute_rewind")[0]?.params["messageId"],
            messageIdFor(1),
          );
          assert.deepStrictEqual(
            messageIdsOf(fake, snapshot.providerThread.nativeThreadRef?.nativeId),
            [],
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("refusals and failures", () => {
    it.effect("is refused while a turn is active, before anything is sent", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { opened, thread, turns } = yield* turnsOn(fake, 1);
        yield* startTurn(opened.runtime, thread, { runOrdinal: 2, runtimeMode: "auto" });
        const before = methodsOf(fake).length;

        const failure = yield* opened.runtime
          .rollbackThread(rollbackToStart(thread, turns))
          .pipe(Effect.flip);

        assert.isTrue(isRollbackError(failure));
        assert.strictEqual(methodsOf(fake).length, before);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-SUB-014] is refused while a wake is pending, with no rewind sent", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { offers, continuationRequests } = yield* makeOfferQueue();
        const opened = yield* openRuntime(fake, { runtimeMode: "auto", continuationRequests });
        const thread = yield* ensure(opened, { runtimeMode: "auto" });
        yield* runTurn(opened, fake, thread, { runtimeMode: "auto", text: "prompt 1" });
        yield* runTurn(opened, fake, thread, {
          runOrdinal: 2,
          runtimeMode: "auto",
          text: "prompt 2",
        });
        const turns = providerTurnsOf(opened.all);
        const process = fake.current();
        yield* process.emit(wakeMessage("9c8b7a6d"));
        yield* process.settle;
        yield* Queue.take(offers);
        const sent = methodsOf(fake).length;

        const failure = yield* opened.runtime
          .rollbackThread(rollbackToTurn(thread, turns, 1))
          .pipe(Effect.flip);

        assert.isTrue(isRollbackError(failure));
        assert.deepStrictEqual(requestsOf(fake, "droid.execute_rewind"), []);
        assert.strictEqual(methodsOf(fake).length, sent, "nothing at all was sent");
        assert.deepStrictEqual(
          refIds(providerThreadUpdates(opened.all)).at(-1),
          thread.nativeThreadRef?.nativeId,
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("is refused when the boundary turn has no strong native ref", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { opened, thread, turns } = yield* turnsOn(fake, 2);
        const weak = turns.map((turn) =>
          turn.ordinal === 2
            ? {
                ...turn,
                nativeTurnRef: {
                  driver: DROID_PROVIDER,
                  nativeId: "u2",
                  strength: "weak" as const,
                },
              }
            : turn,
        );
        const before = methodsOf(fake).length;
        const failure = yield* opened.runtime
          .rollbackThread(rollbackToTurn(thread, weak, 1))
          .pipe(Effect.flip);
        assert.isTrue(isRollbackError(failure));
        assert.strictEqual(methodsOf(fake).length, before);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("is refused for a session this process does not host", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { opened, turns } = yield* turnsOn(fake, 2);
        const elsewhere = yield* makeProviderThread("some-other-session");
        const before = methodsOf(fake).length;
        const failure = yield* opened.runtime
          .rollbackThread(rollbackToTurn(elsewhere, turns, 1))
          .pipe(Effect.flip);
        assert.isTrue(isRollbackError(failure));
        assert.strictEqual(methodsOf(fake).length, before);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "a rewound session that fails to load leaves the thread on its session and emits no new ref",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid({
            overrides: {
              "droid.load_session": () => ({ error: { code: -32603, message: "cannot load" } }),
            },
          });
          const { opened, thread, turns } = yield* turnsOn(fake, 2);
          const failure = yield* opened.runtime
            .rollbackThread(rollbackToTurn(thread, turns, 1))
            .pipe(Effect.flip);
          assert.isTrue(isRollbackError(failure));

          yield* runTurn(opened, fake, thread, { runOrdinal: 3, runtimeMode: "auto" });
          assert.deepStrictEqual(
            refIds(providerThreadUpdates(opened.all)).filter(
              (id) => id !== thread.nativeThreadRef?.nativeId,
            ),
            [],
          );
          assert.strictEqual(terminals(opened.all).length, 3);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "a settings frame that fails after the rewind does not undo it, and the next turn sends the settings",
      () =>
        Effect.gen(function* () {
          let settingsFrames = 0;
          const fake = yield* makeFakeDroid({
            overrides: {
              "droid.update_session_settings": () =>
                (settingsFrames += 1) === 1
                  ? { error: { code: -32603, message: "settings unavailable" } }
                  : undefined,
            },
          });
          const { opened, thread, turns } = yield* turnsOn(fake, 2);
          const snapshot = yield* opened.runtime.rollbackThread(rollbackToTurn(thread, turns, 1));
          assert.notStrictEqual(
            snapshot.providerThread.nativeThreadRef?.nativeId,
            thread.nativeThreadRef?.nativeId,
          );

          const before = methodsOf(fake).length;
          yield* runTurn(opened, fake, snapshot.providerThread, {
            runOrdinal: 3,
            runtimeMode: "auto",
          });
          assert.deepStrictEqual(methodsOf(fake).slice(before), [
            "droid.update_session_settings",
            "droid.add_user_message",
          ]);
          assert.strictEqual(settingsFrames, 2);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("session switches only move forward", () => {
    it.effect(
      "[VAL-HIST-008] late events from the retired session never move the ref back, and one provider thread id spans every update",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const { opened, thread, turns } = yield* turnsOn(fake, 2);
          const retiredId = thread.nativeThreadRef?.nativeId;
          const snapshot = yield* opened.runtime.rollbackThread(rollbackToTurn(thread, turns, 1));
          const newId = snapshot.providerThread.nativeThreadRef?.nativeId ?? "";

          yield* startTurn(opened.runtime, snapshot.providerThread, {
            runOrdinal: 3,
            runtimeMode: "auto",
          });
          const process = fake.current();
          // The retired session reports on turn 3 as well: neither its usage nor its completion
          // may reach the thread that now lives on the new session.
          yield* process.emit(
            { ...completed(3), tokenUsage: { inputTokens: 999, outputTokens: 1 } },
            retiredId,
          );
          yield* process.settle;
          assert.deepStrictEqual(
            terminals(opened.all).length,
            2,
            "the late completion ends nothing",
          );

          yield* process.emit(completed(3), newId);
          yield* opened.takeEvent(isTerminal);

          const updates = providerThreadUpdates(opened.all);
          const afterSwitch = updates.slice(refIds(updates).indexOf(newId));
          assert.deepStrictEqual(
            [...new Set(refIds(afterSwitch))],
            [newId],
            "no update after the switch names the retired session",
          );
          assert.deepStrictEqual(
            [...new Set(updates.map((update) => update.id))],
            [PROVIDER_THREAD_ID],
          );
          assert.strictEqual(terminals(opened.all).length, 3);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-HIST-008] a turn built on the pre-rollback row is refused and publishes nothing under the old ref",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const { opened, thread, turns } = yield* turnsOn(fake, 2);
          const retiredId = thread.nativeThreadRef?.nativeId;
          const snapshot = yield* opened.runtime.rollbackThread(rollbackToTurn(thread, turns, 1));
          const newId = snapshot.providerThread.nativeThreadRef?.nativeId ?? "";
          yield* awaitRef(opened, newId);
          const updatesBefore = providerThreadUpdates(opened.all).length;

          const failure = yield* startTurn(opened.runtime, thread, {
            runOrdinal: 3,
            runtimeMode: "auto",
          }).pipe(Effect.flip);

          assert.isTrue(isTurnStartError(failure));
          assert.strictEqual(providerThreadUpdates(opened.all).length, updatesBefore);
          assert.notInclude(refIds(providerThreadUpdates(opened.all)).slice(-1), retiredId);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("restart after a session-id change", () => {
    it.effect(
      "[VAL-HIST-007][VAL-CROSS-001] from the saved block: two turns, one rolled back, close and reopen, and the next turn loads the rewound id, re-sends mode and autonomy, and completes",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const adapter = yield* adapterFromSavedBlock(fake);
          const sessionA = yield* Scope.make();
          const first = yield* openRuntime(fake, {
            adapter,
            runtimeMode: "auto",
            sessionScope: sessionA,
          });
          const thread = yield* ensure(first, { runtimeMode: "auto" });
          const originalId = thread.nativeThreadRef?.nativeId;
          for (const runOrdinal of [1, 2]) {
            yield* runTurn(first, fake, thread, { runOrdinal, runtimeMode: "auto" });
          }
          const snapshot = yield* first.runtime.rollbackThread(
            rollbackToTurn(thread, providerTurnsOf(first.all), 1),
          );
          const rewoundId = snapshot.providerThread.nativeThreadRef?.nativeId;
          assert.isDefined(rewoundId);
          assert.notStrictEqual(rewoundId, originalId);

          // What the orchestrator persists is the last `provider_thread.updated` of the thread.
          yield* awaitRef(first, rewoundId ?? "");
          const persisted = providerThreadUpdates(first.all).at(-1);
          assert.strictEqual(persisted?.nativeThreadRef?.nativeId, rewoundId);
          yield* Scope.close(sessionA, Exit.void);

          const second = yield* openRuntime(fake, { adapter, runtimeMode: "auto" });
          const restarted = fake.current();
          assert.notStrictEqual(restarted, undefined);
          const resumed = yield* second.runtime.resumeThread({
            providerThread: persisted ?? thread,
            threadId: THREAD_ID,
            modelSelection: selection("high"),
            runtimePolicy: policy("auto"),
          });
          assert.strictEqual(resumed.id, PROVIDER_THREAD_ID);
          assert.strictEqual(resumed.nativeThreadRef?.nativeId, rewoundId);

          yield* runTurn(second, fake, resumed, { runOrdinal: 3, runtimeMode: "auto" });

          assert.deepStrictEqual(
            restarted.requests().map((request) => request.method),
            ["droid.load_session", "droid.update_session_settings", "droid.add_user_message"],
          );
          assert.deepStrictEqual(restarted.requests()[0]?.params, {
            sessionId: rewoundId,
            loadAllMessages: true,
          });
          assert.notInclude(
            restarted
              .requests()
              .map((request) => JSON.stringify(request.params))
              .join(),
            originalId ?? "",
            "the restarted process never names the original session",
          );
          assert.deepStrictEqual(restarted.requests()[1]?.params, AUTO_SETTINGS);
          const terminal = terminals(second.all).at(-1);
          assert.strictEqual(terminal?.status, "completed");
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-HIST-007] a rollback asked of a fresh process still resumes the newest id after the next restart",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid({
            sessions: {
              "stored-session": {
                settings: FAKE_DROID_DEFAULT_SETTINGS,
                messages: [
                  { id: "u1", role: "user", content: [{ type: "text", text: "first" }] },
                  { id: "u2", role: "user", content: [{ type: "text", text: "second" }] },
                ],
              },
            },
          });
          const first = yield* openRuntime(fake);
          const row = yield* makeProviderThread("stored-session");
          const snapshot = yield* first.runtime.rollbackThread(
            rollbackToTurn(row, storedTurns(row), 1),
          );
          const rewoundId = snapshot.providerThread.nativeThreadRef?.nativeId;

          const second = yield* openRuntime(fake);
          yield* second.runtime.resumeThread({ providerThread: snapshot.providerThread });
          assert.deepStrictEqual(requestsOf(fake, "droid.load_session").at(-1)?.params, {
            sessionId: rewoundId,
            loadAllMessages: true,
          });
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("a history with plan-mode turns and a folded steer", () => {
    const text = (value: string) => [{ type: "text", text: value }];
    const stored = (
      id: string,
      role: "user" | "assistant",
      parentId: string | null,
      content: string,
      visibility?: "llm_only",
    ) => ({ id, role, parentId, content: text(content), ...(visibility ? { visibility } : {}) });

    /**
     * u1 is an auto turn. u2 and u3 are spec turns: u2 carries the `llm_only` spec reminder, u3
     * has an assistant parent. u4 is the first default turn after spec mode, so it carries "Spec
     * mode has ended.", and the steer Droid folded into it hangs off its reply with Droid's own
     * steer reminder. u5 follows.
     */
    const HISTORY = [
      stored("context-u1", "user", null, "<system-reminder>context</system-reminder>", "llm_only"),
      stored("u1", "user", "context-u1", "Remember APPLE."),
      stored("a1", "assistant", "u1", "OK"),
      stored("context-u2", "user", "a1", "<system-reminder>context</system-reminder>", "llm_only"),
      stored("u2", "user", "context-u2", "Reply pong."),
      stored(
        "__internal__system-reminder__s",
        "user",
        "u2",
        "<system-reminder>\nSpec mode is active. Do NOT edit files",
        "llm_only",
      ),
      stored("a2", "assistant", "__internal__system-reminder__s", "pong"),
      stored("u3", "user", "a2", "Reply ping."),
      stored("a3", "assistant", "u3", "ping"),
      stored("u4", "user", "a3", "Count to 60."),
      stored(
        "__internal__system-reminder__e",
        "user",
        "u4",
        "<system-reminder>Spec mode has ended.</system-reminder>",
        "llm_only",
      ),
      stored("a4", "assistant", "__internal__system-reminder__e", "1 2 3"),
      stored("steer", "user", "a4", "Then reply QUEUED."),
      stored(
        "__internal__system-reminder__f",
        "user",
        "steer",
        "<system-reminder>A follow-up user message arrived while you were working. Treat it as steering, not implicit cancellation.</system-reminder>",
        "llm_only",
      ),
      stored("a5", "assistant", "__internal__system-reminder__f", "QUEUED"),
      stored("u5", "user", "a5", "Reply done."),
      stored("a6", "assistant", "u5", "done"),
    ];

    // The visible conversation, oldest first: u1, u2, u3, u4 with its steer, u5.
    const VISIBLE = [
      ["user", "Remember APPLE."],
      ["assistant", "OK"],
      ["user", "Reply pong."],
      ["assistant", "pong"],
      ["user", "Reply ping."],
      ["assistant", "ping"],
      ["user", "Count to 60."],
      ["assistant", "1 2 3"],
      ["user", "Then reply QUEUED."],
      ["assistant", "QUEUED"],
      ["user", "Reply done."],
      ["assistant", "done"],
    ];

    it.effect(
      "[VAL-PLAN-011] rolls back to each checkpoint without dropping a spec or post-spec turn or splitting the folded steer from its turn",
      () =>
        Effect.gen(function* () {
          const checkpoints: ReadonlyArray<{
            readonly name: string;
            readonly toTurn: number | null;
            readonly rewindsTo: string | null;
            readonly visibleCount: number;
          }> = [
            { name: "thread start", toTurn: null, rewindsTo: "u1", visibleCount: 0 },
            { name: "u1, the auto turn", toTurn: 1, rewindsTo: "u2", visibleCount: 2 },
            {
              name: "u2, a spec turn with the reminder",
              toTurn: 2,
              rewindsTo: "u3",
              visibleCount: 4,
            },
            {
              name: "u3, an assistant-parented spec turn",
              toTurn: 3,
              rewindsTo: "u4",
              visibleCount: 6,
            },
            // The steer stays with u4: the cut falls before u5, not before the steer.
            {
              name: "u4, the turn the steer folded into",
              toTurn: 4,
              rewindsTo: "u5",
              visibleCount: 10,
            },
            { name: "u5, the latest turn", toTurn: 5, rewindsTo: null, visibleCount: 12 },
          ];

          const outcomes = yield* Effect.forEach(checkpoints, (checkpoint) =>
            Effect.gen(function* () {
              const fake = yield* makeFakeDroid({
                sessions: {
                  "stored-session": { settings: FAKE_DROID_DEFAULT_SETTINGS, messages: HISTORY },
                },
              });
              const opened = yield* openRuntime(fake);
              const row = yield* makeProviderThread("stored-session");
              // The steer joined u4's T3 turn, so it has no provider turn of its own.
              const turns = storedTurns(row, ["u1", "u2", "u3", "u4", "u5"]);
              const snapshot = yield* opened.runtime.rollbackThread(
                checkpoint.toTurn === null
                  ? rollbackToStart(row, turns)
                  : rollbackToTurn(row, turns, checkpoint.toTurn),
              );
              const rewind = requestsOf(fake, "droid.execute_rewind")[0];
              return {
                name: checkpoint.name,
                rewindsTo: rewind === undefined ? null : String(rewind.params["messageId"]),
                visible: snapshot.messages.map((message) => [message.role, message.text]),
              };
            }).pipe(Effect.scoped),
          );

          assert.deepStrictEqual(
            outcomes,
            checkpoints.map((checkpoint) => ({
              name: checkpoint.name,
              rewindsTo: checkpoint.rewindsTo,
              visible: VISIBLE.slice(0, checkpoint.visibleCount),
            })),
          );
        }).pipe(Effect.provide(layerTest)),
    );
  });

  describe("plan, implement, roll back", () => {
    it.effect(
      "[VAL-CROSS-002] after rolling back the implement turn, the plan turn stays a target and the next plan turn sends spec mode again",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake, { runtimeMode: "auto" });
          const thread = yield* ensure(opened, { runtimeMode: "auto" });

          yield* planTurn(opened, fake, thread, { runOrdinal: 1, runtimeMode: "auto" });
          assert.isTrue(
            opened.all.some(
              (event) =>
                event.type === "turn_item.updated" && event.turnItem.type === "proposed_plan",
            ),
            "the plan turn produced a proposed_plan",
          );
          yield* runTurn(opened, fake, thread, {
            runOrdinal: 2,
            runtimeMode: "auto",
            interactionMode: "default",
          });
          assert.deepStrictEqual(
            requestsOf(fake, "droid.update_session_settings").map(
              (request) => request.params["interactionMode"],
            ),
            ["spec", "auto"],
            "implementing switches Droid back to auto mode",
          );
          const implementSettings = requestsOf(fake, "droid.update_session_settings")[1]?.params;
          assert.strictEqual(implementSettings?.["autonomyLevel"], "medium");

          const turns = providerTurnsOf(opened.all);
          const snapshot = yield* opened.runtime.rollbackThread(rollbackToTurn(thread, turns, 1));
          assert.strictEqual(
            requestsOf(fake, "droid.execute_rewind")[0]?.params["messageId"],
            messageIdFor(2),
            "the plan turn stays; the implement turn is the one rewound",
          );
          assert.deepStrictEqual(
            toTurn(turns, 1).nativeTurnRef?.nativeId,
            messageIdFor(1),
            "the plan turn is still a rollback target",
          );

          const before = methodsOf(fake).length;
          yield* planTurn(opened, fake, snapshot.providerThread, {
            runOrdinal: 3,
            runtimeMode: "auto",
          });
          const sentAfter = requestsOf(fake, "droid.update_session_settings")
            .slice(2)
            .map((request) => request.params);
          assert.strictEqual(
            sentAfter.at(-1)?.["interactionMode"],
            "spec",
            "the plan turn sends spec mode again; the adapter does not assume the rewound session kept it",
          );
          assert.strictEqual(sentAfter.at(-1)?.["specModeModelId"], MODEL);
          assert.strictEqual(methodsOf(fake).at(-1), "droid.add_user_message");
          assert.isAbove(methodsOf(fake).length, before);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("injected context", () => {
    it.effect(
      "[VAL-TURN-021] the snapshot of a rewound session omits context-* and system-reminder messages, in events and frames alike",
      () =>
        Effect.gen(function* () {
          const CONTEXT_TEXT = "SYNTHETIC-PRIVATE-CONTEXT-0e51-never-visible";
          const REMINDER_TEXT = "SYNTHETIC-SYSTEM-REMINDER-77ad-never-visible";
          const LLM_ONLY_TEXT = "SYNTHETIC-LLM-ONLY-c3f0-never-visible";
          const text = (value: string) => [{ type: "text", text: value }];
          const fake = yield* makeFakeDroid({
            sessions: {
              "stored-session": {
                settings: FAKE_DROID_DEFAULT_SETTINGS,
                messages: [
                  {
                    id: "context-0b1c",
                    role: "user",
                    visibility: "llm_only",
                    content: text(CONTEXT_TEXT),
                  },
                  { id: "u1", role: "user", content: text("first") },
                  {
                    id: "__internal__system-reminder__91",
                    role: "user",
                    visibility: "llm_only",
                    content: text(REMINDER_TEXT),
                  },
                  { id: "a1", role: "assistant", content: text("one") },
                  { id: "v1", role: "user", visibility: "llm_only", content: text(LLM_ONLY_TEXT) },
                  { id: "u2", role: "user", content: text("second") },
                  { id: "a2", role: "assistant", content: text("two") },
                ],
              },
            },
          });
          const opened = yield* openRuntime(fake);
          const row = yield* makeProviderThread("stored-session");
          const resumed = yield* opened.runtime.resumeThread({ providerThread: row });

          const snapshot = yield* opened.runtime.rollbackThread(
            rollbackToTurn(resumed, storedTurns(row), 1),
          );
          const rewoundId = snapshot.providerThread.nativeThreadRef?.nativeId ?? "";
          yield* awaitRef(opened, rewoundId);
          const read = yield* opened.runtime.readThreadSnapshot({
            providerThread: snapshot.providerThread,
          });

          assert.deepStrictEqual(
            snapshot.messages.map((message) => message.text),
            ["first", "one"],
          );
          assert.deepStrictEqual(
            read.messages.map((message) => message.text),
            ["first", "one"],
            "the history read after the rewind reuses the same filter",
          );
          const visible = JSON.stringify({ events: opened.all, snapshot, read });
          for (const secret of [CONTEXT_TEXT, REMINDER_TEXT, LLM_ONLY_TEXT]) {
            assert.notInclude(visible, secret);
          }
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });
});
