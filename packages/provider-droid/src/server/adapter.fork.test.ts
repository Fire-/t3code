import { assert, describe, it } from "@effect/vitest";
import {
  CheckpointId,
  NodeId,
  ThreadId,
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
  ensure,
  issueT3Mcp,
  layerTest,
  makeOfferQueue,
  makeProviderThread,
  messageIdFor,
  methodsOf,
  openRuntime,
  providerThreadUpdates,
  providerTurnsOf,
  refIds,
  requestsOf,
  runTurn,
  startTurn,
  THREAD_ID,
  wakeMessage,
} from "./adapterHarness.testkit.ts";
import { FAKE_DROID_DEFAULT_SETTINGS, makeFakeDroid, type FakeDroid } from "./fakeDroid.testkit.ts";

const isForkError = Schema.is(ProviderAdapter.ProviderAdapterForkThreadError);

const forkError = (error: unknown) =>
  isForkError(error) ? error : assert.fail("expected a ProviderAdapterForkThreadError");

type ForkInput = ProviderAdapter.ProviderAdapterV2ForkThreadInput;

const FORK_TITLE = "T3 Code fork";
const TARGET_THREAD_ID = ThreadId.make("thread-droid-fork-target");
const OWNER_NODE_ID = NodeId.make("node:fork-owner");
const SOURCE_ID = "source-session";
const INJECTED = "SYNTHETIC-PRIVATE-CONTEXT-DO-NOT-LEAK";
const REMINDER = "SYNTHETIC-REMINDER-DO-NOT-LEAK";
const OTHER_THREAD_ID = "provider-thread:other" as OrchestrationV2ProviderThread["id"];

const text = (value: string) => [{ type: "text", text: value }];

/** A stored session with Droid's plumbing next to three user turns. */
const SOURCE_MESSAGES = [
  { id: "context-1", role: "user", visibility: "llm_only", content: text(INJECTED) },
  { id: "u1", role: "user", content: text("first question") },
  { id: "a1", role: "assistant", content: text("first answer") },
  {
    id: "__internal__system-reminder__1",
    role: "user",
    visibility: "llm_only",
    content: text(REMINDER),
  },
  { id: "u2", role: "user", content: text("second question") },
  { id: "a2", role: "assistant", content: text("second answer") },
  { id: "u3", role: "user", content: text("third question") },
  { id: "a3", role: "assistant", content: text("third answer") },
];

const storedSource = () =>
  makeFakeDroid({
    sessions: {
      [SOURCE_ID]: { settings: FAKE_DROID_DEFAULT_SETTINGS, messages: SOURCE_MESSAGES },
    },
  });

const turnFor = (
  providerThread: Pick<OrchestrationV2ProviderThread, "id" | "createdAt">,
  ordinal: number,
  nativeId: string | null,
): OrchestrationV2ProviderTurn => ({
  id: `provider-turn:${providerThread.id}:${ordinal}` as OrchestrationV2ProviderTurn["id"],
  providerThreadId: providerThread.id,
  nodeId: NodeId.make(`node:fork-turn:${ordinal}`),
  runAttemptId: null,
  nativeTurnRef:
    nativeId === null ? null : { driver: DROID_PROVIDER, nativeId, strength: "strong" as const },
  ordinal,
  status: "completed",
  startedAt: providerThread.createdAt,
  completedAt: providerThread.createdAt,
});

const foreignThread = (source: OrchestrationV2ProviderThread) => ({
  id: OTHER_THREAD_ID,
  createdAt: source.createdAt,
});

/** The orchestrator's source thread: a stored session and the turns it recorded. */
const sourceOnStoredSession = Effect.fnUntraced(function* () {
  const source = yield* makeProviderThread(SOURCE_ID);
  const turns = ["u1", "u2", "u3"].map((nativeId, index) => turnFor(source, index + 1, nativeId));
  return { source, turns };
});

const forkInput = (
  sourceProviderThread: OrchestrationV2ProviderThread,
  options: {
    readonly turns?: ReadonlyArray<OrchestrationV2ProviderTurn>;
    readonly target?: OrchestrationV2ProviderTurn["id"] | undefined;
  } = {},
): ForkInput => ({
  sourceProviderThread,
  ...(options.turns === undefined ? {} : { sourceProviderTurns: options.turns }),
  ...(options.target === undefined ? {} : { providerTurnId: options.target }),
  targetThreadId: TARGET_THREAD_ID,
  ownerNodeId: OWNER_NODE_ID,
});

const messageIdsOf = (fake: FakeDroid, sessionId: string | null | undefined) =>
  (fake.sessions.get(String(sessionId))?.messages ?? []).map((message) =>
    Schema.is(Schema.Struct({ id: Schema.String }))(message) ? message.id : undefined,
  );

const loadedSessions = (fake: FakeDroid) =>
  requestsOf(fake, "droid.load_session").map((request) => request.params["sessionId"]);

const HEAD_METHODS = ["droid.load_session", "droid.fork_session", "droid.load_session"];
const REWOUND_METHODS = [
  "droid.load_session",
  "droid.fork_session",
  "droid.execute_rewind",
  "droid.load_session",
];

describe("DroidAdapterV2 fork", () => {
  it.effect(
    "[VAL-HIST-015] a fork from the head loads the source, forks it, loads the fork, and returns a thread for the target bound to the new session",
    () =>
      Effect.gen(function* () {
        const fake = yield* storedSource();
        const before = JSON.stringify(fake.sessions.get(SOURCE_ID));
        const opened = yield* openRuntime(fake);
        const { source } = yield* sourceOnStoredSession();

        const forked = yield* opened.runtime.forkThread(forkInput(source));

        const forkId = forked.nativeThreadRef?.nativeId;
        assert.isDefined(forkId);
        assert.notStrictEqual(forkId, SOURCE_ID);
        assert.deepStrictEqual(methodsOf(fake), HEAD_METHODS);
        assert.deepStrictEqual(
          requestsOf(fake, "droid.load_session").map((request) => request.params),
          [
            { sessionId: SOURCE_ID, loadAllMessages: true },
            { sessionId: forkId, loadAllMessages: true },
          ],
        );
        assert.deepStrictEqual(requestsOf(fake, "droid.fork_session")[0]?.params, {
          title: FORK_TITLE,
        });
        assert.strictEqual(forked.appThreadId, TARGET_THREAD_ID);
        assert.strictEqual(forked.ownerNodeId, OWNER_NODE_ID);
        assert.strictEqual(forked.nativeThreadRef?.strength, "strong");
        assert.strictEqual(forked.nativeThreadRef?.driver, DROID_PROVIDER);
        assert.notStrictEqual(forked.id, source.id);
        assert.deepStrictEqual(forked.forkedFrom, { providerThreadId: source.id });
        assert.strictEqual(forked.status, "idle");
        assert.isNull(forked.firstRunOrdinal);
        assert.isNull(forked.lastRunOrdinal);
        assert.deepStrictEqual(forked.handoffIds, []);
        assert.deepStrictEqual(forked.pendingBackgroundTasks, []);
        assert.strictEqual(fake.current().sessionId(), forkId, "the process serves the fork");
        assert.strictEqual(fake.sessions.get(forkId ?? "")?.title, FORK_TITLE);
        assert.strictEqual(
          JSON.stringify(fake.sessions.get(SOURCE_ID)),
          before,
          "the source session is unchanged",
        );
        assert.deepStrictEqual(messageIdsOf(fake, forkId), messageIdsOf(fake, SOURCE_ID));

        yield* fake.current().settle;
        assert.deepStrictEqual(
          providerThreadUpdates(opened.all),
          [],
          "the orchestrator persists the returned thread; nothing is published for it",
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("a fork loads the source and the rewound copy with the target thread's T3 server", () =>
    Effect.gen(function* () {
      yield* issueT3Mcp(THREAD_ID, "source-token");
      const targetServers = yield* issueT3Mcp(TARGET_THREAD_ID, "target-token");
      const fake = yield* storedSource();
      const opened = yield* openRuntime(fake);
      const { source, turns } = yield* sourceOnStoredSession();

      yield* opened.runtime.forkThread(forkInput(source, { turns, target: turns[0]?.id }));

      assert.deepStrictEqual(methodsOf(fake), REWOUND_METHODS);
      assert.deepStrictEqual(
        requestsOf(fake, "droid.load_session").map((request) => request.params["mcpServers"]),
        [targetServers, targetServers],
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-015] a fork from the latest turn is a fork from the head: no rewind is sent",
    () =>
      Effect.gen(function* () {
        const fake = yield* storedSource();
        const opened = yield* openRuntime(fake);
        const { source, turns } = yield* sourceOnStoredSession();
        const latest = turns[2];

        const forked = yield* opened.runtime.forkThread(
          forkInput(source, { turns, target: latest?.id }),
        );

        assert.deepStrictEqual(methodsOf(fake), HEAD_METHODS);
        assert.deepStrictEqual(forked.forkedFrom, {
          providerThreadId: source.id,
          providerTurnId: latest?.id,
        });
        assert.deepStrictEqual(
          messageIdsOf(fake, forked.nativeThreadRef?.nativeId),
          messageIdsOf(fake, SOURCE_ID),
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-015] a fork from an earlier turn rewinds the fork before the first later turn and returns the rewound session",
    () =>
      Effect.gen(function* () {
        const fake = yield* storedSource();
        const before = JSON.stringify(fake.sessions.get(SOURCE_ID));
        const opened = yield* openRuntime(fake);
        const { source, turns } = yield* sourceOnStoredSession();
        const earlier = turns[0];

        const forked = yield* opened.runtime.forkThread(
          forkInput(source, { turns, target: earlier?.id }),
        );

        const requests = fake.current().requests();
        assert.deepStrictEqual(
          requests.map((request) => request.method),
          REWOUND_METHODS,
        );
        const forkId = String(requests[2]?.params["sessionId"]);
        assert.isTrue(fake.sessions.has(forkId), "the rewind names a session Droid created");
        assert.notStrictEqual(forkId, SOURCE_ID);
        assert.deepStrictEqual(requests[2]?.params, {
          sessionId: forkId,
          messageId: "u2",
          filesToRestore: [],
          filesToDelete: [],
          forkTitle: FORK_TITLE,
        });
        const finalId = forked.nativeThreadRef?.nativeId;
        assert.notStrictEqual(finalId, forkId, "the rewind's session is the one returned");
        assert.notStrictEqual(finalId, SOURCE_ID);
        assert.deepStrictEqual(requests[3]?.params, { sessionId: finalId, loadAllMessages: true });
        assert.deepStrictEqual(messageIdsOf(fake, finalId), [
          "context-1",
          "u1",
          "a1",
          "__internal__system-reminder__1",
        ]);
        assert.deepStrictEqual(forked.forkedFrom, {
          providerThreadId: source.id,
          providerTurnId: earlier?.id,
        });
        assert.strictEqual(forked.appThreadId, TARGET_THREAD_ID);
        assert.strictEqual(
          JSON.stringify(fake.sessions.get(SOURCE_ID)),
          before,
          "the source session is unchanged",
        );
        assert.deepStrictEqual(
          messageIdsOf(fake, forkId),
          messageIdsOf(fake, SOURCE_ID),
          "the intermediate fork keeps its history on Droid's side",
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("[VAL-HIST-015] a middle turn drops only the turns after it", () =>
    Effect.gen(function* () {
      const fake = yield* storedSource();
      const opened = yield* openRuntime(fake);
      const { source, turns } = yield* sourceOnStoredSession();

      const forked = yield* opened.runtime.forkThread(
        forkInput(source, { turns, target: turns[1]?.id }),
      );

      assert.strictEqual(requestsOf(fake, "droid.execute_rewind")[0]?.params["messageId"], "u3");
      assert.deepStrictEqual(messageIdsOf(fake, forked.nativeThreadRef?.nativeId), [
        "context-1",
        "u1",
        "a1",
        "__internal__system-reminder__1",
        "u2",
        "a2",
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  describe("across a wake turn", () => {
    const WAKE_MESSAGE_ID = "task-completion:7a6b5c4d";

    // The report Droid keeps between the turn that launched the task and the next user message,
    // and the reply of the turn Droid starts for it.
    const wakeSource = () =>
      makeFakeDroid({
        sessions: {
          [SOURCE_ID]: {
            settings: FAKE_DROID_DEFAULT_SETTINGS,
            messages: [
              { id: "u1", role: "user", content: text("first question") },
              { id: "a1", role: "assistant", content: text("first answer") },
              {
                id: WAKE_MESSAGE_ID,
                role: "user",
                visibility: "llm_only",
                content: text("Background task completed."),
              },
              { id: "a-wake", role: "assistant", content: text("the task is done") },
              { id: "u2", role: "user", content: text("second question") },
              { id: "a2", role: "assistant", content: text("second answer") },
            ],
          },
        },
      });

    const wakeTurns = Effect.fnUntraced(function* () {
      const source = yield* makeProviderThread(SOURCE_ID);
      const turns = ["u1", WAKE_MESSAGE_ID, "u2"].map((nativeId, index) =>
        turnFor(source, index + 1, nativeId),
      );
      return { source, turns };
    });

    it.effect(
      "[VAL-HIST-015] a fork from the turn before a wake turn is rewound at the wake's task-completion message",
      () =>
        Effect.gen(function* () {
          const fake = yield* wakeSource();
          const opened = yield* openRuntime(fake);
          const { source, turns } = yield* wakeTurns();

          const forked = yield* opened.runtime.forkThread(
            forkInput(source, { turns, target: turns[0]?.id }),
          );

          assert.strictEqual(
            requestsOf(fake, "droid.execute_rewind")[0]?.params["messageId"],
            WAKE_MESSAGE_ID,
          );
          assert.deepStrictEqual(messageIdsOf(fake, forked.nativeThreadRef?.nativeId), [
            "u1",
            "a1",
          ]);
          assert.strictEqual(messageIdsOf(fake, SOURCE_ID).length, 6, "the source is unchanged");
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-HIST-015] a fork from a wake turn keeps the report and its reply and is rewound at the next user message",
      () =>
        Effect.gen(function* () {
          const fake = yield* wakeSource();
          const opened = yield* openRuntime(fake);
          const { source, turns } = yield* wakeTurns();

          const forked = yield* opened.runtime.forkThread(
            forkInput(source, { turns, target: turns[1]?.id }),
          );

          assert.strictEqual(
            requestsOf(fake, "droid.execute_rewind")[0]?.params["messageId"],
            "u2",
          );
          assert.deepStrictEqual(messageIdsOf(fake, forked.nativeThreadRef?.nativeId), [
            "u1",
            "a1",
            WAKE_MESSAGE_ID,
            "a-wake",
          ]);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  it.effect(
    "[VAL-HIST-015] a mixed input: other provider threads' turns and rolled-back turns are ignored, counting turns as the input arrives",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        // Three turns, then a rollback to turn 1 and a new turn: provider turns 2 and 3 name
        // messages the live history no longer holds.
        const origin = yield* openRuntime(fake, { runtimeMode: "auto" });
        const thread = yield* ensure(origin, { runtimeMode: "auto" });
        for (const runOrdinal of [1, 2, 3]) {
          yield* runTurn(origin, fake, thread, { runOrdinal, runtimeMode: "auto" });
        }
        const recorded = providerTurnsOf(origin.all);
        const first = recorded[0];
        assert.isDefined(first);
        const rolledBack = yield* origin.runtime.rollbackThread({
          providerThread: thread,
          target: {
            type: "provider_turn",
            checkpointId: CheckpointId.make("checkpoint:droid-fork:1"),
            appRunOrdinal: 1,
            providerTurn: first as OrchestrationV2ProviderTurn,
          },
          providerThreadTurns: recorded,
        });
        yield* runTurn(origin, fake, rolledBack.providerThread, {
          runOrdinal: 4,
          runtimeMode: "auto",
        });
        const sourceRow = rolledBack.providerThread;
        const sourceTurns = providerTurnsOf(origin.all);
        assert.deepStrictEqual(
          sourceTurns.map((turn) => turn.ordinal),
          [1, 2, 3, 4],
        );
        assert.deepStrictEqual(messageIdsOf(fake, sourceRow.nativeThreadRef?.nativeId), [
          messageIdFor(1),
          messageIdFor(4),
        ]);
        const strangers = [
          turnFor(foreignThread(sourceRow), 2, "u-other"),
          turnFor(foreignThread(sourceRow), 5, "u-other-2"),
        ];
        const turns = [...strangers, ...sourceTurns];

        const target = yield* openRuntime(fake);
        const forked = yield* target.runtime.forkThread(
          forkInput(sourceRow, { turns, target: sourceTurns[0]?.id }),
        );

        const targetProcess = fake.current();
        assert.deepStrictEqual(targetProcess.methods(), REWOUND_METHODS);
        assert.strictEqual(
          targetProcess.requests()[2]?.params["messageId"],
          messageIdFor(4),
          "the first later turn the history still holds, not a rolled-back or foreign one",
        );
        assert.deepStrictEqual(messageIdsOf(fake, forked.nativeThreadRef?.nativeId), [
          messageIdFor(1),
        ]);

        // The latest turn is the head even though rolled-back ordinals sit below it.
        const head = yield* openRuntime(fake);
        yield* head.runtime.forkThread(forkInput(sourceRow, { turns, target: sourceTurns[3]?.id }));
        assert.deepStrictEqual(fake.current().methods(), HEAD_METHODS);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-015] a turn of another provider thread is never the rewind boundary, even when its message is in the history",
    () =>
      Effect.gen(function* () {
        const fake = yield* storedSource();
        const opened = yield* openRuntime(fake);
        const source = yield* makeProviderThread(SOURCE_ID);
        const turns = [
          turnFor(source, 1, "u1"),
          turnFor(foreignThread(source), 2, "u2"),
          turnFor(source, 3, "u3"),
        ];

        yield* opened.runtime.forkThread(forkInput(source, { turns, target: turns[0]?.id }));

        assert.strictEqual(requestsOf(fake, "droid.execute_rewind")[0]?.params["messageId"], "u3");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("[VAL-HIST-015] a target that is not one of the source thread's turns is refused", () =>
    Effect.gen(function* () {
      const fake = yield* storedSource();
      const opened = yield* openRuntime(fake);
      const { source, turns } = yield* sourceOnStoredSession();
      const foreign = turnFor(foreignThread(source), 2, "u-other");
      const unknown = turnFor(source, 9, "u9");

      for (const target of [foreign, unknown]) {
        const error = forkError(
          yield* opened.runtime
            .forkThread(forkInput(source, { turns: [foreign, ...turns], target: target.id }))
            .pipe(Effect.flip),
        );
        assert.strictEqual(error.providerThreadId, source.id);
      }
      assert.deepStrictEqual(methodsOf(fake), [], "nothing was sent for an unusable target");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-015] the loaded fork's history is filtered like any loaded session: injected context and reminders never surface",
    () =>
      Effect.gen(function* () {
        const fake = yield* storedSource();
        const opened = yield* openRuntime(fake);
        const { source, turns } = yield* sourceOnStoredSession();

        const forked = yield* opened.runtime.forkThread(
          forkInput(source, { turns, target: turns[1]?.id }),
        );
        const snapshot = yield* opened.runtime.readThreadSnapshot({ providerThread: forked });
        yield* fake.current().settle;

        assert.deepStrictEqual(
          snapshot.messages.map((message) => message.text),
          ["first question", "first answer", "second question", "second answer"],
        );
        const everything = JSON.stringify([snapshot, opened.all, forked]);
        assert.notInclude(everything, INJECTED);
        assert.notInclude(everything, REMINDER);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-015] a fork is refused, before anything is sent, while a turn is active",
    () =>
      Effect.gen(function* () {
        const fake = yield* storedSource();
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened);
        yield* startTurn(opened.runtime, thread);
        const { source } = yield* sourceOnStoredSession();
        const sent = fake.current().methods().length;

        forkError(yield* opened.runtime.forkThread(forkInput(source)).pipe(Effect.flip));

        assert.strictEqual(fake.current().methods().length, sent);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-SUB-014] a fork is refused, before anything is sent, while a wake is pending",
    () =>
      Effect.gen(function* () {
        const fake = yield* storedSource();
        const { offers, continuationRequests } = yield* makeOfferQueue();
        const opened = yield* openRuntime(fake, { continuationRequests });
        const thread = yield* ensure(opened);
        const process = fake.current();
        yield* process.emit(wakeMessage("1f2e3d4c"));
        yield* process.settle;
        yield* Queue.take(offers);
        const { source } = yield* sourceOnStoredSession();
        const sent = process.methods().length;

        forkError(yield* opened.runtime.forkThread(forkInput(source)).pipe(Effect.flip));

        assert.strictEqual(process.methods().length, sent, "no load_session and no fork_session");
        assert.deepStrictEqual(requestsOf(fake, "droid.fork_session"), [], "nothing was forked");
        assert.isTrue(yield* opened.runtime.hasPendingBackgroundWorkForThread!(thread));
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-015] a source without a Droid session is refused before anything is sent",
    () =>
      Effect.gen(function* () {
        const fake = yield* storedSource();
        const opened = yield* openRuntime(fake);
        const source = yield* makeProviderThread(null);

        forkError(yield* opened.runtime.forkThread(forkInput(source)).pipe(Effect.flip));

        assert.deepStrictEqual(methodsOf(fake), []);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-015] a source Droid cannot load fails the fork with the typed error and leaves the runtime usable",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake);
        const source = yield* makeProviderThread("session-that-is-gone");

        const error = forkError(
          yield* opened.runtime.forkThread(forkInput(source)).pipe(Effect.flip),
        );

        assert.strictEqual(error.providerThreadId, source.id);
        assert.deepStrictEqual(methodsOf(fake), ["droid.load_session"]);
        const thread = yield* ensure(opened);
        yield* runTurn(opened, fake, thread);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-015] a fork that fails halfway puts the process back on the session the runtime already serves",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({
          sessions: {
            [SOURCE_ID]: { settings: FAKE_DROID_DEFAULT_SETTINGS, messages: SOURCE_MESSAGES },
          },
          overrides: {
            "droid.fork_session": () => ({ error: { code: -32603, message: "fork failed" } }),
          },
        });
        const opened = yield* openRuntime(fake);
        const bound = yield* ensure(opened);
        const boundId = bound.nativeThreadRef?.nativeId;
        const { source } = yield* sourceOnStoredSession();

        forkError(yield* opened.runtime.forkThread(forkInput(source)).pipe(Effect.flip));

        assert.deepStrictEqual(loadedSessions(fake), [SOURCE_ID, boundId]);
        assert.strictEqual(fake.current().sessionId(), boundId);
        yield* startTurn(opened.runtime, bound);
        assert.deepStrictEqual(messageIdsOf(fake, boundId), [messageIdFor(1)]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-HIST-015] the forked thread takes turns: the next one re-sends the settings and then the message",
    () =>
      Effect.gen(function* () {
        const fake = yield* storedSource();
        const opened = yield* openRuntime(fake, { runtimeMode: "auto" });
        const { source, turns } = yield* sourceOnStoredSession();
        const forked = yield* opened.runtime.forkThread(
          forkInput(source, { turns, target: turns[0]?.id }),
        );

        yield* runTurn(opened, fake, forked, { runOrdinal: 1, runtimeMode: "auto" });

        assert.deepStrictEqual(fake.current().methods().slice(-2), [
          "droid.update_session_settings",
          "droid.add_user_message",
        ]);
        const finalId = forked.nativeThreadRef?.nativeId;
        assert.strictEqual(requestsOf(fake, "droid.add_user_message").length, 1);
        assert.strictEqual(fake.current().sessionId(), finalId);
        const published = providerThreadUpdates(opened.all);
        assert.deepStrictEqual([...new Set(refIds(published))], [finalId]);
        assert.deepStrictEqual([...new Set(published.map((thread) => thread.id))], [forked.id]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});

describe("DroidAdapterV2 restart after a fork", () => {
  it.effect(
    "[VAL-HIST-007] from the saved block: fork from an earlier turn, close and reopen, and the next turn loads the fork's final id and never an earlier one",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const adapter = yield* adapterFromSavedBlock(fake);

        const sourceScope = yield* Scope.make();
        const origin = yield* openRuntime(fake, {
          adapter,
          runtimeMode: "auto",
          sessionScope: sourceScope,
        });
        const thread = yield* ensure(origin, { runtimeMode: "auto" });
        for (const runOrdinal of [1, 2]) {
          yield* runTurn(origin, fake, thread, { runOrdinal, runtimeMode: "auto" });
        }
        const sourceTurns = providerTurnsOf(origin.all);
        const sourceId = thread.nativeThreadRef?.nativeId;

        const targetScope = yield* Scope.make();
        const target = yield* openRuntime(fake, {
          adapter,
          runtimeMode: "auto",
          sessionScope: targetScope,
        });
        const forked = yield* target.runtime.forkThread(
          forkInput(thread, { turns: sourceTurns, target: sourceTurns[0]?.id }),
        );
        const finalId = forked.nativeThreadRef?.nativeId ?? "";
        const intermediateId = requestsOf(fake, "droid.execute_rewind")[0]?.params["sessionId"];
        assert.notStrictEqual(finalId, sourceId);
        assert.notStrictEqual(finalId, intermediateId);
        yield* Scope.close(targetScope, Exit.void);
        yield* Scope.close(sourceScope, Exit.void);

        const restarted = yield* openRuntime(fake, { adapter, runtimeMode: "auto" });
        const process = fake.current();
        const resumed = yield* restarted.runtime.resumeThread({
          providerThread: forked,
          threadId: TARGET_THREAD_ID,
        });
        assert.strictEqual(resumed.id, forked.id);
        assert.strictEqual(resumed.nativeThreadRef?.nativeId, finalId);
        yield* runTurn(restarted, fake, resumed, { runOrdinal: 3, runtimeMode: "auto" });

        assert.deepStrictEqual(
          process.requests().map((request) => request.method),
          ["droid.load_session", "droid.update_session_settings", "droid.add_user_message"],
        );
        assert.deepStrictEqual(process.requests()[0]?.params, {
          sessionId: finalId,
          loadAllMessages: true,
        });
        assert.deepStrictEqual(
          [...new Set(refIds(providerThreadUpdates(restarted.all)))],
          [finalId],
          "nothing names the source or the intermediate session",
        );
        assert.deepStrictEqual(messageIdsOf(fake, finalId), [messageIdFor(1), messageIdFor(3)]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
