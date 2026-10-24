import { expect, it } from "@effect/vitest";
import {
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ThreadId,
  TurnItemId,
  type ApplicationStoredEvent,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import {
  CLAUDE_MODEL_SELECTION,
  CODEX_MODEL_SELECTION,
} from "../orchestration-v2/testkit/fixtures/shared.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as AgentSessionImporter from "./AgentSessionImporter.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as ProjectService from "./ProjectService.ts";

const projectId = ProjectId.make("agent-session-import-project");
const timestamp = "2026-09-01T10:00:00.000Z";
const now = DateTime.makeUnsafe(timestamp);
const stores = Layer.mergeAll(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);
const persistence = Layer.mergeAll(
  stores,
  EventSink.layer.pipe(Layer.provide(stores)),
  ProviderSessionRuntime.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
  IdAllocator.layer,
);

function candidate(
  modelSelection: typeof CODEX_MODEL_SELECTION,
  providerSessionId: string,
  provider: "codex" | "claudeAgent" | "droid" = "codex",
  activities: ReadonlyArray<AgentSessionScanner.AgentSessionThreadActivity> = [],
) {
  const providerInstanceId = modelSelection.instanceId;
  return {
    _tag: "Importable" as const,
    refreshHistory: false,
    source: {
      provider,
      providerInstanceId,
      providerSessionId,
      filePath: `/tmp/${providerInstanceId}/${providerSessionId}.jsonl`,
      size: 100,
      mtimeMs: 2,
      device: 3,
      inode: 4,
      birthtimeMs: 1,
    },
    thread: {
      source: provider,
      providerInstanceId,
      providerSessionId,
      title: "Imported thread",
      model: modelSelection.model,
      createdAt: timestamp,
      updatedAt: timestamp,
      messages: [
        { role: "user" as const, text: "Fix it", createdAt: timestamp },
        { role: "assistant" as const, text: "Fixed", createdAt: timestamp },
      ],
      activities,
    },
  } satisfies AgentSessionScanner.AgentSessionRecentThread;
}

function importId(source: ReturnType<typeof candidate>) {
  return ThreadId.make(
    `import:${source.source.providerInstanceId}:${source.source.providerSessionId}`,
  );
}

const seedOwner = Effect.fn(function* (
  source: ReturnType<typeof candidate>,
  lifecycle: "active" | "archived" | "deleted" = "active",
) {
  const sink = yield* EventSink.EventSinkV2;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const { providerInstanceId, providerSessionId, provider } = source.source;
  const threadId = ThreadId.make(`owner:${providerInstanceId}:${providerSessionId}`);
  const driver = ProviderDriverKind.make(provider);
  const providerThread: OrchestrationV2ProviderThread = {
    id: ids.derive.providerThread({
      driver,
      nativeThreadId: providerSessionId,
    }),
    driver,
    providerInstanceId,
    providerSessionId: null,
    appThreadId: threadId,
    ownerNodeId: null,
    nativeThreadRef: { driver, nativeId: providerSessionId, strength: "strong" },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: 1,
    lastRunOrdinal: 1,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  };
  const thread: OrchestrationV2AppThread = {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId,
    title: "Native thread",
    providerInstanceId,
    modelSelection: { instanceId: providerInstanceId, model: source.thread.model },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: providerThread.id,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: lifecycle === "archived" ? now : null,
    deletedAt: lifecycle === "deleted" ? now : null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
  };
  yield* sink.write({
    events: [
      {
        id: EventId.make(`${threadId}:created`),
        type: "thread.created",
        threadId,
        occurredAt: now,
        payload: thread,
      },
      {
        id: EventId.make(`${threadId}:provider`),
        type: "provider-thread.updated",
        threadId,
        occurredAt: now,
        payload: providerThread,
      },
    ],
  });
  return threadId;
});

const makeImporter = Effect.fn(function* (
  candidates: ReadonlyArray<AgentSessionScanner.AgentSessionRecentThread>,
  beforeTransaction: Effect.Effect<void> = Effect.void,
) {
  const sql = yield* SqlClient.SqlClient;
  const events = yield* EventStore.EventStoreV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const published: Array<ApplicationStoredEvent> = [];
  const withTransaction: typeof sql.withTransaction = (effect) =>
    beforeTransaction.pipe(Effect.andThen(sql.withTransaction(effect)));
  const importSql = new Proxy(sql, {
    get: (target, key, receiver) =>
      key === "withTransaction" ? withTransaction : Reflect.get(target, key, receiver),
  });
  const sinkLayer = Layer.fresh(EventSink.layer).pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(SqlClient.SqlClient, importSql),
        Layer.succeed(ProjectionStore.ProjectionStoreV2, projections),
        Layer.succeed(EventStore.EventStoreV2, {
          ...events,
          publishCommitted: (batch) =>
            events.publishCommitted(batch).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  published.push(...batch);
                }),
              ),
            ),
        }),
      ),
    ),
  );
  const context = yield* Layer.build(
    AgentSessionImporter.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          sinkLayer,
          Layer.succeed(IdAllocator.IdAllocatorV2, yield* IdAllocator.IdAllocatorV2),
          Layer.succeed(
            ProviderSessionRuntime.ProviderSessionRuntimeRepository,
            yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository,
          ),
          Layer.mock(AgentSessionScanner.AgentSessionScanner)({
            recentThreads: () => Stream.fromIterable(candidates),
          }),
          Layer.mock(ProjectService.ProjectService)({
            getById: () =>
              Effect.succeed(
                Option.some({
                  id: projectId,
                  title: "Project",
                  workspaceRoot: "/workspace/project",
                  defaultModelSelection: null,
                  scripts: [],
                  createdAt: timestamp,
                  updatedAt: timestamp,
                  deletedAt: null,
                }),
              ),
          }),
          Layer.mock(Orchestrator.OrchestratorV2)({
            getThreadRecords: (threadId, fields) =>
              projections
                .getThreadRecords(threadId, fields)
                .pipe(
                  Effect.mapError(
                    (cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
                  ),
                ),
          }),
        ),
      ),
    ),
  );
  const importer = Context.get(context, AgentSessionImporter.AgentSessionImporter);
  return { run: importer.importRecentAgentThreads({ projectId }), published };
});

it.effect(
  "ignores owned sessions, including archived and deleted threads, but imports other instances and unowned sessions",
  () =>
    Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const events = yield* EventStore.EventStoreV2;
      const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const active = candidate(CODEX_MODEL_SELECTION, "active");
      const archived = candidate(
        CLAUDE_MODEL_SELECTION,
        "020e7f00-dce6-406b-8ec6-ed90498945d6",
        "claudeAgent",
      );
      const deleted = candidate(CODEX_MODEL_SELECTION, "deleted");
      const owners = [
        yield* seedOwner(active),
        yield* seedOwner(archived, "archived"),
        yield* seedOwner(deleted, "deleted"),
      ];
      const before = yield* Effect.forEach(owners, (id) =>
        projections.getThreadRecords(id, ["providerThreads"]),
      );
      const otherInstance = candidate(
        { ...CLAUDE_MODEL_SELECTION, instanceId: ProviderInstanceId.make("claude-other") },
        archived.source.providerSessionId,
        "claudeAgent",
      );
      const unowned = candidate(CODEX_MODEL_SELECTION, "unowned");
      const importer = yield* makeImporter([active, archived, deleted, otherInstance, unowned]);

      expect(yield* importer.run).toEqual({ importedCount: 2, skippedCount: 0 });
      for (const source of [active, archived, deleted]) {
        const threadId = importId(source);
        expect(yield* Effect.option(projections.getThread(threadId))).toEqual(Option.none());
        expect(yield* events.read({ threadId }).pipe(Stream.runCollect)).toEqual([]);
        expect(yield* runtimes.getByThreadId({ threadId })).toEqual(Option.none());
      }
      expect(
        yield* Effect.forEach(owners, (id) =>
          projections.getThreadRecords(id, ["providerThreads"]),
        ),
      ).toEqual(before);
      expect(
        new Set(
          importer.published.flatMap((stored) =>
            "event" in stored ? [stored.event.threadId] : [],
          ),
        ),
      ).toEqual(new Set([importId(otherInstance), importId(unowned)]));
      for (const source of [otherInstance, unowned]) {
        const threadId = importId(source);
        const records = yield* projections.getThreadRecords(threadId, [
          "messages",
          "providerThreads",
        ]);
        expect(records.messages.map((message) => message.text)).toEqual(["Fix it", "Fixed"]);
        expect(records.thread.historyOrigin).toBe("v1_import");
        expect(records.thread.activeProviderThreadId).toBe(records.providerThreads[0]?.id);
        expect(records.providerThreads[0]?.nativeThreadRef?.nativeId).toBe(
          source.source.providerSessionId,
        );
        expect(Option.getOrThrow(yield* runtimes.getByThreadId({ threadId }))).toMatchObject({
          providerInstanceId: source.source.providerInstanceId,
          resumeCursor:
            source.source.provider === "codex"
              ? { threadId: source.source.providerSessionId }
              : { threadId, resume: source.source.providerSessionId },
          runtimePayload: { importedTranscripts: [source.source] },
        });
      }
    }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("rechecks ownership after the import reaches the commit transaction", () =>
  Effect.gen(function* () {
    const enteringTransaction = yield* Deferred.make<void>();
    const continueTransaction = yield* Deferred.make<void>();
    const source = candidate(CODEX_MODEL_SELECTION, "claimed-during-import");
    const importer = yield* makeImporter(
      [source],
      Deferred.succeed(enteringTransaction, undefined).pipe(
        Effect.andThen(Deferred.await(continueTransaction)),
      ),
    );
    const pending = yield* Effect.forkScoped(importer.run);
    yield* Deferred.await(enteringTransaction);
    // Commit ownership after all pre-transaction reads, before the import acquires SQLite.
    const ownerId = yield* seedOwner(source);
    yield* Deferred.succeed(continueTransaction, undefined);
    expect(yield* Fiber.join(pending)).toEqual({ importedCount: 0, skippedCount: 0 });
    expect(yield* importer.run).toEqual({ importedCount: 0, skippedCount: 0 });
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const events = yield* EventStore.EventStoreV2;
    const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
    expect(yield* Effect.option(projections.getThread(importId(source)))).toEqual(Option.none());
    expect(yield* events.read({ threadId: importId(source) }).pipe(Stream.runCollect)).toEqual([]);
    expect(yield* runtimes.list()).toEqual([]);
    expect(importer.published).toEqual([]);
    expect(
      (yield* projections.getThreadRecords(ownerId, ["providerThreads"])).providerThreads[0]
        ?.appThreadId,
    ).toBe(ownerId);
  }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect(
  "retries failed commits and runtime persistence without replaying committed history",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const events = yield* EventStore.EventStoreV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const source = candidate(CODEX_MODEL_SELECTION, "retry");
      const threadId = importId(source);
      const importer = yield* makeImporter([source]);
      yield* sql`CREATE TEMP TRIGGER fail_import BEFORE INSERT ON orchestration_events WHEN NEW.event_type = 'message.updated' BEGIN SELECT RAISE(ABORT, 'test publication failure'); END`;
      expect(yield* importer.run).toEqual({
        importedCount: 0,
        skippedCount: 1,
        skippedReasons: [{ reason: "failed", count: 1 }],
      });
      expect(yield* events.read({ threadId }).pipe(Stream.runCollect)).toEqual([]);
      expect(yield* Effect.option(projections.getThread(threadId))).toEqual(Option.none());
      expect(yield* runtimes.list()).toEqual([]);
      expect(importer.published).toEqual([]);
      yield* sql`DROP TRIGGER fail_import`;

      yield* sql`CREATE TEMP TRIGGER fail_runtime BEFORE INSERT ON provider_session_runtime BEGIN SELECT RAISE(ABORT, 'test runtime failure'); END`;
      expect(yield* importer.run).toEqual({
        importedCount: 0,
        skippedCount: 1,
        skippedReasons: [{ reason: "failed", count: 1 }],
      });
      const committed = yield* events.read({ threadId }).pipe(Stream.runCollect);
      expect(committed).toHaveLength(6);
      expect(importer.published).toHaveLength(6);
      expect(yield* runtimes.list()).toEqual([]);
      yield* sql`DROP TRIGGER fail_runtime`;

      expect(yield* importer.run).toEqual({ importedCount: 1, skippedCount: 0 });
      expect(yield* importer.run).toEqual({ importedCount: 1, skippedCount: 0 });
      expect(yield* events.read({ threadId }).pipe(Stream.runCollect)).toEqual(committed);
      expect(importer.published).toHaveLength(6);
      expect(
        (yield* projections.getThreadRecords(threadId, ["messages"])).messages.map(
          (message) => message.text,
        ),
      ).toEqual(["Fix it", "Fixed"]);
      expect(Option.getOrThrow(yield* runtimes.getByThreadId({ threadId }))).toMatchObject({
        providerInstanceId: source.source.providerInstanceId,
        resumeCursor: { threadId: source.source.providerSessionId },
        runtimePayload: { importedTranscripts: [source.source] },
      });
    }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("counts empty sessions apart from sessions that could not be imported", () =>
  Effect.gen(function* () {
    const skipped = (reason: AgentSessionScanner.AgentSessionSkipReason) =>
      ({ _tag: "Skipped", reason }) as const;
    const importer = yield* makeImporter([
      skipped("empty"),
      skipped("too-large"),
      candidate(CODEX_MODEL_SELECTION, "imported"),
      skipped("empty"),
      skipped("too-large"),
      skipped("changed"),
    ]);
    expect(yield* importer.run).toEqual({
      importedCount: 1,
      skippedCount: 3,
      emptyCount: 2,
      skippedReasons: [
        { reason: "too-large", count: 2 },
        { reason: "changed", count: 1 },
      ],
    });
  }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect(
  "skips sessions a T3 thread rewound into another session, but not ones forked into an import",
  () =>
    Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const droid = { instanceId: ProviderInstanceId.make("droid"), model: "claude-opus-5-5" };
      const session = (id: string, continuedIn?: ReadonlyArray<string>) => ({
        ...candidate(droid, id, "droid"),
        ...(continuedIn === undefined ? {} : { continuedIn }),
      });
      yield* seedOwner(session("rewound"));
      const rolledBack = session("rolled-back", ["rewound", "rewound-unowned"]);
      const cliParent = session("cli-parent", ["cli-fork"]);
      const importer = yield* makeImporter([session("cli-fork"), rolledBack, cliParent]);

      expect(yield* importer.run).toEqual({ importedCount: 2, skippedCount: 0 });
      expect(yield* Effect.option(projections.getThread(importId(rolledBack)))).toEqual(
        Option.none(),
      );
      expect((yield* projections.getThread(importId(cliParent))).historyOrigin).toBe("v1_import");
    }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("imports the session's reasoning effort with its model", () =>
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const droid = { instanceId: ProviderInstanceId.make("droid"), model: "claude-opus-5-5" };
    const base = candidate(droid, "droid-effort", "droid");
    const source = { ...base, thread: { ...base.thread, reasoningEffort: "xhigh" } };
    expect(yield* (yield* makeImporter([source])).run).toEqual({
      importedCount: 1,
      skippedCount: 0,
    });
    expect((yield* projections.getThread(importId(base))).modelSelection).toEqual({
      ...droid,
      options: [{ id: "reasoningEffort", value: "xhigh" }],
    });
  }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("imports a worktree session onto its worktree and branch", () =>
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const base = candidate(CODEX_MODEL_SELECTION, "in-worktree", "codex");
    const worktree = { path: "/repo-worktrees/feature", branch: "feature/wt" };
    yield* (yield* makeImporter([{ ...base, thread: { ...base.thread, worktree } }])).run;
    const thread = yield* projections.getThread(importId(base));
    expect({ worktreePath: thread.worktreePath, branch: thread.branch }).toEqual({
      worktreePath: worktree.path,
      branch: worktree.branch,
    });
  }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("imports activity between messages as settled turn items in transcript order", () =>
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const at = { startedAt: timestamp, completedAt: timestamp };
    const source = candidate(CODEX_MODEL_SELECTION, "with-activity", "codex", [
      { position: 1, ...at, activity: { kind: "notice", message: "Earlier history was trimmed" } },
      { position: 1, ...at, activity: { kind: "reasoning", text: "Run the tests" } },
      {
        position: 1,
        ...at,
        activity: {
          kind: "tool",
          item: {
            status: "completed",
            title: "Execute",
            type: "command_execution",
            input: "vp test",
            output: "ok",
          },
        },
      },
      {
        position: 1,
        ...at,
        activity: {
          kind: "subagent",
          title: "Find the flake",
          prompt: "Find it",
          status: "completed",
          result: "setup.ts",
        },
      },
      {
        position: 2,
        ...at,
        activity: {
          kind: "tool",
          item: {
            status: "interrupted",
            title: "Grep",
            type: "dynamic_tool",
            toolName: "Grep",
            input: {},
          },
        },
      },
    ]);
    const importer = yield* makeImporter([source]);

    expect(yield* importer.run).toEqual({ importedCount: 1, skippedCount: 0 });
    const { turnItems } = yield* projections.getThreadRecords(importId(source), ["turnItems"]);
    expect(
      turnItems
        .toSorted((left, right) => left.ordinal - right.ordinal)
        .map((item) => [item.ordinal, item.type, item.status, item.runId]),
    ).toEqual([
      [1, "user_message", "completed", null],
      [2, "system_notice", "completed", null],
      [3, "reasoning", "completed", null],
      [4, "command_execution", "completed", null],
      [5, "subagent", "completed", null],
      [6, "assistant_message", "completed", null],
      [7, "dynamic_tool", "interrupted", null],
    ]);
    expect(turnItems.find((item) => item.type === "subagent")).toMatchObject({
      title: "Find the flake",
      prompt: "Find it",
      result: "setup.ts",
      childThreadId: null,
    });
    expect(turnItems.find((item) => item.type === "reasoning")).toMatchObject({
      text: "Run the tests",
      streaming: false,
      title: null,
    });
  }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect(
  "refreshes a thread imported before its thinking was, in place and up to the thread's first run",
  () =>
    Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const sink = yield* EventSink.EventSinkV2;
      const at = (second: number) => `2026-09-01T10:00:${String(second).padStart(2, "0")}.000Z`;
      const message = (role: "user" | "assistant", text: string, second: number) => ({
        role,
        text,
        createdAt: at(second),
      });
      const activity = (
        position: number,
        second: number,
        value: AgentSessionScanner.AgentSessionThreadActivity["activity"],
      ) => ({ position, startedAt: at(second), completedAt: at(second), activity: value });
      const tool = {
        kind: "tool",
        item: {
          status: "completed",
          title: "Execute",
          type: "command_execution",
          input: "vp test",
          output: "ok",
        },
      } as const;
      const droid = { instanceId: ProviderInstanceId.make("droid"), model: "claude-opus-5-5" };
      const base = candidate(droid, "droid-session", "droid");
      const threadId = importId(base);
      const original = {
        ...base,
        thread: {
          ...base.thread,
          messages: [message("user", "Fix it", 0), message("assistant", "Fixed", 3)],
          activities: [activity(1, 2, tool)],
        },
      };
      expect(yield* (yield* makeImporter([original])).run).toEqual({
        importedCount: 1,
        skippedCount: 0,
      });
      const importedIds = (yield* projections.getThreadRecords(threadId, ["turnItems"])).turnItems
        .map((item) => item.id)
        .toSorted();

      // The user continues the thread in T3; the session file grows with that run.
      const runId = RunId.make("run:refresh:1");
      const runAt = DateTime.makeUnsafe(at(10));
      yield* sink.write({
        events: [
          {
            id: EventId.make("event:refresh:run"),
            type: "run.created",
            threadId,
            runId,
            occurredAt: runAt,
            payload: {
              id: runId,
              threadId,
              ordinal: 1,
              providerInstanceId: droid.instanceId,
              modelSelection: droid,
              providerThreadId: null,
              userMessageId: MessageId.make("message:refresh:run"),
              rootNodeId: null,
              activeAttemptId: null,
              status: "completed",
              requestedAt: runAt,
              startedAt: runAt,
              completedAt: runAt,
              checkpointId: null,
              contextHandoffId: null,
            },
          },
          {
            id: EventId.make("event:refresh:reply"),
            type: "turn-item.updated",
            threadId,
            runId,
            occurredAt: runAt,
            payload: {
              id: TurnItemId.make("turn-item:refresh:reply"),
              threadId,
              runId,
              nodeId: null,
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: 0,
              status: "completed",
              title: null,
              startedAt: runAt,
              completedAt: runAt,
              updatedAt: runAt,
              type: "assistant_message",
              messageId: MessageId.make("message:refresh:reply"),
              text: "Live reply",
              streaming: false,
            },
          },
        ],
      });

      const refreshed = {
        ...original,
        refreshHistory: true,
        source: { ...original.source, size: 200, historyVersion: 2 },
        thread: {
          ...original.thread,
          messages: [
            ...original.thread.messages,
            message("user", "Live prompt", 11),
            message("assistant", "Live reply", 12),
          ],
          activities: [
            activity(1, 1, { kind: "reasoning", text: "Check the tests" }),
            activity(1, 2, tool),
            activity(1, 3, { kind: "reasoning", text: "They pass" }),
            activity(3, 12, { kind: "reasoning", text: "Thinking during the run" }),
          ],
        },
      };
      expect(yield* (yield* makeImporter([refreshed])).run).toEqual({
        importedCount: 1,
        skippedCount: 0,
      });

      const { turnItems } = yield* projections.getThreadRecords(threadId, ["turnItems"]);
      const ordered = turnItems.toSorted((left, right) => left.ordinal - right.ordinal);
      expect(
        ordered.map((item) => [item.type, "text" in item ? item.text : item.title, item.runId]),
      ).toEqual([
        ["user_message", "Fix it", null],
        ["reasoning", "Check the tests", null],
        ["command_execution", "Execute", null],
        ["reasoning", "They pass", null],
        ["assistant_message", "Fixed", null],
        ["assistant_message", "Live reply", runId],
      ]);
      expect(ordered.at(-1)?.ordinal).toBe(1_000_001);
      expect(
        importedIds.every((id) => turnItems.some((item) => item.id === id && item.runId === null)),
      ).toBe(true);
      expect(
        Option.getOrThrow(yield* runtimes.getByThreadId({ threadId })).runtimePayload,
      ).toMatchObject({ importedTranscripts: [refreshed.source] });
    }).pipe(Effect.scoped, Effect.provide(persistence)),
);
