import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProjectId,
  ThreadId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ThreadProjection,
  type ProviderReplayTranscript,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import { makeProviderReplayGate } from "@t3tools/provider-testing/replayGate";
import { readProviderReplayTranscript } from "@t3tools/provider-testing/replayTranscript";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import {
  DROID_TASK_COMPLETION_LABEL,
  DroidOrchestratorReplayHarness,
} from "./Adapters/DroidAdapterV2.testkit.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import {
  DROID_BACKGROUND_SUBAGENT_PROMPT,
  DROID_BACKGROUND_SUBAGENT_WAKE_REPLY,
} from "./testkit/fixtures/droid_background_subagent/input.ts";
import {
  DROID_MODEL_SELECTION,
  MULTI_TURN_FIRST_PROMPT,
  MULTI_TURN_SECOND_PROMPT,
} from "./testkit/fixtures/shared.ts";

const threadId = ThreadId.make("thread:droid-queued-messages");
const projectId = ProjectId.make("project:droid-queued-messages");

const readTranscript = (fixture: string) =>
  readProviderReplayTranscript(
    new URL(`./testkit/fixtures/${fixture}/droid_transcript.ndjson`, import.meta.url),
  ).pipe(Effect.provide(NodeServices.layer));

function assistantText(projection: OrchestrationV2ThreadProjection, runId: string | undefined) {
  return projection.turnItems
    .flatMap((item) =>
      item.runId === runId && item.type === "assistant_message" ? [item.text.trim()] : [],
    )
    .join("\n");
}

function recordedWakeId(transcript: ProviderReplayTranscript): string {
  const report = transcript.entries.find(
    (entry) => entry.type === "emit_inbound" && entry.label === DROID_TASK_COMPLETION_LABEL,
  );
  const message = JSON.stringify(report);
  const id = /"id":"(task-completion:[^"]+)"/u.exec(message)?.[1];
  if (id === undefined) throw new Error("the transcript has no task-completion report");
  return id;
}

/**
 * Runs Droid's replayed transcript with the effect worker off, so a run that finished its native
 * turn stays `waiting` (and blocking) until the test drains the worker by hand.
 */
const withDroidReplay = <A, E, R>(
  transcript: ProviderReplayTranscript,
  options: { readonly gate: ReturnType<typeof makeProviderReplayGate> | undefined },
  body: (input: {
    readonly orchestrator: Orchestrator.OrchestratorV2["Service"];
    readonly worker: EffectWorker.OrchestrationEffectWorkerV2["Service"];
    readonly cwd: string;
    readonly sessionStatuses: Ref.Ref<ReadonlyArray<string>>;
    readonly watch: (
      predicate: (event: OrchestrationV2DomainEvent) => boolean,
    ) => Effect.Effect<Fiber.Fiber<void, Orchestrator.OrchestratorV2Error>, never, Scope.Scope>;
  }) => Effect.Effect<A, E, R>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("droid-queued-messages");
      const decoded = yield* DroidOrchestratorReplayHarness.decodeTranscript(transcript);
      const registry = DroidOrchestratorReplayHarness.makeProviderAdapterRegistryLayer(
        decoded,
        options.gate === undefined ? {} : { replayGate: options.gate },
      );
      return yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const sessionStatuses = yield* Ref.make<ReadonlyArray<string>>([]);
        yield* orchestrator.streamDomainEvents.pipe(
          Stream.runForEach((event) =>
            event.type === "provider-session.updated" || event.type === "provider-session.attached"
              ? Ref.update(sessionStatuses, (seen) => [...seen, event.payload.status])
              : Effect.void,
          ),
          Effect.forkScoped,
        );
        const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
          orchestrator.streamDomainEvents.pipe(
            Stream.filter(predicate),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
            Effect.tap(() => Effect.yieldNow),
          );
        return yield* body({ orchestrator, worker, cwd, sessionStatuses, watch });
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry({ name: "droid-queued-messages" }, registry, {
            runEffectWorker: false,
            runContinuationWorker: true,
          }),
        ),
      );
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

const createThread = (orchestrator: Orchestrator.OrchestratorV2["Service"], cwd: string) =>
  orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("create"),
    threadId,
    projectId,
    title: "Droid queued messages",
    modelSelection: DROID_MODEL_SELECTION,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: cwd,
    createdBy: "user",
    creationSource: "web",
  });

const isRunWaiting = (event: OrchestrationV2DomainEvent) =>
  event.type === "run.updated" && event.payload.status === "waiting";

it.effect(
  "[VAL-SUB-017] a wake reported while the previous run is still finalizing is queued behind it and runs once",
  () =>
    Effect.gen(function* () {
      const transcript = yield* readTranscript("droid_background_subagent");
      const gate = makeProviderReplayGate([DROID_TASK_COMPLETION_LABEL]);
      yield* withDroidReplay(
        transcript,
        { gate },
        Effect.fn(function* ({ orchestrator, worker, cwd, sessionStatuses, watch }) {
          yield* createThread(orchestrator, cwd);
          const rootWaiting = yield* watch(isRunWaiting);
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("first"),
            threadId,
            messageId: MessageId.make("first"),
            text: DROID_BACKGROUND_SUBAGENT_PROMPT,
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          // One effect: start the turn. Draining on would also run the checkpoint capture the
          // finished turn enqueues and settle run 1 before the report arrives.
          yield* worker.drain(1);
          yield* Fiber.join(rootWaiting);
          // Droid ended its turn and then went idle; T3 still holds run 1 until capture runs.
          yield* Effect.promise(() => gate.waitForReached(DROID_TASK_COMPLETION_LABEL));
          const finalizing = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(
            finalizing.runs.map((run) => run.status),
            ["waiting"],
          );

          const queuedRun = yield* watch(
            (event) =>
              (event.type === "run.created" || event.type === "run.updated") &&
              event.payload.status === "queued",
          );
          const stopped = yield* watch(
            (event) =>
              (event.type === "provider-session.updated" ||
                event.type === "provider-session.attached") &&
              (event.payload.status === "stopped" || event.payload.status === "error"),
          );
          gate.release(DROID_TASK_COMPLETION_LABEL);
          const outcome = yield* Effect.raceFirst(
            Fiber.join(queuedRun).pipe(Effect.as("queued" as const)),
            Fiber.join(stopped).pipe(Effect.as("stopped" as const)),
          );
          assert.equal(outcome, "queued", "the wake is queued, not refused and killed");

          const queued = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(
            queued.runs.map((run) => run.status),
            ["waiting", "queued"],
          );
          const continuation = queued.runs[1]!;
          const message = queued.messages.find(
            (candidate) => candidate.id === continuation.userMessageId,
          );
          assert.equal(`${message?.createdBy}:${message?.creationSource}`, "agent:provider");
          assert.notInclude(yield* Ref.get(sessionStatuses), "stopped");

          const continuationWaiting = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === continuation.id &&
              event.payload.status === "waiting",
          );
          yield* worker.drain();
          yield* orchestrator.resumeQueuedRuns;
          yield* worker.drain();
          yield* Fiber.join(continuationWaiting);
          yield* worker.drain();

          const after = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(
            after.runs.map((run) => run.status),
            ["completed", "completed"],
          );
          assert.lengthOf(
            after.messages.filter(
              (candidate) => candidate.role === "user" && candidate.creationSource === "provider",
            ),
            1,
            "exactly one provider-created continuation",
          );
          assert.include(
            assistantText(after, continuation.id),
            DROID_BACKGROUND_SUBAGENT_WAKE_REPLY,
          );
          const wakeTurn = after.providerTurns.find(
            (turn) => turn.runAttemptId === after.runs[1]?.activeAttemptId,
          );
          assert.equal(wakeTurn?.nativeTurnRef?.nativeId, recordedWakeId(transcript));
          assert.notInclude(yield* Ref.get(sessionStatuses), "stopped");
          assert.notInclude(yield* Ref.get(sessionStatuses), "error");
        }),
      );
    }),
);

it.effect.each(["queue_after_active", "start_immediately"] as const)(
  "[VAL-TURN-022] a user message sent with %s behind an active Droid run is queued and then runs as a Droid turn",
  (mode) =>
    Effect.gen(function* () {
      const transcript = yield* readTranscript("multi_turn");
      yield* withDroidReplay(
        transcript,
        { gate: undefined },
        Effect.fn(function* ({ orchestrator, worker, cwd, watch }) {
          yield* createThread(orchestrator, cwd);
          const rootWaiting = yield* watch(isRunWaiting);
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("first"),
            threadId,
            messageId: MessageId.make("first"),
            text: MULTI_TURN_FIRST_PROMPT,
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* worker.drain(1);
          yield* Fiber.join(rootWaiting);

          const secondId = MessageId.make("second");
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("second"),
            threadId,
            messageId: secondId,
            text: MULTI_TURN_SECOND_PROMPT,
            attachments: [],
            dispatchMode: { type: mode },
            createdBy: "user",
            creationSource: "web",
          });
          const queued = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(
            queued.runs.map((run) => run.status),
            ["waiting", "queued"],
            "the message is accepted as a queued run behind the active one",
          );
          assert.equal(queued.runs[1]?.userMessageId, secondId);

          const secondWaiting = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === queued.runs[1]!.id &&
              event.payload.status === "waiting",
          );
          yield* worker.drain();
          yield* orchestrator.resumeQueuedRuns;
          yield* worker.drain();
          yield* Fiber.join(secondWaiting);
          yield* worker.drain();

          const after = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(
            after.runs.map((run) => run.status),
            ["completed", "completed"],
          );
          assert.include(assistantText(after, after.runs[1]?.id), "second fixture turn complete");
          assert.equal(
            after.providerTurns.filter((turn) => turn.status === "completed").length,
            2,
            "the queued message ran as its own Droid turn",
          );
        }),
      );
    }),
);
