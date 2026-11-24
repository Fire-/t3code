/**
 * Rebuilds a Droid run whose live events never reached its thread, from Droid's own transcript of
 * it. A Droid process can answer a turn on a session the adapter is not listening to; the thread
 * then holds only the tool rows that got through, still running. The transcript has every result,
 * thought, and reply, so they replace the stuck rows and fill the gaps between them.
 */
import {
  EventId,
  ProviderDriverKind,
  ThreadId,
  type OrchestrationV2ContextHandoff,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import {
  droidTranscriptEntries,
  droidTranscriptRecords,
  type DroidTranscriptActivity,
  type DroidTranscriptEntry,
} from "@t3tools/provider-droid/server";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

import * as ContextHandoffService from "../orchestration-v2/ContextHandoffService.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";

const DROID = ProviderDriverKind.make("droid");
const ACTIVE = new Set(["idle", "pending", "running", "waiting"]);
const TOOL_USE_ID = /:tool:([^:]+)$/u;

type ToolFields = Extract<DroidTranscriptActivity, { readonly kind: "tool" }>["item"];

export interface DroidRunRecovery {
  readonly run: OrchestrationV2Run;
  /** Every item of the run, in the order the thread shows them. */
  readonly items: ReadonlyArray<OrchestrationV2TurnItem>;
  /** The items plus each AskUser exchange as messages; for the next turn's handoff, not the thread. */
  readonly handoffItems: ReadonlyArray<OrchestrationV2TurnItem>;
  /** Replies the live turn never showed. */
  readonly messages: ReadonlyArray<OrchestrationV2ConversationMessage>;
  readonly attempts: ReadonlyArray<OrchestrationV2RunAttempt>;
  readonly providerTurns: ReadonlyArray<OrchestrationV2ProviderTurn>;
  readonly nodes: ReadonlyArray<OrchestrationV2ExecutionNode>;
  readonly settled: number;
  readonly added: number;
}

const millis = (value: DateTime.Utc | null | undefined) =>
  value == null ? Number.NEGATIVE_INFINITY : DateTime.toEpochMillis(value);

const parse = (timestamp: string | undefined) =>
  timestamp === undefined ? Option.none() : DateTime.make(timestamp);

type Step =
  | { readonly kind: "settled"; readonly item: OrchestrationV2TurnItem }
  | {
      readonly kind: "added";
      readonly item: OrchestrationV2TurnItem;
      readonly message?: OrchestrationV2ConversationMessage;
    };

/** Handoff history keeps only messages, so each AskUser exchange is retold as a question and an answer. */
function withAskUserExchanges(
  items: ReadonlyArray<OrchestrationV2TurnItem>,
  derive: IdAllocator.IdAllocatorV2Derive,
): ReadonlyArray<OrchestrationV2TurnItem> {
  return items.flatMap((item): ReadonlyArray<OrchestrationV2TurnItem> => {
    if (item.type !== "dynamic_tool" || item.toolName !== "AskUser") return [item];
    const questionnaire = Predicate.isObject(item.input) ? item.input["questionnaire"] : undefined;
    if (typeof questionnaire !== "string") return [item];
    const synthetic = (part: "question" | "response") => {
      const nativeItemId = `${item.nativeItemRef?.nativeId ?? item.id}:ask-user:${part}`;
      return {
        id: derive.turnItemFromProviderItem({ driver: DROID, nativeItemId }),
        messageId: derive.messageFromProviderItem({ driver: DROID, nativeItemId }),
        threadId: item.threadId,
        runId: item.runId,
        nodeId: item.nodeId,
        providerThreadId: item.providerThreadId,
        providerTurnId: item.providerTurnId,
        nativeItemRef: { driver: DROID, nativeId: nativeItemId, strength: "weak" as const },
        parentItemId: null,
        ordinal: item.ordinal,
        title: null,
        startedAt: item.startedAt,
        completedAt: item.completedAt,
        updatedAt: item.updatedAt,
      };
    };
    const question: OrchestrationV2TurnItem = {
      ...synthetic("question"),
      type: "assistant_message",
      status: item.status,
      text: `<ask_user>\n${questionnaire}\n</ask_user>`,
      streaming: false,
    };
    if (typeof item.output !== "string" || item.output.trim() === "") return [item, question];
    return [
      item,
      question,
      {
        ...synthetic("response"),
        type: "user_message",
        status: "completed",
        createdBy: "user",
        creationSource: "provider",
        inputIntent: "steer",
        text: `<ask_user_response>\n${item.output}\n</ask_user_response>`,
        attachments: [],
      },
    ];
  });
}

/** * The run as the transcript tells it. Tool rows the live turn showed keep their ids and place;
 * each one settles with its result, or as interrupted if it never got one. Thinking, replies,
 * notices, and calls the live turn never showed go between them in transcript order. A Script's
 * own calls stay out, as they do live. User messages are already on the thread.
 */
export function droidRunRecovery(input: {
  readonly run: OrchestrationV2Run;
  readonly projection: Pick<
    OrchestrationV2ThreadProjection,
    "attempts" | "nodes" | "providerTurns" | "turnItems" | "messages"
  >;
  readonly entries: ReadonlyArray<DroidTranscriptEntry>;
  readonly derive: IdAllocator.IdAllocatorV2Derive;
}): DroidRunRecovery {
  const { run, projection, derive } = input;
  const requestedAt = DateTime.toEpochMillis(run.requestedAt);
  const entries = input.entries.filter((entry) => {
    const time = parse(entry.type === "message" ? entry.timestamp : entry.startedAt);
    return Option.isNone(time) || DateTime.toEpochMillis(time.value) >= requestedAt;
  });
  const endedAt = entries
    .flatMap((entry) =>
      (entry.type === "message" ? [entry.timestamp] : [entry.startedAt, entry.completedAt]).flatMap(
        (timestamp) => Option.toArray(parse(timestamp)),
      ),
    )
    .reduce(
      (latest, time) => (millis(time) > millis(latest) ? time : latest),
      run.startedAt ?? run.requestedAt,
    );
  const attempts = projection.attempts.filter((attempt) => attempt.runId === run.id);
  const attemptIds = new Set(attempts.map((attempt) => attempt.id));
  const providerTurns = projection.providerTurns
    .filter((turn) => turn.runAttemptId !== null && attemptIds.has(turn.runAttemptId))
    .toSorted((left, right) => millis(left.startedAt) - millis(right.startedAt));
  const providerTurnAt = (time: DateTime.Utc) =>
    providerTurns.findLast((turn) => millis(turn.startedAt) <= millis(time)) ?? providerTurns[0];
  const runItems = projection.turnItems
    .filter((item) => item.runId === run.id)
    .toSorted((left, right) => left.ordinal - right.ordinal);
  const liveTools = new Map(
    runItems.flatMap((item) => {
      const toolUseId = TOOL_USE_ID.exec(item.nativeItemRef?.nativeId ?? "")?.[1];
      return toolUseId === undefined ? [] : [[toolUseId, item] as const];
    }),
  );

  const added = (
    nativeItemId: string,
    strength: "strong" | "weak",
    startedAt: DateTime.Utc,
    completedAt: DateTime.Utc,
  ) => {
    const providerTurn = providerTurnAt(startedAt);
    return {
      id: derive.turnItemFromProviderItem({ driver: DROID, nativeItemId }),
      threadId: run.threadId,
      runId: run.id,
      nodeId: run.rootNodeId,
      providerThreadId: run.providerThreadId,
      providerTurnId: providerTurn?.id ?? null,
      nativeItemRef: { driver: DROID, nativeId: nativeItemId, strength },
      parentItemId: null,
      ordinal: 0,
      startedAt,
      completedAt,
      updatedAt: completedAt,
    };
  };
  const turnPrefix = (time: DateTime.Utc) => providerTurnAt(time)?.id ?? run.id;
  const settle = (
    item: OrchestrationV2TurnItem,
    fields: ToolFields | Pick<OrchestrationV2TurnItem, "status">,
    completedAt: DateTime.Utc,
  ) =>
    ({
      ...item,
      ...fields,
      completedAt,
      updatedAt: completedAt,
    }) as OrchestrationV2TurnItem;

  const steps = entries.flatMap((entry, index): ReadonlyArray<Step> => {
    if (entry.type === "message") {
      if (entry.role === "user") return [];
      const time = Option.getOrElse(parse(entry.timestamp), () => endedAt);
      const nativeItemId = `${turnPrefix(time)}:recovered:${index}`;
      const messageId = derive.messageFromProviderItem({ driver: DROID, nativeItemId });
      const base = added(nativeItemId, "weak", time, time);
      return [
        {
          kind: "added",
          item: {
            ...base,
            status: "completed",
            title: null,
            type: "assistant_message",
            messageId,
            text: entry.text,
            streaming: false,
          },
          message: {
            id: messageId,
            threadId: run.threadId,
            runId: run.id,
            nodeId: run.rootNodeId,
            role: "assistant",
            text: entry.text,
            attachments: [],
            streaming: false,
            createdBy: "agent",
            creationSource: "provider",
            createdAt: time,
            updatedAt: time,
          },
        },
      ];
    }
    if (entry.scriptToolUseId !== undefined) return [];
    const startedAt = Option.getOrElse(parse(entry.startedAt), () => endedAt);
    const { activity } = entry;
    const settledAt =
      (activity.kind === "tool"
        ? activity.item.status
        : activity.kind === "subagent"
          ? activity.status
          : "completed") === "interrupted"
        ? endedAt
        : Option.getOrElse(parse(entry.completedAt), () => startedAt);
    const live = entry.toolUseId === undefined ? undefined : liveTools.get(entry.toolUseId);
    if (live !== undefined && (activity.kind === "tool" || activity.kind === "subagent")) {
      return [
        {
          kind: "settled",
          item: settle(
            live,
            activity.kind === "tool" ? activity.item : { status: activity.status },
            settledAt,
          ),
        },
      ];
    }
    const nativeItemId =
      entry.toolUseId === undefined
        ? `${turnPrefix(startedAt)}:recovered:${index}`
        : `${turnPrefix(startedAt)}:tool:${entry.toolUseId}`;
    const base = added(
      nativeItemId,
      entry.toolUseId === undefined ? "weak" : "strong",
      startedAt,
      settledAt,
    );
    const item: OrchestrationV2TurnItem =
      activity.kind === "tool"
        ? { ...base, ...activity.item }
        : activity.kind === "subagent"
          ? {
              ...base,
              type: "subagent",
              status: activity.status,
              title: activity.title,
              subagentId: derive.nodeFromProviderItem({ driver: DROID, nativeItemId }),
              origin: "provider_native",
              driver: DROID,
              providerInstanceId: run.providerInstanceId,
              childThreadId: null,
              prompt: activity.prompt,
              result: activity.result,
            }
          : activity.kind === "reasoning"
            ? {
                ...base,
                type: "reasoning",
                status: "completed",
                title: null,
                text: activity.text,
                streaming: false,
              }
            : {
                ...base,
                type: "system_notice",
                status: "completed",
                title: activity.message,
                message: activity.message,
              };
    return [{ kind: "added", item }];
  });

  // Live rows keep their order. Added items go before the next row the transcript settles, and
  // around the rows it does not know (user messages, todo lists, questions) by time.
  const settledById = new Map(
    steps.flatMap((step) => (step.kind === "settled" ? [[step.item.id, step.item] as const] : [])),
  );
  const leftover = (item: OrchestrationV2TurnItem) =>
    settledById.get(item.id) ??
    (ACTIVE.has(item.status) ? settle(item, { status: "interrupted" }, endedAt) : item);
  const ordered: Array<OrchestrationV2TurnItem> = [];
  const placed = new Set<string>();
  const place = (item: OrchestrationV2TurnItem) => {
    placed.add(item.id);
    ordered.push(item);
  };
  let liveIndex = 0;
  let stepIndex = 0;
  while (liveIndex < runItems.length || stepIndex < steps.length) {
    const live = runItems[liveIndex];
    const step = steps[stepIndex];
    if (live !== undefined && placed.has(live.id)) {
      liveIndex += 1;
    } else if (step === undefined) {
      place(leftover(live!));
      liveIndex += 1;
    } else if (step.kind === "settled") {
      if (placed.has(step.item.id)) {
        stepIndex += 1;
      } else if (live === undefined) {
        place(step.item);
        stepIndex += 1;
      } else {
        place(leftover(live));
        liveIndex += 1;
        if (live.id === step.item.id) stepIndex += 1;
      }
    } else if (
      live !== undefined &&
      !settledById.has(live.id) &&
      millis(live.startedAt) <= millis(step.item.startedAt)
    ) {
      place(leftover(live));
      liveIndex += 1;
    } else {
      place(step.item);
      stepIndex += 1;
    }
  }
  const firstOrdinal = Math.max(run.ordinal * 1_000_000, ...runItems.map((item) => item.ordinal));
  const items = ordered.map((item, index) => ({ ...item, ordinal: firstOrdinal + index + 1 }));

  const itemsByNode = new Map(
    items.flatMap((item) =>
      item.nodeId === null || item.nodeId === run.rootNodeId ? [] : [[item.nodeId, item] as const],
    ),
  );
  const nodes = projection.nodes
    .filter((node) => node.runId === run.id && ACTIVE.has(node.status))
    .map((node): OrchestrationV2ExecutionNode => {
      const item = itemsByNode.get(node.id);
      return item === undefined || ACTIVE.has(item.status)
        ? { ...node, status: "interrupted", completedAt: endedAt }
        : { ...node, status: item.status, completedAt: item.completedAt ?? endedAt };
    });

  return {
    run: { ...run, status: "interrupted", queuePosition: null, completedAt: endedAt },
    items,
    handoffItems: withAskUserExchanges(items, derive),
    messages: [
      ...steps.flatMap((step) =>
        step.kind === "added" && step.message !== undefined ? [step.message] : [],
      ),
      ...projection.messages
        .filter((message) => message.runId === run.id && message.streaming)
        .map((message) => ({ ...message, streaming: false, updatedAt: endedAt })),
    ],
    attempts: attempts
      .filter((attempt) => attempt.status === "pending" || attempt.status === "running")
      .map((attempt) => ({ ...attempt, status: "interrupted", completedAt: endedAt })),
    providerTurns: providerTurns
      .filter((turn) => turn.status === "pending" || turn.status === "running")
      .map((turn) => ({ ...turn, status: "interrupted", completedAt: endedAt })),
    nodes,
    settled: settledById.size,
    added: steps.filter((step) => step.kind === "added").length,
  };
}

export class DroidRunRecoveryError extends Schema.TaggedError<DroidRunRecoveryError>()(
  "DroidRunRecoveryError",
  { detail: Schema.String },
) {
  override get message(): string {
    return this.detail;
  }
}

/**
 * Writes the recovery of one run, with a context handoff so the run's own session hears what it
 * missed on the thread's next turn. Refuses a run that finished or was already recovered.
 */
export const recoverDroidRun = Effect.fn("DroidRunRecovery.recover")(function* (input: {
  readonly threadId: ThreadId;
  readonly runOrdinal: number;
  readonly transcriptPath: string;
  readonly dryRun: boolean;
}) {
  const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
  const eventSink = yield* EventSink.EventSinkV2;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const contextHandoffs = yield* ContextHandoffService.ContextHandoffServiceV2;
  const fileSystem = yield* FileSystem.FileSystem;

  const projection = yield* projectionStore.getThreadProjection(input.threadId);
  const run = projection.runs.find((candidate) => candidate.ordinal === input.runOrdinal);
  if (run === undefined) {
    return yield* new DroidRunRecoveryError({
      detail: `Thread ${input.threadId} has no run ${input.runOrdinal}.`,
    });
  }
  if (run.providerThreadId === null || run.status === "completed" || run.status === "interrupted") {
    return yield* new DroidRunRecoveryError({
      detail: `Run ${input.runOrdinal} is ${run.status}; only an unfinished or cancelled run can be recovered.`,
    });
  }
  const records = droidTranscriptRecords(yield* fileSystem.readFileString(input.transcriptPath));
  const now = yield* DateTime.now;
  const recovery = droidRunRecovery({
    run,
    projection,
    entries: droidTranscriptEntries(records),
    derive: idAllocator.derive,
  });
  const handoff: OrchestrationV2ContextHandoff = yield* contextHandoffs.prepareProviderHandoff({
    threadId: input.threadId,
    targetRunId: run.id,
    transferId: null,
    fromProviderThreadIds: [run.providerThreadId],
    toProviderThreadId: run.providerThreadId,
    fromProviderInstanceId: run.providerInstanceId,
    toProviderInstanceId: run.providerInstanceId,
    coveredRunOrdinals: { from: run.ordinal, to: run.ordinal },
    strategy: "delta_since_target_last_seen",
    runs: projection.runs.map((candidate) => (candidate.id === run.id ? recovery.run : candidate)),
    items: recovery.handoffItems,
    createdAt: now,
  });

  let sequence = 0;
  const common = (nodeId?: OrchestrationV2ExecutionNode["id"] | null) => ({
    id: EventId.make(`droid-run-recovery:${run.id}:${String(sequence++).padStart(6, "0")}`),
    threadId: input.threadId,
    runId: run.id,
    ...(nodeId == null ? {} : { nodeId }),
    providerInstanceId: run.providerInstanceId,
    occurredAt: now,
  });
  const events: ReadonlyArray<OrchestrationV2DomainEvent> = [
    { ...common(), type: "run.updated", payload: recovery.run },
    ...recovery.attempts.map((payload): OrchestrationV2DomainEvent => ({
      ...common(payload.rootNodeId),
      type: "run-attempt.updated",
      payload,
    })),
    ...recovery.providerTurns.map((payload): OrchestrationV2DomainEvent => ({
      ...common(payload.nodeId),
      type: "provider-turn.updated",
      payload,
    })),
    ...recovery.nodes.map((payload): OrchestrationV2DomainEvent => ({
      ...common(payload.id),
      type: "node.updated",
      payload,
    })),
    ...recovery.messages.map((payload): OrchestrationV2DomainEvent => ({
      ...common(payload.nodeId),
      type: "message.updated",
      payload,
    })),
    ...recovery.items.map((payload): OrchestrationV2DomainEvent => ({
      ...common(payload.nodeId),
      type: "turn-item.updated",
      payload,
    })),
    { ...common(), type: "context-handoff.updated", payload: handoff },
  ];
  if (!input.dryRun) yield* eventSink.write({ repositionTurnItems: true, events });
  return {
    runId: run.id,
    statusBefore: run.status,
    items: recovery.items.length,
    settled: recovery.settled,
    added: recovery.added,
    interrupted: recovery.items.filter((item) => item.status === "interrupted").length,
    handoffMessages: handoff.history?.messages.length ?? 0,
    handoffOmittedItems: handoff.history?.omittedItems ?? 0,
    events: events.length,
    written: !input.dryRun,
  };
});
