/**
 * Droid `Task` subagents to v2 subagent events. Pure, like `events.ts`: each call takes the
 * subagent's record and returns the next record plus the events to emit. The adapter owns the
 * records, the clock, and the roster of pending background tasks.
 *
 * A record freezes the ids it launched under (run, provider turn, nodes), so a background
 * subagent can still settle after the turn that launched it has ended.
 *
 * A child session gets a read-only child thread. Droid does not stream a child's events, so the
 * thread holds the Task prompt while the child runs and its transcript once it has ended.
 */
import type {
  ModelSelection,
  NodeId,
  OrchestrationV2AppThread,
  OrchestrationV2PendingBackgroundTask,
  OrchestrationV2ProviderRef,
  OrchestrationV2Subagent,
  OrchestrationV2TurnItem,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import type { ProviderAdapterV2Event } from "@t3tools/provider-core/server/ProviderAdapter";
import {
  makeSubagentChildThread,
  makeSubagentConversationArtifacts,
  subagentThreadTitle,
} from "@t3tools/provider-core/server/subagentProjection";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";

import { DROID_DRIVER, type DroidEventContext, type DroidNotification } from "./events.ts";
import {
  boundDroidToolOutput,
  droidTaskResultReport,
  droidToolKind,
  isDroidBackgroundTaskLaunch,
  type DroidTaskCompletion,
} from "./protocol.ts";
import type { DroidTranscriptEntry } from "./sessionTranscripts.ts";

type DroidSubagentSettleStatus = "completed" | "failed" | "cancelled" | "interrupted";

export interface DroidSubagentOutcome {
  readonly status: DroidSubagentSettleStatus;
  readonly result: string | null;
}

/** How a background Task ends when Droid reports its completion. */
export const droidTaskCompletionOutcome = (
  completion: DroidTaskCompletion,
): DroidSubagentOutcome => ({
  status:
    completion.status === "completed"
      ? "completed"
      : completion.status === "stopped"
        ? "cancelled"
        : "failed",
  result: completion.summary === undefined ? null : boundDroidToolOutput(completion.summary),
});

interface ChildThreadSurface {
  readonly sessionId: string;
  readonly threadId: ThreadId;
  readonly rootNodeId: NodeId;
}

interface SubagentSurface {
  readonly nodeId: NodeId;
  readonly turnItemId: TurnItemId;
  readonly threadId: ThreadId;
  readonly runId: RunId | null;
  readonly rootNodeId: NodeId;
  readonly providerInstanceId: ProviderInstanceId;
  readonly providerThreadId: ProviderThreadId;
  readonly ordinal: number;
  readonly startedAt: DateTime.Utc;
  /** The child session; null for a Task that ended before Droid started one. */
  readonly nativeTaskRef: OrchestrationV2ProviderRef | null;
  /** The ref the roster's task id is read from, so the roster and the item name one task. */
  readonly nativeItemRef: OrchestrationV2ProviderRef;
  /** Null exactly when `nativeTaskRef` is: no child session, no child thread. */
  readonly child: ChildThreadSurface | null;
}

export interface DroidSubagent {
  readonly toolUseId: string;
  readonly providerTurnId: ProviderTurnId;
  /**
   * The Task call returns at once and the report arrives as a message later. Set by `await:
   * false`, or by Droid's launch reply: a call without `await` runs wherever Droid's default puts
   * it, and that default has changed between versions.
   */
  readonly background: boolean;
  readonly prompt: string;
  readonly title: string | null;
  readonly childSessionId: string | undefined;
  /** `pending` from the Task call until the child session starts or the call ends without one. */
  readonly status: OrchestrationV2Subagent["status"];
  /** Set once the subagent has been announced. */
  readonly surface: SubagentSurface | undefined;
  /** The report it settled with. */
  readonly result: string | null;
  /**
   * For a launch whose child session another subagent was still running: that subagent's Task
   * call. Droid drops such a launch's prompt, and an awaited one returns the running subagent's
   * outcome instead.
   */
  readonly heldBy: string | undefined;
}

/** What a child thread copies from the thread whose turn launched the subagent. */
export interface DroidSubagentParent {
  readonly appThread: OrchestrationV2AppThread;
  readonly modelSelection: ModelSelection;
  /** Which subagent of the thread this is; names a child thread that has no title or prompt. */
  readonly number: number;
}

// Ordinals on a child thread: the prompt, then its transcript.
const CHILD_PROMPT_ORDINAL = 100;

export interface DroidSubagentUpdate {
  readonly subagent: DroidSubagent;
  readonly events: ReadonlyArray<ProviderAdapterV2Event>;
  /** The update announced the subagent, so its item ordinal is taken. */
  readonly surfaced: boolean;
}

const asTrimmed = (value: unknown): string | undefined => {
  if (!Predicate.isString(value)) return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
};

export const isSettledDroidSubagent = (subagent: DroidSubagent): boolean =>
  subagent.status === "completed" ||
  subagent.status === "failed" ||
  subagent.status === "cancelled" ||
  subagent.status === "interrupted";

export function droidTaskCall(
  notification: DroidNotification,
): { readonly toolUseId: string; readonly input: Readonly<Record<string, unknown>> } | undefined {
  const toolUse = notification["toolUse"];
  if (!Predicate.isObject(toolUse)) return undefined;
  const name = asTrimmed(toolUse["name"]);
  const toolUseId = asTrimmed(toolUse["id"]);
  if (name === undefined || toolUseId === undefined || droidToolKind(name) !== "task") {
    return undefined;
  }
  const input = toolUse["input"];
  return { toolUseId, input: Predicate.isObject(input) ? input : {} };
}

/**
 * Droid sends a Task call twice: first with an empty input, then with the whole one. Once the
 * child has started, or the Task has ended, the input is final and later frames change nothing.
 */
export function noteDroidTaskCall(
  known: DroidSubagent | undefined,
  ctx: DroidEventContext,
  call: { readonly toolUseId: string; readonly input: Readonly<Record<string, unknown>> },
): DroidSubagent {
  if (known !== undefined && known.status !== "pending") return known;
  const { input } = call;
  return {
    toolUseId: call.toolUseId,
    providerTurnId: known?.providerTurnId ?? ctx.providerTurn.id,
    background: Predicate.isBoolean(input["await"])
      ? !input["await"]
      : (known?.background ?? false),
    prompt: asTrimmed(input["prompt"]) ?? known?.prompt ?? "",
    title: asTrimmed(input["description"]) ?? known?.title ?? null,
    childSessionId: undefined,
    status: "pending",
    surface: undefined,
    result: null,
    heldBy: undefined,
  };
}

function childRootNodeEvent(
  surface: SubagentSurface,
  child: ChildThreadSurface,
  status: OrchestrationV2Subagent["status"],
  completedAt: DateTime.Utc | null,
): ProviderAdapterV2Event {
  return {
    type: "node.updated",
    driver: DROID_DRIVER,
    node: {
      id: child.rootNodeId,
      threadId: child.threadId,
      runId: null,
      parentNodeId: null,
      rootNodeId: child.rootNodeId,
      kind: "root_turn",
      status,
      countsForRun: false,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: surface.nativeTaskRef,
      runtimeRequestId: null,
      checkpointScopeId: null,
      startedAt: surface.startedAt,
      completedAt,
    },
  };
}

function childConversationEvents(
  surface: SubagentSurface,
  child: ChildThreadSurface,
  input: {
    readonly nativeItemId: string;
    readonly role: "user" | "assistant";
    readonly text: string;
    readonly ordinal: number;
    readonly at: DateTime.Utc;
    /** Only the Task prompt comes from the parent thread. */
    readonly fromParent?: boolean;
  },
  derive: DroidEventContext["derive"],
): ReadonlyArray<ProviderAdapterV2Event> {
  const { message, turnItem } = makeSubagentConversationArtifacts({
    messageId: derive.messageFromProviderItem({
      driver: DROID_DRIVER,
      nativeItemId: input.nativeItemId,
    }),
    ...(input.fromParent === true ? { senderThreadId: surface.threadId } : {}),
    turnItemId: derive.turnItemFromProviderItem({
      driver: DROID_DRIVER,
      nativeItemId: input.nativeItemId,
    }),
    threadId: child.threadId,
    rootNodeId: child.rootNodeId,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: { driver: DROID_DRIVER, nativeId: input.nativeItemId, strength: "weak" },
    role: input.role,
    text: input.text,
    ordinal: input.ordinal,
    now: input.at,
  });
  return [
    { type: "message.updated", driver: DROID_DRIVER, message },
    { type: "turn_item.updated", driver: DROID_DRIVER, turnItem },
  ];
}

function childThreadOpened(
  subagent: DroidSubagent,
  surface: SubagentSurface,
  child: ChildThreadSurface,
  parent: DroidSubagentParent,
  derive: DroidEventContext["derive"],
  now: DateTime.Utc,
): ReadonlyArray<ProviderAdapterV2Event> {
  return [
    {
      type: "app_thread.created",
      driver: DROID_DRIVER,
      // A Task `resume` launches the same child session again; its thread keeps its first title.
      createOnly: true,
      appThread: makeSubagentChildThread({
        parentThread: parent.appThread,
        childThreadId: child.threadId,
        parentNodeId: surface.nodeId,
        // Droid keys its process by session, so the child never gets a provider thread of its own.
        activeProviderThreadId: null,
        providerInstanceId: surface.providerInstanceId,
        modelSelection: parent.modelSelection,
        title: subagentThreadTitle({
          parentTitle: parent.appThread.title,
          title: subagent.title,
          prompt: subagent.prompt,
          ordinal: parent.number,
        }),
        now,
        createdBy: "agent",
        creationSource: "provider",
      }),
    },
    childRootNodeEvent(surface, child, "running", null),
    ...(subagent.prompt === ""
      ? []
      : childConversationEvents(
          surface,
          child,
          {
            nativeItemId: `${child.sessionId}:prompt:${subagent.toolUseId}`,
            role: "user",
            text: subagent.prompt,
            ordinal: CHILD_PROMPT_ORDINAL,
            at: now,
            fromParent: true,
          },
          derive,
        )),
  ];
}

function subagentEvents(
  subagent: DroidSubagent,
  surface: SubagentSurface,
  status: OrchestrationV2Subagent["status"],
  result: string | null,
  now: DateTime.Utc,
): ReadonlyArray<ProviderAdapterV2Event> {
  const settled = status !== "running";
  const completedAt = settled ? now : null;
  return [
    // A background subagent's run stops listening once the subagent's own item settles, so its
    // child thread ends first.
    ...(settled && surface.child !== null && subagent.heldBy === undefined
      ? [childRootNodeEvent(surface, surface.child, status, completedAt)]
      : []),
    {
      type: "subagent.updated",
      driver: DROID_DRIVER,
      subagent: {
        id: surface.nodeId,
        threadId: surface.threadId,
        runId: surface.runId,
        parentNodeId: surface.rootNodeId,
        origin: "provider_native",
        createdBy: "agent",
        driver: DROID_DRIVER,
        providerInstanceId: surface.providerInstanceId,
        providerThreadId: surface.providerThreadId,
        childThreadId: surface.child?.threadId ?? null,
        nativeTaskRef: surface.nativeTaskRef,
        prompt: subagent.prompt,
        title: subagent.title,
        model: null,
        status,
        result,
        startedAt: surface.startedAt,
        completedAt,
        updatedAt: now,
      },
    },
    {
      type: "node.updated",
      driver: DROID_DRIVER,
      node: {
        id: surface.nodeId,
        threadId: surface.threadId,
        runId: surface.runId,
        parentNodeId: surface.rootNodeId,
        rootNodeId: surface.rootNodeId,
        kind: "subagent",
        status,
        countsForRun: false,
        providerThreadId: surface.providerThreadId,
        providerTurnId: subagent.providerTurnId,
        nativeItemRef: surface.nativeItemRef,
        runtimeRequestId: null,
        checkpointScopeId: null,
        startedAt: surface.startedAt,
        completedAt,
      },
    },
    {
      type: "turn_item.updated",
      driver: DROID_DRIVER,
      turnItem: {
        id: surface.turnItemId,
        threadId: surface.threadId,
        runId: surface.runId,
        nodeId: surface.nodeId,
        providerThreadId: surface.providerThreadId,
        providerTurnId: subagent.providerTurnId,
        nativeItemRef: surface.nativeItemRef,
        parentItemId: null,
        ordinal: surface.ordinal,
        status,
        title: subagent.title,
        startedAt: surface.startedAt,
        completedAt,
        updatedAt: now,
        type: "subagent",
        subagentId: surface.nodeId,
        origin: "provider_native",
        driver: DROID_DRIVER,
        providerInstanceId: surface.providerInstanceId,
        childThreadId: surface.child?.threadId ?? null,
        prompt: subagent.prompt,
        result,
      },
    },
  ];
}

function announce(
  known: DroidSubagent,
  ctx: DroidEventContext,
  launch: { readonly childSessionId: string; readonly parent: DroidSubagentParent } | undefined,
  ordinal: number,
  now: DateTime.Utc,
): DroidSubagentUpdate {
  const nativeItemId = `${known.providerTurnId}:subagent:${known.toolUseId}`;
  const childSessionId = launch?.childSessionId;
  const nativeTaskRef: OrchestrationV2ProviderRef | null =
    childSessionId === undefined
      ? null
      : { driver: DROID_DRIVER, nativeId: childSessionId, strength: "strong" };
  const child: ChildThreadSurface | null =
    childSessionId === undefined
      ? null
      : {
          sessionId: childSessionId,
          threadId: ctx.derive.threadFromProviderThread({
            driver: DROID_DRIVER,
            nativeThreadId: childSessionId,
          }),
          rootNodeId: ctx.derive.nodeFromProviderItem({
            driver: DROID_DRIVER,
            nativeItemId: `${childSessionId}:root`,
          }),
        };
  const surface: SubagentSurface = {
    nodeId: ctx.derive.nodeFromProviderItem({ driver: DROID_DRIVER, nativeItemId }),
    turnItemId: ctx.derive.turnItemFromProviderItem({ driver: DROID_DRIVER, nativeItemId }),
    threadId: ctx.threadId,
    runId: ctx.runId,
    rootNodeId: ctx.rootNodeId,
    providerInstanceId: ctx.providerThread.providerInstanceId,
    providerThreadId: ctx.providerThread.id,
    ordinal,
    startedAt: now,
    nativeTaskRef,
    nativeItemRef: nativeTaskRef ?? {
      driver: DROID_DRIVER,
      nativeId: known.toolUseId,
      strength: "strong",
    },
    child,
  };
  const subagent: DroidSubagent = { ...known, childSessionId, status: "running", surface };
  return {
    subagent,
    events: [
      ...(child === null || launch === undefined || known.heldBy !== undefined
        ? []
        : childThreadOpened(subagent, surface, child, launch.parent, ctx.derive, now)),
      ...subagentEvents(subagent, surface, "running", null, now),
    ],
    surfaced: true,
  };
}

const DROPPED_FOLLOW_UP = "Droid dropped this follow-up because the subagent was still running.";

/**
 * `child_session_available` starts the subagent. Droid has named the child by now, which is why
 * the subagent starts here and not at the Task call. Returns nothing for a duplicate or a late
 * frame of a subagent that has started or ended: those never reopen it.
 *
 * A Task `resume` of a child another subagent is still running gets a child session too, but
 * Droid never delivers its prompt: the child goes on with what it was doing. That launch ends
 * failed at once and leaves the child thread to the subagent running it.
 */
export function launchDroidSubagent(
  known: DroidSubagent | undefined,
  ctx: DroidEventContext,
  notification: DroidNotification,
  ordinal: number,
  now: DateTime.Utc,
  parent: DroidSubagentParent,
  subagents: Iterable<DroidSubagent>,
): DroidSubagentUpdate | undefined {
  const toolUseId = asTrimmed(notification["toolUseId"]);
  const childSessionId = asTrimmed(notification["childSessionId"]);
  if (toolUseId === undefined || childSessionId === undefined) return undefined;
  if (known !== undefined && known.status !== "pending") return undefined;
  const description = asTrimmed(notification["description"]);
  const call: DroidSubagent = known ?? {
    toolUseId,
    providerTurnId: ctx.providerTurn.id,
    background: false,
    prompt: description ?? "",
    title: description ?? null,
    childSessionId: undefined,
    status: "pending",
    surface: undefined,
    result: null,
    heldBy: undefined,
  };
  const holder = [...subagents].find(
    (subagent) =>
      subagent.toolUseId !== toolUseId &&
      subagent.status === "running" &&
      subagent.childSessionId === childSessionId,
  );
  const announced = announce(
    { ...call, heldBy: holder?.toolUseId },
    ctx,
    { childSessionId, parent },
    ordinal,
    now,
  );
  if (holder === undefined) return announced;
  const dropped = settleDroidSubagent(
    announced.subagent,
    { status: "failed", result: DROPPED_FOLLOW_UP },
    now,
  );
  if (dropped === undefined) return announced;
  return { ...dropped, events: [...announced.events, ...dropped.events], surfaced: true };
}

export function settleDroidSubagent(
  subagent: DroidSubagent,
  outcome: DroidSubagentOutcome,
  now: DateTime.Utc,
): DroidSubagentUpdate | undefined {
  const { surface } = subagent;
  if (surface === undefined || isSettledDroidSubagent(subagent)) return undefined;
  return {
    subagent: { ...subagent, status: outcome.status, result: outcome.result },
    events: subagentEvents(subagent, surface, outcome.status, outcome.result, now),
    surfaced: false,
  };
}

interface DroidTaskStop {
  readonly interrupted: boolean;
  readonly declined: boolean;
}

/**
 * A background launch's result only acknowledges it, and marks the subagent as one. Returns
 * "awaited" for a result that ends the Task instead.
 */
function noteDroidBackgroundLaunch(
  known: DroidSubagent,
  notification: DroidNotification,
): DroidSubagentUpdate | "awaited" | undefined {
  if (notification["isError"] === true) return "awaited";
  if (!known.background && !isDroidBackgroundTaskLaunch(notification["content"])) return "awaited";
  return known.background
    ? undefined
    : { subagent: { ...known, background: true }, events: [], surfaced: false };
}

const droidTaskResultOutcome = (
  notification: DroidNotification,
  stop: DroidTaskStop,
): DroidSubagentOutcome => ({
  status:
    notification["isError"] !== true
      ? "completed"
      : stop.interrupted
        ? "interrupted"
        : stop.declined
          ? "cancelled"
          : "failed",
  result: droidTaskResultReport(notification["content"]) ?? null,
});

/**
 * The `tool_result` of a Task. An awaited Task's result is the report and ends the subagent. A
 * background Task's result only acknowledges the launch, unless it is an error: then the launch
 * failed and no report will come. A result that arrives before any child session started still
 * announces the subagent, so a Task that failed to launch is not lost.
 */
export function settleDroidTaskResult(
  known: DroidSubagent | undefined,
  ctx: DroidEventContext,
  notification: DroidNotification,
  stop: DroidTaskStop,
  ordinal: number,
  now: DateTime.Utc,
): DroidSubagentUpdate | undefined {
  if (known === undefined || isSettledDroidSubagent(known)) return undefined;
  const launch = noteDroidBackgroundLaunch(known, notification);
  if (launch !== "awaited") return launch;
  const announced =
    known.surface === undefined
      ? announce(known, ctx, undefined, ordinal, now)
      : { subagent: known, events: [], surfaced: false };
  const settled = settleDroidSubagent(
    announced.subagent,
    droidTaskResultOutcome(notification, stop),
    now,
  );
  if (settled === undefined) return undefined;
  return {
    subagent: settled.subagent,
    events: [...announced.events, ...settled.events],
    surfaced: announced.surfaced,
  };
}

/**
 * The `tool_result` of a launch Droid dropped because its child was busy. An awaited one waited
 * for the subagent running the child and carries that subagent's outcome, so it ends that one.
 */
export function settleDroppedDroidTaskResult(
  dropped: DroidSubagent,
  holder: DroidSubagent | undefined,
  notification: DroidNotification,
  stop: DroidTaskStop,
  now: DateTime.Utc,
): DroidSubagentUpdate | undefined {
  const launch = noteDroidBackgroundLaunch(dropped, notification);
  if (launch !== "awaited") return launch;
  if (holder === undefined) return undefined;
  return settleDroidSubagent(holder, droidTaskResultOutcome(notification, stop), now);
}

/**
 * A child thread's history once its subagent has ended, from the child session's transcript, which
 * holds every launch of that session so far. When the transcript holds no reply (it could not be
 * read, or the child never answered), the report stands in.
 */
export function droidChildThreadHistory(
  subagent: DroidSubagent,
  entries: ReadonlyArray<DroidTranscriptEntry>,
  derive: DroidEventContext["derive"],
  now: DateTime.Utc,
): ReadonlyArray<ProviderAdapterV2Event> {
  const surface = subagent.surface;
  const child = surface?.child ?? null;
  if (surface === undefined || child === null) return [];
  const at = (timestamp: string | undefined) =>
    Option.getOrElse(DateTime.make(timestamp ?? Number.NaN), () => now);
  // Every user message in a child session is a Task prompt: the first launch's, or a resume's
  // follow-up in Droid's wrapping. Each launch already put its own on the thread.
  const history = entries.filter((entry) => !(entry.type === "message" && entry.role === "user"));
  const events = history.flatMap((entry, index): ReadonlyArray<ProviderAdapterV2Event> => {
    const nativeItemId = `${child.sessionId}:history:${index}`;
    const ordinal = CHILD_PROMPT_ORDINAL + 1 + index;
    if (entry.type === "message") {
      return childConversationEvents(
        surface,
        child,
        { nativeItemId, role: entry.role, text: entry.text, ordinal, at: at(entry.timestamp) },
        derive,
      );
    }
    const completedAt = at(entry.completedAt);
    const base = {
      id: derive.turnItemFromProviderItem({ driver: DROID_DRIVER, nativeItemId }),
      threadId: child.threadId,
      runId: null,
      nodeId: child.rootNodeId,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: { driver: DROID_DRIVER, nativeId: nativeItemId, strength: "weak" as const },
      parentItemId: null,
      ordinal,
      startedAt: at(entry.startedAt),
      completedAt,
      updatedAt: completedAt,
    };
    const { activity } = entry;
    const turnItem: OrchestrationV2TurnItem =
      activity.kind === "tool"
        ? { ...base, ...activity.item }
        : activity.kind === "subagent"
          ? {
              ...base,
              type: "subagent",
              status: activity.status,
              title: activity.title,
              subagentId: derive.nodeFromProviderItem({ driver: DROID_DRIVER, nativeItemId }),
              origin: "provider_native",
              driver: DROID_DRIVER,
              providerInstanceId: surface.providerInstanceId,
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
    return [{ type: "turn_item.updated", driver: DROID_DRIVER, turnItem }];
  });
  const replied = history.some((entry) => entry.type === "message" && entry.role === "assistant");
  if (replied || subagent.result === null || subagent.result === "") return events;
  return [
    ...events,
    ...childConversationEvents(
      surface,
      child,
      {
        nativeItemId: `${child.sessionId}:report:${subagent.toolUseId}`,
        role: "assistant",
        text: subagent.result,
        ordinal: CHILD_PROMPT_ORDINAL + 1 + history.length,
        at: now,
      },
      derive,
    ),
  ];
}

/** What `pendingBackgroundTasks` lists: the background subagents still running. */
export function droidSubagentRoster(
  subagents: Iterable<DroidSubagent>,
): ReadonlyArray<OrchestrationV2PendingBackgroundTask> {
  return [...subagents].flatMap((subagent) =>
    subagent.background && subagent.status === "running" && subagent.childSessionId !== undefined
      ? [
          {
            taskId: subagent.childSessionId,
            kind: "subagent" as const,
            ...(subagent.title === null ? {} : { description: subagent.title }),
          },
        ]
      : [],
  );
}
