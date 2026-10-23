/**
 * Droid `Task` subagents to v2 subagent events. Pure, like `events.ts`: each call takes the
 * subagent's record and returns the next record plus the events to emit. The adapter owns the
 * records, the clock, and the roster of pending background tasks.
 *
 * A record freezes the ids it launched under (run, provider turn, nodes), so a background
 * subagent can still settle after the turn that launched it has ended.
 */
import type {
  NodeId,
  OrchestrationV2PendingBackgroundTask,
  OrchestrationV2ProviderRef,
  OrchestrationV2Subagent,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import type { ProviderAdapterV2Event } from "@t3tools/provider-core/server/ProviderAdapter";
import type * as DateTime from "effect/DateTime";
import * as Predicate from "effect/Predicate";

import { DROID_DRIVER, type DroidEventContext, type DroidNotification } from "./events.ts";
import { droidTaskResultReport, droidToolKind } from "./protocol.ts";

type DroidSubagentSettleStatus = "completed" | "failed" | "cancelled" | "interrupted";

export interface DroidSubagentOutcome {
  readonly status: DroidSubagentSettleStatus;
  readonly result: string | null;
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
}

export interface DroidSubagent {
  readonly toolUseId: string;
  readonly providerTurnId: ProviderTurnId;
  /** `await: false`: the Task call returns at once and the report arrives as a message later. */
  readonly background: boolean;
  readonly prompt: string;
  readonly title: string | null;
  readonly childSessionId: string | undefined;
  /** `pending` from the Task call until the child session starts or the call ends without one. */
  readonly status: OrchestrationV2Subagent["status"];
  /** Set once the subagent has been announced. */
  readonly surface: SubagentSurface | undefined;
}

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

const isSettledDroidSubagent = (subagent: DroidSubagent): boolean =>
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
  };
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
        childThreadId: null,
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
        childThreadId: null,
        prompt: subagent.prompt,
        result,
      },
    },
  ];
}

function announce(
  known: DroidSubagent,
  ctx: DroidEventContext,
  childSessionId: string | undefined,
  ordinal: number,
  now: DateTime.Utc,
): DroidSubagentUpdate {
  const nativeItemId = `${known.providerTurnId}:subagent:${known.toolUseId}`;
  const nativeTaskRef: OrchestrationV2ProviderRef | null =
    childSessionId === undefined
      ? null
      : { driver: DROID_DRIVER, nativeId: childSessionId, strength: "strong" };
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
  };
  const subagent: DroidSubagent = { ...known, childSessionId, status: "running", surface };
  return {
    subagent,
    events: subagentEvents(subagent, surface, "running", null, now),
    surfaced: true,
  };
}

/**
 * `child_session_available` starts the subagent. Droid has named the child by now, which is why
 * the subagent starts here and not at the Task call. Returns nothing for a duplicate or a late
 * frame of a subagent that has started or ended: those never reopen it.
 */
export function launchDroidSubagent(
  known: DroidSubagent | undefined,
  ctx: DroidEventContext,
  notification: DroidNotification,
  ordinal: number,
  now: DateTime.Utc,
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
  };
  return announce(call, ctx, childSessionId, ordinal, now);
}

export function settleDroidSubagent(
  subagent: DroidSubagent,
  outcome: DroidSubagentOutcome,
  now: DateTime.Utc,
): DroidSubagentUpdate | undefined {
  const { surface } = subagent;
  if (surface === undefined || isSettledDroidSubagent(subagent)) return undefined;
  return {
    subagent: { ...subagent, status: outcome.status },
    events: subagentEvents(subagent, surface, outcome.status, outcome.result, now),
    surfaced: false,
  };
}

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
  stop: { readonly interrupted: boolean; readonly declined: boolean },
  ordinal: number,
  now: DateTime.Utc,
): DroidSubagentUpdate | undefined {
  if (known === undefined || isSettledDroidSubagent(known)) return undefined;
  const failed = notification["isError"] === true;
  if (known.background && !failed) return undefined;
  const announced =
    known.surface === undefined
      ? announce(known, ctx, undefined, ordinal, now)
      : { subagent: known, events: [], surfaced: false };
  const settled = settleDroidSubagent(
    announced.subagent,
    {
      status: !failed
        ? "completed"
        : stop.interrupted
          ? "interrupted"
          : stop.declined
            ? "cancelled"
            : "failed",
      result: droidTaskResultReport(notification["content"]) ?? null,
    },
    now,
  );
  if (settled === undefined) return undefined;
  return {
    subagent: settled.subagent,
    events: [...announced.events, ...settled.events],
    surfaced: announced.surfaced,
  };
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
