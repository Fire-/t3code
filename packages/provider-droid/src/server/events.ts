/**
 * Droid session notifications to v2 provider events. Pure: each call takes the turn's mapper
 * state and returns the next state plus the events to emit. No Effect services and no I/O; the
 * adapter owns the clock, logging, requests, subagents, and `turn.terminal`.
 *
 * `ctx.providerThread` and `ctx.providerTurn` must be the adapter's current records: events that
 * carry them (`provider_thread.updated`) are built from them, so the adapter adopts the emitted
 * record before the next call.
 */
import {
  type NodeId,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2PlanArtifact,
  type OrchestrationV2PlanStep,
  type OrchestrationV2ProviderRef,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2TurnItem,
  type PlanId,
  type RunId,
  type ThreadId,
  type ThreadTokenUsageSnapshot,
  type TurnTokenUsage,
  ProviderDriverKind,
} from "@t3tools/contracts";
import type { IdAllocatorV2Derive } from "@t3tools/provider-core/server/IdAllocator";
import type { ProviderAdapterV2Event } from "@t3tools/provider-core/server/ProviderAdapter";
import { mcpToolPresentation } from "@t3tools/provider-core/server/mcpToolPresentation";
import * as DateTime from "effect/DateTime";
import * as Equal from "effect/Equal";
import * as Predicate from "effect/Predicate";
import * as Struct from "effect/Struct";

import {
  type DroidTokenCounts,
  droidEditDiffCounts,
  droidMcpToolName,
  droidMessageText,
  droidScriptTitle,
  droidTokenCounts,
  droidToolKind,
  droidToolPath,
  droidToolResultOutput,
  isDroidNotificationType,
  isZeroDroidUsage,
  splitDroidExecuteOutput,
  subtractDroidCounts,
} from "./protocol.ts";

export const DROID_DRIVER = ProviderDriverKind.make("droid");

export const DROID_ZERO_COUNTS: DroidTokenCounts = {
  inputTokens: 0,
  outputTokens: 0,
  cacheCreationTokens: 0,
  cacheReadTokens: 0,
  thinkingTokens: 0,
};

export type DroidNotification = { readonly type: string } & Readonly<Record<string, unknown>>;

export interface DroidEventContext {
  readonly threadId: ThreadId;
  readonly runId: RunId | null;
  readonly rootNodeId: NodeId;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly providerTurn: OrchestrationV2ProviderTurn;
  readonly derive: Pick<
    IdAllocatorV2Derive,
    "turnItemFromProviderItem" | "nodeFromProviderItem" | "messageFromProviderItem"
  >;
  /** Droid does not report a context window in usage notifications; the model list does. */
  readonly contextWindow?: number | undefined;
  /**
   * Minimum gap between streaming updates of one block. Deltas inside the gap only grow the
   * block's text; `flushDroidStreams` (or the block's completion) emits the latest text.
   */
  readonly streamFlushMs?: number | undefined;
}

export interface DroidAssistantModel {
  readonly modelId?: string;
  readonly apiProvider?: string;
  readonly reasoningEffort?: string;
}

interface StreamSurface {
  readonly ordinal: number;
  readonly startedAt: DateTime.Utc;
  readonly lastEmittedAt: DateTime.Utc;
  readonly pending: boolean;
}

interface StreamBlock {
  readonly nativeItemId: string;
  readonly kind: "text" | "thinking";
  readonly droidMessageId: string;
  readonly text: string;
  readonly surface: StreamSurface | undefined;
  readonly completed: boolean;
}

type ToolItemType = "command_execution" | "file_change" | "dynamic_tool";

interface ToolSurface {
  readonly itemType: ToolItemType;
  readonly ordinal: number;
  readonly startedAt: DateTime.Utc;
}

interface ToolState {
  readonly nativeItemId: string;
  readonly name: string;
  readonly input: unknown;
  readonly surface: ToolSurface | undefined;
  readonly completed: boolean;
  /** The last `fullOutput` of a command's progress frames. */
  readonly progressOutput: string;
  readonly progressExitCode: number | undefined;
}

export interface DroidMapperState {
  readonly nextItemOrdinal: number;
  readonly blocks: ReadonlyMap<string, StreamBlock>;
  /** Retractions per Droid message id: a re-streamed message gets fresh item ids. */
  readonly generations: ReadonlyMap<string, number>;
  readonly tools: ReadonlyMap<string, ToolState>;
  /** The model the latest assistant message ran on (Droid may reroute a request). */
  readonly assistantModel: DroidAssistantModel | undefined;
  /** The model this turn asked for; undefined for the router, which picks its own. */
  readonly requestedModelId: string | undefined;
  /** Models already announced as a reroute this turn, so each is announced once. */
  readonly reroutedModels: ReadonlySet<string>;
  /** Session-wide counts at turn start and now; undefined while unknown. */
  readonly turnBaseline: DroidTokenCounts | undefined;
  readonly sessionUsage: DroidTokenCounts | undefined;
  readonly contextUsage: ThreadTokenUsageSnapshot | undefined;
  /** Set when `agent_turn_completed` arrives; carry it on the provider turn's final update. */
  readonly turnTokenUsage: TurnTokenUsage | undefined;
  /**
   * Set when Stop is sent to Droid. Droid answers a cancelled tool with an error `tool_result`
   * (sometimes two) before the cancelled completion; that is a stop, not a tool failure.
   */
  readonly interruptRequested: boolean;
  /**
   * Tools whose permission the user declined. Droid answers each with an error `tool_result`;
   * that is a refusal the user chose, so the tool settles `cancelled` rather than `failed`.
   */
  readonly declinedToolUseIds: ReadonlySet<string>;
  /** This turn's todo lists by TodoWrite tool use. */
  readonly todoLists: ReadonlyMap<
    string,
    { readonly planId: PlanId; readonly ordinal: number; readonly startedAt: DateTime.Utc }
  >;
}

export interface DroidMapResult {
  readonly state: DroidMapperState;
  readonly events: ReadonlyArray<ProviderAdapterV2Event>;
  /** Set for a notification type the mapper does not know; the adapter logs it. */
  readonly unknownType?: string;
}

/**
 * `sessionUsage` is the Droid session's cumulative counts when the turn starts: zero for a
 * session this process just created, undefined when unknown (a loaded session). `contextUsage`
 * is the provider thread's current snapshot.
 */
export function createDroidTurnState(input: {
  readonly firstItemOrdinal: number;
  readonly sessionUsage?: DroidTokenCounts | undefined;
  readonly contextUsage?: ThreadTokenUsageSnapshot | undefined;
  readonly requestedModelId?: string | undefined;
}): DroidMapperState {
  return {
    nextItemOrdinal: input.firstItemOrdinal,
    blocks: new Map(),
    generations: new Map(),
    tools: new Map(),
    assistantModel: undefined,
    requestedModelId: input.requestedModelId,
    reroutedModels: new Set(),
    turnBaseline: input.sessionUsage,
    sessionUsage: input.sessionUsage,
    contextUsage: input.contextUsage,
    turnTokenUsage: undefined,
    interruptRequested: false,
    declinedToolUseIds: new Set(),
    todoLists: new Map(),
  };
}

const asString = (value: unknown): string | undefined =>
  Predicate.isString(value) ? value : undefined;

function asTrimmed(value: unknown): string | undefined {
  if (!Predicate.isString(value)) return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

const asInteger = (value: unknown): number | undefined =>
  Predicate.isNumber(value) && Number.isInteger(value) ? value : undefined;

function withEntry<K, V>(map: ReadonlyMap<K, V>, key: K, value: V): ReadonlyMap<K, V> {
  const next = new Map(map);
  next.set(key, value);
  return next;
}

const unchanged = (state: DroidMapperState): DroidMapResult => ({ state, events: [] });

function providerRef(nativeItemId: string): OrchestrationV2ProviderRef {
  return { driver: DROID_DRIVER, nativeId: nativeItemId, strength: "strong" };
}

function itemBase(
  ctx: DroidEventContext,
  nativeItemId: string,
  ordinal: number,
  startedAt: DateTime.Utc,
  updatedAt: DateTime.Utc,
) {
  return {
    id: ctx.derive.turnItemFromProviderItem({ driver: DROID_DRIVER, nativeItemId }),
    threadId: ctx.threadId,
    runId: ctx.runId,
    nodeId: ctx.derive.nodeFromProviderItem({ driver: DROID_DRIVER, nativeItemId }),
    providerThreadId: ctx.providerThread.id,
    providerTurnId: ctx.providerTurn.id,
    nativeItemRef: providerRef(nativeItemId),
    parentItemId: null,
    ordinal,
    startedAt,
    updatedAt,
  };
}

function nodeEvent(
  ctx: DroidEventContext,
  nativeItemId: string,
  kind: OrchestrationV2ExecutionNode["kind"],
  status: OrchestrationV2ExecutionNode["status"],
  startedAt: DateTime.Utc,
  completedAt: DateTime.Utc | null,
): ProviderAdapterV2Event {
  return {
    type: "node.updated",
    driver: DROID_DRIVER,
    node: {
      id: ctx.derive.nodeFromProviderItem({ driver: DROID_DRIVER, nativeItemId }),
      threadId: ctx.threadId,
      runId: ctx.runId,
      parentNodeId: ctx.rootNodeId,
      rootNodeId: ctx.rootNodeId,
      kind,
      status,
      countsForRun: false,
      providerThreadId: ctx.providerThread.id,
      providerTurnId: ctx.providerTurn.id,
      nativeItemRef: providerRef(nativeItemId),
      runtimeRequestId: null,
      checkpointScopeId: null,
      startedAt,
      completedAt,
    },
  };
}

// ---------------------------------------------------------------------------
// Streaming text and reasoning
// ---------------------------------------------------------------------------

const blockKey = (kind: StreamBlock["kind"], messageId: string, blockIndex: number): string =>
  `${kind}:${messageId}:${blockIndex}`;

/** The provider turn id leads every native item id: Droid message ids repeat across forks. */
function blockNativeItemId(
  state: DroidMapperState,
  ctx: DroidEventContext,
  kind: StreamBlock["kind"],
  messageId: string,
  blockIndex: number,
): string {
  const generation = state.generations.get(messageId) ?? 0;
  const suffix = generation > 0 ? `-r${generation}` : "";
  const label = kind === "text" ? "text" : "reasoning";
  return `${ctx.providerTurn.id}:${label}:${messageId}:${blockIndex}${suffix}`;
}

function blockLocation(
  notification: DroidNotification,
): { readonly messageId: string; readonly blockIndex: number } | undefined {
  const messageId = asTrimmed(notification["messageId"]);
  const blockIndex = asInteger(notification["blockIndex"]);
  return messageId !== undefined && blockIndex !== undefined
    ? { messageId, blockIndex }
    : undefined;
}

function newBlock(
  state: DroidMapperState,
  ctx: DroidEventContext,
  kind: StreamBlock["kind"],
  messageId: string,
  blockIndex: number,
): StreamBlock {
  return {
    nativeItemId: blockNativeItemId(state, ctx, kind, messageId, blockIndex),
    kind,
    droidMessageId: messageId,
    text: "",
    surface: undefined,
    completed: false,
  };
}

function blockEvents(
  ctx: DroidEventContext,
  block: StreamBlock,
  surface: StreamSurface,
  now: DateTime.Utc,
  phase: "first" | "update" | "final",
): ReadonlyArray<ProviderAdapterV2Event> {
  const streaming = phase !== "final";
  const status = streaming ? "running" : "completed";
  const completedAt = streaming ? null : now;
  const base = itemBase(ctx, block.nativeItemId, surface.ordinal, surface.startedAt, now);
  const node =
    phase === "update"
      ? []
      : [
          nodeEvent(
            ctx,
            block.nativeItemId,
            block.kind === "text" ? "assistant_message" : "reasoning",
            status,
            surface.startedAt,
            completedAt,
          ),
        ];
  if (block.kind === "thinking") {
    return [
      ...node,
      {
        type: "turn_item.updated",
        driver: DROID_DRIVER,
        turnItem: {
          ...base,
          status,
          title: null,
          completedAt,
          type: "reasoning",
          text: block.text,
          streaming,
        },
      },
    ];
  }
  const messageId = ctx.derive.messageFromProviderItem({
    driver: DROID_DRIVER,
    nativeItemId: block.nativeItemId,
  });
  return [
    ...node,
    {
      type: "turn_item.updated",
      driver: DROID_DRIVER,
      turnItem: {
        ...base,
        status,
        title: null,
        completedAt,
        type: "assistant_message",
        messageId,
        text: block.text,
        streaming,
      },
    },
    {
      type: "message.updated",
      driver: DROID_DRIVER,
      message: {
        id: messageId,
        threadId: ctx.threadId,
        runId: ctx.runId,
        nodeId: base.nodeId,
        role: "assistant",
        text: block.text,
        attachments: [],
        streaming,
        createdBy: "agent",
        creationSource: "provider",
        createdAt: surface.startedAt,
        updatedAt: now,
      },
    },
  ];
}

function mapTextDelta(
  state: DroidMapperState,
  ctx: DroidEventContext,
  notification: DroidNotification,
  kind: StreamBlock["kind"],
  now: DateTime.Utc,
): DroidMapResult {
  const location = blockLocation(notification);
  const delta = asString(notification["textDelta"]);
  if (location === undefined || delta === undefined || delta.length === 0) return unchanged(state);
  const key = blockKey(kind, location.messageId, location.blockIndex);
  const current =
    state.blocks.get(key) ?? newBlock(state, ctx, kind, location.messageId, location.blockIndex);
  if (current.completed) return unchanged(state);
  const block: StreamBlock = { ...current, text: current.text + delta };

  // Droid streams whitespace-only text blocks (e.g. "\n\n" before a tool call); holding them back
  // until real text arrives keeps empty bubbles out of the thread.
  if (block.surface === undefined && block.text.trim().length === 0) {
    return { state: { ...state, blocks: withEntry(state.blocks, key, block) }, events: [] };
  }

  if (block.surface === undefined) {
    const surface: StreamSurface = {
      ordinal: state.nextItemOrdinal,
      startedAt: now,
      lastEmittedAt: now,
      pending: false,
    };
    const surfaced = { ...block, surface };
    return {
      state: {
        ...state,
        nextItemOrdinal: state.nextItemOrdinal + 1,
        blocks: withEntry(state.blocks, key, surfaced),
      },
      events: blockEvents(ctx, surfaced, surface, now, "first"),
    };
  }

  const flushMs = ctx.streamFlushMs ?? 0;
  const sinceLast =
    DateTime.toEpochMillis(now) - DateTime.toEpochMillis(block.surface.lastEmittedAt);
  if (flushMs > 0 && sinceLast < flushMs) {
    const held = { ...block, surface: { ...block.surface, pending: true } };
    return { state: { ...state, blocks: withEntry(state.blocks, key, held) }, events: [] };
  }
  const surface = { ...block.surface, lastEmittedAt: now, pending: false };
  const emitted = { ...block, surface };
  return {
    state: { ...state, blocks: withEntry(state.blocks, key, emitted) },
    events: blockEvents(ctx, emitted, surface, now, "update"),
  };
}

function completeBlock(
  ctx: DroidEventContext,
  block: StreamBlock,
  now: DateTime.Utc,
): { readonly block: StreamBlock; readonly events: ReadonlyArray<ProviderAdapterV2Event> } {
  if (block.surface === undefined || block.completed) return { block, events: [] };
  return {
    block: { ...block, completed: true, surface: { ...block.surface, pending: false } },
    events: blockEvents(ctx, block, block.surface, now, "final"),
  };
}

function mapTextComplete(
  state: DroidMapperState,
  ctx: DroidEventContext,
  notification: DroidNotification,
  kind: StreamBlock["kind"],
  now: DateTime.Utc,
): DroidMapResult {
  const location = blockLocation(notification);
  if (location === undefined) return unchanged(state);
  const key = blockKey(kind, location.messageId, location.blockIndex);
  const block = state.blocks.get(key);
  // A block that never surfaced stays open: the committed assistant message may still carry text.
  if (block === undefined || block.surface === undefined || block.completed) {
    return unchanged(state);
  }
  const completed = completeBlock(ctx, block, now);
  return {
    state: { ...state, blocks: withEntry(state.blocks, key, completed.block) },
    events: completed.events,
  };
}

function mapRetraction(
  state: DroidMapperState,
  ctx: DroidEventContext,
  notification: DroidNotification,
  now: DateTime.Utc,
): DroidMapResult {
  const messageId = asTrimmed(notification["messageId"]);
  if (messageId === undefined) return unchanged(state);
  const events: Array<ProviderAdapterV2Event> = [];
  const blocks = new Map(state.blocks);
  for (const [key, block] of state.blocks) {
    if (block.droidMessageId !== messageId) continue;
    events.push(...completeBlock(ctx, block, now).events);
    blocks.delete(key);
  }
  return {
    state: {
      ...state,
      blocks,
      generations: withEntry(
        state.generations,
        messageId,
        (state.generations.get(messageId) ?? 0) + 1,
      ),
    },
    events,
  };
}

function assistantModelOf(message: Readonly<Record<string, unknown>>): DroidAssistantModel {
  const modelId = asTrimmed(message["modelId"]);
  const apiProvider = asTrimmed(message["apiProvider"]);
  const reasoningEffort = asTrimmed(message["reasoningEffort"]);
  return {
    ...(modelId === undefined ? {} : { modelId }),
    ...(apiProvider === undefined ? {} : { apiProvider }),
    ...(reasoningEffort === undefined ? {} : { reasoningEffort }),
  };
}

function systemNotice(
  state: DroidMapperState,
  ctx: DroidEventContext,
  nativeItemId: string,
  message: string,
  now: DateTime.Utc,
): DroidMapResult {
  const ordinal = state.nextItemOrdinal;
  return {
    state: { ...state, nextItemOrdinal: ordinal + 1 },
    events: [
      nodeEvent(ctx, nativeItemId, "system", "completed", now, now),
      {
        type: "turn_item.updated",
        driver: DROID_DRIVER,
        turnItem: {
          ...itemBase(ctx, nativeItemId, ordinal, now, now),
          status: "completed",
          title: message,
          completedAt: now,
          type: "system_notice",
          message,
        },
      },
    ],
  };
}

/**
 * Droid may answer a request with another model than the one asked for. The first reply of each
 * such model in a turn gets one notice; the router asks for no model, so it never reroutes. Droid
 * keeps that model for the session without saying so, and the adapter asks for the thread's model
 * again on the next turn, which the notice promises.
 */
function rerouteNotice(
  state: DroidMapperState,
  ctx: DroidEventContext,
  answeredBy: DroidAssistantModel,
  now: DateTime.Utc,
): DroidMapResult | undefined {
  const { modelId, apiProvider } = answeredBy;
  const requested = state.requestedModelId;
  if (
    requested === undefined ||
    modelId === undefined ||
    modelId === requested ||
    state.reroutedModels.has(modelId)
  ) {
    return undefined;
  }
  const message = `Droid answered with ${modelId}${
    apiProvider === undefined ? "" : ` (${apiProvider})`
  } instead of the requested ${requested}. Switching back to ${requested} on your next message.`;
  return systemNotice(
    { ...state, reroutedModels: new Set([...state.reroutedModels, modelId]) },
    ctx,
    `${ctx.providerTurn.id}:reroute:${modelId}`,
    message,
    now,
  );
}

/** Droid explains a model switch, a failed sign-in and the like in messages only the user sees. */
function droidNotice(
  state: DroidMapperState,
  ctx: DroidEventContext,
  message: Readonly<Record<string, unknown>>,
  now: DateTime.Utc,
): DroidMapResult {
  const messageId = asTrimmed(message["id"]);
  const text = droidMessageText(message["content"]).trim();
  if (messageId === undefined || text.length === 0) return unchanged(state);
  return systemNotice(state, ctx, `${ctx.providerTurn.id}:notice:${messageId}`, text, now);
}

/**
 * The committed assistant message settles what streaming left open and surfaces text blocks
 * that never streamed. Reasoning is only ever shown from its stream.
 */
function mapCreateMessage(
  state: DroidMapperState,
  ctx: DroidEventContext,
  notification: DroidNotification,
  now: DateTime.Utc,
): DroidMapResult {
  const message = notification["message"];
  if (!Predicate.isObject(message)) return unchanged(state);
  if (message["visibility"] === "user_only" && message["role"] !== "assistant") {
    return droidNotice(state, ctx, message, now);
  }
  // User messages belong to t3; system, tool, and llm_only messages are plumbing.
  if (message["role"] !== "assistant" || message["visibility"] === "llm_only") {
    return unchanged(state);
  }
  const answeredBy = assistantModelOf(message);
  let next: DroidMapperState = { ...state, assistantModel: answeredBy };
  const events: Array<ProviderAdapterV2Event> = [];
  const reroute = rerouteNotice(next, ctx, answeredBy, now);
  if (reroute !== undefined) {
    events.push(...reroute.events);
    next = reroute.state;
  }
  const messageId = asTrimmed(message["id"]);
  const content = message["content"];
  if (messageId === undefined || !Array.isArray(content)) return { state: next, events };

  content.forEach((entry, blockIndex) => {
    if (!Predicate.isObject(entry)) return;
    const kind =
      entry["type"] === "text" ? "text" : entry["type"] === "thinking" ? "thinking" : null;
    if (kind === null) return;
    const key = blockKey(kind, messageId, blockIndex);
    const existing = next.blocks.get(key);
    if (existing?.completed) return;
    if (existing?.surface !== undefined) {
      const completed = completeBlock(ctx, existing, now);
      events.push(...completed.events);
      next = { ...next, blocks: withEntry(next.blocks, key, completed.block) };
      return;
    }
    const text = kind === "text" ? asTrimmed(entry["text"]) : undefined;
    if (text === undefined) return;
    const base = existing ?? newBlock(next, ctx, kind, messageId, blockIndex);
    const surface: StreamSurface = {
      ordinal: next.nextItemOrdinal,
      startedAt: now,
      lastEmittedAt: now,
      pending: false,
    };
    const completed: StreamBlock = { ...base, text, surface, completed: true };
    events.push(...blockEvents(ctx, completed, surface, now, "final"));
    next = {
      ...next,
      nextItemOrdinal: next.nextItemOrdinal + 1,
      blocks: withEntry(next.blocks, key, completed),
    };
  });
  return { state: next, events };
}

/** Emits the latest text of blocks whose streaming updates were held back by `streamFlushMs`. */
export function flushDroidStreams(
  state: DroidMapperState,
  ctx: DroidEventContext,
  now: DateTime.Utc,
): DroidMapResult {
  const events: Array<ProviderAdapterV2Event> = [];
  const blocks = new Map(state.blocks);
  for (const [key, block] of state.blocks) {
    if (block.surface === undefined || !block.surface.pending || block.completed) continue;
    const surface = { ...block.surface, lastEmittedAt: now, pending: false };
    events.push(...blockEvents(ctx, block, surface, now, "update"));
    blocks.set(key, { ...block, surface });
  }
  return events.length === 0 ? unchanged(state) : { state: { ...state, blocks }, events };
}

// ---------------------------------------------------------------------------
// Tools
// ---------------------------------------------------------------------------

type ToolSettleStatus = "completed" | "failed" | "interrupted" | "cancelled";

export interface DroidToolSettlement {
  readonly status: ToolSettleStatus;
  /** The `tool_result` payload; absent when the turn ended without one. */
  readonly content?: unknown;
}

type TurnItemBaseKey =
  | "id"
  | "threadId"
  | "runId"
  | "nodeId"
  | "providerThreadId"
  | "providerTurnId"
  | "nativeItemRef"
  | "parentItemId"
  | "ordinal"
  | "startedAt"
  | "completedAt"
  | "updatedAt";
type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never;

/** A tool's item without the ids and times that place it in a thread. */
export type DroidToolItemFields = DistributiveOmit<
  Extract<OrchestrationV2TurnItem, { readonly type: ToolItemType }>,
  TurnItemBaseKey
>;

const isEmptyInput = (input: unknown): boolean =>
  input === undefined ||
  input === null ||
  (Predicate.isObject(input) && Object.keys(input).length === 0);

/**
 * An item keeps the type it first appears with. File tools wait for their path (Droid's first
 * `tool_call` frame has an empty input); a tool whose final input has no path is a dynamic tool.
 */
export function droidToolItemType(
  name: string,
  input: unknown,
  final: boolean,
): ToolItemType | undefined {
  switch (droidToolKind(name)) {
    case "execute":
      return "command_execution";
    case "edit":
      if (droidToolPath(input) !== undefined) return "file_change";
      return final || !isEmptyInput(input) ? "dynamic_tool" : undefined;
    default:
      return "dynamic_tool";
  }
}

/**
 * How a tool call reads as an item. Live turns and imported transcripts both build tool items
 * here, so a tool looks the same whichever way it reached the thread.
 */
export function droidToolItemFields(
  tool: {
    readonly name: string;
    readonly input: unknown;
    readonly progressExitCode?: number | undefined;
  },
  itemType: ToolItemType,
  settlement: DroidToolSettlement | undefined,
): DroidToolItemFields {
  const status = settlement?.status ?? "running";
  const resultText =
    settlement === undefined ? undefined : droidToolResultOutput(settlement.content).text;
  const input = Predicate.isObject(tool.input) ? tool.input : {};

  if (itemType === "command_execution") {
    const { output, exitCode: footerExitCode } = splitDroidExecuteOutput(resultText);
    const exitCode = footerExitCode ?? tool.progressExitCode;
    return {
      status,
      title: tool.name,
      type: "command_execution",
      input: asString(input["command"]) ?? "",
      ...(output === undefined ? {} : { output }),
      ...(exitCode === undefined ? {} : { exitCode }),
    };
  }

  const fileName = droidToolPath(tool.input);
  if (itemType === "file_change" && fileName !== undefined) {
    const oldStr = asString(input["old_str"]);
    const newStr = asString(input["new_str"]) ?? asString(input["content"]);
    const counts = status === "completed" ? droidEditDiffCounts(settlement?.content) : undefined;
    const diffStr = status === "failed" ? resultText : undefined;
    return {
      status,
      title: tool.name,
      type: "file_change",
      fileName,
      ...(oldStr === undefined ? {} : { oldStr }),
      ...(newStr === undefined ? {} : { newStr }),
      ...(counts === undefined ? {} : counts),
      ...(diffStr === undefined ? {} : { diffStr }),
    };
  }

  // Script runs JavaScript, so clients get it as their JavaScript tool: a `code` input that
  // renders highlighted and height-capped instead of one unbounded plain-text `script` line.
  if (tool.name === "Script") {
    const code = asString(input["script"]);
    return {
      status,
      type: "dynamic_tool",
      title: code === undefined ? tool.name : droidScriptTitle(code),
      toolName: "js",
      input: code === undefined ? {} : { code },
      ...(resultText === undefined ? {} : { output: resultText }),
    };
  }

  const toolName = droidMcpToolName(tool.name);
  return {
    status,
    type: "dynamic_tool",
    title: toolName,
    ...mcpToolPresentation({ toolName }),
    toolName,
    input: tool.input,
    ...(resultText === undefined ? {} : { output: resultText }),
  };
}

function toolItemEvent(
  ctx: DroidEventContext,
  tool: ToolState,
  surface: ToolSurface,
  now: DateTime.Utc,
  settlement: DroidToolSettlement | undefined,
): ProviderAdapterV2Event {
  return {
    type: "turn_item.updated",
    driver: DROID_DRIVER,
    turnItem: {
      ...itemBase(ctx, tool.nativeItemId, surface.ordinal, surface.startedAt, now),
      completedAt: settlement === undefined ? null : now,
      ...droidToolItemFields(tool, surface.itemType, settlement),
    },
  };
}

/** Records the tool and emits its item (and node on first appearance and on settlement). */
function updateTool(
  state: DroidMapperState,
  ctx: DroidEventContext,
  tool: ToolState,
  now: DateTime.Utc,
  settlement: DroidToolSettlement | undefined,
): DroidMapResult {
  const itemType =
    tool.surface?.itemType ?? droidToolItemType(tool.name, tool.input, settlement !== undefined);
  const completed = settlement !== undefined;
  if (itemType === undefined) {
    return {
      state: { ...state, tools: withEntry(state.tools, tool.nativeItemId, { ...tool, completed }) },
      events: [],
    };
  }
  const isNew = tool.surface === undefined;
  const surface: ToolSurface = tool.surface ?? {
    itemType,
    ordinal: state.nextItemOrdinal,
    startedAt: now,
  };
  const events: Array<ProviderAdapterV2Event> = [];
  if (isNew || completed) {
    events.push(
      nodeEvent(
        ctx,
        tool.nativeItemId,
        "tool_call",
        settlement?.status ?? "running",
        surface.startedAt,
        completed ? now : null,
      ),
    );
  }
  events.push(toolItemEvent(ctx, tool, surface, now, settlement));
  return {
    state: {
      ...state,
      nextItemOrdinal: isNew ? state.nextItemOrdinal + 1 : state.nextItemOrdinal,
      tools: withEntry(state.tools, tool.nativeItemId, { ...tool, surface, completed }),
    },
    events,
  };
}

const toolNativeItemId = (ctx: DroidEventContext, toolUseId: string): string =>
  `${ctx.providerTurn.id}:tool:${toolUseId}`;

function mapToolCall(
  state: DroidMapperState,
  ctx: DroidEventContext,
  notification: DroidNotification,
  now: DateTime.Utc,
): DroidMapResult {
  const toolUse = notification["toolUse"];
  const toolUseId = Predicate.isObject(toolUse) ? asTrimmed(toolUse["id"]) : undefined;
  if (!Predicate.isObject(toolUse) || toolUseId === undefined) return unchanged(state);
  const name = asTrimmed(toolUse["name"]);
  // A Task is a subagent, not a tool row: `subagents.ts` projects it.
  if (name !== undefined && droidToolKind(name) === "task") return unchanged(state);
  const nativeItemId = toolNativeItemId(ctx, toolUseId);
  const existing = state.tools.get(nativeItemId);
  if (existing?.completed) return unchanged(state);
  const tool: ToolState = {
    nativeItemId,
    name: name ?? existing?.name ?? "Tool",
    input: toolUse["input"] ?? existing?.input ?? {},
    surface: existing?.surface,
    completed: false,
    progressOutput: existing?.progressOutput ?? "",
    progressExitCode: existing?.progressExitCode,
  };
  return updateTool(state, ctx, tool, now, undefined);
}

function mapToolResult(
  state: DroidMapperState,
  ctx: DroidEventContext,
  notification: DroidNotification,
  now: DateTime.Utc,
): DroidMapResult {
  const toolUseId = asTrimmed(notification["toolUseId"]);
  const tool =
    toolUseId === undefined ? undefined : state.tools.get(toolNativeItemId(ctx, toolUseId));
  if (tool === undefined || tool.completed) return unchanged(state);
  const errorStatus = state.interruptRequested
    ? "interrupted"
    : toolUseId !== undefined && state.declinedToolUseIds.has(toolUseId)
      ? "cancelled"
      : "failed";
  return updateTool(state, ctx, tool, now, {
    status: notification["isError"] === true ? errorStatus : "completed",
    content: notification["content"],
  });
}

/**
 * Command output goes out as `command_output.delta` only. `fullOutput` is the accumulated
 * output (mid frames also carry a recent-tail `text`), so a frame's chunk is what grew since
 * the previous frame. When Droid rewrites rather than extends it (truncation), no chunk is
 * sent: the final output still arrives with the item.
 */
function mapToolProgress(
  state: DroidMapperState,
  ctx: DroidEventContext,
  notification: DroidNotification,
): DroidMapResult {
  const toolUseId = asTrimmed(notification["toolUseId"]);
  const tool =
    toolUseId === undefined ? undefined : state.tools.get(toolNativeItemId(ctx, toolUseId));
  const update = notification["update"];
  if (
    tool === undefined ||
    tool.completed ||
    tool.surface?.itemType !== "command_execution" ||
    !Predicate.isObject(update)
  ) {
    return unchanged(state);
  }
  const fullOutput = asString(update["fullOutput"]);
  const text = asString(update["text"]);
  const chunk =
    fullOutput === undefined
      ? text
      : fullOutput.startsWith(tool.progressOutput)
        ? fullOutput.slice(tool.progressOutput.length)
        : undefined;
  const next: ToolState = {
    ...tool,
    progressOutput: fullOutput ?? tool.progressOutput,
    progressExitCode: asInteger(update["exitCode"]) ?? tool.progressExitCode,
  };
  const nextState = { ...state, tools: withEntry(state.tools, tool.nativeItemId, next) };
  if (chunk === undefined || chunk.length === 0) return { state: nextState, events: [] };
  return {
    state: nextState,
    events: [
      {
        type: "command_output.delta",
        driver: DROID_DRIVER,
        threadId: ctx.threadId,
        itemId: ctx.derive.turnItemFromProviderItem({
          driver: DROID_DRIVER,
          nativeItemId: tool.nativeItemId,
        }),
        chunk,
      },
    ],
  };
}

// ---------------------------------------------------------------------------
// Proposed plans
// ---------------------------------------------------------------------------

/**
 * The plan Droid wrote before asking to leave spec mode: a `proposed_plan` item in the turn and
 * its artifact, which supersedes the thread's previous proposal. `sourceId` is the ExitSpecMode
 * tool use (or the request) that carried the plan. The adapter allocates `planId` and keeps the
 * returned `plan` as the next call's `previous`.
 */
export function mapDroidProposedPlan(
  state: DroidMapperState,
  ctx: DroidEventContext,
  input: {
    readonly planId: PlanId;
    readonly sourceId: string;
    readonly markdown: string;
    readonly previous: OrchestrationV2PlanArtifact | undefined;
  },
  now: DateTime.Utc,
): DroidMapResult & { readonly plan: OrchestrationV2PlanArtifact } {
  const nativeItemId = `${ctx.providerTurn.id}:plan:${input.sourceId}`;
  const base = itemBase(ctx, nativeItemId, state.nextItemOrdinal, now, now);
  const plan: OrchestrationV2PlanArtifact = {
    id: input.planId,
    threadId: ctx.threadId,
    runId: ctx.runId,
    nodeId: base.nodeId,
    kind: "proposed_plan",
    status: "active",
    markdown: input.markdown,
  };
  const events: Array<ProviderAdapterV2Event> = [];
  if (input.previous !== undefined) {
    events.push({
      type: "plan.updated",
      driver: DROID_DRIVER,
      plan: { ...input.previous, status: "superseded" },
    });
  }
  events.push(
    nodeEvent(ctx, nativeItemId, "plan", "completed", now, now),
    { type: "plan.updated", driver: DROID_DRIVER, plan },
    {
      type: "turn_item.updated",
      driver: DROID_DRIVER,
      turnItem: {
        ...base,
        status: "completed",
        title: null,
        completedAt: now,
        type: "proposed_plan",
        planId: input.planId,
        markdown: input.markdown,
        streaming: false,
      },
    },
  );
  return { state: { ...state, nextItemOrdinal: state.nextItemOrdinal + 1 }, events, plan };
}

// ---------------------------------------------------------------------------
// Todo lists
// ---------------------------------------------------------------------------

const DROID_TODO_LINE = /^\s*\d+\.\s*\[([a-z_]+)\]\s*(.*?)\s*$/u;

/** A TodoWrite call's list. Droid sends `todos` as numbered `N. [status] text` lines. */
export function droidTodoSteps(input: unknown): ReadonlyArray<OrchestrationV2PlanStep> {
  const todos = Predicate.isObject(input) ? input["todos"] : undefined;
  if (!Predicate.isString(todos)) return [];
  const steps: Array<OrchestrationV2PlanStep> = [];
  for (const line of todos.split("\n")) {
    const match = DROID_TODO_LINE.exec(line);
    const text = match?.[2];
    if (match === null || text === undefined || text.length === 0) continue;
    steps.push({
      id: `todo-${steps.length}`,
      text,
      status:
        match[1] === "completed" ? "completed" : match[1] === "in_progress" ? "running" : "pending",
    });
  }
  return steps;
}

/**
 * The todo list a TodoWrite call wrote: a `todo_list` item and its artifact, which supersedes
 * the thread's previous list. `sourceId` is the tool use; Droid can resend a tool call as its
 * input fills in, so a repeat updates the same item. The adapter allocates `planId` for a new
 * `sourceId` and keeps the returned `plan` as the next call's `previous`.
 */
export function mapDroidTodoList(
  state: DroidMapperState,
  ctx: DroidEventContext,
  input: {
    readonly planId: PlanId;
    readonly sourceId: string;
    readonly steps: ReadonlyArray<OrchestrationV2PlanStep>;
    readonly previous: OrchestrationV2PlanArtifact | undefined;
  },
  now: DateTime.Utc,
): DroidMapResult & { readonly plan: OrchestrationV2PlanArtifact } {
  const nativeItemId = `${ctx.providerTurn.id}:todos:${input.sourceId}`;
  const known = state.todoLists.get(input.sourceId);
  const ordinal = known?.ordinal ?? state.nextItemOrdinal;
  const base = itemBase(ctx, nativeItemId, ordinal, known?.startedAt ?? now, now);
  const plan: OrchestrationV2PlanArtifact = {
    id: input.planId,
    threadId: ctx.threadId,
    runId: ctx.runId,
    nodeId: base.nodeId,
    kind: "todo_list",
    status: input.steps.every((step) => step.status === "completed") ? "completed" : "active",
    steps: input.steps,
  };
  const events: Array<ProviderAdapterV2Event> = [];
  if (input.previous !== undefined && input.previous.id !== input.planId) {
    events.push({
      type: "plan.updated",
      driver: DROID_DRIVER,
      plan: { ...input.previous, status: "superseded" },
    });
  }
  events.push(
    nodeEvent(ctx, nativeItemId, "todo_list", "completed", base.startedAt, now),
    { type: "plan.updated", driver: DROID_DRIVER, plan },
    {
      type: "turn_item.updated",
      driver: DROID_DRIVER,
      turnItem: {
        ...base,
        status: "completed",
        title: null,
        completedAt: now,
        type: "todo_list",
        planId: input.planId,
        steps: input.steps,
      },
    },
  );
  return {
    state: {
      ...state,
      nextItemOrdinal: known === undefined ? state.nextItemOrdinal + 1 : state.nextItemOrdinal,
      todoLists: withEntry(state.todoLists, input.sourceId, {
        planId: input.planId,
        ordinal,
        startedAt: base.startedAt,
      }),
    },
    events,
    plan,
  };
}

// ---------------------------------------------------------------------------
// Token usage
// ---------------------------------------------------------------------------

/** Droid's `inputTokens` excludes cache reads and writes; t3's input counts both. */
const contextInputTokens = (counts: DroidTokenCounts): number =>
  counts.inputTokens + counts.cacheReadTokens + counts.cacheCreationTokens;

const processedTokens = (counts: DroidTokenCounts): number =>
  contextInputTokens(counts) + counts.outputTokens;

const maxTokensOf = (ctx: DroidEventContext): { readonly maxTokens?: number } =>
  ctx.contextWindow !== undefined && Number.isInteger(ctx.contextWindow) && ctx.contextWindow > 0
    ? { maxTokens: ctx.contextWindow }
    : {};

function totalsFields(totals: DroidTokenCounts | undefined) {
  if (totals === undefined || processedTokens(totals) <= 0) return {};
  return {
    totalProcessedTokens: processedTokens(totals),
    inputTokens: contextInputTokens(totals),
    outputTokens: totals.outputTokens,
    ...(totals.cacheReadTokens > 0 ? { cachedInputTokens: totals.cacheReadTokens } : {}),
    ...(totals.thinkingTokens > 0 ? { reasoningOutputTokens: totals.thinkingTokens } : {}),
  };
}

const withTotals = (
  snapshot: ThreadTokenUsageSnapshot,
  totals: DroidTokenCounts,
): ThreadTokenUsageSnapshot => ({
  ...Struct.omit(snapshot, [
    "totalProcessedTokens",
    "inputTokens",
    "outputTokens",
    "cachedInputTokens",
    "reasoningOutputTokens",
  ]),
  ...totalsFields(totals),
});

const sameSnapshot = (left: ThreadTokenUsageSnapshot, right: ThreadTokenUsageSnapshot): boolean =>
  Equal.equals(left, right);

function threadUsageEvent(
  ctx: DroidEventContext,
  contextUsage: ThreadTokenUsageSnapshot,
  now: DateTime.Utc,
): ProviderAdapterV2Event {
  return {
    type: "provider_thread.updated",
    driver: DROID_DRIVER,
    providerThread: { ...ctx.providerThread, contextUsage, updatedAt: now },
  };
}

/** The context size is the last model call's input plus output; totals are the session's. */
function mapTokenUsage(
  state: DroidMapperState,
  ctx: DroidEventContext,
  notification: DroidNotification,
  now: DateTime.Utc,
): DroidMapResult {
  const totals = droidTokenCounts(notification["tokenUsage"]);
  const last = droidTokenCounts(notification["lastCallTokenUsage"]);
  const next = totals === undefined ? state : { ...state, sessionUsage: totals };
  const usedTokens = last === undefined ? 0 : processedTokens(last);
  if (last === undefined || usedTokens <= 0) return { state: next, events: [] };
  const snapshot: ThreadTokenUsageSnapshot = {
    usedTokens,
    ...maxTokensOf(ctx),
    lastUsedTokens: usedTokens,
    lastInputTokens: contextInputTokens(last),
    lastOutputTokens: last.outputTokens,
    ...(last.cacheReadTokens > 0 ? { lastCachedInputTokens: last.cacheReadTokens } : {}),
    ...totalsFields(totals),
  };
  return {
    state: { ...next, contextUsage: snapshot },
    events: [threadUsageEvent(ctx, snapshot, now)],
  };
}

const nonZero = (counts: DroidTokenCounts | undefined): DroidTokenCounts | undefined =>
  counts !== undefined && !isZeroDroidUsage(counts) ? counts : undefined;

function turnUsage(
  counts: DroidTokenCounts,
  hasSubagents: boolean,
  partial: boolean,
): TurnTokenUsage {
  const common = {
    usageScope: "main_agent",
    hasSubagents,
    inputTokens: contextInputTokens(counts),
    outputTokens: counts.outputTokens,
    ...(counts.cacheReadTokens > 0 ? { cachedInputTokens: counts.cacheReadTokens } : {}),
    ...(counts.cacheCreationTokens > 0 ? { cacheCreationTokens: counts.cacheCreationTokens } : {}),
    ...(counts.thinkingTokens > 0 && counts.thinkingTokens <= counts.outputTokens
      ? { reasoningTokens: counts.thinkingTokens }
      : {}),
  } as const;
  return partial ? { ...common, usageStatus: "partial" } : { ...common, usageStatus: "complete" };
}

/**
 * The turn's main-agent usage. `agent_turn_completed.tokenUsage` includes Task children, so
 * theirs is subtracted. A zero or missing report (a turn cancelled before any call, or the
 * `process_exit` terminator) falls back to the session's growth since the turn began, which is
 * exact whenever the session total at turn start is known: a fresh session starts at zero.
 * Only a loaded session whose total was never seen is `unavailable`.
 */
export function droidTurnTokenUsage(
  state: DroidMapperState,
  completion: DroidNotification | undefined,
): TurnTokenUsage {
  const child = droidTokenCounts(completion?.["childTokenUsage"]);
  const hasSubagents = child !== undefined && !isZeroDroidUsage(child);
  const reported = nonZero(droidTokenCounts(completion?.["tokenUsage"]));
  if (reported !== undefined) {
    const main = child === undefined ? undefined : subtractDroidCounts(reported, child);
    return turnUsage(main?.counts ?? reported, hasSubagents, main?.clamped ?? false);
  }
  if (state.turnBaseline !== undefined && state.sessionUsage !== undefined) {
    const growth = subtractDroidCounts(state.sessionUsage, state.turnBaseline);
    return turnUsage(growth.counts, hasSubagents, growth.clamped);
  }
  return { usageScope: "main_agent", usageStatus: "unavailable", hasSubagents };
}

const addCounts = (left: number | undefined, right: number | undefined): number | undefined =>
  left === undefined ? right : right === undefined ? left : left + right;

/**
 * A t3 turn that folded in a steer sees one completion per native turn Droid ran for it; the
 * turn's usage is their sum, and any gap makes the sum partial.
 */
function addTurnUsage(previous: TurnTokenUsage | undefined, next: TurnTokenUsage): TurnTokenUsage {
  if (previous === undefined) return next;
  const optional = (
    key: "cachedInputTokens" | "cacheCreationTokens" | "reasoningTokens",
  ): Partial<Record<typeof key, number>> => {
    const sum = addCounts(previous[key], next[key]);
    return sum === undefined ? {} : { [key]: sum };
  };
  const common = {
    usageScope: "main_agent",
    hasSubagents: previous.hasSubagents || next.hasSubagents,
    ...optional("cachedInputTokens"),
    ...optional("cacheCreationTokens"),
    ...optional("reasoningTokens"),
  } as const;
  if (previous.usageStatus === "complete" && next.usageStatus === "complete") {
    return {
      ...common,
      usageStatus: "complete",
      inputTokens: previous.inputTokens + next.inputTokens,
      outputTokens: previous.outputTokens + next.outputTokens,
    };
  }
  const inputTokens = addCounts(previous.inputTokens, next.inputTokens);
  const outputTokens = addCounts(previous.outputTokens, next.outputTokens);
  const known = inputTokens !== undefined || outputTokens !== undefined;
  return {
    ...common,
    usageStatus: known ? "partial" : "unavailable",
    ...(inputTokens === undefined ? {} : { inputTokens }),
    ...(outputTokens === undefined ? {} : { outputTokens }),
  };
}

/**
 * Completion usage keeps the last call's context size and refreshes the session totals. With no
 * per-call report to build on, the session totals stand in for the context size, and a session
 * known to be at zero reports exactly zero.
 */
function completionContextUsage(
  state: DroidMapperState,
  ctx: DroidEventContext,
  notification: DroidNotification,
): ThreadTokenUsageSnapshot | undefined {
  const cumulative = nonZero(droidTokenCounts(notification["cumulativeTokenUsage"]));
  const previous = state.contextUsage;
  if (previous !== undefined) {
    if (cumulative === undefined) return undefined;
    const refreshed = withTotals(previous, cumulative);
    return sameSnapshot(previous, refreshed) ? undefined : refreshed;
  }
  const totals = cumulative ?? nonZero(droidTokenCounts(notification["tokenUsage"]));
  if (totals !== undefined) {
    return { usedTokens: processedTokens(totals), ...maxTokensOf(ctx), ...totalsFields(totals) };
  }
  return state.sessionUsage !== undefined && isZeroDroidUsage(state.sessionUsage)
    ? { usedTokens: 0, ...maxTokensOf(ctx) }
    : undefined;
}

/**
 * Reports usage only; the adapter builds `turn.terminal` from the reason and the settled
 * items (`settleDroidTurn`).
 */
function mapTurnCompleted(
  state: DroidMapperState,
  ctx: DroidEventContext,
  notification: DroidNotification,
  now: DateTime.Utc,
): DroidMapResult {
  const turnTokenUsage = addTurnUsage(
    state.turnTokenUsage,
    droidTurnTokenUsage(state, notification),
  );
  const snapshot = completionContextUsage(state, ctx, notification);
  const cumulative = nonZero(droidTokenCounts(notification["cumulativeTokenUsage"]));
  const sessionUsage = cumulative ?? state.sessionUsage;
  return {
    state: {
      ...state,
      turnTokenUsage,
      // A follow-on native turn measures its growth from here, not from the t3 turn's start.
      turnBaseline: sessionUsage,
      sessionUsage,
      contextUsage: snapshot ?? state.contextUsage,
    },
    events: snapshot === undefined ? [] : [threadUsageEvent(ctx, snapshot, now)],
  };
}

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

export type DroidCompactionStatus = "running" | "completed" | "failed" | "cancelled";

const COMPACTION_TITLE: Readonly<Record<DroidCompactionStatus, string>> = {
  running: "Compacting context...",
  completed: "Context compacted",
  failed: "Context compaction failed",
  cancelled: "Context compaction stopped",
};

/**
 * The compaction item of a compaction turn and its node. The adapter asks for the compaction and
 * owns when it settles, so the same `ordinal` and `startedAt` go with every status of one run.
 */
export function droidCompactionEvents(
  ctx: DroidEventContext,
  compaction: {
    readonly ordinal: number;
    readonly status: DroidCompactionStatus;
    readonly startedAt: DateTime.Utc;
  },
  now: DateTime.Utc,
): ReadonlyArray<ProviderAdapterV2Event> {
  const { ordinal, status, startedAt } = compaction;
  const nativeItemId = `compaction:${ctx.providerTurn.id}:${ordinal}`;
  const completedAt = status === "running" ? null : now;
  return [
    nodeEvent(ctx, nativeItemId, "system", status, startedAt, completedAt),
    {
      type: "turn_item.updated",
      driver: DROID_DRIVER,
      turnItem: {
        ...itemBase(ctx, nativeItemId, ordinal, startedAt, now),
        status,
        title: COMPACTION_TITLE[status],
        completedAt,
        type: "compaction",
        driver: DROID_DRIVER,
      },
    },
  ];
}

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

/**
 * Maps one Droid notification. Content, tool, and usage notifications map here; lifecycle and
 * request notifications (`settings_updated`, `error`, `permission_resolved`, ...) are the
 * adapter's and produce no events. An unrecognized type returns `unknownType` so it is logged
 * and dropped.
 */
export function mapDroidNotification(
  state: DroidMapperState,
  ctx: DroidEventContext,
  notification: DroidNotification,
  now: DateTime.Utc,
): DroidMapResult {
  switch (notification.type) {
    case "create_message":
      return mapCreateMessage(state, ctx, notification, now);
    case "assistant_text_delta":
      return mapTextDelta(state, ctx, notification, "text", now);
    case "thinking_text_delta":
      return mapTextDelta(state, ctx, notification, "thinking", now);
    case "assistant_text_complete":
      return mapTextComplete(state, ctx, notification, "text", now);
    case "thinking_text_complete":
      return mapTextComplete(state, ctx, notification, "thinking", now);
    case "assistant_message_retracted":
      return mapRetraction(state, ctx, notification, now);
    case "tool_call":
      return mapToolCall(state, ctx, notification, now);
    case "tool_progress_update":
      return mapToolProgress(state, ctx, notification);
    case "tool_result":
      return mapToolResult(state, ctx, notification, now);
    case "session_token_usage_changed":
      return mapTokenUsage(state, ctx, notification, now);
    case "agent_turn_completed":
      return mapTurnCompleted(state, ctx, notification, now);
    default:
      return isDroidNotificationType(notification.type)
        ? unchanged(state)
        : { state, events: [], unknownType: notification.type };
  }
}

/**
 * Closes what a finished turn left open: surfaced streaming items complete with their text, and
 * tools without a result end `interrupted` or `cancelled` with the turn, otherwise `failed`.
 * Every terminal path runs this before `turn.terminal`.
 */
export function settleDroidTurn(
  state: DroidMapperState,
  ctx: DroidEventContext,
  outcome: "completed" | "failed" | "interrupted" | "cancelled",
  now: DateTime.Utc,
): DroidMapResult {
  const events: Array<ProviderAdapterV2Event> = [];
  const blocks = new Map(state.blocks);
  for (const [key, block] of state.blocks) {
    const completed = completeBlock(ctx, block, now);
    events.push(...completed.events);
    blocks.set(key, completed.block);
  }
  const status: ToolSettleStatus =
    outcome === "interrupted" || outcome === "cancelled" ? outcome : "failed";
  let next: DroidMapperState = { ...state, blocks };
  for (const tool of state.tools.values()) {
    if (tool.completed) continue;
    const settled = updateTool(next, ctx, tool, now, { status });
    events.push(...settled.events);
    next = settled.state;
  }
  return { state: next, events };
}
