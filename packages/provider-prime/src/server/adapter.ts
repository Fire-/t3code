import * as HostProcess from "@t3tools/shared/HostProcess";
import { AgentScope } from "@t3tools/shared/AgentScope";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { readToolOutputImage } from "@t3tools/shared/toolOutput";
import * as Predicate from "effect/Predicate";
import {
  ProviderDriverKind,
  type ChatAttachment,
  type ModelSelection,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderFailure,
  OrchestrationV2ProviderGoal,
  type OrchestrationV2ProviderRef,
  type OrchestrationV2ProviderRetry,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  type OrchestrationV2UserInputQuestion,
  type ProviderApprovalDecision,
  type ProviderInstanceId,
  type OrchestrationV2ProviderTurnTokenUsage,
} from "@t3tools/contracts";
import { PrimeSettings } from "../settings.ts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Option from "effect/Option";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import { mcpToolPresentation } from "@t3tools/provider-core/server/mcpToolPresentation";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import { primeSessionCommandName } from "./commands.ts";
import {
  isPrimeBusyRejection,
  primeAgentEndErrorMessage,
  primeAgentEndFailed,
  primeAutonomousContinuation,
  primeAutonomousNotice,
  primeCustomMessageHidden,
  primeCustomMessageNotice,
  primeContentText as contentText,
  primeDiscardedHistory,
  primeForkBoundary,
  primeForkMessages,
  primeErrorMessage,
  primeHeartbeatNotification,
  primeIpythonFileChange,
  primeIpythonUpdate,
  primeMessageLead,
  primeProviderGoal,
  primeRlmChildSignature,
  primeRlmChildView,
  primeSessionCommandFailure,
  primeSessionForkReader,
  primeToolItem,
  primeTurnFingerprint,
  primeUserEntryHash,
  PRIME_STRANDED_LEADS,
  type PrimeAutonomousContinuation,
  type PrimeForkMessage,
  type PrimeStrandedLead,
  type PrimeTurnLead,
} from "./events.ts";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
  type ProviderAdapterDriverCreateInput,
} from "@t3tools/provider-core/server/adapterDriver";
import {
  makeProviderFailure,
  makeProviderRetryTurnItem,
} from "@t3tools/provider-core/server/failure";
import { turnScopedSelectionTransition } from "@t3tools/provider-core/server/selectionTransition";
import {
  classifyPrimeEvent,
  decodePrimeState,
  isPrimeStateIdle,
  makePrimeRpcConnection,
  parsePrimeModelSlug,
  PrimeRpcError,
  type PrimeRpcTimeoutError,
  primeRecordField as recordField,
  primeRecordNumber as recordNumber,
  primeRecordString as recordString,
  type PrimeRpcConnection,
  type PrimeRpcRecord,
  type PrimeSessionActionSnapshot,
} from "./rpc.ts";
import {
  buildPrimeRpcLaunch,
  materializePrimeT3McpExtension,
  resolvePrimeLaunchArgs,
} from "./mcpInjection.ts";
import { PRIME_FILE_CHANGE_TOOLS } from "./mcpExtensionSource.ts";

export const PRIME_PROVIDER = ProviderDriverKind.make("prime-agent");
const PRIME_DRIVER_KIND = PRIME_PROVIDER;
const DEFAULT_PRIME_SETTINGS = Schema.decodeSync(PrimeSettings)({});

/**
 * Sentinel model slug meaning "do not call set_model": Prime resolves the model
 * from the user's own settings.json (`defaultProvider`/`defaultModel`).
 */
const PRIME_INHERIT_MODEL_SLUG = "default";

const STREAM_FLUSH_MS = 50;
const PRIME_REQUEST_TIMEOUT_MS = 15_000;
const PRIME_SESSION_TIMEOUT_MS = 60_000;
const PRIME_UNSOLICITED_ACTIVITY_ERROR =
  "Prime started agent work outside an active T3 turn. The session was stopped to prevent invisible tool execution.";
// A failed or timed-out get_state is retried as error recovery, not polling:
// a read that answers busy is never repeated on a timer.
const SETTLE_PROBE_MAX_ATTEMPTS = 3;
const SETTLE_PROBE_RETRY_DELAY = Duration.millis(100);

const providerGoalsEqual = Schema.toEquivalence(Schema.NullOr(OrchestrationV2ProviderGoal));

/** A cron job (modes/agent-connection/types.ts AgentCronJob) that will fire again. */
const hasActiveJob = (entries: unknown, jobField?: string): boolean =>
  Array.isArray(entries) &&
  entries.some(
    (entry) =>
      recordString(jobField === undefined ? entry : recordField(entry, jobField), "status") ===
      "active",
  );

// Prime keeps a disconnected client's worker alive for a 30 s grace period, and
// switch_session fails with SessionAlreadyActiveError (core/session-lease.ts)
// until that worker releases the session lease.
const PRIME_SESSION_ACTIVE_RETRY = Schedule.spaced(Duration.seconds(1)).pipe(
  Schedule.upTo({ duration: Duration.seconds(45) }),
);
const PRIME_SESSION_ALREADY_ACTIVE = /Session is already active in (.+?): /;

/**
 * ProviderAdapterV2Error is a closed union in provider-core, so this travels as
 * the cause of ProviderAdapterResumeThreadError.
 */
class PrimeSessionLeaseConflictError extends Schema.TaggedError<PrimeSessionLeaseConflictError>()(
  "PrimeSessionLeaseConflictError",
  { sessionId: Schema.String },
) {
  override get message(): string {
    return `Prime session ${this.sessionId} is still open in another prime-agent process. Stop it with \`prime-agent stop ${this.sessionId}\` and try again.`;
  }
}

const PRIME_STRANDED_HISTORY: Record<PrimeStrandedLead, string> = {
  heartbeat: "a heartbeat continuation",
  goal: "a goal run",
  autonomous: "an autonomous-mode update",
  notice: "a Prime notice",
  output: "agent output",
};

/**
 * Prime's RPC `fork` cuts only before a user entry (modes/rpc/rpc-mode.ts
 * "fork", core/agent-session-runtime.ts fork), and RPC mode has no command
 * that cuts after any other entry. Travels as the cause of
 * ProviderAdapterRollbackThreadError or ProviderAdapterForkThreadError.
 */
class PrimeRollbackBoundaryError extends Schema.TaggedError<PrimeRollbackBoundaryError>()(
  "PrimeRollbackBoundaryError",
  {
    operation: Schema.Literals(["rollback", "fork"]),
    turnOrdinal: Schema.Number,
    lead: Schema.Literals(PRIME_STRANDED_LEADS),
  },
) {
  override get message(): string {
    const turn = `turn ${this.turnOrdinal} (${PRIME_STRANDED_HISTORY[this.lead]})`;
    const reason = `that turn added to the conversation before any user message, and Prime can only cut its session before a user message`;
    return this.operation === "rollback"
      ? `Prime cannot roll back past ${turn}: ${reason}. The thread is unchanged.`
      : `Prime cannot fork from before ${turn}: ${reason}. The source thread is unchanged.`;
  }
}

function primeSessionAlreadyActive(
  error: PrimeRpcError | PrimeRpcTimeoutError,
): { readonly activeSessionId: string | undefined } | undefined {
  if (error._tag !== "PrimeRpcError") return undefined;
  const match = error.detail?.match(PRIME_SESSION_ALREADY_ACTIVE);
  if (match === null || match === undefined) return undefined;
  return { activeSessionId: match[1] === "another process" ? undefined : match[1] };
}

/** Prime's agent_end has no `aborted` flag; an aborted run ends with an aborted assistant message. */
function primeRunAborted(agentEnd: PrimeRpcRecord): boolean {
  const messages = agentEnd["messages"];
  if (!Array.isArray(messages)) return false;
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    if (recordString(messages[index], "role") === "assistant") {
      return recordString(messages[index], "stopReason") === "aborted";
    }
  }
  return false;
}

const PRIME_MODEL_ERROR = "Prime reported a model error.";
const isPrimeRpcError = Schema.is(PrimeRpcError);

/** The transport's own diagnosis (for example the exit code) says why Prime died. */
function primeExitMessage(cause: Cause.Cause<unknown>): string {
  const error = Cause.squash(cause);
  const detail = isPrimeRpcError(error) ? error.detail : undefined;
  return detail === undefined
    ? "Prime process exited unexpectedly."
    : `Prime process exited unexpectedly: ${detail}.`;
}

function primeSessionIdFromPath(sessionPath: string): string {
  const name = sessionPath.replace(/\\/g, "/").split("/").pop() ?? sessionPath;
  return name.endsWith(".jsonl") ? name.slice(0, -".jsonl".length) : name;
}
const PrimeProviderCapabilitiesV2 = {
  runtimePolicy: { enforcement: "client-boundary" },
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: true,
    supportsProviderSwitchingViaHandoff: true,
    // Mode changes restart this process so the injected permission hook gets
    // one immutable policy for its whole lifetime.
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: true,
    canRollbackThread: true,
    canForkThread: true,
    canForkFromTurn: true,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: false,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: true,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: true,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: true,
    streamsReasoning: true,
    streamsToolOutput: true,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: true,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: true,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: true,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: true,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: true,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: true,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: true,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: true,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: true,
    // CommandPolicy.ensureRollback requires the snapshot whenever provider
    // rollback is enabled; rollbackThread returns the updated provider thread.
    providerRollbackReturnsSnapshot: true,
    providerCanReadConversationSnapshot: true,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "weak",
    nativeItemIds: "strong",
    nativeRequestIds: "strong",
  },
} satisfies OrchestrationV2ProviderCapabilities;

export interface PrimeAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly settings: PrimeSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly continuationRequests?: {
    readonly offer: (
      request: ProviderContinuationRequests.ProviderContinuationRequest,
    ) => Effect.Effect<void>;
  };
}

const MAX_STRUCTURED_TOOL_OUTPUT_BYTES = 64 * 1024;

function isBoundedStructuredToolValue(value: unknown): boolean {
  let bytes = MAX_STRUCTURED_TOOL_OUTPUT_BYTES;
  let nodes = 4_096;
  const visit = (entry: unknown, depth: number): boolean => {
    if (depth > 32 || --nodes < 0 || bytes < 0) return false;
    if (Array.isArray(entry)) {
      bytes -= 2 + entry.length;
      return entry.every((item) => visit(item, depth + 1));
    }
    if (Predicate.isObject(entry)) {
      bytes -= 2;
      for (const key in entry) {
        if (!Object.hasOwn(entry, key)) continue;
        if (key.length > bytes) return false;
        bytes -= Buffer.byteLength(JSON.stringify(key), "utf8") + 2;
        if (!visit(entry[key], depth + 1)) return false;
      }
      return bytes >= 0;
    }
    if (typeof entry === "string" && entry.length > bytes) return false;
    bytes -= Buffer.byteLength(JSON.stringify(entry) ?? "null", "utf8");
    return bytes >= 0;
  };
  return visit(value, 0);
}

function primeToolOutput(result: unknown, toolName: string): unknown {
  const rawContent = recordField(result, "content");
  const content = Array.isArray(rawContent)
    ? rawContent
    : typeof rawContent === "string"
      ? [{ type: "text", text: rawContent }]
      : [];
  let structured = recordField(result, "structuredContent");
  const details = recordField(result, "details");
  const isMcpResult =
    toolName.startsWith("mcp__") &&
    recordString(details, "server") !== undefined &&
    recordString(details, "tool") !== undefined &&
    Array.isArray(recordField(structured, "content"));
  if (isMcpResult) {
    structured = recordField(structured, "structuredContent");
  }
  if (typeof structured === "string" && structured === contentText(content)) {
    structured = undefined;
  }
  // Native read repeats its image and first text note as structured output.
  // Drop only that exact mirror; an extension's extra metadata stays distinct.
  if (toolName === "read" && Predicate.isObject(structured)) {
    const image = readToolOutputImage(structured);
    const note =
      recordString(
        content.find((block) => recordField(block, "type") === "text"),
        "text",
      ) ?? "";
    if (
      image !== null &&
      Object.keys(structured).every((key) => ["type", "data", "mimeType", "note"].includes(key)) &&
      recordString(structured, "note") === note &&
      content.some((block) => {
        const match = readToolOutputImage(block);
        return match !== null && match.mimeType === image.mimeType && match.data === image.data;
      })
    ) {
      structured = undefined;
    }
  }
  if (structured !== undefined && !isBoundedStructuredToolValue(structured)) {
    return {
      content: [
        ...content,
        {
          type: "text",
          text: "Structured output omitted because it exceeds the stored result limit.",
        },
      ],
    };
  }
  if (
    !isMcpResult &&
    structured === undefined &&
    content.every((block) => recordField(block, "type") === "text")
  ) {
    const text = contentText(content);
    return text.length > 0 ? text : undefined;
  }
  return {
    content,
    ...(structured === undefined ? {} : { structuredContent: structured }),
  };
}

function providerRef(
  nativeId: string,
  strength: "strong" | "weak" = "strong",
): OrchestrationV2ProviderRef {
  return { driver: PRIME_PROVIDER, nativeId, strength };
}

const PRIME_THINKING_LEVELS = new Set(["off", "minimal", "low", "medium", "high", "xhigh", "max"]);

interface PrimeStreamItemState {
  readonly nativeItemId: string;
  readonly kind: "assistant_message" | "reasoning";
  text: string;
  completed: boolean;
  flushScheduled: boolean;
  readonly startedAt: DateTime.Utc;
}

type PrimeCompactionStatus = "running" | "completed" | "failed" | "cancelled";

interface PrimeCompactionState {
  readonly nativeItemId: string;
  readonly startedAt: DateTime.Utc;
}

interface PrimeProviderRetryState {
  readonly retry: OrchestrationV2ProviderRetry;
  readonly failure: OrchestrationV2ProviderFailure;
  readonly startedAt: DateTime.Utc;
  readonly itemOrdinal: number;
}

function compactionTitle(status: PrimeCompactionStatus): string {
  switch (status) {
    case "running":
      return "Compacting context...";
    case "completed":
      return "Context compacted";
    case "failed":
      return "Context compaction failed";
    case "cancelled":
      return "Context compaction stopped";
  }
}

interface ActivePrimeTurn {
  readonly turnInput: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly providerTurn: OrchestrationV2ProviderTurn;
  readonly startedAt: DateTime.Utc;
  readonly itemOrdinals: Map<string, number>;
  nextItemOrdinal: number;
  messageOrdinal: number;
  readonly streamItems: Map<string, PrimeStreamItemState>;
  readonly toolArgs: Map<string, unknown>;
  /** Accumulated ipython stream chunks, by tool call. */
  readonly toolOutput: Map<string, string>;
  readonly toolStartedAt: Map<string, DateTime.Utc>;
  /** Tool calls started without a matching end, by tool name, so a terminal can close them. */
  readonly openToolCalls: Map<string, string>;
  interrupted: boolean;
  nativeAborted: boolean;
  sawAgentActivity: boolean;
  adoptedWake: boolean;
  readonly promptMayBeCommandOnly: boolean;
  /**
   * Prime's built-in session commands (`compact`, `refine`, ...) this turn's
   * prompt or accepted steers named. Only these may fail the turn: Prime also
   * reports self-started refinement through the same frames.
   */
  readonly sessionCommands: Set<string>;
  /** Hashes of the user entries this turn added to the session, in order. */
  readonly userEntryHashes: Array<string>;
  /** The first model-visible message this turn added; rollback can cut cleanly only before a user entry. */
  historyLead: Exclude<PrimeTurnLead, "none"> | null;
  latestCompactionAfterTokens: number | null;
  lastLiveUsedTokens: number | null;
  /** Bumped by every transition that can make an in-flight `get_state` probe stale. */
  settleProbeGeneration: number;
  /** Cleared by a later `agent_start`: Prime runs goal and queued work as new runs. */
  sawAgentEnd: boolean;
  sessionCommandSeen: boolean;
  /**
   * A session command (`/goal status`, `/reload`) can finish without any
   * agent run, so its active action clearing is a settle trigger of its own.
   */
  sessionCommandDone: boolean;
  sawCompaction: boolean;
  activeCompaction: PrimeCompactionState | null;
  activeProviderRetry: PrimeProviderRetryState | null;
  /**
   * Between `auto_retry_start` and `auto_retry_end`. Prime sends the retry
   * after the failed run's `agent_end` and reports an idle `get_state` (no
   * active action) while it waits out the delay, so only this keeps the
   * turn open (core/agent-session.ts `_retryAfterDelay`).
   */
  autoRetryPending: boolean;
  failure: ReturnType<typeof makeProviderFailure> | null;
  rejectedPromptFailure: ReturnType<typeof makeProviderFailure> | null;
}

interface PendingPrimePrompt {
  readonly nativeRequestId: string;
  readonly method: "select" | "confirm" | "input" | "editor";
  readonly questionId: string;
  readonly approvalKey: string;
  readonly expiresAt: number | null;
  expirationFiber: Fiber.Fiber<void> | null;
  runtimeRequest: OrchestrationV2RuntimeRequest;
  readonly node: OrchestrationV2ExecutionNode;
  readonly turnItem: OrchestrationV2TurnItem;
}

function primeApprovalRequestKind(title: string): "command" | "file-change" {
  const toolName = /^Allow (\S+)\?$/.exec(title)?.[1];
  return toolName !== undefined &&
    (PRIME_FILE_CHANGE_TOOLS as ReadonlyArray<string>).includes(toolName)
    ? "file-change"
    : "command";
}

interface PrimeThreadState {
  providerThread: OrchestrationV2ProviderThread;
  activeTurn: ActivePrimeTurn | null;
}

interface PrimeRlmChildState {
  readonly turn: ActivePrimeTurn;
  readonly nativeId: string;
  readonly startedAt: DateTime.Utc;
  signature: string;
  terminal: boolean;
}

interface PrimeWake {
  readonly state: PrimeThreadState;
  readonly events: Array<PrimeRpcRecord>;
}

export const makePrimeAdapterV2 = Effect.fn("makePrimeAdapterV2")(function* (
  options: PrimeAdapterV2Options,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const agentScope = yield* AgentScope;
  const fileSystem = yield* FileSystem.FileSystem;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const host = yield* ProviderHost.ProviderHost;
  const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
  const { continuationRequests } = options;

  const protocolError = (detail: string, payload?: unknown) =>
    new ProviderAdapter.ProviderAdapterProtocolError({
      driver: PRIME_PROVIDER,
      detail,
      ...(payload === undefined ? {} : { payload }),
    });

  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: PRIME_PROVIDER,
    getCapabilities: () => Effect.succeed(PrimeProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed(turnScopedSelectionTransition()),
    openSession: Effect.fn("PrimeAdapterV2.openSession")(function* (
      input: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
    ) {
      const scope = yield* Effect.scope;
      const cwd = input.runtimePolicy.cwd ?? host.paths.cwd;
      const mcpSession = yield* mcpSessions.read(input.threadId);
      const provideCacheFs = <A, E>(effect: Effect.Effect<A, E, FileSystem.FileSystem>) =>
        effect.pipe(
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.mapError(
            (cause) =>
              new ProviderAdapter.ProviderAdapterOpenSessionError({
                driver: PRIME_PROVIDER,
                providerSessionId: input.providerSessionId,
                cause,
              }),
          ),
        );
      // The extension owns both the optional MCP bridge and Prime's permission
      // hook. Materialize it even when this session has no MCP credential so
      // Supervised never silently degrades to unrestricted tool execution.
      const extensionPath = yield* provideCacheFs(
        materializePrimeT3McpExtension(host.paths.providerStatusCacheDir),
      );
      const resolvedLaunchArgs = resolvePrimeLaunchArgs(options.settings.launchArgs);
      if (!resolvedLaunchArgs.ok) {
        return yield* protocolError(resolvedLaunchArgs.message);
      }
      const launch = buildPrimeRpcLaunch({
        launchArgs: resolvedLaunchArgs.args,
        environment: options.environment,
        mcpSession,
        extensionPath,
        runtimeMode: input.runtimePolicy.runtimeMode,
      });
      const scopedLaunch = yield* agentScope.wrap({
        command: options.settings.binaryPath || "prime-agent",
        args: launch.args,
        name: "prime-agent",
        threadId: input.threadId,
        env: launch.env,
      });
      const connection: PrimeRpcConnection = yield* makePrimeRpcConnection({
        command: scopedLaunch.command,
        args: scopedLaunch.args,
        cwd,
        env: launch.env,
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.mapError(
          (cause) =>
            new ProviderAdapter.ProviderAdapterOpenSessionError({
              driver: PRIME_PROVIDER,
              providerSessionId: input.providerSessionId,
              cause,
            }),
        ),
      );
      const now = yield* DateTime.now;
      let sessionEntity: OrchestrationV2ProviderSession = {
        id: input.providerSessionId,
        driver: PRIME_PROVIDER,
        providerInstanceId: options.instanceId,
        status: "ready",
        cwd,
        model: input.modelSelection.model,
        capabilities: PrimeProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const events = yield* Queue.unbounded<
        ProviderAdapter.ProviderAdapterV2Event,
        ProviderAdapter.ProviderAdapterV2Error | Cause.Done
      >();
      const pendingPrompts = new Map<string, PendingPrimePrompt>();
      const sessionApprovals = new Set<string>();
      // Answering a dialog and terminalizing a turn both publish lifecycle
      // events. Prime can settle immediately after `extension_ui_response`, so
      // serialize the two paths to stop `turn.terminal` from overtaking the
      // dialog's own resolution updates.
      const sessionEventPermit = yield* Semaphore.make(1);
      let threadState: PrimeThreadState | null = null;
      let registrationAttempted = false;
      let lastNativeThreadId: string | null = null;
      let stopRequested = false;
      let unsolicitedActivityDetected = false;
      let pendingWake: PrimeWake | null = null;
      const rlmChildren = new Map<string, PrimeRlmChildState>();
      // Prime emits session_action_update only on change, so the latest
      // snapshot is session state that outlives any single turn.
      let latestSessionActions: PrimeSessionActionSnapshot | null = null;
      // The orchestrator drops provider_thread.updated outside a run, so a goal
      // Prime reports between runs is kept for the next registration or turn.
      // `undefined` means Prime has not reported one for this session.
      let latestGoal: OrchestrationV2ProviderGoal | null | undefined = undefined;
      // Prime reports schedules and heartbeats only when asked, so this is
      // refreshed at each settle and pins the idle release in between.
      let activeBackgroundJobs = false;
      let rollbackBarrier: Deferred.Deferred<
        void,
        ProviderAdapter.ProviderAdapterProtocolError
      > | null = null;
      let closed = false;
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          closed = true;
          pendingWake = null;
          if (rollbackBarrier !== null)
            yield* Deferred.fail(
              rollbackBarrier,
              protocolError("Prime session closed during rollback"),
            );
        }),
      );
      let appliedModel: string | null = null;
      let appliedThinking: string | null = null;
      // Recorded when sent, not when accepted: a name Prime rejects would be
      // rejected again before every prompt.
      let requestedSessionName: string | null = null;
      const outOfTurnExtensionErrors: Array<PrimeRpcRecord> = [];
      // Prime's own configured defaults, captured from the first `get_state` so
      // that selecting the displayed "Prime default" again can restore them. Prime
      // has no "unset" commands, so the baselines have to be replayed
      // explicitly.
      let baselineModel: { provider: string; modelId: string } | null = null;
      let baselineThinking: string | null = null;
      let contextWindow: number | null = null;
      const modelContextWindows = new Map<string, number>();
      let modelsDiscovered = false;
      // Prompt responses carry no id. Keep their session-wide send order and
      // owner so a late ack from a settled turn cannot affect the next turn.
      const pendingPromptResponses: Array<{
        readonly providerTurnId: OrchestrationV2ProviderTurn["id"];
        readonly kind: "turn_start" | "steer";
        readonly sessionCommand?: string | undefined;
      }> = [];
      const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
        Queue.offer(events, event).pipe(Effect.asVoid);

      const updateProviderSession = (
        status: OrchestrationV2ProviderSession["status"],
        lastError: string | null = sessionEntity.lastError,
      ) =>
        Effect.gen(function* () {
          const updatedAt = yield* DateTime.now;
          sessionEntity = { ...sessionEntity, status, lastError, updatedAt };
          yield* emit({
            type: "provider_session.updated",
            driver: PRIME_PROVIDER,
            providerSession: sessionEntity,
          });
        });

      const updateProviderThread = (
        state: PrimeThreadState,
        patch: Partial<OrchestrationV2ProviderThread>,
      ) =>
        Effect.gen(function* () {
          const updatedAt = yield* DateTime.now;
          state.providerThread = { ...state.providerThread, ...patch, updatedAt };
          yield* emit({
            type: "provider_thread.updated",
            driver: PRIME_PROVIDER,
            providerThread: state.providerThread,
          });
        });

      const goalPatch = (state: PrimeThreadState): Partial<OrchestrationV2ProviderThread> =>
        latestGoal === undefined ||
        providerGoalsEqual(state.providerThread.goal ?? null, latestGoal)
          ? {}
          : { goal: latestGoal };

      const itemOrdinal = (turn: ActivePrimeTurn, nativeItemId: string): number => {
        const existing = turn.itemOrdinals.get(nativeItemId);
        if (existing !== undefined) return existing;
        const ordinal = turn.nextItemOrdinal++;
        turn.itemOrdinals.set(nativeItemId, ordinal);
        return ordinal;
      };

      const request = (record: PrimeRpcRecord, timeoutMs = PRIME_REQUEST_TIMEOUT_MS) =>
        connection.request(record, timeoutMs);

      const nonNegativeInteger = (input: unknown, key: string): number | undefined => {
        const value = recordNumber(input, key);
        return value === undefined ? undefined : Math.max(0, Math.trunc(value));
      };

      const rememberModelContextWindow = (model: unknown): number | null => {
        const provider = recordString(model, "provider");
        const id = recordString(model, "id");
        const capacity = nonNegativeInteger(model, "contextWindow");
        if (provider !== undefined && id !== undefined && capacity !== undefined && capacity > 0) {
          modelContextWindows.set(`${provider}/${id}`, capacity);
          return capacity;
        }
        return null;
      };

      const lifecycleRequest = (record: PrimeRpcRecord) =>
        request(record, PRIME_SESSION_TIMEOUT_MS).pipe(
          // A local timeout does not cancel Prime's lifecycle hook. Retire the
          // process before fallback can race its eventual switch/new-session.
          Effect.tapError((error) =>
            Effect.logWarning("Prime session lifecycle request failed", {
              providerSessionId: input.providerSessionId,
              operation: record["type"],
              errorTag: error._tag,
            }),
          ),
          Effect.catchTags({
            PrimeRpcTimeoutError: (error) =>
              connection.terminate.pipe(Effect.andThen(Effect.fail(error))),
          }),
          Effect.onInterrupt(() => connection.terminate),
        );

      const resumeSession = (sessionPath: string) =>
        lifecycleRequest({ type: "switch_session", sessionPath }).pipe(
          Effect.retry({
            schedule: PRIME_SESSION_ACTIVE_RETRY,
            while: (error) => primeSessionAlreadyActive(error) !== undefined,
          }),
          Effect.catchTag(
            "PrimeRpcError",
            (error): Effect.Effect<never, PrimeRpcError | PrimeSessionLeaseConflictError> => {
              const active = primeSessionAlreadyActive(error);
              if (active === undefined) return Effect.fail(error);
              const conflict = new PrimeSessionLeaseConflictError({
                sessionId: active.activeSessionId ?? primeSessionIdFromPath(sessionPath),
              });
              // ProviderTurnStartService logs only the error tag before it
              // falls back to a fresh native session, so say why here.
              return Effect.logWarning(conflict.message, {
                providerSessionId: input.providerSessionId,
                sessionId: conflict.sessionId,
              }).pipe(Effect.andThen(Effect.fail(conflict)));
            },
          ),
        );

      const tokenUsageFromStats = (
        stats: unknown,
        fallbackUsedTokens: number | null,
        updatedAt: DateTime.Utc,
      ): OrchestrationV2ProviderTurnTokenUsage | undefined => {
        const contextUsage = recordField(stats, "contextUsage");
        const maxTokens = nonNegativeInteger(contextUsage, "contextWindow");
        const usedTokens =
          nonNegativeInteger(contextUsage, "tokens") ?? fallbackUsedTokens ?? undefined;
        if (usedTokens === undefined || maxTokens === undefined || maxTokens === 0)
          return undefined;

        const totals = recordField(stats, "tokens");
        const inputTokens = nonNegativeInteger(totals, "input");
        const cachedInputTokens = nonNegativeInteger(totals, "cacheRead");
        const outputTokens = nonNegativeInteger(totals, "output");
        return {
          usedTokens,
          maxTokens,
          ...(inputTokens === undefined ? {} : { inputTokens }),
          ...(cachedInputTokens === undefined ? {} : { cachedInputTokens }),
          ...(outputTokens === undefined ? {} : { outputTokens }),
          updatedAt: DateTime.formatIso(updatedAt),
        };
      };

      const readTokenUsage = (fallbackUsedTokens: number | null, updatedAt: DateTime.Utc) =>
        request({ type: "get_session_stats" }, 2_000).pipe(
          Effect.map((stats) => tokenUsageFromStats(stats, fallbackUsedTokens, updatedAt)),
          Effect.orElseSucceed(() => undefined),
        );

      /**
       * Pi 0.84.2+ (and so Prime) attaches the current message's cumulative usage to every streaming update.
       */
      const reportLiveUsage = (turn: ActivePrimeTurn, usage: unknown) =>
        Effect.gen(function* () {
          const usedTokens = nonNegativeInteger(usage, "totalTokens");
          if (
            usedTokens === undefined ||
            usedTokens === 0 ||
            usedTokens === turn.lastLiveUsedTokens ||
            contextWindow === null ||
            contextWindow === 0
          ) {
            return;
          }
          turn.lastLiveUsedTokens = usedTokens;
          const inputTokens = nonNegativeInteger(usage, "input");
          const cachedInputTokens = nonNegativeInteger(usage, "cacheRead");
          const outputTokens = nonNegativeInteger(usage, "output");
          const updatedAt = yield* DateTime.now;
          yield* emit({
            type: "provider_turn.updated",
            driver: PRIME_PROVIDER,
            threadId: turn.turnInput.threadId,
            providerTurn: {
              ...turn.providerTurn,
              tokenUsage: {
                usedTokens,
                maxTokens: contextWindow,
                ...(inputTokens === undefined ? {} : { inputTokens }),
                ...(cachedInputTokens === undefined ? {} : { cachedInputTokens }),
                ...(outputTokens === undefined ? {} : { outputTokens }),
                updatedAt: DateTime.formatIso(updatedAt),
              },
            },
          });
        });

      const baseItemFields = (
        turn: ActivePrimeTurn,
        nativeItemId: string,
        startedAt: DateTime.Utc,
        updatedAt: DateTime.Utc,
      ) => ({
        id: idAllocator.derive.turnItemFromProviderItem({
          driver: PRIME_PROVIDER,
          nativeItemId,
        }),
        threadId: turn.turnInput.threadId,
        runId: turn.turnInput.runId,
        nodeId: idAllocator.derive.nodeFromProviderItem({
          driver: PRIME_PROVIDER,
          nativeItemId,
        }),
        providerThreadId: turn.turnInput.providerThread.id,
        providerTurnId: turn.providerTurn.id,
        nativeItemRef: providerRef(nativeItemId),
        parentItemId: null,
        ordinal: itemOrdinal(turn, nativeItemId),
        startedAt,
        updatedAt,
      });

      const emitItemNode = (
        turn: ActivePrimeTurn,
        nativeItemId: string,
        kind: OrchestrationV2ExecutionNode["kind"],
        status: OrchestrationV2ExecutionNode["status"],
        startedAt: DateTime.Utc,
        completedAt: DateTime.Utc | null,
      ) =>
        emit({
          type: "node.updated",
          driver: PRIME_PROVIDER,
          node: {
            id: idAllocator.derive.nodeFromProviderItem({
              driver: PRIME_PROVIDER,
              nativeItemId,
            }),
            threadId: turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            parentNodeId: turn.turnInput.rootNodeId,
            rootNodeId: turn.turnInput.rootNodeId,
            kind,
            status,
            countsForRun: false,
            providerThreadId: turn.turnInput.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            nativeItemRef: providerRef(nativeItemId),
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt,
            completedAt,
          },
        });

      const emitSystemNotice = Effect.fnUntraced(function* (
        turn: ActivePrimeTurn,
        nativeItemId: string,
        message: string,
      ) {
        const emittedAt = yield* DateTime.now;
        yield* emitItemNode(turn, nativeItemId, "system", "completed", emittedAt, emittedAt);
        yield* emit({
          type: "turn_item.updated",
          driver: PRIME_PROVIDER,
          turnItem: {
            ...baseItemFields(turn, nativeItemId, emittedAt, emittedAt),
            status: "completed",
            title: null,
            completedAt: emittedAt,
            type: "system_notice",
            message,
          },
        });
      });

      /** The agent wrote this prompt to itself, so it must never render as the user's words. */
      const emitAutonomousContinuation = Effect.fnUntraced(function* (
        turn: ActivePrimeTurn,
        continuation: PrimeAutonomousContinuation,
      ) {
        const emittedAt = yield* DateTime.now;
        const nativeItemId = `autonomous-continuation:${turn.providerTurn.id}:${turn.nextItemOrdinal}`;
        const messageId = idAllocator.derive.messageFromProviderItem({
          driver: PRIME_PROVIDER,
          nativeItemId,
        });
        yield* emit({
          type: "message.updated",
          driver: PRIME_PROVIDER,
          message: {
            id: messageId,
            threadId: turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            nodeId: turn.turnInput.rootNodeId,
            role: "user",
            text: continuation.text,
            attachments: [],
            streaming: false,
            createdBy: "agent",
            creationSource: "provider",
            createdAt: emittedAt,
            updatedAt: emittedAt,
          },
        });
        yield* emit({
          type: "turn_item.updated",
          driver: PRIME_PROVIDER,
          turnItem: {
            ...baseItemFields(turn, nativeItemId, emittedAt, emittedAt),
            nodeId: turn.turnInput.rootNodeId,
            createdBy: "agent",
            creationSource: "provider",
            status: "completed",
            title: continuation.title,
            completedAt: emittedAt,
            type: "user_message",
            messageId,
            inputIntent: "steer",
            text: continuation.text,
            attachments: [],
          },
        });
      });
      const emitProviderRetry = Effect.fnUntraced(function* (
        turn: ActivePrimeTurn,
        providerRetry: PrimeProviderRetryState,
        status: "running" | "completed" | "failed" | "interrupted" | "cancelled",
        updatedAt: DateTime.Utc,
      ) {
        yield* emit({
          type: "turn_item.updated",
          driver: PRIME_PROVIDER,
          turnItem: makeProviderRetryTurnItem({
            driver: PRIME_PROVIDER,
            threadId: turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            nodeId: turn.turnInput.rootNodeId,
            providerThreadId: turn.turnInput.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            itemOrdinal: providerRetry.itemOrdinal,
            failure: providerRetry.failure,
            retry: providerRetry.retry,
            status,
            startedAt: providerRetry.startedAt,
            updatedAt,
          }),
        });
      });

      const compactionNativeItemId = (turn: ActivePrimeTurn): string =>
        `compaction:${turn.providerTurn.id}:${turn.nextItemOrdinal}`;

      const emitCompaction = Effect.fnUntraced(function* (
        turn: ActivePrimeTurn,
        compaction: PrimeCompactionState,
        status: PrimeCompactionStatus,
        details: {
          readonly summary?: string;
          readonly beforeTokenCount?: number;
          readonly afterTokenCount?: number;
        } = {},
      ) {
        const emittedAt = yield* DateTime.now;
        const completedAt = status === "running" ? null : emittedAt;
        yield* emitItemNode(
          turn,
          compaction.nativeItemId,
          "system",
          status,
          compaction.startedAt,
          completedAt,
        );
        yield* emit({
          type: "turn_item.updated",
          driver: PRIME_PROVIDER,
          turnItem: {
            ...baseItemFields(turn, compaction.nativeItemId, compaction.startedAt, emittedAt),
            status,
            title: compactionTitle(status),
            completedAt,
            type: "compaction",
            driver: PRIME_PROVIDER,
            ...details,
          },
        });
      });

      const emitStreamItem = (
        turn: ActivePrimeTurn,
        item: PrimeStreamItemState,
        streaming: boolean,
      ) =>
        Effect.gen(function* () {
          const emittedAt = yield* DateTime.now;
          const base = baseItemFields(turn, item.nativeItemId, item.startedAt, emittedAt);
          yield* emitItemNode(
            turn,
            item.nativeItemId,
            item.kind,
            streaming ? "running" : "completed",
            item.startedAt,
            streaming ? null : emittedAt,
          );
          if (item.kind === "assistant_message") {
            const messageId = idAllocator.derive.messageFromProviderItem({
              driver: PRIME_PROVIDER,
              nativeItemId: item.nativeItemId,
            });
            yield* emit({
              type: "turn_item.updated",
              driver: PRIME_PROVIDER,
              turnItem: {
                ...base,
                status: streaming ? "running" : "completed",
                title: null,
                completedAt: streaming ? null : emittedAt,
                type: "assistant_message",
                messageId,
                text: item.text,
                streaming,
              },
            });
            yield* emit({
              type: "message.updated",
              driver: PRIME_PROVIDER,
              message: {
                id: messageId,
                threadId: turn.turnInput.threadId,
                runId: turn.turnInput.runId,
                nodeId: idAllocator.derive.nodeFromProviderItem({
                  driver: PRIME_PROVIDER,
                  nativeItemId: item.nativeItemId,
                }),
                role: "assistant",
                text: item.text,
                attachments: [],
                streaming,
                createdBy: "agent",
                creationSource: "provider",
                createdAt: item.startedAt,
                updatedAt: emittedAt,
              },
            });
            return;
          }
          yield* emit({
            type: "turn_item.updated",
            driver: PRIME_PROVIDER,
            turnItem: {
              ...base,
              status: streaming ? "running" : "completed",
              title: null,
              completedAt: streaming ? null : emittedAt,
              type: "reasoning",
              text: item.text,
              streaming,
            },
          });
        });

      const scheduleStreamFlush = (turn: ActivePrimeTurn, item: PrimeStreamItemState) =>
        Effect.gen(function* () {
          if (item.flushScheduled || item.completed) return;
          item.flushScheduled = true;
          yield* Effect.sleep(Duration.millis(STREAM_FLUSH_MS)).pipe(
            Effect.andThen(
              Effect.suspend(() => {
                item.flushScheduled = false;
                return item.completed ? Effect.void : emitStreamItem(turn, item, true);
              }),
            ),
            Effect.forkIn(scope),
          );
        });

      const streamItemFor = Effect.fnUntraced(function* (
        turn: ActivePrimeTurn,
        kind: PrimeStreamItemState["kind"],
        contentIndex: number,
      ) {
        const nativeItemId = `${turn.providerTurn.id}:m${turn.messageOrdinal}:c${contentIndex}`;
        const existing = turn.streamItems.get(nativeItemId);
        if (existing !== undefined) return existing;
        const startedAt = yield* DateTime.now;
        const item: PrimeStreamItemState = {
          nativeItemId,
          kind,
          text: "",
          completed: false,
          flushScheduled: false,
          startedAt,
        };
        turn.streamItems.set(nativeItemId, item);
        itemOrdinal(turn, nativeItemId);
        return item;
      });

      const completeStreamItem = (
        turn: ActivePrimeTurn,
        item: PrimeStreamItemState,
        text?: string,
      ) =>
        Effect.suspend(() => {
          if (item.completed) return Effect.void;
          item.completed = true;
          if (text !== undefined && text.length > 0) item.text = text;
          return item.text.length === 0 ? Effect.void : emitStreamItem(turn, item, false);
        });

      const completeOpenStreamItems = (turn: ActivePrimeTurn) =>
        Effect.forEach(
          Array.from(turn.streamItems.values()).filter((item) => !item.completed),
          (item) => completeStreamItem(turn, item),
          { discard: true },
        );

      const emitToolItem = Effect.fnUntraced(function* (
        turn: ActivePrimeTurn,
        event: PrimeRpcRecord,
        phase: "start" | "update" | "end",
        closedStatus?: "interrupted" | "failed",
      ) {
        const toolCallId = recordString(event, "toolCallId");
        const toolName = recordString(event, "toolName") ?? "tool";
        if (toolCallId === undefined) return;
        if (phase === "start") {
          turn.toolArgs.set(toolCallId, event["args"]);
          turn.openToolCalls.set(toolCallId, toolName);
        } else if (phase === "end") {
          turn.openToolCalls.delete(toolCallId);
        }
        const args = event["args"] ?? turn.toolArgs.get(toolCallId);
        const completed = phase === "end";
        const isError = event["isError"] === true;
        const resultRecord = completed ? event["result"] : event["partialResult"];
        // ipython updates are single stream chunks, unlike Pi's cumulative ones.
        let streamedOutput: string | undefined;
        if (toolName === "ipython") {
          if (phase === "update") {
            const update = primeIpythonUpdate(resultRecord);
            if (update.type === "progress") return;
            streamedOutput = (turn.toolOutput.get(toolCallId) ?? "") + update.text;
            turn.toolOutput.set(toolCallId, streamedOutput);
          } else {
            streamedOutput = turn.toolOutput.get(toolCallId);
          }
        }
        const emittedAt = yield* DateTime.now;
        const startedAt = turn.toolStartedAt.get(toolCallId) ?? emittedAt;
        turn.toolStartedAt.set(toolCallId, startedAt);
        const mapped = primeToolItem({ toolName, args, result: resultRecord, isError });
        const mappedStatus = mapped?.type === "command_execution" ? mapped.status : undefined;
        const status = completed
          ? closedStatus !== undefined
            ? closedStatus
            : mappedStatus === "interrupted" || (isError && turn.interrupted)
              ? "interrupted"
              : isError || mappedStatus === "failed"
                ? "failed"
                : "completed"
          : "running";
        const base = baseItemFields(turn, toolCallId, startedAt, emittedAt);
        yield* emitItemNode(
          turn,
          toolCallId,
          "tool_call",
          status,
          startedAt,
          completed ? emittedAt : null,
        );
        const shared = {
          ...base,
          status,
          completedAt: completed ? emittedAt : null,
          title: toolName,
        } as const;
        if (mapped?.type === "command_execution") {
          const { status: _status, ...item } = mapped;
          const output = phase === "update" ? streamedOutput : (item.output ?? streamedOutput);
          yield* emit({
            type: "turn_item.updated",
            driver: PRIME_PROVIDER,
            turnItem: { ...shared, ...item, ...(output === undefined ? {} : { output }) },
          });
        } else if (mapped?.type === "file_change") {
          yield* emit({
            type: "turn_item.updated",
            driver: PRIME_PROVIDER,
            turnItem: {
              ...shared,
              ...mapped,
              ...(mapped.changes === undefined ? {} : { changes: [...mapped.changes] }),
            },
          });
        } else {
          const output = primeToolOutput(resultRecord, toolName);
          yield* emit({
            type: "turn_item.updated",
            driver: PRIME_PROVIDER,
            turnItem: {
              ...shared,
              type: "dynamic_tool",
              ...mcpToolPresentation({ toolName }),
              toolName,
              input: args ?? {},
              ...(output === undefined ? {} : { output }),
            },
          });
        }
        if (completed && toolName === "ipython") {
          turn.toolOutput.delete(toolCallId);
          const fileChange = primeIpythonFileChange(resultRecord);
          if (fileChange !== undefined) {
            const nativeItemId = `${toolCallId}:diffs`;
            yield* emitItemNode(turn, nativeItemId, "tool_call", "completed", emittedAt, emittedAt);
            yield* emit({
              type: "turn_item.updated",
              driver: PRIME_PROVIDER,
              turnItem: {
                ...baseItemFields(turn, nativeItemId, emittedAt, emittedAt),
                status: "completed",
                completedAt: emittedAt,
                title: toolName,
                ...fileChange,
                ...(fileChange.changes === undefined ? {} : { changes: [...fileChange.changes] }),
              },
            });
          }
        }
        if (toolName === "subagent") {
          yield* emitSubagentTasks(turn, toolCallId, resultRecord, completed);
        }
      });
      const emitSubagentTasks = Effect.fnUntraced(function* (
        turn: ActivePrimeTurn,
        toolCallId: string,
        resultRecord: unknown,
        completed: boolean,
      ) {
        const results = recordField(recordField(resultRecord, "details"), "results");
        if (!Array.isArray(results)) return;
        const emittedAt = yield* DateTime.now;
        const parentNodeId = idAllocator.derive.nodeFromProviderItem({
          driver: PRIME_PROVIDER,
          nativeItemId: toolCallId,
        });
        for (const [index, result] of results.entries()) {
          const agent = recordString(result, "agent");
          const task = recordString(result, "task");
          if (agent === undefined || task === undefined) continue;
          const nativeTaskId = `${toolCallId}:subagent:${recordNumber(result, "step") ?? index}`;
          const subagentId = idAllocator.derive.nodeFromProviderItem({
            driver: PRIME_PROVIDER,
            nativeItemId: nativeTaskId,
          });
          const startedAt = turn.toolStartedAt.get(nativeTaskId) ?? emittedAt;
          turn.toolStartedAt.set(nativeTaskId, startedAt);
          const finished = completed || recordField(result, "finished") === true;
          const stopReason = recordString(result, "stopReason");
          const interrupted = finished && stopReason === "aborted";
          const failed =
            finished &&
            !interrupted &&
            ((recordNumber(result, "exitCode") ?? 0) !== 0 || stopReason === "error");
          const status = interrupted
            ? "interrupted"
            : failed
              ? "failed"
              : finished
                ? "completed"
                : "running";
          const outputText = primeSubagentOutput(result);
          const progress =
            !finished && outputText.length > 0 ? { progress: outputText.slice(0, 200) } : {};
          const resultText = finished && outputText.length > 0 ? outputText.slice(0, 10_000) : null;
          yield* emit({
            type: "subagent.updated",
            driver: PRIME_PROVIDER,
            subagent: {
              id: subagentId,
              threadId: turn.turnInput.threadId,
              runId: turn.turnInput.runId,
              parentNodeId,
              origin: "provider_native",
              createdBy: "agent",
              driver: PRIME_PROVIDER,
              providerInstanceId: options.instanceId,
              providerThreadId: turn.turnInput.providerThread.id,
              childThreadId: null,
              nativeTaskRef: providerRef(nativeTaskId),
              prompt: task,
              title: agent,
              model: recordString(result, "model") ?? null,
              status,
              ...progress,
              result: resultText,
              startedAt,
              completedAt: finished ? emittedAt : null,
              updatedAt: emittedAt,
            },
          });
          yield* emit({
            type: "turn_item.updated",
            driver: PRIME_PROVIDER,
            turnItem: {
              ...baseItemFields(turn, nativeTaskId, startedAt, emittedAt),
              status,
              title: agent,
              completedAt: finished ? emittedAt : null,
              type: "subagent",
              subagentId,
              origin: "provider_native",
              driver: PRIME_PROVIDER,
              providerInstanceId: options.instanceId,
              childThreadId: null,
              prompt: task,
              ...progress,
              result: resultText,
            },
          });
        }
      });

      const cancelPrompt = (pending: PendingPrimePrompt, resolvedAt: DateTime.Utc) =>
        Effect.gen(function* () {
          if (pending.expirationFiber !== null) yield* Fiber.interrupt(pending.expirationFiber);
          yield* connection
            .send({
              type: "extension_ui_response",
              id: pending.nativeRequestId,
              cancelled: true,
            })
            .pipe(Effect.ignore);
          pending.runtimeRequest = {
            ...pending.runtimeRequest,
            status: "cancelled",
            resolvedAt,
          };
          yield* emit({
            type: "runtime_request.updated",
            driver: PRIME_PROVIDER,
            threadId: pending.node.threadId,
            runtimeRequest: pending.runtimeRequest,
          });
          yield* emit({
            type: "node.updated",
            driver: PRIME_PROVIDER,
            node: { ...pending.node, status: "cancelled", completedAt: resolvedAt },
          });
          yield* emit({
            type: "turn_item.updated",
            driver: PRIME_PROVIDER,
            turnItem: {
              ...pending.turnItem,
              status: "cancelled",
              completedAt: resolvedAt,
              updatedAt: resolvedAt,
            },
          });
        });

      const cancelPendingPrompts = (resolvedAt: DateTime.Utc) =>
        Effect.gen(function* () {
          const pending = Array.from(pendingPrompts.values());
          pendingPrompts.clear();
          yield* Effect.forEach(pending, (prompt) => cancelPrompt(prompt, resolvedAt), {
            discard: true,
          });
        });

      const handleExtensionUiRequest = Effect.fnUntraced(function* (event: PrimeRpcRecord) {
        const method = recordString(event, "method");
        const nativeRequestId = recordString(event, "id");
        if (method === undefined) return;
        if (method === "notify") {
          const state = threadState;
          const turn = state?.activeTurn ?? null;
          const message = recordString(event, "message") ?? "";
          if (turn === null || message.length === 0) return;
          const emittedAt = yield* DateTime.now;
          const nativeItemId = `notify:${turn.providerTurn.id}:${turn.nextItemOrdinal}`;
          yield* emitItemNode(turn, nativeItemId, "system", "completed", emittedAt, emittedAt);
          yield* emit({
            type: "turn_item.updated",
            driver: PRIME_PROVIDER,
            turnItem: {
              ...baseItemFields(turn, nativeItemId, emittedAt, emittedAt),
              status: "completed",
              completedAt: emittedAt,
              title: "notify",
              type: "dynamic_tool",
              toolName: "notify",
              input: {
                message,
                notifyType: recordString(event, "notifyType") ?? "info",
              },
            },
          });
          return;
        }
        if (
          method !== "select" &&
          method !== "confirm" &&
          method !== "input" &&
          method !== "editor"
        ) {
          yield* Effect.logDebug("Ignoring prime-agent extension UI update.", { method });
          return;
        }
        if (nativeRequestId === undefined) return;
        const approvalTitle = recordString(event, "title") ?? "";
        const approvalKey = `${approvalTitle.length}:${approvalTitle}${recordString(event, "message") ?? ""}`;
        if (method === "confirm" && sessionApprovals.has(approvalKey)) {
          yield* connection.send({
            type: "extension_ui_response",
            id: nativeRequestId,
            confirmed: true,
          });
          return;
        }
        const state = threadState;
        const turn = state?.activeTurn ?? null;
        const createdAt = yield* DateTime.now;
        const timeout = recordNumber(event, "timeout");
        // Prime treats zero as no timeout and negative durations as immediate expiry.
        const expiresAt =
          timeout === undefined || timeout === 0
            ? null
            : DateTime.toEpochMillis(createdAt) + Math.max(0, timeout);
        const requestId = yield* idAllocator.allocate.runtimeRequest({
          driver: PRIME_PROVIDER,
          ...(turn === null ? {} : { providerTurnId: turn.providerTurn.id }),
          nativeRequestId,
        });
        const nodeId = idAllocator.derive.approvalNode({ requestId });
        const title = recordString(event, "title") ?? method;
        const threadId =
          turn?.turnInput.threadId ?? state?.providerThread.appThreadId ?? input.threadId;
        const providerThreadId = state?.providerThread.id ?? null;
        const providerTurnId = turn?.providerTurn.id ?? null;
        const runtimeRequest: OrchestrationV2RuntimeRequest = {
          id: requestId,
          nodeId,
          providerTurnId,
          nativeRequestRef: providerRef(nativeRequestId),
          kind: method === "confirm" ? "command" : "user_input",
          status: "pending",
          responseCapability: { type: "live", providerSessionId: input.providerSessionId },
          createdAt,
          resolvedAt: null,
        };
        const node: OrchestrationV2ExecutionNode = {
          id: nodeId,
          threadId,
          runId: turn?.turnInput.runId ?? null,
          parentNodeId: turn?.turnInput.rootNodeId ?? null,
          rootNodeId: turn?.turnInput.rootNodeId ?? nodeId,
          kind: method === "confirm" ? "approval_request" : "user_input_request",
          status: "waiting",
          countsForRun: false,
          providerThreadId,
          providerTurnId,
          nativeItemRef: providerRef(nativeRequestId),
          runtimeRequestId: requestId,
          checkpointScopeId: null,
          startedAt: createdAt,
          completedAt: null,
        };
        const itemBase = {
          id: idAllocator.derive.approvalTurnItem({ requestId }),
          threadId,
          runId: turn?.turnInput.runId ?? null,
          nodeId,
          providerThreadId,
          providerTurnId,
          nativeItemRef: providerRef(nativeRequestId),
          parentItemId: null,
          ordinal: turn === null ? 0 : itemOrdinal(turn, nativeRequestId),
          status: "waiting" as const,
          title,
          startedAt: createdAt,
          completedAt: null,
          updatedAt: createdAt,
        };
        const turnItem: OrchestrationV2TurnItem =
          method === "confirm"
            ? {
                ...itemBase,
                type: "approval_request",
                requestId,
                requestKind: primeApprovalRequestKind(title),
                prompt: recordString(event, "message") ?? title,
              }
            : {
                ...itemBase,
                type: "user_input_request",
                requestId,
                questions: [primeQuestion(nativeRequestId, method, title, event)],
              };
        const pending: PendingPrimePrompt = {
          nativeRequestId,
          method,
          questionId: nativeRequestId,
          approvalKey,
          expiresAt,
          expirationFiber: null,
          runtimeRequest,
          node,
          turnItem,
        };
        pendingPrompts.set(String(requestId), pending);
        yield* emit({
          type: "runtime_request.updated",
          driver: PRIME_PROVIDER,
          threadId,
          runtimeRequest,
        });
        yield* emit({ type: "node.updated", driver: PRIME_PROVIDER, node });
        yield* emit({ type: "turn_item.updated", driver: PRIME_PROVIDER, turnItem });
        if (expiresAt !== null) {
          const now = yield* DateTime.now;
          pending.expirationFiber = yield* Effect.sleep(
            Duration.millis(Math.max(0, expiresAt - DateTime.toEpochMillis(now))),
          ).pipe(
            Effect.andThen(
              Queue.offer(connection.events, {
                type: "t3.ui_request_expired",
                requestId: String(requestId),
                pending,
              }),
            ),
            Effect.asVoid,
            Effect.forkIn(scope),
          );
        }
      });

      const emitExtensionError = Effect.fnUntraced(function* (event: PrimeRpcRecord) {
        const state = threadState;
        const turn = state?.activeTurn ?? null;
        if (turn === null) {
          outOfTurnExtensionErrors.push(event);
          return;
        }
        const emittedAt = yield* DateTime.now;
        const nativeItemId = `extension-error:${turn.providerTurn.id}:${turn.nextItemOrdinal}`;
        const extensionName = primeExtensionDisplayName(recordString(event, "extensionPath"));
        const extensionEvent = recordString(event, "event");
        const detail = recordString(event, "error")?.trim();
        const message = [
          `${extensionName} failed${extensionEvent === undefined ? "" : ` during ${extensionEvent}`}.`,
          detail === undefined || detail.length === 0 ? undefined : detail.slice(0, 2_000),
        ]
          .filter((part): part is string => part !== undefined)
          .join("\n\n");
        const failure = makeProviderFailure({
          message,
          class: "provider_error",
          retryable: false,
        });
        yield* emitItemNode(turn, nativeItemId, "system", "failed", emittedAt, emittedAt);
        yield* emit({
          type: "turn_item.updated",
          driver: PRIME_PROVIDER,
          turnItem: {
            ...baseItemFields(turn, nativeItemId, emittedAt, emittedAt),
            status: "failed",
            title: extensionName,
            completedAt: emittedAt,
            type: "error",
            failure,
          },
        });
      });

      const finalizeTurn = Effect.fnUntraced(function* (state: PrimeThreadState, readUsage = true) {
        const turn = state.activeTurn;
        if (turn === null) return;
        state.activeTurn = null;
        const interrupted = turn.interrupted || turn.nativeAborted;
        const completedAt = yield* DateTime.now;
        yield* completeOpenStreamItems(turn);
        // A tool cut off by a process exit, or by Stop's terminate before Prime
        // reports the abort, gets no tool_execution_end and would stay running.
        for (const [toolCallId, toolName] of turn.openToolCalls) {
          yield* emitToolItem(
            turn,
            { type: "tool_execution_end", toolCallId, toolName },
            "end",
            interrupted ? "interrupted" : "failed",
          );
        }
        if (turn.activeCompaction !== null) {
          const status = interrupted ? "cancelled" : turn.failure === null ? "completed" : "failed";
          yield* emitCompaction(turn, turn.activeCompaction, status);
          turn.activeCompaction = null;
        }
        if (turn.activeProviderRetry !== null) {
          if (interrupted) {
            yield* emitProviderRetry(turn, turn.activeProviderRetry, "interrupted", completedAt);
            turn.activeProviderRetry = null;
          } else if (turn.failure === null) {
            yield* emitProviderRetry(turn, turn.activeProviderRetry, "completed", completedAt);
            turn.activeProviderRetry = null;
          }
        }
        yield* cancelPendingPrompts(completedAt);
        // Prime 1.0.0 has no get_entries (modes/rpc/rpc-mode.ts), so the ref stays
        // synthetic; its fingerprint is what rollback and fork match against
        // get_fork_messages.
        const nativeTurnRef =
          turn.providerTurn.nativeTurnRef === null
            ? null
            : {
                ...turn.providerTurn.nativeTurnRef,
                fingerprint: primeTurnFingerprint({
                  lead: turn.historyLead ?? "none",
                  hashes: turn.userEntryHashes,
                }),
              };
        const tokenUsage = readUsage
          ? yield* readTokenUsage(turn.latestCompactionAfterTokens, completedAt)
          : undefined;
        const failure = interrupted ? null : (turn.rejectedPromptFailure ?? turn.failure);
        yield* emit({
          type: "provider_turn.updated",
          driver: PRIME_PROVIDER,
          threadId: turn.turnInput.threadId,
          providerTurn: {
            ...turn.providerTurn,
            nativeTurnRef,
            status: interrupted ? "interrupted" : failure !== null ? "failed" : "completed",
            completedAt,
            ...(tokenUsage === undefined ? {} : { tokenUsage }),
          },
        });
        yield* updateProviderThread(state, { status: "idle", ...goalPatch(state) });
        yield* updateProviderSession(
          failure !== null ? "error" : "ready",
          failure?.message ?? null,
        );
        if (failure !== null) {
          const failureItemId = `terminal-failure:${turn.providerTurn.id}`;
          if (turn.activeProviderRetry !== null) {
            yield* emitProviderRetry(
              turn,
              { ...turn.activeProviderRetry, failure },
              "failed",
              completedAt,
            );
          } else {
            yield* emit({
              type: "turn_item.updated",
              driver: PRIME_PROVIDER,
              turnItem: {
                ...baseItemFields(turn, failureItemId, completedAt, completedAt),
                status: "failed",
                title: null,
                completedAt,
                type: "error",
                failure,
              },
            });
          }
          yield* emit({
            type: "turn.terminal",
            driver: PRIME_PROVIDER,
            providerThreadId: state.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.turnInput.runOrdinal,
            failureItemOrdinal: itemOrdinal(turn, failureItemId),
            status: "failed",
            failure,
            ...(turn.activeProviderRetry === null
              ? {}
              : {
                  retry: turn.activeProviderRetry.retry,
                  retryStartedAt: turn.activeProviderRetry.startedAt,
                }),
            threadDisposition: "reusable",
          });
        } else {
          yield* emit({
            type: "turn.terminal",
            driver: PRIME_PROVIDER,
            providerThreadId: state.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.turnInput.runOrdinal,
            status: interrupted ? "interrupted" : "completed",
            failure: null,
            threadDisposition: "reusable",
          });
        }
      });

      /**
       * Whether the session has schedules or heartbeats that will start runs on
       * their own. Read only at a turn's terminal transition, never on a
       * timer; an unanswered read keeps the last known answer.
       */
      const readBackgroundJobs = Effect.all(
        [request({ type: "list_schedules" }, 2_000), request({ type: "list_heartbeats" }, 2_000)],
        { concurrency: "unbounded" },
      ).pipe(
        Effect.map(
          ([schedules, heartbeats]) =>
            hasActiveJob(recordField(schedules, "jobs")) ||
            hasActiveJob(recordField(heartbeats, "heartbeats"), "job"),
        ),
        Effect.tapError((error) =>
          Effect.logDebug("Prime schedule probe failed.", { error: error.message }),
        ),
        Effect.orElseSucceed(() => undefined),
      );

      /**
       * For terminal paths that skip the idle probe while Prime stays alive:
       * the run may have created a schedule or heartbeat, and the pin is the
       * only thing that keeps T3's idle release from stopping the worker (and
       * Prime from cancelling those jobs). A terminating process has nothing
       * left to pin.
       */
      const refreshBackgroundJobs = Effect.gen(function* () {
        if (stopRequested || closed) return;
        const backgroundJobs = yield* readBackgroundJobs;
        if (backgroundJobs !== undefined) activeBackgroundJobs = backgroundJobs;
      });

      const scheduleSettleProbe = (
        turn: ActivePrimeTurn,
        settleAfterAgentActivity = false,
        attempt = 1,
      ) => {
        const providerTurnId = turn.providerTurn.id;
        const settleProbeGeneration = turn.settleProbeGeneration;
        return request({ type: "get_state" }, 2_000).pipe(
          Effect.flatMap((data) => {
            const probed = decodePrimeState(data);
            return Option.isSome(probed) && isPrimeStateIdle(probed.value)
              ? readBackgroundJobs.pipe(Effect.map((backgroundJobs) => ({ data, backgroundJobs })))
              : Effect.succeed({ data, backgroundJobs: undefined });
          }),
          Effect.matchEffect({
            onSuccess: ({ data, backgroundJobs }) =>
              Queue.offer(connection.events, {
                type: "t3.settle_probe",
                providerTurnId,
                settleAfterAgentActivity,
                settleProbeGeneration,
                attempt,
                data,
                backgroundJobs,
              }),
            // A failed probe still has to reach the pump. Dropping it would
            // leave a command-only turn active forever, because Prime never emits
            // agent events for one.
            onFailure: (error) =>
              Queue.offer(connection.events, {
                type: "t3.settle_probe",
                providerTurnId,
                settleAfterAgentActivity,
                settleProbeGeneration,
                attempt,
                probeError: error.message,
              }),
          }),
          Effect.ignore,
          Effect.forkIn(scope),
        );
      };

      const sessionActionsBusy = (actions: PrimeSessionActionSnapshot | null): boolean =>
        actions !== null && (actions.active !== undefined || actions.queuedCount > 0);

      /**
       * Prime has no `agent_settled`, and `agent_end` is not idle: a goal,
       * queued follow-up, or session command can keep the session working.
       * After a trigger (agent_end or a cleared session command) the turn
       * waits while the pushed session actions are busy, then confirms with
       * one `get_state`. Only transitions call this, never a timer.
       */
      const requestSettle = (turn: ActivePrimeTurn) =>
        Effect.gen(function* () {
          if (!turn.sawAgentEnd && !turn.sessionCommandDone) return;
          if (sessionActionsBusy(latestSessionActions) || turn.activeCompaction !== null) return;
          if (turn.autoRetryPending && !turn.nativeAborted && !turn.interrupted) return;
          turn.settleProbeGeneration += 1;
          yield* scheduleSettleProbe(turn, true);
        });

      /** Compaction can start after a trigger or as a command-only turn. */
      const settleAfterCompaction = (turn: ActivePrimeTurn) =>
        turn.sawAgentEnd || turn.sessionCommandDone
          ? requestSettle(turn)
          : turn.sawAgentActivity
            ? Effect.void
            : scheduleSettleProbe(turn);

      const stopPendingWake = Effect.fnUntraced(function* (wake: PrimeWake) {
        if (pendingWake !== wake || threadState !== wake.state || closed) return;
        pendingWake = null;
        stopRequested = true;
        yield* cancelPendingPrompts(yield* DateTime.now);
        yield* connection.terminate;
      });

      /**
       * An RLM child stays with the turn that first reported it, after that
       * turn's terminal too: a completed run keeps ingesting while a subagent it
       * owns is unsettled (RunExecutionService shouldStopProviderEventIngestion),
       * so a late update needs the original run, never a new turn.
       */
      const handleRlmChildUpdate = Effect.fnUntraced(function* (event: PrimeRpcRecord) {
        const frame = classifyPrimeEvent(event);
        if (frame._tag !== "Prime" || frame.frame.type !== "rlm_child_update") {
          yield* Effect.logDebug("Ignoring a malformed Prime rlm_child_update.");
          return;
        }
        const child = frame.frame.child;
        const view = primeRlmChildView(child);
        if (view === undefined) {
          yield* Effect.logDebug("Ignoring an RLM child with an unknown status.", {
            status: child.status,
          });
          return;
        }
        const known = rlmChildren.get(child.id);
        const signature = primeRlmChildSignature(child);
        if (known?.signature === signature) return;
        // "cancelled" is also Prime's removal signal (rlm.delete_subagent);
        // a child that already finished keeps its outcome.
        if (known?.terminal === true && view.status === "cancelled") return;
        const turn = known?.turn ?? threadState?.activeTurn ?? null;
        if (turn === null) {
          if (pendingWake !== null) {
            pendingWake.events.push(event);
            return;
          }
          yield* Effect.logDebug("Dropping an RLM child update with no turn to attach to.", {
            childId: child.id,
          });
          return;
        }
        const updatedAt = yield* DateTime.now;
        const record: PrimeRlmChildState = known ?? {
          turn,
          // Prime's child id is the child's session directory name
          // (`sub-<8 hex>`), unique only under its parent session.
          nativeId: `rlm:${turn.turnInput.providerThread.id}:${child.id}`,
          startedAt: updatedAt,
          signature,
          terminal: view.terminal,
        };
        record.signature = signature;
        record.terminal = view.terminal;
        rlmChildren.set(child.id, record);
        const subagentId = idAllocator.derive.nodeFromProviderItem({
          driver: PRIME_PROVIDER,
          nativeItemId: record.nativeId,
        });
        const completedAt = view.terminal ? updatedAt : null;
        const progress = view.progress === undefined ? {} : { progress: view.progress };
        yield* emitItemNode(
          turn,
          record.nativeId,
          "subagent",
          view.status,
          record.startedAt,
          completedAt,
        );
        yield* emit({
          type: "subagent.updated",
          driver: PRIME_PROVIDER,
          subagent: {
            id: subagentId,
            threadId: turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            parentNodeId: turn.turnInput.rootNodeId,
            origin: "provider_native",
            createdBy: "agent",
            driver: PRIME_PROVIDER,
            providerInstanceId: options.instanceId,
            providerThreadId: turn.turnInput.providerThread.id,
            childThreadId: null,
            nativeTaskRef: providerRef(record.nativeId),
            prompt: view.title,
            title: view.title,
            model: view.model,
            status: view.status,
            ...progress,
            result: view.result,
            startedAt: record.startedAt,
            completedAt,
            updatedAt,
          },
        });
        yield* emit({
          type: "turn_item.updated",
          driver: PRIME_PROVIDER,
          turnItem: {
            ...baseItemFields(turn, record.nativeId, record.startedAt, updatedAt),
            status: view.status,
            title: view.title,
            completedAt,
            type: "subagent",
            subagentId,
            origin: "provider_native",
            driver: PRIME_PROVIDER,
            providerInstanceId: options.instanceId,
            childThreadId: null,
            prompt: view.title,
            ...progress,
            result: view.result,
          },
        });
      });

      const handleSessionEvent = Effect.fnUntraced(function* (event: PrimeRpcRecord) {
        if (event["type"] === "rlm_child_update") return yield* handleRlmChildUpdate(event);
        // Model-selection hooks can await a dialog before startTurn can claim
        // a wake. Keep interactive requests session-scoped until a turn owns
        // them; buffer native output and notifications for that turn.
        const interactiveUiRequest =
          event["type"] === "extension_ui_request" &&
          ["select", "confirm", "input", "editor"].includes(String(event["method"]));
        if (
          threadState?.activeTurn === null &&
          pendingWake !== null &&
          !interactiveUiRequest &&
          !String(event["type"]).startsWith("t3.") &&
          event["type"] !== "response" &&
          event["type"] !== "session_action_update"
        ) {
          pendingWake.events.push(event);
          if (event["type"] === "agent_start") yield* updateProviderSession("running", null);
          if (event["type"] === "agent_end") yield* updateProviderSession("ready", null);
          return;
        }
        const state = threadState;
        const turn = state?.activeTurn ?? null;
        switch (event["type"]) {
          case "agent_start": {
            if (
              turn === null &&
              state?.providerThread.appThreadId != null &&
              continuationRequests !== undefined &&
              !closed &&
              !stopRequested
            ) {
              const wake: PrimeWake = { state, events: [event] };
              pendingWake = wake;
              if (rollbackBarrier !== null) {
                yield* stopPendingWake(wake);
                return;
              }
              yield* updateProviderSession("running", null);
              yield* continuationRequests
                .offer({
                  threadId: state.providerThread.appThreadId,
                  providerThreadId: state.providerThread.id,
                  driver: PRIME_PROVIDER,
                  detail: null,
                  delivery: "adapter_buffered",
                  dispatchIfCurrent: (dispatch) =>
                    sessionEventPermit.withPermits(1)(
                      Effect.suspend(() =>
                        pendingWake === wake && threadState === state && !closed
                          ? dispatch.pipe(
                              Effect.map(Option.some),
                              Effect.tapError(() => stopPendingWake(wake)),
                            )
                          : Effect.succeed(Option.none()),
                      ),
                    ),
                  clearIfCurrent: () => sessionEventPermit.withPermits(1)(stopPendingWake(wake)),
                })
                .pipe(Effect.tapError(() => stopPendingWake(wake)));
              return;
            }
            if (turn === null) {
              unsolicitedActivityDetected = true;
              yield* updateProviderSession("error", PRIME_UNSOLICITED_ACTIVITY_ERROR);
              yield* connection.terminate;
              return;
            }
            turn.sawAgentActivity = true;
            turn.nativeAborted = false;
            turn.sawAgentEnd = false;
            turn.sessionCommandDone = false;
            turn.settleProbeGeneration += 1;
            return;
          }
          case "message_start": {
            if (turn !== null && recordString(event["message"], "role") === "assistant") {
              turn.sawAgentActivity = true;
              turn.messageOrdinal += 1;
            }
            return;
          }
          case "message_update": {
            if (turn === null) return;
            turn.sawAgentActivity = true;
            yield* reportLiveUsage(turn, event["usage"]);
            const delta = event["assistantMessageEvent"];
            const deltaType = recordString(delta, "type");
            const contentIndex = recordNumber(delta, "contentIndex") ?? 0;
            if (deltaType === "text_delta" || deltaType === "thinking_delta") {
              const item = yield* streamItemFor(
                turn,
                deltaType === "text_delta" ? "assistant_message" : "reasoning",
                contentIndex,
              );
              item.text += recordString(delta, "delta") ?? "";
              yield* scheduleStreamFlush(turn, item);
              return;
            }
            if (deltaType === "text_end" || deltaType === "thinking_end") {
              const item = yield* streamItemFor(
                turn,
                deltaType === "text_end" ? "assistant_message" : "reasoning",
                contentIndex,
              );
              yield* completeStreamItem(
                turn,
                item,
                recordString(delta, "content") ?? recordString(delta, "thinking"),
              );
              return;
            }
            return;
          }
          case "message_end": {
            if (turn === null) return;
            const message = event["message"];
            const role = recordString(message, "role");
            const lead = primeMessageLead(message);
            if (turn.historyLead === null && lead !== undefined) turn.historyLead = lead;
            if (role === "user") {
              const text = contentText(recordField(message, "content"));
              if (text.length > 0) turn.userEntryHashes.push(primeUserEntryHash(text));
              // Prime echoes every prompt and steer as a user message; only its
              // own autonomous continuations become items.
              const continuation = primeAutonomousContinuation(message);
              if (continuation !== undefined) yield* emitAutonomousContinuation(turn, continuation);
              return;
            }
            if (role === "custom") {
              // A failed result row fails the turn; it is not also a notice.
              const failure = primeSessionCommandFailure(message, turn.sessionCommands);
              if (failure !== undefined) {
                if (turn.failure === null) {
                  turn.failure = makeProviderFailure({ message: failure, class: "provider_error" });
                }
                return;
              }
              if (primeCustomMessageHidden(message)) return;
              // Gate rows can arrive as bare message pairs between runs while the
              // autonomous action keeps the turn open.
              const notice = primeAutonomousNotice(message);
              if (notice !== undefined) {
                yield* emitSystemNotice(
                  turn,
                  `autonomous:${turn.providerTurn.id}:${turn.nextItemOrdinal}`,
                  notice,
                );
                return;
              }
              const notification = primeHeartbeatNotification(message);
              if (notification !== undefined) {
                const emittedAt = yield* DateTime.now;
                const nativeItemId = `heartbeat:${turn.providerTurn.id}:${turn.nextItemOrdinal}`;
                yield* emitItemNode(
                  turn,
                  nativeItemId,
                  "system",
                  "completed",
                  emittedAt,
                  emittedAt,
                );
                yield* emit({
                  type: "turn_item.updated",
                  driver: PRIME_PROVIDER,
                  turnItem: {
                    ...baseItemFields(turn, nativeItemId, emittedAt, emittedAt),
                    status: "completed",
                    title: "Heartbeat",
                    completedAt: emittedAt,
                    type: "notification",
                    ...notification,
                  },
                });
                return;
              }
              const customNotice = primeCustomMessageNotice(message);
              if (customNotice !== undefined) {
                yield* emitSystemNotice(
                  turn,
                  `custom:${turn.providerTurn.id}:${turn.nextItemOrdinal}`,
                  customNotice,
                );
              }
              return;
            }
            if (role !== "assistant") return;
            yield* completeOpenStreamItems(turn);
            if (recordString(message, "stopReason") === "error" && turn.failure === null) {
              turn.failure = makeProviderFailure({
                message: primeErrorMessage(message) ?? PRIME_MODEL_ERROR,
                class: "provider_error",
              });
            }
            return;
          }
          case "tool_execution_start":
            if (turn !== null) {
              turn.sawAgentActivity = true;
              yield* emitToolItem(turn, event, "start");
            }
            return;
          case "tool_execution_update":
            if (turn !== null) yield* emitToolItem(turn, event, "update");
            return;
          case "tool_execution_end":
            if (turn !== null) yield* emitToolItem(turn, event, "end");
            return;
          case "compaction_start": {
            if (turn === null) return;
            turn.settleProbeGeneration += 1;
            turn.sawCompaction = true;
            if (turn.activeCompaction !== null) {
              yield* emitCompaction(turn, turn.activeCompaction, "cancelled");
            }
            const startedAt = yield* DateTime.now;
            const compaction = {
              nativeItemId: compactionNativeItemId(turn),
              startedAt,
            } satisfies PrimeCompactionState;
            turn.activeCompaction = compaction;
            yield* emitCompaction(turn, compaction, "running");
            return;
          }
          case "compaction_end": {
            if (turn === null) return;
            const observedAt = yield* DateTime.now;
            const compaction = turn.activeCompaction ?? {
              nativeItemId: compactionNativeItemId(turn),
              startedAt: observedAt,
            };
            turn.activeCompaction = null;
            const result = event["result"];
            if (result === null || result === undefined) {
              if (event["aborted"] === true) {
                yield* emitCompaction(turn, compaction, "cancelled");
                yield* settleAfterCompaction(turn);
                return;
              }
              const errorMessage =
                recordString(event, "errorMessage") ?? "Prime context compaction failed.";
              yield* emitCompaction(turn, compaction, "failed", {
                summary: errorMessage.slice(0, 1_000),
              });
              yield* settleAfterCompaction(turn);
              return;
            }
            // An overflow can surface as a model error (`message_end` with
            // stopReason error) before Prime compacts and retries the turn. Clear
            // that failure only when Prime confirms that compaction will retry;
            // a successful non-retrying compaction must not erase an exhausted
            // provider retry.
            if (event["willRetry"] === true) turn.failure = null;
            turn.latestCompactionAfterTokens =
              nonNegativeInteger(result, "estimatedTokensAfter") ?? null;
            const summary = recordString(result, "summary");
            const beforeTokenCount = nonNegativeInteger(result, "tokensBefore");
            const afterTokenCount = nonNegativeInteger(result, "estimatedTokensAfter");
            yield* emitCompaction(turn, compaction, "completed", {
              ...(summary === undefined ? {} : { summary }),
              ...(beforeTokenCount === undefined ? {} : { beforeTokenCount }),
              ...(afterTokenCount === undefined ? {} : { afterTokenCount }),
            });
            yield* settleAfterCompaction(turn);
            return;
          }
          case "auto_retry_start": {
            if (turn === null) return;
            const emittedAt = yield* DateTime.now;
            const attempt = Math.max(1, Math.trunc(recordNumber(event, "attempt") ?? 1));
            const maxAttempts = Math.max(
              attempt,
              Math.trunc(recordNumber(event, "maxAttempts") ?? attempt),
            );
            const retryDelayMs = Math.max(0, Math.trunc(recordNumber(event, "delayMs") ?? 0));
            const failure = makeProviderFailure({
              message: recordString(event, "errorMessage") ?? "Prime provider request failed.",
              class: "provider_error",
              retryable: true,
            });
            const current = turn.activeProviderRetry;
            const providerRetry = {
              retry: { attempt, maxAttempts, retryDelayMs },
              failure,
              startedAt: current?.startedAt ?? emittedAt,
              itemOrdinal:
                current?.itemOrdinal ??
                itemOrdinal(turn, `terminal-failure:${turn.providerTurn.id}`),
            } satisfies PrimeProviderRetryState;
            turn.activeProviderRetry = providerRetry;
            turn.autoRetryPending = true;
            // A probe sent on the failed run's agent_end would answer idle.
            turn.settleProbeGeneration += 1;
            yield* emitProviderRetry(turn, providerRetry, "running", emittedAt);
            return;
          }
          case "auto_retry_end": {
            if (turn === null) return;
            turn.autoRetryPending = false;
            const emittedAt = yield* DateTime.now;
            if (event["success"] === true) {
              // Prime reports the failed attempt as a model error before
              // retrying, so leaving that failure in place would make the
              // settle terminalize a successful turn as failed.
              if (turn.activeProviderRetry !== null) {
                const attempt = Math.max(
                  1,
                  Math.trunc(
                    recordNumber(event, "attempt") ?? turn.activeProviderRetry.retry.attempt,
                  ),
                );
                const recoveredRetry = {
                  ...turn.activeProviderRetry,
                  retry: { ...turn.activeProviderRetry.retry, attempt },
                };
                yield* emitProviderRetry(turn, recoveredRetry, "completed", emittedAt);
                turn.activeProviderRetry = null;
              }
              turn.failure = null;
              return;
            }
            const failure = makeProviderFailure({
              message: recordString(event, "finalError") ?? "Prime auto-retry failed.",
              class: "provider_error",
              retryable: false,
            });
            const attempt = Math.max(1, Math.trunc(recordNumber(event, "attempt") ?? 1));
            const current = turn.activeProviderRetry;
            const providerRetry = {
              retry: {
                attempt,
                maxAttempts: current?.retry.maxAttempts ?? attempt,
                retryDelayMs: current?.retry.retryDelayMs ?? null,
              },
              failure,
              startedAt: current?.startedAt ?? emittedAt,
              itemOrdinal:
                current?.itemOrdinal ??
                itemOrdinal(turn, `terminal-failure:${turn.providerTurn.id}`),
            } satisfies PrimeProviderRetryState;
            turn.activeProviderRetry = providerRetry;
            turn.failure = failure;
            yield* emitProviderRetry(turn, providerRetry, "failed", emittedAt);
            // Prime sends no further agent_end when it gives up.
            yield* requestSettle(turn);
            return;
          }
          case "extension_ui_request":
            yield* handleExtensionUiRequest(event);
            return;
          case "extension_error": {
            yield* emitExtensionError(event);
            return;
          }
          case "refine_failed": {
            // Self-started refinement also reports here; only a /refine turn owns it.
            if (turn === null || !turn.sessionCommands.has("refine") || turn.failure !== null)
              return;
            turn.failure = makeProviderFailure({
              message: recordString(event, "error") ?? "Prime refinement failed.",
              class: "provider_error",
            });
            return;
          }
          case "agent_end": {
            if (turn === null) return;
            if (turn.interrupted) {
              if (state === null) return;
              yield* refreshBackgroundJobs;
              yield* finalizeTurn(state);
              return;
            }
            const awaitingPromptAck = pendingPromptResponses.some(
              (pending) =>
                pending.providerTurnId === turn.providerTurn.id && pending.kind === "turn_start",
            );
            // Prime acks a prompt before any of its run's events (rpc-mode.ts
            // buffers them behind the response), so an agent_end ahead of the
            // ack with no agent_start belongs to a run that ended earlier.
            if (awaitingPromptAck && !turn.adoptedWake && !turn.sawAgentActivity) return;
            turn.nativeAborted = primeRunAborted(event);
            turn.sawAgentEnd = true;
            if (!turn.nativeAborted) {
              const errorMessage = primeAgentEndErrorMessage(event);
              if (
                errorMessage !== undefined &&
                (turn.failure === null || turn.failure.message === PRIME_MODEL_ERROR)
              ) {
                turn.failure = makeProviderFailure({
                  message: errorMessage,
                  class: "provider_error",
                });
              } else if (turn.failure === null && primeAgentEndFailed(event)) {
                turn.failure = makeProviderFailure({
                  message: PRIME_MODEL_ERROR,
                  class: "provider_error",
                });
              }
            }
            if (turn.adoptedWake && awaitingPromptAck) return;
            yield* requestSettle(turn);
            return;
          }
          case "goal_update": {
            const frame = classifyPrimeEvent(event);
            if (frame._tag !== "Prime" || frame.frame.type !== "goal_update") {
              yield* Effect.logDebug("Ignoring a malformed Prime goal_update.");
              return;
            }
            latestGoal = primeProviderGoal(frame.frame.goal);
            if (state !== null && turn !== null) {
              const patch = goalPatch(state);
              if (patch.goal !== undefined) yield* updateProviderThread(state, patch);
            }
            return;
          }
          case "session_action_update": {
            const frame = classifyPrimeEvent(event);
            if (frame._tag !== "Prime" || frame.frame.type !== "session_action_update") {
              yield* Effect.logDebug("Ignoring a malformed Prime session_action_update.");
              return;
            }
            const actions = frame.frame.actions;
            latestSessionActions = actions;
            if (turn === null) return;
            if (actions.active?.kind === "session_command") {
              turn.sessionCommandSeen = true;
            } else if (turn.sessionCommandSeen) {
              turn.sessionCommandSeen = false;
              turn.sessionCommandDone = true;
            }
            if (sessionActionsBusy(actions)) {
              // A probe answered before Prime picked up this work is stale.
              turn.settleProbeGeneration += 1;
              return;
            }
            yield* requestSettle(turn);
            return;
          }
          case "response": {
            const command = recordString(event, "command");
            const pendingPrompt = command === "prompt" ? pendingPromptResponses.shift() : undefined;
            const responseTurn =
              pendingPrompt?.providerTurnId === turn?.providerTurn.id ? turn : null;
            if (event["success"] === true) {
              if (pendingPrompt?.sessionCommand !== undefined && responseTurn !== null) {
                responseTurn.sessionCommands.add(pendingPrompt.sessionCommand);
              }
              // Deferred success ack. Command-only prompts (pure extension
              // slash commands) never start an agent run and never emit
              // `agent_end`, so probe for idleness. The probe result is
              // re-queued behind any events Prime emitted before answering
              // get_state, which keeps the check stream-ordered.
              if (
                pendingPrompt?.kind === "turn_start" &&
                responseTurn !== null &&
                ((responseTurn.promptMayBeCommandOnly && !responseTurn.sawAgentActivity) ||
                  (responseTurn.adoptedWake &&
                    (responseTurn.sawAgentEnd || responseTurn.promptMayBeCommandOnly)))
              ) {
                yield* scheduleSettleProbe(responseTurn, responseTurn.adoptedWake);
              }
              return;
            }
            if (event["success"] !== false) return;
            if (command === "steer" || pendingPrompt?.kind === "steer") {
              // A rejected steer only means that one message was refused. The
              // turn it was aimed at is still running on Prime, so terminalizing
              // here would report a failure while output keeps streaming.
              yield* Effect.logWarning("Prime rejected a steer message.", {
                errorLength: recordString(event, "error")?.length,
              });
              return;
            }
            // An id-less error that answers no pending prompt is Prime's
            // post-acceptance prompt failure (modes/daemon/daemon-mode.ts
            // broadcasts it without the request id), a parse error, or an
            // unknown-command error. Each ends the open turn.
            const failedTurn =
              command === "prompt" && pendingPrompt?.kind === "turn_start"
                ? responseTurn
                : pendingPrompt === undefined && event["id"] === undefined
                  ? turn
                  : null;
            if (
              failedTurn !== null &&
              failedTurn.sawAgentActivity &&
              isPrimeBusyRejection(recordString(event, "error"))
            ) {
              yield* Effect.logWarning("Prime refused a message while busy; the turn continues.", {
                errorLength: recordString(event, "error")?.length,
              });
              return;
            }
            if (failedTurn !== null) {
              const failure = makeProviderFailure({
                message: recordString(event, "error") ?? "Prime rejected the prompt.",
                class: "provider_error",
              });
              if (failedTurn.adoptedWake && pendingPrompt?.kind === "turn_start") {
                failedTurn.rejectedPromptFailure = failure;
                failedTurn.sawAgentEnd = true;
                yield* scheduleSettleProbe(failedTurn, true);
              } else if (state !== null) {
                failedTurn.failure = failure;
                if (failedTurn.sawAgentActivity) yield* refreshBackgroundJobs;
                yield* finalizeTurn(state);
              }
            }
            return;
          }
          case "t3.ui_request_expired": {
            const requestId = recordString(event, "requestId");
            if (requestId === undefined) return;
            const pending = pendingPrompts.get(requestId);
            if (pending === undefined || pending !== event["pending"]) return;
            pendingPrompts.delete(requestId);
            yield* cancelPrompt(pending, yield* DateTime.now);
            return;
          }
          case "t3.flush_extension_errors": {
            for (const extensionError of outOfTurnExtensionErrors.splice(0)) {
              yield* emitExtensionError(extensionError);
            }
            return;
          }
          case "t3.rollback_barrier": {
            if (rollbackBarrier !== null && event["barrier"] === rollbackBarrier)
              yield* Deferred.succeed(rollbackBarrier, undefined);
            return;
          }
          case "t3.settle_probe":
          case "t3.settle_probe_retry": {
            const settleAfterAgentActivity = event["settleAfterAgentActivity"] === true;
            const attempt = Math.max(1, Math.trunc(recordNumber(event, "attempt") ?? 1));
            // Any transition that bumps the generation (agent_start, compaction,
            // busy session actions) also cancels a pending retry here.
            if (
              state === null ||
              turn === null ||
              turn.providerTurn.id !== event["providerTurnId"] ||
              turn.settleProbeGeneration !== event["settleProbeGeneration"] ||
              (!settleAfterAgentActivity && turn.sawAgentActivity) ||
              turn.activeCompaction !== null
            ) {
              return;
            }
            if (event["type"] === "t3.settle_probe_retry") {
              yield* scheduleSettleProbe(turn, settleAfterAgentActivity, attempt);
              return;
            }
            const probedState = decodePrimeState(event["data"]);
            const probeError =
              recordString(event, "probeError") ??
              (Option.isNone(probedState) ? "Prime get_state returned an unexpected shape." : null);
            if (probeError === null) {
              if (Option.isSome(probedState) && isPrimeStateIdle(probedState.value)) {
                if (typeof event["backgroundJobs"] === "boolean") {
                  activeBackgroundJobs = event["backgroundJobs"];
                }
                yield* finalizeTurn(state);
              }
              return;
            }
            yield* Effect.logWarning("Prime settle get_state failed.", {
              attempt,
              error: probeError,
            });
            if (attempt < SETTLE_PROBE_MAX_ATTEMPTS) {
              yield* Effect.sleep(SETTLE_PROBE_RETRY_DELAY).pipe(
                Effect.andThen(
                  Queue.offer(connection.events, {
                    type: "t3.settle_probe_retry",
                    providerTurnId: event["providerTurnId"],
                    settleAfterAgentActivity,
                    settleProbeGeneration: event["settleProbeGeneration"],
                    attempt: attempt + 1,
                  }),
                ),
                Effect.forkIn(scope),
              );
              return;
            }
            // Without an idle read nothing proves Prime finished, so the turn
            // cannot complete; the session stays usable for the next turn.
            const prior = turn.rejectedPromptFailure ?? turn.failure;
            const probeFailure = `Prime did not confirm that the turn finished: get_state failed ${attempt} times. ${probeError}`;
            turn.rejectedPromptFailure = null;
            turn.failure = makeProviderFailure({
              message: prior === null ? probeFailure : `${prior.message}\n\n${probeFailure}`,
              class: "provider_error",
            });
            yield* refreshBackgroundJobs;
            yield* finalizeTurn(state, false);
            return;
          }
          default: {
            const frame = classifyPrimeEvent(event);
            if (frame._tag === "Unknown") {
              yield* Effect.logDebug("Ignoring unknown prime-agent event.", { type: frame.type });
            }
            return;
          }
        }
      });

      yield* Effect.gen(function* () {
        while (true) {
          const event = yield* Queue.take(connection.events);
          yield* sessionEventPermit.withPermits(1)(handleSessionEvent(event));
        }
      }).pipe(
        Effect.catchCause((cause) =>
          sessionEventPermit.withPermits(1)(
            Effect.gen(function* () {
              const state = threadState;
              if (rollbackBarrier !== null)
                yield* Deferred.fail(
                  rollbackBarrier,
                  protocolError("Prime transport closed during rollback"),
                );
              pendingWake = null;
              const interrupted = state?.activeTurn?.interrupted === true;
              const exitMessage = primeExitMessage(cause);
              if (state?.activeTurn != null) {
                state.activeTurn.failure = interrupted
                  ? null
                  : makeProviderFailure({
                      cause,
                      message: exitMessage,
                      class: "transport_error",
                    });
                yield* finalizeTurn(state, false);
              } else {
                yield* cancelPendingPrompts(yield* DateTime.now);
              }
              if (unsolicitedActivityDetected) {
                yield* updateProviderSession("error", PRIME_UNSOLICITED_ACTIVITY_ERROR);
                yield* Queue.end(events);
              } else if (stopRequested) {
                yield* updateProviderSession("stopped", null);
                yield* Queue.end(events);
              } else {
                yield* updateProviderSession(
                  "error",
                  interrupted ? "Prime process was stopped." : exitMessage,
                );
                yield* Queue.fail(
                  events,
                  new ProviderAdapter.ProviderAdapterEventStreamError({
                    driver: PRIME_PROVIDER,
                    providerSessionId: input.providerSessionId,
                    cause,
                  }),
                );
              }
            }),
          ),
        ),
        Effect.forkIn(scope),
      );

      const registerThread = Effect.fnUntraced(function* (
        threadInput: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
        publish = true,
      ) {
        if (rollbackBarrier !== null)
          return yield* protocolError("Cannot register a Prime thread during rollback");
        if (threadState !== null && threadState.activeTurn !== null) {
          return yield* protocolError("Cannot register a Prime thread while a turn is active");
        }
        if (pendingWake !== null) {
          const state = threadState;
          if (
            state !== null &&
            state === pendingWake.state &&
            state.providerThread.appThreadId === threadInput.threadId &&
            (threadInput.existingProviderThread === undefined ||
              threadInput.existingProviderThread.nativeThreadRef?.nativeId ===
                state.providerThread.nativeThreadRef?.nativeId)
          )
            return state.providerThread;
          return yield* protocolError(
            "Cannot switch Prime threads while native work is awaiting a turn",
          );
        }
        const existing = threadInput.existingProviderThread;
        const resumeId = existing?.nativeThreadRef?.nativeId;
        const needsNewSession = resumeId == null && registrationAttempted;
        registrationAttempted = true;
        if (resumeId != null || needsNewSession) {
          lastNativeThreadId = resumeId ?? lastNativeThreadId;
          // Even a failed lifecycle operation can change Prime's native session.
          // Never leave the old app binding or model defaults usable afterward.
          threadState = null;
          latestGoal = undefined;
          appliedModel = null;
          appliedThinking = null;
          requestedSessionName = null;
          baselineModel = null;
          baselineThinking = null;
          contextWindow = null;
          const result = yield* resumeId != null
            ? resumeSession(resumeId)
            : lifecycleRequest({ type: "new_session" });
          if (recordField(result, "cancelled") === true) {
            return yield* protocolError("A Prime extension cancelled the session switch");
          }
        }
        const stateData = yield* request({ type: "get_state" });
        const registeredState = decodePrimeState(stateData);
        if (Option.isSome(registeredState)) {
          latestSessionActions = registeredState.value.sessionActions ?? null;
          const goal = registeredState.value.goal;
          if (goal !== undefined) latestGoal = primeProviderGoal(goal);
        }
        if (!modelsDiscovered) {
          const modelsData = yield* request({ type: "get_available_models" }).pipe(
            Effect.orElseSucceed(() => undefined),
          );
          const models = recordField(modelsData, "models");
          if (Array.isArray(models)) {
            for (const model of models) rememberModelContextWindow(model);
            modelsDiscovered = true;
          }
        }
        contextWindow = rememberModelContextWindow(recordField(stateData, "model"));
        // Each baseline is captured independently, and only while nothing has
        // been applied yet, so a `get_state` that arrives after our own
        // selection cannot record that selection as Prime's default.
        if (baselineModel === null && appliedModel === null) {
          const stateModel = recordField(stateData, "model");
          const provider = recordString(stateModel, "provider");
          const modelId = recordString(stateModel, "id");
          if (provider !== undefined && modelId !== undefined) {
            baselineModel = { provider, modelId };
          }
        }
        if (baselineThinking === null && appliedThinking === null) {
          baselineThinking = recordString(stateData, "thinkingLevel") ?? null;
        }
        // switch_session accepts a file path, not Prime's display session UUID.
        const nativeId = recordString(stateData, "sessionFile");
        if (nativeId === undefined) {
          return yield* protocolError("get_state returned no persisted sessionFile", stateData);
        }
        if (needsNewSession && nativeId === lastNativeThreadId) {
          return yield* protocolError("Prime did not create a distinct session file");
        }
        lastNativeThreadId = nativeId;
        const createdAt = yield* DateTime.now;
        const providerThread: OrchestrationV2ProviderThread =
          existing !== undefined
            ? {
                ...existing,
                providerSessionId: input.providerSessionId,
                nativeThreadRef: providerRef(nativeId),
                ...(needsNewSession ? { nativeConversationHeadRef: null, contextUsage: null } : {}),
                ...(latestGoal === undefined ? {} : { goal: latestGoal }),
                status: "idle",
                updatedAt: createdAt,
              }
            : {
                id: idAllocator.derive.providerThread({
                  driver: PRIME_PROVIDER,
                  nativeThreadId: nativeId,
                }),
                driver: PRIME_PROVIDER,
                providerInstanceId: options.instanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: threadInput.threadId,
                ownerNodeId: null,
                nativeThreadRef: providerRef(nativeId),
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                pendingBackgroundTasks: [],
                goal: latestGoal ?? null,
                createdAt,
                updatedAt: createdAt,
              };
        threadState = { providerThread, activeTurn: null };
        if (publish)
          yield* emit({
            type: "provider_thread.updated",
            driver: PRIME_PROVIDER,
            providerThread,
          });
        return providerThread;
      });

      const applySelection = Effect.fnUntraced(function* (modelSelection: ModelSelection) {
        const thinking = getModelSelectionStringOptionValue(modelSelection, "thinking");
        if (modelSelection.model === PRIME_INHERIT_MODEL_SLUG) {
          if (appliedModel !== null && baselineModel !== null) {
            const restoredModel = yield* request({ type: "set_model", ...baselineModel });
            contextWindow = rememberModelContextWindow(restoredModel);
            appliedModel = null;
            const updatedAt = yield* DateTime.now;
            sessionEntity = { ...sessionEntity, model: PRIME_INHERIT_MODEL_SLUG, updatedAt };
            yield* emit({
              type: "provider_session.updated",
              driver: PRIME_PROVIDER,
              providerSession: sessionEntity,
            });
          }
          if (
            thinking === undefined &&
            appliedThinking !== null &&
            baselineThinking !== null &&
            appliedThinking !== baselineThinking
          ) {
            yield* request({ type: "set_thinking_level", level: baselineThinking });
            appliedThinking = null;
          }
        } else if (modelSelection.model !== appliedModel) {
          const parsed = parsePrimeModelSlug(modelSelection.model);
          if (parsed === null) {
            return yield* protocolError(
              `Prime model '${modelSelection.model}' must use provider/model format`,
            );
          }
          const selectedModel = yield* request({
            type: "set_model",
            provider: parsed.provider,
            modelId: parsed.modelId,
          });
          contextWindow = rememberModelContextWindow(selectedModel);
          appliedModel = modelSelection.model;
          const updatedAt = yield* DateTime.now;
          sessionEntity = { ...sessionEntity, model: modelSelection.model, updatedAt };
          yield* emit({
            type: "provider_session.updated",
            driver: PRIME_PROVIDER,
            providerSession: sessionEntity,
          });
        }
        if (
          thinking !== undefined &&
          thinking !== appliedThinking &&
          PRIME_THINKING_LEVELS.has(thinking)
        ) {
          yield* request({ type: "set_thinking_level", level: thinking });
          appliedThinking = thinking;
        }
      });

      const resolvePromptPayload = Effect.fnUntraced(function* (
        text: string,
        attachments: ReadonlyArray<ChatAttachment>,
      ) {
        const images: Array<{ type: "image"; data: string; mimeType: string }> = [];
        const extraLines: Array<string> = [];
        for (const attachment of attachments) {
          const path = host.resolveAttachmentPath(attachment);
          if (path === null) continue;
          if (attachment.mimeType.startsWith("image/")) {
            const bytes = yield* fileSystem.readFile(path);
            images.push({
              type: "image",
              data: Buffer.from(bytes).toString("base64"),
              mimeType: attachment.mimeType,
            });
          } else {
            extraLines.push(`[Attachment saved at ${path}]`);
          }
        }
        const message = extraLines.length === 0 ? text : `${text}\n\n${extraLines.join("\n")}`;
        return { message, images };
      });

      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: options.instanceId,
        driver: PRIME_PROVIDER,
        providerSessionId: input.providerSessionId,
        get providerSession() {
          return sessionEntity;
        },
        events: Stream.fromQueue(events),
        hasPendingBackgroundWork: Effect.sync(() => pendingWake !== null || activeBackgroundJobs),
        // Schedules can sit idle for hours, so they pin only the session's
        // idle release, never a run's ingestion.
        hasPendingBackgroundWorkForThread: (providerThread) =>
          Effect.sync(
            () => pendingWake !== null && pendingWake.state.providerThread.id === providerThread.id,
          ),
        getModelContextWindow: (selection) => {
          if (selection.instanceId !== options.instanceId) return undefined;
          const slug =
            selection.model === PRIME_INHERIT_MODEL_SLUG
              ? baselineModel === null
                ? undefined
                : `${baselineModel.provider}/${baselineModel.modelId}`
              : selection.model;
          return slug === undefined ? undefined : modelContextWindows.get(slug);
        },
        ensureThread: (threadInput) =>
          registerThread(threadInput).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterEnsureThreadError({
                  driver: PRIME_PROVIDER,
                  threadId: threadInput.threadId,
                  cause,
                }),
            ),
          ),
        resumeThread: (threadInput) =>
          registerThread({
            threadId:
              threadInput.threadId ?? threadInput.providerThread.appThreadId ?? input.threadId,
            modelSelection: threadInput.modelSelection ?? input.modelSelection,
            runtimePolicy: threadInput.runtimePolicy ?? input.runtimePolicy,
            existingProviderThread: threadInput.providerThread,
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterResumeThreadError({
                  driver: PRIME_PROVIDER,
                  providerSessionId: input.providerSessionId,
                  providerThreadId: threadInput.providerThread.id,
                  cause,
                }),
            ),
          ),
        compactThread: (turnInput) =>
          runtime.startTurn({
            ...turnInput,
            message: { ...turnInput.message, text: "/compact" },
          }),
        startTurn: (turnInput) =>
          Effect.gen(function* () {
            if (rollbackBarrier !== null)
              return yield* protocolError("Cannot start a Prime turn during rollback");
            const state = threadState;
            if (state === null) {
              return yield* protocolError("Prime session has no registered thread");
            }
            if (state.activeTurn !== null) {
              return yield* protocolError(
                `Prime provider thread ${turnInput.providerThread.id} already has an active turn`,
              );
            }
            if (
              state.providerThread.nativeThreadRef?.nativeId !==
              turnInput.providerThread.nativeThreadRef?.nativeId
            ) {
              return yield* protocolError("Prime turn requested for a different native session");
            }
            // The orchestrator adopts a fork under its already-allocated row.
            // Future session updates must retain that authoritative identity.
            const continuation =
              turnInput.message.createdBy === "agent" &&
              turnInput.message.creationSource === "provider";
            if (!continuation) yield* applySelection(turnInput.modelSelection);
            if (turnInput.appThread.title !== requestedSessionName) {
              requestedSessionName = turnInput.appThread.title;
              yield* request({ type: "set_session_name", name: requestedSessionName }).pipe(
                Effect.tapError((error) =>
                  Effect.logWarning("Prime rejected set_session_name; the turn continues.", {
                    error: error.message,
                  }),
                ),
                Effect.ignore,
              );
            }
            // Resolved before the turn is installed: a failure here (an
            // unreadable attachment) must not leave `activeTurn` set, which
            // would reject every later turn as already active.
            // Orchestration instructions reach prime-agent through the T3 MCP
            // extension's before_agent_start system-prompt hook, never by
            // wrapping the user text: a wrapped first message would no
            // longer start with "/" and slash commands would stop expanding.
            const payload = continuation
              ? null
              : yield* resolvePromptPayload(turnInput.message.text, turnInput.message.attachments);
            const startedAt = yield* DateTime.now;
            const syntheticNativeTurnId = `${state.providerThread.id}:attempt:${turnInput.attemptId}`;
            const providerTurn: OrchestrationV2ProviderTurn = {
              id: idAllocator.derive.providerTurn({
                driver: PRIME_PROVIDER,
                nativeTurnId: syntheticNativeTurnId,
              }),
              providerThreadId: turnInput.providerThread.id,
              nodeId: turnInput.rootNodeId,
              runAttemptId: turnInput.attemptId,
              nativeTurnRef: providerRef(syntheticNativeTurnId, "weak"),
              ordinal: turnInput.providerTurnOrdinal,
              status: "running",
              startedAt,
              completedAt: null,
            };
            const initialSessionCommand =
              payload === null ? undefined : primeSessionCommandName(payload.message);
            const activeTurn: ActivePrimeTurn = {
              turnInput,
              providerTurn,
              startedAt,
              itemOrdinals: new Map(),
              nextItemOrdinal: turnInput.providerTurnOrdinal * 100 + 1,
              messageOrdinal: 0,
              streamItems: new Map(),
              toolArgs: new Map(),
              toolOutput: new Map(),
              toolStartedAt: new Map(),
              openToolCalls: new Map(),
              interrupted: false,
              nativeAborted: false,
              sawAgentActivity: false,
              adoptedWake: false,
              promptMayBeCommandOnly: payload?.message.trimStart().startsWith("/") ?? false,
              sessionCommands: new Set(
                initialSessionCommand === undefined ? [] : [initialSessionCommand],
              ),
              userEntryHashes: [],
              historyLead: null,
              latestCompactionAfterTokens: null,
              lastLiveUsedTokens: null,
              settleProbeGeneration: 0,
              sawAgentEnd: false,
              sessionCommandSeen: false,
              sessionCommandDone: false,
              sawCompaction: false,
              activeCompaction: null,
              activeProviderRetry: null,
              autoRetryPending: false,
              failure: null,
              rejectedPromptFailure: null,
            };
            // Only the install/send/start-event boundary excludes the event
            // pump. Earlier correlated requests must leave the pump free so
            // project trust, login, and session-switch dialogs can be shown
            // and answered instead of deadlocking the caller.
            yield* Effect.gen(function* () {
              if (
                threadState !== state ||
                state.activeTurn !== null ||
                closed ||
                stopRequested ||
                rollbackBarrier !== null ||
                state.providerThread.nativeThreadRef?.nativeId !==
                  turnInput.providerThread.nativeThreadRef?.nativeId
              ) {
                return yield* protocolError("Prime turn ownership changed before it could start");
              }
              const wake = pendingWake?.state === state ? pendingWake : null;
              if (wake !== null && wake.state.providerThread.appThreadId !== turnInput.threadId) {
                return yield* protocolError("Prime native wake belongs to a different app thread");
              }
              pendingWake = null;
              state.providerThread = turnInput.providerThread;
              state.activeTurn = activeTurn;
              activeTurn.adoptedWake = wake !== null && !continuation;
              if (payload !== null) {
                yield* connection.send({
                  type: "prompt",
                  message: payload.message,
                  streamingBehavior: "steer",
                  ...(payload.images.length === 0 ? {} : { images: payload.images }),
                });
                pendingPromptResponses.push({
                  providerTurnId: providerTurn.id,
                  kind: "turn_start",
                });
              }
              yield* emit({
                type: "provider_turn.updated",
                driver: PRIME_PROVIDER,
                threadId: turnInput.threadId,
                providerTurn,
              });
              yield* updateProviderThread(state, {
                status: "active",
                firstRunOrdinal: state.providerThread.firstRunOrdinal ?? turnInput.runOrdinal,
                lastRunOrdinal: turnInput.runOrdinal,
                ...goalPatch(state),
              });
              yield* updateProviderSession("running", null);
              if (wake !== null) {
                for (const event of wake.events) {
                  // The adopting prompt's ack decides when to probe.
                  if (!continuation && event["type"] === "agent_end") {
                    activeTurn.nativeAborted = primeRunAborted(event);
                    activeTurn.sawAgentEnd = true;
                    continue;
                  }
                  yield* handleSessionEvent(event);
                }
              } else if (continuation) {
                yield* finalizeTurn(state);
              }
              if (outOfTurnExtensionErrors.length > 0) {
                yield* Queue.offer(connection.events, { type: "t3.flush_extension_errors" });
              }
            }).pipe(
              sessionEventPermit.withPermits(1),
              Effect.tapError(() =>
                Effect.sync(() => {
                  if (state.activeTurn === activeTurn) state.activeTurn = null;
                }),
              ),
            );
            // Prime acks `prompt` only after slash-command expansion completes,
            // and extension commands may block on user dialogs indefinitely.
            // Rejections therefore return later as id-less response records
            // handled by the event pump.
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver: PRIME_PROVIDER,
                  threadId: turnInput.threadId,
                  providerThreadId: turnInput.providerThread.id,
                  runId: turnInput.runId,
                  cause,
                }),
            ),
          ),
        steerTurn: (steerInput: ProviderAdapter.ProviderAdapterV2SteerInput) =>
          Effect.gen(function* () {
            const turn = threadState?.activeTurn ?? null;
            if (turn === null || turn.providerTurn.id !== steerInput.providerTurnId) {
              return yield* protocolError(`Prime turn ${steerInput.providerTurnId} is not active`);
            }
            const payload = yield* resolvePromptPayload(
              steerInput.message.text,
              steerInput.message.attachments,
            );
            // Prompt with streamingBehavior steer is atomic on Prime's side: it
            // queues during an active run and starts a new run if settlement
            // won the race. A direct `steer` sent after Prime became idle would
            // remain queued forever. Send fire-and-forget under the session
            // permit so a slash-command dialog cannot block the turn, and so
            // settlement cannot overtake the active-turn check.
            yield* sessionEventPermit.withPermits(1)(
              Effect.gen(function* () {
                if (threadState?.activeTurn !== turn) {
                  return yield* protocolError(
                    `Prime turn ${steerInput.providerTurnId} is not active`,
                  );
                }
                yield* connection.send({
                  type: "prompt",
                  message: payload.message,
                  streamingBehavior: "steer",
                  ...(payload.images.length === 0 ? {} : { images: payload.images }),
                });
                pendingPromptResponses.push({
                  providerTurnId: turn.providerTurn.id,
                  kind: "steer",
                  sessionCommand: primeSessionCommandName(payload.message),
                });
                turn.settleProbeGeneration += 1;
              }),
            );
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterSteerRunError({
                  driver: PRIME_PROVIDER,
                  providerThreadId: steerInput.providerThread.id,
                  providerTurnId: steerInput.providerTurnId,
                  cause,
                }),
            ),
          ),
        interruptTurn: (interruptInput) =>
          Effect.gen(function* () {
            const turn = threadState?.activeTurn ?? null;
            if (turn === null && interruptInput.requestRuntimeRestart === true) {
              if (pendingWake !== null)
                yield* sessionEventPermit.withPermits(1)(stopPendingWake(pendingWake));
              return;
            }
            if (turn === null || turn.providerTurn.id !== interruptInput.providerTurnId) {
              return yield* protocolError(
                `Prime turn ${interruptInput.providerTurnId} is not active`,
              );
            }
            turn.interrupted = true;
            if (
              interruptInput.requestRuntimeRestart === true ||
              turn.sawAgentEnd ||
              turn.activeCompaction !== null
            ) {
              // Prime's generic abort does not cancel manual compaction. Terminate
              // so Stop covers user /compact as well as detached recovery compact.
              stopRequested = true;
              if (interruptInput.requestRuntimeRestart === true && !turn.sawAgentEnd) {
                yield* request({ type: "abort" }, 2_000).pipe(Effect.ignore);
              }
              // Holding the event permit lets a finalize that is already
              // running finish before the kill.
              yield* sessionEventPermit.withPermits(1)(connection.terminate);
              return;
            }
            yield* request({ type: "abort" }).pipe(
              Effect.tapError(() => Effect.sync(() => (turn.interrupted = false))),
            );
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterInterruptError({
                  driver: PRIME_PROVIDER,
                  providerThreadId: interruptInput.providerThread.id,
                  providerTurnId: interruptInput.providerTurnId,
                  cause,
                }),
            ),
          ),
        respondToRuntimeRequest: (requestInput) =>
          Effect.gen(function* () {
            const pending = pendingPrompts.get(String(requestInput.requestId));
            if (pending === undefined) {
              return yield* protocolError(
                `No pending Prime extension request ${requestInput.requestId}`,
              );
            }
            const answeredAt = yield* DateTime.now;
            if (
              pending.expiresAt !== null &&
              DateTime.toEpochMillis(answeredAt) >= pending.expiresAt
            ) {
              pendingPrompts.delete(String(requestInput.requestId));
              yield* cancelPrompt(pending, answeredAt);
              return yield* protocolError(
                `Prime extension request ${requestInput.requestId} has expired`,
              );
            }
            const response = primeUiResponse(pending, requestInput.decision, requestInput.answers);
            yield* connection.send({
              type: "extension_ui_response",
              id: pending.nativeRequestId,
              ...response,
            });
            pendingPrompts.delete(String(requestInput.requestId));
            if (pending.expirationFiber !== null) yield* Fiber.interrupt(pending.expirationFiber);
            if (pending.method === "confirm" && requestInput.decision === "acceptForSession") {
              sessionApprovals.add(pending.approvalKey);
            }
            const resolvedAt = yield* DateTime.now;
            pending.runtimeRequest = {
              ...pending.runtimeRequest,
              status: "resolved",
              ...(requestInput.decision === undefined ? {} : { decision: requestInput.decision }),
              resolvedAt,
            };
            yield* emit({
              type: "runtime_request.updated",
              driver: PRIME_PROVIDER,
              threadId: pending.node.threadId,
              runtimeRequest: pending.runtimeRequest,
            });
            yield* emit({
              type: "node.updated",
              driver: PRIME_PROVIDER,
              node: { ...pending.node, status: "completed", completedAt: resolvedAt },
            });
            yield* emit({
              type: "turn_item.updated",
              driver: PRIME_PROVIDER,
              turnItem: {
                ...pending.turnItem,
                status: "completed",
                completedAt: resolvedAt,
                updatedAt: resolvedAt,
              },
            });
          }).pipe(
            sessionEventPermit.withPermits(1),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                  driver: PRIME_PROVIDER,
                  requestId: requestInput.requestId,
                  cause,
                }),
            ),
          ),
        readThreadSnapshot: (snapshotInput) =>
          Effect.gen(function* () {
            const state = threadState;
            const boundNativeId = state?.providerThread.nativeThreadRef?.nativeId;
            const wantedNativeId = snapshotInput.providerThread.nativeThreadRef?.nativeId;
            if (state === null || wantedNativeId == null || boundNativeId !== wantedNativeId) {
              return yield* protocolError(
                "Prime snapshot requested for a thread this session does not host",
              );
            }
            // get_messages is Prime's active-branch view. get_entries returns the
            // whole session tree, including abandoned branches after /tree or
            // fork, which would leak discarded conversation into handoffs.
            const messagesData = yield* request({ type: "get_messages" });
            const activeMessages = recordField(messagesData, "messages");
            const threadId = state.providerThread.appThreadId ?? input.threadId;
            const messages = (Array.isArray(activeMessages) ? activeMessages : []).flatMap(
              (message, index) => {
                const role = recordString(message, "role");
                if (role !== "user" && role !== "assistant") return [];
                const text = contentText(recordField(message, "content"));
                if (text.length === 0) return [];
                const timestamp = recordNumber(message, "timestamp");
                const at = Option.getOrElse(
                  DateTime.make(timestamp ?? Number.NaN),
                  () => state.providerThread.createdAt,
                );
                return [
                  {
                    id: idAllocator.derive.messageFromProviderItem({
                      driver: PRIME_PROVIDER,
                      nativeItemId: `${wantedNativeId}:snapshot-message:${index}`,
                    }),
                    threadId,
                    runId: null,
                    nodeId: null,
                    role: role as "user" | "assistant",
                    text,
                    attachments: [],
                    streaming: false,
                    createdBy: role === "user" ? ("user" as const) : ("agent" as const),
                    creationSource: "provider" as const,
                    createdAt: at,
                    updatedAt: at,
                  },
                ];
              },
            );
            return {
              providerThread: state.providerThread,
              providerTurns: [],
              messages,
              runtimeRequests: [],
            };
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterReadThreadSnapshotError({
                  driver: PRIME_PROVIDER,
                  providerThreadId: snapshotInput.providerThread.id,
                  cause,
                }),
            ),
          ),
        rollbackThread: (rollbackInput) =>
          Effect.gen(function* () {
            const state = threadState;
            if (state === null) {
              return yield* protocolError("Prime session has no registered thread");
            }
            if (state.providerThread.id !== rollbackInput.providerThread.id) {
              return yield* protocolError(
                "Prime rollback requested for a thread this session does not host",
              );
            }
            if (state.activeTurn !== null) {
              return yield* protocolError("Cannot roll back while a Prime turn is active");
            }
            if (pendingWake !== null) {
              return yield* protocolError("Cannot roll back while native Prime work awaits a turn");
            }
            const discarded = primeDiscardedHistory(
              rollbackInput.providerThreadTurns,
              rollbackBoundaryOrdinal(rollbackInput.target),
            );
            if (discarded.type === "unknown") {
              return yield* protocolError(
                "Prime rollback crosses a turn with no recorded session entries",
              );
            }
            if (discarded.type === "stranded") {
              return yield* new PrimeRollbackBoundaryError({
                operation: "rollback",
                turnOrdinal: discarded.ordinal,
                lead: discarded.lead,
              });
            }
            if (discarded.type === "none") {
              return primeThreadSnapshot(state.providerThread);
            }
            return yield* Effect.acquireUseRelease(
              sessionEventPermit.withPermits(1)(
                Effect.gen(function* () {
                  if (
                    rollbackBarrier !== null ||
                    threadState !== state ||
                    state.activeTurn !== null ||
                    pendingWake !== null ||
                    closed ||
                    stopRequested
                  )
                    return yield* protocolError(
                      "Prime rollback ownership changed before its fork could start",
                    );
                  const barrier = yield* Deferred.make<
                    void,
                    ProviderAdapter.ProviderAdapterProtocolError
                  >();
                  rollbackBarrier = barrier;
                  return barrier;
                }),
              ),
              (barrier) =>
                Effect.gen(function* () {
                  const forkMessages = primeForkMessages(
                    yield* request({ type: "get_fork_messages" }),
                  );
                  if (forkMessages === undefined)
                    return yield* protocolError("Prime returned malformed fork messages");
                  const boundary = primeForkBoundary({
                    messages: forkMessages,
                    turns: rollbackInput.providerThreadTurns,
                    boundaryOrdinal: rollbackBoundaryOrdinal(rollbackInput.target),
                    skipUnmatched: false,
                  });
                  if (boundary.type === "error") return yield* protocolError(boundary.message);
                  if (boundary.type === "stranded")
                    return yield* new PrimeRollbackBoundaryError({
                      operation: "rollback",
                      turnOrdinal: boundary.ordinal,
                      lead: boundary.lead,
                    });
                  if (boundary.type === "keep_all")
                    return yield* protocolError(
                      "Prime session has none of the rolled-back turns' entries",
                    );
                  const forkData = yield* lifecycleRequest({
                    type: "fork",
                    entryId: boundary.entryId,
                  });
                  if (recordField(forkData, "cancelled") === true)
                    return yield* protocolError("A Prime extension cancelled the session fork");
                  const forkState = yield* request({ type: "get_state" }).pipe(
                    Effect.onError(() =>
                      Effect.sync(() => {
                        threadState = null;
                      }),
                    ),
                  );
                  const forkSessionFile = recordString(forkState, "sessionFile");
                  if (forkSessionFile === undefined) {
                    threadState = null;
                    return yield* protocolError(
                      "Prime fork did not return a persisted session file",
                    );
                  }
                  // RPC responses bypass the pump. Drain preceding native events
                  // before deciding whether this fork can replace their binding.
                  const queued = yield* Queue.offer(connection.events, {
                    type: "t3.rollback_barrier",
                    barrier,
                  });
                  if (!queued)
                    return yield* protocolError("Prime transport closed during rollback");
                  yield* Deferred.await(barrier);
                  return yield* sessionEventPermit.withPermits(1)(
                    Effect.gen(function* () {
                      if (pendingWake !== null) {
                        yield* stopPendingWake(pendingWake);
                        return yield* protocolError(
                          "Native Prime work started while rollback was changing the session",
                        );
                      }
                      if (
                        threadState !== state ||
                        state.activeTurn !== null ||
                        closed ||
                        stopRequested
                      )
                        return yield* protocolError(
                          "Prime rollback ownership changed during its fork",
                        );
                      lastNativeThreadId = forkSessionFile;
                      appliedModel = null;
                      appliedThinking = null;
                      requestedSessionName = null;
                      yield* updateProviderThread(state, {
                        nativeThreadRef: providerRef(forkSessionFile),
                        nativeConversationHeadRef: null,
                      });
                      rollbackBarrier = null;
                      return primeThreadSnapshot(state.providerThread);
                    }),
                  );
                }).pipe(Effect.onInterrupt(() => connection.terminate)),
              (barrier) =>
                Effect.sync(() => {
                  if (rollbackBarrier === barrier) rollbackBarrier = null;
                }),
            );
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterRollbackThreadError({
                  driver: PRIME_PROVIDER,
                  providerThreadId: rollbackInput.providerThread.id,
                  checkpointId: rollbackInput.target.checkpointId,
                  cause,
                }),
            ),
          ),
        forkThread: (forkInput) =>
          Effect.gen(function* () {
            if (threadState?.activeTurn != null) {
              return yield* protocolError("Cannot fork while a Prime turn is active");
            }
            const source = forkInput.sourceProviderThread;
            const sourceFile = source.nativeThreadRef?.nativeId;
            if (sourceFile == null)
              return yield* protocolError("Prime fork source has no session file");
            const sourceTurns = (forkInput.sourceProviderTurns ?? []).filter(
              (turn) => turn.providerThreadId === source.id,
            );
            const target = sourceTurns.find((turn) => turn.id === forkInput.providerTurnId);
            if (forkInput.providerTurnId !== undefined && target === undefined) {
              return yield* protocolError("Prime fork target turn is missing");
            }
            const discarded =
              target === undefined ? undefined : primeDiscardedHistory(sourceTurns, target.ordinal);
            if (discarded?.type === "unknown") {
              return yield* protocolError(
                "Prime fork source has a turn with no recorded session entries",
              );
            }
            if (discarded?.type === "stranded") {
              return yield* new PrimeRollbackBoundaryError({
                operation: "fork",
                turnOrdinal: discarded.ordinal,
                lead: discarded.lead,
              });
            }
            // The source's projection still lists turns a rollback discarded.
            const forkBoundary = (messages: ReadonlyArray<PrimeForkMessage>, ordinal: number) =>
              Effect.gen(function* () {
                const boundary = primeForkBoundary({
                  messages,
                  turns: sourceTurns,
                  boundaryOrdinal: ordinal,
                  skipUnmatched: true,
                });
                if (boundary.type === "error") return yield* protocolError(boundary.message);
                if (boundary.type === "stranded") {
                  return yield* new PrimeRollbackBoundaryError({
                    operation: "fork",
                    turnOrdinal: boundary.ordinal,
                    lead: boundary.lead,
                  });
                }
                return boundary;
              });
            // `--fork` writes the session copy before any RPC can be asked
            // (main.ts createSessionManager -> SessionManager.forkFrom), and core
            // forks on the target thread's runtime, so the source's Prime process
            // may not exist. Reading the source file read-only, as Prime will read
            // it, refuses an unsupported cut before anything is copied.
            if (target !== undefined) {
              const reader = primeSessionForkReader();
              const read = yield* fileSystem.stream(sourceFile).pipe(
                Stream.decodeText,
                Stream.splitLines,
                Stream.runForEach((line) => Effect.sync(() => reader.line(line))),
                Effect.result,
              );
              if (read._tag === "Failure") {
                return yield* protocolError(
                  `Prime could not read the fork source session: ${read.failure.message}. The source thread is unchanged`,
                );
              }
              const sourceMessages = reader.result();
              if (sourceMessages.type === "invalid") {
                return yield* protocolError(
                  `Prime cannot fork this source session: ${sourceMessages.reason}. The source thread is unchanged`,
                );
              }
              yield* forkBoundary(sourceMessages.messages, target.ordinal);
            }
            // CLI --fork uses Prime's own session format and sets the destination cwd. RPC
            // switch_session/clone alone would retain the source cwd.
            const forkLaunch = buildPrimeRpcLaunch({
              launchArgs: resolvedLaunchArgs.args,
              environment: options.environment,
              mcpSession: undefined,
              extensionPath: undefined,
              disableExtensions: true,
              disableTools: true,
            });
            const nativeId = yield* Effect.scoped(
              Effect.gen(function* () {
                const forkConnection = yield* makePrimeRpcConnection({
                  command: options.settings.binaryPath || "prime-agent",
                  args: [...forkLaunch.args, "--fork", sourceFile],
                  cwd,
                  env: forkLaunch.env,
                });
                yield* Stream.fromQueue(forkConnection.events).pipe(
                  Stream.runDrain,
                  Effect.ignore,
                  Effect.forkScoped,
                );
                if (target !== undefined) {
                  const forkMessages = primeForkMessages(
                    yield* forkConnection.request({ type: "get_fork_messages" }),
                  );
                  if (forkMessages === undefined)
                    return yield* protocolError("Prime returned malformed fork messages");
                  // The copy is what Prime cuts, so it decides again if the source
                  // changed after the read above.
                  const boundary = yield* forkBoundary(forkMessages, target.ordinal);
                  if (boundary.type === "fork") {
                    const result = yield* forkConnection.request({
                      type: "fork",
                      entryId: boundary.entryId,
                    });
                    if (recordField(result, "cancelled") === true) {
                      return yield* protocolError("A Prime extension cancelled the session fork");
                    }
                  }
                }
                const state = yield* forkConnection.request({ type: "get_state" });
                const file = recordString(state, "sessionFile");
                if (file === undefined || file === sourceFile) {
                  return yield* protocolError("Prime fork did not create a distinct session file");
                }
                return file;
              }),
            ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
            const now = yield* DateTime.now;
            return yield* registerThread(
              {
                threadId: forkInput.targetThreadId,
                modelSelection: forkInput.modelSelection ?? input.modelSelection,
                runtimePolicy: forkInput.runtimePolicy ?? input.runtimePolicy,
                existingProviderThread: {
                  ...source,
                  id: idAllocator.derive.providerThread({
                    driver: PRIME_PROVIDER,
                    nativeThreadId: nativeId,
                  }),
                  appThreadId: forkInput.targetThreadId,
                  providerSessionId: input.providerSessionId,
                  providerInstanceId: options.instanceId,
                  ownerNodeId: forkInput.ownerNodeId ?? null,
                  nativeThreadRef: providerRef(nativeId),
                  nativeConversationHeadRef: null,
                  firstRunOrdinal: null,
                  lastRunOrdinal: null,
                  handoffIds: [],
                  pendingBackgroundTasks: [],
                  forkedFrom: {
                    providerThreadId: source.id,
                    ...(forkInput.providerTurnId === undefined
                      ? {}
                      : { providerTurnId: forkInput.providerTurnId }),
                  },
                  createdAt: now,
                  updatedAt: now,
                },
              },
              false,
            );
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterForkThreadError({
                  driver: PRIME_PROVIDER,
                  providerThreadId: forkInput.sourceProviderThread.id,
                  cause,
                }),
            ),
          ),
      };
      return runtime;
    }),
  });
});

function rollbackBoundaryOrdinal(
  target:
    | { readonly type: "thread_start" }
    | { readonly type: "provider_turn"; readonly providerTurn: OrchestrationV2ProviderTurn },
): number {
  return target.type === "thread_start" ? 0 : target.providerTurn.ordinal;
}

function primeSubagentOutput(result: unknown): string {
  const stopReason = recordString(result, "stopReason");
  const failed =
    (recordNumber(result, "exitCode") ?? 0) !== 0 ||
    stopReason === "error" ||
    stopReason === "aborted";
  if (failed) {
    const failure = recordString(result, "errorMessage") || recordString(result, "stderr");
    if (failure !== undefined && failure.length > 0) return failure;
  }
  const messages = recordField(result, "messages");
  if (!Array.isArray(messages)) return "";
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (recordString(message, "role") !== "assistant") continue;
    const text = contentText(recordField(message, "content"));
    if (text.length > 0) return text;
  }
  return "";
}

function primeExtensionDisplayName(extensionPath: string | undefined): string {
  if (extensionPath === undefined) return "Prime extension";
  const normalized = extensionPath.replace(/\\/g, "/").replace(/\/+$/, "");
  const name = normalized.slice(normalized.lastIndexOf("/") + 1).replace(/\.[^.]+$/, "");
  return name.length === 0 ? "Prime extension" : name;
}

function primeThreadSnapshot(
  providerThread: OrchestrationV2ProviderThread,
): ProviderAdapter.ProviderAdapterV2ThreadSnapshot {
  return { providerThread, providerTurns: [], messages: [], runtimeRequests: [] };
}

function primeQuestion(
  questionId: string,
  method: "select" | "input" | "editor",
  title: string,
  event: PrimeRpcRecord,
): OrchestrationV2UserInputQuestion {
  const options =
    method === "select" && Array.isArray(event["options"])
      ? event["options"]
          .filter((option): option is string => typeof option === "string")
          .map((option) => ({ label: option || "Empty value", description: option, value: option }))
      : method === "editor"
        ? []
        : [
            {
              label: "Submit empty value",
              description: "Send an empty string to the extension.",
              value: "",
            },
          ];
  const question = recordString(event, "message") ?? recordString(event, "placeholder") ?? title;
  return {
    id: questionId,
    header: title,
    question,
    ...(method === "editor" ? { initialAnswer: recordString(event, "prefill") ?? "" } : {}),
    options,
  };
}

function primeUiResponse(
  pending: PendingPrimePrompt,
  decision: ProviderApprovalDecision | undefined,
  answers: Record<string, unknown> | undefined,
): PrimeRpcRecord {
  if (pending.method === "confirm") {
    if (decision === "accept" || decision === "acceptForSession") return { confirmed: true };
    if (decision === "decline") return { confirmed: false };
    return { cancelled: true };
  }
  const answer = answers?.[pending.questionId];
  // An empty string is a valid dialog value per the RPC spec (the extension
  // receives ""), distinct from cancelling (the extension receives undefined).
  if (typeof answer === "string") return { value: answer };
  return { cancelled: true };
}

export type PrimeAdapterV2DriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | FileSystem.FileSystem
  | IdAllocator.IdAllocatorV2
  | McpProviderSessions.McpProviderSessions
  | ProviderHost.ProviderHost;

export const PrimeAdapterV2Driver: ProviderAdapterDriver<PrimeSettings, PrimeAdapterV2DriverEnv> = {
  driverKind: PRIME_DRIVER_KIND,
  configSchema: PrimeSettings,
  defaultConfig: (): PrimeSettings => DEFAULT_PRIME_SETTINGS,
  create: Effect.fn("PrimeAdapterV2Driver.create")(
    function* (input: ProviderAdapterDriverCreateInput<PrimeSettings>) {
      const hostEnvironment = yield* HostProcess.Environment;
      const continuationRequests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
      return yield* makePrimeAdapterV2({
        instanceId: input.instanceId,
        settings: { ...input.config, enabled: input.enabled },
        environment: yield* mergeProviderInstanceEnvironment(input.environment, hostEnvironment),
        continuationRequests,
      });
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterDriverCreateError({
              driver: PRIME_DRIVER_KIND,
              instanceId: input.instanceId,
              detail: "Failed to create Prime adapter.",
              cause,
            }),
        ),
      ),
  ),
};
