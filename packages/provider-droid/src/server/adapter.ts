import { AgentScope } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import {
  ProviderDriverKind,
  type ChatAttachment,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2PlanArtifact,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2ProviderRef,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
  type ProviderAdapterDriverCreateInput,
} from "@t3tools/provider-core/server/adapterDriver";
import {
  makeProviderFailure,
  makeProviderFailureTurnItem,
} from "@t3tools/provider-core/server/failure";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import { turnScopedSelectionTransition } from "@t3tools/provider-core/server/selectionTransition";

import { DroidSettings } from "../settings.ts";
import { DroidAuthSignal } from "./authSignal.ts";
import {
  createDroidTurnState,
  DROID_ZERO_COUNTS,
  droidCompactionEvents,
  mapDroidNotification,
  mapDroidProposedPlan,
  settleDroidTurn,
  type DroidEventContext,
  type DroidMapperState,
  type DroidNotification,
} from "./events.ts";
import {
  droidForkRewindMessageId,
  droidMessageIds,
  droidRollbackMessageId,
  visibleDroidHistory,
  type DroidHistoryMessage,
} from "./history.ts";
import {
  DROID_ASK_USER_CANCELLED,
  DROID_PICK_OPTIONS_CANCELLED,
  type DroidTokenCounts,
} from "./protocol.ts";
import {
  DROID_CANCEL_OPTION,
  DROID_EXIT_SPEC_MODE,
  droidApprovalRequest,
  droidAskUserReply,
  droidExitSpecModePlan,
  droidPermissionReply,
  droidUserInputRequest,
  type DroidApprovalRequest,
  type DroidUserInputRequest,
} from "./requests.ts";
import {
  DroidRpcTerminatedError,
  makeDroidRpcRuntime,
  makeRedactor,
  type DroidRpcEvent,
} from "./rpc.ts";
import {
  droidHoldsSettings,
  droidModelCatalog,
  droidSessionSettings,
  droidSettingsParams,
  resolveDroidModelId,
  withDroidReport,
  type DroidKnownSettings,
  type DroidModelInfo,
  type DroidSessionSettings,
} from "./sessionSettings.ts";
import { definedDroidEnvironment } from "./status.ts";

export const DROID_PROVIDER = ProviderDriverKind.make("droid");
const DEFAULT_DROID_SETTINGS = Schema.decodeSync(DroidSettings)({});

const DROID_SIGN_IN_HINT = "Set FACTORY_API_KEY or run `droid` once to sign in.";

const DroidProviderCapabilitiesV2 = {
  // Droid enforces the autonomy level itself; t3 sends it and never gates tools.
  runtimePolicy: { enforcement: "native" },
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: true,
    supportsProviderSwitchingViaHandoff: true,
    supportsRuntimeModeSwitchInSession: true,
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
    exposesNativeTurnId: true,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: true,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: false,
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
    supportsCommandApproval: false,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: false,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: false,
    approvalCallbacksAreLiveOnly: false,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: true,
    supportsStructuredQuestions: true,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: false,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: false,
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
    providerRollbackReturnsSnapshot: true,
    providerCanReadConversationSnapshot: true,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "strong",
    nativeItemIds: "strong",
    nativeRequestIds: "strong",
  },
} satisfies OrchestrationV2ProviderCapabilities;

export interface DroidAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly settings: DroidSettings;
  readonly environment: NodeJS.ProcessEnv;
}

type TurnDisposition = "reusable" | "broken";

type TurnOutcome =
  | { readonly kind: "completed" | "interrupted"; readonly disposition: TurnDisposition }
  | {
      readonly kind: "failed";
      readonly failure: OrchestrationV2ProviderFailure;
      readonly disposition: TurnDisposition;
    };

interface ActiveDroidTurn {
  readonly turnInput: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly providerTurn: OrchestrationV2ProviderTurn;
  mapper: DroidMapperState;
  /** The latest `error` notification's text: Droid sends the reason before the failing completion. */
  errorMessage: string | undefined;
  /** Droid completed the native turn this t3 turn started. */
  nativeSettled: boolean;
  /**
   * Steers Droid accepted but has not echoed yet, by message id with the text sent. Droid queues
   * a steer behind the running native turn, so it may only start after that turn's completion:
   * the t3 turn waits for it.
   */
  readonly queuedSteers: Map<string, string>;
  /** Steers Droid runs as native turns of their own; each owes an `agent_turn_completed`. */
  readonly awaitingNative: Set<string>;
  /**
   * Set on a compaction turn until its item settles. The turn has no native turn behind it, so
   * the item settles with the turn, whichever way that ends.
   */
  compaction: { readonly ordinal: number; readonly startedAt: DateTime.Utc } | undefined;
}

type PendingDroidRequestKind =
  | { readonly type: "permission"; readonly request: DroidApprovalRequest }
  | { readonly type: "user_input"; readonly request: DroidUserInputRequest };

/** An inbound Droid request t3 has shown and Droid still waits on. */
interface PendingDroidRequest {
  readonly inboundId: string;
  readonly kind: PendingDroidRequestKind;
  readonly turn: ActiveDroidTurn;
  readonly runtimeRequest: OrchestrationV2RuntimeRequest;
  readonly node: OrchestrationV2ExecutionNode;
  readonly turnItem: OrchestrationV2TurnItem;
}

interface DroidThreadState {
  providerThread: OrchestrationV2ProviderThread;
  activeTurn: ActiveDroidTurn | null;
  history: ReadonlyArray<DroidHistoryMessage>;
  /** The thread's newest proposed plan; a later proposal supersedes it. */
  latestPlan: OrchestrationV2PlanArtifact | undefined;
}

const MAX_ERROR_MESSAGE_CHARS = 1_000;

/** Droid names the session a rewind forks after this. */
const DROID_REWIND_TITLE = "T3 Code rewind";

/** Droid names the session a fork creates after this, and the rewind of a fork from an earlier turn. */
const DROID_FORK_TITLE = "T3 Code fork";

const isRpcTerminated = Schema.is(DroidRpcTerminatedError);

const field = (value: unknown, key: string): unknown =>
  Predicate.isObject(value) ? value[key] : undefined;

const describeFailure = (error: unknown): string =>
  Predicate.isError(error) ? error.message : String(error);

const droidRef = (nativeId: string): OrchestrationV2ProviderRef => ({
  driver: DROID_PROVIDER,
  nativeId,
  strength: "strong",
});

function terminalFailure(
  reason: string,
  errorMessage: string | undefined,
): OrchestrationV2ProviderFailure {
  switch (reason) {
    case "model_authentication_failed":
      return makeProviderFailure({
        message: `${errorMessage ?? "Droid could not authenticate."} ${DROID_SIGN_IN_HINT}`,
        class: "provider_error",
        code: reason,
        retryable: false,
      });
    case "model_rate_limited":
      return makeProviderFailure({
        message: errorMessage ?? "Droid hit a model rate limit. Try again in a moment.",
        class: "provider_error",
        code: reason,
        retryable: true,
      });
    default:
      return makeProviderFailure({
        message: errorMessage ?? `Droid ended the turn with reason ${reason}.`,
        class: "provider_error",
        code: reason,
        retryable: false,
      });
  }
}

/**
 * Droid reports `process_exit` when it is shutting down (stdin closed, SIGTERM) and exits right
 * after, so the session is gone whether this arrives as a completion or as the EOF behind it.
 * During a close the turn was stopped on purpose.
 */
function processExitOutcome(closing: boolean, message: string): TurnOutcome {
  return closing
    ? { kind: "interrupted", disposition: "broken" }
    : {
        kind: "failed",
        failure: makeProviderFailure({
          message,
          class: "transport_error",
          code: "process_exit",
          retryable: false,
        }),
        disposition: "broken",
      };
}

function classifyCompletion(
  reason: unknown,
  errorMessage: string | undefined,
  closing: boolean,
): TurnOutcome {
  switch (reason) {
    case undefined:
    case "completed":
    case "spec_handoff":
      return { kind: "completed", disposition: "reusable" };
    case "cancelled":
    case "permission_rejected":
      return { kind: "interrupted", disposition: "reusable" };
    case "process_exit":
      return processExitOutcome(closing, errorMessage ?? "Droid exited before the turn finished.");
    default:
      return {
        kind: "failed",
        failure: terminalFailure(String(reason), errorMessage),
        disposition: "reusable",
      };
  }
}

/** Droid's echo of a queued steer tells whether it joined the running native turn or began one. */
function noteQueuedSteerStarted(turn: ActiveDroidTurn, notification: DroidNotification): void {
  const message = notification["message"];
  const messageId = field(message, "id");
  if (field(message, "role") !== "user" || !Predicate.isString(messageId)) return;
  if (!turn.queuedSteers.delete(messageId)) return;
  if (turn.nativeSettled && turn.awaitingNative.size === 0) turn.awaitingNative.add(messageId);
}

/**
 * Droid names a dropped message by its text and the JSON-RPC id of the frame that queued it, an
 * id t3 never sees, so the text picks the steer. One notification covers one message.
 */
function dropDiscardedSteer(turn: ActiveDroidTurn, notification: DroidNotification): void {
  const text = notification["text"];
  if (!Predicate.isString(text)) {
    turn.queuedSteers.clear();
    return;
  }
  for (const [messageId, queued] of turn.queuedSteers) {
    if (queued === text) {
      turn.queuedSteers.delete(messageId);
      return;
    }
  }
}

const cancelReply = (kind: PendingDroidRequestKind): unknown =>
  kind.type === "permission"
    ? { selectedOption: kind.request.cancelOptionId }
    : DROID_ASK_USER_CANCELLED;

export const makeDroidAdapterV2 = Effect.fn("makeDroidAdapterV2")(function* (
  options: DroidAdapterV2Options,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const agentScope = yield* AgentScope;
  const fileSystem = yield* FileSystem.FileSystem;
  const pathService = yield* Path.Path;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const host = yield* ProviderHost.ProviderHost;
  const authSignal = yield* DroidAuthSignal;
  const hostEnvironment = yield* HostProcess.Environment;
  const processEnvironment = definedDroidEnvironment(options.environment);
  const redactor = makeRedactor(
    processEnvironment["FACTORY_API_KEY"] ?? hostEnvironment["FACTORY_API_KEY"],
  );

  const protocolError = (detail: string) =>
    new ProviderAdapter.ProviderAdapterProtocolError({ driver: DROID_PROVIDER, detail });

  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: DROID_PROVIDER,
    getCapabilities: () => Effect.succeed(DroidProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed(turnScopedSelectionTransition()),
    openSession: Effect.fn("DroidAdapterV2.openSession")(function* (
      input: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
    ) {
      const scope = yield* Effect.scope;
      const cwd = input.runtimePolicy.cwd ?? host.paths.cwd;
      const rpc = yield* makeDroidRpcRuntime({
        binaryPath: options.settings.binaryPath || "droid",
        cwd,
        env: processEnvironment,
        wrapLaunch: (launch) =>
          agentScope.wrap({
            ...launch,
            name: "droid",
            threadId: input.threadId,
            env: processEnvironment,
          }),
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(FileSystem.FileSystem, fileSystem),
        Effect.provideService(Path.Path, pathService),
        Effect.mapError(
          (cause) =>
            new ProviderAdapter.ProviderAdapterOpenSessionError({
              driver: DROID_PROVIDER,
              providerSessionId: input.providerSessionId,
              cause,
            }),
        ),
      );
      const now = yield* DateTime.now;
      let sessionEntity: OrchestrationV2ProviderSession = {
        id: input.providerSessionId,
        driver: DROID_PROVIDER,
        providerInstanceId: options.instanceId,
        status: "ready",
        cwd,
        model: resolveDroidModelId(input.modelSelection.model),
        capabilities: DroidProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const events = yield* Queue.unbounded<
        ProviderAdapter.ProviderAdapterV2Event,
        ProviderAdapter.ProviderAdapterV2Error | Cause.Done
      >();
      // Event handling and the start of a turn exclude each other, so a terminal cannot overtake
      // the events that open its turn and a turn is never installed while another settles.
      const permit = yield* Semaphore.make(1);

      let threadState: DroidThreadState | null = null;
      /**
       * The settings Droid is known to hold: what the adapter sent, what `initialize_session` and
       * `load_session` reported, and what `settings_updated` announced since.
       */
      let known: DroidKnownSettings = {};
      /** `load_session` takes no settings, so the next turn sends them whatever `known` says. */
      let settingsOwed = false;
      /** The model list the latest `initialize_session` or `load_session` reported. */
      let models: ReadonlyArray<DroidModelInfo> | undefined;
      /**
       * The policy and selection the thread last asked with. Only a turn names them and a rollback
       * names neither, so its frame is computed from these against the rewound session's models.
       */
      let asked = {
        runtimePolicy: input.runtimePolicy,
        modelSelection: input.modelSelection,
      };
      let sessionUsage: DroidTokenCounts | undefined;
      let closing = false;
      let ended = false;
      /** A stop killed the process on purpose: its exit ends the session without failing anything. */
      let stopRequested = false;
      /** Resolved once the event stream has ended and the session has published how. */
      const eventsEnded = yield* Deferred.make<void>();
      /** Keyed by the runtime request id; an entry exists exactly while Droid waits on the request. */
      const pendingRequests = new Map<string, PendingDroidRequest>();

      const logWarning = (message: string) => Effect.logWarning(redactor.text(message));
      const logDebug = (message: string) => Effect.logDebug(redactor.text(message));
      const request = (method: string, params: object = {}) => rpc.request(method, params);
      const boundNativeId = () => threadState?.providerThread.nativeThreadRef?.nativeId;

      const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
        Queue.offer(events, event).pipe(Effect.asVoid);

      const updateProviderSession = (
        status: OrchestrationV2ProviderSession["status"],
        lastError: string | null = sessionEntity.lastError,
        model: string | null = sessionEntity.model,
      ) =>
        Effect.gen(function* () {
          const updatedAt = yield* DateTime.now;
          sessionEntity = { ...sessionEntity, status, lastError, model, updatedAt };
          yield* emit({
            type: "provider_session.updated",
            driver: DROID_PROVIDER,
            providerSession: sessionEntity,
          });
        });

      const updateProviderThread = (
        state: DroidThreadState,
        patch: Partial<OrchestrationV2ProviderThread>,
      ) =>
        Effect.gen(function* () {
          const updatedAt = yield* DateTime.now;
          state.providerThread = { ...state.providerThread, ...patch, updatedAt };
          yield* emit({
            type: "provider_thread.updated",
            driver: DROID_PROVIDER,
            providerThread: state.providerThread,
          });
        });

      const eventContext = (state: DroidThreadState, turn: ActiveDroidTurn): DroidEventContext => ({
        threadId: turn.turnInput.threadId,
        runId: turn.turnInput.runId,
        rootNodeId: turn.turnInput.rootNodeId,
        providerThread: state.providerThread,
        providerTurn: turn.providerTurn,
        derive: idAllocator.derive,
      });

      /** The mapper builds `provider_thread.updated` from the thread it was given, so adopt each one. */
      const emitMapped = Effect.fnUntraced(function* (
        state: DroidThreadState,
        events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>,
      ) {
        for (const event of events) {
          if (event.type === "provider_thread.updated") state.providerThread = event.providerThread;
          yield* emit(event);
        }
      });

      const openRequest = Effect.fnUntraced(function* (
        state: DroidThreadState,
        turn: ActiveDroidTurn,
        inboundId: string,
        kind: PendingDroidRequestKind,
      ) {
        const createdAt = yield* DateTime.now;
        const requestId = yield* idAllocator.allocate.runtimeRequest({
          driver: DROID_PROVIDER,
          providerTurnId: turn.providerTurn.id,
          nativeRequestId: inboundId,
        });
        const nodeId = idAllocator.derive.approvalNode({ requestId });
        const nativeRef = droidRef(inboundId);
        const ordinal = turn.mapper.nextItemOrdinal;
        turn.mapper = { ...turn.mapper, nextItemOrdinal: ordinal + 1 };
        const runtimeRequest: OrchestrationV2RuntimeRequest = {
          id: requestId,
          nodeId,
          providerTurnId: turn.providerTurn.id,
          nativeRequestRef: nativeRef,
          kind: kind.type === "permission" ? kind.request.requestKind : "user_input",
          status: "pending",
          responseCapability: { type: "live", providerSessionId: input.providerSessionId },
          createdAt,
          resolvedAt: null,
        };
        const node: OrchestrationV2ExecutionNode = {
          id: nodeId,
          threadId: turn.turnInput.threadId,
          runId: turn.turnInput.runId,
          parentNodeId: turn.turnInput.rootNodeId,
          rootNodeId: turn.turnInput.rootNodeId,
          kind: kind.type === "permission" ? "approval_request" : "user_input_request",
          status: "waiting",
          countsForRun: false,
          providerThreadId: state.providerThread.id,
          providerTurnId: turn.providerTurn.id,
          nativeItemRef: nativeRef,
          runtimeRequestId: requestId,
          checkpointScopeId: null,
          startedAt: createdAt,
          completedAt: null,
        };
        const itemBase = {
          id: idAllocator.derive.approvalTurnItem({ requestId }),
          threadId: turn.turnInput.threadId,
          runId: turn.turnInput.runId,
          nodeId,
          providerThreadId: state.providerThread.id,
          providerTurnId: turn.providerTurn.id,
          nativeItemRef: nativeRef,
          parentItemId: null,
          ordinal,
          status: "waiting",
          startedAt: createdAt,
          completedAt: null,
          updatedAt: createdAt,
        } as const;
        const turnItem: OrchestrationV2TurnItem =
          kind.type === "permission"
            ? {
                ...itemBase,
                title: kind.request.title,
                type: "approval_request",
                requestId,
                requestKind: kind.request.requestKind,
                ...(kind.request.prompt === undefined ? {} : { prompt: kind.request.prompt }),
                options: kind.request.options,
              }
            : {
                ...itemBase,
                title: kind.request.questions[0]?.header ?? "Droid question",
                type: "user_input_request",
                requestId,
                questions: kind.request.questions,
              };
        pendingRequests.set(String(requestId), {
          inboundId,
          kind,
          turn,
          runtimeRequest,
          node,
          turnItem,
        });
        yield* emit({
          type: "runtime_request.updated",
          driver: DROID_PROVIDER,
          threadId: node.threadId,
          runtimeRequest,
        });
        yield* emit({ type: "node.updated", driver: DROID_PROVIDER, node });
        yield* emit({ type: "turn_item.updated", driver: DROID_PROVIDER, turnItem });
      });

      /** The caller has already removed `pending` from the registry. */
      const settleRequest = Effect.fnUntraced(function* (
        pending: PendingDroidRequest,
        status: "resolved" | "cancelled",
        outcome: Pick<OrchestrationV2RuntimeRequest, "decision" | "answers">,
      ) {
        const resolvedAt = yield* DateTime.now;
        yield* emit({
          type: "runtime_request.updated",
          driver: DROID_PROVIDER,
          threadId: pending.node.threadId,
          runtimeRequest: { ...pending.runtimeRequest, ...outcome, status, resolvedAt },
        });
        yield* emit({
          type: "node.updated",
          driver: DROID_PROVIDER,
          node: {
            ...pending.node,
            status: status === "resolved" ? "completed" : "cancelled",
            completedAt: resolvedAt,
          },
        });
        yield* emit({
          type: "turn_item.updated",
          driver: DROID_PROVIDER,
          turnItem: {
            ...pending.turnItem,
            status: status === "resolved" ? "completed" : "cancelled",
            completedAt: resolvedAt,
            updatedAt: resolvedAt,
          },
        });
      });

      /**
       * Droid waits on every inbound request it sent, so a request t3 stops showing is answered
       * with its cancel shape first. `reply` is false once the process is gone.
       */
      const cancelOpenRequests = Effect.fnUntraced(function* (reply: boolean) {
        const open = [...pendingRequests.values()];
        pendingRequests.clear();
        for (const pending of open) {
          if (reply) {
            yield* rpc.respond(pending.inboundId, cancelReply(pending.kind)).pipe(Effect.ignore);
          }
          yield* settleRequest(pending, "cancelled", { decision: "cancel" });
        }
      });

      /** Adopts the orchestrator's row when there is one so its identity survives the new session. */
      const bindThread = (
        sessionId: string,
        row: OrchestrationV2ProviderThread | undefined,
        appThreadId: OrchestrationV2ProviderThread["appThreadId"],
        at: DateTime.Utc,
      ): OrchestrationV2ProviderThread =>
        row !== undefined
          ? {
              ...row,
              providerSessionId: input.providerSessionId,
              nativeThreadRef: droidRef(sessionId),
              status: "idle",
              updatedAt: at,
            }
          : {
              id: idAllocator.derive.providerThread({
                driver: DROID_PROVIDER,
                nativeThreadId: sessionId,
              }),
              driver: DROID_PROVIDER,
              providerInstanceId: options.instanceId,
              providerSessionId: input.providerSessionId,
              appThreadId,
              ownerNodeId: null,
              nativeThreadRef: droidRef(sessionId),
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              pendingBackgroundTasks: [],
              contextUsage: null,
              nativeMetadata: null,
              goal: null,
              createdAt: at,
              updatedAt: at,
            };

      /**
       * Settles the turn and emits its one terminal. Detaching the turn first is what makes a
       * duplicate or late completion harmless.
       */
      const finishTurn = Effect.fnUntraced(function* (
        state: DroidThreadState,
        turn: ActiveDroidTurn,
        outcome: TurnOutcome,
      ) {
        if (state.activeTurn !== turn) return;
        state.activeTurn = null;
        // Before the terminal: no request may outlive the turn it belongs to.
        yield* cancelOpenRequests(!ended);
        if (turn.compaction !== undefined) {
          const { ordinal, startedAt } = turn.compaction;
          turn.compaction = undefined;
          yield* emitMapped(
            state,
            droidCompactionEvents(
              eventContext(state, turn),
              {
                ordinal,
                startedAt,
                status:
                  outcome.kind === "completed"
                    ? "completed"
                    : outcome.kind === "failed"
                      ? "failed"
                      : "cancelled",
              },
              yield* DateTime.now,
            ),
          );
        }
        const completedAt = yield* DateTime.now;
        const settled = settleDroidTurn(
          turn.mapper,
          eventContext(state, turn),
          outcome.kind,
          completedAt,
        );
        turn.mapper = settled.state;
        yield* emitMapped(state, settled.events);
        sessionUsage = turn.mapper.sessionUsage;

        yield* emit({
          type: "provider_turn.updated",
          driver: DROID_PROVIDER,
          threadId: turn.turnInput.threadId,
          providerTurn: {
            ...turn.providerTurn,
            status: outcome.kind,
            completedAt,
            ...(turn.mapper.turnTokenUsage === undefined
              ? {}
              : { turnTokenUsage: turn.mapper.turnTokenUsage }),
          },
        });
        yield* updateProviderThread(state, {
          status: outcome.disposition === "broken" ? "error" : "idle",
        });

        const base = {
          type: "turn.terminal",
          driver: DROID_PROVIDER,
          providerThreadId: state.providerThread.id,
          providerTurnId: turn.providerTurn.id,
          runOrdinal: turn.turnInput.runOrdinal,
          threadDisposition: outcome.disposition,
        } as const;
        if (outcome.kind === "failed") {
          const failureItemOrdinal = settled.state.nextItemOrdinal;
          yield* emit({
            type: "turn_item.updated",
            driver: DROID_PROVIDER,
            turnItem: makeProviderFailureTurnItem({
              driver: DROID_PROVIDER,
              threadId: turn.turnInput.threadId,
              runId: turn.turnInput.runId,
              nodeId: turn.turnInput.rootNodeId,
              providerThreadId: state.providerThread.id,
              providerTurnId: turn.providerTurn.id,
              itemOrdinal: failureItemOrdinal,
              failure: outcome.failure,
              occurredAt: completedAt,
            }),
          });
          yield* emit({
            ...base,
            status: "failed",
            failure: outcome.failure,
            failureItemOrdinal,
          });
        } else {
          yield* emit({ ...base, status: outcome.kind, failure: null });
        }
      });

      /** Ends the turn and moves the session to the status the outcome leaves it in. */
      const settleTurn = Effect.fnUntraced(function* (
        state: DroidThreadState,
        turn: ActiveDroidTurn,
        outcome: TurnOutcome,
      ) {
        yield* finishTurn(state, turn, outcome);
        const lastError = outcome.kind === "failed" ? outcome.failure.message : null;
        if (outcome.disposition === "broken") {
          // Droid exits right after this, but the EOF is a separate event: the session must not
          // look ready or take a turn in between.
          ended = true;
          yield* updateProviderSession(closing ? "stopped" : "error", lastError);
        } else {
          yield* updateProviderSession("ready", lastError);
        }
      });

      const settleNativeOutcome = Effect.fnUntraced(function* (
        state: DroidThreadState,
        turn: ActiveDroidTurn,
        outcome: TurnOutcome,
      ) {
        // Signalled before the terminal goes out, so whoever sees the terminal sees the status flip.
        if (outcome.kind === "failed") {
          if (outcome.failure.code === "model_authentication_failed") {
            yield* authSignal.authenticationFailed;
          }
        } else if (outcome.kind === "completed") {
          yield* authSignal.authenticated;
        }
        yield* settleTurn(state, turn, outcome);
      });

      const handleCompletion = Effect.fnUntraced(function* (
        state: DroidThreadState,
        turn: ActiveDroidTurn,
        notification: DroidNotification,
      ) {
        const completedAt = yield* DateTime.now;
        const mapped = mapDroidNotification(
          turn.mapper,
          eventContext(state, turn),
          notification,
          completedAt,
        );
        turn.mapper = mapped.state;
        yield* emitMapped(state, mapped.events);

        const outcome = classifyCompletion(notification["reason"], turn.errorMessage, closing);
        // A steer Droid has queued or is running natively belongs to this t3 turn, so a clean
        // completion of an earlier native turn does not end it. Any other outcome does.
        if (
          outcome.kind === "completed" &&
          (turn.queuedSteers.size > 0 || turn.awaitingNative.size > 0)
        ) {
          return;
        }
        yield* settleNativeOutcome(state, turn, outcome);
      });

      const handleDiscardedSteers = Effect.fnUntraced(function* (
        state: DroidThreadState,
        turn: ActiveDroidTurn,
        notification: DroidNotification,
      ) {
        dropDiscardedSteer(turn, notification);
        if (!turn.nativeSettled || turn.queuedSteers.size > 0 || turn.awaitingNative.size > 0) {
          return;
        }
        // Only a clean completion waits for steers, so that is the outcome the discard releases.
        // A Stop in flight ends it the way the interrupt would have.
        yield* settleNativeOutcome(state, turn, {
          kind: turn.mapper.interruptRequested ? "interrupted" : "completed",
          disposition: "reusable",
        });
      });

      const handleNotification = Effect.fnUntraced(function* (notification: DroidNotification) {
        const state = threadState;
        if (state === null) return;
        if (notification.type === "settings_updated") {
          known = withDroidReport(known, notification["settings"], { interactionMode: false });
          return;
        }
        if (notification.type === "spec_handoff_session_created") {
          // Only a `proceed_new_session*` reply starts one and t3 never sends it. The new session
          // is empty, so the thread keeps the session it has.
          const describeId = (value: unknown) => (Predicate.isString(value) ? value : "unknown");
          yield* logWarning(
            `Droid started session ${describeId(notification["newSessionId"])} for a spec handoff (${describeId(notification["specHandoffId"])}) from session ${boundNativeId()}; the thread stays on its own session.`,
          );
          return;
        }
        const turn = state.activeTurn;
        if (turn === null) return;
        if (notification.type === "error") {
          const message = field(notification, "message");
          if (Predicate.isString(message) && message.trim().length > 0) {
            turn.errorMessage = redactor.text(message.trim()).slice(0, MAX_ERROR_MESSAGE_CHARS);
          }
          return;
        }
        if (notification.type === "agent_turn_completed") {
          const nativeTurnId = notification["turnId"];
          if (nativeTurnId === turn.turnInput.message.messageId && !turn.nativeSettled) {
            turn.nativeSettled = true;
          } else if (
            !Predicate.isString(nativeTurnId) ||
            !turn.awaitingNative.delete(nativeTurnId)
          ) {
            return;
          }
          return yield* handleCompletion(state, turn, notification);
        }
        if (notification.type === "queued_messages_discarded") {
          return yield* handleDiscardedSteers(state, turn, notification);
        }
        if (notification.type === "create_message") noteQueuedSteerStarted(turn, notification);
        const mapped = mapDroidNotification(
          turn.mapper,
          eventContext(state, turn),
          notification,
          yield* DateTime.now,
        );
        if (mapped.unknownType !== undefined) {
          yield* logDebug(
            `Dropping a Droid notification of an unknown type: ${mapped.unknownType}`,
          );
          return;
        }
        turn.mapper = mapped.state;
        yield* emitMapped(state, mapped.events);
      });

      const projectProposedPlan = Effect.fnUntraced(function* (
        state: DroidThreadState,
        turn: ActiveDroidTurn,
        sourceId: string,
        markdown: string,
      ) {
        const planId = yield* idAllocator.allocate.plan({
          threadId: turn.turnInput.threadId,
          runId: turn.turnInput.runId,
          driver: DROID_PROVIDER,
        });
        const mapped = mapDroidProposedPlan(
          turn.mapper,
          eventContext(state, turn),
          { planId, sourceId, markdown, previous: state.latestPlan },
          yield* DateTime.now,
        );
        turn.mapper = mapped.state;
        state.latestPlan = mapped.plan;
        yield* emitMapped(state, mapped.events);
      });

      /**
       * Droid asks to leave spec mode once its plan is written. t3 captures the plan as a proposal
       * and cancels the request: Droid stays in spec mode, finishes the turn without implementing,
       * and implementing is a later turn of the user's choosing. The user is never asked, and no
       * `proceed_*` option is ever sent (the `proceed_new_session*` ones would move Droid to a
       * session t3 does not follow).
       */
      const answerExitSpecMode = Effect.fnUntraced(function* (
        state: DroidThreadState | null,
        turn: ActiveDroidTurn | null,
        inboundId: string,
        request: DroidApprovalRequest,
        params: unknown,
      ) {
        // The cancel leaves Droid in spec mode, so the next turn that wants auto must send it.
        known = { ...known, interactionMode: "spec" };
        if (state !== null && turn !== null) {
          const markdown = droidExitSpecModePlan(params);
          if (markdown === undefined) {
            yield* logWarning(
              "Droid sent an exit_spec_mode request without a plan; cancelling it.",
            );
          } else {
            // The reply must go out whatever happens to the projection.
            yield* projectProposedPlan(
              state,
              turn,
              request.toolUseIds[0] ?? inboundId,
              markdown,
            ).pipe(
              Effect.catchCause((cause) =>
                logWarning(`Droid's plan could not be projected: ${Cause.pretty(cause)}`),
              ),
            );
          }
          // Droid reports the cancelled ExitSpecMode as an error result; that is the plan being
          // held for the user, not a failed tool.
          turn.mapper = {
            ...turn.mapper,
            declinedToolUseIds: new Set([...turn.mapper.declinedToolUseIds, ...request.toolUseIds]),
          };
        } else {
          yield* logDebug("Cancelling a Droid exit_spec_mode request that arrived outside a turn.");
        }
        yield* rpc.respond(inboundId, { selectedOption: DROID_CANCEL_OPTION }).pipe(Effect.ignore);
      });

      const handleInbound = Effect.fnUntraced(function* (
        event: Extract<DroidRpcEvent, { readonly _tag: "Request" }>,
      ) {
        const state = threadState;
        const turn = state?.activeTurn ?? null;
        const reply = (result: unknown) => rpc.respond(event.id, result).pipe(Effect.ignore);
        switch (event.method) {
          case "droid.pick_options":
            // No t3 surface offers a list pick, so Droid is told it was declined.
            return yield* reply(DROID_PICK_OPTIONS_CANCELLED);
          case "droid.ask_user": {
            const request = droidUserInputRequest(event.params);
            if (state === null || turn === null || request.native.length === 0) {
              return yield* reply(DROID_ASK_USER_CANCELLED);
            }
            return yield* openRequest(state, turn, event.id, { type: "user_input", request });
          }
          case "droid.request_permission": {
            const request = droidApprovalRequest(event.params);
            if (request.confirmationType === DROID_EXIT_SPEC_MODE) {
              return yield* answerExitSpecMode(state, turn, event.id, request, event.params);
            }
            if (request.confirmationType === "ask_user") {
              // Droid has only been seen asking questions through `droid.ask_user`.
              yield* logWarning(
                "Droid sent a permission request with confirmationType ask_user; cancelling it.",
              );
              return yield* reply({ selectedOption: DROID_CANCEL_OPTION });
            }
            if (state === null || turn === null) {
              yield* logDebug("Cancelling a Droid permission request that arrived outside a turn.");
              return yield* reply({ selectedOption: request.cancelOptionId });
            }
            return yield* openRequest(state, turn, event.id, { type: "permission", request });
          }
        }
      });

      const handleEvent = Effect.fnUntraced(function* (event: DroidRpcEvent) {
        if (event._tag === "Request") return yield* handleInbound(event);
        const bound = boundNativeId();
        if (bound === undefined || (event.sessionId !== undefined && event.sessionId !== bound)) {
          return;
        }
        yield* handleNotification(event.notification);
      });

      const handleEnd = (cause: Cause.Cause<unknown> | undefined) =>
        permit
          .withPermits(1)(
            Effect.gen(function* () {
              ended = true;
              const state = threadState;
              const turn = state?.activeTurn ?? null;
              if (state !== null && turn !== null) {
                yield* finishTurn(
                  state,
                  turn,
                  processExitOutcome(closing, "Droid process exited unexpectedly."),
                );
              }
              const deliberate = closing || stopRequested;
              if (cause !== undefined && !deliberate) {
                yield* logWarning(`Droid process ended: ${Cause.pretty(cause)}`);
              }
              if (deliberate) {
                yield* updateProviderSession("stopped", null);
                yield* Queue.end(events);
              } else {
                yield* updateProviderSession("error", "Droid process exited unexpectedly.");
                yield* Queue.fail(
                  events,
                  new ProviderAdapter.ProviderAdapterEventStreamError({
                    driver: DROID_PROVIDER,
                    providerSessionId: input.providerSessionId,
                    cause,
                  }),
                );
              }
            }),
          )
          .pipe(Effect.ensuring(Deferred.succeed(eventsEnded, undefined)));

      yield* rpc.events.pipe(
        Stream.runForEach((event) =>
          permit.withPermits(1)(
            handleEvent(event).pipe(
              Effect.catchCauseIf(
                (cause) => !Cause.hasInterruptsOnly(cause),
                (cause) => logWarning(`Droid event handling failed: ${Cause.pretty(cause)}`),
              ),
            ),
          ),
        ),
        Effect.matchCauseEffect({
          onSuccess: () => handleEnd(undefined),
          onFailure: (cause) => (Cause.hasInterruptsOnly(cause) ? Effect.void : handleEnd(cause)),
        }),
        Effect.forkIn(scope),
      );

      // Added after the pump so it runs first on close, while the pump can still publish the stop.
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          closing = true;
        }).pipe(Effect.andThen(rpc.close), Effect.ignore),
      );

      /**
       * Sends one full settings frame. `known` takes the frame first so the echoes Droid sends for
       * this very frame refine it rather than race it; a frame that fails leaves nothing known, so
       * the next turn sends the settings again.
       */
      const sendSettings = (settings: DroidSessionSettings) => {
        known = droidSettingsParams(settings);
        settingsOwed = false;
        return request("droid.update_session_settings", droidSettingsParams(settings)).pipe(
          Effect.tapError(() =>
            Effect.sync(() => {
              known = {};
            }),
          ),
        );
      };

      /** The settings a turn or thread asks for, against the models the session reported. Records the ask. */
      const desiredSettings = (
        policy: ProviderAdapter.ProviderAdapterV2RuntimePolicy,
        modelSelection: ProviderAdapter.ProviderAdapterV2TurnInput["modelSelection"],
      ) => {
        asked = { runtimePolicy: policy, modelSelection };
        return droidSessionSettings({
          runtimeMode: policy.runtimeMode,
          interactionMode: policy.interactionMode,
          modelSelection,
          models,
        });
      };

      /** A session answered `initialize_session`: it holds what was sent, as Droid reports it. */
      const noteStartedSession = (sent: DroidSessionSettings, result: unknown) => {
        models = droidModelCatalog(result) ?? models;
        known = withDroidReport(droidSettingsParams(sent), field(result, "settings"), {
          interactionMode: true,
        });
        settingsOwed = false;
      };

      /**
       * A session answered `load_session`. Droid reports what the stored session holds, but the load
       * took no settings, so one full frame still follows before the next message.
       */
      const noteLoadedSession = (result: unknown) => {
        models = droidModelCatalog(result) ?? models;
        known = withDroidReport({}, field(result, "settings"), { interactionMode: true });
        settingsOwed = true;
      };

      /** Opens a new Droid session carrying `settings` and makes it the thread's session. */
      const startSession = Effect.fnUntraced(function* (
        settings: DroidSessionSettings,
        row: OrchestrationV2ProviderThread | undefined,
        appThreadId: OrchestrationV2ProviderThread["appThreadId"],
      ) {
        const result = yield* request("droid.initialize_session", {
          machineId: "default",
          cwd,
          ...droidSettingsParams(settings),
        });
        const sessionId = field(result, "sessionId");
        if (!Predicate.isString(sessionId) || sessionId.length === 0) {
          return yield* protocolError("initialize_session returned no sessionId");
        }
        const at = yield* DateTime.now;
        const providerThread = bindThread(sessionId, row, appThreadId, at);
        const state: DroidThreadState = {
          providerThread,
          activeTurn: null,
          history: [],
          latestPlan: undefined,
        };
        noteStartedSession(settings, result);
        sessionUsage = DROID_ZERO_COUNTS;
        threadState = state;
        yield* emit({
          type: "provider_thread.updated",
          driver: DROID_PROVIDER,
          providerThread,
        });
        return state;
      });

      /**
       * Loads a stored session and makes it the thread's session. The thread keeps its provider
       * thread id and takes `nativeId` as its ref, so this is how a resume and a rewind both land.
       * The state is replaced only once the load has succeeded.
       */
      const adoptSession = Effect.fnUntraced(function* (
        nativeId: string,
        row: OrchestrationV2ProviderThread,
        appThreadId: OrchestrationV2ProviderThread["appThreadId"],
      ) {
        const result = yield* request("droid.load_session", {
          sessionId: nativeId,
          loadAllMessages: true,
        }).pipe(
          Effect.tapError((error) =>
            logWarning(`Droid could not load session ${nativeId}: ${describeFailure(error)}`),
          ),
        );
        const at = yield* DateTime.now;
        const providerThread = bindThread(nativeId, row, appThreadId, at);
        const state: DroidThreadState = {
          providerThread,
          activeTurn: null,
          history: visibleDroidHistory(field(field(result, "session"), "messages")),
          latestPlan: undefined,
        };
        noteLoadedSession(result);
        sessionUsage = undefined;
        threadState = state;
        yield* emit({
          type: "provider_thread.updated",
          driver: DROID_PROVIDER,
          providerThread,
        });
        return state;
      });

      const ensureThread = Effect.fnUntraced(function* (
        threadInput: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
      ) {
        if (ended) return yield* protocolError("The Droid process has exited");
        if (threadState?.activeTurn != null) {
          return yield* protocolError("Cannot start a Droid session while a turn is active");
        }
        threadState = null;
        known = {};
        const settings = desiredSettings(threadInput.runtimePolicy, threadInput.modelSelection);
        const state = yield* startSession(
          settings,
          threadInput.existingProviderThread,
          threadInput.threadId,
        );
        return state.providerThread;
      });

      const resumeThread = Effect.fnUntraced(function* (
        row: OrchestrationV2ProviderThread,
        appThreadId: OrchestrationV2ProviderThread["appThreadId"],
      ) {
        const nativeId = row.nativeThreadRef?.nativeId;
        if (nativeId === undefined || nativeId === null) {
          return yield* protocolError("The provider thread has no Droid session to resume");
        }
        if (ended) return yield* protocolError("The Droid process has exited");
        const current = threadState;
        if (current !== null && boundNativeId() === nativeId) return current.providerThread;
        if (current?.activeTurn != null) {
          return yield* protocolError("Cannot resume a Droid session while a turn is active");
        }
        threadState = null;
        known = {};
        const state = yield* adoptSession(nativeId, row, appThreadId);
        return state.providerThread;
      });

      const resolvePromptPayload = Effect.fnUntraced(function* (
        text: string,
        attachments: ReadonlyArray<ChatAttachment>,
      ) {
        const images: Array<{ type: "base64"; data: string; mediaType: string }> = [];
        const extraLines: Array<string> = [];
        for (const attachment of attachments) {
          const path = host.resolveAttachmentPath(attachment);
          if (path === null) continue;
          if (attachment.mimeType.startsWith("image/")) {
            const bytes = yield* fileSystem.readFile(path);
            images.push({
              type: "base64",
              data: Buffer.from(bytes).toString("base64"),
              mediaType: attachment.mimeType,
            });
          } else {
            extraLines.push(`[Attachment saved at ${path}]`);
          }
        }
        return {
          text: extraLines.length === 0 ? text : `${text}\n\n${extraLines.join("\n")}`,
          images,
        };
      });

      const startTurn = Effect.fnUntraced(function* (
        turnInput: ProviderAdapter.ProviderAdapterV2TurnInput,
      ) {
        const state = threadState;
        if (state === null) return yield* protocolError("Droid session has no registered thread");
        if (ended) return yield* protocolError("The Droid process has exited");
        if (state.activeTurn !== null) {
          return yield* protocolError(
            `Droid provider thread ${turnInput.providerThread.id} already has an active turn`,
          );
        }
        if (
          state.providerThread.nativeThreadRef?.nativeId !==
          turnInput.providerThread.nativeThreadRef?.nativeId
        ) {
          return yield* protocolError("Droid turn requested for a different native session");
        }

        // One frame covers runtime mode, interaction mode, model, and effort, and goes out only
        // when it would change something Droid is known to hold. The runtime mode stays the source
        // of truth for autonomy, so a level Droid raised on its own is sent back here.
        const desired = desiredSettings(turnInput.runtimePolicy, turnInput.modelSelection);
        if (settingsOwed || !droidHoldsSettings(known, desired)) yield* sendSettings(desired);

        const payload = yield* resolvePromptPayload(
          turnInput.message.text,
          turnInput.message.attachments,
        );
        const messageId = String(turnInput.message.messageId);

        yield* Effect.gen(function* () {
          if (
            threadState !== state ||
            state.activeTurn !== null ||
            ended ||
            closing ||
            state.providerThread.nativeThreadRef?.nativeId !==
              turnInput.providerThread.nativeThreadRef?.nativeId
          ) {
            return yield* protocolError("Droid turn ownership changed before it could start");
          }
          const startedAt = yield* DateTime.now;
          state.providerThread = turnInput.providerThread;
          const providerTurn: OrchestrationV2ProviderTurn = {
            id: idAllocator.derive.providerTurn({
              driver: DROID_PROVIDER,
              nativeTurnId: messageId,
            }),
            providerThreadId: turnInput.providerThread.id,
            nodeId: turnInput.rootNodeId,
            runAttemptId: turnInput.attemptId,
            nativeTurnRef: droidRef(messageId),
            ordinal: turnInput.providerTurnOrdinal,
            status: "running",
            startedAt,
            completedAt: null,
          };
          const turn: ActiveDroidTurn = {
            turnInput,
            providerTurn,
            mapper: createDroidTurnState({
              firstItemOrdinal: turnInput.providerTurnOrdinal * 100 + 1,
              sessionUsage,
              contextUsage: state.providerThread.contextUsage ?? undefined,
              requestedModelId: desired.router ? undefined : desired.modelId,
            }),
            errorMessage: undefined,
            nativeSettled: false,
            queuedSteers: new Map(),
            awaitingNative: new Set(),
            compaction: undefined,
          };
          // Installed before the request so nothing Droid sends right after the ack is lost.
          state.activeTurn = turn;
          yield* request("droid.add_user_message", {
            text: payload.text,
            messageId,
            ...(payload.images.length === 0 ? {} : { images: payload.images }),
          }).pipe(
            Effect.onExit((exit) =>
              Exit.isFailure(exit)
                ? Effect.sync(() => {
                    if (state.activeTurn === turn) state.activeTurn = null;
                  })
                : Effect.void,
            ),
          );
          yield* emit({
            type: "provider_turn.updated",
            driver: DROID_PROVIDER,
            threadId: turnInput.threadId,
            providerTurn,
          });
          yield* updateProviderThread(state, {
            status: "active",
            firstRunOrdinal: state.providerThread.firstRunOrdinal ?? turnInput.runOrdinal,
            lastRunOrdinal: turnInput.runOrdinal,
          });
          yield* updateProviderSession("running", null, desired.modelId);
        }).pipe(permit.withPermits(1));
      });

      /**
       * Droid does not stop a compaction. It answers `interrupt_session` at once, goes on
       * compacting, and queues the next message behind it. Killing the process is the only stop,
       * and the stored session is still the one from before the compaction, so the next turn
       * loads it on a new process.
       */
      const stopCompaction = Effect.fnUntraced(function* (
        state: DroidThreadState,
        turn: ActiveDroidTurn,
      ) {
        const killed = yield* permit.withPermits(1)(
          Effect.gen(function* () {
            if (state.activeTurn !== turn) return false;
            stopRequested = true;
            // Nothing more is sent to a process on its way out.
            ended = true;
            yield* finishTurn(state, turn, { kind: "interrupted", disposition: "reusable" });
            // Still under the permit, so the end of the event stream is handled after the turn
            // settled and cannot report it as a process exit.
            yield* Effect.uninterruptible(rpc.kill);
            return true;
          }),
        );
        // The next turn opens a new session once this one has published that it stopped.
        if (killed) yield* Deferred.await(eventsEnded);
      });

      const interruptTurn = Effect.fnUntraced(function* (
        interruptInput: ProviderAdapter.ProviderAdapterV2InterruptInput,
      ) {
        const state = threadState;
        const turn = state?.activeTurn ?? null;
        if (
          state === null ||
          turn === null ||
          turn.providerTurn.id !== interruptInput.providerTurnId
        ) {
          return yield* protocolError(`Droid turn ${interruptInput.providerTurnId} is not active`);
        }
        if (turn.compaction !== undefined) return yield* stopCompaction(state, turn);
        // Under the event permit so a notification being mapped cannot overwrite the flag with
        // the state it read before it.
        const markInterruptRequested = (interruptRequested: boolean) =>
          permit.withPermits(1)(
            Effect.sync(() => {
              turn.mapper = { ...turn.mapper, interruptRequested };
            }),
          );
        // Set before the request: Droid's cancel answers can arrive before the reply does.
        yield* markInterruptRequested(true);
        yield* request("droid.interrupt_session", {}).pipe(
          Effect.tapError(() => markInterruptRequested(false)),
        );
        yield* permit.withPermits(1)(
          Effect.gen(function* () {
            if (state.activeTurn !== turn) return;
            // A stopped turn takes no answers, and Droid hears one for each request it sent.
            yield* cancelOpenRequests(true);
            // With no native turn running, only a queued steer holds the t3 turn open, and
            // Droid sends no completion for a turn it never started.
            if (turn.nativeSettled && turn.awaitingNative.size === 0) {
              yield* finishTurn(state, turn, { kind: "interrupted", disposition: "reusable" });
              yield* updateProviderSession("ready", null);
            }
          }),
        );
      });

      const steerTurn = Effect.fnUntraced(function* (
        steerInput: ProviderAdapter.ProviderAdapterV2SteerInput,
      ) {
        const notActive = () =>
          protocolError(`Droid turn ${steerInput.providerTurnId} is not active`);
        const target = threadState?.activeTurn ?? null;
        if (target === null || target.providerTurn.id !== steerInput.providerTurnId) {
          return yield* notActive();
        }
        const message = steerInput.message;
        const payload = yield* resolvePromptPayload(message.text, message.attachments);
        const messageId = String(message.messageId);

        // A turn finishes under this permit, so once the turn is seen active here a late steer
        // cannot slip in behind its terminal: it fails before anything is sent, as in Pi.
        yield* Effect.gen(function* () {
          const state = threadState;
          if (state === null || state.activeTurn !== target || ended || closing) {
            return yield* notActive();
          }
          if (target.compaction !== undefined) {
            return yield* protocolError("Droid takes no message while it compacts the session");
          }
          // No turn item here: the orchestrator already records the steer's user_message under
          // this message id, and an adapter item with the same id would overwrite its intent
          // and context.
          target.queuedSteers.set(messageId, payload.text);
          yield* request("droid.add_user_message", {
            text: payload.text,
            messageId,
            queuePlacement: "end_of_turn",
            ...(payload.images.length === 0 ? {} : { images: payload.images }),
          }).pipe(
            Effect.tapError(() =>
              Effect.sync(() => {
                target.queuedSteers.delete(messageId);
              }),
            ),
          );
        }).pipe(permit.withPermits(1));
      });

      const respondToRuntimeRequest = Effect.fnUntraced(function* (
        requestInput: ProviderAdapter.ProviderAdapterV2RuntimeRequestResponseInput,
      ) {
        const key = String(requestInput.requestId);
        const pending = pendingRequests.get(key);
        if (pending === undefined) {
          return yield* protocolError(`Droid has no open request ${requestInput.requestId}`);
        }
        const { kind } = pending;
        const decision = requestInput.decision;
        let reply: unknown;
        let status: "resolved" | "cancelled" = "resolved";
        let outcome: Pick<OrchestrationV2RuntimeRequest, "decision" | "answers"> = {};
        if (kind.type === "permission") {
          if (decision === undefined) return yield* protocolError("An approval needs a decision");
          reply = droidPermissionReply(kind.request, decision);
          if (reply === undefined) {
            return yield* protocolError(`Droid offered no option for ${decision} in this request`);
          }
          status = decision === "cancel" ? "cancelled" : "resolved";
          outcome = { decision };
        } else {
          const answered =
            decision === "cancel" || decision === "decline"
              ? undefined
              : droidAskUserReply(kind.request, requestInput.answers);
          reply = answered ?? DROID_ASK_USER_CANCELLED;
          if (answered === undefined) {
            status = "cancelled";
            outcome = { decision: "cancel" };
          } else if (requestInput.answers !== undefined) {
            outcome = { answers: requestInput.answers };
          }
        }
        yield* rpc.respond(pending.inboundId, reply);
        pendingRequests.delete(key);
        if (kind.type === "permission" && (decision === "decline" || decision === "cancel")) {
          // Droid reports the declined tool as an error result; that is a refusal, not a failure.
          pending.turn.mapper = {
            ...pending.turn.mapper,
            declinedToolUseIds: new Set([
              ...pending.turn.mapper.declinedToolUseIds,
              ...kind.request.toolUseIds,
            ]),
          };
        }
        yield* settleRequest(pending, status, outcome);
      });

      const threadSnapshot = (
        state: DroidThreadState,
      ): ProviderAdapter.ProviderAdapterV2ThreadSnapshot => {
        const threadId = state.providerThread.appThreadId ?? input.threadId;
        const nativeId = state.providerThread.nativeThreadRef?.nativeId ?? "";
        const messages = state.history.map((message, index): OrchestrationV2ConversationMessage => {
          const at = Option.getOrElse(
            DateTime.make(message.createdAt ?? Number.NaN),
            () => state.providerThread.createdAt,
          );
          return {
            id: idAllocator.derive.messageFromProviderItem({
              driver: DROID_PROVIDER,
              nativeItemId: `${nativeId}:snapshot-message:${index}`,
            }),
            threadId,
            runId: null,
            nodeId: null,
            role: message.role,
            text: message.text,
            attachments: [],
            streaming: false,
            createdBy: message.role === "user" ? "user" : "agent",
            creationSource: "provider",
            createdAt: at,
            updatedAt: at,
          };
        });
        return {
          providerThread: state.providerThread,
          providerTurns: [],
          messages,
          runtimeRequests: [],
        };
      };

      const readThreadSnapshot = Effect.fnUntraced(function* (
        snapshotInput: ProviderAdapter.ProviderAdapterV2ReadThreadSnapshotInput,
      ) {
        const state = threadState;
        const wantedNativeId = snapshotInput.providerThread.nativeThreadRef?.nativeId;
        if (state === null || wantedNativeId == null || boundNativeId() !== wantedNativeId) {
          return yield* protocolError(
            "Droid snapshot requested for a thread this session does not host",
          );
        }
        return threadSnapshot(state);
      });

      const newSessionIdOf = (method: string, result: unknown) => {
        const newSessionId = field(result, "newSessionId");
        return Predicate.isString(newSessionId) && newSessionId.length > 0
          ? Effect.succeed(newSessionId)
          : Effect.fail(protocolError(`${method} returned no newSessionId`));
      };

      /**
       * True while a wake is armed for the session: a background task whose completion would start
       * a turn on its own, so rewinding under it would let that turn land on the wrong history.
       * Nothing arms a wake yet; whatever does must answer here, and a rollback then refuses.
       */
      const wakePending = () => false;

      /**
       * Moves the thread to a new Droid session that ends just before the first message of the
       * turn after the target. The rewind forks a session and leaves the process on the one it
       * holds, so the new one is loaded before it takes any frame. The session id only ever
       * moves forward: the old session keeps its history on Droid's side, and nothing here or in
       * `handleEvent` looks at it again.
       */
      const rollbackThread = Effect.fnUntraced(function* (
        rollbackInput: ProviderAdapter.ProviderAdapterV2RollbackThreadInput,
      ) {
        const row = rollbackInput.providerThread;
        const sourceId = row.nativeThreadRef?.nativeId;
        if (sourceId === undefined || sourceId === null) {
          return yield* protocolError("The provider thread has no Droid session to roll back");
        }
        const messageId = droidRollbackMessageId({
          target: rollbackInput.target,
          providerThreadTurns: rollbackInput.providerThreadTurns.filter(
            (turn) => turn.providerThreadId === row.id,
          ),
        });
        if (messageId === undefined) {
          return yield* protocolError(
            "The Droid turn after the rollback target has no strong message id to rewind to",
          );
        }
        const appThreadId = row.appThreadId ?? input.threadId;

        // Under the event permit, like the start of a turn: no notification is mapped between
        // the checks below and the swap of the session.
        return yield* Effect.gen(function* () {
          if (ended || closing) return yield* protocolError("The Droid process has exited");
          const state = threadState;
          if (state?.activeTurn != null) {
            return yield* protocolError("Cannot roll back a Droid session while a turn is active");
          }
          if (wakePending()) {
            return yield* protocolError("Cannot roll back a Droid session while a wake is pending");
          }
          if (state !== null && boundNativeId() !== sourceId) {
            return yield* protocolError(
              "Droid rollback requested for a thread this session does not host",
            );
          }
          if (messageId === null) {
            // No later turn reached Droid, so there is nothing to discard.
            const hosted = state ?? (yield* adoptSession(sourceId, row, appThreadId));
            return threadSnapshot(hosted);
          }

          const rewind = request("droid.execute_rewind", {
            sessionId: sourceId,
            messageId,
            filesToRestore: [],
            filesToDelete: [],
            forkTitle: DROID_REWIND_TITLE,
          }).pipe(Effect.flatMap((result) => newSessionIdOf("execute_rewind", result)));
          // The cut falls before the first user message, which Droid may refuse to rewind to. The
          // thread then moves to an empty session, the same state with nothing before it.
          const rewoundId =
            rollbackInput.target.type === "thread_start"
              ? yield* rewind.pipe(
                  Effect.catch((error) =>
                    logWarning(
                      `Droid could not rewind session ${sourceId} to its start (${describeFailure(error)}); starting a fresh session.`,
                    ).pipe(Effect.as(undefined)),
                  ),
                )
              : yield* rewind;
          const settingsNow = () => desiredSettings(asked.runtimePolicy, asked.modelSelection);
          if (rewoundId === undefined) {
            return threadSnapshot(yield* startSession(settingsNow(), row, appThreadId));
          }

          const rewound = yield* adoptSession(rewoundId, row, appThreadId);
          // The load took no settings, so they go out now, computed against the rewound session's
          // model list. A failed frame leaves nothing known and the next turn sends them again.
          yield* sendSettings(settingsNow()).pipe(
            Effect.catch((error) =>
              logWarning(
                `Droid did not take the settings of the rewound session: ${describeFailure(error)}`,
              ),
            ),
          );
          return threadSnapshot(rewound);
        }).pipe(permit.withPermits(1));
      });

      /**
       * Moves the thread to the session Droid compacted into. The state is updated in place so the
       * turn that asked for the compaction stays attached to it.
       */
      const switchSession = Effect.fnUntraced(function* (
        state: DroidThreadState,
        turn: ActiveDroidTurn,
        nativeId: string,
      ) {
        const result = yield* request("droid.load_session", {
          sessionId: nativeId,
          loadAllMessages: true,
        });
        noteLoadedSession(result);
        state.history = visibleDroidHistory(field(field(result, "session"), "messages"));
        // The counts belong to the retired session.
        turn.mapper = { ...turn.mapper, sessionUsage: undefined, turnBaseline: undefined };
        yield* updateProviderThread(state, { nativeThreadRef: droidRef(nativeId) });
      });

      /**
       * Waits for Droid's answer to `compact_session`, then settles the compaction turn. A stop (a
       * kill) or an exit that came first has settled it already, and the request's failure then
       * changes nothing.
       */
      const settleCompaction = Effect.fnUntraced(function* (
        state: DroidThreadState,
        turn: ActiveDroidTurn,
      ) {
        const exit = yield* Effect.exit(request("droid.compact_session", {}));
        if (Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)) return;
        yield* Effect.gen(function* () {
          if (state.activeTurn !== turn) return;
          if (Exit.isFailure(exit)) {
            const error = Cause.squash(exit.cause);
            if (isRpcTerminated(error)) {
              return yield* settleTurn(
                state,
                turn,
                processExitOutcome(closing, "Droid exited during the compaction."),
              );
            }
            return yield* settleTurn(state, turn, {
              kind: "failed",
              failure: makeProviderFailure({
                message: `Droid context compaction failed: ${redactor
                  .text(describeFailure(error))
                  .slice(0, MAX_ERROR_MESSAGE_CHARS)}`,
                class: "provider_error",
                code: "compaction_failed",
                retryable: false,
              }),
              disposition: "reusable",
            });
          }
          // Droid keeps the session id unless it moves the conversation to a new session.
          const newSessionId = field(exit.value, "newSessionId");
          if (
            Predicate.isString(newSessionId) &&
            newSessionId.length > 0 &&
            newSessionId !== boundNativeId()
          ) {
            const switched = yield* Effect.exit(switchSession(state, turn, newSessionId));
            if (Exit.isFailure(switched)) {
              const detail = describeFailure(Cause.squash(switched.cause));
              yield* logWarning(
                `Droid compacted into session ${newSessionId} but it could not be loaded: ${detail}`,
              );
              // The process may serve either session, so nothing more is sent to it.
              return yield* settleTurn(state, turn, {
                kind: "failed",
                failure: makeProviderFailure({
                  message: "Droid compacted the session but the new session could not be loaded.",
                  class: "provider_error",
                  code: "compaction_session_lost",
                  retryable: false,
                }),
                disposition: "broken",
              });
            }
          }
          yield* settleTurn(state, turn, { kind: "completed", disposition: "reusable" });
        }).pipe(permit.withPermits(1));
      });

      /**
       * Starts a compaction turn. `compact_session` takes no user message, so the provider turn has
       * no native turn and no message id to rewind to. The turn is open when this returns; the
       * request runs beside it and settles it, like the events of any other turn.
       */
      const compactThread = Effect.fnUntraced(function* (
        turnInput: ProviderAdapter.ProviderAdapterV2TurnInput,
      ) {
        const state = threadState;
        if (state === null) return yield* protocolError("Droid session has no registered thread");
        if (ended) return yield* protocolError("The Droid process has exited");
        if (state.activeTurn !== null) {
          return yield* protocolError(
            `Droid provider thread ${turnInput.providerThread.id} already has an active turn`,
          );
        }
        const sameSession = () =>
          state.providerThread.nativeThreadRef?.nativeId ===
          turnInput.providerThread.nativeThreadRef?.nativeId;
        if (!sameSession()) {
          return yield* protocolError("Droid compaction requested for a different native session");
        }

        const turn = yield* Effect.gen(function* () {
          if (
            threadState !== state ||
            state.activeTurn !== null ||
            ended ||
            closing ||
            !sameSession()
          ) {
            return yield* protocolError("Droid turn ownership changed before it could start");
          }
          const startedAt = yield* DateTime.now;
          state.providerThread = turnInput.providerThread;
          const providerTurn: OrchestrationV2ProviderTurn = {
            id: idAllocator.derive.providerTurn({
              driver: DROID_PROVIDER,
              nativeTurnId: `compaction:${String(turnInput.message.messageId)}`,
            }),
            providerThreadId: turnInput.providerThread.id,
            nodeId: turnInput.rootNodeId,
            runAttemptId: turnInput.attemptId,
            nativeTurnRef: null,
            ordinal: turnInput.providerTurnOrdinal,
            status: "running",
            startedAt,
            completedAt: null,
          };
          const opened = createDroidTurnState({
            firstItemOrdinal: turnInput.providerTurnOrdinal * 100 + 1,
            sessionUsage,
            contextUsage: state.providerThread.contextUsage ?? undefined,
          });
          const compaction = { ordinal: opened.nextItemOrdinal, startedAt };
          const compacting: ActiveDroidTurn = {
            turnInput,
            providerTurn,
            mapper: { ...opened, nextItemOrdinal: compaction.ordinal + 1 },
            errorMessage: undefined,
            // No native turn runs, so none owes a completion.
            nativeSettled: true,
            queuedSteers: new Map(),
            awaitingNative: new Set(),
            compaction,
          };
          state.activeTurn = compacting;
          yield* emit({
            type: "provider_turn.updated",
            driver: DROID_PROVIDER,
            threadId: turnInput.threadId,
            providerTurn,
          });
          yield* updateProviderThread(state, {
            status: "active",
            firstRunOrdinal: state.providerThread.firstRunOrdinal ?? turnInput.runOrdinal,
            lastRunOrdinal: turnInput.runOrdinal,
          });
          yield* updateProviderSession("running", null);
          yield* emitMapped(
            state,
            droidCompactionEvents(
              eventContext(state, compacting),
              { ...compaction, status: "running" },
              startedAt,
            ),
          );
          return compacting;
        }).pipe(permit.withPermits(1));

        yield* settleCompaction(state, turn).pipe(Effect.forkIn(scope));
      });

      /**
       * A fork that fails after the source was loaded leaves the process on the source. The thread
       * the runtime already hosts gets its session back; if that fails too, nothing is hosted, so
       * no turn can go to the wrong session.
       */
      const returnToHosted = Effect.fnUntraced(function* (hosted: DroidThreadState | null) {
        const hostedId = hosted?.providerThread.nativeThreadRef?.nativeId;
        if (hosted === null || hostedId === undefined || hostedId === null) return;
        known = {};
        yield* request("droid.load_session", { sessionId: hostedId, loadAllMessages: true }).pipe(
          Effect.map(noteLoadedSession),
          Effect.catch((error) =>
            Effect.sync(() => {
              if (threadState === hosted) threadState = null;
            }).pipe(
              Effect.andThen(
                logWarning(
                  `Droid could not return to session ${hostedId} after a failed fork: ${describeFailure(error)}`,
                ),
              ),
            ),
          ),
        );
      });

      /**
       * Forks the source's Droid session for another thread, cut after the target turn. Droid's
       * `fork_session` forks whichever session the process holds and leaves the process there, so
       * the source is loaded first. A fork from an earlier turn then rewinds the copy, which is
       * another new session; the source is never touched. The thread the runtime hosted before is
       * replaced: a runtime serves one thread, and the fork's is the one the caller goes on with.
       */
      const forkThread = Effect.fnUntraced(function* (
        forkInput: ProviderAdapter.ProviderAdapterV2ForkThreadInput,
      ) {
        const source = forkInput.sourceProviderThread;
        const sourceId = source.nativeThreadRef?.nativeId;
        if (sourceId === undefined || sourceId === null) {
          return yield* protocolError("The source provider thread has no Droid session to fork");
        }
        // The turns come unfiltered: those of other provider threads of the same app thread are in
        // them, and so are turns of runs a rollback removed.
        const sourceTurns = (forkInput.sourceProviderTurns ?? []).filter(
          (turn) => turn.providerThreadId === source.id,
        );
        const target =
          forkInput.providerTurnId === undefined
            ? undefined
            : sourceTurns.find((turn) => turn.id === forkInput.providerTurnId);
        if (forkInput.providerTurnId !== undefined && target === undefined) {
          return yield* protocolError("The fork target is not a turn of the source thread");
        }

        return yield* Effect.gen(function* () {
          if (ended || closing) return yield* protocolError("The Droid process has exited");
          const hosted = threadState;
          if (hosted?.activeTurn != null) {
            return yield* protocolError("Cannot fork a Droid session while a turn is active");
          }
          if (wakePending()) {
            return yield* protocolError("Cannot fork a Droid session while a wake is pending");
          }
          const load = (sessionId: string) =>
            request("droid.load_session", { sessionId, loadAllMessages: true });

          const sourceLoaded = yield* load(sourceId).pipe(
            Effect.tapError((error) =>
              logWarning(`Droid could not load session ${sourceId}: ${describeFailure(error)}`),
            ),
          );
          // From here the process serves the source, whichever way this ends.
          const forked = yield* Effect.gen(function* () {
            const forkedId = yield* request("droid.fork_session", { title: DROID_FORK_TITLE }).pipe(
              Effect.flatMap((result) => newSessionIdOf("fork_session", result)),
            );
            const rewindTo = droidForkRewindMessageId({
              targetOrdinal: target?.ordinal,
              providerThreadTurns: sourceTurns,
              sourceMessageIds: droidMessageIds(field(field(sourceLoaded, "session"), "messages")),
            });
            const finalId =
              rewindTo === null
                ? forkedId
                : yield* request("droid.execute_rewind", {
                    sessionId: forkedId,
                    messageId: rewindTo,
                    filesToRestore: [],
                    filesToDelete: [],
                    forkTitle: DROID_FORK_TITLE,
                  }).pipe(Effect.flatMap((result) => newSessionIdOf("execute_rewind", result)));
            return { finalId, loaded: yield* load(finalId) };
          }).pipe(Effect.onError(() => returnToHosted(hosted)));

          const providerThread: OrchestrationV2ProviderThread = {
            ...bindThread(forked.finalId, undefined, forkInput.targetThreadId, yield* DateTime.now),
            ownerNodeId: forkInput.ownerNodeId ?? null,
            forkedFrom: {
              providerThreadId: source.id,
              ...(forkInput.providerTurnId === undefined
                ? {}
                : { providerTurnId: forkInput.providerTurnId }),
            },
          };
          noteLoadedSession(forked.loaded);
          sessionUsage = undefined;
          threadState = {
            providerThread,
            activeTurn: null,
            history: visibleDroidHistory(field(field(forked.loaded, "session"), "messages")),
            latestPlan: undefined,
          };
          return providerThread;
        }).pipe(permit.withPermits(1));
      });

      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: options.instanceId,
        driver: DROID_PROVIDER,
        providerSessionId: input.providerSessionId,
        get providerSession() {
          return sessionEntity;
        },
        events: Stream.fromQueue(events),
        ensureThread: (threadInput) =>
          ensureThread(threadInput).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterEnsureThreadError({
                  driver: DROID_PROVIDER,
                  threadId: threadInput.threadId,
                  cause,
                }),
            ),
          ),
        resumeThread: (threadInput) =>
          resumeThread(
            threadInput.providerThread,
            threadInput.providerThread.appThreadId ?? threadInput.threadId ?? input.threadId,
          ).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterResumeThreadError({
                  driver: DROID_PROVIDER,
                  providerSessionId: input.providerSessionId,
                  providerThreadId: threadInput.providerThread.id,
                  cause,
                }),
            ),
          ),
        startTurn: (turnInput) =>
          startTurn(turnInput).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver: DROID_PROVIDER,
                  threadId: turnInput.threadId,
                  providerThreadId: turnInput.providerThread.id,
                  runId: turnInput.runId,
                  cause,
                }),
            ),
          ),
        steerTurn: (steerInput) =>
          steerTurn(steerInput).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterSteerRunError({
                  driver: DROID_PROVIDER,
                  providerThreadId: steerInput.providerThread.id,
                  providerTurnId: steerInput.providerTurnId,
                  cause,
                }),
            ),
          ),
        interruptTurn: (interruptInput) =>
          interruptTurn(interruptInput).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterInterruptError({
                  driver: DROID_PROVIDER,
                  providerThreadId: interruptInput.providerThread.id,
                  providerTurnId: interruptInput.providerTurnId,
                  cause,
                }),
            ),
          ),
        respondToRuntimeRequest: (requestInput) =>
          respondToRuntimeRequest(requestInput).pipe(
            permit.withPermits(1),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                  driver: DROID_PROVIDER,
                  requestId: requestInput.requestId,
                  cause,
                }),
            ),
          ),
        readThreadSnapshot: (snapshotInput) =>
          readThreadSnapshot(snapshotInput).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterReadThreadSnapshotError({
                  driver: DROID_PROVIDER,
                  providerThreadId: snapshotInput.providerThread.id,
                  cause,
                }),
            ),
          ),
        rollbackThread: (rollbackInput) =>
          rollbackThread(rollbackInput).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterRollbackThreadError({
                  driver: DROID_PROVIDER,
                  providerThreadId: rollbackInput.providerThread.id,
                  checkpointId: rollbackInput.target.checkpointId,
                  cause,
                }),
            ),
          ),
        compactThread: (turnInput) =>
          compactThread(turnInput).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver: DROID_PROVIDER,
                  threadId: turnInput.threadId,
                  providerThreadId: turnInput.providerThread.id,
                  runId: turnInput.runId,
                  cause,
                }),
            ),
          ),
        forkThread: (forkInput) =>
          forkThread(forkInput).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterForkThreadError({
                  driver: DROID_PROVIDER,
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

export type DroidAdapterV2DriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | FileSystem.FileSystem
  | Path.Path
  | IdAllocator.IdAllocatorV2
  | ProviderHost.ProviderHost;

export const DroidAdapterV2Driver: ProviderAdapterDriver<DroidSettings, DroidAdapterV2DriverEnv> = {
  driverKind: DROID_PROVIDER,
  configSchema: DroidSettings,
  defaultConfig: (): DroidSettings => DEFAULT_DROID_SETTINGS,
  create: Effect.fn("DroidAdapterV2Driver.create")(
    function* (input: ProviderAdapterDriverCreateInput<DroidSettings>) {
      const hostEnvironment = yield* HostProcess.Environment;
      return yield* makeDroidAdapterV2({
        instanceId: input.instanceId,
        settings: { ...input.config, enabled: input.enabled },
        environment: yield* mergeProviderInstanceEnvironment(input.environment, hostEnvironment),
      });
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterDriverCreateError({
              driver: DROID_PROVIDER,
              instanceId: input.instanceId,
              detail: "Failed to create Droid adapter.",
              cause,
            }),
        ),
      ),
  ),
};
