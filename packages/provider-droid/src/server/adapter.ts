import { AgentScope } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import {
  ProviderDriverKind,
  type ChatAttachment,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ExecutionNode,
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
  mapDroidNotification,
  settleDroidTurn,
  type DroidEventContext,
  type DroidMapperState,
  type DroidNotification,
} from "./events.ts";
import { visibleDroidHistory, type DroidHistoryMessage } from "./history.ts";
import {
  DROID_ASK_USER_CANCELLED,
  DROID_PICK_OPTIONS_CANCELLED,
  type DroidTokenCounts,
} from "./protocol.ts";
import {
  DROID_CANCEL_OPTION,
  droidApprovalRequest,
  droidAskUserReply,
  droidPermissionReply,
  droidUserInputRequest,
  type DroidApprovalRequest,
  type DroidUserInputRequest,
} from "./requests.ts";
import { makeDroidRpcRuntime, makeRedactor, type DroidRpcEvent } from "./rpc.ts";
import {
  droidSessionSettings,
  droidSettingsParams,
  echoedDroidAutonomy,
  sameDroidSessionSettings,
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
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
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
    emitsProposedPlan: false,
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
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
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
}

const MAX_ERROR_MESSAGE_CHARS = 1_000;

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
        model: input.modelSelection.model,
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
      /** Unknown until the settings frame of the next turn: `load_session` takes no settings. */
      let applied: DroidSessionSettings | undefined;
      let sessionUsage: DroidTokenCounts | undefined;
      let closing = false;
      let ended = false;
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
          const level = echoedDroidAutonomy(notification);
          if (applied !== undefined && level !== undefined) {
            applied = { ...applied, autonomyLevel: level };
          }
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
        permit.withPermits(1)(
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
            if (cause !== undefined && !closing) {
              yield* logWarning(`Droid process ended: ${Cause.pretty(cause)}`);
            }
            if (closing) {
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
        );

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

      const settingsFrame = (settings: DroidSessionSettings) =>
        request("droid.update_session_settings", droidSettingsParams(settings));

      const ensureThread = Effect.fnUntraced(function* (
        threadInput: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
      ) {
        if (ended) return yield* protocolError("The Droid process has exited");
        if (threadState?.activeTurn != null) {
          return yield* protocolError("Cannot start a Droid session while a turn is active");
        }
        threadState = null;
        applied = undefined;
        const settings = droidSessionSettings({
          runtimeMode: threadInput.runtimePolicy.runtimeMode,
          interactionMode: threadInput.runtimePolicy.interactionMode,
          modelSelection: threadInput.modelSelection,
        });
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
        const providerThread = bindThread(
          sessionId,
          threadInput.existingProviderThread,
          threadInput.threadId,
          at,
        );
        applied = settings;
        sessionUsage = DROID_ZERO_COUNTS;
        threadState = { providerThread, activeTurn: null, history: [] };
        yield* emit({
          type: "provider_thread.updated",
          driver: DROID_PROVIDER,
          providerThread,
        });
        return providerThread;
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
        applied = undefined;
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
        sessionUsage = undefined;
        threadState = {
          providerThread,
          activeTurn: null,
          history: visibleDroidHistory(field(field(result, "session"), "messages")),
        };
        yield* emit({
          type: "provider_thread.updated",
          driver: DROID_PROVIDER,
          providerThread,
        });
        return providerThread;
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

        // One frame covers runtime mode, interaction mode, model, and effort; `applied` is set
        // first so the echoes Droid sends for this very frame refine it rather than race it.
        const desired = droidSessionSettings({
          runtimeMode: turnInput.runtimePolicy.runtimeMode,
          interactionMode: turnInput.runtimePolicy.interactionMode,
          modelSelection: turnInput.modelSelection,
        });
        if (applied === undefined || !sameDroidSessionSettings(applied, desired)) {
          applied = desired;
          yield* settingsFrame(desired).pipe(
            Effect.tapError(() =>
              Effect.sync(() => {
                applied = undefined;
              }),
            ),
          );
        }

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
            }),
            errorMessage: undefined,
            nativeSettled: false,
            queuedSteers: new Map(),
            awaitingNative: new Set(),
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
          yield* updateProviderSession("running", null, turnInput.modelSelection.model);
        }).pipe(permit.withPermits(1));
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
        const threadId = state.providerThread.appThreadId ?? input.threadId;
        const messages = state.history.map((message, index): OrchestrationV2ConversationMessage => {
          const at = Option.getOrElse(
            DateTime.make(message.createdAt ?? Number.NaN),
            () => state.providerThread.createdAt,
          );
          return {
            id: idAllocator.derive.messageFromProviderItem({
              driver: DROID_PROVIDER,
              nativeItemId: `${wantedNativeId}:snapshot-message:${index}`,
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
          Effect.fail(
            new ProviderAdapter.ProviderAdapterRollbackThreadError({
              driver: DROID_PROVIDER,
              providerThreadId: rollbackInput.providerThread.id,
              cause: protocolError("Droid does not support rolling back a thread yet"),
            }),
          ),
        forkThread: (forkInput) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterForkThreadError({
              driver: DROID_PROVIDER,
              providerThreadId: forkInput.sourceProviderThread.id,
              cause: protocolError("Droid does not support forking a thread yet"),
            }),
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
