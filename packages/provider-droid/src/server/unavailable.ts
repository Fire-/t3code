/**
 * Stand-ins so the driver can register before its adapter and text generation
 * exist. They claim no capability and fail every use with the typed error the
 * caller already handles.
 */
import {
  ProviderDriverKind,
  TextGenerationError,
  type OrchestrationV2ProviderCapabilities,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { turnScopedSelectionTransition } from "@t3tools/provider-core/server/selectionTransition";
import * as TextGenerationOperations from "@t3tools/provider-core/server/textGenerationOperations";

const DROID_PROVIDER = ProviderDriverKind.make("droid");

const NO_CAPABILITIES = {
  runtimePolicy: { enforcement: "client-boundary" },
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: false,
    supportsProviderSwitchingViaHandoff: false,
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: false,
    canReadThreadSnapshot: false,
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: false,
  },
  turns: {
    exposesNativeTurnId: false,
    emitsTurnStarted: false,
    emitsTurnCompleted: false,
    supportsInterrupt: false,
    supportsActiveSteering: false,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: false,
    terminalStatusQuality: "none",
  },
  streaming: {
    streamsAssistantText: false,
    streamsReasoning: false,
    streamsToolOutput: false,
    streamsPlanText: false,
    emitsMessageCompleted: false,
  },
  tools: {
    exposesToolItemIds: false,
    emitsToolStarted: false,
    emitsToolCompleted: false,
    emitsToolOutput: false,
    supportsMcpTools: false,
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
    supportsStructuredQuestions: false,
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
    acceptsSyntheticUserContext: false,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: false,
    supportsDeltaHandoff: false,
    supportsFullThreadHandoff: false,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: false,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: false,
  },
  identity: {
    nativeThreadIds: "none",
    nativeTurnIds: "none",
    nativeItemIds: "none",
    nativeRequestIds: "none",
  },
} satisfies OrchestrationV2ProviderCapabilities;

export const makeUnavailableDroidAdapter = (instanceId: ProviderInstanceId) =>
  ProviderAdapter.ProviderAdapterV2.of({
    instanceId,
    driver: DROID_PROVIDER,
    getCapabilities: () => Effect.succeed(NO_CAPABILITIES),
    planSelectionTransition: () => Effect.succeed(turnScopedSelectionTransition()),
    openSession: (input) =>
      Effect.fail(
        new ProviderAdapter.ProviderAdapterOpenSessionError({
          driver: DROID_PROVIDER,
          providerSessionId: input.providerSessionId,
          cause: new Error("The Droid orchestration adapter is not available yet."),
        }),
      ),
  });

export const unavailableDroidTextGeneration = TextGenerationOperations.fromRunner(
  "DroidTextGeneration",
  (request) =>
    Effect.fail(
      new TextGenerationError({
        operation: request.operation,
        detail: "Droid text generation is not available yet.",
      }),
    ),
);
