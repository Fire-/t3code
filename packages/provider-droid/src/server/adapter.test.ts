import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  type ChatAttachment,
  type OrchestrationV2ProviderCapabilities,
  type RuntimeMode,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import { AgentScope, type AgentScopeShape } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";

import { DROID_PROVIDER, DroidAdapterV2Driver, makeDroidAdapterV2 } from "./adapter.ts";
import {
  asRecord,
  captureLogs,
  completed,
  DROID_INSTANCE_ID,
  ensure,
  isTerminal,
  layerTest,
  makeAdapter,
  makeProviderThread,
  MODEL,
  messageIdFor,
  methodsOf,
  openItems,
  openRuntime,
  PROVIDER_THREAD_ID,
  policy,
  requestsOf,
  runTurn,
  selection,
  SESSION_ID,
  sessionStatuses,
  startTurn,
  terminals,
  THREAD_ID,
} from "./adapterHarness.testkit.ts";
import { DROID_PRESENTATION } from "./status.ts";
import { FAKE_DROID_DEFAULT_SETTINGS, makeFakeDroid, type FakeDroid } from "./fakeDroid.testkit.ts";
import { makeRpcMockAgent } from "./rpcMockAgent.testkit.ts";

const isResumeError = Schema.is(ProviderAdapter.ProviderAdapterResumeThreadError);

const AUTONOMY_BY_MODE = [
  ["approval-required", "off"],
  ["auto-accept-edits", "low"],
  ["auto", "medium"],
  ["full-access", "high"],
] as const satisfies ReadonlyArray<readonly [RuntimeMode, string]>;

/** Runs on the live clock; the timeout only fails a hung test. */
const live = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.timeout("30 seconds"), TestClock.withLive);

const EXPECTED_CAPABILITIES = {
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
    providerRollbackReturnsSnapshot: true,
    providerCanReadConversationSnapshot: true,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "strong",
    nativeItemIds: "strong",
    nativeRequestIds: "strong",
  },
} as const satisfies OrchestrationV2ProviderCapabilities;

describe("DroidAdapterV2", () => {
  describe("capabilities and registration", () => {
    it.effect(
      "[VAL-TURN-002][VAL-REQ-015][VAL-SUB-016] pins the capability object, with steering, structured questions, and subagents on, and the four runtime modes",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const adapter = yield* makeAdapter(fake.spawner);
          assert.deepStrictEqual(yield* adapter.getCapabilities(), EXPECTED_CAPABILITIES);
          assert.strictEqual(adapter.driver, DROID_PROVIDER);
          assert.deepStrictEqual(
            [...DROID_PRESENTATION.supportedRuntimeModes],
            ["approval-required", "auto-accept-edits", "auto", "full-access"],
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-TURN-001] the adapter driver builds the adapter for a Droid instance", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const adapter = yield* DroidAdapterV2Driver.create({
          instanceId: DROID_INSTANCE_ID,
          displayName: undefined,
          accentColor: undefined,
          enabled: true,
          environment: [],
          config: { enabled: true, binaryPath: "droid", customModels: [] },
        }).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner),
          Effect.provideService(HostProcess.Environment, {}),
        );
        assert.strictEqual(DroidAdapterV2Driver.driverKind, DROID_PROVIDER);
        assert.strictEqual(adapter.instanceId, DROID_INSTANCE_ID);
        assert.strictEqual(adapter.driver, DROID_PROVIDER);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("agent scope", () => {
    it.effect(
      "spawns the command and args AgentScope.wrap returns, named droid for the thread",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const launches: Array<ChildProcess.StandardCommand> = [];
          const spawner = ChildProcessSpawner.make((command) => {
            if (!ChildProcess.isStandardCommand(command)) return Effect.die("Unexpected pipeline");
            launches.push(command);
            return fake.spawner.spawn(command);
          });
          const wrapped: Array<Parameters<AgentScopeShape["wrap"]>[0]> = [];
          const agentScope: AgentScopeShape = {
            ...AgentScope.defaultValue(),
            wrap: (input) =>
              Effect.sync(() => {
                wrapped.push(input);
                return {
                  command: "scope-launcher",
                  args: ["--unit", "droid-test.scope", "--", input.command, ...input.args],
                };
              }),
          };
          const adapter = yield* makeDroidAdapterV2({
            instanceId: DROID_INSTANCE_ID,
            settings: { enabled: true, binaryPath: "/opt/droid/bin/droid", customModels: [] },
            environment: { PATH: "/opt/droid/bin" },
          }).pipe(
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
            Effect.provideService(AgentScope, agentScope),
          );
          yield* adapter.openSession({
            threadId: THREAD_ID,
            providerSessionId: SESSION_ID,
            modelSelection: selection(),
            runtimePolicy: policy(),
          });

          assert.strictEqual(wrapped.length, 1);
          const [request] = wrapped;
          assert.strictEqual(request?.command, "/opt/droid/bin/droid");
          assert.strictEqual(request?.name, "droid");
          assert.strictEqual(request?.threadId, THREAD_ID);
          assert.deepStrictEqual(request?.env, { PATH: "/opt/droid/bin" });
          assert.deepStrictEqual(request?.args.slice(0, 5), [
            "exec",
            "--input-format",
            "stream-jsonrpc",
            "--output-format",
            "stream-jsonrpc",
          ]);
          assert.strictEqual(launches.length, 1);
          assert.strictEqual(launches[0]?.command, "scope-launcher");
          assert.deepStrictEqual(launches[0]?.args, [
            "--unit",
            "droid-test.scope",
            "--",
            "/opt/droid/bin/droid",
            ...(request?.args ?? []),
          ]);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("new threads", () => {
    it.effect(
      "[VAL-TURN-003] ensureThread sends one initialize_session with explicit settings and keeps the row id",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          const host = yield* ProviderHost.ProviderHost;
          const existing = yield* makeProviderThread();
          const thread = yield* opened.runtime.ensureThread({
            threadId: THREAD_ID,
            modelSelection: selection("high"),
            runtimePolicy: policy("approval-required"),
            existingProviderThread: existing,
          });

          assert.deepStrictEqual(methodsOf(fake), ["droid.initialize_session"]);
          assert.deepStrictEqual(requestsOf(fake, "droid.initialize_session")[0]?.params, {
            machineId: "default",
            cwd: host.paths.cwd,
            interactionMode: "auto",
            modelId: MODEL,
            reasoningEffort: "high",
            autonomyLevel: "off",
          });
          assert.strictEqual(thread.id, PROVIDER_THREAD_ID);
          const sessionId = fake.current().sessionId();
          assert.isDefined(sessionId);
          assert.deepStrictEqual(thread.nativeThreadRef, {
            driver: DROID_PROVIDER,
            nativeId: sessionId,
            strength: "strong",
          });
          assert.strictEqual(thread.providerSessionId, SESSION_ID);
          assert.strictEqual(thread.status, "idle");
          const updated = yield* opened.takeEvent(
            (event) => event.type === "provider_thread.updated",
          );
          assert.isTrue(
            updated.type === "provider_thread.updated" &&
              updated.providerThread.nativeThreadRef?.nativeId === thread.nativeThreadRef?.nativeId,
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-003] ensureThread without an existing row derives the provider thread id",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          const thread = yield* opened.runtime.ensureThread({
            threadId: THREAD_ID,
            modelSelection: selection("high"),
            runtimePolicy: policy("approval-required"),
          });
          assert.strictEqual(thread.appThreadId, THREAD_ID);
          assert.strictEqual(thread.nativeThreadRef?.nativeId, fake.current().sessionId());
          assert.strictEqual(thread.driver, "droid");
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-TURN-003] a model without an effort option sends no reasoningEffort", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake);
        yield* ensure(opened, { effort: undefined });
        const params = requestsOf(fake, "droid.initialize_session")[0]?.params ?? {};
        assert.isFalse("reasoningEffort" in params);
        assert.strictEqual(params["interactionMode"], "auto");
        assert.strictEqual(params["modelId"], MODEL);
        assert.strictEqual(params["autonomyLevel"], "off");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-TURN-003] plan interaction mode starts the session in spec mode", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake, { interactionMode: "plan" });
        const existing = yield* makeProviderThread();
        yield* opened.runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: selection("high"),
          runtimePolicy: policy("approval-required", "plan"),
          existingProviderThread: existing,
        });
        const params = requestsOf(fake, "droid.initialize_session")[0]?.params ?? {};
        assert.strictEqual(params["interactionMode"], "spec");
        assert.strictEqual(params["specModeModelId"], MODEL);
        assert.strictEqual(params["specModeReasoningEffort"], "high");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-003] a failed initialize_session fails ensureThread with its typed error",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid({
            overrides: {
              "droid.initialize_session": () => ({ error: { code: -32603, message: "no" } }),
            },
          });
          const opened = yield* openRuntime(fake);
          const existing = yield* makeProviderThread();
          const exit = yield* opened.runtime
            .ensureThread({
              threadId: THREAD_ID,
              modelSelection: selection("high"),
              runtimePolicy: policy(),
              existingProviderThread: existing,
            })
            .pipe(Effect.exit);
          assert.isTrue(Exit.isFailure(exit));
          const error = Exit.isFailure(exit) ? exit.cause : undefined;
          assert.include(JSON.stringify(error), "ProviderAdapterEnsureThreadError");
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect.each(AUTONOMY_BY_MODE)(
      "[VAL-AUTO-001] initialize_session for %s sends autonomyLevel %s",
      ([runtimeMode, autonomyLevel]) =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake, { runtimeMode });
          yield* ensure(opened, { runtimeMode });
          assert.strictEqual(
            requestsOf(fake, "droid.initialize_session")[0]?.params["autonomyLevel"],
            autonomyLevel,
          );
          assert.strictEqual(requestsOf(fake, "droid.initialize_session").length, 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("resume", () => {
    const stored = {
      "stored-session": {
        settings: { ...FAKE_DROID_DEFAULT_SETTINGS, autonomyLevel: "high" },
        messages: [],
      },
    };

    it.effect.each(AUTONOMY_BY_MODE)(
      "[VAL-TURN-004][VAL-AUTO-003] resume loads the stored session, then one settings frame precedes the turn (%s)",
      ([runtimeMode, autonomyLevel]) =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid({ sessions: stored });
          const opened = yield* openRuntime(fake, { runtimeMode });
          const row = yield* makeProviderThread("stored-session");
          const resumed = yield* opened.runtime.resumeThread({
            providerThread: row,
            threadId: THREAD_ID,
            modelSelection: selection("high"),
            runtimePolicy: policy(runtimeMode),
          });
          assert.deepStrictEqual(methodsOf(fake), ["droid.load_session"]);
          assert.deepStrictEqual(requestsOf(fake, "droid.load_session")[0]?.params, {
            sessionId: "stored-session",
            loadAllMessages: true,
          });
          assert.strictEqual(resumed.id, row.id);
          assert.strictEqual(resumed.nativeThreadRef?.nativeId, "stored-session");

          yield* startTurn(opened.runtime, resumed, { runtimeMode });
          assert.deepStrictEqual(methodsOf(fake), [
            "droid.load_session",
            "droid.update_session_settings",
            "droid.add_user_message",
          ]);
          assert.deepStrictEqual(requestsOf(fake, "droid.update_session_settings")[0]?.params, {
            interactionMode: "auto",
            autonomyLevel,
            modelId: MODEL,
            reasoningEffort: "high",
          });
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-TURN-004] resume is a no-op when the process is already on that session", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ sessions: stored });
        const opened = yield* openRuntime(fake);
        const row = yield* makeProviderThread("stored-session");
        const first = yield* opened.runtime.resumeThread({ providerThread: row });
        const second = yield* opened.runtime.resumeThread({ providerThread: first });
        assert.deepStrictEqual(methodsOf(fake), ["droid.load_session"]);
        assert.strictEqual(second.id, first.id);
        assert.deepStrictEqual(second.nativeThreadRef, first.nativeThreadRef);
        assert.strictEqual(second.updatedAt, first.updatedAt);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-TURN-004] openSession with a stored thread id does not start a session", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ sessions: stored });
        const opened = yield* openRuntime(fake);
        yield* opened.runtime.resumeThread({
          providerThread: yield* makeProviderThread("stored-session"),
        });
        assert.notInclude(methodsOf(fake), "droid.initialize_session");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-005] -32004 fails with ProviderAdapterResumeThreadError and logs the full message",
      () =>
        Effect.gen(function* () {
          const logs = captureLogs();
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          const row = yield* makeProviderThread("missing-session");
          const exit = yield* opened.runtime
            .resumeThread({ providerThread: row })
            .pipe(Effect.exit, logs.provide);
          assert.isTrue(Exit.isFailure(exit));
          if (!Exit.isFailure(exit)) return;
          const failure = exit.cause.reasons.flatMap((reason) =>
            reason._tag === "Fail" ? [reason.error] : [],
          )[0];
          assert.isTrue(isResumeError(failure));
          if (!isResumeError(failure)) return;
          assert.strictEqual(failure.driver, "droid");
          assert.strictEqual(failure.providerThreadId, row.id);
          assert.strictEqual(failure.providerSessionId, SESSION_ID);
          assert.notInclude(methodsOf(fake), "droid.initialize_session");
          assert.isTrue(
            logs.entries.some(
              (entry) => entry.level === "Warn" && entry.text.includes("Session not found"),
            ),
            "the full Droid message reaches the log",
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-TURN-005] any other load failure also fails with the typed resume error", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({
          sessions: stored,
          overrides: { "droid.load_session": () => ({ error: { code: -32603, message: "boom" } }) },
        });
        const opened = yield* openRuntime(fake);
        const exit = yield* opened.runtime
          .resumeThread({ providerThread: yield* makeProviderThread("stored-session") })
          .pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(exit));
        assert.include(JSON.stringify(exit), "ProviderAdapterResumeThreadError");
        assert.notInclude(methodsOf(fake), "droid.initialize_session");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-005] after a failed resume, ensureThread with no native ref initializes a session",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          const row = yield* makeProviderThread("missing-session");
          yield* opened.runtime.resumeThread({ providerThread: row }).pipe(Effect.exit);
          const replacement = yield* opened.runtime.ensureThread({
            threadId: THREAD_ID,
            modelSelection: selection("high"),
            runtimePolicy: policy(),
            existingProviderThread: { ...row, nativeThreadRef: null },
          });
          assert.strictEqual(replacement.id, row.id);
          assert.notStrictEqual(replacement.nativeThreadRef?.nativeId, "missing-session");
          assert.deepStrictEqual(methodsOf(fake), [
            "droid.load_session",
            "droid.initialize_session",
          ]);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("turn identity and wire sequence", () => {
    it.effect(
      "[VAL-TURN-006] add_user_message carries the message id, and only a matching completion ends the turn",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          const thread = yield* ensure(opened);
          yield* startTurn(opened.runtime, thread, { text: "Say hi" });

          assert.deepStrictEqual(methodsOf(fake), [
            "droid.initialize_session",
            "droid.add_user_message",
          ]);
          assert.deepStrictEqual(requestsOf(fake, "droid.add_user_message")[0]?.params, {
            text: "Say hi",
            messageId: messageIdFor(1),
          });
          const started = yield* opened.takeEvent(
            (event) =>
              event.type === "provider_turn.updated" && event.providerTurn.status === "running",
          );
          assert.isTrue(
            started.type === "provider_turn.updated" &&
              started.providerTurn.nativeTurnRef?.nativeId === messageIdFor(1) &&
              started.providerTurn.nativeTurnRef.strength === "strong" &&
              started.providerTurn.nativeTurnRef.driver === "droid",
          );

          const process = fake.current();
          yield* process.emit({
            type: "agent_turn_completed",
            reason: "completed",
            turnId: "someone-elses-turn",
          });
          yield* process.settle;
          assert.deepStrictEqual(terminals(opened.all), []);

          yield* process.emit(completed(1));
          const terminal = yield* opened.takeEvent(isTerminal);
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-006] the provider turn row ends completed with the turn's token usage",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          const thread = yield* ensure(opened);
          yield* runTurn(opened, fake, thread);
          const final = opened.all.findLast(
            (event) =>
              event.type === "provider_turn.updated" && event.providerTurn.status === "completed",
          );
          assert.isDefined(final);
          assert.isTrue(
            final?.type === "provider_turn.updated" &&
              final.providerTurn.completedAt !== null &&
              final.providerTurn.turnTokenUsage?.usageStatus !== undefined,
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-STATUS-009] a turn with token counts publishes the thread's contextUsage", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened);
        yield* runTurn(opened, fake, thread);
        const withUsage = opened.all.findLast(
          (event) =>
            event.type === "provider_thread.updated" && event.providerThread.contextUsage !== null,
        );
        assert.isTrue(
          withUsage?.type === "provider_thread.updated" &&
            (withUsage.providerThread.contextUsage?.usedTokens ?? 0) > 0,
          "the presentation's reportsContextWindow is backed by a published contextUsage",
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-TURN-006] a turn already running rejects another start", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened);
        yield* startTurn(opened.runtime, thread);
        const second = yield* startTurn(opened.runtime, thread, { runOrdinal: 2 }).pipe(
          Effect.exit,
        );
        assert.isTrue(Exit.isFailure(second));
        assert.include(JSON.stringify(second), "ProviderAdapterTurnStartError");
        assert.strictEqual(requestsOf(fake, "droid.add_user_message").length, 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-006] a turn before any thread is registered fails with the typed start error",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          const row = yield* makeProviderThread("never-registered");
          const exit = yield* startTurn(opened.runtime, row).pipe(Effect.exit);
          assert.isTrue(Exit.isFailure(exit));
          assert.include(JSON.stringify(exit), "ProviderAdapterTurnStartError");
          assert.deepStrictEqual(methodsOf(fake), []);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-TURN-006] images travel as base64 and other attachments as saved paths", () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const host = yield* ProviderHost.ProviderHost;
        const image = {
          type: "image",
          id: "image-1",
          name: "shot.png",
          mimeType: "image/png",
          sizeBytes: 3,
        } satisfies ChatAttachment;
        const note = {
          type: "file",
          id: "note-1",
          name: "note.txt",
          mimeType: "text/plain",
          sizeBytes: 5,
        } satisfies ChatAttachment;
        yield* fs.writeFile(
          path.join(host.paths.attachmentsDir, "image-1"),
          new Uint8Array([1, 2, 3]),
        );
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened);
        yield* startTurn(opened.runtime, thread, {
          text: "Look",
          attachments: [image, note],
        });
        const params = requestsOf(fake, "droid.add_user_message")[0]?.params ?? {};
        assert.deepStrictEqual(params["images"], [
          { type: "base64", data: "AQID", mediaType: "image/png" },
        ]);
        assert.strictEqual(
          params["text"],
          `Look\n\n[Attachment saved at ${path.join(host.paths.attachmentsDir, "note-1")}]`,
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("autonomy", () => {
    it.effect(
      "[VAL-AUTO-004] a turn in a different runtime mode sends one settings frame with the new level first",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          const thread = yield* ensure(opened);
          yield* runTurn(opened, fake, thread, { runtimeMode: "approval-required" });
          yield* runTurn(opened, fake, thread, { runOrdinal: 2, runtimeMode: "full-access" });

          assert.deepStrictEqual(methodsOf(fake), [
            "droid.initialize_session",
            "droid.add_user_message",
            "droid.update_session_settings",
            "droid.add_user_message",
          ]);
          assert.deepStrictEqual(requestsOf(fake, "droid.update_session_settings")[0]?.params, {
            interactionMode: "auto",
            autonomyLevel: "high",
            modelId: MODEL,
            reasoningEffort: "high",
          });
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-AUTO-005] nothing is re-sent while mode, model, and effort are unchanged", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened);
        yield* runTurn(opened, fake, thread);
        yield* runTurn(opened, fake, thread, { runOrdinal: 2 });
        yield* runTurn(opened, fake, thread, { runOrdinal: 3 });
        assert.deepStrictEqual(methodsOf(fake), [
          "droid.initialize_session",
          "droid.add_user_message",
          "droid.add_user_message",
          "droid.add_user_message",
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-AUTO-005] a changed model or effort sends one settings frame", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened);
        yield* runTurn(opened, fake, thread);
        yield* runTurn(opened, fake, thread, { runOrdinal: 2, effort: "low" });
        yield* runTurn(opened, fake, thread, {
          runOrdinal: 3,
          effort: "low",
          model: "gpt-5-codex",
        });
        const updates = requestsOf(fake, "droid.update_session_settings").map(
          (request) => request.params,
        );
        assert.deepStrictEqual(updates, [
          {
            interactionMode: "auto",
            autonomyLevel: "off",
            modelId: MODEL,
            reasoningEffort: "low",
          },
          {
            interactionMode: "auto",
            autonomyLevel: "off",
            modelId: "gpt-5-codex",
            reasoningEffort: "low",
          },
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-AUTO-005] entering plan mode sends spec settings and leaving it sends auto",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          const thread = yield* ensure(opened);
          yield* runTurn(opened, fake, thread);
          yield* runTurn(opened, fake, thread, { runOrdinal: 2, interactionMode: "plan" });
          yield* runTurn(opened, fake, thread, { runOrdinal: 3 });
          const updates = requestsOf(fake, "droid.update_session_settings").map(
            (request) => request.params,
          );
          assert.deepStrictEqual(
            updates.map((params) => params["interactionMode"]),
            ["spec", "auto"],
          );
          assert.strictEqual(updates[0]?.["specModeModelId"], MODEL);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-AUTO-006] autonomy Droid raised on its own is reset to the policy level", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened);
        yield* runTurn(opened, fake, thread);
        yield* fake.current().emit(
          {
            type: "settings_updated",
            settings: {
              ...FAKE_DROID_DEFAULT_SETTINGS,
              reasoningEffort: "high",
              autonomyLevel: "low",
            },
          },
          null,
        );
        yield* fake.current().settle;
        yield* runTurn(opened, fake, thread, { runOrdinal: 2 });
        assert.deepStrictEqual(
          requestsOf(fake, "droid.update_session_settings").map((request) => request.params),
          [
            {
              interactionMode: "auto",
              autonomyLevel: "off",
              modelId: MODEL,
              reasoningEffort: "high",
            },
          ],
        );
        assert.deepStrictEqual(methodsOf(fake).slice(-2), [
          "droid.update_session_settings",
          "droid.add_user_message",
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-AUTO-006] a settings_updated that reports the policy level re-sends nothing",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          const thread = yield* ensure(opened);
          yield* runTurn(opened, fake, thread);
          yield* fake.current().emit(
            {
              type: "settings_updated",
              settings: { ...FAKE_DROID_DEFAULT_SETTINGS, reasoningEffort: "high" },
            },
            null,
          );
          yield* fake.current().settle;
          yield* runTurn(opened, fake, thread, { runOrdinal: 2 });
          assert.deepStrictEqual(requestsOf(fake, "droid.update_session_settings"), []);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-AUTO-006] the last settings echo wins when Droid's echoes arrive out of order",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid({
            settingsEchoes: (next, previous) => [previous, next, next],
          });
          const opened = yield* openRuntime(fake);
          const thread = yield* ensure(opened);
          yield* runTurn(opened, fake, thread, { runtimeMode: "approval-required" });
          yield* runTurn(opened, fake, thread, { runOrdinal: 2, runtimeMode: "auto-accept-edits" });
          yield* runTurn(opened, fake, thread, { runOrdinal: 3, runtimeMode: "auto-accept-edits" });
          assert.deepStrictEqual(
            requestsOf(fake, "droid.update_session_settings").map(
              (request) => request.params["autonomyLevel"],
            ),
            ["low"],
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-AUTO-003] a resumed session's stored autonomy is replaced before the next turn",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid({
            sessions: {
              "stored-session": {
                settings: { ...FAKE_DROID_DEFAULT_SETTINGS, autonomyLevel: "off" },
                messages: [],
              },
            },
          });
          const opened = yield* openRuntime(fake);
          const resumed = yield* opened.runtime.resumeThread({
            providerThread: yield* makeProviderThread("stored-session"),
          });
          yield* fake.current().settle;
          // The stored level already equals the policy level, yet Droid's load takes no settings,
          // so the frame is still sent.
          yield* runTurn(opened, fake, resumed, { runtimeMode: "approval-required" });
          yield* runTurn(opened, fake, resumed, {
            runOrdinal: 2,
            runtimeMode: "approval-required",
          });
          assert.deepStrictEqual(methodsOf(fake), [
            "droid.load_session",
            "droid.update_session_settings",
            "droid.add_user_message",
            "droid.add_user_message",
          ]);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("terminals", () => {
    const cases = [
      { reason: "completed", status: "completed", disposition: "reusable", failed: false },
      { reason: "spec_handoff", status: "completed", disposition: "reusable", failed: false },
      { reason: "cancelled", status: "interrupted", disposition: "reusable", failed: false },
      {
        reason: "permission_rejected",
        status: "interrupted",
        disposition: "reusable",
        failed: false,
      },
      {
        reason: "model_authentication_failed",
        status: "failed",
        disposition: "reusable",
        failed: true,
      },
      { reason: "model_rate_limited", status: "failed", disposition: "reusable", failed: true },
      { reason: "process_exit", status: "failed", disposition: "broken", failed: true },
    ] as const;

    it.effect.each(cases)(
      "[VAL-TURN-013] reason $reason ends the turn $status with one terminal",
      ({ reason, status, disposition, failed }) =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          const thread = yield* ensure(opened);
          yield* startTurn(opened.runtime, thread);
          const process = fake.current();
          yield* process.emit({
            type: "tool_call",
            toolUse: { type: "tool_use", id: "tool-1", name: "Execute", input: { command: "ls" } },
          });
          yield* process.emit(completed(1, reason));
          const terminal = yield* opened.takeEvent(isTerminal);
          yield* process.settle;

          assert.strictEqual(terminals(opened.all).length, 1);
          assert.isTrue(terminal.type === "turn.terminal");
          if (terminal.type !== "turn.terminal") return;
          assert.strictEqual(terminal.status, status);
          assert.strictEqual(terminal.threadDisposition, disposition);
          assert.strictEqual(terminal.providerTurnId, terminals(opened.all)[0]?.providerTurnId);
          if (failed) {
            assert.isTrue("failureItemOrdinal" in terminal);
            assert.isNotNull(terminal.failure);
            const failureIndex = opened.all.findIndex(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.type === "error" &&
                "failureItemOrdinal" in terminal &&
                event.turnItem.ordinal === terminal.failureItemOrdinal,
            );
            assert.isAbove(failureIndex, -1, "an error item carries the failure ordinal");
            assert.isBelow(failureIndex, opened.all.indexOf(terminal));
          } else {
            assert.isNull(terminal.failure);
          }
          const tool = opened.all.findLast(
            (event) => event.type === "turn_item.updated" && event.turnItem.type !== "error",
          );
          assert.isTrue(
            tool?.type === "turn_item.updated" && tool.turnItem.status !== "running",
            "no tool item of the turn is still running after its terminal",
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-TURN-013] a duplicate or late completion does not emit a second terminal", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake);
        const thread = yield* ensure(opened);
        yield* startTurn(opened.runtime, thread);
        const process = fake.current();
        yield* process.emit(completed(1));
        yield* process.emit(completed(1));
        yield* opened.takeEvent(isTerminal);
        yield* process.emit(completed(1));
        yield* process.settle;
        assert.strictEqual(terminals(opened.all).length, 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-013] interrupting sends interrupt_session and the cancelled completion ends interrupted",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          const thread = yield* ensure(opened);
          yield* startTurn(opened.runtime, thread);
          const turn = yield* opened.takeEvent((event) => event.type === "provider_turn.updated");
          assert.isTrue(turn.type === "provider_turn.updated");
          if (turn.type !== "provider_turn.updated") return;
          yield* opened.runtime.interruptTurn({
            providerThread: thread,
            providerTurnId: turn.providerTurn.id,
          });
          assert.deepStrictEqual(
            requestsOf(fake, "droid.interrupt_session").map((r) => r.params),
            [{}],
          );
          yield* fake.current().emit(completed(1, "cancelled"));
          const terminal = yield* opened.takeEvent(isTerminal);
          assert.isTrue(
            terminal.type === "turn.terminal" &&
              terminal.status === "interrupted" &&
              terminal.failure === null,
          );
          const final = opened.all.findLast((event) => event.type === "provider_turn.updated");
          assert.isTrue(
            final?.type === "provider_turn.updated" && final.providerTurn.status === "interrupted",
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    describe("a tool Stop cancelled", () => {
      /** Starts a turn with a running command, asks to stop it, then plays Droid's cancel answer. */
      const stopRunningCommand = (fake: FakeDroid) =>
        Effect.gen(function* () {
          const opened = yield* openRuntime(fake);
          const thread = yield* ensure(opened);
          yield* startTurn(opened.runtime, thread);
          const turn = yield* opened.takeEvent((event) => event.type === "provider_turn.updated");
          if (turn.type !== "provider_turn.updated") return assert.fail("no provider turn");
          const process = fake.current();
          yield* process.emit({
            type: "tool_call",
            toolUse: {
              type: "tool_use",
              id: "tool-1",
              name: "Execute",
              input: { command: "sleep 60" },
            },
          });
          const exit = yield* opened.runtime
            .interruptTurn({ providerThread: thread, providerTurnId: turn.providerTurn.id })
            .pipe(Effect.exit);
          // Droid sends the error result twice before the cancelled completion.
          for (const messageId of ["tool-result-1", "tool-result-2"]) {
            yield* process.emit({
              type: "tool_result",
              toolUseId: "tool-1",
              messageId,
              content: "Tool execution cancelled by user",
              isError: true,
            });
          }
          yield* process.emit(completed(1, "cancelled"));
          yield* opened.takeEvent(isTerminal);
          yield* process.settle;
          const commandStatuses = opened.all.flatMap((event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "command_execution"
              ? [event.turnItem.status]
              : [],
          );
          return { exit, commandStatuses, terminalCount: terminals(opened.all).length };
        });

      it.effect(
        "[VAL-TURN-020] the error result Droid sends after interrupt_session settles the command interrupted",
        () =>
          Effect.gen(function* () {
            const stopped = yield* stopRunningCommand(yield* makeFakeDroid());
            assert.isTrue(Exit.isSuccess(stopped.exit));
            assert.deepStrictEqual(stopped.commandStatuses, ["running", "interrupted"]);
            assert.strictEqual(stopped.terminalCount, 1);
          }).pipe(Effect.scoped, Effect.provide(layerTest)),
      );

      it.effect(
        "[VAL-TURN-020] a failed interrupt_session leaves the later tool error failed, not interrupted",
        () =>
          Effect.gen(function* () {
            const stopped = yield* stopRunningCommand(
              yield* makeFakeDroid({
                overrides: {
                  "droid.interrupt_session": () => ({ error: { code: -32603, message: "no" } }),
                },
              }),
            );
            assert.isTrue(Exit.isFailure(stopped.exit));
            assert.deepStrictEqual(stopped.commandStatuses, ["running", "failed"]);
          }).pipe(Effect.scoped, Effect.provide(layerTest)),
      );
    });

    it.effect(
      "[VAL-TURN-013] interrupting a turn that is not active fails with the typed error",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          const thread = yield* ensure(opened);
          const exit = yield* opened.runtime
            .interruptTurn({
              providerThread: thread,
              providerTurnId: "provider-turn:other" as never,
            })
            .pipe(Effect.exit);
          assert.isTrue(Exit.isFailure(exit));
          assert.include(JSON.stringify(exit), "ProviderAdapterInterruptError");
          assert.deepStrictEqual(requestsOf(fake, "droid.interrupt_session"), []);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-015] an unknown notification mid-turn is logged and dropped, and the turn completes",
      () => {
        // Installed around the whole test: the notification is dropped on a fiber started at open.
        const logs = captureLogs();
        return Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          const thread = yield* ensure(opened);
          yield* startTurn(opened.runtime, thread);
          // The last event startTurn publishes.
          yield* opened.takeEvent(
            (event) =>
              event.type === "provider_session.updated" &&
              event.providerSession.status === "running",
          );
          const before = opened.all.length;
          const process = fake.current();
          yield* process.emit({
            type: "brand_new_notification",
            payload: "SYNTHETIC-UNKNOWN-PAYLOAD",
          });
          yield* process.settle;
          yield* process.emit(completed(1));
          const terminal = yield* opened.takeEvent(isTerminal);
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          yield* opened.takeEvent((event) => event.type === "provider_session.updated");
          assert.deepStrictEqual(
            opened.all.slice(before).map((event) => event.type),
            [
              "provider_thread.updated",
              "provider_turn.updated",
              "provider_thread.updated",
              "turn.terminal",
              "provider_session.updated",
            ],
            "only the completion's own events follow the dropped notification",
          );
          assert.isTrue(
            logs.entries.some((entry) => entry.text.includes("brand_new_notification")),
            "the dropped type is logged",
          );
        }).pipe(logs.provide, Effect.scoped, Effect.provide(layerTest));
      },
    );

    it.effect(
      "[VAL-REQ-014] inbound requests with no turn to show them in are cancelled at once so Droid never waits on a hidden prompt",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          yield* ensure(opened);
          const process = fake.current();
          yield* process.emitInbound("perm-1", "droid.request_permission", {
            toolUses: [],
            options: [],
          });
          yield* process.emitInbound("ask-1", "droid.ask_user", {
            toolCallId: "t",
            questions: [{ index: 0, question: "Which one?", options: ["a", "b"] }],
          });
          yield* process.emitInbound("pick-1", "droid.pick_options", { options: [] });
          yield* process.settle;
          const answers = Object.fromEntries(process.answers().map((a) => [a.id, a.result]));
          assert.deepStrictEqual(answers["perm-1"], { selectedOption: "cancel" });
          assert.deepStrictEqual(answers["ask-1"], { cancelled: true, answers: [] });
          assert.deepStrictEqual(answers["pick-1"], { selectedIds: [], cancelled: true });
          assert.deepStrictEqual(terminals(opened.all), []);
          assert.isFalse(opened.all.some((event) => event.type === "runtime_request.updated"));
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("process exit", () => {
    it.effect(
      "[VAL-TURN-014] the process exiting mid-turn fails the turn broken and settles open items",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          const thread = yield* ensure(opened);
          yield* startTurn(opened.runtime, thread);
          const process = fake.current();
          yield* process.emit({
            type: "tool_call",
            toolUse: { type: "tool_use", id: "tool-1", name: "Execute", input: { command: "ls" } },
          });
          yield* process.emit({
            type: "assistant_text_delta",
            messageId: "assistant-1",
            blockIndex: 0,
            textDelta: "Working",
          });
          yield* process.settle;
          yield* process.exit(1);

          const terminal = yield* opened.takeEvent(isTerminal);
          assert.isTrue(terminal.type === "turn.terminal");
          if (terminal.type !== "turn.terminal") return;
          assert.strictEqual(terminal.status, "failed");
          assert.strictEqual(terminal.threadDisposition, "broken");
          assert.isTrue("failureItemOrdinal" in terminal);
          assert.strictEqual(terminal.failure?.class, "transport_error");

          const session = yield* opened.takeEvent(
            (event) =>
              event.type === "provider_session.updated" &&
              (event.providerSession.status === "stopped" ||
                event.providerSession.status === "error"),
          );
          assert.isTrue(session.type === "provider_session.updated");
          assert.strictEqual(terminals(opened.all).length, 1);
          const lastTool = opened.all.findLast(
            (event) =>
              event.type === "turn_item.updated" && event.turnItem.type === "command_execution",
          );
          const lastText = opened.all.findLast(
            (event) =>
              event.type === "turn_item.updated" && event.turnItem.type === "assistant_message",
          );
          assert.isTrue(
            lastTool?.type === "turn_item.updated" && lastTool.turnItem.status === "failed",
          );
          assert.isTrue(
            lastText?.type === "turn_item.updated" && lastText.turnItem.status === "completed",
          );
          const runtimeRequests = opened.all.filter(
            (event) => event.type === "runtime_request.updated",
          );
          assert.isTrue(
            runtimeRequests.every(
              (event) =>
                event.type === "runtime_request.updated" &&
                event.runtimeRequest.status !== "pending",
            ),
          );
          const ended = yield* Deferred.await(opened.streamEnded);
          assert.isTrue(Exit.isFailure(ended));
          assert.include(JSON.stringify(ended), "ProviderAdapterEventStreamError");
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-014] a process_exit completion fails the turn broken and reports the session before the EOF follows",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          const thread = yield* ensure(opened);
          yield* startTurn(opened.runtime, thread);
          const process = fake.current();
          yield* process.emit({
            type: "tool_call",
            toolUse: { type: "tool_use", id: "tool-1", name: "Execute", input: { command: "ls" } },
          });
          yield* process.emit({
            type: "assistant_text_delta",
            messageId: "assistant-1",
            blockIndex: 0,
            textDelta: "Working",
          });
          yield* process.emit(completed(1, "process_exit"));

          const terminal = yield* opened.takeEvent(isTerminal);
          assert.isTrue(terminal.type === "turn.terminal");
          if (terminal.type !== "turn.terminal") return;
          assert.strictEqual(terminal.status, "failed");
          assert.strictEqual(terminal.threadDisposition, "broken");
          assert.strictEqual(terminal.failure?.class, "transport_error");
          assert.strictEqual(terminal.failure?.code, "process_exit");
          yield* opened.takeEvent((event) => event.type === "provider_session.updated");

          // Nothing has ended the process yet: the notification alone settled everything.
          assert.isFalse(yield* Deferred.isDone(opened.streamEnded));
          assert.deepStrictEqual(openItems(opened.all), []);
          const tool = opened.all.findLast(
            (event) =>
              event.type === "turn_item.updated" && event.turnItem.type === "command_execution",
          );
          assert.isTrue(tool?.type === "turn_item.updated" && tool.turnItem.status === "failed");
          const threadUpdate = opened.all.findLast(
            (event) => event.type === "provider_thread.updated",
          );
          assert.isTrue(
            threadUpdate?.type === "provider_thread.updated" &&
              threadUpdate.providerThread.status === "error",
          );
          const session = opened.all.findLast((event) => event.type === "provider_session.updated");
          assert.isTrue(
            session?.type === "provider_session.updated" &&
              session.providerSession.status === "error" &&
              session.providerSession.lastError !== null,
          );
          assert.notInclude(sessionStatuses(opened.all), "ready");

          const next = yield* startTurn(opened.runtime, thread, { runOrdinal: 2 }).pipe(
            Effect.exit,
          );
          assert.isTrue(Exit.isFailure(next), "a session whose process is exiting takes no turn");

          yield* process.exit(0);
          const ended = yield* Deferred.await(opened.streamEnded);
          assert.isTrue(Exit.isFailure(ended));
          assert.strictEqual(terminals(opened.all).length, 1);
          assert.deepStrictEqual(openItems(opened.all), []);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-014] a process_exit completion while the session closes ends the turn interrupted, broken, and stopped",
      () =>
        Effect.gen(function* () {
          const interruptSeen = yield* Deferred.make<void>();
          const fake = yield* makeFakeDroid({
            overrides: {
              // Held so the close stays in flight while Droid reports the exit.
              "droid.interrupt_session": () => {
                Deferred.doneUnsafe(interruptSeen, Exit.void);
                return "silent";
              },
            },
          });
          const sessionScope = yield* Scope.make();
          const opened = yield* openRuntime(fake, { sessionScope });
          const thread = yield* ensure(opened);
          yield* startTurn(opened.runtime, thread);
          const process = fake.current();
          yield* process.emit({
            type: "tool_call",
            toolUse: { type: "tool_use", id: "tool-1", name: "Execute", input: { command: "ls" } },
          });
          yield* process.settle;
          yield* Scope.close(sessionScope, Exit.void).pipe(Effect.forkScoped);
          yield* Deferred.await(interruptSeen);
          yield* process.emit(completed(1, "process_exit"));

          const terminal = yield* opened.takeEvent(isTerminal);
          assert.isTrue(
            terminal.type === "turn.terminal" &&
              terminal.status === "interrupted" &&
              terminal.threadDisposition === "broken" &&
              terminal.failure === null,
          );
          yield* opened.takeEvent((event) => event.type === "provider_session.updated");
          assert.strictEqual(sessionStatuses(opened.all).at(-1), "stopped");

          yield* TestClock.adjust("10 seconds");
          const ended = yield* Deferred.await(opened.streamEnded);
          assert.isTrue(Exit.isSuccess(ended));
          assert.strictEqual(terminals(opened.all).length, 1);
          assert.deepStrictEqual(openItems(opened.all), []);
          const tool = opened.all.findLast(
            (event) =>
              event.type === "turn_item.updated" && event.turnItem.type === "command_execution",
          );
          assert.isTrue(
            tool?.type === "turn_item.updated" && tool.turnItem.status === "interrupted",
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-014] the process exiting while idle ends the session without a terminal",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake);
          yield* ensure(opened);
          yield* fake.current().exit(0);
          yield* opened.takeEvent(
            (event) =>
              event.type === "provider_session.updated" &&
              (event.providerSession.status === "stopped" ||
                event.providerSession.status === "error"),
          );
          assert.deepStrictEqual(terminals(opened.all), []);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-TURN-006] closing the runtime scope closes the Droid session", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        yield* Effect.gen(function* () {
          const opened = yield* openRuntime(fake);
          yield* ensure(opened);
        }).pipe(Effect.scoped);
        assert.include(methodsOf(fake), "droid.close_session");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("authentication failures", () => {
    it.effect(
      "[VAL-STATUS-008] model_authentication_failed signals the failure and a later completed turn recovers",
      () =>
        Effect.gen(function* () {
          const signals: Array<string> = [];
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake, {
            authSignal: {
              authenticationFailed: Effect.sync(() => void signals.push("failed")),
              authenticated: Effect.sync(() => void signals.push("authenticated")),
            },
          });
          const thread = yield* ensure(opened);
          const process = fake.current();
          yield* startTurn(opened.runtime, thread);
          yield* process.emit({
            type: "error",
            message: "No access token available",
            errorType: "AuthenticationError",
          });
          yield* process.emit(completed(1, "model_authentication_failed"));
          const failed = yield* opened.takeEvent(isTerminal);
          assert.isTrue(
            failed.type === "turn.terminal" &&
              failed.status === "failed" &&
              failed.failure?.code === "model_authentication_failed" &&
              failed.failure.message.includes("No access token available"),
          );
          assert.deepStrictEqual(signals, ["failed"]);

          yield* runTurn(opened, fake, thread, { runOrdinal: 2 });
          assert.deepStrictEqual(signals, ["failed", "authenticated"]);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-STATUS-008] other failures and interrupted turns send no authentication signal",
      () =>
        Effect.gen(function* () {
          const signals: Array<string> = [];
          const fake = yield* makeFakeDroid();
          const opened = yield* openRuntime(fake, {
            authSignal: {
              authenticationFailed: Effect.sync(() => void signals.push("failed")),
              authenticated: Effect.sync(() => void signals.push("authenticated")),
            },
          });
          const thread = yield* ensure(opened);
          yield* runTurn(opened, fake, thread, { reason: "model_rate_limited" });
          yield* runTurn(opened, fake, thread, { runOrdinal: 2, reason: "cancelled" });
          assert.deepStrictEqual(signals, []);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("history and snapshots", () => {
    const userMessage = (id: string, text: string, extra: Record<string, unknown> = {}) => ({
      id,
      role: "user",
      content: [{ type: "text", text }],
      createdAt: 1_700_000_000_000,
      ...extra,
    });
    const assistantMessage = (id: string, text: string) => ({
      id,
      role: "assistant",
      content: [{ type: "text", text }],
      createdAt: 1_700_000_001_000,
    });

    it.effect(
      "[VAL-TURN-016] readThreadSnapshot returns the visible history and the flags say so",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid({
            sessions: {
              "stored-session": {
                settings: FAKE_DROID_DEFAULT_SETTINGS,
                messages: [
                  userMessage("u1", "What is 2+2?"),
                  assistantMessage("a1", "It is 4."),
                  {
                    id: "t1",
                    role: "tool",
                    content: [{ type: "tool_result", content: "ignored" }],
                  },
                ],
              },
            },
          });
          const opened = yield* openRuntime(fake);
          const capabilities = yield* opened.adapter.getCapabilities();
          assert.isTrue(capabilities.threads.canReadThreadSnapshot);
          assert.isTrue(capabilities.checkpointing.providerCanReadConversationSnapshot);
          const resumed = yield* opened.runtime.resumeThread({
            providerThread: yield* makeProviderThread("stored-session"),
          });
          const snapshot = yield* opened.runtime.readThreadSnapshot({ providerThread: resumed });
          assert.deepStrictEqual(
            snapshot.messages.map((message) => [message.role, message.text, message.createdBy]),
            [
              ["user", "What is 2+2?", "user"],
              ["assistant", "It is 4.", "agent"],
            ],
          );
          assert.deepStrictEqual(snapshot.providerTurns, []);
          assert.deepStrictEqual(snapshot.runtimeRequests, []);
          assert.strictEqual(snapshot.providerThread.id, resumed.id);
          assert.isTrue(
            snapshot.messages.every(
              (message) => message.threadId === THREAD_ID && message.streaming === false,
            ),
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("[VAL-TURN-016] a snapshot for a thread this session does not host is refused", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const opened = yield* openRuntime(fake);
        const exit = yield* opened.runtime
          .readThreadSnapshot({ providerThread: yield* makeProviderThread("elsewhere") })
          .pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(exit));
        assert.include(JSON.stringify(exit), "ProviderAdapterReadThreadSnapshotError");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "[VAL-TURN-021] injected context messages never appear in an event, an item, or a snapshot",
      () =>
        Effect.gen(function* () {
          const CONTEXT_TEXT = "SYNTHETIC-PRIVATE-CONTEXT-7f3a-never-visible";
          const REMINDER_TEXT = "SYNTHETIC-SYSTEM-REMINDER-91bc-never-visible";
          const LLM_ONLY_TEXT = "SYNTHETIC-LLM-ONLY-4d2e-never-visible";
          const fake = yield* makeFakeDroid({
            sessions: {
              "stored-session": {
                settings: FAKE_DROID_DEFAULT_SETTINGS,
                messages: [
                  userMessage("context-0b1c2d3e", CONTEXT_TEXT, { visibility: "llm_only" }),
                  userMessage("u1", "What is 2+2?"),
                  userMessage("__internal__system-reminder__8a9b", REMINDER_TEXT, {
                    visibility: "llm_only",
                  }),
                  assistantMessage("a1", "It is 4."),
                  userMessage("v1", LLM_ONLY_TEXT, { visibility: "llm_only" }),
                ],
              },
            },
          });
          const opened = yield* openRuntime(fake);
          const resumed = yield* opened.runtime.resumeThread({
            providerThread: yield* makeProviderThread("stored-session"),
          });
          yield* startTurn(opened.runtime, resumed, { text: "And 3+3?" });
          const process = fake.current();
          yield* process.emit({
            type: "create_message",
            message: {
              id: "context-live",
              role: "user",
              visibility: "llm_only",
              content: [],
            },
          });
          yield* process.emit({
            type: "assistant_text_delta",
            messageId: "a2",
            blockIndex: 0,
            textDelta: "It is 6.",
          });
          yield* process.emit({ type: "assistant_text_complete", messageId: "a2", blockIndex: 0 });
          yield* process.emit(completed(1));
          yield* opened.takeEvent(isTerminal);
          const snapshot = yield* opened.runtime.readThreadSnapshot({ providerThread: resumed });

          const everything = JSON.stringify({
            events: opened.all,
            snapshot,
            frames: fake.current().frames(),
          });
          const withoutFrames = JSON.stringify({ events: opened.all, snapshot });
          for (const secret of [CONTEXT_TEXT, REMINDER_TEXT, LLM_ONLY_TEXT]) {
            assert.notInclude(withoutFrames, secret);
          }
          assert.include(everything, "And 3+3?");
          assert.deepStrictEqual(
            snapshot.messages.map((message) => message.text),
            ["What is 2+2?", "It is 4."],
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("against the mock agent", () => {
    const openMock = Effect.fnUntraced(function* (env: Record<string, string> = {}) {
      const mock = yield* makeRpcMockAgent(env);
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const opened = yield* openRuntime(undefined, {
        spawner,
        binaryPath: mock.binaryPath,
      });
      return { mock, opened };
    });

    const mockLayer = Layer.mergeAll(
      layerTest,
      TestProviderHost.layer({ cwd: process.cwd() }).pipe(Layer.provide(NodeServices.layer)),
    );

    it.effect("[VAL-TURN-006] a plain turn completes through the real runtime", () =>
      live(
        Effect.gen(function* () {
          const { opened } = yield* openMock();
          const thread = yield* ensure(opened);
          yield* startTurn(opened.runtime, thread, { text: "Say hi" });
          const terminal = yield* opened.takeEvent(isTerminal);
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          const started = opened.all.find(
            (event) =>
              event.type === "provider_turn.updated" && event.providerTurn.status === "running",
          );
          assert.isTrue(
            started?.type === "provider_turn.updated" &&
              started.providerTurn.nativeTurnRef?.nativeId === messageIdFor(1),
          );
        }).pipe(Effect.scoped, Effect.provide(mockLayer)),
      ),
    );

    it.effect("[VAL-TURN-015] UNKNOWN_EVENT mid-turn is dropped and the turn completes", () =>
      live(
        Effect.gen(function* () {
          const { opened } = yield* openMock();
          const thread = yield* ensure(opened);
          yield* startTurn(opened.runtime, thread, { text: "UNKNOWN_EVENT please" });
          const terminal = yield* opened.takeEvent(isTerminal);
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          assert.isFalse(
            opened.all.some(
              (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
            ),
          );
        }).pipe(Effect.scoped, Effect.provide(mockLayer)),
      ),
    );

    it.effect("[VAL-TURN-014] CRASH_MID_TURN fails the turn broken and the session stops", () =>
      live(
        Effect.gen(function* () {
          const { opened } = yield* openMock();
          const thread = yield* ensure(opened);
          yield* startTurn(opened.runtime, thread, { text: "CRASH_MID_TURN please" });
          const terminal = yield* opened.takeEvent(isTerminal);
          assert.isTrue(
            terminal.type === "turn.terminal" &&
              terminal.status === "failed" &&
              terminal.threadDisposition === "broken",
          );
          const ended = yield* Deferred.await(opened.streamEnded);
          assert.isTrue(Exit.isFailure(ended));
          assert.strictEqual(terminals(opened.all).length, 1);
        }).pipe(Effect.scoped, Effect.provide(mockLayer)),
      ),
    );

    it.effect(
      "[VAL-TURN-014] EXIT_MID_TURN completes with process_exit and fails the turn broken",
      () =>
        live(
          Effect.gen(function* () {
            const { opened } = yield* openMock();
            const thread = yield* ensure(opened);
            yield* startTurn(opened.runtime, thread, { text: "EXIT_MID_TURN please" });
            const terminal = yield* opened.takeEvent(isTerminal);
            assert.isTrue(
              terminal.type === "turn.terminal" &&
                terminal.status === "failed" &&
                terminal.threadDisposition === "broken" &&
                terminal.failure?.code === "process_exit",
            );
            // The mock exits right after the completion, so the EOF follows it.
            const ended = yield* Deferred.await(opened.streamEnded);
            assert.isTrue(Exit.isFailure(ended));
            assert.strictEqual(terminals(opened.all).length, 1);
            assert.deepStrictEqual(openItems(opened.all), []);
            assert.notInclude(sessionStatuses(opened.all), "ready");
            const finalStatus = sessionStatuses(opened.all).at(-1);
            assert.isTrue(finalStatus === "stopped" || finalStatus === "error");
            const items = opened.all.filter(
              (event) => event.type === "turn_item.updated" && event.turnItem.type !== "error",
            );
            assert.isAbove(items.length, 0);
            const runtimeRequests = opened.all.filter(
              (event) => event.type === "runtime_request.updated",
            );
            assert.isTrue(
              runtimeRequests.every(
                (event) =>
                  event.type === "runtime_request.updated" &&
                  event.runtimeRequest.status !== "pending",
              ),
            );
          }).pipe(Effect.scoped, Effect.provide(mockLayer)),
        ),
    );

    it.effect(
      "[VAL-STATUS-008] AUTH_FAIL fails the turn and signals the authentication failure",
      () =>
        live(
          Effect.gen(function* () {
            const signals: Array<string> = [];
            const mock = yield* makeRpcMockAgent();
            const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
            const opened = yield* openRuntime(undefined, {
              spawner,
              binaryPath: mock.binaryPath,
              authSignal: {
                authenticationFailed: Effect.sync(() => void signals.push("failed")),
                authenticated: Effect.sync(() => void signals.push("authenticated")),
              },
            });
            const thread = yield* ensure(opened);
            yield* startTurn(opened.runtime, thread, { text: "AUTH_FAIL please" });
            const terminal = yield* opened.takeEvent(isTerminal);
            assert.isTrue(
              terminal.type === "turn.terminal" &&
                terminal.status === "failed" &&
                terminal.failure?.code === "model_authentication_failed",
            );
            assert.deepStrictEqual(signals, ["failed"]);
            yield* startTurn(opened.runtime, thread, { runOrdinal: 2, text: "plain again" });
            yield* opened.takeEvent(isTerminal);
            assert.deepStrictEqual(signals, ["failed", "authenticated"]);
          }).pipe(Effect.scoped, Effect.provide(mockLayer)),
        ),
    );

    it.effect("[VAL-TURN-004] a second process resumes the first process's session", () =>
      live(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-droid-state-" });
          const mock = yield* makeRpcMockAgent({ DROID_MOCK_STATE_DIR: stateDir });
          const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
          const first = yield* Effect.gen(function* () {
            const opened = yield* openRuntime(undefined, { spawner, binaryPath: mock.binaryPath });
            return yield* ensure(opened);
          }).pipe(Effect.scoped);
          const second = yield* openRuntime(undefined, { spawner, binaryPath: mock.binaryPath });
          const resumed = yield* second.runtime.resumeThread({ providerThread: first });
          assert.strictEqual(resumed.nativeThreadRef?.nativeId, first.nativeThreadRef?.nativeId);
          yield* startTurn(second.runtime, resumed, { text: "after resume" });
          const terminal = yield* second.takeEvent(isTerminal);
          assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
          const recv = (yield* mock.readLog)
            .filter((entry) => entry["type"] === "recv")
            .map((entry) => asRecord(entry["message"])["method"]);
          assert.deepStrictEqual(
            recv.filter((method) => method === "droid.update_session_settings"),
            ["droid.update_session_settings"],
          );
        }).pipe(Effect.scoped, Effect.provide(mockLayer)),
      ),
    );
  });
});
