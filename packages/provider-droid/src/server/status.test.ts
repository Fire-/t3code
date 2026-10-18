import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import type { CustomModelSetting, ServerProvider } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Logger from "effect/Logger";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { vi } from "vite-plus/test";

import type { DroidSettings } from "../settings.ts";
import { DROID_HOOKS_OFF_SETTINGS } from "./hooksOffSettings.ts";
import { DROID_PROTOCOL_VERSION } from "./protocol.ts";
import { makeRpcMockAgent } from "./rpcMockAgent.testkit.ts";
import {
  buildDroidModelsFromListModels,
  buildInitialDroidProviderSnapshot,
  checkDroidProviderStatus,
  detectDroidAuthHint,
  DROID_DISCOVERY_TIMEOUT_MS,
  DROID_PRESENTATION,
} from "./status.ts";

const encoder = new TextEncoder();
const decodeLine = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);
const encodeLine = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

/** Outside the valid PID range, so cleanup of a fake process can never signal a real one. */
const FAKE_PID = 999_999_999;
/** An obviously fake key (ruling D14); nothing here ever holds a real credential. */
const FAKE_API_KEY = "not-a-real-factory-key";
const NOT_A_CREDENTIAL = "not-a-credential-do-not-read";

const standardCommand = (command: ChildProcess.Command): ChildProcess.StandardCommand =>
  ChildProcess.isStandardCommand(command) ? command : assert.fail("unexpected pipeline");

const asRecord = (value: unknown): Record<string, unknown> =>
  Predicate.isObject(value) ? value : assert.fail(`expected an object, got ${typeof value}`);

function processHandle(input: {
  readonly stdout?: string;
  readonly stderr?: string;
  readonly exitCode?: number;
  readonly hangs?: boolean;
}) {
  const bytes = (value: string | undefined) =>
    value === undefined || value.length === 0
      ? Stream.empty
      : Stream.succeed(encoder.encode(value));
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(FAKE_PID),
    exitCode: input.hangs
      ? Effect.never
      : Effect.succeed(ChildProcessSpawner.ExitCode(input.exitCode ?? 0)),
    isRunning: Effect.succeed(input.hangs === true),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.drain,
    stdout: bytes(input.stdout),
    stderr: bytes(input.stderr),
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
}

function recordingSpawner(
  respond: (command: ChildProcess.StandardCommand) => ReturnType<typeof processHandle>,
) {
  const launches: Array<ChildProcess.StandardCommand> = [];
  const spawner = ChildProcessSpawner.make((command) => {
    launches.push(standardCommand(command));
    return Effect.succeed(respond(standardCommand(command)));
  });
  return { spawner, launches };
}

type DiscoveryScript =
  | { readonly kind: "result"; readonly result: unknown }
  | { readonly kind: "error" }
  | { readonly kind: "silent" }
  | { readonly kind: "exits" };

const replyLine = (id: unknown, body: Record<string, unknown>) =>
  `${encodeLine({
    jsonrpc: "2.0",
    type: "response",
    factoryApiVersion: "1.0.0",
    factoryProtocolVersion: DROID_PROTOCOL_VERSION,
    id,
    ...body,
  })}\n`;

/**
 * A `droid` that answers `--version` and, for the stream-jsonrpc spawn, plays one discovery
 * script. `requested` completes once `droid.list_models` has been written to its stdin.
 */
const makeScriptedDroid = Effect.fn("makeScriptedDroid")(function* (
  script: DiscoveryScript,
  version: { readonly stdout?: string; readonly exitCode?: number } = { stdout: "0.244.0\n" },
) {
  const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
  const exitCode = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
  const requested = yield* Deferred.make<void>();
  const launches: Array<ChildProcess.StandardCommand> = [];
  const received: Array<Record<string, unknown>> = [];
  const decoder = new TextDecoder();
  let pendingInput = "";

  const answer = Effect.fnUntraced(function* (line: Record<string, unknown>) {
    received.push(line);
    if (line.method !== "droid.list_models") return;
    yield* Deferred.succeed(requested, undefined);
    switch (script.kind) {
      case "result":
        yield* Queue.offer(stdout, encoder.encode(replyLine(line.id, { result: script.result })));
        return;
      case "error":
        yield* Queue.offer(
          stdout,
          encoder.encode(replyLine(line.id, { error: { code: -32603, message: "boom" } })),
        );
        return;
      case "silent":
        return;
      case "exits":
        yield* Queue.end(stdout);
        yield* Deferred.succeed(exitCode, ChildProcessSpawner.ExitCode(1));
        return;
    }
  });

  const rpcHandle = ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(FAKE_PID),
    exitCode: Deferred.await(exitCode),
    isRunning: Effect.map(Deferred.isDone(exitCode), (done) => !done),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.forEach((chunk: Uint8Array) =>
      Effect.suspend(() => {
        pendingInput += decoder.decode(chunk, { stream: true });
        const lines = pendingInput.split("\n");
        pendingInput = lines.pop() ?? "";
        return Effect.forEach(
          lines.filter((line) => line.trim() !== ""),
          (line) => answer(decodeLine(line)),
          { discard: true },
        );
      }),
    ),
    stdout: Stream.fromQueue(stdout),
    stderr: Stream.empty,
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });

  const spawner = ChildProcessSpawner.make((command) => {
    const standard = standardCommand(command);
    launches.push(standard);
    return Effect.succeed(standard.args.includes("--version") ? processHandle(version) : rpcHandle);
  });
  return {
    spawner,
    launches,
    received,
    requested,
    markExited: (code: number) =>
      Deferred.doneUnsafe(exitCode, Exit.succeed(ChildProcessSpawner.ExitCode(code))),
  };
});

const execSpawns = (launches: ReadonlyArray<ChildProcess.StandardCommand>) =>
  launches.filter((launch) => launch.args[0] === "exec");

/** Routes `--version` to a canned answer and everything else to the real spawner. */
const withVersionProbe = (real: ChildProcessSpawner.ChildProcessSpawner["Service"]) =>
  ChildProcessSpawner.make((command) => {
    const standard = standardCommand(command);
    return standard.args.includes("--version")
      ? Effect.succeed(processHandle({ stdout: "0.244.0\n" }))
      : real.spawn(standard);
  });

const settings = (overrides: Partial<DroidSettings> = {}): DroidSettings => ({
  enabled: true,
  binaryPath: "droid",
  customModels: [],
  ...overrides,
});

const provide = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
  >,
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
) =>
  effect.pipe(
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    Effect.provide(NodeServices.layer),
  );

/** Real subprocess tests run on the live clock; the timeout only fails a hung test. */
const live = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.timeout("30 seconds"), TestClock.withLive);

const yieldSome = Effect.forEach(Array.from({ length: 16 }), () => Effect.yieldNow, {
  discard: true,
});

const captureLogs = () => {
  const entries: Array<{ readonly level: string; readonly text: string }> = [];
  const logger = Logger.make<unknown, void>((options) => {
    const message = Array.isArray(options.message) ? options.message : [options.message];
    const annotations = options.fiber.getRef(References.CurrentLogAnnotations);
    entries.push({
      level: options.logLevel,
      text: [
        ...message.map(String),
        ...Object.entries(annotations).map(([key, value]) => `${key}=${String(value)}`),
      ].join(" "),
    });
  });
  const provideLogs = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.provide(Logger.layer([logger], { mergeWithExisting: false })),
      Effect.provideService(References.MinimumLogLevel, "All"),
    );
  return { entries, provideLogs };
};

const listedModel = (overrides: Record<string, unknown> = {}) => ({
  id: "claude-opus-5-5",
  modelId: "claude-opus-5-5",
  modelProvider: "anthropic",
  displayName: "Opus 5.5",
  shortDisplayName: "Opus 5.5",
  supportedReasoningEfforts: ["low", "medium", "high", "xhigh", "max"],
  defaultReasoningEffort: "medium",
  deprecated: false,
  noImageSupport: false,
  supportsPDFs: true,
  usesUSBasedInference: false,
  tokenMultiplier: 1.6,
  isNew: false,
  isCustom: false,
  disabled: false,
  ...overrides,
});

const listed = (...models: ReadonlyArray<Record<string, unknown>>) => ({ models });

const receivedMethods = (log: ReadonlyArray<Record<string, unknown>>) =>
  log
    .filter((entry) => entry.type === "recv")
    .map((entry) => asRecord(entry.message).method)
    .filter(Predicate.isString);

const M2_RUNTIME_MODES: ReadonlyArray<string> = [
  "approval-required",
  "auto-accept-edits",
  "auto",
  "full-access",
];

const assertM2Presentation = (
  snapshot: Pick<
    ServerProvider,
    | "displayName"
    | "supportedRuntimeModes"
    | "requiresNewThreadForModelChange"
    | "reportsContextWindow"
    | "supportsConversationRollback"
    | "showInteractionModeToggle"
  >,
) => {
  assert.equal(snapshot.displayName, "Droid");
  assert.deepEqual<ReadonlyArray<string> | undefined>(
    snapshot.supportedRuntimeModes,
    M2_RUNTIME_MODES,
  );
  assert.strictEqual(snapshot.requiresNewThreadForModelChange, false);
  assert.strictEqual(snapshot.reportsContextWindow, false);
  assert.strictEqual(snapshot.supportsConversationRollback, false);
  assert.strictEqual(snapshot.showInteractionModeToggle, false);
};

describe("DROID_PRESENTATION", () => {
  it("[VAL-REG-002][VAL-STATUS-009] names Droid and pins the M2 values", () => {
    assert.equal(DROID_PRESENTATION.displayName, "Droid");
    assert.deepEqual<ReadonlyArray<string>>(
      DROID_PRESENTATION.supportedRuntimeModes,
      M2_RUNTIME_MODES,
    );
    assert.isFalse(DROID_PRESENTATION.requiresNewThreadForModelChange);
    assert.isFalse(DROID_PRESENTATION.reportsContextWindow);
    // Each turns on in the feature that implements it: VAL-PLAN-012 (M4) and VAL-HIST-004 (M5).
    assert.isFalse(DROID_PRESENTATION.supportsConversationRollback);
    assert.isFalse(DROID_PRESENTATION.showInteractionModeToggle);
  });
});

describe("buildInitialDroidProviderSnapshot", () => {
  it.effect("reports a disabled provider before any probe", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialDroidProviderSnapshot(settings({ enabled: false }));
      assert.equal(snapshot.status, "disabled");
      assert.isFalse(snapshot.enabled);
      assert.equal(snapshot.message, "Droid is disabled in T3 Code settings.");
      assertM2Presentation(snapshot);
    }),
  );

  it.effect("reports a pending probe for an enabled provider and carries the presentation", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialDroidProviderSnapshot(settings());
      assert.equal(snapshot.status, "warning");
      assert.equal(snapshot.message, "Checking Droid CLI availability...");
      assert.equal(snapshot.auth.status, "unknown");
      assertM2Presentation(snapshot);
    }),
  );
});

describe("buildDroidModelsFromListModels", () => {
  it("[VAL-STATUS-002] groups models by the subProvider derived from modelProvider", () => {
    const models = buildDroidModelsFromListModels(
      listed(
        listedModel({ id: "a", modelProvider: "anthropic" }),
        listedModel({ id: "b", modelProvider: "openai" }),
        listedModel({ id: "c", modelProvider: "google" }),
        listedModel({ id: "d", modelProvider: "factory" }),
        listedModel({ id: "e", modelProvider: "xai" }),
        listedModel({ id: "f", modelProvider: "mistral" }),
        listedModel({ id: "g", modelProvider: " ANTHROPIC " }),
        listedModel({ id: "h", modelProvider: 7 }),
        listedModel({ id: "i", modelProvider: "  " }),
      ),
    );
    assert.deepEqual(
      models.map((model) => [model.slug, model.subProvider]),
      [
        ["a", "Anthropic"],
        ["b", "OpenAI"],
        ["c", "Google"],
        ["d", "Droid Core"],
        ["e", "xAI"],
        ["f", "Mistral"],
        ["g", "Anthropic"],
        ["h", undefined],
        ["i", undefined],
      ],
    );
  });

  it("[VAL-STATUS-002] gives the router no group even when it names a provider", () => {
    const [router] = buildDroidModelsFromListModels(
      listed(listedModel({ id: "auto", kind: "router", modelProvider: "factory" })),
    );
    assert.isUndefined(router?.subProvider);
    assert.isFalse("subProvider" in (router ?? {}));
  });

  it("[VAL-STATUS-003] keeps the multiplier and promo label in the name", () => {
    const names = buildDroidModelsFromListModels(
      listed(
        listedModel({ id: "a", tokenMultiplier: 0.272, promoLabel: "50% off" }),
        listedModel({ id: "b", tokenMultiplier: 0.30000000000000004 }),
        listedModel({ id: "c", tokenMultiplier: 1.6 }),
        listedModel({ id: "d", tokenMultiplier: 1 }),
        listedModel({ id: "e", tokenMultiplier: 0 }),
        listedModel({ id: "f", tokenMultiplier: 0.123456 }),
        listedModel({ id: "g", tokenMultiplier: 0.5, promoLabel: "  New  " }),
      ),
    ).map((model) => model.name);
    assert.deepEqual(names, [
      "Opus 5.5 (0.272x, 50% off)",
      "Opus 5.5 (0.3x)",
      "Opus 5.5 (1.6x)",
      "Opus 5.5 (1x)",
      "Opus 5.5 (0x)",
      "Opus 5.5 (0.1235x)",
      "Opus 5.5 (0.5x, New)",
    ]);
  });

  it("[VAL-STATUS-003] leaves the multiplier out for the router", () => {
    const names = buildDroidModelsFromListModels(
      listed(
        listedModel({ id: "auto", kind: "router", displayName: "Auto Model", tokenMultiplier: 1 }),
        listedModel({
          id: "auto-promo",
          kind: "router",
          displayName: "Auto Model",
          tokenMultiplier: 1,
          promoLabel: "Beta",
        }),
      ),
    ).map((model) => model.name);
    assert.deepEqual(names, ["Auto Model", "Auto Model (Beta)"]);
  });

  it("[VAL-STATUS-003] leaves the multiplier out for values that are negative or not finite", () => {
    const names = buildDroidModelsFromListModels(
      listed(
        listedModel({ id: "negative", tokenMultiplier: -1 }),
        listedModel({ id: "infinite", tokenMultiplier: Number.POSITIVE_INFINITY }),
        listedModel({ id: "nan", tokenMultiplier: Number.NaN }),
        listedModel({ id: "string", tokenMultiplier: "1.5" }),
        listedModel({ id: "null", tokenMultiplier: null }),
        listedModel({ id: "missing", tokenMultiplier: undefined }),
        listedModel({ id: "promo", tokenMultiplier: -2, promoLabel: "Free" }),
      ),
    ).map((model) => model.name);
    assert.deepEqual(names, [
      "Opus 5.5",
      "Opus 5.5",
      "Opus 5.5",
      "Opus 5.5",
      "Opus 5.5",
      "Opus 5.5",
      "Opus 5.5 (Free)",
    ]);
  });

  it("[VAL-STATUS-003] passes model ids through unchanged and deduplicates them", () => {
    const models = buildDroidModelsFromListModels(
      listed(
        listedModel({ id: "claude-opus-4-5-20251101", displayName: "First" }),
        listedModel({ id: "gpt-6-sol-fast" }),
        listedModel({ id: "custom:Mixed.Case_1" }),
        listedModel({ id: "claude-opus-4-5-20251101", displayName: "Second" }),
        listedModel({ id: "   " }),
        listedModel({ id: "" }),
      ),
    );
    assert.deepEqual(
      models.map((model) => model.slug),
      ["claude-opus-4-5-20251101", "gpt-6-sol-fast", "custom:Mixed.Case_1"],
    );
    assert.equal(models[0]?.name, "First (1.6x)");
    assert.isTrue(models.every((model) => model.isCustom === false));
  });

  it("[VAL-STATUS-003] drops deprecated and disabled entries", () => {
    const models = buildDroidModelsFromListModels(
      listed(
        listedModel({ id: "kept" }),
        listedModel({ id: "deprecated", deprecated: true }),
        listedModel({ id: "disabled", disabled: true }),
        listedModel({ id: "both", deprecated: true, disabled: true }),
      ),
    );
    assert.deepEqual(
      models.map((model) => model.slug),
      ["kept"],
    );
  });

  it("[VAL-REG-017] adds a reasoningEffort select that defaults to Droid's default effort", () => {
    const [model] = buildDroidModelsFromListModels(listed(listedModel()));
    assert.deepEqual(model?.capabilities?.optionDescriptors, [
      {
        id: "reasoningEffort",
        label: "Reasoning",
        type: "select",
        options: [
          { id: "low", label: "Low" },
          { id: "medium", label: "Medium", isDefault: true },
          { id: "high", label: "High" },
          { id: "xhigh", label: "Xhigh" },
          { id: "max", label: "Max" },
        ],
        currentValue: "medium",
      },
    ]);
  });

  it("[VAL-REG-017] falls back to the first effort when Droid's default is not supported", () => {
    const [model] = buildDroidModelsFromListModels(
      listed(
        listedModel({
          supportedReasoningEfforts: ["off", "high"],
          defaultReasoningEffort: "ultra",
        }),
      ),
    );
    const descriptor = model?.capabilities?.optionDescriptors?.[0];
    assert.equal(descriptor?.type, "select");
    assert.equal(descriptor?.currentValue, "off");
  });

  it("[VAL-REG-017] gives models without efforts, and the router, no reasoning option", () => {
    const models = buildDroidModelsFromListModels(
      listed(
        listedModel({ id: "plain", supportedReasoningEfforts: [] }),
        listedModel({ id: "unset", supportedReasoningEfforts: undefined }),
        listedModel({
          id: "auto",
          kind: "router",
          supportedReasoningEfforts: ["none"],
          defaultReasoningEffort: "none",
        }),
      ),
    );
    assert.deepEqual(
      models.map((model) => model.capabilities?.optionDescriptors),
      [[], [], []],
    );
  });

  it("marks only Droid's default model as the default", () => {
    const models = buildDroidModelsFromListModels(
      listed(listedModel({ id: "gpt-6-sol" }), listedModel({ id: "claude-opus-5-5" })),
    );
    assert.deepEqual(
      models.map((model) => [model.slug, model.isDefault]),
      [
        ["gpt-6-sol", undefined],
        ["claude-opus-5-5", true],
      ],
    );
  });

  it("falls back to the slug when a model has no display name", () => {
    const [model] = buildDroidModelsFromListModels(
      listed(listedModel({ id: "bare", displayName: undefined, tokenMultiplier: undefined })),
    );
    assert.equal(model?.name, "bare");
  });

  it("[VAL-STATUS-005] maps bodies that are not a model list to no models", () => {
    for (const body of [
      undefined,
      null,
      "models",
      [],
      {},
      { models: "not-a-model-list" },
      { models: null },
      { models: [] },
      { models: [null, 3, "x", {}] },
    ]) {
      assert.deepEqual(buildDroidModelsFromListModels(body), []);
    }
  });

  it("skips one unreadable entry without dropping the rest of the list", () => {
    const models = buildDroidModelsFromListModels({
      models: [{ displayName: "No id" }, listedModel({ id: "kept" }), 42],
    });
    assert.deepEqual(
      models.map((model) => model.slug),
      ["kept"],
    );
  });
});

describe("detectDroidAuthHint", () => {
  const withFileSystem = <A, E>(
    effect: Effect.Effect<A, E, FileSystem.FileSystem | Path.Path>,
    files: {
      readonly entries: ReadonlyArray<string> | "missing";
    },
  ) => {
    const listings: Array<string> = [];
    const attempts: Array<string> = [];
    const refuse = (operation: string) => (path: string) => {
      attempts.push(`${operation} ${path}`);
      return Effect.die(`${operation} must never be called`);
    };
    const fileSystem = FileSystem.makeNoop({
      readDirectory: (path) => {
        listings.push(path);
        return files.entries === "missing"
          ? Effect.fail(
              PlatformError.systemError({
                _tag: "NotFound",
                module: "FileSystem",
                method: "readDirectory",
                pathOrDescriptor: path,
              }),
            )
          : Effect.succeed([...files.entries]);
      },
      readFile: refuse("readFile"),
      readFileString: refuse("readFileString"),
      open: refuse("open"),
      stream: (path) => {
        attempts.push(`stream ${path}`);
        return Stream.die("stream must never be called");
      },
      copy: refuse("copy"),
      copyFile: refuse("copyFile"),
    });
    return {
      listings,
      attempts,
      run: effect.pipe(
        Effect.provideService(FileSystem.FileSystem, fileSystem),
        Effect.provide(NodeServices.layer),
      ),
    };
  };

  it.effect("[VAL-STATUS-006] reports an API key without touching the filesystem", () =>
    Effect.gen(function* () {
      const probe = withFileSystem(detectDroidAuthHint({ FACTORY_API_KEY: FAKE_API_KEY }), {
        entries: ["auth.json"],
      });
      assert.equal(yield* probe.run, "api-key");
      assert.deepEqual(probe.listings, []);
      assert.deepEqual(probe.attempts, []);
    }),
  );

  it.effect("[VAL-STATUS-006] reports an auth* entry from a listing and never reads a file", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const probe = withFileSystem(
        detectDroidAuthHint({ HOME: "/home/someone", FACTORY_API_KEY: "   " }),
        { entries: ["settings.json", "auth.v2.file", "sessions"] },
      );
      assert.equal(yield* probe.run, "auth-file");
      assert.deepEqual(probe.listings, [path.join("/home/someone", ".factory")]);
      assert.deepEqual(probe.attempts, []);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("[VAL-STATUS-006] gives no hint when nothing decides, and never reads a file", () =>
    Effect.gen(function* () {
      const cases = [
        { environment: { HOME: "/home/someone" }, entries: ["settings.json", "sessions"] },
        { environment: { HOME: "/home/someone" }, entries: "missing" as const },
        { environment: { HOME: "/home/someone", FACTORY_API_KEY: "" }, entries: [] },
        { environment: {}, entries: ["auth.json"] },
        { environment: { HOME: "  ", USERPROFILE: "" }, entries: ["auth.json"] },
      ];
      for (const { environment, entries } of cases) {
        const probe = withFileSystem(detectDroidAuthHint(environment), { entries });
        assert.isUndefined(yield* probe.run);
        assert.deepEqual(probe.attempts, []);
      }
    }),
  );

  it.effect("[VAL-STATUS-006] falls back to USERPROFILE when HOME is blank", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      for (const environment of [
        { USERPROFILE: "C:\\Users\\someone" },
        { HOME: "  ", USERPROFILE: "C:\\Users\\someone" },
        { HOME: "", USERPROFILE: "C:\\Users\\someone" },
      ]) {
        const probe = withFileSystem(detectDroidAuthHint(environment), {
          entries: ["auth.json"],
        });
        assert.equal(yield* probe.run, "auth-file");
        assert.deepEqual(probe.listings, [path.join("C:\\Users\\someone", ".factory")]);
        assert.deepEqual(probe.attempts, []);
      }
      const homeWins = withFileSystem(
        detectDroidAuthHint({ HOME: "/home/someone", USERPROFILE: "C:\\Users\\other" }),
        { entries: [] },
      );
      yield* homeWins.run;
      assert.deepEqual(homeWins.listings, [path.join("/home/someone", ".factory")]);
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});

describe("checkDroidProviderStatus", () => {
  it.effect("never spawns Droid while the provider is disabled", () =>
    Effect.gen(function* () {
      const snapshot = yield* provide(
        checkDroidProviderStatus(settings({ enabled: false })),
        ChildProcessSpawner.make(() => Effect.die("Disabled Droid must not spawn")),
      );
      assert.equal(snapshot.status, "disabled");
      assert.isFalse(snapshot.enabled);
      assert.equal(snapshot.message, "Droid is disabled in T3 Code settings.");
      assertM2Presentation(snapshot);
    }),
  );

  it.effect("probes `droid --version`, discovers models, and reports a ready provider", () =>
    Effect.gen(function* () {
      const droid = yield* makeScriptedDroid({
        kind: "result",
        result: listed(listedModel({ id: "claude-opus-5-5" })),
      });
      const snapshot = yield* provide(
        checkDroidProviderStatus(settings({ binaryPath: "/opt/bin/droid" }), {
          T3_TEST_VARIABLE: "kept",
        }),
        droid.spawner,
      );
      assert.equal(snapshot.status, "ready");
      assert.isTrue(snapshot.installed);
      assert.equal(snapshot.version, "0.244.0");
      assert.equal(snapshot.auth.status, "unknown");
      assert.deepEqual(
        snapshot.models.map((model) => model.slug),
        ["claude-opus-5-5"],
      );
      assert.lengthOf(droid.launches, 2);
      assert.equal(droid.launches[0]!.command, "/opt/bin/droid");
      assert.deepEqual(droid.launches[0]!.args, ["--version"]);
      assert.equal(droid.launches[0]!.options.env?.T3_TEST_VARIABLE, "kept");
      assert.equal(droid.launches[1]!.options.env?.T3_TEST_VARIABLE, "kept");
    }),
  );

  it.effect("[VAL-STATUS-009] carries the M2 presentation on a ready and a degraded snapshot", () =>
    Effect.gen(function* () {
      const ready = yield* makeScriptedDroid({ kind: "result", result: listed(listedModel()) });
      const degraded = yield* makeScriptedDroid({ kind: "error" });
      assertM2Presentation(yield* provide(checkDroidProviderStatus(settings()), ready.spawner));
      const snapshot = yield* provide(checkDroidProviderStatus(settings()), degraded.spawner);
      assert.equal(snapshot.status, "warning");
      assertM2Presentation(snapshot);
    }),
  );

  it.effect("probes the default binary name when binaryPath is empty", () =>
    Effect.gen(function* () {
      const droid = yield* makeScriptedDroid(
        { kind: "result", result: listed(listedModel()) },
        { stdout: "droid 0.244.0\n" },
      );
      const snapshot = yield* provide(
        checkDroidProviderStatus(settings({ binaryPath: "" })),
        droid.spawner,
      );
      assert.equal(snapshot.version, "0.244.0");
      assert.equal(droid.launches[0]!.command, "droid");
      assert.equal(droid.launches[1]!.command, "droid");
    }),
  );

  it.effect("[VAL-STATUS-004] appends custom models next to the discovered ones", () =>
    Effect.gen(function* () {
      const customModels: ReadonlyArray<CustomModelSetting> = [
        "claude-opus-5-5",
        "custom:my-model",
        { slug: "custom:named", name: "Named model" },
      ];
      const droid = yield* makeScriptedDroid({
        kind: "result",
        result: listed(
          listedModel({ id: "claude-opus-5-5" }),
          listedModel({ id: "gpt-6-sol", modelProvider: "openai", displayName: "GPT-6 Sol" }),
        ),
      });
      const ready = yield* provide(
        checkDroidProviderStatus(settings({ customModels })),
        droid.spawner,
      );
      assert.deepEqual(
        ready.models.map((model) => [model.slug, model.name, model.isCustom]),
        [
          ["claude-opus-5-5", "Opus 5.5 (1.6x)", false],
          ["gpt-6-sol", "GPT-6 Sol (1.6x)", false],
          ["custom:my-model", "custom:my-model", true],
          ["custom:named", "Named model", true],
        ],
      );
      const pending = yield* buildInitialDroidProviderSnapshot(settings({ customModels }));
      assert.deepEqual(
        pending.models.map((model) => [model.slug, model.isCustom]),
        [
          ["claude-opus-5-5", true],
          ["custom:my-model", true],
          ["custom:named", true],
        ],
      );
    }),
  );

  it.effect("[VAL-STATUS-005] reports a missing binary as not installed and skips discovery", () =>
    Effect.gen(function* () {
      const launches: Array<ChildProcess.StandardCommand> = [];
      const spawner = ChildProcessSpawner.make((command) => {
        launches.push(standardCommand(command));
        return Effect.fail(
          PlatformError.systemError({
            _tag: "NotFound",
            module: "ChildProcess",
            method: "spawn",
            description: "spawn droid ENOENT",
          }),
        );
      });
      const snapshot = yield* provide(
        checkDroidProviderStatus(settings({ customModels: ["custom:kept"] })),
        spawner,
      );
      assert.equal(snapshot.status, "error");
      assert.isFalse(snapshot.installed);
      assert.isNull(snapshot.version);
      assert.include(snapshot.message ?? "", "Droid CLI (`droid`) is not installed");
      assert.deepEqual(
        snapshot.models.map((model) => model.slug),
        ["custom:kept"],
      );
      assert.lengthOf(launches, 1);
    }),
  );

  it.effect("reports another spawn failure as installed but not runnable", () =>
    Effect.gen(function* () {
      const spawner = ChildProcessSpawner.make(() =>
        Effect.fail(
          PlatformError.systemError({
            _tag: "PermissionDenied",
            module: "ChildProcess",
            method: "spawn",
            description: "spawn droid EACCES",
          }),
        ),
      );
      const snapshot = yield* provide(checkDroidProviderStatus(settings()), spawner);
      assert.equal(snapshot.status, "error");
      assert.isTrue(snapshot.installed);
      assert.include(snapshot.message ?? "", "Failed to execute Droid CLI health check");
    }),
  );

  it.effect("[VAL-STATUS-005] reports a failing `--version` as installed and skips discovery", () =>
    Effect.gen(function* () {
      const { spawner, launches } = recordingSpawner(() =>
        processHandle({ stderr: "boom\n", exitCode: 1 }),
      );
      const snapshot = yield* provide(checkDroidProviderStatus(settings()), spawner);
      assert.equal(snapshot.status, "error");
      assert.isTrue(snapshot.installed);
      assert.include(snapshot.message ?? "", "installed but failed to run");
      assert.lengthOf(launches, 1);
      assert.deepEqual(launches[0]!.args, ["--version"]);
    }),
  );

  it.effect("reports a version it cannot parse as an error", () =>
    Effect.gen(function* () {
      const { spawner, launches } = recordingSpawner(() =>
        processHandle({ stdout: "droid dev build\n" }),
      );
      const snapshot = yield* provide(checkDroidProviderStatus(settings()), spawner);
      assert.equal(snapshot.status, "error");
      assert.isNull(snapshot.version);
      assert.include(snapshot.message ?? "", "could not determine the Droid version");
      assert.lengthOf(launches, 1);
    }),
  );

  it.effect("times out a version probe that never exits", () =>
    Effect.gen(function* () {
      const spawned = yield* Deferred.make<void>();
      const spawner = ChildProcessSpawner.make(() =>
        Deferred.succeed(spawned, undefined).pipe(Effect.as(processHandle({ hangs: true }))),
      );
      const probe = yield* provide(checkDroidProviderStatus(settings()), spawner).pipe(
        Effect.forkChild,
      );
      yield* Deferred.await(spawned);
      yield* TestClock.adjust("4 seconds");
      const snapshot = yield* Fiber.join(probe);
      assert.equal(snapshot.status, "error");
      assert.isTrue(snapshot.installed);
      assert.include(snapshot.message ?? "", "timed out while running `droid --version`");
    }),
  );
});

describe("Droid model discovery", () => {
  it.effect(
    "[VAL-STATUS-001][VAL-REG-017] lists models from a neutral temp cwd and sends only list_models",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const real = yield* ChildProcessSpawner.ChildProcessSpawner;
        const agent = yield* makeRpcMockAgent();
        const tempRoot = path.dirname(yield* fs.makeTempDirectoryScoped());
        const snapshot = yield* checkDroidProviderStatus(
          settings({ binaryPath: agent.binaryPath }),
          {},
        ).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, withVersionProbe(real)),
        );
        const log = yield* agent.readLog;

        const args = (log.find((entry) => entry.type === "argv")?.args ?? []) as Array<string>;
        assert.deepEqual(args.slice(0, 5), [
          "exec",
          "--input-format",
          "stream-jsonrpc",
          "--output-format",
          "stream-jsonrpc",
        ]);
        const cwd = args[args.indexOf("--cwd") + 1] ?? assert.fail("no --cwd");
        const settingsPath = args[args.indexOf("--settings") + 1] ?? assert.fail("no --settings");
        assert.equal(
          path.dirname(cwd),
          tempRoot,
          "the cwd is a fresh directory in the OS temp dir",
        );
        assert.notEqual(cwd, agent.projectDir);
        assert.isTrue(path.relative(agent.projectDir, cwd).startsWith(".."));
        const settingsEntry = asRecord(log.find((entry) => entry.type === "settings_file"));
        assert.equal(settingsEntry.path, settingsPath);
        assert.equal(settingsEntry.content, DROID_HOOKS_OFF_SETTINGS);

        assert.deepEqual(receivedMethods(log), ["droid.list_models"]);
        assert.isDefined(
          log.find((entry) => entry.type === "exit"),
          "the discovery child is gone when the probe returns",
        );
        assert.isFalse(yield* fs.exists(cwd), "the temp cwd is removed afterwards");
        assert.isFalse(yield* fs.exists(settingsPath), "the hooks-off settings file is removed");

        assert.equal(snapshot.status, "ready");
        assert.deepEqual(
          snapshot.models.map((model) => [model.slug, model.name, model.subProvider]),
          [
            ["auto", "Auto Model", undefined],
            ["claude-opus-5-5", "Opus 5.5 (1.6x)", "Anthropic"],
            ["gpt-6-sol", "GPT-6 Sol (0.8x)", "OpenAI"],
            ["deepseek-v4.1-flash", "DeepSeek V4.1 Flash (0.12x)", "Droid Core"],
            ["glm-5.3", "GLM-5.3 (0.56x)", "Droid Core"],
          ],
        );
        const opus = snapshot.models.find((model) => model.slug === "claude-opus-5-5");
        const effort = opus?.capabilities?.optionDescriptors?.[0];
        assert.equal(effort?.id, "reasoningEffort");
        assert.equal(effort?.type === "select" ? effort.currentValue : undefined, "medium");
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), live),
  );

  it.effect("[VAL-STATUS-001] never opens a session or sends a message on repeated probes", () =>
    Effect.gen(function* () {
      const real = yield* ChildProcessSpawner.ChildProcessSpawner;
      const agent = yield* makeRpcMockAgent();
      const probe = checkDroidProviderStatus(settings({ binaryPath: agent.binaryPath }), {}).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, withVersionProbe(real)),
      );
      yield* probe;
      yield* probe;
      const methods = receivedMethods(yield* agent.readLog);
      assert.deepEqual(methods, ["droid.list_models", "droid.list_models"]);
      for (const forbidden of [
        "droid.initialize_session",
        "droid.load_session",
        "droid.add_user_message",
      ]) {
        assert.notInclude(methods, forbidden);
      }
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), live),
  );

  it.effect("[VAL-STATUS-005] a list_models error degrades to a warning with the custom list", () =>
    Effect.gen(function* () {
      const real = yield* ChildProcessSpawner.ChildProcessSpawner;
      const agent = yield* makeRpcMockAgent({ DROID_MOCK_LIST_MODELS: "error" });
      const snapshot = yield* checkDroidProviderStatus(
        settings({ binaryPath: agent.binaryPath, customModels: ["custom:kept"] }),
        {},
      ).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, withVersionProbe(real)),
      );
      assert.equal(snapshot.status, "warning");
      assert.isTrue(snapshot.installed);
      assert.equal(snapshot.version, "0.244.0");
      assert.include(snapshot.message ?? "", "model discovery failed or timed out");
      assert.deepEqual(
        snapshot.models.map((model) => model.slug),
        ["custom:kept"],
      );
      assertM2Presentation(snapshot);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), live),
  );

  it.effect("[VAL-STATUS-005] malformed list_models data degrades to a warning", () =>
    Effect.gen(function* () {
      const real = yield* ChildProcessSpawner.ChildProcessSpawner;
      const agent = yield* makeRpcMockAgent({ DROID_MOCK_LIST_MODELS: "malformed" });
      const snapshot = yield* checkDroidProviderStatus(
        settings({ binaryPath: agent.binaryPath, customModels: ["custom:kept"] }),
        {},
      ).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, withVersionProbe(real)),
      );
      assert.equal(snapshot.status, "warning");
      assert.deepEqual(
        snapshot.models.map((model) => model.slug),
        ["custom:kept"],
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), live),
  );

  it.effect("[VAL-STATUS-005] a result that maps to an empty list degrades to a warning", () =>
    Effect.gen(function* () {
      for (const result of [
        { models: [] },
        listed(listedModel({ deprecated: true }), listedModel({ id: "off", disabled: true })),
      ]) {
        const droid = yield* makeScriptedDroid({ kind: "result", result });
        const snapshot = yield* provide(
          checkDroidProviderStatus(settings({ customModels: ["custom:kept"] })),
          droid.spawner,
        );
        assert.equal(snapshot.status, "warning");
        assert.deepEqual(
          snapshot.models.map((model) => model.slug),
          ["custom:kept"],
        );
      }
    }),
  );

  it.effect("[VAL-STATUS-005] a discovery process that exits without replying degrades", () =>
    Effect.gen(function* () {
      const droid = yield* makeScriptedDroid({ kind: "exits" });
      const snapshot = yield* provide(checkDroidProviderStatus(settings()), droid.spawner);
      assert.equal(snapshot.status, "warning");
      assert.deepEqual(snapshot.models, []);
    }),
  );

  it.effect(
    "[VAL-STATUS-005] a discovery that never replies times out, warns, and kills the child",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const droid = yield* makeScriptedDroid({ kind: "silent" });
        const kills: Array<{ readonly pid: number; readonly signal: string | number | undefined }> =
          [];
        let terminated = false;
        yield* Effect.acquireRelease(
          Effect.sync(() =>
            vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
              kills.push({ pid, signal });
              if (signal === "SIGTERM") {
                terminated = true;
                droid.markExited(143);
              } else if (signal === 0 && terminated) {
                throw Object.assign(new Error("kill ESRCH"), { code: "ESRCH" });
              }
              return true;
            }),
          ),
          (spy) => Effect.sync(() => spy.mockRestore()),
        );

        const probe = yield* provide(
          checkDroidProviderStatus(settings({ customModels: ["custom:kept"] })),
          droid.spawner,
        ).pipe(Effect.forkChild);
        yield* Deferred.await(droid.requested);
        yield* yieldSome;

        yield* TestClock.adjust(Duration.millis(DROID_DISCOVERY_TIMEOUT_MS - 1));
        yield* yieldSome;
        assert.isUndefined(probe.pollUnsafe(), "the probe waits for the whole discovery timeout");
        assert.deepEqual(kills, []);

        yield* TestClock.adjust(Duration.millis(1));
        const snapshot = yield* Fiber.join(probe);

        assert.equal(snapshot.status, "warning");
        assert.isTrue(snapshot.installed);
        assert.deepEqual(
          snapshot.models.map((model) => model.slug),
          ["custom:kept"],
        );
        assert.include(snapshot.message ?? "", "model discovery failed or timed out");
        assert.isTrue(
          kills.some((call) => call.pid === -FAKE_PID && call.signal === "SIGTERM"),
          "the timed-out discovery child's process group is signalled",
        );
        assert.isFalse(kills.some((call) => call.signal === "SIGKILL"));
        const cwd = execSpawns(droid.launches)[0]?.options.cwd ?? assert.fail("no discovery spawn");
        assert.isFalse(yield* fs.exists(cwd), "the temp cwd is removed after a timeout");
        assert.deepEqual(
          droid.received.map((line) => line.method),
          ["droid.list_models"],
        );
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("Droid auth hint in the probe", () => {
  const probeWithHome = (environment: NodeJS.ProcessEnv) =>
    Effect.gen(function* () {
      const droid = yield* makeScriptedDroid({ kind: "result", result: listed(listedModel()) });
      return yield* provide(checkDroidProviderStatus(settings(), environment), droid.spawner);
    });

  it.effect("[VAL-STATUS-006] only changes the message, and auth stays unknown", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const home = yield* fs.makeTempDirectoryScoped({ prefix: "t3-droid-home-" });
      yield* fs.makeDirectory(path.join(home, ".factory"));
      const authFile = path.join(home, ".factory", "auth.json");
      yield* fs.writeFileString(authFile, NOT_A_CREDENTIAL);

      const withAuthFile = yield* probeWithHome({ HOME: home });
      assert.include(withAuthFile.message ?? "", "Droid credentials found under ~/.factory.");
      assert.equal(withAuthFile.auth.status, "unknown");
      assert.notInclude(JSON.stringify(withAuthFile), NOT_A_CREDENTIAL);
      assert.notInclude(JSON.stringify(withAuthFile), authFile);

      const withKey = yield* probeWithHome({ HOME: home, FACTORY_API_KEY: FAKE_API_KEY });
      assert.include(withKey.message ?? "", "FACTORY_API_KEY is set.");
      assert.equal(withKey.auth.status, "unknown");
      assert.notInclude(JSON.stringify(withKey), FAKE_API_KEY);

      const bare = yield* fs.makeTempDirectoryScoped({ prefix: "t3-droid-bare-home-" });
      const withNothing = yield* probeWithHome({ HOME: bare });
      assert.include(withNothing.message ?? "", "Set FACTORY_API_KEY or run `droid` once");
      assert.equal(withNothing.auth.status, "unknown");
      assert.equal(withNothing.status, "ready");

      const viaUserProfile = yield* probeWithHome({ HOME: " ", USERPROFILE: home });
      assert.include(viaUserProfile.message ?? "", "Droid credentials found under ~/.factory.");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("[VAL-STATUS-006] lists ~/.factory without opening anything in it", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const home = yield* fs.makeTempDirectoryScoped({ prefix: "t3-droid-home-" });
      yield* fs.makeDirectory(path.join(home, ".factory"));
      yield* fs.writeFileString(path.join(home, ".factory", "auth.json"), NOT_A_CREDENTIAL);
      const factoryDir = path.join(home, ".factory");

      const touched: Array<string> = [];
      const listings: Array<string> = [];
      const underFactory = (target: string) => target.startsWith(factoryDir);
      const watching = FileSystem.makeNoop({
        ...fs,
        readDirectory: (target, options) => {
          listings.push(target);
          return fs.readDirectory(target, options);
        },
        readFile: (target) => {
          if (underFactory(target)) touched.push(`readFile ${target}`);
          return fs.readFile(target);
        },
        readFileString: (target, encoding) => {
          if (underFactory(target)) touched.push(`readFileString ${target}`);
          return fs.readFileString(target, encoding);
        },
        open: (target, options) => {
          if (underFactory(target)) touched.push(`open ${target}`);
          return fs.open(target, options);
        },
        stream: (target, options) => {
          if (underFactory(target)) touched.push(`stream ${target}`);
          return fs.stream(target, options);
        },
        copy: (from, to, options) => {
          if (underFactory(from)) touched.push(`copy ${from}`);
          return fs.copy(from, to, options);
        },
        copyFile: (from, to) => {
          if (underFactory(from)) touched.push(`copyFile ${from}`);
          return fs.copyFile(from, to);
        },
      });
      const droid = yield* makeScriptedDroid({ kind: "result", result: listed(listedModel()) });
      const snapshot = yield* checkDroidProviderStatus(settings(), { HOME: home }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, droid.spawner),
        Effect.provideService(FileSystem.FileSystem, watching),
      );
      assert.include(snapshot.message ?? "", "Droid credentials found under ~/.factory.");
      assert.deepEqual(touched, []);
      assert.include(listings, factoryDir);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("FACTORY_API_KEY in discovery", () => {
  it.effect(
    "[VAL-RPC-014] reaches the discovery spawn and never appears in the snapshot, message, or logs",
    () => {
      const logs = captureLogs();
      return Effect.gen(function* () {
        const real = yield* ChildProcessSpawner.ChildProcessSpawner;
        const probe = (agentEnv: Record<string, string>) =>
          Effect.gen(function* () {
            const agent = yield* makeRpcMockAgent(agentEnv);
            const snapshot = yield* checkDroidProviderStatus(
              settings({ binaryPath: agent.binaryPath, customModels: ["custom:kept"] }),
              { FACTORY_API_KEY: FAKE_API_KEY },
            ).pipe(
              Effect.provideService(
                ChildProcessSpawner.ChildProcessSpawner,
                withVersionProbe(real),
              ),
            );
            return { snapshot, log: yield* agent.readLog };
          });

        const ready = yield* probe({});
        const failed = yield* probe({ DROID_MOCK_LIST_MODELS: "error" });

        for (const { snapshot, log } of [ready, failed]) {
          const envEntry = asRecord(log.find((entry) => entry.type === "env"));
          assert.isTrue(envEntry.factoryApiKeySet, "the discovery child inherits the key");
          assert.include(envEntry.factoryEnvNames as ReadonlyArray<string>, "FACTORY_API_KEY");
          assert.notInclude(JSON.stringify(snapshot), FAKE_API_KEY);
          assert.notInclude(snapshot.message ?? "", FAKE_API_KEY);
          assert.notInclude(JSON.stringify(log), FAKE_API_KEY);
        }
        assert.equal(ready.snapshot.status, "ready");
        assert.equal(failed.snapshot.status, "warning");
        assert.isTrue(
          logs.entries.some((entry) => entry.text.includes("model discovery failed")),
          "the failed discovery is logged, so the log capture is live",
        );
        for (const entry of logs.entries) {
          assert.notInclude(entry.text, FAKE_API_KEY);
        }
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), live, logs.provideLogs);
    },
  );

  it.effect("[VAL-RPC-014] discovery inherits the key and keeps it out of the probe's output", () =>
    Effect.gen(function* () {
      const droid = yield* makeScriptedDroid({ kind: "result", result: listed(listedModel()) });
      const snapshot = yield* provide(
        checkDroidProviderStatus(settings(), { FACTORY_API_KEY: FAKE_API_KEY }),
        droid.spawner,
      );
      for (const launch of droid.launches) {
        assert.equal(launch.options.env?.FACTORY_API_KEY, FAKE_API_KEY);
        assert.notInclude(launch.args.join(" "), FAKE_API_KEY);
      }
      assert.lengthOf(droid.launches, 2);
      assert.notInclude(JSON.stringify(snapshot), FAKE_API_KEY);
    }),
  );
});
