import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/http/HttpClient";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { fallbackTextGenerationModelSelection } from "@t3tools/shared/serverSettings";

import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import type { DroidSettings } from "../settings.ts";
import { DroidDriver } from "./driver.ts";
import { DROID_HOOKS_OFF_SETTINGS } from "./hooksOffSettings.ts";
import { DROID_PRESENTATION } from "./status.ts";

const layerTest = Layer.mergeAll(
  TestProviderHost.layer({
    cwd: "/machine",
    settings: { ...DEFAULT_SERVER_SETTINGS, enableProviderUpdateChecks: false },
    runBackgroundWork: false,
  }),
  ProviderLatestVersions.layer,
  Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make(() => Effect.die("Unexpected HTTP")),
  ),
).pipe(Layer.provideMerge(NodeServices.layer));

const encoder = new TextEncoder();

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const makeSpawnedHandle = (stdout: Uint8Array) =>
  ChildProcessSpawner.makeHandle({
    // Outside the valid PID range, so cleanup cannot signal a real process.
    pid: ChildProcessSpawner.ProcessId(999_999_999),
    exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.drain,
    stdout: Stream.succeed(stdout),
    stderr: Stream.empty,
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });

const makeDroidSpawner = (version = "0.244.0") => {
  const launches: Array<ChildProcess.StandardCommand> = [];
  const spawner = ChildProcessSpawner.make((command) => {
    if (!ChildProcess.isStandardCommand(command)) return Effect.die("Unexpected pipeline");
    launches.push(command);
    return Effect.succeed(makeSpawnedHandle(encoder.encode(`${version}\n`)));
  });
  return { spawner, launches };
};

const create = (config: Partial<DroidSettings> = {}, enabled = true) =>
  DroidDriver.create({
    instanceId: ProviderInstanceId.make("droid-test"),
    displayName: "My Droid",
    accentColor: "#abcdef",
    environment: [{ name: "T3_DROID_TEST", value: "instance-value", sensitive: false }],
    enabled,
    config: { ...DroidDriver.defaultConfig(), binaryPath: "custom-droid", ...config },
  });

it.layer(layerTest)("DroidDriver", (it) => {
  it.effect("[VAL-REG-002] identifies as the droid driver named Droid", () =>
    Effect.sync(() => {
      assert.equal(DroidDriver.driverKind, ProviderDriverKind.make("droid"));
      assert.equal(DroidDriver.metadata.displayName, "Droid");
      assert.equal(DroidDriver.metadata.displayName, DROID_PRESENTATION.displayName);
      assert.isTrue(DroidDriver.metadata.supportsMultipleInstances);
    }),
  );

  it.effect("[VAL-REG-005] defaults to an empty, disabled config on the droid binary", () =>
    Effect.sync(() => {
      assert.deepEqual(DroidDriver.defaultConfig(), {
        enabled: false,
        binaryPath: "droid",
        customModels: [],
      });
    }),
  );

  it.effect("stamps the instance identity on a probed snapshot and lists custom models", () =>
    Effect.gen(function* () {
      const { spawner, launches } = makeDroidSpawner();
      const instance = yield* create({ customModels: ["custom:my-model"] }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );
      yield* instance.snapshot.refresh;
      const snapshot = yield* instance.snapshot.getSnapshot;
      assert.equal(instance.driverKind, "droid");
      assert.equal(snapshot.driver, "droid");
      assert.equal(snapshot.instanceId, "droid-test");
      assert.equal(snapshot.displayName, "My Droid");
      assert.equal(snapshot.accentColor, "#abcdef");
      assert.equal(snapshot.status, "ready");
      assert.equal(snapshot.version, "0.244.0");
      assert.equal(snapshot.continuation?.groupKey, instance.continuationIdentity.continuationKey);
      assert.deepEqual(
        snapshot.models.map((model) => model.slug),
        ["custom:my-model"],
      );
      const probe = launches.find((launch) => launch.args.includes("--version"));
      assert.isDefined(probe);
      assert.equal(probe!.command, "custom-droid");
      assert.equal(probe!.options.env?.T3_DROID_TEST, "instance-value");
    }).pipe(Effect.scoped),
  );

  it.effect("[VAL-REG-004] does not spawn Droid for a disabled instance", () =>
    Effect.gen(function* () {
      const instance = yield* create({}, false).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() => Effect.die("Disabled Droid must not spawn")),
        ),
      );
      yield* instance.snapshot.refresh;
      const snapshot = yield* instance.snapshot.getSnapshot;
      assert.isFalse(instance.enabled);
      assert.isFalse(snapshot.enabled);
      assert.equal(snapshot.status, "disabled");
    }).pipe(Effect.scoped),
  );

  it.effect("keeps Droid updates manual because the CLI updates itself", () =>
    Effect.gen(function* () {
      const { spawner } = makeDroidSpawner();
      const instance = yield* create().pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );
      yield* instance.snapshot.refresh;
      const maintenance = yield* instance.snapshot.resolveMaintenance();
      assert.equal(maintenance.provider, "droid");
      assert.isNull(maintenance.update);
      assert.isNull(maintenance.packageName);
    }).pipe(Effect.scoped),
  );

  it.effect("claims no orchestration capability before the adapter lands", () =>
    Effect.gen(function* () {
      const { spawner } = makeDroidSpawner();
      const instance = yield* create().pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );
      const adapter = instance.orchestrationAdapter;
      assert.equal(adapter.driver, "droid");
      assert.equal(adapter.instanceId, "droid-test");
      const capabilities = yield* adapter.getCapabilities();
      const claimed = Object.entries(capabilities).flatMap(([group, flags]) =>
        Object.entries(flags as Record<string, unknown>).flatMap(([name, value]) =>
          value === true || value === "strong" || value === "weak" ? [`${group}.${name}`] : [],
        ),
      );
      assert.deepEqual(claimed, []);
      const error = yield* Effect.flip(
        adapter.openSession({
          threadId: ThreadId.make("thread-1"),
          providerSessionId: ProviderSessionId.make("session-1"),
          modelSelection: { instanceId: ProviderInstanceId.make("droid-test"), model: "m" },
          runtimePolicy: { enforcement: "client-boundary" } as never,
        }),
      );
      assert.equal(error._tag, "ProviderAdapterOpenSessionError");
    }).pipe(Effect.scoped),
  );

  it.effect(
    "[VAL-CROSS-007] runs each text generation call as a separate read-only one-shot droid exec for the saved instance",
    () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const saved = yield* Schema.decodeEffect(DroidDriver.configSchema)({
          enabled: true,
          binaryPath: "/opt/droid/bin/droid",
        });
        const launches: Array<{
          readonly command: ChildProcess.StandardCommand;
          readonly settings: string;
        }> = [];
        const spawner = ChildProcessSpawner.make((command) => {
          if (!ChildProcess.isStandardCommand(command)) return Effect.die("Unexpected pipeline");
          const settingsPath = command.args[command.args.indexOf("--settings") + 1] ?? "";
          return fileSystem.readFileString(settingsPath).pipe(
            Effect.orElseSucceed(() => ""),
            Effect.map((settings) => {
              launches.push({ command, settings });
              return makeSpawnedHandle(
                encoder.encode(
                  `${encodeJson({
                    type: "result",
                    is_error: false,
                    result: encodeJson({ title: "Fix login", branch: "fix-login" }),
                  })}\n`,
                ),
              );
            }),
          );
        });
        const instance = yield* DroidDriver.create({
          instanceId: ProviderInstanceId.make("droid-test"),
          displayName: "My Droid",
          accentColor: undefined,
          environment: [{ name: "T3_DROID_TEST", value: "instance-value", sensitive: false }],
          enabled: true,
          config: saved,
        }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
        const modelSelection = fallbackTextGenerationModelSelection(
          {},
          (_instanceId, driver) => driver === "droid",
        );
        assert.isDefined(modelSelection);

        assert.sameMembers(Object.keys(instance.textGeneration), [
          "generateCommitMessage",
          "generatePrContent",
          "generateBranchName",
          "generateThreadTitle",
          "generateReviewGuide",
        ]);
        yield* instance.textGeneration.generateThreadTitle({
          cwd: "/machine/project",
          message: "Fix the login redirect",
          modelSelection: modelSelection!,
        });
        yield* instance.textGeneration.generateBranchName({
          cwd: "/machine/project",
          message: "Fix the login redirect",
          modelSelection: modelSelection!,
        });
        const review = yield* Effect.flip(
          instance.textGeneration.generateReviewGuide({
            cwd: "/machine/project",
            number: 1,
            title: "Fix login",
            description: "",
            baseBranch: "main",
            baseCommit: "0123456789abcdef0123456789abcdef01234567",
            settledReview: "",
            modelSelection: modelSelection!,
          }),
        );
        assert.equal(review._tag, "TextGenerationError");

        const execs = launches.filter(({ command }) => command.args[0] === "exec");
        assert.lengthOf(execs, 2, "one separate spawn per call; the review guide never spawns");
        assert.notEqual(
          execs[0]?.command.options.cwd,
          execs[1]?.command.options.cwd,
          "each call gets its own temporary directory",
        );
        for (const { command, settings } of execs) {
          assert.equal(command.command, "/opt/droid/bin/droid");
          assert.equal(command.args[command.args.indexOf("-m") + 1], "claude-haiku-5-5");
          assert.include(command.args, "--only-tools");
          const workingDirectory = command.args[command.args.indexOf("--cwd") + 1];
          assert.equal(command.options.cwd, workingDirectory);
          assert.isTrue(path.relative("/machine/project", workingDirectory ?? "").startsWith(".."));
          assert.notInclude(command.args, "/machine/project");
          assert.equal(settings, DROID_HOOKS_OFF_SETTINGS);
          assert.equal(command.options.env?.T3_DROID_TEST, "instance-value");
        }
        // Nothing here talks to a thread's stream-jsonrpc process: no launch opens one, and
        // every one-shot child gets a closed stdin.
        for (const { command } of launches) {
          assert.notInclude(command.args, "stream-jsonrpc");
          assert.notInclude(command.args, "--input-format");
        }
        for (const { command } of execs) {
          assert.equal(command.options.stdin, "ignore");
        }
      }).pipe(Effect.scoped),
  );
});
