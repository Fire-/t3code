import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import type { CustomModelSetting } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as PlatformError from "effect/PlatformError";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import type { DroidSettings } from "../settings.ts";
import {
  buildInitialDroidProviderSnapshot,
  checkDroidProviderStatus,
  DROID_PRESENTATION,
} from "./status.ts";

const encoder = new TextEncoder();

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
    // Outside the valid PID range, so cleanup cannot signal a real process.
    pid: ChildProcessSpawner.ProcessId(999_999_999),
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
    if (!ChildProcess.isStandardCommand(command)) return Effect.die("Unexpected pipeline");
    launches.push(command);
    return Effect.succeed(respond(command));
  });
  return { spawner, launches };
}

const settings = (overrides: Partial<DroidSettings> = {}): DroidSettings => ({
  enabled: true,
  binaryPath: "droid",
  customModels: [],
  ...overrides,
});

const provide = <A, E>(
  effect: Effect.Effect<A, E, ChildProcessSpawner.ChildProcessSpawner>,
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
) =>
  effect.pipe(
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    Effect.provide(NodeServices.layer),
  );

describe("DROID_PRESENTATION", () => {
  it("[VAL-REG-002][VAL-STATUS-009] names Droid and lists only what exists at M1", () => {
    assert.equal(DROID_PRESENTATION.displayName, "Droid");
    assert.deepEqual(DROID_PRESENTATION.supportedRuntimeModes, [
      "approval-required",
      "auto-accept-edits",
      "auto",
      "full-access",
    ]);
    assert.isFalse(DROID_PRESENTATION.requiresNewThreadForModelChange);
    assert.isFalse(DROID_PRESENTATION.showInteractionModeToggle);
    assert.isFalse(DROID_PRESENTATION.supportsConversationRollback);
    assert.isFalse(DROID_PRESENTATION.reportsContextWindow);
  });
});

describe("buildInitialDroidProviderSnapshot", () => {
  it.effect("reports a disabled provider before any probe", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialDroidProviderSnapshot(settings({ enabled: false }));
      assert.equal(snapshot.status, "disabled");
      assert.isFalse(snapshot.enabled);
      assert.equal(snapshot.displayName, "Droid");
      assert.equal(snapshot.message, "Droid is disabled in T3 Code settings.");
    }),
  );

  it.effect("reports a pending probe for an enabled provider and carries the presentation", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialDroidProviderSnapshot(settings());
      assert.equal(snapshot.status, "warning");
      assert.equal(snapshot.message, "Checking Droid CLI availability...");
      assert.equal(snapshot.auth.status, "unknown");
      assert.deepEqual(snapshot.supportedRuntimeModes, DROID_PRESENTATION.supportedRuntimeModes);
      assert.equal(snapshot.requiresNewThreadForModelChange, false);
      assert.equal(snapshot.showInteractionModeToggle, false);
      assert.equal(snapshot.supportsConversationRollback, false);
    }),
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
    }),
  );

  it.effect("probes `droid --version` and reports a ready provider with unknown auth", () =>
    Effect.gen(function* () {
      const { spawner, launches } = recordingSpawner(() => processHandle({ stdout: "0.244.0\n" }));
      const snapshot = yield* provide(
        checkDroidProviderStatus(settings({ binaryPath: "/opt/bin/droid" }), {
          T3_TEST_VARIABLE: "kept",
        }),
        spawner,
      );
      assert.equal(snapshot.status, "ready");
      assert.isTrue(snapshot.installed);
      assert.equal(snapshot.version, "0.244.0");
      assert.equal(snapshot.auth.status, "unknown");
      assert.equal(snapshot.displayName, "Droid");
      assert.lengthOf(launches, 1);
      assert.equal(launches[0]!.command, "/opt/bin/droid");
      assert.deepEqual(launches[0]!.args, ["--version"]);
      assert.equal(launches[0]!.options.env?.T3_TEST_VARIABLE, "kept");
    }),
  );

  it.effect("probes the default binary name when binaryPath is empty", () =>
    Effect.gen(function* () {
      const { spawner, launches } = recordingSpawner(() =>
        processHandle({ stdout: "droid 0.244.0\n" }),
      );
      const snapshot = yield* provide(
        checkDroidProviderStatus(settings({ binaryPath: "" })),
        spawner,
      );
      assert.equal(snapshot.version, "0.244.0");
      assert.equal(launches[0]!.command, "droid");
    }),
  );

  it.effect("lists custom models only, with no built-in or discovered models", () =>
    Effect.gen(function* () {
      const customModels: ReadonlyArray<CustomModelSetting> = [
        "custom:my-model",
        { slug: "custom:named", name: "Named model" },
      ];
      const { spawner } = recordingSpawner(() => processHandle({ stdout: "0.244.0\n" }));
      const ready = yield* provide(checkDroidProviderStatus(settings({ customModels })), spawner);
      const pending = yield* buildInitialDroidProviderSnapshot(settings({ customModels }));
      for (const snapshot of [ready, pending]) {
        assert.deepEqual(
          snapshot.models.map((model) => [model.slug, model.name, model.isCustom]),
          [
            ["custom:my-model", "custom:my-model", true],
            ["custom:named", "Named model", true],
          ],
        );
      }
      const noCustom = yield* provide(checkDroidProviderStatus(settings()), spawner);
      assert.deepEqual(noCustom.models, []);
    }),
  );

  it.effect("reports a missing binary as not installed", () =>
    Effect.gen(function* () {
      const spawner = ChildProcessSpawner.make(() =>
        Effect.fail(
          PlatformError.systemError({
            _tag: "NotFound",
            module: "ChildProcess",
            method: "spawn",
            description: "spawn droid ENOENT",
          }),
        ),
      );
      const snapshot = yield* provide(checkDroidProviderStatus(settings()), spawner);
      assert.equal(snapshot.status, "error");
      assert.isFalse(snapshot.installed);
      assert.isNull(snapshot.version);
      assert.include(snapshot.message ?? "", "Droid CLI (`droid`) is not installed");
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

  it.effect("reports a non-zero exit as installed but failing", () =>
    Effect.gen(function* () {
      const { spawner } = recordingSpawner(() => processHandle({ stderr: "boom\n", exitCode: 1 }));
      const snapshot = yield* provide(checkDroidProviderStatus(settings()), spawner);
      assert.equal(snapshot.status, "error");
      assert.isTrue(snapshot.installed);
      assert.include(snapshot.message ?? "", "installed but failed to run");
    }),
  );

  it.effect("reports a version it cannot parse as an error", () =>
    Effect.gen(function* () {
      const { spawner } = recordingSpawner(() => processHandle({ stdout: "droid dev build\n" }));
      const snapshot = yield* provide(checkDroidProviderStatus(settings()), spawner);
      assert.equal(snapshot.status, "error");
      assert.isNull(snapshot.version);
      assert.include(snapshot.message ?? "", "could not determine the Droid version");
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

  it.effect("keeps the API key out of the snapshot while passing it to the child", () =>
    Effect.gen(function* () {
      const apiKey = "fk-secret-value-123";
      const { spawner, launches } = recordingSpawner(() => processHandle({ stdout: "0.244.0\n" }));
      const snapshot = yield* provide(
        checkDroidProviderStatus(settings(), { FACTORY_API_KEY: apiKey }),
        spawner,
      );
      assert.equal(launches[0]!.options.env?.FACTORY_API_KEY, apiKey);
      assert.notInclude(JSON.stringify(snapshot), apiKey);
    }),
  );
});
