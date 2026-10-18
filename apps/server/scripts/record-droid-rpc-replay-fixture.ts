import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import type { ProviderReplayEntry, ProviderReplayTranscript } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Console from "effect/Console";
import * as Crypto from "effect/Crypto";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import { TestClock } from "effect/testing";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import * as OS from "node:os";

import {
  DROID_RPC_REPLAY_PROTOCOL,
  DroidOrchestratorReplayHarness,
  makeDroidRecordingSpawner,
} from "../src/orchestration-v2/Adapters/DroidAdapterV2.testkit.ts";
import * as DroidAdapterV2Testkit from "../src/orchestration-v2/Adapters/DroidAdapterV2.testkit.ts";
import { DROID_PROVIDER } from "@t3tools/provider-droid/server";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { provideDeterministicTestRuntime } from "../src/orchestration-v2/testkit/DeterministicRuntime.ts";
import { ORCHESTRATOR_REPLAY_FIXTURES } from "../src/orchestration-v2/testkit/fixtures/index.ts";
import { materializeFixtureInput } from "../src/orchestration-v2/testkit/fixtures/shared.ts";
import { runOrchestratorV2ProviderReplayScenario } from "../src/orchestration-v2/testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import {
  buildLeakChecks,
  describeLeaks,
  findTranscriptLeaks,
  readGlobalInstructionTexts,
  REJECTED_TRANSCRIPT_DIRECTORY,
  writeRejectedTranscript,
} from "./droidReplayLeakCheck.ts";
import { DROID_PROBE_FIXTURES } from "./droidReplayProbes.ts";
import { collectHostIds, collectMachineIds, scrubDroidEntries } from "./droidReplayScrub.ts";

/**
 * Which fixtures may be recorded, and what each needs around the binary. The fixture
 * registration in `fixtures/index.ts` owns the prompts, the model and the runtime mode.
 *
 * - `awaitPreviousExit`: start a Droid process only after the previous one exited. A fixture
 *   sends its next message immediately after the idle release, so without this the second
 *   process could load a session the first is still closing.
 */
interface DroidRecordingScenario {
  readonly awaitPreviousExit?: boolean;
}

const DROID_RECORDING_SCENARIOS: Readonly<Record<string, DroidRecordingScenario>> = {
  simple: {},
  multi_turn: {},
  provider_thread_resume: { awaitPreviousExit: true },
  turn_interrupt_mid_tool: {},
  message_steering: {},
  plan_questions: {},
  proposed_plan: {},
  thread_rollback: {},
  thread_rollback_after_stop: { awaitPreviousExit: true },
  droid_compaction: {},
  thread_fork_native_prior_turn: {},
  // Probes (`droidReplayProbes.ts`) record to /tmp, not to a fixture file. The exit_spec_mode
  // ones restart the process between their two turns, and so does the compaction stop (a kill).
  exit_spec_mode_off: { awaitPreviousExit: true },
  exit_spec_mode_high: { awaitPreviousExit: true },
  thread_start_rewind: {},
  compaction_stop_kill: { awaitPreviousExit: true },
};

/** Model turns cost real credits, so only the model agreed for fixtures may record. */
const RECORDABLE_MODELS = new Set(["claude-haiku-5-5"]);

const RECORDER_NAME = "live-droid-recorder";
const DROID_BINARY = "droid";
const CLOCK_TICK = Duration.millis(20);
const PREVIOUS_EXIT_TIMEOUT = Duration.seconds(20);
/** Droid reads these files for the user's own settings; a recording must leave them untouched. */
const SETTINGS_FILES = ["settings.json", "settings.local.json"] as const;
/** Very short host names would match inside ordinary words. */
const MIN_HOST_NAME_CHARS = 4;

function readArgValue(name: string): string | undefined {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

const scenarioName = readArgValue("--scenario");
const recordableFixtures = [...ORCHESTRATOR_REPLAY_FIXTURES, ...DROID_PROBE_FIXTURES];
const recordableNames = recordableFixtures
  .filter(
    (entry) =>
      DROID_RECORDING_SCENARIOS[entry.name] !== undefined &&
      entry.providers.some((provider) => provider.driver === DROID_PROVIDER),
  )
  .map((entry) => entry.name);
const fixture = recordableFixtures.find((entry) => entry.name === scenarioName);
const variant = fixture?.providers.find((provider) => provider.driver === DROID_PROVIDER);
const scenario = scenarioName === undefined ? undefined : DROID_RECORDING_SCENARIOS[scenarioName];
if (fixture === undefined || variant === undefined || scenario === undefined) {
  throw new Error(
    `Pass --scenario with a fixture that registers Droid and has a recorder entry: ${recordableNames.join(", ")}`,
  );
}
if (!RECORDABLE_MODELS.has(variant.modelSelection.model)) {
  throw new Error(
    `Refusing to record ${fixture.name} with ${variant.modelSelection.model}; use one of ${[...RECORDABLE_MODELS].join(", ")}.`,
  );
}

const home = OS.homedir();
const userName = OS.userInfo().username;
const hostName = OS.hostname();
const hostNames = [hostName, hostName.split(".")[0] ?? ""].filter(
  (name) => name.length >= MIN_HOST_NAME_CHARS,
);
const systemTmpDir = OS.tmpdir().replace(/\/+$/u, "");

class DroidRecorderError extends Schema.TaggedError<DroidRecorderError>()("DroidRecorderError", {
  detail: Schema.String,
}) {
  override get message(): string {
    return this.detail;
  }
}

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

function encodeTranscriptNdjson(transcript: ProviderReplayTranscript): string {
  const { entries, ...metadata } = transcript;
  return [
    encodeJson({ type: "transcript_start", ...metadata }),
    ...entries.map((entry) => encodeJson(entry)),
    "",
  ].join("\n");
}

/**
 * Replay runs on a frozen TestClock; recording shares that runtime so fixture `advance_clock`
 * steps behave identically, but lets it follow wall time so a live Droid's timeouts and the
 * adapter's exit wait still elapse.
 */
const followWallClock = Effect.gen(function* () {
  while (true) {
    yield* TestClock.withLive(Effect.sleep(CLOCK_TICK));
    yield* TestClock.adjust(CLOCK_TICK);
  }
});

const readDroidVersion = Effect.gen(function* () {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  return (yield* spawner.string(ChildProcess.make(DROID_BINARY, ["--version"]))).trim();
});

/** Hashes only: the settings contents are never held past the digest or printed. */
const settingsHashes = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const hashes: Record<string, string> = {};
  for (const name of SETTINGS_FILES) {
    const bytes = yield* fs.readFile(path.join(home, ".factory", name)).pipe(Effect.option);
    if (bytes._tag === "Some") {
      const digest = yield* crypto.digest("SHA-256", bytes.value);
      hashes[name] = Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
    }
  }
  return hashes;
});

const describeHashes = (hashes: Readonly<Record<string, string>>): string =>
  Object.entries(hashes)
    .map(([name, hash]) => `${name}=${hash.slice(0, 12)}`)
    .join(" ") || "no settings files";

const runGit = (cwd: string, args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const exitCode = yield* spawner.exitCode(ChildProcess.make("git", args, { cwd }));
    if (Number(exitCode) !== 0) {
      return yield* new DroidRecorderError({
        detail: `git ${args.join(" ")} failed with exit ${exitCode}`,
      });
    }
  });

/** Mirrors `makeCheckpointWorkspace`, but under `/tmp` instead of the per-user temp dir. */
const makeRecordingWorkspace = (workspace: string, files: Readonly<Record<string, string>>) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    yield* fs.makeDirectory(workspace, { recursive: true });
    yield* runGit(workspace, ["init"]);
    yield* runGit(workspace, ["config", "user.name", "T3 Code Test"]);
    yield* runGit(workspace, ["config", "user.email", "t3code-test@example.com"]);
    yield* fs.writeFileString(path.join(workspace, "README.md"), `# ${fixture.name}\n`);
    for (const [relativePath, contents] of Object.entries(files)) {
      const filePath = path.join(workspace, relativePath);
      yield* fs.makeDirectory(path.dirname(filePath), { recursive: true });
      yield* fs.writeFileString(filePath, contents);
    }
    yield* runGit(workspace, ["add", "README.md", ...Object.keys(files)]);
    yield* runGit(workspace, ["commit", "-m", "initial"]);
  });

/**
 * Below the adapter, so what it does never lands in the transcript (which records the adapter's
 * own command). Droid authenticates only through the real home (`FACTORY_HOME_OVERRIDE` loses
 * the login), so the environment is left alone and the only isolation is ordering: a resume
 * starts after the previous process has exited.
 */
function makeIsolatingSpawner(
  live: ChildProcessSpawner.ChildProcessSpawner["Service"],
  awaitPreviousExit: boolean,
): ChildProcessSpawner.ChildProcessSpawner["Service"] {
  let previousExit: Effect.Effect<void> = Effect.void;
  return ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      if (awaitPreviousExit) {
        const exited = yield* previousExit.pipe(
          Effect.timeoutOption(PREVIOUS_EXIT_TIMEOUT),
          // Wall time: the recording's TestClock can jump past this bound.
          Effect.provideService(Clock.Clock, Clock.Clock.defaultValue()),
        );
        if (Option.isNone(exited)) {
          return yield* Effect.die(
            new DroidRecorderError({ detail: "the previous Droid process did not exit" }),
          );
        }
      }
      const handle = yield* live.spawn(command);
      previousExit = handle.exitCode.pipe(Effect.ignore);
      return handle;
    }),
  );
}

/** The version a peer reports on every frame it sends. */
function recordedProtocolVersion(entries: ReadonlyArray<ProviderReplayEntry>): string | undefined {
  for (const entry of entries) {
    if (entry.type !== "emit_inbound" || !Predicate.isObject(entry.frame)) continue;
    const version = Reflect.get(entry.frame, "factoryProtocolVersion");
    if (Predicate.isString(version)) return version;
  }
  return undefined;
}

interface RecordingPaths {
  readonly root: string;
  readonly workspace: string;
}

const recordTranscript = (input: {
  readonly paths: RecordingPaths;
  readonly droidVersion: string;
  readonly outputPath: string;
  readonly fixtureInput: ReturnType<typeof fixture.buildInput>;
}) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const { paths, droidVersion, outputPath, fixtureInput } = input;
    const entries: Array<ProviderReplayEntry> = [];
    const placeholder: ProviderReplayTranscript = {
      provider: DROID_PROVIDER,
      protocol: DROID_RPC_REPLAY_PROTOCOL,
      version: droidVersion,
      scenario: fixture.name,
      entries: [],
    };
    const layerRecordingSpawner = Layer.effect(
      ChildProcessSpawner.ChildProcessSpawner,
      Effect.map(Effect.service(ChildProcessSpawner.ChildProcessSpawner), (live) =>
        makeDroidRecordingSpawner(
          makeIsolatingSpawner(live, scenario.awaitPreviousExit ?? false),
          entries,
        ),
      ),
    ).pipe(Layer.provide(NodeServices.layer));

    const materializeInput = materializeFixtureInput({
      scenario: fixture.name,
      fixtureInput,
      driver: variant.driver,
      modelSelection: variant.modelSelection,
    }).pipe(Effect.provide(IdAllocator.layer));
    const continuationOptions =
      variant.runContinuationWorker === true ? { runContinuationWorker: true } : {};

    const scrubOptions = {
      modelId: variant.modelSelection.model,
      workspace: paths.workspace,
      recordingRoot: paths.root,
      systemTmpDir,
      home,
      userName,
      hostNames,
    };
    // A scenario step that fails mid-run (a pending approval nobody answers) loses the frames
    // already recorded, so keep the scrubbed prefix for diagnosis instead of another model turn.
    const keepPartialRecording = Effect.suspend(() =>
      writeRejectedTranscript({
        directory: REJECTED_TRANSCRIPT_DIRECTORY,
        fixture: `${fixture.name}.partial`,
        encoded: encodeTranscriptNdjson({
          ...placeholder,
          entries: scrubDroidEntries(entries, scrubOptions),
        }),
      }).pipe(
        Effect.tap((file) =>
          Console.error(`Live scenario failed; partial scrubbed transcript kept at ${file}`),
        ),
        Effect.ignore,
      ),
    );

    yield* Effect.gen(function* () {
      yield* followWallClock.pipe(Effect.forkScoped);
      const materialized = yield* materializeInput;
      yield* runOrchestratorV2ProviderReplayScenario(
        {
          name: `${fixture.name}/droid:record`,
          transcript: placeholder,
          commands: materialized.commands,
          steps: materialized.steps,
          projectionThreadIds: materialized.projectionThreadIds,
          runtimePolicyOverride: { ...variant.runtimePolicyOverride, cwd: paths.workspace },
        },
        {
          driver: DROID_PROVIDER,
          decodeTranscript: Effect.succeed,
          makeProviderAdapterRegistryLayer: () =>
            DroidAdapterV2Testkit.layer({
              scenario: fixture.name,
              binaryPath: DROID_BINARY,
              spawner: layerRecordingSpawner,
            }),
        },
        continuationOptions,
      );
    }).pipe(
      Effect.scoped,
      provideDeterministicTestRuntime,
      Effect.tapCause(() => keepPartialRecording),
    );

    const hostIds = collectHostIds(entries);
    const machineIds = collectMachineIds(entries);
    const protocolVersion = recordedProtocolVersion(entries);
    const transcript = {
      ...placeholder,
      metadata: {
        generatedBy: RECORDER_NAME,
        model: variant.modelSelection.model,
        droidVersion,
        factoryProtocolVersion: protocolVersion ?? "unknown",
      },
      entries: scrubDroidEntries(entries, scrubOptions),
    } satisfies ProviderReplayTranscript;

    const encoded = encodeTranscriptNdjson(transcript);
    // A rejected recording stays on this machine, outside the worktree, so it can be inspected
    // or fixed without spending another model turn.
    const keepRejected = (reason: string) =>
      writeRejectedTranscript({
        directory: REJECTED_TRANSCRIPT_DIRECTORY,
        fixture: fixture.name,
        encoded,
      }).pipe(
        Effect.tap((file) => Console.error(`${reason}; scrubbed transcript kept at ${file}`)),
        Effect.ignore,
      );

    // Canaries live only inside these closures: never printed, never written.
    const checks = buildLeakChecks({
      home,
      userName,
      hostNames,
      hostIds,
      machineIds,
      tempDirs: [paths.root, paths.workspace],
      instructionTexts: yield* readGlobalInstructionTexts(home),
      apiKey: process.env.FACTORY_API_KEY,
    });
    const leaks = findTranscriptLeaks(transcript.entries, checks);
    if (leaks.length > 0) {
      yield* keepRejected("Leak check failed");
      return yield* new DroidRecorderError({
        detail: `Refusing to write ${outputPath}: the scrubbed transcript still contains ${describeLeaks(leaks)}.`,
      });
    }

    // The live projection holds raw paths, so the fixture's assertions run on a replay of the
    // scrubbed transcript. A recording that fails them never replaces the existing fixture.
    yield* Effect.gen(function* () {
      const replayWorkspace = yield* checkpointWorkspace(fixture.name, fixtureInput.workspaceFiles);
      const materialized = yield* materializeInput;
      const replayResult = yield* runOrchestratorV2ProviderReplayScenario(
        {
          name: `${fixture.name}/droid:verify`,
          transcript: yield* DroidOrchestratorReplayHarness.decodeTranscript(transcript),
          commands: materialized.commands,
          steps: materialized.steps,
          projectionThreadIds: materialized.projectionThreadIds,
          runtimePolicyOverride: { ...variant.runtimePolicyOverride, cwd: replayWorkspace },
        },
        DroidOrchestratorReplayHarness,
        continuationOptions,
      );
      variant.assertOutput(replayResult, transcript);
    }).pipe(
      Effect.scoped,
      provideDeterministicTestRuntime,
      Effect.tapCause(() => keepRejected("Verification failed")),
    );

    yield* fs.makeDirectory(path.dirname(outputPath), { recursive: true });
    yield* fs.writeFileString(outputPath, encoded);
    yield* Console.log(
      `Wrote ${transcript.entries.length} Droid RPC replay entries to ${outputPath} (droid ${droidVersion}, factoryProtocolVersion ${protocolVersion ?? "unknown"})`,
    );
  });

const record = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const droidVersion = yield* readDroidVersion;
  const outputPath = readArgValue("--out") ?? (yield* path.fromFileUrl(variant.transcriptFile));
  const fixtureInput = (variant.buildInput ?? fixture.buildInput)();

  const hashesBefore = yield* settingsHashes;
  yield* Console.log(`settings hashes before: ${describeHashes(hashesBefore)}`);

  const root = yield* fs.makeTempDirectory({ directory: "/tmp", prefix: "droid-v2-rec-" });
  yield* Effect.addFinalizer(() =>
    fs.remove(root, { recursive: true, force: true }).pipe(Effect.ignore),
  );
  const paths: RecordingPaths = { root, workspace: path.join(root, "ws") };
  yield* makeRecordingWorkspace(paths.workspace, fixtureInput.workspaceFiles ?? {});

  const outcome = yield* Effect.exit(
    recordTranscript({ paths, droidVersion, outputPath, fixtureInput }),
  );

  const hashesAfter = yield* settingsHashes;
  const unchanged = encodeJson(hashesBefore) === encodeJson(hashesAfter);
  yield* Console.log(
    `settings hashes after: ${describeHashes(hashesAfter)} (${unchanged ? "unchanged" : "CHANGED"}); home=real`,
  );
  if (!unchanged) {
    return yield* new DroidRecorderError({
      detail: "A Droid settings file changed during the recording. Stop all live runs.",
    });
  }
  return yield* outcome;
});

record.pipe(Effect.scoped, Effect.provide(NodeServices.layer), NodeRuntime.runMain);
