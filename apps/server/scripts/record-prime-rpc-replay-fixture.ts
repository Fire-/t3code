import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import type {
  ProviderReplayEntry,
  ProviderReplayTranscript,
  RuntimeMode,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Console from "effect/Console";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import { TestClock } from "effect/testing";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import * as Net from "node:net";
import * as OS from "node:os";

import {
  makePrimeRecordingSpawner,
  PRIME_REPLAY_ANY,
  PRIME_RPC_REPLAY_PROTOCOL,
  PrimeOrchestratorReplayHarness,
} from "../src/orchestration-v2/Adapters/PrimeAdapterV2.testkit.ts";
import * as PrimeAdapterV2Testkit from "../src/orchestration-v2/Adapters/PrimeAdapterV2.testkit.ts";
import { PRIME_PROVIDER } from "@t3tools/provider-prime/testing";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { provideDeterministicTestRuntime } from "../src/orchestration-v2/testkit/DeterministicRuntime.ts";
import { ORCHESTRATOR_REPLAY_FIXTURES } from "../src/orchestration-v2/testkit/fixtures/index.ts";
import { materializeFixtureInput } from "../src/orchestration-v2/testkit/fixtures/shared.ts";
import { runOrchestratorV2ProviderReplayScenario } from "../src/orchestration-v2/testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";

/**
 * How each recordable Prime scenario launches. The fixture registration in
 * `fixtures/index.ts` owns the prompts, the model and the thread's runtime
 * mode; this table owns what the recorder adds around the binary.
 *
 * - `bridge`: keep T3's MCP bridge extension (`-e`). Only bridge and approval
 *   scenarios need it; everywhere else the recorder removes it so the user's
 *   environment cannot reach the transcript through it.
 * - `runtimeMode`: the runtime mode the fixture input must declare (the bridge
 *   gates `ipython` from it), checked before any model turn is spent.
 * - `launchArgs`: extra Prime flags such as `--autonomous-gate <command>`.
 *   They become part of the transcript's launch args, so replay passes them too.
 * - `awaitPreviousExit`: start a Prime process only after the previous one
 *   exited. Prime's daemon keeps the session lease until the old client's
 *   SIGTERM shutdown finishes, which in production ends long before the
 *   user's next message after an idle release; a fixture sends that message
 *   immediately, so without this the resume races the shutdown and records
 *   `SessionAlreadyActiveError` retries that a frozen replay clock never
 *   advances past.
 *
 * Steering and interrupt scenarios need no entry beyond `{ bridge: false }`:
 * their behaviour comes from the fixture input steps.
 */
interface PrimeRecordingScenario {
  readonly bridge: boolean;
  readonly runtimeMode?: RuntimeMode;
  readonly launchArgs?: ReadonlyArray<string>;
  readonly awaitPreviousExit?: boolean;
}

const PRIME_RECORDING_SCENARIOS: Readonly<Record<string, PrimeRecordingScenario>> = {
  simple: { bridge: false },
  multi_turn: { bridge: false },
  message_steering: { bridge: false },
  provider_thread_resume: { bridge: false, awaitPreviousExit: true },
};

/** Model turns cost real credits, so only the two models agreed for fixtures may record. */
const RECORDABLE_MODELS = new Set(["mistral/ministral-3b-latest", "mistral/zai-glm-5-3"]);

/** Keeps the user's skills, context files and discovered extensions out of the recording. */
const HERMETIC_LAUNCH_ARGS = ["--no-skills", "--no-context-files", "--no-extensions"];
const CLOCK_TICK = Duration.millis(20);
const DAEMON_SHUTDOWN_TIMEOUT = Duration.seconds(10);
const PREVIOUS_EXIT_TIMEOUT = Duration.seconds(20);
const PRIME_DAEMON_PROTOCOL_NAME = "prime-agent.daemon";

function readArgValue(name: string): string | undefined {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

const scenarioName = readArgValue("--scenario");
const recordableNames = ORCHESTRATOR_REPLAY_FIXTURES.filter(
  (entry) =>
    PRIME_RECORDING_SCENARIOS[entry.name] !== undefined &&
    entry.providers.some((provider) => provider.driver === PRIME_PROVIDER),
).map((entry) => entry.name);
const fixture = ORCHESTRATOR_REPLAY_FIXTURES.find((entry) => entry.name === scenarioName);
const variant = fixture?.providers.find((provider) => provider.driver === PRIME_PROVIDER);
const scenario = scenarioName === undefined ? undefined : PRIME_RECORDING_SCENARIOS[scenarioName];
if (fixture === undefined || variant === undefined || scenario === undefined) {
  throw new Error(
    `Pass --scenario with a fixture that registers Prime and has a recorder entry: ${recordableNames.join(", ")}`,
  );
}
if (!RECORDABLE_MODELS.has(variant.modelSelection.model)) {
  throw new Error(
    `Refusing to record ${fixture.name} with ${variant.modelSelection.model}; use one of ${[...RECORDABLE_MODELS].join(", ")}.`,
  );
}
const fixtureRuntimeMode = fixture.buildInput().runtimeMode ?? "full-access";
if (scenario.runtimeMode !== undefined && scenario.runtimeMode !== fixtureRuntimeMode) {
  throw new Error(
    `${fixture.name} must declare runtimeMode ${scenario.runtimeMode} in its fixture input (found ${fixtureRuntimeMode}).`,
  );
}

const primeBinary = process.env.T3_PRIME_BIN ?? "prime-agent";
const launchArgs = [...HERMETIC_LAUNCH_ARGS, ...(scenario.launchArgs ?? [])].join(" ");
const home = OS.homedir();
const userName = OS.userInfo().username;
const hostName = OS.hostname();
const systemTmpDir = OS.tmpdir().replace(/\/+$/u, "");

/**
 * Prime injects the user's global continual-harness memories into every new
 * session as a hidden `harness_digest` custom message
 * (`core/agent-session.ts` `_ensureHarnessDigestContext`), with no flag to
 * turn it off, so the recorder replaces its content.
 */
const HARNESS_DIGEST_CUSTOM_TYPE = "harness_digest";
const HARNESS_DIGEST_PLACEHOLDER = "<harness-digest>";
/** Very short host names (a two-letter one matches inside ordinary words) are neither replaced nor checked. */
const hostNames = [hostName, hostName.split(".")[0] ?? ""].filter((name) => name.length >= 4);

const UUID_PATTERN = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/giu;
const ISO_TIMESTAMP_PATTERN = /\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\b/gu;
const NORMALIZED_EPOCH_MS = DateTime.toEpochMillis(DateTime.makeUnsafe("2026-01-01T00:00:00.000Z"));

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** macOS reports `/tmp/...` and `/var/folders/...` paths through their `/private` realpath too. */
function withPrivateAlias(path: string): ReadonlyArray<string> {
  return path.startsWith("/private/")
    ? [path, path.slice("/private".length)]
    : [`/private${path}`, path];
}

interface RecordingPaths {
  readonly root: string;
  readonly workspace: string;
  readonly sessionDir: string;
  readonly daemonSocket: string;
}

/**
 * Session files, the workspace, Prime's agent dir, the home directory, host
 * and user names, UUIDs and wall-clock times become stable placeholders applied
 * consistently to both directions, so values the adapter echoes back (a session
 * path in `switch_session` or `--resume <file>`) still line up on replay.
 */
function normalizeEntries(input: {
  readonly entries: ReadonlyArray<ProviderReplayEntry>;
  readonly paths: RecordingPaths;
  readonly modelSlug: string;
}): ReadonlyArray<ProviderReplayEntry> {
  const sessionFiles = new Map<string, string>();
  const uuids = new Map<string, string>();
  let timeAnchorMs: number | undefined;
  const shiftTime = (ms: number): number => {
    timeAnchorMs ??= ms;
    return NORMALIZED_EPOCH_MS + (ms - timeAnchorMs);
  };
  const sessionDirs = withPrivateAlias(input.paths.sessionDir);
  const sessionFilePattern = new RegExp(
    `(?:${sessionDirs.map(escapeRegExp).join("|")})/[^\\s"']+?\\.jsonl`,
    "gu",
  );
  const replacements: ReadonlyArray<readonly [string, string]> = [
    ...sessionDirs.map((dir) => [dir, "/sessions"] as const),
    ...withPrivateAlias(input.paths.workspace).map((dir) => [dir, "<workspace>"] as const),
    ...withPrivateAlias(input.paths.daemonSocket).map((path) => [path, "<daemon-socket>"] as const),
    ...withPrivateAlias(input.paths.root).map((dir) => [dir, "/recording"] as const),
    ...withPrivateAlias(systemTmpDir).map((dir) => [dir, "<tmpdir>"] as const),
    [`${home}/.prime/agent`, "<prime-agent-dir>"],
    [home, "~"],
  ];
  const userPattern = new RegExp(`\\b${escapeRegExp(userName)}\\b`, "gu");
  const normalizeString = (value: string): string => {
    let next = value.replace(sessionFilePattern, (file) => {
      const key = file.startsWith("/private/") ? file.slice("/private".length) : file;
      const existing = sessionFiles.get(key);
      if (existing !== undefined) return existing;
      const normalized = `/sessions/session-${sessionFiles.size + 1}.jsonl`;
      sessionFiles.set(key, normalized);
      return normalized;
    });
    for (const [from, to] of replacements) next = next.replaceAll(from, to);
    for (const name of hostNames) next = next.replaceAll(name, "<host>");
    return next
      .replace(userPattern, "<user>")
      .replace(ISO_TIMESTAMP_PATTERN, (iso) =>
        DateTime.formatIso(
          DateTime.makeUnsafe(shiftTime(DateTime.toEpochMillis(DateTime.makeUnsafe(iso)))),
        ),
      )
      .replace(UUID_PATTERN, (uuid) => {
        const existing = uuids.get(uuid.toLowerCase());
        if (existing !== undefined) return existing;
        const normalized = `00000000-0000-4000-8000-${String(uuids.size + 1).padStart(12, "0")}`;
        uuids.set(uuid.toLowerCase(), normalized);
        return normalized;
      });
  };
  const normalizeValue = (value: unknown): unknown => {
    if (typeof value === "string") return normalizeString(value);
    if (Array.isArray(value)) return value.map(normalizeValue);
    if (!isRecord(value)) return value;
    const record =
      value.role === "custom" && value.customType === HARNESS_DIGEST_CUSTOM_TYPE
        ? {
            ...value,
            content: HARNESS_DIGEST_PLACEHOLDER,
            details: { digest: HARNESS_DIGEST_PLACEHOLDER, stateFingerprint: "<fingerprint>" },
          }
        : value;
    return Object.fromEntries(
      Object.entries(record).map(([key, entry]) => [
        key,
        key === "thinkingSignature" || key === "textSignature"
          ? "<signature>"
          : key === "timestamp" && typeof entry === "number"
            ? shiftTime(entry)
            : normalizeValue(entry),
      ]),
    );
  };
  const normalizeFrame = (frame: unknown): unknown => {
    if (!isRecord(frame)) return normalizeValue(frame);
    const args = frame.args;
    if (frame.type === "process_start" && Array.isArray(args)) {
      return normalizeValue({
        ...frame,
        args: args.map((arg, index) =>
          args[index - 1] === "-e" || args[index - 1] === "--extension" ? PRIME_REPLAY_ANY : arg,
        ),
      });
    }
    const data = frame.data;
    if (frame.command === "get_available_models" && isRecord(data) && Array.isArray(data.models)) {
      return normalizeValue({
        ...frame,
        data: {
          ...data,
          models: data.models.filter(
            (model) => isRecord(model) && `${model.provider}/${model.id}` === input.modelSlug,
          ),
        },
      });
    }
    return normalizeValue(frame);
  };
  return input.entries.map((entry) =>
    entry.type === "runtime_exit" ? entry : { ...entry, frame: normalizeFrame(entry.frame) },
  );
}

/** Strings that must never reach a committed transcript, checked after normalization. */
function forbiddenTranscriptContent(
  paths: RecordingPaths,
): ReadonlyArray<readonly [string, RegExp]> {
  const literal = (value: string) => new RegExp(escapeRegExp(value), "u");
  return [
    ["/Users/", /\/Users\//u],
    ["/home/", /\/home\//u],
    ["user name", new RegExp(`\\b${escapeRegExp(userName)}\\b`, "u")],
    ["fire", /\bfire\b/u],
    [".prime/", /\.prime\//u],
    ["/var/folders", /\/var\/folders/u],
    ["/private/tmp", /\/private\/tmp/u],
    ...hostNames.map((name) => ["host name", literal(name)] as const),
    ["daemon socket", literal(paths.daemonSocket)],
    ["recording root", literal(paths.root)],
  ];
}

class PrimeRecorderError extends Schema.TaggedError<PrimeRecorderError>()("PrimeRecorderError", {
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
 * Replay runs on a frozen TestClock; recording shares that runtime so fixture
 * `advance_clock` steps behave identically, but lets it follow wall time so a
 * live Prime's timeouts and termination grace periods still elapse.
 */
const followWallClock = Effect.gen(function* () {
  while (true) {
    yield* TestClock.withLive(Effect.sleep(CLOCK_TICK));
    yield* TestClock.adjust(CLOCK_TICK);
  }
});

const readPrimeVersion = Effect.gen(function* () {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  // Prime prints its version on stderr.
  const output = yield* spawner.string(ChildProcess.make(primeBinary, ["--version"]), {
    includeStderr: true,
  });
  return output.trim();
});

const runGit = (cwd: string, args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const exitCode = yield* spawner.exitCode(ChildProcess.make("git", args, { cwd }));
    if (Number(exitCode) !== 0) {
      return yield* new PrimeRecorderError({
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

function withoutExtensionArgs(args: ReadonlyArray<string>): ReadonlyArray<string> {
  return args.filter(
    (arg, index) =>
      arg !== "-e" &&
      arg !== "--extension" &&
      args[index - 1] !== "-e" &&
      args[index - 1] !== "--extension",
  );
}

/**
 * The adapter treats `--daemon-socket` as reserved, so the private socket and
 * the throwaway session dir are added below the adapter, where the transcript
 * (which records the adapter's own command) never sees them.
 */
function makeIsolatingSpawner(
  live: ChildProcessSpawner.ChildProcessSpawner["Service"],
  input: {
    readonly paths: RecordingPaths;
    readonly bridge: boolean;
    readonly awaitPreviousExit: boolean;
    readonly invocations: Array<string>;
  },
): ChildProcessSpawner.ChildProcessSpawner["Service"] {
  const { paths, bridge, awaitPreviousExit, invocations } = input;
  let previousExit: Effect.Effect<void> = Effect.void;
  return ChildProcessSpawner.make((command) => {
    if (!ChildProcess.isStandardCommand(command) || command.command !== primeBinary) {
      return live.spawn(command);
    }
    const args = [
      ...(bridge ? command.args : withoutExtensionArgs(command.args)),
      "--daemon-socket",
      paths.daemonSocket,
      "--session-dir",
      paths.sessionDir,
    ];
    return Effect.gen(function* () {
      if (awaitPreviousExit) {
        const exited = yield* previousExit.pipe(
          Effect.timeoutOption(PREVIOUS_EXIT_TIMEOUT),
          // Wall time: the recording's TestClock can jump past this bound.
          Effect.provideService(Clock.Clock, Clock.Clock.defaultValue()),
        );
        if (Option.isNone(exited)) {
          return yield* Effect.die(
            new PrimeRecorderError({ detail: "the previous Prime process did not exit" }),
          );
        }
      }
      invocations.push(`prime-agent ${args.join(" ")}`);
      const handle = yield* live.spawn(ChildProcess.make(command.command, args, command.options));
      previousExit = handle.exitCode.pipe(Effect.ignore);
      return handle;
    });
  });
}

interface DaemonHello {
  readonly clientId: string;
  readonly supervisorPid: number;
  readonly protocolVersion: number;
}

interface DaemonShutdownReply {
  readonly hello: DaemonHello;
  readonly accepted: boolean;
}

/**
 * Prime's own per-socket stop (`cli/daemon-launch.ts` `shutdownDaemonAndWait`):
 * read `daemon_hello`, send one `shutdown` command for that client, and let the
 * supervisor close. `prime-agent shutdown` would stop every daemon, the user's
 * included, so it is never used.
 */
const sendDaemonShutdown = (socketPath: string) =>
  Effect.callback<DaemonShutdownReply, PrimeRecorderError>((resume) => {
    const socket = Net.createConnection(socketPath);
    let buffer = "";
    let hello: DaemonHello | undefined;
    let accepted = false;
    let settled = false;
    const fail = (detail: string) => Effect.fail(new PrimeRecorderError({ detail }));
    const finish = (effect: Effect.Effect<DaemonShutdownReply, PrimeRecorderError>) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      resume(effect);
    };
    socket.setEncoding("utf8");
    socket.on("data", (chunk: string) => {
      buffer += chunk;
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      for (const line of lines) {
        let message: unknown;
        try {
          message = JSON.parse(line);
        } catch {
          continue;
        }
        if (!isRecord(message)) continue;
        if (message.type === "daemon_hello" && hello === undefined) {
          const protocol = isRecord(message.protocol) ? message.protocol : {};
          if (typeof message.clientId !== "string" || typeof message.supervisorPid !== "number") {
            finish(fail("daemon_hello lacks clientId or supervisorPid"));
            return;
          }
          hello = {
            clientId: message.clientId,
            supervisorPid: message.supervisorPid,
            protocolVersion: typeof protocol.version === "number" ? protocol.version : 7,
          };
          socket.write(
            `${JSON.stringify({
              type: "command",
              id: "ctl_1",
              protocol: { name: PRIME_DAEMON_PROTOCOL_NAME, version: hello.protocolVersion },
              clientId: hello.clientId,
              command: { type: "shutdown", id: "ctl_1" },
            })}\n`,
          );
        } else if (message.type === "response" && message.command === "shutdown") {
          accepted = message.success === true;
          if (!accepted) {
            finish(fail(`daemon refused shutdown: ${line}`));
            return;
          }
        } else if (message.type === "daemon_closing" && hello !== undefined) {
          finish(Effect.succeed({ hello, accepted }));
          return;
        }
      }
    });
    socket.on("error", (error) => finish(fail(`daemon socket error: ${error.message}`)));
    socket.on("close", () =>
      finish(
        hello === undefined
          ? fail("daemon socket closed before daemon_hello")
          : Effect.succeed({ hello, accepted }),
      ),
    );
    return Effect.sync(() => socket.destroy());
  });

function processExists(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== "ESRCH";
  }
}

/** Stops the recorder's private daemon and confirms its socket, lock and supervisor are gone. */
const stopPrivateDaemon = (socketPath: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    if (!(yield* fs.exists(socketPath))) {
      yield* Console.log(`private daemon: none was started on ${socketPath}`);
      return;
    }
    const reply = yield* sendDaemonShutdown(socketPath).pipe(
      Effect.timeoutOrElse({
        duration: DAEMON_SHUTDOWN_TIMEOUT,
        orElse: () =>
          Effect.fail(new PrimeRecorderError({ detail: "no daemon_closing within the timeout" })),
      }),
    );
    const gone = Effect.gen(function* () {
      const socketLeft = yield* fs.exists(socketPath);
      const lockLeft = yield* fs.exists(`${socketPath}.lock`);
      return !socketLeft && !lockLeft && !processExists(reply.hello.supervisorPid);
    });
    const stopped = yield* gone.pipe(
      Effect.repeat({
        until: (done) => done,
        schedule: Schedule.spaced(Duration.millis(100)),
      }),
      Effect.timeoutOrElse({
        duration: DAEMON_SHUTDOWN_TIMEOUT,
        orElse: () => Effect.succeed(false),
      }),
    );
    if (!stopped) {
      return yield* new PrimeRecorderError({
        detail: `private daemon on ${socketPath} (supervisorPid ${reply.hello.supervisorPid}) did not exit`,
      });
    }
    yield* Console.log(
      `private daemon: shutdown accepted=${reply.accepted}; supervisorPid ${reply.hello.supervisorPid} exited; ${socketPath} and its lock removed`,
    );
  }).pipe(
    Effect.catch((error) =>
      Console.error(
        `private daemon: FAILED to stop ${socketPath}: ${String(error)}. Stop it with the per-socket shutdown message; never with \`prime-agent shutdown\`.`,
      ),
    ),
  );

const record = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const primeVersion = yield* readPrimeVersion;
  const outputPath = readArgValue("--out") ?? (yield* path.fromFileUrl(variant.transcriptFile));
  const fixtureInput = fixture.buildInput();

  const root = yield* fs.makeTempDirectory({ directory: "/tmp", prefix: `pv2-${fixture.name}-` });
  yield* fs.makeDirectory("/tmp/pv2r", { recursive: true });
  // macOS caps unix socket paths near 104 bytes, so the socket name stays short.
  const paths: RecordingPaths = {
    root,
    workspace: path.join(root, "ws"),
    sessionDir: path.join(root, "sessions"),
    daemonSocket: `/tmp/pv2r/${path.basename(root).slice(-6)}.sock`,
  };
  yield* fs.makeDirectory(paths.sessionDir, { recursive: true });
  yield* makeRecordingWorkspace(paths.workspace, fixtureInput.workspaceFiles ?? {});
  yield* Effect.addFinalizer(() =>
    fs.remove(root, { recursive: true, force: true }).pipe(Effect.ignore),
  );
  const invocations: Array<string> = [`prime-agent --version (printed ${primeVersion})`];
  // Registered after the workspace cleanup so it runs first, and on every exit path.
  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      for (const invocation of invocations) yield* Console.log(`invocation: ${invocation}`);
      yield* stopPrivateDaemon(paths.daemonSocket);
    }).pipe(Effect.provideService(FileSystem.FileSystem, fs)),
  );

  const entries: Array<ProviderReplayEntry> = [];
  const placeholder: ProviderReplayTranscript = {
    provider: PRIME_PROVIDER,
    protocol: PRIME_RPC_REPLAY_PROTOCOL,
    version: primeVersion,
    scenario: fixture.name,
    entries: [],
  };
  const layerRecordingSpawner = Layer.effect(
    ChildProcessSpawner.ChildProcessSpawner,
    Effect.map(Effect.service(ChildProcessSpawner.ChildProcessSpawner), (live) =>
      makePrimeRecordingSpawner(
        makeIsolatingSpawner(live, {
          paths,
          bridge: scenario.bridge,
          awaitPreviousExit: scenario.awaitPreviousExit ?? false,
          invocations,
        }),
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

  yield* Effect.gen(function* () {
    yield* followWallClock.pipe(Effect.forkScoped);
    const materialized = yield* materializeInput;
    yield* runOrchestratorV2ProviderReplayScenario(
      {
        name: `${fixture.name}/prime-agent:record`,
        transcript: placeholder,
        commands: materialized.commands,
        steps: materialized.steps,
        projectionThreadIds: materialized.projectionThreadIds,
        runtimePolicyOverride: { ...variant.runtimePolicyOverride, cwd: paths.workspace },
      },
      {
        driver: PRIME_PROVIDER,
        decodeTranscript: Effect.succeed,
        makeProviderAdapterRegistryLayer: () =>
          PrimeAdapterV2Testkit.layer({
            scenario: fixture.name,
            binaryPath: primeBinary,
            launchArgs,
            spawner: layerRecordingSpawner,
          }),
      },
      continuationOptions,
    );
  }).pipe(Effect.scoped, provideDeterministicTestRuntime);

  const transcript = {
    ...placeholder,
    metadata: {
      generatedBy: "live-prime-recorder",
      primeVersion,
      model: variant.modelSelection.model,
      launchArgs,
      bridge: scenario.bridge,
      recorderIsolation:
        "--daemon-socket <private /tmp socket> --session-dir /sessions added below the adapter",
    },
    entries: normalizeEntries({ entries, paths, modelSlug: variant.modelSelection.model }),
  } satisfies ProviderReplayTranscript;

  const encoded = encodeTranscriptNdjson(transcript);
  // A rejected recording stays on this machine, outside the worktree, so it can
  // be inspected or fixed without spending another model turn.
  const rejectedPath = `/tmp/pv2r/${fixture.name}.rejected.ndjson`;
  const keepRejected = (reason: string) =>
    fs
      .writeFileString(rejectedPath, encoded)
      .pipe(
        Effect.andThen(Console.error(`${reason}; normalized transcript kept at ${rejectedPath}`)),
        Effect.ignore,
      );
  const leaks = forbiddenTranscriptContent(paths).filter(([, pattern]) => pattern.test(encoded));
  if (leaks.length > 0) {
    const lines = encoded.split("\n");
    const details = leaks.map(([name, pattern]) => {
      const line = lines.findIndex((candidate) => pattern.test(candidate));
      return `${name} (transcript line ${line + 1})`;
    });
    yield* keepRejected("Scrub check failed");
    return yield* new PrimeRecorderError({
      detail: `Refusing to write ${outputPath}: normalization left ${details.join(", ")}.`,
    });
  }

  // The live projection still holds raw session paths, so the fixture's
  // assertions run on a replay of the normalized transcript. A recording that
  // fails them never replaces the existing fixture.
  yield* Effect.gen(function* () {
    const replayWorkspace = yield* checkpointWorkspace(fixture.name, fixtureInput.workspaceFiles);
    const materialized = yield* materializeInput;
    const replayResult = yield* runOrchestratorV2ProviderReplayScenario(
      {
        name: `${fixture.name}/prime-agent:verify`,
        transcript: yield* PrimeOrchestratorReplayHarness.decodeTranscript(transcript),
        commands: materialized.commands,
        steps: materialized.steps,
        projectionThreadIds: materialized.projectionThreadIds,
        runtimePolicyOverride: { ...variant.runtimePolicyOverride, cwd: replayWorkspace },
      },
      PrimeOrchestratorReplayHarness,
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
    `Wrote ${transcript.entries.length} Prime RPC replay entries to ${outputPath}`,
  );
});

record.pipe(Effect.scoped, Effect.provide(NodeServices.layer), NodeRuntime.runMain);
