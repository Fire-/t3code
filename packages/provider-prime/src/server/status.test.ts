import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { checkPrimeProviderStatus, MINIMUM_PRIME_VERSION } from "./status.ts";

const decodeJsonLine = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const encodeJsonLine = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const encoder = new TextEncoder();

// Trimmed from a live `get_available_models` response (Prime v0.9.8 line).
const GLM_5_3 = {
  id: "zai-glm-5-3",
  name: "GLM-5.3",
  api: "mistral-conversations",
  provider: "mistral",
  reasoning: true,
  thinkingLevelMap: {
    off: null,
    minimal: null,
    low: "low",
    medium: null,
    high: "high",
    xhigh: null,
    max: "max",
  },
  input: ["text"],
  contextWindow: 1_048_576,
  maxTokens: 131_072,
};
const MINISTRAL_3B = {
  id: "ministral-3b-latest",
  name: "Ministral 3B (latest)",
  api: "mistral-conversations",
  provider: "mistral",
  reasoning: false,
  input: ["text"],
  contextWindow: 128_000,
  maxTokens: 128_000,
};

interface FakePrimeOptions {
  readonly versionOutput?: string;
  readonly missingBinary?: boolean;
  readonly models?: ReadonlyArray<Record<string, unknown>>;
  readonly modelsError?: string;
}

interface SpawnRecord {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
}

function finishedProcess(input: { readonly stdout?: string; readonly exitCode?: number }) {
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(999_999_998),
    exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(input.exitCode ?? 0)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.drain,
    stdout:
      input.stdout === undefined ? Stream.empty : Stream.succeed(encoder.encode(input.stdout)),
    stderr: Stream.empty,
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
}

const makeRpcProcess = (options: FakePrimeOptions, received: Array<Record<string, unknown>>) =>
  Effect.gen(function* () {
    const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
    const emit = (record: Record<string, unknown>) =>
      Queue.offer(stdout, encoder.encode(`${encodeJsonLine(record)}\n`));
    let buffered = "";
    const onStdin = (chunk: Uint8Array) =>
      Effect.gen(function* () {
        buffered += new TextDecoder().decode(chunk);
        const lines = buffered.split("\n");
        buffered = lines.pop() ?? "";
        for (const line of lines.filter((candidate) => candidate.length > 0)) {
          const record = decodeJsonLine(line) as Record<string, unknown>;
          received.push(record);
          const response = { type: "response", id: record["id"], command: record["type"] };
          switch (record["type"]) {
            case "get_available_models":
              yield* emit(
                options.modelsError === undefined
                  ? {
                      ...response,
                      success: true,
                      data: { models: options.models ?? [GLM_5_3, MINISTRAL_3B] },
                    }
                  : { ...response, success: false, error: options.modelsError },
              );
              break;
            case "get_state":
              yield* emit({
                ...response,
                success: true,
                data: { model: GLM_5_3, thinkingLevel: "max", isStreaming: false },
              });
              break;
            case "get_commands":
              yield* emit({ ...response, success: true, data: { commands: [] } });
              break;
            default:
              yield* emit({ ...response, success: true });
          }
        }
      });
    return ChildProcessSpawner.makeHandle({
      // Outside the valid pid range, so PrimeRpc's process-group kill never lands.
      pid: ChildProcessSpawner.ProcessId(999_999_999),
      exitCode: Effect.never,
      isRunning: Effect.succeed(true),
      kill: () => Effect.void,
      unref: Effect.succeed(Effect.void),
      stdin: Sink.forEach(onStdin),
      stdout: Stream.fromQueue(stdout),
      stderr: Stream.empty,
      all: Stream.empty,
      getInputFd: () => Sink.drain,
      getOutputFd: () => Stream.empty,
    });
  });

function makeFakePrime(options: FakePrimeOptions = {}) {
  const spawns: Array<SpawnRecord> = [];
  const received: Array<Record<string, unknown>> = [];
  const spawner = ChildProcessSpawner.make((command) => {
    const record = ChildProcess.isStandardCommand(command)
      ? { command: command.command, args: [...command.args] }
      : { command: "", args: [] };
    spawns.push(record);
    if (options.missingBinary === true) {
      return Effect.fail(
        PlatformError.systemError({
          _tag: "NotFound",
          module: "ChildProcess",
          method: "spawn",
          description: "prime-agent missing",
        }),
      );
    }
    if (record.args.includes("--version")) {
      return Effect.succeed(finishedProcess({ stdout: options.versionOutput ?? "1.0.0\n" }));
    }
    return makeRpcProcess(options, received);
  });
  return { spawner, spawns, received };
}

const settings = {
  enabled: true,
  binaryPath: "prime-agent",
  launchArgs: "",
  customModels: [],
} as const;

/** A temp Prime agent dir; `withAuthFile` writes an unreadable, non-JSON auth.json. */
const makeAgentDir = (withAuthFile: boolean) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const agentDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-prime-agent-" });
    if (withAuthFile) {
      const authFile = path.join(agentDir, "auth.json");
      yield* fs.writeFileString(authFile, "not json {");
      yield* fs.chmod(authFile, 0o000);
    }
    return agentDir;
  });

const runStatus = (
  fake: ReturnType<typeof makeFakePrime>,
  environment: NodeJS.ProcessEnv,
  primeSettings: Parameters<typeof checkPrimeProviderStatus>[0] = settings,
) =>
  checkPrimeProviderStatus(primeSettings, environment).pipe(
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner),
  );

const rpcSpawns = (fake: ReturnType<typeof makeFakePrime>) =>
  fake.spawns.filter((spawn) => spawn.args.includes("rpc"));

describe("checkPrimeProviderStatus", () => {
  it.effect("[VAL-STATUS-001] accepts 1.0.0, the 0.9.8 floor, and a prime-agent prefix", () =>
    Effect.gen(function* () {
      const agentDir = yield* makeAgentDir(true);
      for (const [output, expected] of [
        ["1.0.0\n", "1.0.0"],
        ["0.9.8\n", "0.9.8"],
        ["prime-agent 1.0.0\n", "1.0.0"],
      ] as const) {
        const snapshot = yield* runStatus(makeFakePrime({ versionOutput: output }), {
          PRIME_AGENT_CODING_AGENT_DIR: agentDir,
        });
        assert.equal(snapshot.status, "ready", output);
        assert.equal(snapshot.installed, true);
        assert.equal(snapshot.version, expected);
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("[VAL-STATUS-001] reports 0.9.7 as below the 0.9.8 minimum with update guidance", () =>
    Effect.gen(function* () {
      const fake = makeFakePrime({ versionOutput: "0.9.7\n" });
      const snapshot = yield* runStatus(fake, {});
      assert.equal(MINIMUM_PRIME_VERSION, "0.9.8");
      assert.equal(snapshot.status, "error");
      assert.equal(snapshot.installed, true);
      assert.equal(snapshot.version, "0.9.7");
      assert.equal(
        snapshot.message,
        "Prime 0.9.7 is unsupported. Update to Prime 0.9.8 or newer with `prime-agent update`.",
      );
      assert.lengthOf(rpcSpawns(fake), 0);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("[VAL-STATUS-001] reports unparsable version output as a clear error", () =>
    Effect.gen(function* () {
      const fake = makeFakePrime({ versionOutput: "garbage output\n" });
      const snapshot = yield* runStatus(fake, {});
      assert.equal(snapshot.status, "error");
      assert.equal(snapshot.installed, true);
      assert.isNull(snapshot.version);
      assert.equal(
        snapshot.message,
        "T3 Code could not read the Prime version from `prime-agent --version`. Prime 0.9.8 or newer is required.",
      );
      assert.lengthOf(rpcSpawns(fake), 0);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("[VAL-STATUS-002] a non-blank PRIME_API_KEY authenticates without leaking it", () =>
    Effect.gen(function* () {
      const agentDir = yield* makeAgentDir(false);
      const secret = "not-a-real-prime-key";
      const snapshot = yield* runStatus(makeFakePrime(), {
        PRIME_API_KEY: secret,
        PRIME_AGENT_CODING_AGENT_DIR: agentDir,
      });
      assert.equal(snapshot.status, "ready");
      assert.deepEqual(snapshot.auth, {
        status: "authenticated",
        type: "api_key",
        label: "Prime API key",
      });
      assert.notInclude(JSON.stringify(snapshot), secret);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("[VAL-STATUS-002] a blank PRIME_API_KEY falls through to the auth.json check", () =>
    Effect.gen(function* () {
      const withoutFile = yield* runStatus(makeFakePrime(), {
        PRIME_API_KEY: "   ",
        PRIME_AGENT_CODING_AGENT_DIR: yield* makeAgentDir(false),
      });
      assert.equal(withoutFile.auth.status, "unauthenticated");

      const withFile = yield* runStatus(makeFakePrime(), {
        PRIME_API_KEY: "",
        PRIME_AGENT_CODING_AGENT_DIR: yield* makeAgentDir(true),
      });
      assert.deepEqual(withFile.auth, {
        status: "authenticated",
        type: "cached_token",
        label: "Prime account",
      });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "[VAL-STATUS-003] an existing auth.json authenticates even when it is unreadable and not JSON",
    () =>
      Effect.gen(function* () {
        const snapshot = yield* runStatus(makeFakePrime(), {
          PRIME_AGENT_CODING_AGENT_DIR: yield* makeAgentDir(true),
        });
        assert.equal(snapshot.status, "ready");
        assert.equal(snapshot.auth.status, "authenticated");
        assert.isUndefined(snapshot.message);
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("[VAL-STATUS-003] defaults the agent dir to ~/.prime/agent", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const home = yield* fs.makeTempDirectoryScoped({ prefix: "t3-prime-home-" });
      const fake = makeFakePrime();
      const signedOut = yield* runStatus(fake, {}).pipe(
        Effect.provideService(HostProcess.HomeDirectory, home),
      );
      assert.equal(signedOut.auth.status, "unauthenticated");

      yield* fs.makeDirectory(path.join(home, ".prime", "agent"), { recursive: true });
      yield* fs.writeFileString(path.join(home, ".prime", "agent", "auth.json"), "");
      const signedIn = yield* runStatus(fake, {}).pipe(
        Effect.provideService(HostProcess.HomeDirectory, home),
      );
      assert.equal(signedIn.auth.status, "authenticated");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "[VAL-STATUS-003] a missing auth.json reports unauthenticated with sign-in guidance",
    () =>
      Effect.gen(function* () {
        const fake = makeFakePrime();
        const snapshot = yield* runStatus(fake, {
          PRIME_AGENT_CODING_AGENT_DIR: yield* makeAgentDir(false),
        });
        assert.equal(snapshot.status, "error");
        assert.equal(snapshot.installed, true);
        assert.equal(snapshot.version, "1.0.0");
        assert.equal(snapshot.auth.status, "unauthenticated");
        assert.equal(
          snapshot.message,
          "Prime is not signed in. Run `prime-agent` in a terminal and sign in with /login, or set PRIME_API_KEY in this instance's environment variables.",
        );
        assert.lengthOf(rpcSpawns(fake), 0);
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "[VAL-STATUS-004] discovers provider/id models over a sessionless, tool-less, extension-less RPC probe",
    () =>
      Effect.gen(function* () {
        const fake = makeFakePrime();
        const snapshot = yield* runStatus(
          fake,
          { PRIME_AGENT_CODING_AGENT_DIR: yield* makeAgentDir(true) },
          {
            ...settings,
            customModels: ["acme/custom-model", { slug: "mistral/zai-glm-5-3", name: "My GLM" }],
          },
        );

        const [rpc, ...otherRpcSpawns] = rpcSpawns(fake);
        assert.lengthOf(otherRpcSpawns, 0);
        assert.equal(rpc?.command, "prime-agent");
        assert.deepEqual(rpc?.args.slice(0, 2), ["--mode", "rpc"]);
        assert.sameMembers(
          [...(rpc?.args ?? [])],
          ["--mode", "rpc", "--no-session", "--no-tools", "--no-extensions"],
        );
        assert.include(
          fake.received.map((record) => record["type"]),
          "get_available_models",
        );
        // Discovery only reads; nothing may create, resume, or drive a session.
        for (const record of fake.received) {
          assert.include(["get_state", "get_available_models", "get_commands"], record["type"]);
        }

        assert.equal(snapshot.status, "ready");
        assert.deepEqual(
          snapshot.models.map((model) => [model.slug, model.name, model.isCustom]),
          [
            ["default", "Prime default", false],
            ["mistral/zai-glm-5-3", "My GLM", false],
            ["mistral/ministral-3b-latest", "Ministral 3B (latest)", false],
            ["acme/custom-model", "acme/custom-model", true],
          ],
        );
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "[VAL-SET-005] discovered models expose thinking under option id thinking with per-model levels",
    () =>
      Effect.gen(function* () {
        const snapshot = yield* runStatus(makeFakePrime(), {
          PRIME_AGENT_CODING_AGENT_DIR: yield* makeAgentDir(true),
        });
        const glm = snapshot.models.find((model) => model.slug === "mistral/zai-glm-5-3");
        const thinking = glm?.capabilities?.optionDescriptors?.[0];
        assert.equal(thinking?.id, "thinking");
        assert.equal(thinking?.type, "select");
        if (thinking?.type !== "select") return;
        assert.deepEqual(
          thinking.options.map((option) => [option.id, option.isDefault === true]),
          [
            ["low", false],
            ["high", false],
            ["max", true],
          ],
        );
        const ministral = snapshot.models.find(
          (model) => model.slug === "mistral/ministral-3b-latest",
        );
        assert.deepEqual(ministral?.capabilities?.optionDescriptors ?? [], []);
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("[VAL-STATUS-004] models that share a name are told apart by their id", () =>
    Effect.gen(function* () {
      // Prime's catalog names both of these "Devstral 2".
      const devstral = { ...MINISTRAL_3B, name: "Devstral 2" };
      const snapshot = yield* runStatus(
        makeFakePrime({
          models: [
            { ...devstral, id: "devstral-2512" },
            { ...devstral, id: "devstral-latest" },
            MINISTRAL_3B,
          ],
        }),
        { PRIME_AGENT_CODING_AGENT_DIR: yield* makeAgentDir(true) },
      );
      assert.deepEqual(
        snapshot.models.map((model) => model.name),
        [
          "Prime default",
          "Devstral 2 (devstral-2512)",
          "Devstral 2 (devstral-latest)",
          "Ministral 3B (latest)",
        ],
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("[VAL-STATUS-005] a missing binary reports not installed with Prime's installer", () =>
    Effect.gen(function* () {
      const snapshot = yield* runStatus(makeFakePrime({ missingBinary: true }), {});
      assert.equal(snapshot.status, "error");
      assert.equal(snapshot.installed, false);
      assert.equal(
        snapshot.message,
        "Prime (`prime-agent`) is not installed or not on PATH. Install it with Prime's installer: `curl -fsSL https://app.primeintellect.ai/prime-agent/install.sh | sh`.",
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "[VAL-STATUS-005] a failing get_available_models degrades to a warning with fallback models",
    () =>
      Effect.gen(function* () {
        const snapshot = yield* runStatus(
          makeFakePrime({ modelsError: "model registry unavailable" }),
          { PRIME_AGENT_CODING_AGENT_DIR: yield* makeAgentDir(true) },
          { ...settings, customModels: ["mistral/zai-glm-5-3"] },
        );
        assert.equal(snapshot.status, "warning");
        assert.equal(snapshot.installed, true);
        assert.equal(snapshot.version, "1.0.0");
        assert.equal(snapshot.auth.status, "authenticated");
        assert.equal(
          snapshot.message,
          "Prime is installed, but T3 Code could not load its models. The model list may be incomplete.",
        );
        assert.deepEqual(
          snapshot.models.map((model) => model.slug),
          ["default", "mistral/zai-glm-5-3"],
        );
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("[VAL-STATUS-005] an empty model list is a warning, not a ready inventory", () =>
    Effect.gen(function* () {
      const snapshot = yield* runStatus(makeFakePrime({ models: [] }), {
        PRIME_AGENT_CODING_AGENT_DIR: yield* makeAgentDir(true),
      });
      assert.equal(snapshot.status, "warning");
      assert.equal(snapshot.auth.status, "authenticated");
      assert.equal(
        snapshot.message,
        "Prime returned no models. Check the providers configured in Prime.",
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
