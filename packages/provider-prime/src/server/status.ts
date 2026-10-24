import {
  type CustomModelSetting,
  type ServerProvider,
  type ServerProviderAuth,
  type ServerProviderModel,
} from "@t3tools/contracts";
import type { PrimeSettings } from "../settings.ts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { causeErrorTag } from "@t3tools/shared/observability";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import { compareSemverVersions } from "@t3tools/shared/semver";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import * as HttpClient from "effect/http/HttpClient";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { buildPrimeRpcLaunch, resolvePrimeLaunchArgs } from "./mcpInjection.ts";
import {
  makePrimeRpcConnection,
  PrimeRpcError,
  PrimeRpcTimeoutError,
  primeRecordField as recordField,
  primeRecordString as recordString,
} from "./rpc.ts";
import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "@t3tools/provider-core/server/snapshotProbe";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  type ProviderMaintenanceCapabilities,
} from "@t3tools/provider-core/server/maintenanceResolver";
import {
  EMPTY_PRIME_MODEL_CAPABILITIES,
  thinkingCapabilitiesForPrimeModel,
} from "./thinkingCapabilities.ts";
import {
  parsePrimeDiscoveredCommands,
  withPrimeBuiltinSlashCommands,
  type PrimeDiscoveredCommands,
} from "./commands.ts";

const PRIME_PRESENTATION = {
  displayName: "Prime",
  showInteractionModeToggle: false,
  supportedRuntimeModes: ["approval-required", "auto-accept-edits", "full-access"],
  reportsContextWindow: true,
  requiresNewThreadForModelChange: false,
} as const;

const VERSION_PROBE_TIMEOUT_MS = 4_000;
const PRIME_RPC_DISCOVERY_TIMEOUT_MS = 15_000;
/**
 * The RPC dialect T3 speaks is Prime's TypeScript line 0.9.8. Installed
 * launchers report the runtime wrapper's version (1.0.0 for a 0.9.8 build),
 * so any newer version is accepted.
 */
export const MINIMUM_PRIME_VERSION = "0.9.8";
const PRIME_API_KEY_ENV = "PRIME_API_KEY";
// Prime's config.ts derives this override name from its package config.
const PRIME_AGENT_DIR_ENV = "PRIME_AGENT_CODING_AGENT_DIR";
const PRIME_INSTALL_GUIDANCE =
  "Install it with Prime's installer: `curl -fsSL https://app.primeintellect.ai/prime-agent/install.sh | sh`.";

const PRIME_DEFAULT_MODEL: ServerProviderModel = {
  slug: "default",
  name: "Prime default",
  isCustom: false,
  capabilities: EMPTY_PRIME_MODEL_CAPABILITIES,
};

interface PrimeDiscovery extends PrimeDiscoveredCommands {
  readonly models: ReadonlyArray<ServerProviderModel>;
}

function primeModelsFromSettings(
  customModels: ReadonlyArray<CustomModelSetting> | undefined,
  discovered: ReadonlyArray<ServerProviderModel> = [],
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(
    [PRIME_DEFAULT_MODEL, ...discovered],
    customModels ?? [],
    EMPTY_PRIME_MODEL_CAPABILITIES,
  );
}

function parseDiscoveredModels(data: unknown, state: unknown): ReadonlyArray<ServerProviderModel> {
  const models = recordField(data, "models");
  if (!Array.isArray(models)) return [];
  const activeModel = recordField(state, "model");
  const activeProvider = recordString(activeModel, "provider");
  const activeModelId = recordString(activeModel, "id");
  const seen = new Set<string>();
  const parsed: Array<{ readonly id: string; readonly model: ServerProviderModel }> = [];
  for (const model of models) {
    const provider = recordString(model, "provider");
    const id = recordString(model, "id");
    if (provider === undefined || id === undefined) continue;
    const slug = `${provider}/${id}`;
    if (seen.has(slug)) continue;
    seen.add(slug);
    parsed.push({
      id,
      model: {
        slug,
        name: recordString(model, "name") ?? slug,
        subProvider: provider,
        isCustom: false,
        capabilities: thinkingCapabilitiesForPrimeModel(
          model,
          provider === activeProvider && id === activeModelId
            ? recordString(state, "thinkingLevel")
            : undefined,
        ),
      },
    });
  }
  // Prime's catalog can give two ids the same name (mistral's devstral-2512
  // and devstral-latest are both "Devstral 2"), which leaves pickers and
  // agents unable to tell them apart.
  const nameCounts = new Map<string, number>();
  for (const { model } of parsed) {
    nameCounts.set(model.name, (nameCounts.get(model.name) ?? 0) + 1);
  }
  return parsed.map(({ id, model }) =>
    (nameCounts.get(model.name) ?? 0) > 1 ? { ...model, name: `${model.name} (${id})` } : model,
  );
}

const makePrimeDiscoveryConnection = Effect.fnUntraced(function* (
  primeSettings: PrimeSettings,
  environment: NodeJS.ProcessEnv,
  launchArgs: ReadonlyArray<string>,
  cwd?: string,
  restricted = false,
) {
  const launch = buildPrimeRpcLaunch({
    launchArgs,
    environment,
    mcpSession: undefined,
    extensionPath: undefined,
    ephemeral: true,
    ...(restricted ? { disableExtensions: true, disableTools: true } : {}),
  });
  const connection = yield* makePrimeRpcConnection({
    command: primeSettings.binaryPath || "prime-agent",
    args: launch.args,
    cwd,
    env: launch.env,
  });
  yield* Stream.fromQueue(connection.events).pipe(
    Stream.runDrain,
    Effect.ignore,
    Effect.forkScoped,
  );
  return connection;
});

const discoverPrimeViaRpc = (
  primeSettings: PrimeSettings,
  environment: NodeJS.ProcessEnv,
  launchArgs: ReadonlyArray<string>,
  cwd?: string,
) =>
  Effect.gen(function* () {
    // Every `--mode rpc` run attaches to Prime's daemon, even with
    // --no-session (main.ts shouldUseDaemonClient). The probe only reads, so it
    // also drops tools and extensions.
    const connection = yield* makePrimeDiscoveryConnection(
      primeSettings,
      environment,
      launchArgs,
      cwd,
      true,
    );
    // get_state only supplies the default thinking level; get_commands is additive.
    const stateData = yield* connection
      .request({ type: "get_state" })
      .pipe(Effect.orElseSucceed(() => undefined));
    const modelsData = yield* connection.request({ type: "get_available_models" });
    const commandsData = yield* connection
      .request({ type: "get_commands" })
      .pipe(Effect.orElseSucceed(() => undefined));
    const { slashCommands, skills } = parsePrimeDiscoveredCommands(commandsData);
    return {
      models: parseDiscoveredModels(modelsData, stateData),
      slashCommands: withPrimeBuiltinSlashCommands(slashCommands),
      skills,
    } satisfies PrimeDiscovery;
  }).pipe(Effect.scoped);

export const discoverPrimeCommandsForCwd = Effect.fn("discoverPrimeCommandsForCwd")(
  function* (primeSettings: PrimeSettings, environment: NodeJS.ProcessEnv, cwd: string) {
    const launchArgs = resolvePrimeLaunchArgs(primeSettings.launchArgs);
    if (!launchArgs.ok) {
      return yield* new PrimeRpcError({ operation: "launch", detail: launchArgs.message });
    }
    const connection = yield* makePrimeDiscoveryConnection(
      primeSettings,
      environment,
      launchArgs.args,
      cwd,
    );
    // A failed read must not replace a previously usable workspace catalog with an empty one.
    const commandsData = yield* connection.request({ type: "get_commands" });
    const { slashCommands, skills } = parsePrimeDiscoveredCommands(commandsData);
    return { slashCommands: withPrimeBuiltinSlashCommands(slashCommands), skills };
  },
  Effect.scoped,
  Effect.timeoutOrElse({
    duration: PRIME_RPC_DISCOVERY_TIMEOUT_MS,
    orElse: () =>
      Effect.fail(
        new PrimeRpcTimeoutError({
          operation: "discovery",
          timeoutMs: PRIME_RPC_DISCOVERY_TIMEOUT_MS,
        }),
      ),
  }),
);

const runPrimeVersionCommand = (primeSettings: PrimeSettings, environment: NodeJS.ProcessEnv) =>
  Effect.gen(function* () {
    const command = primeSettings.binaryPath || "prime-agent";
    const spawnCommand = yield* resolveSpawnCommand(command, ["--version"], {
      env: environment,
    });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });

export function buildInitialPrimeProviderSnapshot(
  primeSettings: PrimeSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = primeModelsFromSettings(primeSettings.customModels);
    if (!primeSettings.enabled) {
      return buildServerProvider({
        presentation: PRIME_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Prime is disabled in T3 Code settings.",
        },
      });
    }
    return buildServerProvider({
      presentation: PRIME_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking Prime CLI availability...",
      },
    });
  });
}

function expandHomePath(input: string, home: string, path: Path.Path): string {
  if (input === "~") return home;
  if (input.startsWith("~/") || input.startsWith("~\\")) return path.join(home, input.slice(2));
  return input;
}

/**
 * Credentials are detected without spawning Prime and without reading them:
 * a non-blank PRIME_API_KEY, else the existence of `<agentDir>/auth.json`.
 * A filesystem failure degrades to "unknown" rather than "unauthenticated".
 */
const detectPrimeAuth = Effect.fn("detectPrimeAuth")(function* (
  environment: NodeJS.ProcessEnv,
): Effect.fn.Return<ServerProviderAuth, never, FileSystem.FileSystem | Path.Path> {
  if (environment[PRIME_API_KEY_ENV]?.trim()) {
    return { status: "authenticated", type: "api_key", label: "Prime API key" };
  }
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const home = yield* HostProcess.HomeDirectory;
  const configuredAgentDir = environment[PRIME_AGENT_DIR_ENV]?.trim();
  const agentDir = configuredAgentDir
    ? expandHomePath(configuredAgentDir, home, path)
    : path.join(home, ".prime", "agent");
  const exists = yield* fileSystem.exists(path.join(agentDir, "auth.json")).pipe(Effect.result);
  if (Result.isFailure(exists)) return { status: "unknown" };
  return exists.success
    ? { status: "authenticated", type: "cached_token", label: "Prime account" }
    : { status: "unauthenticated" };
});

export const checkPrimeProviderStatus = Effect.fn("checkPrimeProviderStatus")(function* (
  primeSettings: PrimeSettings,
  environment: NodeJS.ProcessEnv = process.env,
  cwd?: string,
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = primeModelsFromSettings(primeSettings.customModels);
  const draft = (probe: Parameters<typeof buildServerProvider>[0]["probe"]) =>
    buildServerProvider({
      presentation: PRIME_PRESENTATION,
      enabled: primeSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe,
    });

  if (!primeSettings.enabled) {
    return draft({
      installed: false,
      version: null,
      status: "warning",
      auth: { status: "unknown" },
      message: "Prime is disabled in T3 Code settings.",
    });
  }

  const versionResult = yield* runPrimeVersionCommand(primeSettings, environment).pipe(
    Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionResult)) {
    const error = versionResult.failure;
    yield* Effect.logWarning("Prime CLI health check failed.", { errorTag: error._tag });
    return draft({
      installed: !isCommandMissingCause(error),
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: isCommandMissingCause(error)
        ? `Prime (\`prime-agent\`) is not installed or not on PATH. ${PRIME_INSTALL_GUIDANCE}`
        : "Failed to execute Prime CLI health check.",
    });
  }

  if (Option.isNone(versionResult.success)) {
    return draft({
      installed: true,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: "Prime CLI is installed but timed out while running `prime-agent --version`.",
    });
  }

  const versionOutput = versionResult.success.value;
  const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
  if (versionOutput.code !== 0) {
    return draft({
      installed: true,
      version,
      status: "error",
      auth: { status: "unknown" },
      message: "Prime CLI is installed but failed to run.",
    });
  }

  if (version === null) {
    return draft({
      installed: true,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: `T3 Code could not read the Prime version from \`prime-agent --version\`. Prime ${MINIMUM_PRIME_VERSION} or newer is required.`,
    });
  }

  if (compareSemverVersions(version, MINIMUM_PRIME_VERSION) < 0) {
    return draft({
      installed: true,
      version,
      status: "error",
      auth: { status: "unknown" },
      message: `Prime ${version} is unsupported. Update to Prime ${MINIMUM_PRIME_VERSION} or newer with \`prime-agent update\`.`,
    });
  }

  const auth = yield* detectPrimeAuth(environment);
  if (auth.status === "unauthenticated") {
    return draft({
      installed: true,
      version,
      status: "error",
      auth,
      // Prime signs in from its own TUI (`/login`); it has no login subcommand.
      message:
        "Prime is not signed in. Run `prime-agent` in a terminal and sign in with /login, or set PRIME_API_KEY in this instance's environment variables.",
    });
  }

  const resolvedLaunchArgs = resolvePrimeLaunchArgs(primeSettings.launchArgs);
  if (!resolvedLaunchArgs.ok) {
    return draft({
      installed: true,
      version,
      status: "error",
      auth,
      message: resolvedLaunchArgs.message,
    });
  }

  const discoveryExit = yield* discoverPrimeViaRpc(
    primeSettings,
    environment,
    resolvedLaunchArgs.args,
    cwd,
  ).pipe(Effect.timeoutOption(PRIME_RPC_DISCOVERY_TIMEOUT_MS), Effect.exit);
  // A non-ready status keeps the last-known models (ProviderRegistry retention),
  // so a failed or empty discovery must not report ready.
  if (Exit.isFailure(discoveryExit) || Option.isNone(discoveryExit.value)) {
    if (Exit.isFailure(discoveryExit)) {
      yield* Effect.logWarning("Prime RPC discovery failed.", {
        errorTag: causeErrorTag(discoveryExit.cause),
      });
    }
    return draft({
      installed: true,
      version,
      status: "warning",
      auth,
      message:
        "Prime is installed, but T3 Code could not load its models. The model list may be incomplete.",
    });
  }

  const discovery = discoveryExit.value.value;
  return buildServerProvider({
    presentation: PRIME_PRESENTATION,
    enabled: primeSettings.enabled,
    checkedAt,
    models: primeModelsFromSettings(primeSettings.customModels, discovery.models),
    slashCommands: discovery.slashCommands,
    skills: discovery.skills,
    probe: {
      installed: true,
      version,
      status: discovery.models.length > 0 ? "ready" : "warning",
      auth,
      ...(discovery.models.length > 0
        ? {}
        : { message: "Prime returned no models. Check the providers configured in Prime." }),
    },
  });
});

export const enrichPrimeSnapshot = (input: {
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  readonly enableProviderUpdateChecks?: boolean;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
}): Effect.Effect<
  void,
  never,
  HttpClient.HttpClient | ProviderLatestVersions.ProviderLatestVersions
> => {
  const { snapshot, publishSnapshot } = input;
  return enrichProviderSnapshotWithVersionAdvisory(snapshot, input.maintenanceCapabilities, {
    enableProviderUpdateChecks: input.enableProviderUpdateChecks,
  }).pipe(
    Effect.flatMap((enrichedSnapshot) => publishSnapshot(enrichedSnapshot)),
    Effect.catchCause((cause) =>
      Effect.logWarning("Prime version advisory enrichment failed", {
        errorTag: causeErrorTag(cause),
      }),
    ),
    Effect.asVoid,
  );
};
