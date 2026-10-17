import {
  type CustomModelSetting,
  type ServerProvider,
  type ServerProviderModel,
} from "@t3tools/contracts";
import type { PrimeSettings } from "../settings.ts";
import { causeErrorTag } from "@t3tools/shared/observability";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import { compareSemverVersions } from "@t3tools/shared/semver";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
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
 * get_entries arrived in 0.80.3 and agent_settled landed in source at 0.80.4.
 * Version 0.80.5 was the first published package containing both hooks.
 */
export const MINIMUM_PRIME_VERSION = "0.80.5";

const PRIME_DEFAULT_MODEL: ServerProviderModel = {
  slug: "default",
  name: "Prime default",
  isCustom: false,
  capabilities: EMPTY_PRIME_MODEL_CAPABILITIES,
};

interface PrimeDiscovery extends PrimeDiscoveredCommands {
  readonly models: ReadonlyArray<ServerProviderModel>;
  readonly authenticated: boolean;
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
  const parsed: Array<ServerProviderModel> = [];
  for (const model of models) {
    const provider = recordString(model, "provider");
    const id = recordString(model, "id");
    if (provider === undefined || id === undefined) continue;
    const slug = `${provider}/${id}`;
    if (seen.has(slug)) continue;
    seen.add(slug);
    parsed.push({
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
    });
  }
  return parsed;
}

const makePrimeDiscoveryConnection = Effect.fnUntraced(function* (
  primeSettings: PrimeSettings,
  environment: NodeJS.ProcessEnv,
  launchArgs: ReadonlyArray<string>,
  cwd?: string,
) {
  const launch = buildPrimeRpcLaunch({
    launchArgs,
    environment,
    mcpSession: undefined,
    extensionPath: undefined,
    ephemeral: true,
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
    const connection = yield* makePrimeDiscoveryConnection(
      primeSettings,
      environment,
      launchArgs,
      cwd,
    );
    const stateData = yield* connection.request({ type: "get_state" });
    const modelsData = yield* connection.request({ type: "get_available_models" });
    const commandsData = yield* connection
      .request({ type: "get_commands" })
      .pipe(Effect.orElseSucceed(() => undefined));
    const discoveredModels = parseDiscoveredModels(modelsData, stateData);
    const { slashCommands, skills } = parsePrimeDiscoveredCommands(commandsData);
    return {
      models: discoveredModels,
      slashCommands: withPrimeBuiltinSlashCommands(slashCommands),
      skills,
      authenticated: discoveredModels.length > 0,
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

export const checkPrimeProviderStatus = Effect.fn("checkPrimeProviderStatus")(function* (
  primeSettings: PrimeSettings,
  environment: NodeJS.ProcessEnv = process.env,
  cwd?: string,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = primeModelsFromSettings(primeSettings.customModels);

  if (!primeSettings.enabled) {
    return buildServerProvider({
      presentation: PRIME_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Prime is disabled in T3 Code settings.",
      },
    });
  }

  const versionResult = yield* runPrimeVersionCommand(primeSettings, environment).pipe(
    Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionResult)) {
    const error = versionResult.failure;
    yield* Effect.logWarning("Prime CLI health check failed.", { errorTag: error._tag });
    return buildServerProvider({
      presentation: PRIME_PRESENTATION,
      enabled: primeSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(error)
          ? "Prime CLI (`prime-agent`) is not installed or not on PATH. Install with `npm install -g --ignore-scripts @earendil-works/pi-coding-agent`."
          : "Failed to execute Prime CLI health check.",
      },
    });
  }

  if (Option.isNone(versionResult.success)) {
    return buildServerProvider({
      presentation: PRIME_PRESENTATION,
      enabled: primeSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "Prime CLI is installed but timed out while running `prime-agent --version`.",
      },
    });
  }

  const versionOutput = versionResult.success.value;
  const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
  if (versionOutput.code !== 0) {
    return buildServerProvider({
      presentation: PRIME_PRESENTATION,
      enabled: primeSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: "Prime CLI is installed but failed to run.",
      },
    });
  }

  if (version === null) {
    return buildServerProvider({
      presentation: PRIME_PRESENTATION,
      enabled: primeSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: `T3 Code could not determine the Prime version. Prime ${MINIMUM_PRIME_VERSION} or newer is required.`,
      },
    });
  }

  if (compareSemverVersions(version, MINIMUM_PRIME_VERSION) < 0) {
    return buildServerProvider({
      presentation: PRIME_PRESENTATION,
      enabled: primeSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: `Prime ${version} is unsupported. Update to Prime ${MINIMUM_PRIME_VERSION} or newer.`,
      },
    });
  }

  const resolvedLaunchArgs = resolvePrimeLaunchArgs(primeSettings.launchArgs);
  if (!resolvedLaunchArgs.ok) {
    return buildServerProvider({
      presentation: PRIME_PRESENTATION,
      enabled: primeSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: resolvedLaunchArgs.message,
      },
    });
  }

  const discoveryExit = yield* discoverPrimeViaRpc(
    primeSettings,
    environment,
    resolvedLaunchArgs.args,
    cwd,
  ).pipe(Effect.timeoutOption(PRIME_RPC_DISCOVERY_TIMEOUT_MS), Effect.exit);
  if (Exit.isFailure(discoveryExit)) {
    yield* Effect.logWarning("Prime RPC discovery failed.", {
      errorTag: causeErrorTag(discoveryExit.cause),
    });
    return buildServerProvider({
      presentation: PRIME_PRESENTATION,
      enabled: primeSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "ready",
        auth: { status: "unknown" },
        message:
          "Prime is available, but T3 Code could not refresh its models and commands. The live session will retry startup.",
      },
    });
  }
  if (Option.isNone(discoveryExit.value)) {
    return buildServerProvider({
      presentation: PRIME_PRESENTATION,
      enabled: primeSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "ready",
        auth: { status: "unknown" },
        message:
          "Prime is available, but model and command discovery needs interactive input. The live session will handle it.",
      },
    });
  }

  const discovery = discoveryExit.value.value;
  const models = primeModelsFromSettings(primeSettings.customModels, discovery.models);
  return buildServerProvider({
    presentation: PRIME_PRESENTATION,
    enabled: primeSettings.enabled,
    checkedAt,
    models,
    slashCommands: discovery.slashCommands,
    skills: discovery.skills,
    probe: {
      installed: true,
      version,
      status: discovery.authenticated ? "ready" : "warning",
      auth: {
        status: discovery.authenticated ? "authenticated" : "unauthenticated",
        type: "prime-agent",
      },
      ...(discovery.authenticated
        ? {}
        : {
            message:
              "Prime has no usable models. Run `prime-agent` in a terminal and use /login, or configure an API key in ~/.prime/agent.",
          }),
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
