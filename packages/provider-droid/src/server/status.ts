import {
  DEFAULT_MODEL_BY_PROVIDER,
  ProviderDriverKind,
  type CustomModelSetting,
  type ModelCapabilities,
  type ServerProvider,
  type ServerProviderModel,
} from "@t3tools/contracts";
import type { DroidSettings } from "../settings.ts";
import { createModelCapabilities } from "@t3tools/shared/model";
import { causeErrorTag } from "@t3tools/shared/observability";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as HttpClient from "effect/http/HttpClient";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import {
  buildSelectOptionDescriptor,
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

import { makeDroidRpcRuntime } from "./rpc.ts";

export const DROID_PRESENTATION = {
  displayName: "Droid",
  showInteractionModeToggle: false,
  supportedRuntimeModes: ["approval-required", "auto-accept-edits", "auto", "full-access"],
  reportsContextWindow: true,
  requiresNewThreadForModelChange: false,
  supportsConversationRollback: false,
} as const;

const VERSION_PROBE_TIMEOUT_MS = 4_000;
// Booting Droid in RPC mode and reading its model registry; the same bound Pi uses for discovery.
export const DROID_DISCOVERY_TIMEOUT_MS = 15_000;

const DROID_DEFAULT_MODEL_SLUG = DEFAULT_MODEL_BY_PROVIDER[ProviderDriverKind.make("droid")];
/** The option id the adapter reads when it opens a session. */
const REASONING_EFFORT_OPTION_ID = "reasoningEffort";

const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({ optionDescriptors: [] });

/** Group labels Droid's own model picker uses for each `modelProvider`. */
const MODEL_PROVIDER_LABELS = new Map([
  ["anthropic", "Anthropic"],
  ["openai", "OpenAI"],
  ["google", "Google"],
  ["factory", "Droid Core"],
  ["xai", "xAI"],
]);

type ListedModel = { readonly [key: string]: unknown };

const capitalize = (value: string) => value.charAt(0).toUpperCase() + value.slice(1);

function modelGroup(entry: ListedModel): string | undefined {
  // The router spans every provider, so it belongs to no group.
  if (entry.kind === "router" || !Predicate.isString(entry.modelProvider)) return undefined;
  const provider = entry.modelProvider.trim();
  if (provider === "") return undefined;
  return MODEL_PROVIDER_LABELS.get(provider.toLowerCase()) ?? capitalize(provider);
}

/**
 * The credit multiplier and promo Droid shows beside each model, e.g. `0.272x, 50% off`.
 * The router bills at whichever model it routes to, so its listed multiplier is skipped.
 */
function pricingLabel(entry: ListedModel): string | undefined {
  const parts: Array<string> = [];
  const multiplier = entry.tokenMultiplier;
  if (
    entry.kind !== "router" &&
    Predicate.isNumber(multiplier) &&
    Number.isFinite(multiplier) &&
    multiplier >= 0
  ) {
    // Four significant digits drop float noise such as 0.30000000000000004.
    parts.push(`${Number(multiplier.toPrecision(4))}x`);
  }
  if (Predicate.isString(entry.promoLabel) && entry.promoLabel.trim() !== "") {
    parts.push(entry.promoLabel.trim());
  }
  return parts.length > 0 ? parts.join(", ") : undefined;
}

function reasoningCapabilities(entry: ListedModel): ModelCapabilities {
  // The router picks its own effort, so there is nothing for the user to choose.
  if (entry.kind === "router" || !Array.isArray(entry.supportedReasoningEfforts)) {
    return EMPTY_CAPABILITIES;
  }
  const efforts = entry.supportedReasoningEfforts.filter(Predicate.isString);
  if (efforts.length === 0) return EMPTY_CAPABILITIES;
  const defaultEffort =
    Predicate.isString(entry.defaultReasoningEffort) &&
    efforts.includes(entry.defaultReasoningEffort)
      ? entry.defaultReasoningEffort
      : efforts[0];
  return createModelCapabilities({
    optionDescriptors: [
      buildSelectOptionDescriptor({
        id: REASONING_EFFORT_OPTION_ID,
        label: "Reasoning",
        options: efforts.map((effort) => ({
          value: effort,
          label: capitalize(effort),
          isDefault: effort === defaultEffort,
        })),
      }),
    ],
  });
}

/**
 * Maps a `droid.list_models` result into provider models. Ids pass through unchanged,
 * deprecated and disabled entries drop out, names carry Droid's credit multiplier and promo,
 * and each model's provider becomes its `subProvider` group. One unreadable entry is skipped
 * without losing the rest; a body that is not a model list maps to no models.
 */
export function buildDroidModelsFromListModels(
  result: unknown,
): ReadonlyArray<ServerProviderModel> {
  const entries: ReadonlyArray<unknown> =
    Predicate.isObject(result) && Array.isArray(result.models) ? result.models : [];
  const seen = new Set<string>();
  return entries.flatMap((entry): Array<ServerProviderModel> => {
    if (!Predicate.isObject(entry) || !Predicate.isString(entry.id)) return [];
    const slug = entry.id;
    if (slug.trim() === "" || seen.has(slug)) return [];
    if (entry.deprecated === true || entry.disabled === true) return [];
    seen.add(slug);
    const displayName = (Predicate.isString(entry.displayName) && entry.displayName.trim()) || slug;
    const pricing = pricingLabel(entry);
    const subProvider = modelGroup(entry);
    return [
      {
        slug,
        name: pricing ? `${displayName} (${pricing})` : displayName,
        ...(subProvider ? { subProvider } : {}),
        isCustom: false,
        ...(slug === DROID_DEFAULT_MODEL_SLUG ? { isDefault: true } : {}),
        capabilities: reasoningCapabilities(entry),
      },
    ];
  });
}

function droidModelsFromSettings(
  customModels: ReadonlyArray<CustomModelSetting> | undefined,
  discovered: ReadonlyArray<ServerProviderModel> = [],
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(discovered, customModels ?? [], EMPTY_CAPABILITIES);
}

export const definedDroidEnvironment = (environment: NodeJS.ProcessEnv): Record<string, string> =>
  Object.fromEntries(
    Object.entries(environment).filter(
      (entry): entry is [string, string] => typeof entry[1] === "string",
    ),
  );

/**
 * Boots Droid once in RPC mode and reads its model registry. The cwd is a fresh temp
 * directory, never a project checkout, so no workspace MCP servers or hooks start; the
 * runtime adds the hooks-off settings file. Only `droid.list_models` goes over the wire, then
 * closing the scope ends the child. That is deliberate: `droid.close_session` is a session
 * method and no session exists. It never fails: an error, a malformed or empty result, or a
 * timeout comes back as a short reason that is safe to log.
 */
const discoverDroidModels = (
  droidSettings: DroidSettings,
  environment: NodeJS.ProcessEnv,
): Effect.Effect<
  Result.Result<ReadonlyArray<ServerProviderModel>, string>,
  never,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
> =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const neutralCwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-droid-probe-cwd-" });
    const runtime = yield* makeDroidRpcRuntime({
      binaryPath: droidSettings.binaryPath || "droid",
      cwd: neutralCwd,
      env: definedDroidEnvironment(environment),
    });
    return buildDroidModelsFromListModels(yield* runtime.request("droid.list_models", {}));
  }).pipe(
    Effect.scoped,
    Effect.timeoutOption(DROID_DISCOVERY_TIMEOUT_MS),
    Effect.map(
      Option.match({
        onNone: () => Result.fail("timed out"),
        onSome: (models) =>
          models.length === 0 ? Result.fail("returned no models") : Result.succeed(models),
      }),
    ),
    Effect.catchCause((cause) => Effect.succeed(Result.fail(causeErrorTag(cause)))),
  );

/**
 * Best-effort credential hint. It looks at two things only: whether `FACTORY_API_KEY` is
 * non-blank, and whether a listing of `~/.factory` has an entry named `auth*`. No file in
 * there is ever opened, read, copied, or logged.
 */
export const detectDroidAuthHint = Effect.fn("detectDroidAuthHint")(function* (
  environment: NodeJS.ProcessEnv,
): Effect.fn.Return<"api-key" | "auth-file" | undefined, never, FileSystem.FileSystem | Path.Path> {
  if (environment["FACTORY_API_KEY"]?.trim()) return "api-key";
  const home = environment["HOME"]?.trim() || environment["USERPROFILE"]?.trim();
  if (!home) return undefined;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const entries = yield* fileSystem.readDirectory(path.join(home, ".factory")).pipe(Effect.result);
  if (Result.isFailure(entries)) return undefined;
  return entries.success.some((name) => name.startsWith("auth")) ? "auth-file" : undefined;
});

const authHintMessage = (hint: "api-key" | "auth-file" | undefined): string =>
  hint === "api-key"
    ? " FACTORY_API_KEY is set."
    : hint === "auth-file"
      ? " Droid credentials found under ~/.factory."
      : " Set FACTORY_API_KEY or run `droid` once to sign in.";

const runDroidVersionCommand = (droidSettings: DroidSettings, environment: NodeJS.ProcessEnv) =>
  Effect.gen(function* () {
    const command = droidSettings.binaryPath || "droid";
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

const unknownAuth = { status: "unknown" } as const;

/**
 * `droid --version` and model discovery cannot see a rejected login, so a turn that ends
 * `model_authentication_failed` is the only evidence. The status stays usable: the picker keeps
 * Droid and the next completed turn clears this.
 */
export const withDroidAuthFailure = (draft: ServerProviderDraft): ServerProviderDraft =>
  !draft.enabled || !draft.installed || draft.status === "error"
    ? draft
    : {
        ...draft,
        auth: { status: "unauthenticated" },
        message:
          "Droid could not authenticate its last turn. Set FACTORY_API_KEY or run `droid` once to sign in.",
      };

export function buildInitialDroidProviderSnapshot(
  droidSettings: DroidSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = droidModelsFromSettings(droidSettings.customModels);
    if (!droidSettings.enabled) {
      return buildServerProvider({
        presentation: DROID_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: unknownAuth,
          message: "Droid is disabled in T3 Code settings.",
        },
      });
    }
    return buildServerProvider({
      presentation: DROID_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: unknownAuth,
        message: "Checking Droid CLI availability...",
      },
    });
  });
}

export const checkDroidProviderStatus = Effect.fn("checkDroidProviderStatus")(function* (
  droidSettings: DroidSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = droidModelsFromSettings(droidSettings.customModels);
  const build = (
    probe: Parameters<typeof buildServerProvider>[0]["probe"],
    models: ReadonlyArray<ServerProviderModel> = fallbackModels,
  ) =>
    buildServerProvider({
      presentation: DROID_PRESENTATION,
      enabled: droidSettings.enabled,
      checkedAt,
      models,
      probe,
    });

  if (!droidSettings.enabled) {
    return build({
      installed: false,
      version: null,
      status: "warning",
      auth: unknownAuth,
      message: "Droid is disabled in T3 Code settings.",
    });
  }

  const versionResult = yield* runDroidVersionCommand(droidSettings, environment).pipe(
    Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionResult)) {
    const error = versionResult.failure;
    yield* Effect.logWarning("Droid CLI health check failed.", { errorTag: error._tag });
    return build({
      installed: !isCommandMissingCause(error),
      version: null,
      status: "error",
      auth: unknownAuth,
      message: isCommandMissingCause(error)
        ? "Droid CLI (`droid`) is not installed or not on PATH."
        : "Failed to execute Droid CLI health check.",
    });
  }

  if (Option.isNone(versionResult.success)) {
    return build({
      installed: true,
      version: null,
      status: "error",
      auth: unknownAuth,
      message: "Droid CLI is installed but timed out while running `droid --version`.",
    });
  }

  const versionOutput = versionResult.success.value;
  const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
  if (versionOutput.code !== 0) {
    return build({
      installed: true,
      version,
      status: "error",
      auth: unknownAuth,
      message: "Droid CLI is installed but failed to run.",
    });
  }

  if (version === null) {
    return build({
      installed: true,
      version: null,
      status: "error",
      auth: unknownAuth,
      message: "T3 Code could not determine the Droid version.",
    });
  }

  // A failed discovery degrades the model picker; it must not make chats fail, so it is a
  // warning and the registry keeps the last known list.
  const discovered = yield* discoverDroidModels(droidSettings, environment);
  if (Result.isFailure(discovered)) {
    yield* Effect.logWarning("Droid model discovery failed.", { reason: discovered.failure });
  }
  const authSuffix = authHintMessage(yield* detectDroidAuthHint(environment));
  return Result.isSuccess(discovered)
    ? build(
        {
          installed: true,
          version,
          status: "ready",
          auth: unknownAuth,
          message: `Droid CLI is ready.${authSuffix}`,
        },
        droidModelsFromSettings(droidSettings.customModels, discovered.success),
      )
    : build({
        installed: true,
        version,
        status: "warning",
        auth: unknownAuth,
        message: `Droid CLI is installed but model discovery failed or timed out. The model list may be incomplete.${authSuffix}`,
      });
});

export const enrichDroidSnapshot = (input: {
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  readonly enableProviderUpdateChecks?: boolean;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
}): Effect.Effect<
  void,
  never,
  HttpClient.HttpClient | ProviderLatestVersions.ProviderLatestVersions
> =>
  enrichProviderSnapshotWithVersionAdvisory(input.snapshot, input.maintenanceCapabilities, {
    enableProviderUpdateChecks: input.enableProviderUpdateChecks,
  }).pipe(
    Effect.flatMap((enrichedSnapshot) => input.publishSnapshot(enrichedSnapshot)),
    Effect.catchCause((cause) =>
      Effect.logWarning("Droid version advisory enrichment failed", {
        errorTag: causeErrorTag(cause),
      }),
    ),
    Effect.asVoid,
  );
