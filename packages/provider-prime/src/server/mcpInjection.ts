import { tokenizeCliArgs } from "@t3tools/shared/cliArgs";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";

import type { McpProviderSessionConfig } from "@t3tools/provider-core/server/mcpSession";
import {
  PRIME_T3_MCP_EXTENSION_FILENAME,
  PRIME_T3_MCP_EXTENSION_SOURCE,
  primeRuntimeMode,
  T3_MCP_BEARER_ENV,
  T3_MCP_URL_ENV,
  T3_PRIME_RUNTIME_MODE_ENV,
  T3_PRIME_MCP_EXTENSION_PATH_ENV,
} from "./mcpExtensionSource.ts";

// Prime's flag parser: packages/coding-agent/src/cli/args.ts. T3 owns the
// session (switch_session), the working directory (spawn cwd), and the daemon
// (Prime's default one, so a user-chosen socket cannot strand sessions).
const RESERVED_PRIME_LAUNCH_ARGUMENTS = new Set([
  "--continue",
  "-c",
  "--cwd",
  "--daemon-socket",
  "--export",
  "--fork",
  "--help",
  "-h",
  "--list-models",
  "--mode",
  "--no-session",
  "--print",
  "-p",
  "--resume",
  "-r",
  "--version",
  "-v",
]);

const PRIME_ARGUMENTS_WITH_VALUES = new Set([
  "--api-key",
  "--append-system-prompt",
  "--autonomous-gate",
  "--autonomous-gate-retries",
  "--autonomous-gate-timeout-ms",
  "--autonomous-max-continuations",
  "--autonomous-max-tokens",
  "--autonomous-max-turns",
  "--autonomous-timeout-ms",
  "--extension",
  "-e",
  "--goal",
  "--goal-token-budget",
  "--model",
  "--models",
  "--prompt-template",
  "--provider",
  "--session-dir",
  "--skill",
  "--system-prompt",
  "--theme",
  "--thinking",
  "--tools",
  "-t",
]);

const PRIME_ARGUMENTS_WITHOUT_VALUES = new Set([
  "--autonomous",
  "--no-builtin-tools",
  "-nbt",
  "--no-context-files",
  "-nc",
  "--no-extensions",
  "-ne",
  "--no-prompt-templates",
  "-np",
  "--no-skills",
  "-ns",
  "--no-themes",
  "--no-tools",
  "-nt",
  "--offline",
  "--verbose",
]);

export type PrimeLaunchArgsResolution =
  | { readonly ok: true; readonly args: ReadonlyArray<string> }
  | { readonly ok: false; readonly message: string };

function reservedPrimeArgument(arg: string): string | undefined {
  for (const reserved of RESERVED_PRIME_LAUNCH_ARGUMENTS) {
    if (arg === reserved || (reserved.startsWith("--") && arg.startsWith(`${reserved}=`))) {
      return reserved;
    }
  }
  return undefined;
}

function normalizePrimeBuiltInEqualsArguments(args: ReadonlyArray<string>): ReadonlyArray<string> {
  return args.flatMap((arg) => {
    const equalsIndex = arg.indexOf("=");
    if (equalsIndex <= 0) return [arg];
    const option = arg.slice(0, equalsIndex);
    return PRIME_ARGUMENTS_WITH_VALUES.has(option) ? [option, arg.slice(equalsIndex + 1)] : [arg];
  });
}

export function resolvePrimeLaunchArgs(launchArgs: string): PrimeLaunchArgsResolution {
  // Prime parses equals-form tokens only as extension flags, even when their name
  // matches a built-in option. Split known built-ins while leaving arbitrary
  // extension flags in their native form.
  const args = normalizePrimeBuiltInEqualsArguments(tokenizeCliArgs(launchArgs));
  let hasProvider = false;
  let hasModel = false;
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === undefined) continue;
    const reserved = reservedPrimeArgument(arg);
    if (reserved !== undefined) {
      return {
        ok: false,
        message: `Prime launch argument '${reserved}' is controlled by T3 Code and cannot be overridden.`,
      };
    }
    if (arg === "--") {
      return {
        ok: false,
        message: "Prime launch arguments cannot include positional prompts.",
      };
    }
    if (PRIME_ARGUMENTS_WITH_VALUES.has(arg)) {
      const value = args[index + 1];
      if (value === undefined) {
        return { ok: false, message: `Prime launch argument '${arg}' requires a value.` };
      }
      if (arg === "--provider") hasProvider = true;
      if (arg === "--model") hasModel = true;
      index += 1;
      continue;
    }
    if (PRIME_ARGUMENTS_WITHOUT_VALUES.has(arg) || (arg.startsWith("--") && arg.includes("="))) {
      continue;
    }
    if (arg.startsWith("--")) {
      // Prime extensions may register arbitrary long flags. Treat one following
      // non-flag token as that extension flag's value.
      if (
        args[index + 1] !== undefined &&
        !args[index + 1]!.startsWith("-") &&
        !args[index + 1]!.startsWith("@")
      ) {
        index += 1;
      }
      continue;
    }
    if (arg.startsWith("-")) {
      return { ok: false, message: `Prime launch argument '${arg}' is not supported by T3 Code.` };
    }
    return {
      ok: false,
      message: `Prime launch arguments cannot include positional prompt '${arg}'.`,
    };
  }
  if (hasProvider && !hasModel) {
    return { ok: false, message: "Prime launch argument '--provider' requires '--model'." };
  }
  return { ok: true, args };
}

function bearerTokenFromAuthorizationHeader(header: string): string {
  return header.startsWith("Bearer ") ? header.slice("Bearer ".length) : header;
}

function normalizedPrimePath(value: string): string {
  return value.replace(/\\/g, "/").replace(/\/+$/, "");
}

function hasExplicitExtension(args: ReadonlyArray<string>, extensionPath: string): boolean {
  const wanted = normalizedPrimePath(extensionPath);
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg !== "--extension" && arg !== "-e") continue;
    const configured = args[index + 1];
    if (configured !== undefined && normalizedPrimePath(configured) === wanted) return true;
    index += 1;
  }
  return false;
}

function withoutExplicitExtensions(args: ReadonlyArray<string>): ReadonlyArray<string> {
  const filtered: string[] = [];
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === undefined) continue;
    if (arg === "--extension" || arg === "-e") {
      index += 1;
      continue;
    }
    if (arg.startsWith("--extension=") || arg.startsWith("-e=")) continue;
    filtered.push(arg);
  }
  return filtered;
}

function withoutToolSelectionArgs(args: ReadonlyArray<string>): ReadonlyArray<string> {
  const filtered: string[] = [];
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === undefined) continue;
    if (arg === "--tools" || arg === "-t") {
      index += 1;
      continue;
    }
    if (
      arg === "--no-tools" ||
      arg === "-nt" ||
      arg === "--no-builtin-tools" ||
      arg === "-nbt" ||
      arg.startsWith("--tools=") ||
      arg.startsWith("-t=")
    ) {
      continue;
    }
    filtered.push(arg);
  }
  return filtered;
}

function primeT3McpExtensionDestPath(cacheDir: string): string {
  return `${cacheDir.replace(/\\/g, "/")}/${PRIME_T3_MCP_EXTENSION_FILENAME}`;
}

export const materializePrimeT3McpExtension = Effect.fn("materializePrimeT3McpExtension")(
  function* (cacheDir: string) {
    const fs = yield* FileSystem.FileSystem;
    yield* fs.makeDirectory(cacheDir, { recursive: true });
    const dest = primeT3McpExtensionDestPath(cacheDir);
    const existing = yield* fs.readFileString(dest).pipe(Effect.orElseSucceed(() => ""));
    if (existing !== PRIME_T3_MCP_EXTENSION_SOURCE) {
      yield* fs.writeFileString(dest, PRIME_T3_MCP_EXTENSION_SOURCE);
    }
    return dest;
  },
);

export function buildPrimeRpcLaunch(input: {
  readonly launchArgs: ReadonlyArray<string>;
  readonly environment: NodeJS.ProcessEnv;
  readonly mcpSession: McpProviderSessionConfig | undefined;
  readonly extensionPath: string | undefined;
  readonly ephemeral?: boolean;
  readonly disableExtensions?: boolean;
  readonly disableTools?: boolean;
  readonly runtimeMode?: "approval-required" | "auto-accept-edits" | "auto" | "full-access";
}): {
  readonly args: ReadonlyArray<string>;
  readonly env: NodeJS.ProcessEnv;
  readonly hasT3Mcp: boolean;
} {
  const hasT3Extension = input.disableExtensions !== true && input.extensionPath !== undefined;
  const hasT3Mcp = hasT3Extension && input.mcpSession !== undefined;
  const extensionSafeArgs =
    input.disableExtensions === true
      ? withoutExplicitExtensions(input.launchArgs)
      : input.launchArgs;
  const launchArgs =
    input.disableTools === true ? withoutToolSelectionArgs(extensionSafeArgs) : extensionSafeArgs;
  const args = [
    "--mode",
    "rpc",
    ...(input.ephemeral === true ? ["--no-session"] : []),
    ...launchArgs,
    // Restrictions follow user launch args so a configured --tools or
    // --extension cannot silently re-enable unattended text-generation code.
    ...(input.disableExtensions === true ? ["--no-extensions"] : []),
    ...(input.disableTools === true ? ["--no-tools"] : []),
  ];
  if (
    hasT3Extension &&
    input.extensionPath !== undefined &&
    !hasExplicitExtension(args, input.extensionPath)
  ) {
    args.push("-e", input.extensionPath);
  }
  const environment = { ...input.environment };
  // These values belong to the current T3 session. Never let a Prime child reuse
  // credentials inherited from the server or a parent provider process.
  delete environment[T3_MCP_URL_ENV];
  delete environment[T3_MCP_BEARER_ENV];
  delete environment[T3_PRIME_MCP_EXTENSION_PATH_ENV];
  delete environment[T3_PRIME_RUNTIME_MODE_ENV];

  return {
    args,
    env: {
      ...environment,
      ...(hasT3Extension && input.extensionPath !== undefined
        ? { [T3_PRIME_MCP_EXTENSION_PATH_ENV]: input.extensionPath }
        : {}),
      ...(hasT3Extension && input.runtimeMode !== undefined
        ? { [T3_PRIME_RUNTIME_MODE_ENV]: primeRuntimeMode(input.runtimeMode) }
        : {}),
      ...(hasT3Mcp && input.mcpSession !== undefined
        ? {
            [T3_MCP_URL_ENV]: input.mcpSession.endpoint,
            [T3_MCP_BEARER_ENV]: bearerTokenFromAuthorizationHeader(
              input.mcpSession.authorizationHeader,
            ),
          }
        : {}),
    },
    hasT3Mcp,
  };
}
