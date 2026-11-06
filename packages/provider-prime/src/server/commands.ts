import { type ServerProviderSkill, type ServerProviderSlashCommand } from "@t3tools/contracts";
import * as Predicate from "effect/Predicate";

// Prime's built-in session commands (core/slash-commands.ts SESSION_SLASH_COMMAND_NAMES).
// get_commands omits them, and Prime parses them from prompt text into session actions.
export const PRIME_SESSION_SLASH_COMMANDS: ReadonlyArray<ServerProviderSlashCommand> = [
  {
    name: "compact",
    description: "Compact the session context; optional instructions focus the summary",
    input: { hint: "[instructions]" },
  },
  {
    name: "refine",
    description: "Refine continual harness prompt notes, skills, subagents, and memory",
  },
  {
    name: "goal",
    description: "Set or view a persistent goal; supports pause, resume, and clear",
    input: { hint: "<objective|status|pause|resume|clear>" },
  },
  {
    name: "autonomous",
    description: "Set or view autonomous mode with an optional budget",
    input: { hint: "on|off|status" },
  },
  {
    name: "reload",
    description: "Reload settings, auth, MCP servers, skills, prompts, and the runtime",
  },
];

const PRIME_SESSION_COMMAND_NAMES: ReadonlySet<string> = new Set(
  PRIME_SESSION_SLASH_COMMANDS.map((command) => command.name),
);

/** Mirrors Prime's parseSessionSlashCommand: a leading `/name`, then whitespace or the end. */
export function primeSessionCommandName(text: string): string | undefined {
  const name = /^\/(\S+)(?:\s|$)/u.exec(text.trim())?.[1];
  return name !== undefined && PRIME_SESSION_COMMAND_NAMES.has(name) ? name : undefined;
}

export function withPrimeBuiltinSlashCommands(
  commands: ReadonlyArray<ServerProviderSlashCommand>,
): ReadonlyArray<ServerProviderSlashCommand> {
  const seen = new Set(PRIME_SESSION_COMMAND_NAMES);
  const merged = [...PRIME_SESSION_SLASH_COMMANDS];
  for (const command of commands) {
    if (seen.has(command.name)) continue;
    seen.add(command.name);
    merged.push(command);
  }
  return merged;
}
export interface PrimeDiscoveredCommands {
  readonly slashCommands: ReadonlyArray<ServerProviderSlashCommand>;
  readonly skills: ReadonlyArray<ServerProviderSkill>;
}

function normalizePrimeSkillScope(scope: string | undefined): string | undefined {
  if (scope === undefined) return undefined;
  const normalized = scope.trim().toLowerCase();
  if (normalized === "global" || normalized === "personal") return "user";
  if (normalized === "workspace" || normalized === "local") return "project";
  return scope;
}

export function parsePrimeDiscoveredCommands(data: unknown): PrimeDiscoveredCommands {
  const commands = recordField(data, "commands");
  if (!Array.isArray(commands)) return { slashCommands: [], skills: [] };
  const slashCommands: Array<ServerProviderSlashCommand> = [];
  const skills: Array<ServerProviderSkill> = [];
  for (const command of commands) {
    const commandName = recordString(command, "name");
    if (commandName === undefined || commandName.length === 0) continue;
    const description = recordString(command, "description");
    if (recordString(command, "source") === "skill") {
      const name = commandName.startsWith("skill:")
        ? commandName.slice("skill:".length)
        : commandName;
      if (name.length === 0) continue;
      const sourceInfo = recordField(command, "sourceInfo");
      const commandInterface = recordField(command, "interface");
      const path =
        recordString(sourceInfo, "path") ?? recordString(command, "path") ?? `prime:skill:${name}`;
      const scope = normalizePrimeSkillScope(
        recordString(sourceInfo, "scope") ?? recordString(command, "location"),
      );
      const displayName =
        recordString(command, "displayName") ??
        recordString(sourceInfo, "displayName") ??
        recordString(commandInterface, "displayName");
      const shortDescription =
        recordString(command, "shortDescription") ??
        recordString(sourceInfo, "shortDescription") ??
        recordString(commandInterface, "shortDescription");
      skills.push({
        name,
        path,
        enabled: true,
        ...(description === undefined ? {} : { description }),
        ...(scope === undefined ? {} : { scope }),
        ...(displayName === undefined ? {} : { displayName }),
        ...(shortDescription === undefined ? {} : { shortDescription }),
      });
      continue;
    }
    slashCommands.push({
      name: commandName,
      ...(description === undefined ? {} : { description }),
    });
  }
  return { slashCommands, skills };
}

function recordField(input: unknown, key: string): unknown {
  return Predicate.isObject(input) ? input[key] : undefined;
}

function recordString(input: unknown, key: string): string | undefined {
  const value = recordField(input, key);
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}
