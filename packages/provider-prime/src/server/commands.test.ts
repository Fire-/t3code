import { expect, it } from "@effect/vitest";

import {
  parsePrimeDiscoveredCommands,
  PRIME_SESSION_SLASH_COMMANDS,
  primeSessionCommandName,
  withPrimeBuiltinSlashCommands,
} from "./commands.ts";

it("maps current Prime skill metadata to T3's user and project skill scopes", () => {
  expect(
    parsePrimeDiscoveredCommands({
      commands: [
        {
          name: "skill:global-review",
          description: "Review changes.",
          source: "skill",
          sourceInfo: {
            path: "/home/test/.agents/skills/global-review/SKILL.md",
            scope: "user",
          },
        },
        {
          name: "skill:project-deploy",
          description: "Deploy this project.",
          source: "skill",
          sourceInfo: {
            path: "/workspace/.agents/skills/project-deploy/SKILL.md",
            scope: "project",
          },
        },
        { name: "hello", description: "Say hello.", source: "extension" },
      ],
    }),
  ).toEqual({
    skills: [
      {
        name: "global-review",
        description: "Review changes.",
        path: "/home/test/.agents/skills/global-review/SKILL.md",
        scope: "user",
        enabled: true,
      },
      {
        name: "project-deploy",
        description: "Deploy this project.",
        path: "/workspace/.agents/skills/project-deploy/SKILL.md",
        scope: "project",
        enabled: true,
      },
    ],
    slashCommands: [{ name: "hello", description: "Say hello." }],
  });
});

it("maps Prime global location and interface labels onto T3 skill fields", () => {
  expect(
    parsePrimeDiscoveredCommands({
      commands: [
        {
          name: "skill:global-review",
          description: "Review changes.",
          source: "skill",
          location: "global",
          interface: {
            displayName: "Global Review",
            shortDescription: "Review diffs.",
          },
        },
      ],
    }),
  ).toEqual({
    skills: [
      {
        name: "global-review",
        description: "Review changes.",
        path: "prime:skill:global-review",
        scope: "user",
        enabled: true,
        displayName: "Global Review",
        shortDescription: "Review diffs.",
      },
    ],
    slashCommands: [],
  });
});

it("[VAL-CMD-001] lists Prime's built-in session commands with descriptions ahead of get_commands entries, without duplicates", () => {
  const discovered = parsePrimeDiscoveredCommands({
    commands: [
      { name: "hello", description: "Say hello.", source: "extension" },
      { name: "review", description: "Review template.", source: "prompt" },
      { name: "skill:deploy", description: "Deploy.", source: "skill" },
      { name: "goal", description: "Extension goal.", source: "extension" },
      { name: "hello", description: "Second hello.", source: "extension" },
    ],
  });
  const commands = withPrimeBuiltinSlashCommands(discovered.slashCommands);
  expect(commands.map((command) => command.name)).toEqual([
    "compact",
    "refine",
    "goal",
    "autonomous",
    "reload",
    "hello",
    "review",
  ]);
  expect(commands.slice(0, 5)).toEqual(PRIME_SESSION_SLASH_COMMANDS);
  for (const command of PRIME_SESSION_SLASH_COMMANDS) {
    expect(command.description?.length ?? 0).toBeGreaterThan(0);
  }
  expect(commands.find((command) => command.name === "goal")?.input?.hint).toBe(
    "<objective|status|pause|resume|clear>",
  );
  expect(commands.find((command) => command.name === "autonomous")?.input?.hint).toBe(
    "on|off|status",
  );
  expect(commands.find((command) => command.name === "hello")?.description).toBe("Say hello.");
  expect(discovered.skills.map((skill) => skill.name)).toEqual(["deploy"]);
  expect(withPrimeBuiltinSlashCommands([])).toEqual(PRIME_SESSION_SLASH_COMMANDS);
});

it("recognizes Prime's session command names the way Prime parses slash text", () => {
  expect(primeSessionCommandName("/compact")).toBe("compact");
  expect(primeSessionCommandName("  /goal status ")).toBe("goal");
  expect(primeSessionCommandName("/goal ship the release")).toBe("goal");
  expect(primeSessionCommandName("/autonomous on --max-turns 3")).toBe("autonomous");
  expect(primeSessionCommandName("/compacted")).toBeUndefined();
  expect(primeSessionCommandName("/compact-now")).toBeUndefined();
  expect(primeSessionCommandName("please /compact")).toBeUndefined();
  expect(primeSessionCommandName("/goa")).toBeUndefined();
});
