import { expect, it } from "@effect/vitest";

import {
  parsePrimeCompactCommand,
  parsePrimeDiscoveredCommands,
  PRIME_COMPACT_SLASH_COMMAND,
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

it("parses a standalone /compact command and optional instructions", () => {
  expect(parsePrimeCompactCommand("/compact")).toEqual({});
  expect(parsePrimeCompactCommand("  /compact  ")).toEqual({});
  expect(parsePrimeCompactCommand("/compact keep the auth rewrite")).toEqual({
    customInstructions: "keep the auth rewrite",
  });
  expect(parsePrimeCompactCommand("/compacted")).toBeNull();
  expect(parsePrimeCompactCommand("/compact-now")).toBeNull();
  expect(parsePrimeCompactCommand("please /compact")).toBeNull();
});

it("prepends the builtin compact command without duplicating a discovered one", () => {
  expect(withPrimeBuiltinSlashCommands([{ name: "hello", description: "Say hello." }])).toEqual([
    PRIME_COMPACT_SLASH_COMMAND,
    { name: "hello", description: "Say hello." },
  ]);
  expect(
    withPrimeBuiltinSlashCommands([
      { name: "compact", description: "Extension compact." },
      { name: "hello" },
    ]),
  ).toEqual([PRIME_COMPACT_SLASH_COMMAND, { name: "hello" }]);
});
