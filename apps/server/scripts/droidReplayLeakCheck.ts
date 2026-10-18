import type { ProviderReplayEntry } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";

export const REJECTED_TRANSCRIPT_DIRECTORY = "/tmp/droid-v2-rejected";

/** The personal instruction files Droid injects into every session, relative to the home directory. */
const GLOBAL_INSTRUCTION_FILES = [".agents/AGENTS.md", ".factory/AGENTS.md", ".claude/CLAUDE.md"];
/** Shorter lines (headings, list stubs) are not distinctive enough to prove a copy. */
const MIN_INSTRUCTION_LINE_CHARS = 20;
/** Shorter values would match inside ordinary words. */
const MIN_API_KEY_CHARS = 8;
const MIN_HOST_NAME_CHARS = 4;

export interface LeakCheck {
  readonly category: string;
  readonly matches: (text: string) => boolean;
}

export interface TranscriptLeak {
  readonly category: string;
  /** One-based line of the NDJSON transcript, where line 1 is the header. */
  readonly line: number;
}

export interface LeakCheckInput {
  readonly home: string;
  readonly userName: string;
  readonly hostNames: ReadonlyArray<string>;
  readonly hostIds: ReadonlyArray<string>;
  /** Recording-only directories that must have become placeholders. */
  readonly tempDirs: ReadonlyArray<string>;
  /** Text of the global instruction files; only its distinctive lines are kept, in memory. */
  readonly instructionTexts: ReadonlyArray<string>;
  /** `FACTORY_API_KEY` of the recorder's environment, when set. */
  readonly apiKey: string | undefined;
}

const escapeRegExp = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");

const collapseWhitespace = (value: string): string => value.replace(/\s+/gu, " ");

const containsAny = (category: string, values: ReadonlyArray<string>): LeakCheck => {
  const needles = values.filter((value) => value.length > 0);
  return { category, matches: (text) => needles.some((needle) => text.includes(needle)) };
};

const matchesPattern = (category: string, pattern: RegExp): LeakCheck => ({
  category,
  matches: (text) => pattern.test(text),
});

function instructionLines(texts: ReadonlyArray<string>): ReadonlyArray<string> {
  const lines = texts.flatMap((text) =>
    text
      .split(/\r?\n/u)
      .map((line) => collapseWhitespace(line.trim()))
      .filter((line) => line.length >= MIN_INSTRUCTION_LINE_CHARS),
  );
  return [...new Set(lines)];
}

/**
 * The checks only ever hold their needles in closures: nothing here is serializable, so no
 * code path can print or persist the canary list.
 */
export function buildLeakChecks(input: LeakCheckInput): ReadonlyArray<LeakCheck> {
  const lines = instructionLines(input.instructionTexts);
  const apiKey = input.apiKey?.trim() ?? "";
  const userName = input.userName.trim();
  return [
    {
      category: "home path",
      matches: (text) =>
        text.includes("/Users/") ||
        text.includes("/home/") ||
        (input.home.length > 1 && text.includes(input.home)),
    },
    ...(userName.length === 0
      ? []
      : [matchesPattern("user name", new RegExp(`\\b${escapeRegExp(userName)}\\b`, "u"))]),
    containsAny(
      "host name",
      input.hostNames.filter((name) => name.length >= MIN_HOST_NAME_CHARS),
    ),
    containsAny("host id", input.hostIds),
    containsAny("Droid home path", [".factory/"]),
    containsAny("agents home path", [".agents/"]),
    containsAny("system temp path", ["/var/folders", "/private/tmp", "/private/var"]),
    containsAny("recording directory", input.tempDirs),
    {
      category: "global instruction line",
      matches: (text) => {
        const collapsed = collapseWhitespace(text);
        return lines.some((line) => collapsed.includes(line));
      },
    },
    apiKey.length >= MIN_API_KEY_CHARS
      ? containsAny("api key", [apiKey])
      : { category: "api key", matches: () => false },
  ];
}

function walkStrings(value: unknown, visit: (text: string) => void): void {
  if (Predicate.isString(value)) {
    visit(value);
  } else if (Array.isArray(value)) {
    for (const entry of value) walkStrings(entry, visit);
  } else if (Predicate.isObject(value)) {
    for (const [key, entry] of Object.entries(value)) {
      visit(key);
      walkStrings(entry, visit);
    }
  }
}

interface TextPiece {
  /** Pieces with the same key belong to one assistant message or reasoning block set. */
  readonly stream: string;
  readonly text: string;
}

const field = (value: unknown, key: string): unknown =>
  Predicate.isObject(value) ? value[key] : undefined;

/** The streamed pieces a frame adds to an assistant message's text or reasoning. */
function streamPieces(frame: unknown): ReadonlyArray<TextPiece> {
  const notification = field(field(frame, "params"), "notification");
  const type = field(notification, "type");
  const kind =
    type === "assistant_text_delta" ? "text" : type === "thinking_text_delta" ? "reasoning" : null;
  const text = field(notification, "textDelta");
  if (kind === null || !Predicate.isString(text)) return [];
  const messageId = field(notification, "messageId");
  return [{ stream: `${kind}:${Predicate.isString(messageId) ? messageId : ""}`, text }];
}

/** The text and reasoning of every complete message inside a frame, each block set joined. */
function messageTexts(value: unknown, into: Array<string> = []): ReadonlyArray<string> {
  if (Array.isArray(value)) {
    for (const entry of value) messageTexts(entry, into);
  } else if (Predicate.isObject(value)) {
    const content = value["content"];
    if (Array.isArray(content)) {
      for (const [type, key] of [
        ["text", "text"],
        ["thinking", "thinking"],
      ] as const) {
        const blocks = content.flatMap((block) =>
          field(block, "type") === type && Predicate.isString(field(block, key))
            ? [String(field(block, key))]
            : [],
        );
        if (blocks.length > 1) into.push(blocks.join(""));
      }
    }
    for (const entry of Object.values(value)) messageTexts(entry, into);
  }
  return into;
}

/**
 * Scans every transcript line, every string in it, and the joined text of each assistant message
 * and reasoning stream, so a canary split across deltas or blocks is still found. It reports
 * categories and line numbers, never the matched text.
 */
export function findTranscriptLeaks(
  entries: ReadonlyArray<ProviderReplayEntry>,
  checks: ReadonlyArray<LeakCheck>,
): ReadonlyArray<TranscriptLeak> {
  const leaks = new Map<string, TranscriptLeak>();
  const streams = new Map<string, string>();
  const reportedStreams = new Set<string>();
  const report = (category: string, line: number) =>
    leaks.set(`${category}:${line}`, { category, line });
  const scan = (text: string, line: number) => {
    for (const check of checks) if (check.matches(text)) report(check.category, line);
  };

  for (const [index, entry] of entries.entries()) {
    const line = index + 2;
    scan(JSON.stringify(entry), line);
    if (entry.type === "runtime_exit") continue;
    walkStrings(entry.frame, (text) => scan(text, line));
    for (const text of messageTexts(entry.frame)) scan(text, line);
    for (const piece of streamPieces(entry.frame)) {
      const joined = (streams.get(piece.stream) ?? "") + piece.text;
      streams.set(piece.stream, joined);
      for (const check of checks) {
        const reported = `${piece.stream}|${check.category}`;
        if (reportedStreams.has(reported) || !check.matches(joined)) continue;
        reportedStreams.add(reported);
        report(check.category, line);
      }
    }
  }
  return [...leaks.values()].toSorted(
    (left, right) => left.line - right.line || left.category.localeCompare(right.category),
  );
}

export const describeLeaks = (leaks: ReadonlyArray<TranscriptLeak>): string =>
  leaks.map((leak) => `${leak.category} (transcript line ${leak.line})`).join(", ");

/** Reads the global instruction files that exist; a missing or unreadable one is skipped. */
export const readGlobalInstructionTexts = (home: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const texts: Array<string> = [];
    for (const relative of GLOBAL_INSTRUCTION_FILES) {
      const text = yield* fs.readFileString(path.join(home, relative)).pipe(Effect.option);
      if (text._tag === "Some" && !texts.includes(text.value)) texts.push(text.value);
    }
    return texts;
  });

/**
 * A rejected transcript still holds the leaked text, so it stays under the system temp directory,
 * never in the worktree, for inspection without another model turn.
 */
export const writeRejectedTranscript = (input: {
  readonly directory: string;
  readonly fixture: string;
  readonly encoded: string;
}) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    yield* fs.makeDirectory(input.directory, { recursive: true });
    const file = path.join(input.directory, `${input.fixture}.rejected.ndjson`);
    yield* fs.writeFileString(file, input.encoded);
    return file;
  });
