import type { ProviderReplayEntry } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Predicate from "effect/Predicate";

const HIDDEN_CONTEXT_PLACEHOLDER = "<hidden-context>";
const HOST_ID_PLACEHOLDER = "<host-id>";
const ANY_PLACEHOLDER = "<any>";

/** Wall-clock values are shifted so the earliest one lands here and offsets between them survive. */
const FIXED_EPOCH_MS = Date.parse("2026-01-01T00:00:00.000Z");

/**
 * Messages Droid injects for the model alone: global instruction files, skills, memory, and
 * system reminders. They hold the user's private text, so the recorder blanks them structurally
 * instead of hoping a text match finds every line.
 */
const HIDDEN_MESSAGE_ID = /^(?:context-|__internal__system-reminder__)/u;
const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/giu;
const ISO_TIMESTAMP = /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z/gu;
const TIME_KEY = /^(?:timestamp|time|[a-z]+At)$/u;
const MODEL_LIST_KEYS = new Set(["models", "availableModels"]);
const LAUNCH_VALUE_FLAGS = new Set(["--cwd", "--settings"]);
const TRACE_KEYS = ["traceparent", "tracestate"];
const PATH_END = "(?![A-Za-z0-9_-])";
const EPOCH_MS_FLOOR = 1e12;
const EPOCH_MS_CEILING = 1e14;

export interface DroidScrubEnvironment {
  /** The one model the fixture records; every other model entry is dropped from model lists. */
  readonly modelId: string;
  readonly workspace: string;
  readonly recordingRoot: string;
  readonly systemTmpDir: string;
  readonly home: string;
  readonly userName: string;
  readonly hostNames: ReadonlyArray<string>;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  Predicate.isObject(value) && !Array.isArray(value);

const escapeRegExp = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");

const withoutPrivateAlias = (path: string): string => path.replace(/^\/private(?=\/)/u, "");

/** macOS reports `/tmp` and `/var` through `/private`, so both spellings reach the transcript. */
const pathPattern = (path: string): RegExp =>
  new RegExp(`(?:/private)?${escapeRegExp(withoutPrivateAlias(path))}${PATH_END}`, "gu");

/** Host ids come from the responses themselves, so a leak check can look for what was replaced. */
export function collectHostIds(entries: ReadonlyArray<ProviderReplayEntry>): ReadonlyArray<string> {
  const found = new Set<string>();
  const visit = (value: unknown): void => {
    if (Array.isArray(value)) {
      for (const item of value) visit(item);
    } else if (isRecord(value)) {
      for (const [key, item] of Object.entries(value)) {
        if (key === "hostId" && Predicate.isString(item) && item.length > 0) found.add(item);
        else visit(item);
      }
    }
  };
  for (const entry of entries) if (entry.type !== "runtime_exit") visit(entry.frame);
  return [...found];
}

function blankHiddenContent(record: Record<string, unknown>): Record<string, unknown> {
  if (
    !Predicate.isString(record.id) ||
    !HIDDEN_MESSAGE_ID.test(record.id) ||
    !("content" in record)
  ) {
    return record;
  }
  return {
    ...record,
    content: Array.isArray(record.content)
      ? [{ type: "text", text: HIDDEN_CONTEXT_PLACEHOLDER }]
      : HIDDEN_CONTEXT_PLACEHOLDER,
  };
}

function wildcardLaunchInputs(entry: ProviderReplayEntry): ProviderReplayEntry {
  if (entry.type !== "expect_outbound" || !isRecord(entry.frame)) return entry;
  const frame = entry.frame;
  const launchArgs: ReadonlyArray<unknown> | undefined = Array.isArray(frame.args)
    ? frame.args
    : undefined;
  if (frame.type === "process_start" && launchArgs !== undefined) {
    const args = launchArgs.map((arg, index) =>
      LAUNCH_VALUE_FLAGS.has(String(launchArgs[index - 1])) ? ANY_PLACEHOLDER : arg,
    );
    return { ...entry, frame: { ...frame, args } };
  }
  if (frame.type === "request" && isRecord(frame.params) && Predicate.isString(frame.params.cwd)) {
    return { ...entry, frame: { ...frame, params: { ...frame.params, cwd: ANY_PLACEHOLDER } } };
  }
  return entry;
}

function makeTextScrubber(environment: DroidScrubEnvironment, hostIds: ReadonlyArray<string>) {
  const uuids = new Map<string, string>();
  let anchor: number | undefined;

  const shiftEpoch = (milliseconds: number): number => {
    anchor ??= milliseconds;
    return FIXED_EPOCH_MS + (milliseconds - anchor);
  };

  const replacements: ReadonlyArray<readonly [RegExp, string]> = [
    [pathPattern(environment.workspace), "<workspace>"],
    [pathPattern(environment.recordingRoot), "<recording-root>"],
    [pathPattern(environment.systemTmpDir), "<tmpdir>"],
    [new RegExp(`${escapeRegExp(environment.home)}${PATH_END}`, "gu"), "~"],
    ...[...environment.hostNames]
      .filter((name) => name.length > 0)
      .toSorted((left, right) => right.length - left.length)
      .map((name): readonly [RegExp, string] => [new RegExp(escapeRegExp(name), "giu"), "<host>"]),
    ...(environment.userName.length === 0
      ? []
      : [
          [
            new RegExp(
              `(?<![A-Za-z0-9_])${escapeRegExp(environment.userName)}(?![A-Za-z0-9_])`,
              "gu",
            ),
            "<user>",
          ] as const,
        ]),
  ];

  const text = (value: string): string => {
    let scrubbed = value;
    for (const hostId of hostIds) scrubbed = scrubbed.replaceAll(hostId, HOST_ID_PLACEHOLDER);
    for (const [pattern, replacement] of replacements) {
      scrubbed = scrubbed.replace(pattern, replacement);
    }
    return scrubbed
      .replace(ISO_TIMESTAMP, (stamp) =>
        DateTime.formatIso(DateTime.makeUnsafe(shiftEpoch(Date.parse(stamp)))),
      )
      .replace(UUID, (id) => {
        const key = id.toLowerCase();
        const known = uuids.get(key);
        if (known !== undefined) return known;
        const placeholder = `00000000-0000-4000-8000-${String(uuids.size + 1).padStart(12, "0")}`;
        uuids.set(key, placeholder);
        return placeholder;
      });
  };

  const number = (key: string | undefined, value: number): number =>
    key !== undefined &&
    TIME_KEY.test(key) &&
    Number.isInteger(value) &&
    value >= EPOCH_MS_FLOOR &&
    value < EPOCH_MS_CEILING
      ? shiftEpoch(value)
      : value;

  return { text, number };
}

/**
 * Makes a recorded Droid transcript safe to commit and stable across recordings. It only
 * rewrites values: `emit_inbound` frames keep Droid's shape, and the adapter's own frames keep
 * everything it chose except the machine-specific launch inputs.
 */
export function scrubDroidEntries(
  entries: ReadonlyArray<ProviderReplayEntry>,
  environment: DroidScrubEnvironment,
): Array<ProviderReplayEntry> {
  const scrubber = makeTextScrubber(environment, collectHostIds(entries));

  const walk = (value: unknown, key?: string): unknown => {
    if (Predicate.isString(value)) {
      return key === "hostId" ? HOST_ID_PLACEHOLDER : scrubber.text(value);
    }
    if (Predicate.isNumber(value)) return scrubber.number(key, value);
    if (Array.isArray(value)) {
      const keepsOnlyModel =
        key !== undefined &&
        MODEL_LIST_KEYS.has(key) &&
        value.every((item) => isRecord(item) && Predicate.isString(item.id));
      return (
        keepsOnlyModel
          ? value.filter((item) => isRecord(item) && item.id === environment.modelId)
          : value
      ).map((item) => walk(item));
    }
    if (!isRecord(value)) return value;
    const scrubbed: Record<string, unknown> = {};
    for (const [name, item] of Object.entries(blankHiddenContent(value))) {
      if (name === "_meta" && isRecord(item)) {
        const rest = Object.fromEntries(
          Object.entries(item).filter(([metaKey]) => !TRACE_KEYS.includes(metaKey)),
        );
        if (Object.keys(rest).length > 0) scrubbed[name] = walk(rest);
        continue;
      }
      scrubbed[scrubber.text(name)] = walk(item, name);
    }
    return scrubbed;
  };

  return entries.map((entry) => {
    if (entry.type === "runtime_exit") return entry;
    const launched = wildcardLaunchInputs(entry);
    return launched.type === "runtime_exit"
      ? launched
      : { ...launched, frame: walk(launched.frame) };
  });
}
