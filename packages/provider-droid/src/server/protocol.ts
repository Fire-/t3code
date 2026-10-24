/**
 * Droid's stream-jsonrpc wire format: versions, frame schemas, per-method timeouts, and the
 * pure parsers for the payload shapes the adapter reads. No Effect services and no I/O.
 */
import * as Duration from "effect/Duration";
import * as Exit from "effect/Exit";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

export const DROID_API_VERSION = "1.0.0";
/** `factoryProtocolVersion` of the Droid build this package was written against (droid 0.244.0). */
export const DROID_PROTOCOL_VERSION = "1.269.0";
export const DROID_STDERR_TAIL_MAX = 16 * 1024;
export const DROID_NOTIFICATION_METHOD = "droid.session_notification";

export const JSON_RPC_METHOD_NOT_FOUND = -32601;
export const JSON_RPC_INTERNAL_ERROR = -32603;

const DROID_INBOUND_REQUEST_METHODS = [
  "droid.request_permission",
  "droid.ask_user",
  "droid.pick_options",
] as const;

export type DroidInboundRequestMethod = (typeof DROID_INBOUND_REQUEST_METHODS)[number];

export const isDroidInboundRequestMethod = (method: string): method is DroidInboundRequestMethod =>
  (DROID_INBOUND_REQUEST_METHODS as ReadonlyArray<string>).includes(method);

/**
 * Notification types the adapter consumes. Anything else (including `mission_*`, hook and
 * structured-output notifications, and types a newer Droid adds) is logged and dropped.
 */
export const DROID_NOTIFICATION_TYPES = [
  "droid_working_state_changed",
  "settings_updated",
  "create_message",
  "assistant_text_delta",
  "thinking_text_delta",
  "assistant_text_complete",
  "thinking_text_complete",
  "assistant_message_retracted",
  "tool_call",
  "tool_execution_phase_changed",
  "tool_progress_update",
  "tool_result",
  "child_session_available",
  "permission_resolved",
  "session_token_usage_changed",
  "session_title_updated",
  "mcp_status_changed",
  "session_compacted",
  "error",
  "llm_retry",
  "queued_messages_discarded",
  "agent_turn_completed",
  "spec_handoff_session_created",
] as const;

export type DroidNotificationType = (typeof DROID_NOTIFICATION_TYPES)[number];

export const isDroidNotificationType = (type: string): type is DroidNotificationType =>
  (DROID_NOTIFICATION_TYPES as ReadonlyArray<string>).includes(type);

/** Per-request timeout. No timeout ever covers a whole turn. */
export function timeoutForMethod(method: string): Duration.Duration {
  switch (method) {
    case "droid.initialize_session":
    case "droid.load_session":
    case "droid.execute_rewind":
      return Duration.seconds(60);
    case "droid.compact_session":
      return Duration.seconds(240);
    default:
      return Duration.seconds(30);
  }
}

const WireRecord = Schema.Record(Schema.String, Schema.Unknown);

const NotificationBody = Schema.StructWithRest(Schema.Struct({ type: Schema.String }), [
  WireRecord,
]);

export const DroidResponseFrame = Schema.Struct({
  type: Schema.Literal("response"),
  id: Schema.optional(Schema.NullOr(Schema.String)),
  factoryProtocolVersion: Schema.optional(Schema.String),
  result: Schema.optional(Schema.Unknown),
  error: Schema.optional(Schema.Unknown),
});

export const DroidRequestFrame = Schema.Struct({
  type: Schema.Literal("request"),
  id: Schema.String,
  method: Schema.String,
  params: Schema.optional(Schema.Unknown),
});

export const DroidNotificationFrame = Schema.Struct({
  type: Schema.Literal("notification"),
  method: Schema.String,
  params: Schema.optional(Schema.Unknown),
});

export const DroidFrame = Schema.Union([
  DroidResponseFrame,
  DroidRequestFrame,
  DroidNotificationFrame,
]);

export type DroidFrame = typeof DroidFrame.Type;

export const decodeDroidFrame = Schema.decodeUnknownOption(DroidFrame);

const NotificationParams = Schema.StructWithRest(
  Schema.Struct({ notification: NotificationBody }),
  [WireRecord],
);

export const decodeDroidNotificationParams = Schema.decodeUnknownOption(NotificationParams);

const encodeJsonExit = Schema.encodeUnknownExit(Schema.fromJsonString(Schema.Unknown));
const decodeJsonExit = Schema.decodeUnknownExit(Schema.fromJsonString(Schema.Unknown));

function encodeJson(value: unknown): string | undefined {
  const result = encodeJsonExit(value);
  return Exit.isSuccess(result) ? result.value : undefined;
}

function decodeJsonObject(text: string): Record<string, unknown> | undefined {
  if (!text.trimStart().startsWith("{")) return undefined;
  const result = decodeJsonExit(text);
  return Exit.isSuccess(result) && Predicate.isObject(result.value) ? result.value : undefined;
}

const asString = (value: unknown): string | undefined =>
  Predicate.isString(value) ? value : undefined;

function asTrimmed(value: unknown): string | undefined {
  if (!Predicate.isString(value)) return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function asCount(value: unknown): number {
  return Predicate.isNumber(value) && Number.isFinite(value) && value > 0 ? Math.floor(value) : 0;
}

/** Tool output kept on a tool row, counted in characters; the end is kept. */
export const DROID_TOOL_OUTPUT_MAX_CHARS = 8_000;
const DETAIL_MAX_CHARS = 2_000;
const TRUNCATION_MARKER = "…";

export function boundDroidToolOutput(text: string): string {
  if (text.length <= DROID_TOOL_OUTPUT_MAX_CHARS) return text;
  return `${TRUNCATION_MARKER}${text.slice(text.length - (DROID_TOOL_OUTPUT_MAX_CHARS - TRUNCATION_MARKER.length))}`;
}

function truncateDetail(value: string): string {
  return value.length > DETAIL_MAX_CHARS ? `${value.slice(0, DETAIL_MAX_CHARS - 3)}...` : value;
}

export type DroidToolKind = "execute" | "edit" | "read" | "search" | "fetch" | "task" | "unknown";

export function droidToolKind(toolName: string): DroidToolKind {
  switch (toolName) {
    case "Execute":
      return "execute";
    case "Task":
      return "task";
    case "Edit":
    case "MultiEdit":
    case "Create":
    case "ApplyPatch":
      return "edit";
    case "Read":
    case "LS":
      return "read";
    case "Grep":
    case "Glob":
    case "ToolSearch":
    case "WebSearch":
      return "search";
    case "FetchUrl":
      return "fetch";
    default:
      return "unknown";
  }
}

const TOOL_SUMMARY_KEYS = [
  "command",
  "file_path",
  "path",
  "pattern",
  "query",
  "url",
  "description",
] as const;

/** The first meaningful input field, for a tool row's title. */
export function droidToolSummary(input: unknown): string | undefined {
  if (!Predicate.isObject(input)) return undefined;
  for (const key of TOOL_SUMMARY_KEYS) {
    const value = asTrimmed(input[key]);
    if (value) return truncateDetail(value);
  }
  return undefined;
}

/** A Script row's title names the tools its JavaScript calls as `tools.Name(...)`. */
export function droidScriptTitle(script: string): string {
  const names = new Set<string>();
  for (const match of script.matchAll(/\btools\.([A-Za-z_$][\w$]*)\s*\(/g)) {
    if (match[1] !== undefined) names.add(match[1]);
  }
  return names.size === 0 ? "Script" : `Script: ${[...names].join(", ")}`;
}

/**
 * Droid names an MCP tool `<server>___<tool>`. Clients recognize T3's own tools, and show other
 * servers' tools as integrations, in the `mcp__<server>__<tool>` form Claude uses.
 */
export function droidMcpToolName(name: string): string {
  const match = /^(.+?)___(.+)$/.exec(name);
  return match === null ? name : `mcp__${match[1]}__${match[2]}`;
}

export function droidToolPath(input: unknown): string | undefined {
  if (!Predicate.isObject(input)) return undefined;
  return asTrimmed(input["file_path"]) ?? asTrimmed(input["path"]);
}

export interface DroidToolOutput {
  readonly text: string | undefined;
  readonly stdout: string | undefined;
  readonly stderr: string | undefined;
}

/** `tool_result.content` is a tool-specific string (sometimes JSON text) or an array of blocks. */
export function droidToolResultOutput(content: unknown): DroidToolOutput {
  if (Predicate.isString(content)) {
    const parsed = decodeJsonObject(content);
    const stdout = asString(parsed?.["stdout"]);
    const stderr = asString(parsed?.["stderr"]);
    return {
      text: content.trim().length > 0 ? boundDroidToolOutput(content) : undefined,
      stdout: stdout ? boundDroidToolOutput(stdout) : undefined,
      stderr: stderr ? boundDroidToolOutput(stderr) : undefined,
    };
  }
  if (Array.isArray(content)) {
    const text = textBlocks(content)
      .filter((chunk) => chunk.trim().length > 0)
      .join("\n");
    return {
      text: text.length > 0 ? boundDroidToolOutput(text) : undefined,
      stdout: undefined,
      stderr: undefined,
    };
  }
  if (content === undefined || content === null) {
    return { text: undefined, stdout: undefined, stderr: undefined };
  }
  const encoded = encodeJson(content);
  return {
    text: encoded ? boundDroidToolOutput(encoded) : undefined,
    stdout: undefined,
    stderr: undefined,
  };
}

function textBlocks(content: ReadonlyArray<unknown>): ReadonlyArray<string> {
  return content.flatMap((block) =>
    Predicate.isObject(block) && block["type"] === "text" && Predicate.isString(block["text"])
      ? [block["text"]]
      : [],
  );
}

const EXECUTE_EXIT_FOOTER_MARKER = "[Process exited with code ";

/**
 * `Execute` results end with `[Process exited with code N]` after blank lines. The footer
 * becomes the exit code; the rest is the command's output.
 */
export function splitDroidExecuteOutput(text: string | undefined): {
  readonly output: string | undefined;
  readonly exitCode: number | undefined;
} {
  if (text === undefined) return { output: undefined, exitCode: undefined };
  const markerAt = text.lastIndexOf(EXECUTE_EXIT_FOOTER_MARKER);
  const footer =
    markerAt === -1
      ? undefined
      : /^\[Process exited with code (-?\d+)\]\s*$/u.exec(text.slice(markerAt));
  if (!footer?.[1]) return { output: text, exitCode: undefined };
  const output = text.slice(0, markerAt).trimEnd();
  return { output: output.length > 0 ? output : undefined, exitCode: Number(footer[1]) };
}

/** Added and removed line counts from an `Edit` result's `diffLines`. */
export function droidEditDiffCounts(
  content: unknown,
): { readonly additions: number; readonly deletions: number } | undefined {
  const parsed = Predicate.isString(content) ? decodeJsonObject(content) : undefined;
  const lines = parsed?.["diffLines"];
  if (!Array.isArray(lines)) return undefined;
  const count = (type: string) =>
    lines.filter((line) => Predicate.isObject(line) && line["type"] === type).length;
  return { additions: count("added"), deletions: count("removed") };
}

/** The text blocks of a stored or streamed Droid message. */
export function droidMessageText(content: unknown): string {
  if (Predicate.isString(content)) return content;
  return Array.isArray(content) ? textBlocks(content).join("\n") : "";
}

/** An awaited Task's result leads with Droid's own framing; the rest is the child's report. */
export function awaitedTaskReport(text: string | undefined): string | undefined {
  const lines = (text ?? "").split("\n");
  if (lines[0]?.startsWith("session_id:")) lines.shift();
  if (lines[0]?.startsWith("Use this report as the source of record")) lines.shift();
  return asTrimmed(lines.join("\n"));
}

/**
 * The report of a `Task` tool result: the text with Droid's framing lines removed, stripped
 * before it is bounded so a long report cannot cut the framing in half.
 */
export function droidTaskResultReport(content: unknown): string | undefined {
  const text = Predicate.isString(content)
    ? content
    : Array.isArray(content)
      ? textBlocks(content).join("\n")
      : undefined;
  const report = awaitedTaskReport(text);
  return report === undefined ? undefined : boundDroidToolOutput(report);
}

/** Droid's id prefix for the message a background task's completion arrives as. */
const DROID_TASK_COMPLETION_ID_PREFIX = "task-completion:";

export interface DroidTaskCompletion {
  readonly taskId: string;
  readonly status: "completed" | "failed" | "stopped";
  readonly summary: string | undefined;
}

/**
 * Parses the message Droid injects when a background task ends (the CLI's template): a
 * `Background task <completed|stopped|error>.` line, `key: value` lines (`failure:` only
 * on errors) ending with a multi-line `output:`, then a paragraph of instructions to the
 * model that opens with `Background task "`.
 */
export function parseDroidTaskCompletion(text: string): DroidTaskCompletion | undefined {
  const head = /^Background task (\w+)\./u.exec(text);
  const taskId = /^task_id: *(\S+)/mu.exec(text)?.[1];
  if (!head || !taskId) return undefined;
  const status =
    head[1] === "completed" ? "completed" : head[1] === "stopped" ? "stopped" : "failed";
  const trailer = text.lastIndexOf('\n\nBackground task "');
  const body = trailer === -1 ? text : text.slice(0, trailer);
  const rawOutput = asTrimmed(/^output: ?([\s\S]*)$/mu.exec(body)?.[1]);
  const output = rawOutput === "No output available" ? undefined : rawOutput;
  const failure = asTrimmed(/^failure: *(.*)$/mu.exec(body)?.[1]);
  return { taskId, status, summary: status === "failed" ? (failure ?? output) : output };
}

/**
 * The id of the `create_message` Droid opens a turn of its own with when a background task ends,
 * whatever the text says: the native turn that follows carries this id as its `turnId`.
 */
export function droidTaskCompletionMessageId(
  notification: Readonly<Record<string, unknown>>,
): string | undefined {
  const message = notification["message"];
  if (!Predicate.isObject(message)) return undefined;
  const id = asString(message["id"]);
  return id !== undefined && id.startsWith(DROID_TASK_COMPLETION_ID_PREFIX) ? id : undefined;
}

/**
 * The completion a `create_message` notification carries, when it is Droid's background-task
 * message: its id has the `task-completion:` prefix and its text follows the CLI's template. The
 * live role is `system`; a stored one reads as `user`, so the role is not checked.
 */
export function droidTaskCompletionOf(
  notification: Readonly<Record<string, unknown>>,
): DroidTaskCompletion | undefined {
  const message = notification["message"];
  if (!Predicate.isObject(message) || droidTaskCompletionMessageId(notification) === undefined) {
    return undefined;
  }
  return parseDroidTaskCompletion(droidMessageText(message["content"]));
}

export interface DroidTokenCounts {
  readonly inputTokens: number;
  readonly outputTokens: number;
  readonly cacheCreationTokens: number;
  readonly cacheReadTokens: number;
  readonly thinkingTokens: number;
}

export function droidTokenCounts(value: unknown): DroidTokenCounts | undefined {
  if (!Predicate.isObject(value)) return undefined;
  return {
    inputTokens: asCount(value["inputTokens"]),
    outputTokens: asCount(value["outputTokens"]),
    cacheCreationTokens: asCount(value["cacheCreationTokens"]),
    cacheReadTokens: asCount(value["cacheReadTokens"]),
    thinkingTokens: asCount(value["thinkingTokens"]),
  };
}

export function isZeroDroidUsage(counts: DroidTokenCounts): boolean {
  return (
    counts.inputTokens +
      counts.outputTokens +
      counts.cacheCreationTokens +
      counts.cacheReadTokens +
      counts.thinkingTokens ===
    0
  );
}

/** Per-field `minuend - subtrahend` clamped at 0; `clamped` says whether any field went negative. */
export function subtractDroidCounts(
  minuend: DroidTokenCounts,
  subtrahend: DroidTokenCounts,
): { readonly counts: DroidTokenCounts; readonly clamped: boolean } {
  let clamped = false;
  const field = (key: keyof DroidTokenCounts) => {
    const difference = minuend[key] - subtrahend[key];
    if (difference < 0) clamped = true;
    return Math.max(0, difference);
  };
  const counts = {
    inputTokens: field("inputTokens"),
    outputTokens: field("outputTokens"),
    cacheCreationTokens: field("cacheCreationTokens"),
    cacheReadTokens: field("cacheReadTokens"),
    thinkingTokens: field("thinkingTokens"),
  };
  return { counts, clamped };
}

export interface DroidAskUserQuestion {
  readonly index: number;
  readonly topic: string | undefined;
  readonly question: string;
  readonly options: ReadonlyArray<string>;
  readonly multiSelect: boolean;
}

export function droidAskUserQuestionsFromParams(
  params: unknown,
): ReadonlyArray<DroidAskUserQuestion> {
  const questions =
    Predicate.isObject(params) && Array.isArray(params["questions"]) ? params["questions"] : [];
  return questions.flatMap((entry, position) => {
    if (!Predicate.isObject(entry)) return [];
    const topic = asTrimmed(entry["topic"]);
    const question = asTrimmed(entry["question"]) ?? topic;
    if (!question) return [];
    const index =
      Predicate.isNumber(entry["index"]) && Number.isInteger(entry["index"])
        ? entry["index"]
        : position + 1;
    const options = Array.isArray(entry["options"])
      ? entry["options"].flatMap((option) => {
          const label = asTrimmed(option);
          return label ? [label] : [];
        })
      : [];
    return [{ index, topic, question, options, multiSelect: entry["multiSelect"] === true }];
  });
}

export const DROID_ASK_USER_CANCELLED = { cancelled: true, answers: [] } as const;

export const DROID_PICK_OPTIONS_CANCELLED = { selectedIds: [], cancelled: true } as const;
