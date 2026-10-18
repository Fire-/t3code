// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

/**
 * Mock `droid` CLI for tests. Plain Node (run through type stripping), launched
 * through `@t3tools/provider-testing/fakeCli`, so `process.argv.slice(2)` holds the CLI args.
 *
 * Modes (by argv):
 * - `--version` anywhere: prints `0.244.0` and exits 0 before stdin is touched.
 * - `exec --input-format stream-jsonrpc --output-format stream-jsonrpc --cwd <dir> --settings <file>`:
 *   Droid's stream-jsonrpc protocol on stdin/stdout (LF-delimited JSON, Droid envelope,
 *   string ids). Implemented methods: `droid.list_models`, `droid.initialize_session`,
 *   `droid.load_session`, `droid.add_user_message`, `droid.interrupt_session`,
 *   `droid.update_session_settings`, `droid.compact_session`, `droid.execute_rewind`,
 *   `droid.close_session`. `compact_session` answers -32603 before any completed turn and
 *   -32602 while a turn runs, else replies with the SAME session id and follows the response
 *   with `session_compacted`. `execute_rewind` forks the session so it ends just before the
 *   given messageId (tagged `rewind`) and answers `{newSessionId, counts 0}`; the process
 *   stays on the old session until `load_session`. The next `add_user_message` is preceded by
 *   a `session_title_updated` on the pre-rewind session id (SYNTHETIC: a late notification
 *   from the session the rewind left behind). Inbound validation mirrors
 *   Droid: -32700 (id null) for non-JSON, a `type` other than request/response, a missing
 *   `factoryApiVersion`, or a non-string id; -32601 for unknown methods; -32602 for a missing
 *   `params`; -32603 for a session method before `initialize_session`/`load_session`;
 *   -32004 `Session not found` for `load_session` with an id this process never created and
 *   no state file holds (ids not matching `[A-Za-z0-9_-]+` are always rejected). A loaded
 *   session's result has no `sessionId`, matching Droid.
 *
 * Environment:
 * - `DROID_MOCK_LOG`: file path; one JSON line is appended per event: `argv` (with the child's pid), `env`
 *   (whether `FACTORY_API_KEY` reached the child and the names of every `FACTORY_*` variable it
 *   saw; never the values),
 *   `settings_file` (path + content, null when missing), `recv` (every stdin line,
 *   written BEFORE any reply), `response` (client answer to a mock request), `stale_response`,
 *   `invalid_option`, `no_reply`, `tool_executed`, `session_switch` (from + to ids),
 *   `stdin_eof`, `sigterm`, `exit`.
 * - `DROID_MOCK_PROTOCOL_VERSION`: overrides the outgoing `factoryProtocolVersion` (default 1.269.0).
 * - `DROID_MOCK_LIST_MODELS`: `error` answers `droid.list_models` with a -32603 error; `noreply`
 *   never answers it (the request hangs until the caller times out and closes); `malformed`
 *   answers with a body that decodes to no models.
 * - `DROID_MOCK_STUBBORN=1`: same as the `STUBBORN` token, from process start.
 * - `DROID_MOCK_SIGTERM=die`: installs no SIGTERM handler, so SIGTERM ends the process by signal.
 * - `DROID_MOCK_STATE_DIR`: opt-in directory standing in for Droid's session store, so a later
 *   mock process can `load_session` a session an earlier one created. `<dir>/<sessionId>.json`
 *   is written synchronously on `initialize_session`, on every appended message (before its
 *   `create_message` notification, as real Droid persists each message when created, so a
 *   SIGTERM or crash mid-turn keeps what was emitted), and after every finished turn, and read
 *   only by `load_session` when the id is not in this process's own store. Unset: sessions live
 *   only as long as the process.
 *
 * RPC trigger tokens (substring of `add_user_message.text`; default is a plain text turn):
 * - `TOOL_ME`: Execute tool turn without a permission prompt; `tool_call` fires twice per tool.
 * - `APPROVE_ME`: Execute tool behind an inbound `droid.request_permission` (exec, low impact);
 *   held until the client answers, the client interrupts, or stdin ends. Skipped when the
 *   session autonomy level already covers low impact.
 * - `APPROVE_FILE`: like `APPROVE_ME` with a Create tool (`create` confirmation).
 * - `ASK_ME`: AskUser tool behind an inbound `droid.ask_user` (two questions, the second
 *   multi-select); `{cancelled: true}` or a JSON-RPC error cancels the tool and still ends the
 *   turn `completed`.
 * - `ASK_PERM_ME`: AskUser behind an inbound `droid.request_permission` with
 *   `confirmationType: "ask_user"` (SDK shape; answers travel in the result's `comment`).
 *   An interrupt resolves it with a `permission_resolved` cancel, like a permission.
 * - `PICK_ME`: SlackChannelPicker tool behind `droid.pick_options`; any answer cancels the tool.
 * - `PLAN_ME`: ExitSpecMode tool behind an `exit_spec_mode` permission prompt (9 options,
 *   including the hidden `proceed_new_session*` ones); the plan text is in `details.plan`.
 * - `SWITCH_SESSION`: like `PLAN_ME`, but an approving answer hands off to a fresh session id
 *   (logged as `session_switch`): the implementation runs there as a Droid-generated `impl-turn`
 *   whose completion does not settle the original turn, which ends `spec_handoff` on the old id.
 *   Matches the live order (p13 line 42): the retired session's `session_title_updated` still
 *   arrives mid-turn, after the handoff and before the old-id `spec_handoff` completion, which
 *   is the old session's FINAL event (nothing follows it).
 * - `INTERRUPT_ME`: streams part of a reply, then holds until `interrupt_session` (or stdin EOF).
 * - `INTERRUPT_WEDGED`: like INTERRUPT_ME, but every `interrupt_session` is answered with a
 *   -32603 error and the turn stays silent and held (Droid wedged mid-turn). The second and
 *   later interrupts are announced with a `session_title_updated` so a test can observe the
 *   watchdog's own interrupt landing. Only stdin EOF ends it (with `process_exit`).
 * - `INTERRUPT_GHOST`: like INTERRUPT_ME, but `interrupt_session` is never answered and the
 *   turn never ends. Each received interrupt is announced with a `session_title_updated` so
 *   a test can observe it landing. Only stdin EOF ends it (with `process_exit`).
 * - `STEER_ME`: holds after the thinking deltas until a second `add_user_message` (the steer),
 *   acknowledges it, finishes the first reply, echoes the steer's user `create_message`, answers
 *   it with `QUEUED`, and sends ONE completion carrying the first turn id.
 * - `STEER_DROP`: like STEER_ME, but after acknowledging the steer it holds for
 *   `interrupt_session` and then reports `queued_messages_discarded` (unscoped, with the steer
 *   text and the steer's JSON-RPC request id) before the `cancelled` completion.
 *   `STEER_DROP_ECHO` (SYNTHETIC): after the discard and the cancelled tail, the steer's user
 *   `create_message` and a completion for it arrive anyway - the discard report and the
 *   delivery crossed.
 * - `LATE_STEER`: holds after the first text delta until the steer arrives, acknowledges it,
 *   then completes the FIRST turn before the steer's user `create_message`; the steer then runs
 *   as its own turn and completes with the steer's messageId as `turnId` (p13 race).
 *   `LATE_STEER_FOLD`: the steer's turn is deferred until the client's next message, which
 *   folds into it (see `deferredDroidTurn`, SYNTHETIC timing).
 * - `BG_TASK`: launches a Task subagent with `await: false` (`child_session_available`, then
 *   the launch acknowledgement as its result), completes, and the subagent's report runs as a
 *   turn Droid starts itself: a `task-completion:*` system message, a reply, and a completion
 *   with that id (p17). `BG_TASK_FOLD` / `BG_TASK_BEHIND` defer the report until the client's
 *   next message, which folds into the report turn or runs as its own turn after it.
 * - `RETRACT_ME`: one pre-retract text delta, `llm_retry`, `assistant_message_retracted`, then
 *   the same messageId streams `QUEUED` from scratch and the turn completes.
 * - `AUTH_FAIL`: an `error` notification (`AuthenticationError`), a `user_only` system message,
 *   and a completion with `reason: "model_authentication_failed"`; the process stays up.
 * - `AUTO_COMPACT`: a plain turn that compacts mid-turn (`compacting_conversation` +
 *   `session_compacted`) after the assistant message, same session id.
 * - `EOF_TOOLS`: announces an Execute tool, then holds before running it. `interrupt_session`
 *   cancels the tool (no `tool_executed`) and ends the turn `cancelled`; stdin EOF runs that tool
 *   and a second one, then completes with `process_exit` and exits 0.
 * - `LONG_TOOL`: an Execute tool that enters the `executing` phase (with the start progress
 *   update) and then goes completely silent, as real Droid does during a long tool (no
 *   heartbeat). Held until any client message: a steer releases it (acknowledged, folded in,
 *   turn completes `completed`), `interrupt_session` cancels the tool (turn `cancelled`), and
 *   stdin EOF lets it finish with `process_exit`.
 * - `STALL_AFTER_TOOL`: like LONG_TOOL, but once released the tool result is followed by
 *   more silence instead of a completion; the second hold ends on `interrupt_session`
 *   (cancelled tail) or stdin EOF (turn completes with `process_exit`). A releasing steer's
 *   fold-in echo precedes the tool result (it maps to no canonical event, so the result can
 *   never be observed before the echo was processed).
 * - `EXIT_MID_TURN`: behaves as if stdin ended right after the turn started: `idle`, the plain
 *   reply, `agent_turn_completed {reason: "process_exit"}`, exit 0.
 * - `IDLE_MID_TURN`: plain turn with an `idle` working state between two text deltas.
 * - `REROUTE_ME`: plain turn whose assistant message reports `deepseek-v4.1-flash` (baseten)
 *   regardless of the requested model.
 * - `HUGE_RESULT`: tool turn whose `tool_result` line is over 10 MB; the content ends with
 *   U+2028 U+2029 `end`.
 * - `UNKNOWN_EVENT`: unknown notification types, an unknown notification method, and a
 *   notification without a `notification` field, mid-turn.
 * - `UNKNOWN_INBOUND`: sends an inbound request with method `droid.future_request`, holds until
 *   the client answers it (expected: -32601), then finishes a plain turn.
 * - `NO_REPLY`: the `add_user_message` request is never answered and no turn starts.
 * - `REJECT_MESSAGE`: the `add_user_message` request is answered with a -32603 error and no
 *   turn starts (the session stays alive).
 * - `CRASH_ME`: without answering `add_user_message`, floods stderr (64 KB, last line
 *   `fatal: mock crash`; when `FACTORY_API_KEY` is set, the line before it echoes the key, as a
 *   failing real Droid may) and exits 1.
 * - `CRASH_MID_TURN`: answers `add_user_message`, starts the turn, then crashes like `CRASH_ME`.
 * - `MALFORMED_LINE`: without answering `add_user_message`, writes one stderr line and one
 *   non-JSON stdout line, then waits.
 * - `MALFORMED_MID_TURN`: answers `add_user_message`, starts the turn, then writes like
 *   `MALFORMED_LINE` and waits.
 * - `STUBBORN`: from then on `close_session` is answered but does not exit, stdin EOF is
 *   ignored, and SIGTERM only logs `sigterm`; the process exits only on SIGKILL. The turn itself
 *   is a plain text turn.
 *
 * Holds are released only by client messages (an answer, `interrupt_session`, stdin EOF), never
 * by timers or file watches. `close_session` replies `{}` then exits 0; stdin EOF while idle
 * exits 0; stdin EOF mid-turn finishes the scripted turn (an open permission ends it with zero
 * usage), completes it with `reason: "process_exit"`, and exits 0. SIGTERM ends an open turn
 * with `process_exit` and exits 0.
 */

const args = process.argv.slice(2);
const logPath = process.env.DROID_MOCK_LOG;

function log(entry: Record<string, unknown>): void {
  if (logPath) {
    NodeFS.appendFileSync(logPath, `${JSON.stringify(entry)}\n`, "utf8");
  }
}

log({ type: "argv", args, pid: process.pid });
log({
  type: "env",
  factoryApiKeySet: (process.env.FACTORY_API_KEY ?? "") !== "",
  factoryEnvNames: Object.keys(process.env)
    .filter((name) => name.startsWith("FACTORY_"))
    .sort(),
});

if (args.includes("--version")) {
  NodeFS.writeSync(1, "0.244.0\n");
  process.exit(0);
}

process.on("exit", (code) => log({ type: "exit", code }));
// The parent may close our stdout before we exit; that is not an error for a mock.
process.stdout.on("error", () => process.exit(0));

type Json = Record<string, unknown>;

const isRecord = (value: unknown): value is Json =>
  typeof value === "object" && value !== null && !Array.isArray(value);

function flagValue(...names: ReadonlyArray<string>): string | undefined {
  for (let index = 0; index < args.length - 1; index++) {
    if (names.includes(args[index] ?? "")) {
      return args[index + 1];
    }
  }
  return undefined;
}

function exitAfterFlush(code: number): void {
  process.stderr.write("", () => {
    process.stdout.write("", () => process.exit(code));
  });
}

function readSettingsFile(): boolean {
  const path = flagValue("--settings");
  if (path === undefined) {
    log({ type: "settings_file", path: null, content: null });
    return false;
  }
  let content: string | null = null;
  try {
    content = NodeFS.readFileSync(path, "utf8");
  } catch {
    content = null;
  }
  log({ type: "settings_file", path, content });
  if (content === null) {
    return false;
  }
  try {
    const parsed: unknown = JSON.parse(content);
    return isRecord(parsed) && isRecord(parsed.hooks) && parsed.hooks.hooksDisabled === true;
  } catch {
    return false;
  }
}

// ---------------------------------------------------------------------------
// RPC mode: envelope and output
// ---------------------------------------------------------------------------

const API_VERSION = "1.0.0";
const PROTOCOL_VERSION = process.env.DROID_MOCK_PROTOCOL_VERSION || "1.269.0";

function send(message: Json): void {
  process.stdout.write(`${JSON.stringify(message)}\n`);
}

function respond(id: string, result: unknown): void {
  send({
    jsonrpc: "2.0",
    type: "response",
    factoryApiVersion: API_VERSION,
    factoryProtocolVersion: PROTOCOL_VERSION,
    id,
    result,
  });
}

function respondError(id: string | null, code: number, message: string): void {
  send({
    jsonrpc: "2.0",
    type: "response",
    factoryApiVersion: API_VERSION,
    factoryProtocolVersion: PROTOCOL_VERSION,
    id,
    error: { code, message },
  });
}

function sendNotification(params: Json, method = "droid.session_notification"): void {
  send({
    jsonrpc: "2.0",
    type: "notification",
    factoryApiVersion: API_VERSION,
    factoryProtocolVersion: PROTOCOL_VERSION,
    method,
    params,
  });
}

/** Notification carrying `params.sessionId` (the adapter reads it for session switches). */
function notify(notification: Json, sessionId: string | undefined = session?.id): void {
  sendNotification(sessionId === undefined ? { notification } : { sessionId, notification });
}

/**
 * Notification without `params.sessionId`: real Droid omits it for `tool_call`,
 * `permission_resolved`, `settings_updated`, `mcp_status_changed`, `hook_execution_*`, and
 * `session_token_usage_changed` (which carries it inside the notification instead).
 */
function notifyUnscoped(notification: Json): void {
  sendNotification({ notification });
}

// ---------------------------------------------------------------------------
// RPC mode: models, settings, sessions
// ---------------------------------------------------------------------------

const MODELS: ReadonlyArray<Json> = [
  {
    id: "auto",
    modelId: "auto",
    kind: "router",
    modelProvider: "factory",
    displayName: "Auto Model",
    shortDisplayName: "Auto Model",
    supportedReasoningEfforts: ["none"],
    defaultReasoningEffort: "none",
    deprecated: false,
    noImageSupport: false,
    supportsPDFs: false,
    tokenMultiplier: 1,
    isNew: false,
    isCustom: false,
    disabled: false,
  },
  {
    id: "claude-opus-5-5",
    modelId: "claude-opus-5-5",
    modelProvider: "anthropic",
    displayName: "Opus 5.5",
    shortDisplayName: "Opus 5.5",
    supportedReasoningEfforts: ["low", "medium", "high", "xhigh", "max"],
    defaultReasoningEffort: "medium",
    deprecated: false,
    noImageSupport: false,
    supportsPDFs: true,
    tokenMultiplier: 1.6,
    isNew: false,
    compactThroughLatestSignedThinking: true,
    isCustom: false,
    disabled: false,
  },
  {
    id: "gpt-6-sol",
    modelId: "gpt-6-sol",
    modelProvider: "openai",
    displayName: "GPT-6 Sol",
    shortDisplayName: "GPT-6 Sol",
    supportedReasoningEfforts: ["none", "low", "medium", "high", "xhigh", "max"],
    defaultReasoningEffort: "medium",
    deprecated: false,
    noImageSupport: false,
    supportsPDFs: true,
    tokenMultiplier: 0.8,
    isNew: false,
    isCustom: false,
    disabled: false,
  },
  {
    id: "deepseek-v4.1-flash",
    modelId: "deepseek-v4.1-flash",
    modelProvider: "factory",
    displayName: "DeepSeek V4.1 Flash",
    shortDisplayName: "DeepSeek V4.1 Flash",
    supportedReasoningEfforts: ["off", "low", "high", "max"],
    defaultReasoningEffort: "high",
    deprecated: false,
    noImageSupport: false,
    supportsPDFs: false,
    usesUSBasedInference: true,
    tokenMultiplier: 0.12,
    isNew: false,
    isCustom: false,
    disabled: false,
  },
  {
    id: "glm-5.3",
    modelId: "glm-5.3",
    modelProvider: "factory",
    displayName: "GLM-5.3",
    shortDisplayName: "GLM-5.3",
    supportedReasoningEfforts: ["low", "high", "max"],
    defaultReasoningEffort: "max",
    deprecated: false,
    noImageSupport: true,
    supportsPDFs: false,
    usesUSBasedInference: true,
    tokenMultiplier: 0.56,
    isNew: false,
    isCustom: false,
    disabled: false,
  },
  {
    id: "minimax-m2.7",
    modelId: "minimax-m2.7",
    modelProvider: "factory",
    displayName: "MiniMax M2.7 [Deprecated]",
    shortDisplayName: "MiniMax M2.7 [Deprecated]",
    supportedReasoningEfforts: ["high"],
    defaultReasoningEffort: "high",
    deprecated: true,
    deprecationDate: "2026-08-27",
    noImageSupport: true,
    supportsPDFs: false,
    tokenMultiplier: 0.12,
    isNew: false,
    isCustom: false,
    disabled: false,
  },
  {
    id: "qwen3.6-plus-preview",
    modelId: "qwen3.6-plus-preview",
    modelProvider: "factory",
    displayName: "Qwen3.6 Plus (Preview)",
    shortDisplayName: "Qwen3.6 Plus (Preview)",
    supportedReasoningEfforts: ["low"],
    defaultReasoningEffort: "low",
    deprecated: false,
    noImageSupport: false,
    supportsPDFs: false,
    tokenMultiplier: 0.1,
    isNew: false,
    isCustom: false,
    disabled: true,
  },
];

const AUTONOMY_LEVELS = ["off", "low", "medium", "high"] as const;
type AutonomyLevel = (typeof AUTONOMY_LEVELS)[number];

const autonomyRank = (level: unknown): number => {
  const index = AUTONOMY_LEVELS.indexOf(level as AutonomyLevel);
  return index === -1 ? 0 : index;
};

const AUTONOMY_MODES: Record<AutonomyLevel, string> = {
  off: "normal",
  low: "auto-low",
  medium: "auto-medium",
  high: "auto-high",
};

const SETTINGS_KEYS = [
  "modelId",
  "reasoningEffort",
  "interactionMode",
  "autonomyLevel",
  "specModeModelId",
  "specModeReasoningEffort",
] as const;

function defaultSettings(): Json {
  return {
    modelId: "claude-opus-5-5",
    reasoningEffort: "medium",
    autonomyMode: "normal",
    interactionMode: "auto",
    autonomyLevel: "off",
    availableAutonomyLevels: [...AUTONOMY_LEVELS],
    specModeModelId: "claude-opus-5-5",
    specModeReasoningEffort: "medium",
    compactionThresholdCheckEnabled: true,
    additionalToolIds: [],
    enabledToolIds: [],
    disabledToolIds: [],
    restrictToolIds: [],
    toolExecutionMode: "direct_only",
    allowSubagentsInScripts: false,
    migrationActive: false,
  };
}

function mergeSettings(settings: Json, params: Json): Json {
  const merged: Json = { ...settings };
  for (const key of SETTINGS_KEYS) {
    if (params[key] !== undefined) {
      merged[key] = params[key];
    }
  }
  const level = AUTONOMY_LEVELS[autonomyRank(merged.autonomyLevel)] ?? "off";
  merged.autonomyMode = merged.interactionMode === "spec" ? "spec" : AUTONOMY_MODES[level];
  return merged;
}

interface Usage {
  readonly inputTokens: number;
  readonly outputTokens: number;
  readonly cacheCreationTokens: number;
  readonly cacheReadTokens: number;
  readonly thinkingTokens: number;
  readonly factoryCredits: number;
}

const ZERO_USAGE: Usage = {
  inputTokens: 0,
  outputTokens: 0,
  cacheCreationTokens: 0,
  cacheReadTokens: 0,
  thinkingTokens: 0,
  factoryCredits: 0,
};

const TURN_USAGE: Usage = {
  inputTokens: 13939,
  outputTokens: 266,
  cacheCreationTokens: 0,
  cacheReadTokens: 30720,
  thinkingTokens: 0,
  factoryCredits: 2004,
};

const addUsage = (left: Usage, right: Usage): Usage => ({
  inputTokens: left.inputTokens + right.inputTokens,
  outputTokens: left.outputTokens + right.outputTokens,
  cacheCreationTokens: left.cacheCreationTokens + right.cacheCreationTokens,
  cacheReadTokens: left.cacheReadTokens + right.cacheReadTokens,
  thinkingTokens: left.thinkingTokens + right.thinkingTokens,
  factoryCredits: left.factoryCredits + right.factoryCredits,
});

interface Session {
  readonly id: string;
  settings: Json;
  readonly messages: Array<Json>;
  usage: Usage;
  seq: number;
  /** Guards `compact_session`: Droid refuses to compact a session with no finished turn. */
  hasCompletedTurn: boolean;
}

const sessions = new Map<string, Session>();
let session: Session | undefined;

const stateDir = process.env.DROID_MOCK_STATE_DIR || undefined;
const SAFE_SESSION_ID = /^[A-Za-z0-9_-]+$/;

function makeSession(
  id: string,
  settings: Json,
  messages: Array<Json> = [],
  usage: Usage = ZERO_USAGE,
  seq: number = messages.length,
): Session {
  const created: Session = {
    id,
    settings,
    messages,
    usage,
    seq,
    hasCompletedTurn: messages.length > 0,
  };
  sessions.set(id, created);
  return created;
}

const stateFilePath = (dir: string, id: string): string => NodePath.join(dir, `${id}.json`);

function saveSession(saved: Session): void {
  if (stateDir === undefined) {
    return;
  }
  const { settings, messages, usage, seq } = saved;
  NodeFS.writeFileSync(
    stateFilePath(stateDir, saved.id),
    JSON.stringify({ settings, messages, usage, seq }),
    "utf8",
  );
}

/** Call before notifying `create_message`, so anything the client has seen survives an exit. */
function appendMessage(target: Session, message: Json): void {
  target.messages.push(message);
  saveSession(target);
}

function readUsage(value: unknown): Usage {
  if (!isRecord(value)) {
    return ZERO_USAGE;
  }
  const field = (key: keyof Usage): number => {
    const count = value[key];
    return typeof count === "number" ? count : 0;
  };
  return {
    inputTokens: field("inputTokens"),
    outputTokens: field("outputTokens"),
    cacheCreationTokens: field("cacheCreationTokens"),
    cacheReadTokens: field("cacheReadTokens"),
    thinkingTokens: field("thinkingTokens"),
    factoryCredits: field("factoryCredits"),
  };
}

/** Reads a session an earlier mock process saved under `DROID_MOCK_STATE_DIR`. */
function readSavedSession(id: string): Session | undefined {
  if (stateDir === undefined) {
    return undefined;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(NodeFS.readFileSync(stateFilePath(stateDir, id), "utf8"));
  } catch {
    return undefined;
  }
  if (!isRecord(parsed) || !isRecord(parsed.settings) || !Array.isArray(parsed.messages)) {
    return undefined;
  }
  const messages = parsed.messages.filter(isRecord);
  const seq = typeof parsed.seq === "number" ? parsed.seq : messages.length;
  return makeSession(id, parsed.settings, messages, readUsage(parsed.usage), seq);
}

// ---------------------------------------------------------------------------
// RPC mode: turns and holds
// ---------------------------------------------------------------------------

interface Waiter<T> {
  readonly promise: Promise<T>;
  readonly resolve: (value: T) => void;
}

function makeWaiter<T>(): Waiter<T> {
  let resolve: (value: T) => void = () => undefined;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

type Wake =
  | { readonly kind: "interrupt"; readonly requestId: string }
  | { readonly kind: "eof" }
  | { readonly kind: "answer"; readonly message: Json }
  /** A mid-turn `add_user_message`; the scenario owning the hold acknowledges it. */
  | {
      readonly kind: "steer";
      readonly text: string;
      readonly messageId: string;
      readonly requestId: string;
    };

interface Turn {
  readonly id: string;
  readonly requestId: string;
  readonly text: string;
  readonly startedAt: number;
  /** Mutable: a spec handoff (SWITCH_SESSION) moves the turn's tail to the new session. */
  session: Session;
  hold: Waiter<Wake> | undefined;
  openRequestId: string | undefined;
  reachedEof: boolean;
}

let activeTurn: Turn | undefined;
let stubborn = process.env.DROID_MOCK_STUBBORN === "1";
let stdinEnded = false;
let closed = false;
let toolCounter = 0;

/** Mock-originated request ids awaiting a client response. */
const openMockRequests = new Set<string>();
/** Requests settled by an interrupt; a late answer only produces another `idle`. */
const cancelledMockRequests = new Set<string>();

function hold(turn: Turn): Promise<Wake> {
  const waiter = makeWaiter<Wake>();
  turn.hold = waiter;
  return waiter.promise;
}

function wake(turn: Turn, value: Wake): boolean {
  const waiter = turn.hold;
  if (waiter === undefined) {
    return false;
  }
  turn.hold = undefined;
  waiter.resolve(value);
  return true;
}

function sendMockRequest(turn: Turn, method: string, params: Json): string {
  const id = NodeCrypto.randomUUID();
  openMockRequests.add(id);
  turn.openRequestId = id;
  send({
    type: "request",
    jsonrpc: "2.0",
    factoryApiVersion: API_VERSION,
    factoryProtocolVersion: PROTOCOL_VERSION,
    method,
    id,
    params,
  });
  return id;
}

const workingState = (newState: string): void =>
  notify({ type: "droid_working_state_changed", newState });

const nextAssistantId = (): string => NodeCrypto.randomUUID();

function nextSeq(turn: Turn): number {
  turn.session.seq += 1;
  return turn.session.seq;
}

function assistantModel(turn: Turn): Json {
  if (turn.text.includes("REROUTE_ME")) {
    return { modelId: "deepseek-v4.1-flash", apiProvider: "baseten", reasoningEffort: "high" };
  }
  const settings = turn.session.settings;
  if (settings.modelId === "auto") {
    return { modelId: "gpt-6-sol", apiProvider: "openai", reasoningEffort: "medium" };
  }
  const provider = MODELS.find((entry) => entry.id === settings.modelId)?.modelProvider;
  return {
    modelId: settings.modelId,
    apiProvider: provider === "anthropic" || provider === "openai" ? provider : "baseten",
    reasoningEffort: settings.reasoningEffort,
  };
}

function createAssistantMessage(turn: Turn, id: string, content: Array<Json>): void {
  const now = Date.now();
  const message: Json = {
    id,
    role: "assistant",
    content,
    parentId: turn.id,
    seq: nextSeq(turn),
    createdAt: now,
    updatedAt: now,
    ...assistantModel(turn),
  };
  appendMessage(turn.session, message);
  notify({ type: "create_message", message });
}

function streamThinking(turn: Turn, messageId: string): void {
  workingState("thinking");
  for (const textDelta of ["Just", " answering"]) {
    notify({ type: "thinking_text_delta", messageId, blockIndex: 0, textDelta });
  }
  notify({ type: "thinking_text_complete", messageId, blockIndex: 0 });
}

function startTurn(text: string, messageId: string, requestId: string, current: Session): Turn {
  const turn: Turn = {
    id: messageId,
    requestId,
    text,
    startedAt: Date.now(),
    session: current,
    hold: undefined,
    openRequestId: undefined,
    reachedEof: false,
  };
  activeTurn = turn;
  workingState("streaming_assistant_message");
  const firstTurn = current.messages.length === 0;
  const now = Date.now();
  if (firstTurn) {
    // Live Droid persists the context message; it shows up in `load_session` history.
    const contextMessage: Json = {
      id: `context-${messageId}`,
      role: "user",
      content: [],
      visibility: "llm_only",
      parentId: null,
      seq: nextSeq(turn),
      createdAt: now,
      updatedAt: now,
    };
    appendMessage(current, contextMessage);
    notify({ type: "create_message", message: contextMessage });
  }
  const userMessage: Json = {
    id: messageId,
    role: "user",
    content: [{ type: "text", text }],
    interactionMode: current.settings.interactionMode ?? "auto",
    parentId: firstTurn ? `context-${messageId}` : (current.messages.at(-1)?.id ?? null),
    seq: nextSeq(turn),
    createdAt: now,
    updatedAt: now,
  };
  appendMessage(current, userMessage);
  notify({ type: "create_message", message: userMessage, requestId });
  return turn;
}

function emitUsage(current: Session, usage: Usage): void {
  notifyUnscoped({
    type: "session_token_usage_changed",
    sessionId: current.id,
    tokenUsage: current.usage,
    inclusiveTokenUsage: current.usage,
    lastCallTokenUsage: {
      inputTokens: usage.inputTokens,
      outputTokens: usage.outputTokens,
      cacheReadTokens: usage.cacheReadTokens,
    },
  });
}

function finishTurn(
  turn: Turn,
  reason: string,
  usage: Usage = TURN_USAGE,
  trailingIdle: boolean = true,
): void {
  const current = turn.session;
  current.usage = addUsage(current.usage, usage);
  current.hasCompletedTurn = true;
  if (usage !== ZERO_USAGE) {
    emitUsage(current, usage);
  }
  const finalReason = turn.reachedEof && reason === "completed" ? "process_exit" : reason;
  // A turn completes on the session it started in, even after a session switch.
  notify(
    {
      type: "agent_turn_completed",
      reason: finalReason,
      turnId: turn.id,
      tokenUsage: usage,
      cumulativeTokenUsage: current.usage,
      childTokenUsage: ZERO_USAGE,
      cumulativeChildTokenUsage: ZERO_USAGE,
      durationMs: Date.now() - turn.startedAt,
    },
    current.id,
  );
  saveSession(current);
  // A handoff's `spec_handoff` completion is the old session's final event: no idle after it.
  if (trailingIdle) {
    workingState("idle");
    if (finalReason === "completed") {
      // Real Droid often reports `idle` twice at the end of a turn.
      workingState("idle");
    }
  }
  if (activeTurn === turn) {
    activeTurn = undefined;
  }
  if (stdinEnded && !stubborn) {
    exitAfterFlush(0);
  }
}

function emitInterruptTail(turn: Turn, interruptRequestId: string): void {
  const now = Date.now();
  for (const [id, text] of [
    ["sys-cancelled", "Request cancelled by user"],
    ["sys-interrupted", "Request interrupted by user"],
  ] as const) {
    notify({
      type: "create_message",
      message: {
        id: `${id}-${turn.id}`,
        role: "system",
        content: [{ type: "text", text }],
        visibility: "llm_only",
        parentId: turn.id,
        seq: nextSeq(turn),
        createdAt: now,
        updatedAt: now,
      },
    });
  }
  respond(interruptRequestId, {});
  finishTurn(turn, "cancelled", ZERO_USAGE);
  // Live stragglers after the cancelled completion.
  notify({ type: "session_title_updated", title: "Mock title", updateType: "llm_generated" });
  emitUsage(turn.session, ZERO_USAGE);
}

/** S0: thinking + text in one assistant message, title update, `completed`. */
function finishPlainTurn(
  turn: Turn,
  extraBetweenDeltas?: () => void,
  extraBeforeFinish?: () => void,
): void {
  const messageId = nextAssistantId();
  streamThinking(turn, messageId);
  workingState("streaming_assistant_message");
  notify({ type: "assistant_text_delta", messageId, blockIndex: 1, textDelta: "mock " });
  extraBetweenDeltas?.();
  notify({ type: "assistant_text_delta", messageId, blockIndex: 1, textDelta: "reply" });
  notify({ type: "assistant_text_complete", messageId, blockIndex: 1 });
  createAssistantMessage(turn, messageId, [
    { type: "thinking", thinking: "Just answering" },
    { type: "text", text: "mock reply" },
  ]);
  extraBeforeFinish?.();
  notify({ type: "session_title_updated", title: "Mock title", updateType: "llm_generated" });
  finishTurn(turn, "completed");
}

// --- tools -----------------------------------------------------------------

interface MockTool {
  readonly id: string;
  readonly name: string;
  readonly input: Json;
  readonly confirmationType: string;
  readonly details: Json;
  readonly result: string;
}

function executeTool(command: string, result: string): MockTool {
  toolCounter += 1;
  const executable = command.split(" ")[0] ?? command;
  return {
    id: `tool-${toolCounter}`,
    name: "Execute",
    input: { summary: `Run ${command}`, command, riskLevel: "low" },
    confirmationType: "exec",
    details: {
      type: "exec",
      fullCommand: command,
      command: executable,
      extractedCommands: [executable],
      impactLevel: "low",
    },
    result,
  };
}

function createFileTool(): MockTool {
  toolCounter += 1;
  return {
    id: `tool-${toolCounter}`,
    name: "Create",
    input: { file_path: "/workspace/project/out.txt", content: "hello\n" },
    confirmationType: "create",
    details: {
      type: "create",
      filePath: "/workspace/project/out.txt",
      fileName: "out.txt",
      content: "hello\n",
    },
    result: JSON.stringify({ success: true, file_path: "/workspace/project/out.txt" }),
  };
}

/** Assistant message that announces the tool; the first `tool_call` has empty input. */
function announceTool(turn: Turn, tool: MockTool): void {
  const messageId = nextAssistantId();
  streamThinking(turn, messageId);
  workingState("streaming_assistant_message");
  notify({ type: "assistant_text_delta", messageId, blockIndex: 1, textDelta: "\n\n" });
  notifyUnscoped({
    type: "tool_call",
    toolUse: { type: "tool_use", id: tool.id, name: tool.name, input: {} },
  });
  notify({ type: "assistant_text_complete", messageId, blockIndex: 1 });
  notifyUnscoped({
    type: "tool_call",
    toolUse: { type: "tool_use", id: tool.id, name: tool.name, input: tool.input },
  });
  createAssistantMessage(turn, messageId, [
    { type: "thinking", thinking: "Just answering" },
    { type: "text", text: "\n\n" },
    { type: "tool_use", id: tool.id, name: tool.name, input: tool.input },
  ]);
  workingState("executing_tool");
  notify({
    type: "tool_execution_phase_changed",
    toolUseId: tool.id,
    toolName: tool.name,
    phase: "queued",
  });
}

function runTool(tool: MockTool): void {
  notify({
    type: "tool_execution_phase_changed",
    toolUseId: tool.id,
    toolName: tool.name,
    phase: "executing",
  });
  if (tool.name === "Execute") {
    const timestamp = Date.now();
    for (const offset of [0, 12]) {
      notify({
        type: "tool_progress_update",
        toolUseId: tool.id,
        toolName: tool.name,
        update: { type: "status", text: "", timestamp: timestamp + offset, terminalId: "term-1" },
      });
    }
  }
  log({ type: "tool_executed", id: tool.id, name: tool.name });
  notify({
    type: "tool_result",
    toolUseId: tool.id,
    messageId: `tm-${tool.id}`,
    content: tool.result,
    isError: false,
  });
}

/** Second model call after a tool: final text `DONE`, then `completed`. */
function finishAfterTool(turn: Turn): void {
  workingState("streaming_assistant_message");
  workingState("thinking");
  workingState("streaming_assistant_message");
  const messageId = nextAssistantId();
  for (const textDelta of ["DO", "NE"]) {
    notify({ type: "assistant_text_delta", messageId, blockIndex: 0, textDelta });
  }
  notify({ type: "assistant_text_complete", messageId, blockIndex: 0 });
  createAssistantMessage(turn, messageId, [{ type: "text", text: "DONE" }]);
  finishTurn(turn, "completed");
}

function cancelTool(tool: MockTool, content: string): void {
  notify({
    type: "tool_result",
    toolUseId: tool.id,
    messageId: `tm-${tool.id}`,
    content,
    isError: true,
  });
}

const PERMISSION_OPTIONS: ReadonlyArray<Json> = [
  { label: "Yes, allow", value: "proceed_once", selectedColor: "#d78700" },
  {
    label: "Yes, and always allow low impact commands (file edits and read-only commands)",
    value: "proceed_always",
    selectedColor: "#d78700",
  },
  { label: "No, cancel", value: "cancel", selectedColor: "#d75f5f", selectedPrefix: "✕ " },
];

/** S2/S4: tool behind `droid.request_permission`. */
async function runPermissionTurn(turn: Turn, tool: MockTool): Promise<void> {
  announceTool(turn, tool);
  if (autonomyRank(turn.session.settings.autonomyLevel) >= autonomyRank("low")) {
    runTool(tool);
    finishAfterTool(turn);
    return;
  }
  workingState("waiting_for_tool_confirmation");
  const permissionId = sendMockRequest(turn, "droid.request_permission", {
    toolUses: [
      {
        toolUse: { type: "tool_use", id: tool.id, name: tool.name, input: tool.input },
        confirmationType: tool.confirmationType,
        details: tool.details,
      },
    ],
    options: PERMISSION_OPTIONS,
  });
  // Live, usage and title updates keep arriving while the request is open.
  emitUsage(turn.session, ZERO_USAGE);
  notify({ type: "session_title_updated", title: "Mock title", updateType: "llm_generated" });

  const woken = await hold(turn);
  turn.openRequestId = undefined;
  if (woken.kind === "eof") {
    openMockRequests.delete(permissionId);
    cancelledMockRequests.add(permissionId);
    finishTurn(turn, "process_exit", ZERO_USAGE);
    return;
  }
  if (woken.kind === "interrupt") {
    openMockRequests.delete(permissionId);
    cancelledMockRequests.add(permissionId);
    notifyUnscoped({
      type: "permission_resolved",
      requestId: permissionId,
      toolUseIds: [],
      selectedOption: "cancel",
    });
    // Real Droid reports the cancelled tool twice.
    cancelTool(tool, "Error: Tool execution cancelled by user");
    cancelTool(tool, "Error: Tool execution cancelled: Batch cancelled");
    respond(woken.requestId, {});
    finishTurn(turn, "cancelled", ZERO_USAGE);
    return;
  }
  if (woken.kind === "steer") {
    respond(woken.requestId, {});
    finishTurn(turn, "process_exit", ZERO_USAGE);
    return;
  }

  const answer = woken.message;
  const error = isRecord(answer.error) ? answer.error : undefined;
  const result = isRecord(answer.result) ? answer.result : undefined;
  const selected = result?.selectedOption;
  const offered = PERMISSION_OPTIONS.some((option) => option.value === selected);
  if (error === undefined && !offered) {
    log({ type: "invalid_option", id: permissionId, selectedOption: selected ?? null });
  }
  if (error !== undefined || !offered || selected === "cancel") {
    workingState("streaming_assistant_message");
    notifyUnscoped({
      type: "permission_resolved",
      requestId: permissionId,
      toolUseIds: [],
      selectedOption: "cancel",
    });
    cancelTool(
      tool,
      error === undefined
        ? "Error: Tool execution cancelled by user"
        : `Error: Tool execution cancelled: [code ${String(error.code)}] ${String(error.message)}`,
    );
    // A permission cancel ends the turn `completed`, not `permission_rejected`.
    finishTurn(turn, "completed");
    return;
  }

  workingState("executing_tool");
  notifyUnscoped({
    type: "permission_resolved",
    requestId: permissionId,
    toolUseIds: [tool.id],
    selectedOption: selected,
  });
  if (selected === "proceed_always") {
    turn.session.settings = mergeSettings(turn.session.settings, { autonomyLevel: "low" });
    notifyUnscoped({ type: "settings_updated", settings: turn.session.settings });
  }
  runTool(tool);
  finishAfterTool(turn);
}

const PLAN_TEXT = "Create `hello.txt` in the working directory containing exactly `hi`.";

const EXIT_SPEC_MODE_OPTIONS: ReadonlyArray<Json> = [
  { label: "Proceed with implementation", value: "proceed_once", selectedColor: "#d78700" },
  {
    label: "Proceed, and allow file edits and read-only commands (Low)",
    value: "proceed_auto_run_low",
    selectedColor: "#d78700",
  },
  {
    label: "Proceed, and allow reversible commands (Medium)",
    value: "proceed_auto_run_medium",
    selectedColor: "#d78700",
  },
  {
    label: "Proceed, and allow all commands (High)",
    value: "proceed_auto_run_high",
    selectedColor: "#d75f5f",
  },
  {
    label: "Proceed in a new session (no autonomy)",
    value: "proceed_new_session",
    selectedColor: "#d78700",
  },
  {
    label: "Proceed in a new session (Low autonomy)",
    value: "proceed_new_session_low",
    selectedColor: "#d78700",
  },
  {
    label: "Proceed in a new session (Medium autonomy)",
    value: "proceed_new_session_medium",
    selectedColor: "#d78700",
  },
  {
    label: "Proceed in a new session (High autonomy)",
    value: "proceed_new_session_high",
    selectedColor: "#d75f5f",
  },
  { label: "No, keep iterating on spec", value: "cancel", selectedColor: "#d75f5f" },
];

function exitSpecModeTool(): MockTool {
  toolCounter += 1;
  return {
    id: `tool-${toolCounter}`,
    name: "ExitSpecMode",
    input: { plan: PLAN_TEXT },
    confirmationType: "exit_spec_mode",
    details: { type: "exit_spec_mode", plan: PLAN_TEXT },
    result: JSON.stringify({
      approved: true,
      message: "Spec mode exited. The user approved the plan. Continue with the implementation.",
      filePath: "/workspace/project/droid/specs/2026-10-15-create-hello-txt.md",
      isEdited: false,
    }),
  };
}

/** Settles a held exit_spec_mode prompt; returns the selected option, or undefined on cancel. */
function settleSpecPrompt(
  turn: Turn,
  tool: MockTool,
  permissionId: string,
  woken: Wake,
): string | undefined {
  openMockRequests.delete(permissionId);
  cancelledMockRequests.add(permissionId);
  turn.openRequestId = undefined;
  if (woken.kind === "interrupt") {
    notifyUnscoped({
      type: "permission_resolved",
      requestId: permissionId,
      toolUseIds: [],
      selectedOption: "cancel",
    });
    cancelTool(tool, "Error: Tool execution cancelled by user");
    respond(woken.requestId, {});
    finishTurn(turn, "cancelled", ZERO_USAGE);
    return undefined;
  }
  if (woken.kind === "eof") {
    finishTurn(turn, "process_exit", ZERO_USAGE);
    return undefined;
  }
  if (woken.kind === "steer") {
    respond(woken.requestId, {});
    finishTurn(turn, "process_exit", ZERO_USAGE);
    return undefined;
  }
  const answer = woken.message;
  const error = isRecord(answer.error) ? answer.error : undefined;
  const result = isRecord(answer.result) ? answer.result : undefined;
  const selected = typeof result?.selectedOption === "string" ? result.selectedOption : undefined;
  const offered = EXIT_SPEC_MODE_OPTIONS.some((option) => option.value === selected);
  if (error === undefined && selected !== undefined && !offered) {
    log({ type: "invalid_option", id: permissionId, selectedOption: selected });
  }
  if (error !== undefined || !offered || selected === "cancel") {
    workingState("streaming_assistant_message");
    notifyUnscoped({
      type: "permission_resolved",
      requestId: permissionId,
      toolUseIds: [],
      selectedOption: "cancel",
    });
    cancelTool(tool, "Error: Tool execution cancelled by user");
    finishTurn(turn, "completed");
    return undefined;
  }
  return selected as string;
}

/**
 * S11: plan-mode exit_spec_mode prompt. `switchSession` replays the proceed_new_session_low
 * stream: the implementation continues on a fresh session id and the original turn ends with
 * `spec_handoff` after the Droid-generated `impl-turn` completes.
 */
async function runExitSpecModeTurn(turn: Turn, switchSession: boolean): Promise<void> {
  const tool = exitSpecModeTool();
  announceTool(turn, tool);
  workingState("waiting_for_tool_confirmation");
  const permissionId = sendMockRequest(turn, "droid.request_permission", {
    toolUses: [
      {
        toolUse: { type: "tool_use", id: tool.id, name: tool.name, input: tool.input },
        confirmationType: tool.confirmationType,
        details: tool.details,
      },
    ],
    options: EXIT_SPEC_MODE_OPTIONS,
  });
  const selected = settleSpecPrompt(turn, tool, permissionId, await hold(turn));
  if (selected === undefined) {
    return;
  }

  const oldSession = turn.session;
  workingState("executing_tool");
  notifyUnscoped({
    type: "permission_resolved",
    requestId: permissionId,
    toolUseIds: [tool.id],
    selectedOption: switchSession ? "proceed_new_session_low" : selected,
  });
  // Live: proceed_once drops autonomy to off (p13-spec-proceed_once); proceed_auto_run_<tier>
  // sets that tier (p19-plan-tier).
  const tier = /^proceed_auto_run_(low|medium|high)$/u.exec(selected)?.[1];
  oldSession.settings = mergeSettings(oldSession.settings, {
    interactionMode: "auto",
    autonomyLevel: tier ?? "off",
  });
  notifyUnscoped({ type: "settings_updated", settings: oldSession.settings });
  notify(
    {
      type: "tool_execution_phase_changed",
      toolUseId: tool.id,
      toolName: tool.name,
      phase: "executing",
    },
    oldSession.id,
  );
  notify(
    {
      type: "tool_result",
      toolUseId: tool.id,
      messageId: `tm-${tool.id}`,
      content: switchSession
        ? JSON.stringify({
            approved: true,
            message: "Spec mode exited. Handing off to a new session.",
            handoff: { plan: PLAN_TEXT, isNewSession: true },
            isEdited: false,
          })
        : tool.result,
      isError: false,
    },
    oldSession.id,
  );

  if (!switchSession) {
    // After proceed_once the follow-up Create still prompts in a real session, but no test
    // drives that path, so the turn simply continues and completes.
    finishAfterTool(turn);
    return;
  }

  const handedOff = makeSession(
    NodeCrypto.randomUUID(),
    mergeSettings(oldSession.settings, { autonomyLevel: "low" }),
  );
  saveSession(handedOff);
  log({ type: "session_switch", from: oldSession.id, to: handedOff.id });
  session = handedOff;
  notifyUnscoped({ type: "settings_updated", settings: handedOff.settings });

  workingState("streaming_assistant_message");
  const now = Date.now();
  const implMessage: Json = {
    id: "impl-turn",
    role: "user",
    content: [{ type: "text", text: `Implement the following plan:\n\n${PLAN_TEXT}` }],
    parentId: null,
    seq: handedOff.seq + 1,
    createdAt: now,
    updatedAt: now,
  };
  handedOff.seq += 1;
  appendMessage(handedOff, implMessage);
  notify({ type: "create_message", message: implMessage });
  workingState("thinking");

  turn.session = handedOff;
  // Live (p13-spec-proceed_new_session_low, line 42): the retired session's llm_generated
  // title still arrives mid-turn, after the handoff and before its trailing completion.
  notify(
    { type: "session_title_updated", title: "Mock title", updateType: "llm_generated" },
    oldSession.id,
  );
  const createTool = createFileTool();
  announceTool(turn, createTool);
  runTool(createTool);
  const messageId = nextAssistantId();
  notify({ type: "assistant_text_delta", messageId, blockIndex: 0, textDelta: "Implemented." });
  notify({ type: "assistant_text_complete", messageId, blockIndex: 0 });
  createAssistantMessage(turn, messageId, [{ type: "text", text: "Implemented." }]);
  handedOff.usage = addUsage(handedOff.usage, TURN_USAGE);
  emitUsage(handedOff, TURN_USAGE);
  notify(
    {
      type: "agent_turn_completed",
      reason: "completed",
      turnId: "impl-turn",
      tokenUsage: TURN_USAGE,
      cumulativeTokenUsage: handedOff.usage,
      childTokenUsage: ZERO_USAGE,
      cumulativeChildTokenUsage: ZERO_USAGE,
      durationMs: 1,
    },
    handedOff.id,
  );
  workingState("idle");
  // The original turn settles last, on the session it started in. Live, that spec_handoff
  // completion is the old session's final event: nothing follows it on either id.
  turn.session = oldSession;
  finishTurn(turn, "spec_handoff", ZERO_USAGE, false);
}

const ASK_USER_QUESTIONS: ReadonlyArray<Json> = [
  {
    index: 1,
    topic: "Favorite-Color",
    question: "What is your favorite color?",
    options: ["Red", "Blue"],
  },
  {
    index: 2,
    topic: "Letters",
    question: "Which letters do you like?",
    options: ["a", "b", "c"],
    multiSelect: true,
  },
];

function askUserTool(): MockTool {
  toolCounter += 1;
  return {
    id: `tool-${toolCounter}`,
    name: "AskUser",
    input: { questions: ASK_USER_QUESTIONS },
    confirmationType: "ask_user",
    details: { type: "ask_user" },
    result: "",
  };
}

/** S10: AskUser tool behind an inbound `droid.ask_user` request. */
async function runAskUserTurn(turn: Turn): Promise<void> {
  const tool = askUserTool();
  announceTool(turn, tool);
  notify({
    type: "tool_execution_phase_changed",
    toolUseId: tool.id,
    toolName: tool.name,
    phase: "executing",
  });
  workingState("waiting_for_tool_confirmation");
  const requestId = sendMockRequest(turn, "droid.ask_user", {
    toolCallId: tool.id,
    questions: ASK_USER_QUESTIONS,
  });

  const woken = await hold(turn);
  turn.openRequestId = undefined;
  if (woken.kind === "eof") {
    openMockRequests.delete(requestId);
    cancelledMockRequests.add(requestId);
    finishTurn(turn, "process_exit", ZERO_USAGE);
    return;
  }
  if (woken.kind === "interrupt") {
    openMockRequests.delete(requestId);
    cancelledMockRequests.add(requestId);
    cancelTool(tool, "Error: Tool execution cancelled by user");
    respond(woken.requestId, {});
    finishTurn(turn, "cancelled", ZERO_USAGE);
    return;
  }
  if (woken.kind === "steer") {
    respond(woken.requestId, {});
    finishTurn(turn, "process_exit", ZERO_USAGE);
    return;
  }

  openMockRequests.delete(requestId);
  const error = isRecord(woken.message.error) ? woken.message.error : undefined;
  const result = isRecord(woken.message.result) ? woken.message.result : undefined;
  const cancelled = error !== undefined || result?.cancelled === true;
  if (cancelled) {
    workingState("streaming_assistant_message");
    cancelTool(tool, "Error: Tool execution cancelled by user");
    finishTurn(turn, "completed");
    return;
  }
  const answers = Array.isArray(result?.answers) ? result.answers.filter(isRecord) : [];
  const content = answers
    .map(
      (answer, position) =>
        `${position + 1}. [question] ${String(answer.question)}\n[answer] ${String(answer.answer)}`,
    )
    .join("\n\n");
  workingState("streaming_assistant_message");
  notify({
    type: "tool_result",
    toolUseId: tool.id,
    messageId: `tm-${tool.id}`,
    content,
    isError: false,
  });
  finishAfterTool(turn);
}

const ASK_PERM_OPTIONS: ReadonlyArray<Json> = [
  { label: "Answer", value: "proceed_once", selectedColor: "#d78700" },
  { label: "No, cancel", value: "cancel", selectedColor: "#d75f5f", selectedPrefix: "✕ " },
];

/**
 * AskUser behind `droid.request_permission` with `confirmationType: "ask_user"` (SDK shape,
 * never observed live): the answers travel back in the permission result's `comment`, and an
 * interrupt resolves it with a `permission_resolved` cancel.
 */
async function runAskPermissionTurn(turn: Turn): Promise<void> {
  const tool = askUserTool();
  announceTool(turn, tool);
  notify({
    type: "tool_execution_phase_changed",
    toolUseId: tool.id,
    toolName: tool.name,
    phase: "executing",
  });
  workingState("waiting_for_tool_confirmation");
  const requestId = sendMockRequest(turn, "droid.request_permission", {
    toolUses: [
      {
        toolUse: { type: "tool_use", id: tool.id, name: tool.name, input: tool.input },
        confirmationType: "ask_user",
        details: {
          type: "ask_user",
          questionnaire: "What is your favorite color?",
          parsed: { questions: [ASK_USER_QUESTIONS[0]] },
        },
      },
    ],
    options: ASK_PERM_OPTIONS,
  });

  const woken = await hold(turn);
  turn.openRequestId = undefined;
  if (woken.kind === "eof") {
    openMockRequests.delete(requestId);
    cancelledMockRequests.add(requestId);
    finishTurn(turn, "process_exit", ZERO_USAGE);
    return;
  }
  if (woken.kind === "interrupt") {
    openMockRequests.delete(requestId);
    cancelledMockRequests.add(requestId);
    notifyUnscoped({
      type: "permission_resolved",
      requestId,
      toolUseIds: [],
      selectedOption: "cancel",
    });
    cancelTool(tool, "Error: Tool execution cancelled by user");
    respond(woken.requestId, {});
    finishTurn(turn, "cancelled", ZERO_USAGE);
    return;
  }
  if (woken.kind === "steer") {
    respond(woken.requestId, {});
    finishTurn(turn, "process_exit", ZERO_USAGE);
    return;
  }

  openMockRequests.delete(requestId);
  const answer = woken.message;
  const error = isRecord(answer.error) ? answer.error : undefined;
  const result = isRecord(answer.result) ? answer.result : undefined;
  const selected = typeof result?.selectedOption === "string" ? result.selectedOption : undefined;
  if (error !== undefined || selected !== "proceed_once") {
    workingState("streaming_assistant_message");
    notifyUnscoped({
      type: "permission_resolved",
      requestId,
      toolUseIds: [],
      selectedOption: "cancel",
    });
    cancelTool(tool, "Error: Tool execution cancelled by user");
    finishTurn(turn, "completed");
    return;
  }
  workingState("executing_tool");
  notifyUnscoped({
    type: "permission_resolved",
    requestId,
    toolUseIds: [tool.id],
    selectedOption: "proceed_once",
  });
  notify({
    type: "tool_result",
    toolUseId: tool.id,
    messageId: `tm-${tool.id}`,
    content: typeof result?.comment === "string" ? result.comment : "answered",
    isError: false,
  });
  finishAfterTool(turn);
}

/** S15: `droid.pick_options`; t3 answers it at once without opening a request. */
async function runPickOptionsTurn(turn: Turn): Promise<void> {
  toolCounter += 1;
  const tool: MockTool = {
    id: `tool-${toolCounter}`,
    name: "SlackChannelPicker",
    input: { channel: "#general" },
    confirmationType: "pick_options",
    details: { type: "pick_options" },
    result: "",
  };
  announceTool(turn, tool);
  notify({
    type: "tool_execution_phase_changed",
    toolUseId: tool.id,
    toolName: tool.name,
    phase: "executing",
  });
  workingState("waiting_for_tool_confirmation");
  const requestId = sendMockRequest(turn, "droid.pick_options", {
    toolCallId: tool.id,
    title: "Pick channels",
    rows: [
      { id: "c1", label: "#general" },
      { id: "c2", label: "#eng", detail: "Engineering" },
    ],
    multi: true,
    preselectedIds: [],
  });

  const woken = await hold(turn);
  turn.openRequestId = undefined;
  openMockRequests.delete(requestId);
  if (woken.kind === "eof") {
    cancelledMockRequests.add(requestId);
    finishTurn(turn, "process_exit", ZERO_USAGE);
    return;
  }
  if (woken.kind === "interrupt") {
    cancelledMockRequests.add(requestId);
    cancelTool(tool, "Error: Tool execution cancelled by user");
    respond(woken.requestId, {});
    finishTurn(turn, "cancelled", ZERO_USAGE);
    return;
  }
  workingState("streaming_assistant_message");
  cancelTool(tool, "Error: Tool execution cancelled by user");
  finishTurn(turn, "completed");
}

/** S3: partial reply, held until `interrupt_session`; no assistant message on cancel. */
async function runInterruptTurn(turn: Turn): Promise<void> {
  const messageId = nextAssistantId();
  streamThinking(turn, messageId);
  workingState("streaming_assistant_message");
  for (const textDelta of ["1\n2\n3\n", "4\n5\n6\n"]) {
    notify({ type: "assistant_text_delta", messageId, blockIndex: 1, textDelta });
  }
  const woken = await hold(turn);
  if (woken.kind === "interrupt") {
    emitInterruptTail(turn, woken.requestId);
    return;
  }
  notify({ type: "assistant_text_delta", messageId, blockIndex: 1, textDelta: "7\n" });
  notify({ type: "assistant_text_complete", messageId, blockIndex: 1 });
  createAssistantMessage(turn, messageId, [
    { type: "thinking", thinking: "Just answering" },
    { type: "text", text: "1\n2\n3\n4\n5\n6\n7\n" },
  ]);
  finishTurn(turn, "completed");
}

/** INTERRUPT_WEDGED: partial reply, then held; every interrupt errors and the turn stays silent. */
async function runInterruptWedgedTurn(turn: Turn): Promise<void> {
  const messageId = nextAssistantId();
  streamThinking(turn, messageId);
  workingState("streaming_assistant_message");
  notify({ type: "assistant_text_delta", messageId, blockIndex: 1, textDelta: "partial" });
  let interrupts = 0;
  for (;;) {
    const woken = await hold(turn);
    if (woken.kind === "eof") {
      finishTurn(turn, "process_exit", ZERO_USAGE);
      return;
    }
    if (woken.kind === "steer") {
      respond(woken.requestId, {});
      continue;
    }
    if (woken.kind !== "interrupt") continue;
    interrupts += 1;
    if (interrupts >= 2) {
      // The first interrupt is the user's; later ones are the watchdog's. The title update
      // is observable proof a follow-up interrupt landed even though both are answered
      // with an error.
      notify({ type: "session_title_updated", title: "Mock title", updateType: "llm_generated" });
    }
    respondError(woken.requestId, -32603, "Mock is wedged and cannot interrupt.");
  }
}

/** INTERRUPT_GHOST: partial reply, then held; interrupts are never answered, the turn never ends. */
async function runInterruptGhostTurn(turn: Turn): Promise<void> {
  const messageId = nextAssistantId();
  streamThinking(turn, messageId);
  workingState("streaming_assistant_message");
  notify({ type: "assistant_text_delta", messageId, blockIndex: 1, textDelta: "partial" });
  for (;;) {
    const woken = await hold(turn);
    if (woken.kind === "eof") {
      finishTurn(turn, "process_exit", ZERO_USAGE);
      return;
    }
    if (woken.kind === "steer") {
      respond(woken.requestId, {});
      continue;
    }
    if (woken.kind !== "interrupt") continue;
    // The interrupt is observed but never answered; the title update proves it landed.
    notify({ type: "session_title_updated", title: "Mock title", updateType: "llm_generated" });
  }
}

/** S12: a tool held before it runs. Interrupt cancels it; stdin EOF lets Droid keep working. */
async function runEofToolsTurn(turn: Turn): Promise<void> {
  const first = executeTool("sleep 4; echo a", "a\n\n\n[Process exited with code 0]");
  announceTool(turn, first);
  const woken = await hold(turn);
  if (woken.kind === "interrupt") {
    cancelTool(first, "Error: Tool execution cancelled by user");
    emitInterruptTail(turn, woken.requestId);
    return;
  }
  runTool(first);
  const second = executeTool("echo b", "b\n\n\n[Process exited with code 0]");
  announceTool(turn, second);
  runTool(second);
  finishAfterTool(turn);
}

/**
 * S13: a long silent tool. After the `executing` phase begins there is total silence (real
 * Droid sends no heartbeat); the hold ends on any client message.
 */
async function runLongToolTurn(turn: Turn): Promise<void> {
  const tool = executeTool("sleep 25; echo slept", "slept\n\n\n[Process exited with code 0]");
  announceTool(turn, tool);
  notify({
    type: "tool_execution_phase_changed",
    toolUseId: tool.id,
    toolName: tool.name,
    phase: "executing",
  });
  notify({
    type: "tool_progress_update",
    toolUseId: tool.id,
    toolName: tool.name,
    update: { type: "status", text: "", timestamp: Date.now(), terminalId: "term-1" },
  });
  const woken = await hold(turn);
  if (woken.kind === "interrupt") {
    cancelTool(tool, "Error: Tool execution cancelled by user");
    emitInterruptTail(turn, woken.requestId);
    return;
  }
  if (woken.kind === "steer") {
    respond(woken.requestId, {});
  }
  notify({
    type: "tool_progress_update",
    toolUseId: tool.id,
    toolName: tool.name,
    update: { type: "status", text: "", timestamp: Date.now(), terminalId: "term-1" },
  });
  log({ type: "tool_executed", id: tool.id, name: tool.name });
  notify({
    type: "tool_result",
    toolUseId: tool.id,
    messageId: `tm-${tool.id}`,
    content: tool.result,
    isError: false,
  });
  if (woken.kind === "steer") {
    echoSteerMessage(turn, woken);
  }
  finishAfterTool(turn);
}

/** STALL_AFTER_TOOL: the released tool's result is followed by more silence, not a completion. */
async function runStallAfterToolTurn(turn: Turn): Promise<void> {
  const tool = executeTool("sleep 25; echo slept", "slept\n\n\n[Process exited with code 0]");
  announceTool(turn, tool);
  notify({
    type: "tool_execution_phase_changed",
    toolUseId: tool.id,
    toolName: tool.name,
    phase: "executing",
  });
  const woken = await hold(turn);
  if (woken.kind === "interrupt") {
    cancelTool(tool, "Error: Tool execution cancelled by user");
    emitInterruptTail(turn, woken.requestId);
    return;
  }
  if (woken.kind === "steer") {
    respond(woken.requestId, {});
    // The folded steer's echo precedes the tool result: it maps to no canonical event, so
    // a test that observes the result must be able to assume the echo was processed first.
    echoSteerMessage(turn, woken);
  }
  log({ type: "tool_executed", id: tool.id, name: tool.name });
  notify({
    type: "tool_result",
    toolUseId: tool.id,
    messageId: `tm-${tool.id}`,
    content: tool.result,
    isError: false,
  });
  if (woken.kind === "eof") {
    finishAfterTool(turn);
    return;
  }
  // The tool is done but the turn stays silent; only an interrupt (or EOF) ends it.
  const second = await hold(turn);
  if (second.kind === "interrupt") {
    emitInterruptTail(turn, second.requestId);
    return;
  }
  if (second.kind === "steer") {
    respond(second.requestId, {});
    echoSteerMessage(turn, second);
  }
  finishAfterTool(turn);
}

/** The steer's user message, appended and echoed as Droid does (no `context-` message). */
function echoSteerMessage(turn: Turn, steer: Extract<Wake, { readonly kind: "steer" }>): void {
  const now = Date.now();
  const message: Json = {
    id: steer.messageId,
    role: "user",
    content: [{ type: "text", text: steer.text }],
    interactionMode: turn.session.settings.interactionMode ?? "auto",
    parentId: turn.session.messages.at(-1)?.id ?? null,
    seq: nextSeq(turn),
    createdAt: now,
    updatedAt: now,
  };
  appendMessage(turn.session, message);
  notify({ type: "create_message", message, requestId: steer.requestId });
}

/** Second reply after a steer was folded in: text `QUEUED`, no completion of its own. */
function streamQueuedReply(turn: Turn): void {
  const messageId = nextAssistantId();
  streamThinking(turn, messageId);
  workingState("streaming_assistant_message");
  for (const textDelta of ["QUE", "UED"]) {
    notify({ type: "assistant_text_delta", messageId, blockIndex: 1, textDelta });
  }
  notify({ type: "assistant_text_complete", messageId, blockIndex: 1 });
  createAssistantMessage(turn, messageId, [
    { type: "thinking", thinking: "Just answering" },
    { type: "text", text: "QUEUED" },
  ]);
}

/** S5: the steer is folded into the running turn; one completion, with the first turn id. */
async function runSteerTurn(turn: Turn): Promise<void> {
  const first = nextAssistantId();
  streamThinking(turn, first);
  const steer = await hold(turn);
  if (steer.kind === "interrupt") {
    emitInterruptTail(turn, steer.requestId);
    return;
  }
  if (steer.kind !== "steer") {
    finishTurn(turn, "process_exit", ZERO_USAGE);
    return;
  }
  respond(steer.requestId, {});
  workingState("streaming_assistant_message");
  for (const textDelta of ["mock ", "reply"]) {
    notify({ type: "assistant_text_delta", messageId: first, blockIndex: 1, textDelta });
  }
  notify({ type: "assistant_text_complete", messageId: first, blockIndex: 1 });
  createAssistantMessage(turn, first, [
    { type: "thinking", thinking: "Just answering" },
    { type: "text", text: "mock reply" },
  ]);
  emitUsage(turn.session, TURN_USAGE);
  echoSteerMessage(turn, steer);
  emitSteerReminder(turn, steer.messageId);
  streamQueuedReply(turn);
  finishTurn(turn, "completed");
}

/** Trails a message folded into a running turn; live Droid persists it (p18 history). */
function emitSteerReminder(turn: Turn, steerMessageId: string): void {
  const reminder: Json = {
    id: `__internal__system-reminder__${NodeCrypto.randomUUID()}`,
    role: "user",
    content: [
      {
        type: "text",
        text: "<system-reminder>A follow-up user message arrived while you were working. Treat it as steering, not implicit cancellation. Continue all earlier actionable instructions unless the user explicitly canceled or replaced them. Respect blockers that require user input.</system-reminder>",
      },
    ],
    visibility: "llm_only",
    parentId: steerMessageId,
    seq: nextSeq(turn),
    createdAt: Date.now(),
    updatedAt: Date.now(),
  };
  appendMessage(turn.session, reminder);
  notify({ type: "create_message", message: reminder });
}

/** S6: an interrupt discards the queued steer; Droid says so with the steer's request id. */
async function runSteerDropTurn(turn: Turn): Promise<void> {
  const messageId = nextAssistantId();
  streamThinking(turn, messageId);
  const steer = await hold(turn);
  if (steer.kind === "interrupt") {
    emitInterruptTail(turn, steer.requestId);
    return;
  }
  if (steer.kind !== "steer") {
    finishTurn(turn, "process_exit", ZERO_USAGE);
    return;
  }
  respond(steer.requestId, {});
  notify({ type: "thinking_text_delta", messageId, blockIndex: 0, textDelta: " more" });
  const woken = await hold(turn);
  if (woken.kind === "interrupt") {
    notifyUnscoped({
      type: "queued_messages_discarded",
      text: steer.text,
      requestId: steer.requestId,
    });
    emitInterruptTail(turn, woken.requestId);
    if (turn.text.includes("STEER_DROP_ECHO")) {
      // SYNTHETIC race: the discard report and the queued message's delivery cross, so the
      // steer's echo and completion arrive after the turn ended. A client that forgot the
      // discard would promote a phantom turn here. The trailing title update lets a test
      // observe that the echo was processed.
      echoSteerMessage(turn, steer);
      notify({
        type: "agent_turn_completed",
        reason: "completed",
        turnId: steer.messageId,
        tokenUsage: ZERO_USAGE,
        cumulativeTokenUsage: turn.session.usage,
        childTokenUsage: ZERO_USAGE,
        cumulativeChildTokenUsage: ZERO_USAGE,
        durationMs: 1,
      });
      notify({ type: "session_title_updated", title: "after echo", updateType: "manual_rename" });
      workingState("idle");
    }
    return;
  }
  finishTurn(turn, "process_exit", ZERO_USAGE);
}

/**
 * S7: the running turn completes before the steer's user message lands; the steer then runs
 * as its own turn whose completion carries the steer's messageId.
 */
async function runLateSteerTurn(turn: Turn): Promise<void> {
  const first = nextAssistantId();
  streamThinking(turn, first);
  workingState("streaming_assistant_message");
  notify({ type: "assistant_text_delta", messageId: first, blockIndex: 1, textDelta: "first" });
  const steer = await hold(turn);
  if (steer.kind === "interrupt") {
    emitInterruptTail(turn, steer.requestId);
    return;
  }
  if (steer.kind !== "steer") {
    finishTurn(turn, "process_exit", ZERO_USAGE);
    return;
  }
  notify({ type: "assistant_text_complete", messageId: first, blockIndex: 1 });
  createAssistantMessage(turn, first, [
    { type: "thinking", thinking: "Just answering" },
    { type: "text", text: "first" },
  ]);
  // Live (p13-race-at_first_delta): the ack lands after the last assistant message and
  // before the first turn's completion, which precedes the steer's user create_message.
  respond(steer.requestId, {});
  finishTurn(turn, "completed");

  if (turn.text.includes("LATE_STEER_FOLD")) {
    const steerMessage: Json = {
      id: steer.messageId,
      role: "user",
      content: [{ type: "text", text: steer.text }],
      parentId: turn.session.messages.at(-1)?.id ?? null,
    };
    deferredDroidTurn = (followUp) =>
      runDroidInitiatedTurn(turn.session, steerMessage, steer.requestId, followUp, true);
    return;
  }
  workingState("streaming_assistant_message");
  echoSteerMessage(turn, steer);
  workingState("thinking");
  notify({ type: "session_title_updated", title: "Mock title", updateType: "llm_generated" });
  const second = nextAssistantId();
  streamThinking(turn, second);
  workingState("streaming_assistant_message");
  for (const textDelta of ["sec", "ond"]) {
    notify({ type: "assistant_text_delta", messageId: second, blockIndex: 0, textDelta });
  }
  notify({ type: "assistant_text_complete", messageId: second, blockIndex: 0 });
  createAssistantMessage(turn, second, [{ type: "text", text: "second" }]);
  turn.session.usage = addUsage(turn.session.usage, TURN_USAGE);
  emitUsage(turn.session, TURN_USAGE);
  notify({
    type: "agent_turn_completed",
    reason: "completed",
    turnId: steer.messageId,
    tokenUsage: TURN_USAGE,
    cumulativeTokenUsage: turn.session.usage,
    childTokenUsage: ZERO_USAGE,
    cumulativeChildTokenUsage: ZERO_USAGE,
    durationMs: Date.now() - turn.startedAt,
  });
  workingState("idle");
  workingState("idle");
  if (stdinEnded && !stubborn) {
    exitAfterFlush(0);
  }
}

interface FollowUp {
  readonly text: string;
  readonly messageId: string;
  readonly requestId: string;
}

/**
 * SYNTHETIC timing: real Droid starts a turn of its own (late steer, background task report)
 * as soon as it can, so a client message sent right then lands while that turn runs. The
 * mock defers such a turn until the client's next message to make that race deterministic.
 */
let deferredDroidTurn: ((followUp: FollowUp) => void) | undefined;

/**
 * A turn Droid starts itself, opened by `opening` (its id is the completion's `turnId`). A
 * follow-up that lands while it runs is either folded in (`fold`: echoed with the steer
 * reminder, one completion with the Droid turn's id) or, when the loop already made its last
 * model call, run as its own turn after that completion.
 */
function runDroidInitiatedTurn(
  current: Session,
  opening: Json,
  openingRequestId: string | undefined,
  followUp: FollowUp | undefined,
  fold: boolean,
): void {
  const id = String(opening.id);
  const turn: Turn = {
    id,
    requestId: openingRequestId ?? id,
    text: "",
    startedAt: Date.now(),
    session: current,
    hold: undefined,
    openRequestId: undefined,
    reachedEof: false,
  };
  activeTurn = turn;
  workingState("streaming_assistant_message");
  const now = Date.now();
  const stored: Json = { ...opening, seq: nextSeq(turn), createdAt: now, updatedAt: now };
  // Droid streams a task report as a system message but stores it as an llm_only user one.
  appendMessage(current, stored.role === "system" ? { ...stored, role: "user" } : stored);
  notify({
    type: "create_message",
    message: stored,
    requestId: openingRequestId ?? id,
  });
  if (followUp !== undefined && fold) {
    echoSteerMessage(turn, { kind: "steer", ...followUp });
    emitSteerReminder(turn, followUp.messageId);
  }
  finishPlainTurn(turn);
  if (followUp !== undefined && !fold) {
    finishPlainTurn(startTurn(followUp.text, followUp.messageId, followUp.requestId, current));
  }
}

/** Droid's background-task report (the CLI's template; p17 live text). */
function taskReportMessage(current: Session, childSessionId: string): Json {
  const text = [
    "Background task completed.",
    `task_id: ${childSessionId}`,
    "type: probe-slow",
    "reason: completed",
    "description: bg",
    "output: sub-ok",
    "",
    `Background task "bg" (${childSessionId}) just completed. This is a new asynchronous completion event, not a repeat of the launch request. Do not repeat the earlier launch acknowledgement. Briefly report this task's terminal outcome or use its result for any necessary follow-up.`,
  ].join("\n");
  return {
    id: `task-completion:${NodeCrypto.randomUUID()}`,
    role: "system",
    content: [{ type: "text", text }],
    visibility: "llm_only",
    parentId: current.messages.at(-1)?.id ?? null,
  };
}

/**
 * S12 (p17): a Task subagent launched with `await: false`. The launch turn completes; the
 * report then runs as a turn Droid starts itself (`BG_TASK`), or is deferred until the
 * client's next message, which folds into it (`BG_TASK_FOLD`) or runs after it
 * (`BG_TASK_BEHIND`).
 */
function runBackgroundTaskTurn(turn: Turn): void {
  toolCounter += 1;
  const tool: MockTool = {
    id: `tool-${toolCounter}`,
    name: "Task",
    input: { subagent_type: "probe-slow", description: "bg", prompt: "Go.", await: false },
    confirmationType: "none",
    details: {},
    result: "",
  };
  const childSessionId = NodeCrypto.randomUUID();
  announceTool(turn, tool);
  notify({
    type: "tool_execution_phase_changed",
    toolUseId: tool.id,
    toolName: tool.name,
    phase: "executing",
  });
  notify({
    type: "child_session_available",
    childSessionId,
    toolUseId: tool.id,
    subagentType: "probe-slow",
    description: "bg",
    timestamp: Date.now(),
  });
  notify({
    type: "tool_result",
    toolUseId: tool.id,
    messageId: `tm-${tool.id}`,
    content: `Task launched in background.\ntask_id: ${childSessionId}\nsession_id: ${childSessionId}`,
    isError: false,
  });
  finishAfterTool(turn);
  const current = turn.session;
  const fold = turn.text.includes("BG_TASK_FOLD");
  if (fold || turn.text.includes("BG_TASK_BEHIND")) {
    deferredDroidTurn = (followUp) =>
      runDroidInitiatedTurn(
        current,
        taskReportMessage(current, childSessionId),
        undefined,
        followUp,
        fold,
      );
    return;
  }
  runDroidInitiatedTurn(
    current,
    taskReportMessage(current, childSessionId),
    undefined,
    undefined,
    false,
  );
}

/** S8: a retry retracts the partial reply; the same messageId streams again from scratch. */
function runRetractTurn(turn: Turn): void {
  const messageId = nextAssistantId();
  workingState("thinking");
  notify({ type: "assistant_text_delta", messageId, blockIndex: 1, textDelta: "partial " });
  notify({ type: "llm_retry", attempt: 1, reason: "rate_limited" });
  notify({ type: "assistant_message_retracted", messageId });
  streamThinking(turn, messageId);
  workingState("streaming_assistant_message");
  for (const textDelta of ["QUE", "UED"]) {
    notify({ type: "assistant_text_delta", messageId, blockIndex: 1, textDelta });
  }
  notify({ type: "assistant_text_complete", messageId, blockIndex: 1 });
  createAssistantMessage(turn, messageId, [
    { type: "thinking", thinking: "Just answering" },
    { type: "text", text: "QUEUED" },
  ]);
  notify({ type: "session_title_updated", title: "Mock title", updateType: "llm_generated" });
  finishTurn(turn, "completed");
}

/** S9: the first turn unauthenticated; the process stays up afterwards. */
function runAuthFailTurn(turn: Turn): void {
  workingState("thinking");
  notify({
    type: "error",
    message: "No access token available",
    errorType: "Error",
    timestamp: new Date().toISOString(),
    error: { name: "AuthenticationError", message: "No access token available" },
  });
  const now = Date.now();
  notify({
    type: "create_message",
    message: {
      id: `sys-auth-${turn.id}`,
      role: "system",
      content: [
        {
          type: "text",
          text: "Authentication failed. Please log in using /login or set a valid FACTORY_API_KEY environment variable.",
        },
      ],
      visibility: "user_only",
      parentId: turn.id,
      seq: nextSeq(turn),
      createdAt: now,
      updatedAt: now,
    },
  });
  finishTurn(turn, "model_authentication_failed", ZERO_USAGE);
}

function writeMalformedLine(): void {
  process.stderr.write("mock: writing a malformed stdout line\n");
  process.stdout.write("this is not json {\n");
}

/** Sends a request Droid clients do not implement; the client must answer -32601. */
async function runUnknownInboundTurn(turn: Turn): Promise<void> {
  const requestId = sendMockRequest(turn, "droid.future_request", { detail: "from the future" });
  const woken = await hold(turn);
  turn.openRequestId = undefined;
  if (woken.kind === "interrupt") {
    openMockRequests.delete(requestId);
    cancelledMockRequests.add(requestId);
    emitInterruptTail(turn, woken.requestId);
    return;
  }
  finishPlainTurn(turn);
}

function emitUnknownEvents(): void {
  notifyUnscoped({ type: "mission_heartbeat", timestamp: Date.now() });
  notify({ type: "tool_execution_heartbeat", toolUseId: "tool-x", toolName: "Execute" });
  notify({ type: "some_future_event_v2", payload: { nested: true } });
  sendNotification(
    { sessionId: session?.id, notification: { type: "future" } },
    "droid.future_notification",
  );
  sendNotification({ sessionId: session?.id });
}

const HUGE_RESULT_PADDING = 10.5 * 1024 * 1024;

function crash(): void {
  const line = `mock stderr noise ${"x".repeat(1000)}\n`;
  for (let index = 0; index < 64; index++) {
    process.stderr.write(line);
  }
  const apiKey = process.env.FACTORY_API_KEY;
  if (apiKey) {
    process.stderr.write(`mock: authentication failed for key ${apiKey}\n`);
  }
  process.stderr.write("fatal: mock crash\n");
  exitAfterFlush(1);
}

// Keepalive for STUBBORN after stdin EOF: Node exits once stdin closes otherwise. It never
// releases a scenario hold.
function keepAlive(): void {
  setInterval(() => undefined, 1 << 30);
}

async function runTurn(turn: Turn): Promise<void> {
  const text = turn.text;
  if (text.includes("LATE_STEER")) {
    await runLateSteerTurn(turn);
    return;
  }
  if (text.includes("STEER_DROP")) {
    await runSteerDropTurn(turn);
    return;
  }
  if (text.includes("STEER_ME")) {
    await runSteerTurn(turn);
    return;
  }
  if (text.includes("RETRACT_ME")) {
    runRetractTurn(turn);
    return;
  }
  if (text.includes("AUTH_FAIL")) {
    runAuthFailTurn(turn);
    return;
  }
  if (text.includes("AUTO_COMPACT")) {
    finishPlainTurn(turn, undefined, () => {
      workingState("compacting_conversation");
      notify({
        type: "session_compacted",
        summaryId: "sum-1",
        removedCount: 1,
        visibleBoundaryMessageId: null,
      });
      workingState("streaming_assistant_message");
    });
    return;
  }
  if (text.includes("CRASH_MID_TURN")) {
    streamThinking(turn, nextAssistantId());
    crash();
    return;
  }
  if (text.includes("MALFORMED_MID_TURN")) {
    streamThinking(turn, nextAssistantId());
    writeMalformedLine();
    return;
  }
  if (text.includes("EOF_TOOLS")) {
    await runEofToolsTurn(turn);
    return;
  }
  if (text.includes("LONG_TOOL")) {
    await runLongToolTurn(turn);
    return;
  }
  if (text.includes("EXIT_MID_TURN")) {
    stdinEnded = true;
    workingState("idle");
    turn.reachedEof = true;
    finishPlainTurn(turn);
    return;
  }
  if (text.includes("IDLE_MID_TURN")) {
    finishPlainTurn(turn, () => workingState("idle"));
    return;
  }
  if (text.includes("SWITCH_SESSION")) {
    await runExitSpecModeTurn(turn, true);
    return;
  }
  if (text.includes("PLAN_ME")) {
    await runExitSpecModeTurn(turn, false);
    return;
  }
  if (text.includes("ASK_PERM_ME")) {
    await runAskPermissionTurn(turn);
    return;
  }
  if (text.includes("ASK_ME")) {
    await runAskUserTurn(turn);
    return;
  }
  if (text.includes("PICK_ME")) {
    await runPickOptionsTurn(turn);
    return;
  }
  if (text.includes("APPROVE_FILE")) {
    await runPermissionTurn(turn, createFileTool());
    return;
  }
  if (text.includes("APPROVE_ME")) {
    await runPermissionTurn(turn, executeTool("echo one", "one\n\n\n[Process exited with code 0]"));
    return;
  }
  if (text.includes("STALL_AFTER_TOOL")) {
    await runStallAfterToolTurn(turn);
    return;
  }
  if (text.includes("INTERRUPT_WEDGED")) {
    await runInterruptWedgedTurn(turn);
    return;
  }
  if (text.includes("INTERRUPT_GHOST")) {
    await runInterruptGhostTurn(turn);
    return;
  }
  if (text.includes("INTERRUPT_ME")) {
    await runInterruptTurn(turn);
    return;
  }
  if (text.includes("UNKNOWN_INBOUND")) {
    await runUnknownInboundTurn(turn);
    return;
  }
  if (text.includes("HUGE_RESULT")) {
    const tool = executeTool(
      "cat big.txt",
      `big:${"x".repeat(HUGE_RESULT_PADDING)}\u2028\u2029end`,
    );
    announceTool(turn, tool);
    runTool(tool);
    finishAfterTool(turn);
    return;
  }
  if (text.includes("BG_TASK")) {
    runBackgroundTaskTurn(turn);
    return;
  }
  if (text.includes("TOOL_ME")) {
    const tool = executeTool("echo two", "two\n\n\n[Process exited with code 0]");
    announceTool(turn, tool);
    runTool(tool);
    finishAfterTool(turn);
    return;
  }
  if (text.includes("UNKNOWN_EVENT")) {
    finishPlainTurn(turn, emitUnknownEvents);
    return;
  }
  finishPlainTurn(turn);
}

// ---------------------------------------------------------------------------
// RPC mode: request dispatch
// ---------------------------------------------------------------------------

const SESSIONLESS_METHODS = new Set([
  "droid.list_models",
  "droid.initialize_session",
  "droid.load_session",
]);
const SESSION_METHODS = new Set([
  "droid.add_user_message",
  "droid.interrupt_session",
  "droid.update_session_settings",
  "droid.compact_session",
  "droid.execute_rewind",
  "droid.close_session",
]);

let hooksDisabled = false;
/** Set by `execute_rewind`; the next `add_user_message` first emits a late old-id title. */
let rewoundFromSessionId: string | undefined;

function handleInitializeSession(id: string, params: Json): void {
  if (!hooksDisabled) {
    const hook = {
      hookId: "h1",
      hookEventName: "SessionStart",
      hookMatcher: "startup",
      hookCommands: [{ type: "command", command: "echo hook", timeout: 60 }],
      hookOrder: 1,
    };
    notifyUnscoped({ type: "hook_execution_started", ...hook });
    notifyUnscoped({
      type: "hook_execution_completed",
      ...hook,
      hookStatus: "completed",
      hookResults: [{ command: "echo hook", timeout: 60, exitCode: 0, stdout: "", stderr: "" }],
    });
  }
  const created = makeSession(NodeCrypto.randomUUID(), mergeSettings(defaultSettings(), params));
  saveSession(created);
  session = created;
  activeTurn = undefined;
  deferredDroidTurn = undefined;
  notifyUnscoped({ type: "settings_updated", settings: created.settings });
  respond(id, {
    sessionId: created.id,
    hostId: "host-0",
    session: { messages: [] },
    settings: created.settings,
    availableModels: MODELS,
  });
}

function handleLoadSession(id: string, params: Json): void {
  const sessionId = params.sessionId;
  const loaded =
    typeof sessionId === "string" && SAFE_SESSION_ID.test(sessionId)
      ? (sessions.get(sessionId) ?? readSavedSession(sessionId))
      : undefined;
  if (loaded === undefined) {
    respondError(id, -32004, "Session not found");
    return;
  }
  session = loaded;
  activeTurn = undefined;
  deferredDroidTurn = undefined;
  notifyUnscoped({ type: "settings_updated", settings: loaded.settings });
  respond(id, {
    session: { messages: loaded.messages, title: "Mock title" },
    hasOlderMessages: false,
    hostId: "host-0",
    updatedAt: Math.floor(Date.now() / 1000),
    settings: loaded.settings,
    availableModels: MODELS,
    pendingMessageRequestIds: [],
    isAgentLoopInProgress: false,
    workingState: "idle",
    cwd: flagValue("--cwd") ?? "/workspace/project",
    tokenUsage: loaded.usage,
    inclusiveTokenUsage: loaded.usage,
  });
}

function handleAddUserMessage(id: string, params: Json, current: Session): void {
  const text = typeof params.text === "string" ? params.text : "";
  if (text.includes("NO_REPLY")) {
    log({ type: "no_reply", id });
    return;
  }
  if (text.includes("REJECT_MESSAGE")) {
    respondError(id, -32603, "Mock rejected the message.");
    return;
  }
  if (text.includes("CRASH_ME")) {
    crash();
    return;
  }
  if (text.includes("MALFORMED_LINE")) {
    writeMalformedLine();
    return;
  }
  if (text.includes("STUBBORN")) {
    stubborn = true;
  }
  if (rewoundFromSessionId !== undefined) {
    notify(
      { type: "session_title_updated", title: "Late title", updateType: "llm_generated" },
      rewoundFromSessionId,
    );
    rewoundFromSessionId = undefined;
  }
  if (activeTurn !== undefined) {
    // A mid-turn message is a steer; the running scenario decides when to acknowledge it.
    const steerMessageId =
      typeof params.messageId === "string" ? params.messageId : NodeCrypto.randomUUID();
    if (!wake(activeTurn, { kind: "steer", text, messageId: steerMessageId, requestId: id })) {
      respond(id, {});
    }
    return;
  }
  respond(id, {});
  const messageId =
    typeof params.messageId === "string" ? params.messageId : NodeCrypto.randomUUID();
  const droidTurn = deferredDroidTurn;
  if (droidTurn !== undefined) {
    deferredDroidTurn = undefined;
    droidTurn({ text, messageId, requestId: id });
    return;
  }
  const turn = startTurn(text, messageId, id, current);
  void runTurn(turn);
}

function handleRequest(id: string, method: string, params: Json): void {
  if (method === "droid.list_models") {
    // DROID_MOCK_LIST_MODELS drives discovery failure modes for the provider's probe tests.
    const listModelsMode = process.env.DROID_MOCK_LIST_MODELS;
    if (listModelsMode === "error") {
      respondError(id, -32603, "mock list_models failure");
      return;
    }
    if (listModelsMode === "noreply") {
      log({ type: "list_models_no_reply" });
      return;
    }
    if (listModelsMode === "malformed") {
      respond(id, { models: "not-a-model-list" });
      return;
    }
    respond(id, { models: MODELS });
    return;
  }
  if (method === "droid.initialize_session") {
    handleInitializeSession(id, params);
    return;
  }
  if (method === "droid.load_session") {
    handleLoadSession(id, params);
    return;
  }
  const current = session;
  if (current === undefined) {
    respondError(id, -32603, "No active session. Call initialize_session or load_session first.");
    return;
  }
  switch (method) {
    case "droid.add_user_message":
      handleAddUserMessage(id, params, current);
      return;
    case "droid.interrupt_session":
      if (activeTurn === undefined || !wake(activeTurn, { kind: "interrupt", requestId: id })) {
        respond(id, {});
      }
      return;
    case "droid.update_session_settings":
      current.settings = mergeSettings(current.settings, params);
      notifyUnscoped({ type: "settings_updated", requestId: id, settings: current.settings });
      respond(id, {});
      return;
    case "droid.compact_session": {
      if (activeTurn !== undefined) {
        respondError(id, -32602, "Wait for the current turn to finish before compressing.");
        return;
      }
      if (!current.hasCompletedTurn) {
        respondError(id, -32603, "Failed to generate summary for compaction.");
        return;
      }
      workingState("idle");
      notifyUnscoped({ type: "settings_updated", requestId: id, settings: current.settings });
      emitUsage(current, ZERO_USAGE);
      // Default-mode compaction keeps the session id; the divider follows the response.
      respond(id, { newSessionId: current.id, removedCount: 12 });
      notify({
        type: "session_compacted",
        summaryId: "sum-1",
        removedCount: 12,
        visibleBoundaryMessageId: null,
      });
      return;
    }
    case "droid.execute_rewind": {
      const source =
        typeof params.sessionId === "string" && sessions.has(params.sessionId)
          ? sessions.get(params.sessionId)!
          : current;
      const cutIndex = source.messages.findIndex((message) => message.id === params.messageId);
      if (cutIndex === -1) {
        respondError(id, -32602, "Unknown messageId for rewind.");
        return;
      }
      const fork = makeSession(
        NodeCrypto.randomUUID(),
        {
          ...source.settings,
          tags: [{ name: "rewind", metadata: { rewoundFromSessionId: source.id } }],
        },
        source.messages.slice(0, cutIndex),
        source.usage,
        cutIndex,
      );
      saveSession(fork);
      rewoundFromSessionId = source.id;
      respond(id, {
        newSessionId: fork.id,
        restoredCount: 0,
        deletedCount: 0,
        failedRestoreCount: 0,
        failedDeleteCount: 0,
      });
      return;
    }
    case "droid.close_session":
      respond(id, {});
      if (!stubborn) {
        closed = true;
        exitAfterFlush(0);
      }
      return;
  }
}

function handleResponse(id: string, message: Json): void {
  log({ type: "response", id, result: message.result ?? null, error: message.error ?? null });
  if (!openMockRequests.delete(id)) {
    log({ type: "stale_response", id });
    if (cancelledMockRequests.has(id)) {
      workingState("idle");
    }
    return;
  }
  if (activeTurn !== undefined && activeTurn.openRequestId === id) {
    wake(activeTurn, { kind: "answer", message });
  }
}

function handleLine(line: string): void {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    log({ type: "recv", raw: line });
    if (!closed) {
      respondError(null, -32700, "Invalid JSON-RPC message");
    }
    return;
  }
  log({ type: "recv", message: parsed });
  if (closed) {
    return;
  }
  if (
    !isRecord(parsed) ||
    (parsed.type !== "request" && parsed.type !== "response") ||
    typeof parsed.factoryApiVersion !== "string" ||
    typeof parsed.id !== "string"
  ) {
    respondError(null, -32700, "Invalid JSON-RPC message");
    return;
  }
  const id = parsed.id;
  if (parsed.type === "response") {
    handleResponse(id, parsed);
    return;
  }
  const method = parsed.method;
  if (typeof method !== "string") {
    respondError(null, -32700, "Invalid JSON-RPC message");
    return;
  }
  if (!SESSIONLESS_METHODS.has(method) && !SESSION_METHODS.has(method)) {
    respondError(id, -32601, `Unknown method: ${method}`);
    return;
  }
  if (!isRecord(parsed.params)) {
    respondError(id, -32602, `Invalid request for ${method}: params: Required`);
    return;
  }
  handleRequest(id, method, parsed.params);
}

function handleStdinEnd(): void {
  stdinEnded = true;
  log({ type: "stdin_eof" });
  if (closed) {
    return;
  }
  if (stubborn) {
    keepAlive();
    return;
  }
  const turn = activeTurn;
  if (turn === undefined) {
    exitAfterFlush(0);
    return;
  }
  workingState("idle");
  turn.reachedEof = true;
  wake(turn, { kind: "eof" });
}

function handleSigterm(): void {
  log({ type: "sigterm" });
  if (stubborn) {
    return;
  }
  const turn = activeTurn;
  if (turn !== undefined) {
    turn.hold = undefined;
    workingState("idle");
    notify({
      type: "agent_turn_completed",
      reason: "process_exit",
      turnId: turn.id,
      tokenUsage: ZERO_USAGE,
      cumulativeTokenUsage: turn.session.usage,
      childTokenUsage: ZERO_USAGE,
      cumulativeChildTokenUsage: ZERO_USAGE,
      durationMs: Date.now() - turn.startedAt,
    });
    activeTurn = undefined;
  }
  exitAfterFlush(0);
}

function runRpc(): void {
  hooksDisabled = readSettingsFile();
  // Without a handler SIGTERM kills the process, which is how a real Droid usually dies.
  if (process.env.DROID_MOCK_SIGTERM !== "die") {
    process.on("SIGTERM", handleSigterm);
  }
  notifyUnscoped({
    type: "mcp_status_changed",
    servers: [],
    summary: { total: 0, connected: 0, connecting: 0, failed: 0, disabled: 0 },
  });

  let buffer = "";
  process.stdin.setEncoding("utf8");
  process.stdin.on("data", (chunk: string) => {
    buffer += chunk;
    let newline = buffer.indexOf("\n");
    while (newline !== -1) {
      const line = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      if (line.trim() === "") {
        log({ type: "recv", raw: line });
      } else {
        handleLine(line);
      }
      newline = buffer.indexOf("\n");
    }
  });
  process.stdin.on("end", () => {
    if (buffer.trim() !== "") {
      handleLine(buffer);
    }
    buffer = "";
    handleStdinEnd();
  });
}

if (args[0] === "exec" && args.includes("--input-format")) {
  runRpc();
} else {
  process.stderr.write(`droid-mock-agent: unsupported arguments: ${args.join(" ")}\n`);
  process.exit(2);
}
