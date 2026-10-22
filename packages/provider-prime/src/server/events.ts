import type { OrchestrationV2Notification, OrchestrationV2ProviderGoal } from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import {
  primeRecordField as recordField,
  primeRecordNumber,
  primeRecordString as recordString,
  type PrimeGoalState,
  type PrimeRlmChild,
  type PrimeRpcRecord,
} from "./rpc.ts";

function nonBlank(value: string | undefined): string | undefined {
  return value === undefined || value.trim().length === 0 ? undefined : value;
}

/** The detail Prime attached to a failed model call, if any message carries one. */
export function primeErrorMessage(message: unknown): string | undefined {
  return nonBlank(recordString(message, "errorMessage"));
}

/**
 * A model failure that never streamed (a provider 400 before the first token)
 * can reach T3 only through agent_end, so its messages are the last place to
 * find the provider's reason.
 */
export function primeAgentEndErrorMessage(agentEnd: PrimeRpcRecord): string | undefined {
  const messages = agentEnd["messages"];
  if (!Array.isArray(messages)) return undefined;
  for (const message of messages) {
    const errorMessage = primeErrorMessage(message);
    if (errorMessage !== undefined) return errorMessage;
  }
  return undefined;
}

export function primeAgentEndFailed(agentEnd: PrimeRpcRecord): boolean {
  const messages = agentEnd["messages"];
  return (
    Array.isArray(messages) &&
    messages.some((message) => recordString(message, "stopReason") === "error")
  );
}

/**
 * Prime refuses a prompt while it is busy (core/agent-session.ts). The refusal
 * concerns only the message it answers; the run that made Prime busy keeps going.
 */
export function isPrimeBusyRejection(error: string | undefined): boolean {
  return (
    error !== undefined &&
    (error.startsWith("Agent is already processing") ||
      error.startsWith("Agent has queued work") ||
      error.startsWith("Agent became busy before prompt delivery"))
  );
}

/**
 * Prime reports a failed session command only as its result row
 * (core/agent-session.ts _executeQueuedSessionCommand); the prompt was
 * already acked. Rows for other commands belong to other turns.
 */
export function primeSessionCommandFailure(
  message: unknown,
  sessionCommands: ReadonlySet<string>,
): string | undefined {
  if (recordString(message, "customType") !== "session_slash_command_result") return undefined;
  const details = recordField(message, "details");
  if (recordField(details, "success") !== false) return undefined;
  const sessionCommand = recordString(recordField(details, "command"), "name");
  if (sessionCommand === undefined || !sessionCommands.has(sessionCommand)) return undefined;
  return (
    nonBlank(recordString(details, "error")) ??
    nonBlank(primeContentText(recordField(message, "content"))) ??
    `Prime /${sessionCommand} failed.`
  );
}

const PRIME_GOAL_STATUSES = {
  active: "active",
  paused: "paused",
  budget_limited: "budget_limited",
  complete: "complete",
  error: "blocked",
} as const satisfies Record<string, OrchestrationV2ProviderGoal["status"]>;

const isMappedGoalStatus = (status: string): status is keyof typeof PRIME_GOAL_STATUSES =>
  Object.hasOwn(PRIME_GOAL_STATUSES, status);

const wholeCount = (value: number) => Math.max(0, Math.trunc(value));

/**
 * Prime's GoalState (core/goals.ts). `idle` is Prime's cleared goal, and a goal
 * without an objective has nothing T3 can show.
 */
export function primeProviderGoal(goal: PrimeGoalState): OrchestrationV2ProviderGoal | null {
  const objective = goal.objective?.trim() ?? "";
  if (goal.status === "idle" || objective.length === 0) return null;
  const status = isMappedGoalStatus(goal.status)
    ? PRIME_GOAL_STATUSES[goal.status]
    : goal.active
      ? "active"
      : null;
  if (status === null) return null;
  return {
    objective,
    status,
    tokensUsed: wholeCount(goal.tokensUsed),
    tokenBudget: goal.tokenBudget === undefined ? null : wholeCount(goal.tokenBudget),
    timeUsedSeconds: wholeCount(goal.timeUsedSeconds),
  };
}

/**
 * A heartbeat Prime injects into a running session
 * (core/messages.ts createHeartbeatPromptMessage): content is
 * `[heartbeat: <schedule> run#N]\n\n<prompt>`.
 */
export function primeHeartbeatNotification(
  message: unknown,
): OrchestrationV2Notification | undefined {
  if (recordString(message, "customType") !== "heartbeat_prompt") return undefined;
  const content = primeContentText(recordField(message, "content"));
  const header = /^\[heartbeat: (.*) run#\d+\]\n*/u.exec(content);
  const schedule =
    nonBlank(recordString(recordField(message, "details"), "schedule")) ?? nonBlank(header?.[1]);
  const prompt = (header === null ? content : content.slice(header[0].length)).trim();
  return {
    source: { kind: "monitor" },
    outcome: "updated",
    summary: schedule === undefined ? "Heartbeat" : `Heartbeat ${schedule.trim()}`,
    ...(prompt.length === 0 ? {} : { detail: prompt.slice(0, 2_000) }),
  };
}

/**
 * Prime's autonomous continuation prompt (core/autonomous.ts). Continuations
 * built by the hook carry `isAutonomousContinuation`; held ones from
 * createAutonomousContinuationMessage carry only the text marker.
 */
const AUTONOMOUS_CONTINUATION_MARKER = /^\[autonomous-continuation(?::\s*([^\]]*?))?\s*\]/u;

export interface PrimeAutonomousContinuation {
  readonly reason: string | null;
  readonly title: string;
  readonly text: string;
}

export function primeAutonomousContinuation(
  message: unknown,
): PrimeAutonomousContinuation | undefined {
  if (recordString(message, "role") !== "user") return undefined;
  const text = primeContentText(recordField(message, "content"));
  const marker = AUTONOMOUS_CONTINUATION_MARKER.exec(text);
  if (marker === null && recordField(message, "isAutonomousContinuation") !== true) {
    return undefined;
  }
  const reason = nonBlank(marker?.[1])?.trim() ?? null;
  return {
    reason,
    title: reason === "gate-failed" ? "Autonomous gate failed" : "Autonomous continuation",
    text,
  };
}

/** Autonomous lifecycle rows (core/agent-session.ts _emitAutonomousEvent, _emitAutonomousStatus). */
const AUTONOMOUS_NOTICE_TYPES = new Set([
  "autonomous_gate_started",
  "autonomous_gate_failed",
  "autonomous_gate_passed",
  "autonomous_stopped",
  "autonomous_status",
]);

export function primeAutonomousNotice(message: unknown): string | undefined {
  const customType = recordString(message, "customType");
  if (customType === undefined || !AUTONOMOUS_NOTICE_TYPES.has(customType)) return undefined;
  return nonBlank(primeContentText(recordField(message, "content")));
}

/**
 * Prime displays its echo of a session command's slash text, and goal steering
 * as a custom message; T3 already shows the user's message and the goal state.
 */
const HIDDEN_CUSTOM_TYPES = new Set(["session_slash_command", "goal_context"]);

export function primeCustomMessageHidden(message: unknown): boolean {
  const customType = recordString(message, "customType");
  return (
    recordField(message, "display") === false ||
    (customType !== undefined && HIDDEN_CUSTOM_TYPES.has(customType))
  );
}

/** Rows with their own item mapping, or that fail the turn instead. */
function hasDedicatedMapping(message: unknown, customType: string): boolean {
  return (
    AUTONOMOUS_NOTICE_TYPES.has(customType) ||
    customType === "heartbeat_prompt" ||
    (customType === "session_slash_command_result" &&
      recordField(recordField(message, "details"), "success") === false)
  );
}

/** Prime's restore row (core/agent-session.ts _onIpythonStateRestored) lists every revived name. */
function ipythonRestoredSummary(content: string): string {
  if (content.includes("could not be revived")) {
    return "Python kernel state could not be revived — the kernel is starting fresh.";
  }
  const names = /available again:\s*([^.]*)\./u.exec(content)?.[1];
  if (names === undefined) return "Python kernel state restored.";
  const count = names.split(",").filter((name) => name.trim().length > 0).length;
  return `Python kernel state restored — ${count} ${count === 1 ? "name" : "names"} revived.`;
}

/** The notice for a displayed custom message that no dedicated mapping covers. */
export function primeCustomMessageNotice(message: unknown): string | undefined {
  const customType = recordString(message, "customType");
  if (customType === undefined || primeCustomMessageHidden(message)) return undefined;
  if (hasDedicatedMapping(message, customType)) return undefined;
  const content = primeContentText(recordField(message, "content"));
  if (customType === "ipython_state_restored") return ipythonRestoredSummary(content);
  return nonBlank(content);
}
export type PrimeFileChangeDetail = {
  readonly operation: string;
  readonly path: string;
  readonly oldPath?: string;
};

export type PrimeCommandExecutionItem = {
  readonly type: "command_execution";
  readonly input: string;
  readonly output?: string;
  readonly exitCode?: number;
  /** Overrides the status the adapter derives from `isError` and the turn. */
  readonly status?: "failed" | "interrupted";
};

export type PrimeFileChangeItem = {
  readonly type: "file_change";
  readonly fileName: string;
  readonly diffStr?: string;
  readonly newStr?: string;
  readonly changes?: ReadonlyArray<PrimeFileChangeDetail>;
};

/** Undefined means the tool keeps the generic `dynamic_tool` presentation. */
export type PrimeToolItem = PrimeCommandExecutionItem | PrimeFileChangeItem;

export interface PrimeToolCall {
  readonly toolName: string;
  readonly args: unknown;
  /** The end frame's `result`, or an update frame's `partialResult`. */
  readonly result: unknown;
  readonly isError: boolean;
}

export function primeContentText(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content
    .map((block) =>
      recordField(block, "type") === "text" ? (recordString(block, "text") ?? "") : "",
    )
    .join("");
}

function joinOutput(parts: ReadonlyArray<string | undefined>): string {
  let output = "";
  for (const part of parts) {
    if (part === undefined || part.length === 0) continue;
    output += output.length === 0 || output.endsWith("\n") ? part : `\n${part}`;
  }
  return output;
}

function textLines(text: string): ReadonlyArray<string> {
  if (text.length === 0) return [];
  const lines = text.split("\n");
  return lines.at(-1) === "" ? lines.slice(0, -1) : lines;
}

function hunkRange(start: number, count: number): string {
  return `${count === 0 ? start - 1 : start},${count}`;
}

/** Errors come from Prime's kernel (core/kernel/shared.ts `ExecuteResult.error`). */
function ipythonErrorText(error: unknown): string | undefined {
  const traceback = recordField(error, "traceback");
  if (Array.isArray(traceback)) {
    const lines = traceback.filter((line): line is string => typeof line === "string");
    if (lines.length > 0) return lines.join("\n");
  }
  const ename = recordString(error, "ename");
  const evalue = recordString(error, "evalue");
  if (ename === undefined && evalue === undefined) return undefined;
  return [ename, evalue].filter((part) => part !== undefined && part.length > 0).join(": ");
}

/**
 * `IpythonToolDetails` (core/tools/ipython.ts) carries the cell's stdout,
 * stderr, trailing expression value and error separately; the content text
 * mixes in the kernel-restart notice and background output.
 */
function ipythonItem(call: PrimeToolCall): PrimeCommandExecutionItem {
  const details = recordField(call.result, "details");
  const status = recordString(details, "status");
  const detailsOutput = joinOutput([
    recordString(details, "stdout"),
    recordString(details, "stderr"),
    recordString(details, "result"),
    status === "error" ? ipythonErrorText(recordField(details, "error")) : undefined,
  ]);
  const output =
    detailsOutput.length > 0
      ? detailsOutput
      : primeContentText(recordField(call.result, "content"));
  const itemStatus =
    status === "aborted" ? "interrupted" : status === "error" ? "failed" : undefined;
  return {
    type: "command_execution",
    input: recordString(call.args, "code") ?? "",
    ...(output.length > 0 ? { output } : {}),
    ...(itemStatus === undefined ? {} : { status: itemStatus }),
  };
}

function shellItem(call: PrimeToolCall): PrimeCommandExecutionItem {
  const output = primeContentText(recordField(call.result, "content"));
  const exitCode =
    primeRecordNumber(recordField(call.result, "structuredContent"), "exit_code") ??
    primeRecordNumber(recordField(call.result, "details"), "exitCode");
  return {
    type: "command_execution",
    input: recordString(call.args, "command") ?? "",
    ...(output.length > 0 ? { output } : {}),
    ...(exitCode === undefined ? {} : { exitCode }),
  };
}

const NUMBERED_DIFF_LINE = /^([+\- ]) *(\d+) (.*)$/su;
const NUMBERED_DIFF_GAP = /^ +\.\.\.$/u;

/**
 * Prime's edit tool reports `details.diff` in its display format
 * (core/tools/edit-diff.ts `generateDiffString`): `+12 text`, `-12 text` and
 * ` 12 text` lines numbered by new, old and old line respectively, with a
 * bare `...` line where unchanged lines were skipped.
 */
export function primeNumberedDiffToPatch(fileName: string, diff: string): string | undefined {
  const hunks: Array<Array<string>> = [];
  const headers: Array<string> = [];
  let current: Array<string> | null = null;
  let oldStart = 0;
  let newStart = 0;
  let oldCount = 0;
  let newCount = 0;
  // New line number minus old line number at the current position.
  let offset = 0;
  const closeHunk = () => {
    if (current === null) return;
    headers.push(`@@ -${hunkRange(oldStart, oldCount)} +${hunkRange(newStart, newCount)} @@`);
    hunks.push(current);
    current = null;
  };
  for (const line of diff.split("\n")) {
    if (NUMBERED_DIFF_GAP.test(line)) {
      closeHunk();
      continue;
    }
    const match = NUMBERED_DIFF_LINE.exec(line);
    if (match === null) {
      if (line.length === 0) continue;
      return undefined;
    }
    const [, sign, digits, text] = match;
    const lineNumber = Number(digits);
    if (current === null) {
      current = [];
      oldStart = sign === "+" ? lineNumber - offset : lineNumber;
      newStart = sign === "+" ? lineNumber : lineNumber + offset;
      oldCount = 0;
      newCount = 0;
    }
    current.push(`${sign}${text}`);
    if (sign !== "+") oldCount += 1;
    if (sign !== "-") newCount += 1;
    if (sign === "+") offset += 1;
    if (sign === "-") offset -= 1;
  }
  closeHunk();
  if (hunks.length === 0) return undefined;
  const body = hunks.flatMap((lines, index) => [headers[index] ?? "", ...lines]);
  return [`--- ${fileName}`, `+++ ${fileName}`, ...body, ""].join("\n");
}

function editOrWriteItem(call: PrimeToolCall): PrimeFileChangeItem | undefined {
  const fileName = recordString(call.args, "path") ?? recordString(call.args, "file_path");
  if (fileName === undefined || fileName.trim().length === 0) return undefined;
  const details = recordField(call.result, "details");
  const numberedDiff = recordString(details, "diff");
  const outputText = primeContentText(recordField(call.result, "content"));
  const diffStr =
    recordString(details, "patch") ??
    (numberedDiff === undefined ? undefined : primeNumberedDiffToPatch(fileName, numberedDiff)) ??
    (call.isError && outputText.trim().length > 0 ? outputText : undefined);
  const newStr = call.toolName === "write" ? recordString(call.args, "content") : undefined;
  return {
    type: "file_change",
    fileName,
    ...(diffStr === undefined ? {} : { diffStr }),
    ...(newStr === undefined ? {} : { newStr }),
  };
}

function firstPath(args: unknown, keys: ReadonlyArray<string>): string | undefined {
  for (const key of keys) {
    const value = nonBlank(recordString(args, key));
    if (value !== undefined) return value;
  }
  return undefined;
}

function renameItem(call: PrimeToolCall): PrimeFileChangeItem | undefined {
  const oldPath = firstPath(call.args, ["oldPath", "old_path", "from", "source"]);
  const newPath = firstPath(call.args, ["newPath", "new_path", "to", "destination"]);
  if (oldPath === undefined || newPath === undefined) return undefined;
  const outputText = primeContentText(recordField(call.result, "content"));
  return {
    type: "file_change",
    fileName: newPath,
    changes: [{ operation: "rename", path: newPath, oldPath }],
    ...(call.isError && outputText.trim().length > 0 ? { diffStr: outputText } : {}),
  };
}

export function primeToolItem(call: PrimeToolCall): PrimeToolItem | undefined {
  switch (call.toolName) {
    case "ipython":
      return ipythonItem(call);
    case "bash":
    case "powershell":
      return shellItem(call);
    case "edit":
    case "write":
      return editOrWriteItem(call);
    case "rename":
      return renameItem(call);
    default:
      return undefined;
  }
}

/** One cell can edit several files; the cell's file change covers each of them. */
export function primeIpythonFileChange(result: unknown): PrimeFileChangeItem | undefined {
  const diffs = recordField(recordField(result, "details"), "diffs");
  if (!Array.isArray(diffs)) return undefined;
  const changes: Array<PrimeFileChangeDetail> = [];
  const patches: Array<string> = [];
  for (const diff of diffs) {
    const path = nonBlank(recordString(diff, "path"));
    const oldStr = recordString(diff, "oldStr");
    const newStr = recordString(diff, "newStr");
    if (path === undefined || oldStr === undefined || newStr === undefined) continue;
    if (!changes.some((change) => change.path === path)) {
      changes.push({ operation: "update", path });
    }
    const start = primeRecordNumber(diff, "startLine") ?? 1;
    const oldLines = textLines(oldStr);
    const newLines = textLines(newStr);
    patches.push(
      [
        `--- ${path}`,
        `+++ ${path}`,
        `@@ -${hunkRange(start, oldLines.length)} +${hunkRange(start, newLines.length)} @@`,
        ...oldLines.map((line) => `-${line}`),
        ...newLines.map((line) => `+${line}`),
      ].join("\n"),
    );
  }
  const first = changes[0];
  if (first === undefined) return undefined;
  return { type: "file_change", fileName: first.path, changes, diffStr: `${patches.join("\n")}\n` };
}

/** Prime's still-running notice (core/tools/ipython.ts `IPYTHON_HEARTBEAT_INTERVAL_MS`). */
const IPYTHON_HEARTBEAT = /\[ipython cell still running \(\d+s elapsed\)\]$/u;

/**
 * Each ipython update carries one new stream chunk, except the kernel start-up
 * notices and the 30 s heartbeat, which repeats the stream tail and must not
 * be appended again.
 */
export function primeIpythonUpdate(
  partialResult: unknown,
): { readonly type: "output"; readonly text: string } | { readonly type: "progress" } {
  const text = primeContentText(recordField(partialResult, "content"));
  if (
    recordString(recordField(partialResult, "details"), "status") === "starting" ||
    IPYTHON_HEARTBEAT.test(text) ||
    text.length === 0
  ) {
    return { type: "progress" };
  }
  return { type: "output", text };
}

const PRIME_RLM_CHILD_STATUSES = {
  queued: "waiting",
  running: "running",
  done: "completed",
  error: "failed",
  cancelled: "cancelled",
} as const;

const isMappedRlmChildStatus = (status: string): status is keyof typeof PRIME_RLM_CHILD_STATUSES =>
  Object.hasOwn(PRIME_RLM_CHILD_STATUSES, status);

export interface PrimeRlmChildView {
  readonly status: (typeof PRIME_RLM_CHILD_STATUSES)[keyof typeof PRIME_RLM_CHILD_STATUSES];
  readonly terminal: boolean;
  readonly title: string;
  readonly model: string | null;
  readonly progress: string | undefined;
  readonly result: string | null;
}

function primeRlmChildActivity(activity: PrimeRlmChild["activity"]): string | undefined {
  switch (activity?.kind) {
    case "executing":
      return `Running ${nonBlank(activity.toolName) ?? "a tool"}`;
    case "writing":
      return "Writing";
    case "waiting":
      return "Waiting";
    default:
      return undefined;
  }
}

/**
 * An RLM child as T3 shows it. Prime's statuses are RlmChildAgentStatus
 * (core/agent-session.ts); `label` is derived from the child's prompt.
 */
export function primeRlmChildView(child: PrimeRlmChild): PrimeRlmChildView | undefined {
  if (!isMappedRlmChildStatus(child.status)) return undefined;
  const status = PRIME_RLM_CHILD_STATUSES[child.status];
  const terminal = status === "completed" || status === "failed" || status === "cancelled";
  const answer = nonBlank(child.answerPreview);
  const error = nonBlank(child.error);
  const result = !terminal ? undefined : status === "completed" ? answer : (error ?? answer);
  const progress = terminal
    ? undefined
    : (answer ?? nonBlank(child.progressNote) ?? primeRlmChildActivity(child.activity));
  return {
    status,
    terminal,
    title: nonBlank(child.label) ?? nonBlank(child.sessionName) ?? "RLM child",
    model: nonBlank(child.model) ?? null,
    progress: progress?.slice(0, 200),
    result: result?.slice(0, 10_000) ?? null,
  };
}

/** The fields an emitted update shows; equal signatures are exact duplicates. */
export function primeRlmChildSignature(child: PrimeRlmChild): string {
  return JSON.stringify([
    child.status,
    child.label,
    child.sessionName,
    child.model,
    child.activity?.kind,
    child.activity?.toolName,
    child.toolUseCount,
    child.answerPreview,
    child.progressNote,
    child.error,
  ]);
}

const TURN_ENTRIES_FINGERPRINT_PREFIX = "prime-user-entries:v2:";

/**
 * What a turn's model-visible native history began with: a user entry, nothing
 * at all, or another kind of message. Prime's `fork` cuts only before a user
 * entry, so history of any other kind ahead of a turn's first user entry
 * cannot be removed natively.
 */
export const PRIME_STRANDED_LEADS = [
  "heartbeat",
  "goal",
  "autonomous",
  "notice",
  "output",
] as const;
export type PrimeStrandedLead = (typeof PRIME_STRANDED_LEADS)[number];
export type PrimeTurnLead = "user" | "none" | PrimeStrandedLead;
const TURN_LEADS: ReadonlySet<string> = new Set(["user", "none", ...PRIME_STRANDED_LEADS]);
const isTurnLead = (value: string): value is PrimeTurnLead => TURN_LEADS.has(value);
const isStrandedLead = (lead: PrimeTurnLead | undefined): lead is PrimeStrandedLead =>
  lead !== undefined && lead !== "user" && lead !== "none";

export interface PrimeTurnEntries {
  readonly lead: PrimeTurnLead;
  readonly hashes: ReadonlyArray<string>;
}

/**
 * Custom rows Prime keeps out of the model's context (core/messages.ts
 * convertToLlm). The harness digest does reach the model, but it describes
 * the user's Prime harness rather than the conversation, and Prime replaces
 * it whenever it goes stale (core/agent-session.ts _appendHarnessDigestIfStale)
 * and builds context from the newest copy only (core/session-manager.ts
 * buildSessionContext), so a copy kept across a cut holds nothing T3 discarded.
 */
const CONTEXT_EXCLUDED_CUSTOM_TYPES = new Set([
  "session_slash_command",
  "session_slash_command_result",
  "compaction_outcome",
  "mcp_connection_outcome",
  "refinement_outcome",
  "harness_digest",
]);

/**
 * Classifies a persisted message by whether it reaches the model, following
 * core/messages.ts convertToLlm: custom messages count whatever their
 * `display` flag. `user` marks a user entry `get_fork_messages` lists.
 */
export function primeMessageLead(message: unknown): Exclude<PrimeTurnLead, "none"> | undefined {
  switch (recordString(message, "role")) {
    case "user":
      return primeContentText(recordField(message, "content")).length > 0 ? "user" : "output";
    case "custom": {
      const customType = recordString(message, "customType");
      if (customType === undefined) return "notice";
      if (CONTEXT_EXCLUDED_CUSTOM_TYPES.has(customType)) return undefined;
      if (customType === "heartbeat_prompt") return "heartbeat";
      if (customType === "goal_context") return "goal";
      if (AUTONOMOUS_NOTICE_TYPES.has(customType)) return "autonomous";
      return "notice";
    }
    case "bashExecution":
      return recordField(message, "excludeFromContext") === true ? undefined : "output";
    default:
      return "output";
  }
}

/** FNV-1a: a short stable digest, so a turn's ref carries no prompt text. */
export function primeUserEntryHash(text: string): string {
  let hash = 0x811c9dc5;
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(16).padStart(8, "0");
}

/**
 * Records which user entries a turn added to the Prime session, in order, and
 * what its model-visible history began with. Prime's `get_fork_messages` lists
 * exactly the persisted user messages that have text (core/agent-session.ts
 * getUserMessagesForForking), and a T3 turn adds zero (a session command, a
 * heartbeat) or more (prompt, steers, continuations).
 */
export function primeTurnFingerprint(entries: PrimeTurnEntries): string {
  const hashes = entries.hashes.length === 0 ? "none" : entries.hashes.join(".");
  return `${TURN_ENTRIES_FINGERPRINT_PREFIX}${entries.lead}:${hashes}`;
}

export function primeTurnEntries(fingerprint: string | undefined): PrimeTurnEntries | undefined {
  if (fingerprint === undefined || !fingerprint.startsWith(TURN_ENTRIES_FINGERPRINT_PREFIX)) {
    return undefined;
  }
  const body = fingerprint.slice(TURN_ENTRIES_FINGERPRINT_PREFIX.length);
  const separator = body.indexOf(":");
  const lead = body.slice(0, separator);
  if (separator < 0 || !isTurnLead(lead)) return undefined;
  const list = body.slice(separator + 1);
  const hashes = list === "none" ? [] : list.split(".");
  if ((lead === "user" && hashes.length === 0) || (lead === "none" && hashes.length > 0)) {
    return undefined;
  }
  return { lead, hashes };
}

export interface PrimeForkMessage {
  readonly entryId: string;
  readonly text: string;
}

export function primeForkMessages(data: unknown): ReadonlyArray<PrimeForkMessage> | undefined {
  const messages = recordField(data, "messages");
  if (!Array.isArray(messages)) return undefined;
  const result: Array<PrimeForkMessage> = [];
  for (const message of messages) {
    const entryId = recordString(message, "entryId");
    const text = recordString(message, "text");
    if (entryId === undefined || text === undefined) return undefined;
    result.push({ entryId, text });
  }
  return result;
}

interface PrimeBoundaryTurn {
  readonly ordinal: number;
  readonly nativeTurnRef: { readonly fingerprint?: string | undefined } | null;
}

/** A discarded turn whose history ahead of its first user entry a native cut would keep. */
interface PrimeStrandedTurn {
  readonly type: "stranded";
  readonly ordinal: number;
  readonly lead: PrimeStrandedLead;
}

export type PrimeDiscardedHistory =
  | { readonly type: "none" }
  | { readonly type: "fork" }
  | { readonly type: "unknown"; readonly ordinal: number }
  | PrimeStrandedTurn;

/**
 * What discarding the turns after `boundaryOrdinal` asks of the session,
 * from their fingerprints alone: nothing (no model-visible history), a cut
 * before the first discarded turn's prompt, or nothing Prime can do because
 * that turn's history starts with something other than a user entry.
 */
export function primeDiscardedHistory(
  turns: ReadonlyArray<PrimeBoundaryTurn>,
  boundaryOrdinal: number,
): PrimeDiscardedHistory {
  const discarded = turns
    .filter((turn) => turn.ordinal > boundaryOrdinal)
    .toSorted((left, right) => left.ordinal - right.ordinal);
  let first: { readonly ordinal: number; readonly lead: PrimeTurnLead } | undefined;
  for (const turn of discarded) {
    const entries = primeTurnEntries(turn.nativeTurnRef?.fingerprint);
    if (entries === undefined) return { type: "unknown", ordinal: turn.ordinal };
    if (first === undefined && entries.lead !== "none") {
      first = { ordinal: turn.ordinal, lead: entries.lead };
    }
  }
  if (first === undefined) return { type: "none" };
  return isStrandedLead(first.lead)
    ? { type: "stranded", ordinal: first.ordinal, lead: first.lead }
    : { type: "fork" };
}

export type PrimeForkBoundary =
  | { readonly type: "keep_all" }
  | { readonly type: "fork"; readonly entryId: string }
  | { readonly type: "error"; readonly message: string }
  | PrimeStrandedTurn;

const entriesMatch = (
  messages: ReadonlyArray<PrimeForkMessage>,
  end: number,
  hashes: ReadonlyArray<string>,
) =>
  end - hashes.length >= 0 &&
  hashes.every(
    (hash, index) => primeUserEntryHash(messages[end - hashes.length + index]?.text ?? "") === hash,
  );

/**
 * Finds the first user entry the turns after `boundaryOrdinal` added, walking
 * them newest first against the tail of `get_fork_messages`. History before
 * the thread's first turn (a forked session's copied entries) stays put.
 * `skipUnmatched` tolerates turns whose entries are gone from the session,
 * which is what a fork source's rolled-back turns look like; the boundary
 * turn's own entries must still sit right before the result.
 */
export function primeForkBoundary(input: {
  readonly messages: ReadonlyArray<PrimeForkMessage>;
  readonly turns: ReadonlyArray<PrimeBoundaryTurn>;
  readonly boundaryOrdinal: number;
  readonly skipUnmatched: boolean;
}): PrimeForkBoundary {
  const discarded = input.turns
    .filter((turn) => turn.ordinal > input.boundaryOrdinal)
    .toSorted((left, right) => right.ordinal - left.ordinal);
  let cursor = input.messages.length;
  let cutOrdinal: number | undefined;
  const leads = new Map<number, PrimeTurnLead>();
  for (const turn of discarded) {
    const entries = primeTurnEntries(turn.nativeTurnRef?.fingerprint);
    if (entries === undefined) {
      return {
        type: "error",
        message: `Prime turn ${turn.ordinal} has no recorded session entries`,
      };
    }
    leads.set(turn.ordinal, entries.lead);
    const { hashes } = entries;
    if (hashes.length === 0) continue;
    if (entriesMatch(input.messages, cursor, hashes)) {
      cursor -= hashes.length;
      cutOrdinal = turn.ordinal;
    } else if (!input.skipUnmatched) {
      return {
        type: "error",
        message: `Prime session history no longer matches turn ${turn.ordinal}`,
      };
    }
  }
  const target = input.turns.find((turn) => turn.ordinal === input.boundaryOrdinal);
  const targetHashes = primeTurnEntries(target?.nativeTurnRef?.fingerprint)?.hashes;
  if (targetHashes !== undefined && !entriesMatch(input.messages, cursor, targetHashes)) {
    return {
      type: "error",
      message: `Prime session history no longer matches turn ${input.boundaryOrdinal}`,
    };
  }
  // The cut lands before the earliest matched turn's first user entry, so a
  // turn at or before it whose history starts otherwise would survive. A fork
  // source's rolled-back turn of that kind cannot be told apart from a live
  // one (it has no user entry to match), so it is refused too.
  for (const turn of discarded.toReversed()) {
    if (cutOrdinal !== undefined && turn.ordinal > cutOrdinal) break;
    const lead = leads.get(turn.ordinal);
    if (isStrandedLead(lead)) return { type: "stranded", ordinal: turn.ordinal, lead };
  }
  const boundary = input.messages[cursor];
  return boundary === undefined
    ? { type: "keep_all" }
    : { type: "fork", entryId: boundary.entryId };
}

/**
 * `untrusted` means the reader does not recognise the file well enough to
 * predict Prime's list. Its reason is fixed text plus line or version
 * numbers, never file content, so it is safe to log.
 */
export type PrimeSessionForkMessages =
  | { readonly type: "messages"; readonly messages: ReadonlyArray<PrimeForkMessage> }
  | { readonly type: "untrusted"; readonly reason: string };

const decodeSessionLine = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));

/** core/session-manager.ts:42 CURRENT_SESSION_VERSION in Prime 1.0.0. */
const PRIME_SESSION_VERSION = 3;

/** The SessionEntry union, core/session-manager.ts:215-229. */
const PRIME_SESSION_ENTRY_TYPES: ReadonlySet<string> = new Set([
  "message",
  "thinking_level_change",
  "service_tier_change",
  "model_change",
  "compaction",
  "branch_summary",
  "custom",
  "child_usage_attributed",
  "custom_message",
  "label",
  "session_info",
  "session_state",
  "agent_status",
  "git_state",
]);

const USER_PARTS: ReadonlySet<string> = new Set(["text", "image"]);
const ASSISTANT_PARTS: ReadonlySet<string> = new Set(["text", "thinking", "toolCall"]);

/**
 * Content shapes per AgentMessage role: pi-ai's Message (packages/ai/src/
 * types.ts:159-238) plus Prime's CustomAgentMessages (core/messages.ts:308-381).
 */
const PRIME_MESSAGE_CONTENT: ReadonlyMap<
  string,
  { readonly text: boolean; readonly parts: ReadonlySet<string> }
> = new Map([
  ["user", { text: true, parts: USER_PARTS }],
  ["custom", { text: true, parts: USER_PARTS }],
  ["assistant", { text: false, parts: ASSISTANT_PARTS }],
  ["toolResult", { text: false, parts: USER_PARTS }],
]);
/** Roles whose fields hold no user-entry text, so forking never reads them. */
const PRIME_CONTENTLESS_ROLES: ReadonlySet<string> = new Set([
  "bashExecution",
  "branchSummary",
  "compactionSummary",
]);

function isPrimeMessage(message: unknown, version: number): boolean {
  const role = recordString(message, "role");
  if (role === undefined) return false;
  if (PRIME_CONTENTLESS_ROLES.has(role)) return true;
  // migrateV2ToV3 renames `hookMessage` to `custom` (core/session-manager.ts:392).
  const shape = PRIME_MESSAGE_CONTENT.get(
    role === "hookMessage" && version === 2 ? "custom" : role,
  );
  if (shape === undefined) return false;
  const content = recordField(message, "content");
  if (shape.text && typeof content === "string") return true;
  return (
    Array.isArray(content) &&
    content.every((part) => {
      const type = recordString(part, "type");
      if (type === undefined || !shape.parts.has(type)) return false;
      return type !== "text" || recordString(part, "text") !== undefined;
    })
  );
}

/**
 * Reads a Prime session JSONL line by line into what `get_fork_messages`
 * would list after `prime --fork` copied it, without asking Prime to copy it.
 * Mirrors Prime's loader (core/session-manager.ts:401-417 and 753-766: blank
 * lines are skipped, and the first entry must be a session header with a
 * string id) and AgentSession.getUserMessagesForForking (core/agent-session.ts:
 * 14817-14843): every `message` entry with role `user` and non-empty text, in
 * file order, abandoned branches included, because SessionManager.getEntries
 * returns every non-header entry.
 *
 * Prime also skips torn lines, but a line the reader cannot parse or an
 * entry shape it does not know could be a newer format, so either makes the
 * whole result untrusted rather than a guess.
 */
export function primeSessionForkReader() {
  let lineNumber = 0;
  let version: number | undefined;
  let untrusted: string | undefined;
  const messages: Array<PrimeForkMessage> = [];
  const readEntry = (entry: unknown): string | undefined => {
    if (version === undefined) {
      if (recordString(entry, "type") !== "session" || recordString(entry, "id") === undefined) {
        return "the file does not start with a Prime session header";
      }
      // migrateToCurrentVersion (core/session-manager.ts:385-395): v1 entries
      // get fresh random ids, so only the copy knows them; v2 -> v3 only
      // renames `hookMessage` roles. Prime rewrites a migrated file when it
      // opens it, so a session T3 has run turns in is already current.
      const field = recordField(entry, "version");
      if (field === undefined) return "the session uses format version 1, which T3 does not read";
      const parsed = primeRecordNumber(entry, "version");
      if (parsed === undefined) return "the session header has a version T3 does not recognise";
      if (parsed !== 2 && parsed !== PRIME_SESSION_VERSION) {
        return `the session uses format version ${parsed}, and T3 reads versions 2 to ${PRIME_SESSION_VERSION}`;
      }
      version = parsed;
      return undefined;
    }
    const type = recordString(entry, "type");
    if (type === undefined) return `line ${lineNumber} is not a session entry`;
    if (!PRIME_SESSION_ENTRY_TYPES.has(type)) {
      return `line ${lineNumber} has an entry type T3 does not recognise`;
    }
    if (type !== "message") return undefined;
    const message = recordField(entry, "message");
    if (!isPrimeMessage(message, version)) {
      return `line ${lineNumber} has a message T3 does not recognise`;
    }
    if (recordString(message, "role") !== "user") return undefined;
    const text = primeContentText(recordField(message, "content"));
    if (text.length === 0) return undefined;
    const entryId = recordString(entry, "id");
    if (entryId === undefined) return `line ${lineNumber} is a user entry without an id`;
    messages.push({ entryId, text });
    return undefined;
  };
  return {
    line(line: string): void {
      lineNumber += 1;
      if (untrusted !== undefined || line.trim().length === 0) return;
      const parsed = decodeSessionLine(line);
      untrusted = Option.isNone(parsed)
        ? `line ${lineNumber} is not JSON`
        : readEntry(parsed.value);
    },
    result(): PrimeSessionForkMessages {
      if (untrusted !== undefined) return { type: "untrusted", reason: untrusted };
      if (version === undefined) return { type: "untrusted", reason: "the file has no entries" };
      return { type: "messages", messages };
    },
  };
}
