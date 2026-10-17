import {
  primeRecordField as recordField,
  primeRecordNumber,
  primeRecordString as recordString,
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
