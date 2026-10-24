/**
 * Droid's sessions on disk, as the agent-session importer reads them. Droid writes each session to
 * `~/.factory/sessions/<cwd with "/" as "-">/<session id>.jsonl`: a `session_start` record (id,
 * title, cwd, and `callingSessionId` on a subagent's own session), then `message` records that wrap
 * what `load_session` returns as `{ type: "message", id, timestamp, message: { role, content } }`.
 * The folder name is lossy, so the cwd comes from `session_start`.
 */
import * as Predicate from "effect/Predicate";

import { droidToolItemFields, droidToolItemType, type DroidToolItemFields } from "./events.ts";
import { visibleDroidMessage } from "./history.ts";
import {
  droidMessageText,
  droidTaskCompletionOf,
  droidTaskResultReport,
  droidToolKind,
} from "./protocol.ts";
import { droidTaskCompletionOutcome, type DroidSubagentOutcome } from "./subagents.ts";

export const DROID_HOME_DIRECTORY = ".factory";
export const DROID_SESSIONS_DIRECTORY = "sessions";

// Droid's name for a session it has not titled yet.
const PLACEHOLDER_TITLE = "New Session";

export interface DroidTranscriptRecord {
  readonly type?: string | undefined;
  readonly id?: unknown;
  readonly title?: unknown;
  readonly timestamp?: string | undefined;
  readonly message?:
    | {
        readonly role?: string | undefined;
        readonly modelId?: unknown;
        readonly visibility?: unknown;
        readonly content?: unknown;
      }
    | undefined;
}

export interface DroidTranscriptFields {
  readonly sessionId: string | undefined;
  readonly title: string | undefined;
  readonly customTitle: string | null;
  readonly model: string | undefined;
}

const trimmedString = (value: unknown): string | undefined =>
  Predicate.isString(value) && value.trim().length > 0 ? value.trim() : undefined;

/** Session id and title from `session_start`; the model of the latest assistant message. */
export function foldDroidTranscriptFields(
  fields: DroidTranscriptFields,
  record: DroidTranscriptRecord,
): DroidTranscriptFields {
  if (record.type === "session_start") {
    const title = trimmedString(record.title);
    return {
      ...fields,
      sessionId: fields.sessionId ?? trimmedString(record.id),
      title: title !== undefined && title !== PLACEHOLDER_TITLE ? title : fields.title,
    };
  }
  const model =
    record.type === "message" && record.message?.role === "assistant"
      ? trimmedString(record.message.modelId)
      : undefined;
  return model === undefined ? fields : { ...fields, model };
}

const visibleTranscriptMessage = (record: DroidTranscriptRecord) =>
  record.type !== "message" || record.message === undefined
    ? undefined
    : visibleDroidMessage({ ...record.message, id: record.id });

// Droid stores its own notices (errors, usage limits) as user messages only the user sees.
const isDroidNotice = (record: DroidTranscriptRecord) =>
  record.message?.role === "user" && record.message.visibility === "user_only";

/** The text the user saw of a conversation message, or `null` for anything else. */
export function droidTranscriptMessage(
  record: DroidTranscriptRecord,
): { readonly role: "user" | "assistant"; readonly text: string } | null {
  if (isDroidNotice(record)) return null;
  const visible = visibleTranscriptMessage(record);
  return visible === undefined ? null : { role: visible.role, text: visible.text };
}

function droidTranscriptNotice(record: DroidTranscriptRecord): string | undefined {
  return isDroidNotice(record) ? visibleTranscriptMessage(record)?.text : undefined;
}

export type DroidTranscriptActivity =
  | { readonly kind: "tool"; readonly item: DroidToolItemFields }
  | {
      readonly kind: "subagent";
      readonly title: string | null;
      readonly prompt: string;
      readonly status: DroidSubagentOutcome["status"];
      readonly result: string | null;
    }
  | { readonly kind: "notice"; readonly message: string };

export type DroidTranscriptEntry =
  | {
      readonly type: "message";
      readonly role: "user" | "assistant";
      readonly text: string;
      readonly timestamp: string | undefined;
    }
  | {
      readonly type: "activity";
      readonly startedAt: string | undefined;
      readonly completedAt: string | undefined;
      readonly activity: DroidTranscriptActivity;
    };

const blocksOf = (
  record: DroidTranscriptRecord,
): ReadonlyArray<Readonly<Record<string, unknown>>> =>
  record.type === "message" && Array.isArray(record.message?.content)
    ? record.message.content.filter(Predicate.isObject)
    : [];

function storedTaskCompletion(record: DroidTranscriptRecord) {
  return record.type === "message" && record.message !== undefined
    ? droidTaskCompletionOf({ message: { ...record.message, id: record.id } })
    : undefined;
}

/** Whether the record holds history the importer shows: a visible message or notice, tool traffic, or a task report. */
export function isDroidTranscriptHistoryRecord(record: DroidTranscriptRecord): boolean {
  return (
    droidTranscriptMessage(record) !== null ||
    droidTranscriptNotice(record) !== undefined ||
    blocksOf(record).some(
      (block) => block["type"] === "tool_use" || block["type"] === "tool_result",
    ) ||
    storedTaskCompletion(record) !== undefined
  );
}

// A background Task's result only acknowledges the launch; its report arrives later as a message.
const BACKGROUND_TASK_ID = /^task_id: *(\S+)/mu;

interface PendingTool {
  readonly kind: "tool";
  readonly name: string;
  readonly input: unknown;
  readonly startedAt: string | undefined;
  result?: {
    readonly content: unknown;
    readonly isError: boolean;
    readonly at: string | undefined;
  };
}

interface PendingTask {
  readonly kind: "subagent";
  readonly title: string | null;
  readonly prompt: string;
  readonly startedAt: string | undefined;
  outcome?: DroidSubagentOutcome & { readonly at: string | undefined };
}

/**
 * A stored session as the user saw it live: the visible messages, Droid's notices, each tool call
 * settled by its result, and each Task as a subagent with its report. Thinking and plumbing are
 * left out.
 */
export function droidTranscriptEntries(
  records: Iterable<DroidTranscriptRecord>,
): ReadonlyArray<DroidTranscriptEntry> {
  const order: Array<DroidTranscriptEntry | PendingTool | PendingTask> = [];
  const calls = new Map<string, PendingTool | PendingTask>();
  const backgroundTasks = new Map<string, PendingTask>();
  for (const record of records) {
    const at = record.timestamp;
    const completion = storedTaskCompletion(record);
    if (completion !== undefined) {
      const task = backgroundTasks.get(completion.taskId);
      if (task !== undefined && task.outcome === undefined) {
        task.outcome = { ...droidTaskCompletionOutcome(completion), at };
      }
      continue;
    }
    const message = droidTranscriptMessage(record);
    if (message !== null) order.push({ type: "message", ...message, timestamp: at });
    const notice = droidTranscriptNotice(record);
    if (notice !== undefined) {
      order.push({
        type: "activity",
        startedAt: at,
        completedAt: at,
        activity: { kind: "notice", message: notice },
      });
    }
    for (const block of blocksOf(record)) {
      if (block["type"] === "tool_use") {
        const id = trimmedString(block["id"]);
        const name = trimmedString(block["name"]);
        if (id === undefined || name === undefined || calls.has(id)) continue;
        const input = block["input"];
        const call: PendingTool | PendingTask =
          droidToolKind(name) === "task"
            ? {
                kind: "subagent",
                title: Predicate.isObject(input)
                  ? (trimmedString(input["description"]) ?? null)
                  : null,
                prompt: Predicate.isObject(input) ? (trimmedString(input["prompt"]) ?? "") : "",
                startedAt: at,
              }
            : { kind: "tool", name, input: input ?? {}, startedAt: at };
        calls.set(id, call);
        order.push(call);
        continue;
      }
      if (block["type"] !== "tool_result") continue;
      const call = calls.get(trimmedString(block["tool_use_id"]) ?? "");
      const isError = block["is_error"] === true;
      if (call?.kind === "tool" && call.result === undefined) {
        call.result = { content: block["content"], isError, at };
      } else if (call?.kind === "subagent" && call.outcome === undefined) {
        const taskId = BACKGROUND_TASK_ID.exec(droidMessageText(block["content"]))?.[1];
        if (taskId !== undefined && !isError) {
          backgroundTasks.set(taskId, call);
        } else {
          call.outcome = {
            status: isError ? "failed" : "completed",
            result: droidTaskResultReport(block["content"]) ?? null,
            at,
          };
        }
      }
    }
  }
  return order.map((entry): DroidTranscriptEntry => {
    if (!("kind" in entry)) return entry;
    if (entry.kind === "tool") {
      const settlement =
        entry.result === undefined
          ? ({ status: "interrupted" } as const)
          : {
              status: entry.result.isError ? ("failed" as const) : ("completed" as const),
              content: entry.result.content,
            };
      const itemType = droidToolItemType(entry.name, entry.input, true) ?? "dynamic_tool";
      return {
        type: "activity",
        startedAt: entry.startedAt,
        completedAt: entry.result?.at ?? entry.startedAt,
        activity: { kind: "tool", item: droidToolItemFields(entry, itemType, settlement) },
      };
    }
    return {
      type: "activity",
      startedAt: entry.startedAt,
      completedAt: entry.outcome?.at ?? entry.startedAt,
      activity: {
        kind: "subagent",
        title: entry.title,
        prompt: entry.prompt,
        status: entry.outcome?.status ?? "interrupted",
        result: entry.outcome?.result ?? null,
      },
    };
  });
}

/** A subagent's own session. Its parent's transcript already holds the task and its report. */
export function isDroidSubagentSessionStart(record: Readonly<Record<string, unknown>>): boolean {
  return (
    record["type"] === "session_start" && trimmedString(record["callingSessionId"]) !== undefined
  );
}
