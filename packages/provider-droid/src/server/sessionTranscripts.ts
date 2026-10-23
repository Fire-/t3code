/**
 * Droid's sessions on disk, as the agent-session importer reads them. Droid writes each session to
 * `~/.factory/sessions/<cwd with "/" as "-">/<session id>.jsonl`: a `session_start` record (id,
 * title, cwd, and `callingSessionId` on a subagent's own session), then `message` records that wrap
 * what `load_session` returns as `{ type: "message", id, timestamp, message: { role, content } }`.
 * The folder name is lossy, so the cwd comes from `session_start`.
 */
import * as Predicate from "effect/Predicate";

import { visibleDroidMessage } from "./history.ts";

export const DROID_HOME_DIRECTORY = ".factory";
export const DROID_SESSIONS_DIRECTORY = "sessions";

// Droid's name for a session it has not titled yet.
const PLACEHOLDER_TITLE = "New Session";

export interface DroidTranscriptRecord {
  readonly type?: string | undefined;
  readonly id?: unknown;
  readonly title?: unknown;
  readonly message?:
    | {
        readonly role?: string | undefined;
        readonly modelId?: unknown;
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

/** The text the user saw of a conversation message, or `null` for anything else. */
export function droidTranscriptMessage(
  record: DroidTranscriptRecord,
): { readonly role: "user" | "assistant"; readonly text: string } | null {
  if (record.type !== "message" || record.message === undefined) return null;
  const visible = visibleDroidMessage({ ...record.message, id: record.id });
  return visible === undefined ? null : { role: visible.role, text: visible.text };
}

/** A subagent's own session. Its parent's transcript already holds the task and its report. */
export function isDroidSubagentSessionStart(record: Readonly<Record<string, unknown>>): boolean {
  return (
    record["type"] === "session_start" && trimmedString(record["callingSessionId"]) !== undefined
  );
}
