/**
 * The visible conversation in a `load_session` history. Droid stores its own plumbing there next to
 * the conversation: `context-<id>` (the user's private injected context, tens of thousands of
 * characters) and `__internal__system-reminder__<uuid>`, both `llm_only`. None of it may reach an
 * event, an item, or a snapshot, so it is dropped here and never copied.
 */
import * as Predicate from "effect/Predicate";

import { droidMessageText } from "./protocol.ts";

const PLUMBING_ID_PREFIXES = ["context-", "__internal__system-reminder__"] as const;

export interface DroidHistoryMessage {
  readonly role: "user" | "assistant";
  readonly text: string;
  /** Epoch milliseconds, when Droid recorded one. */
  readonly createdAt: number | undefined;
}

export function visibleDroidHistory(messages: unknown): ReadonlyArray<DroidHistoryMessage> {
  if (!Array.isArray(messages)) return [];
  return messages.flatMap((message): ReadonlyArray<DroidHistoryMessage> => {
    if (!Predicate.isObject(message)) return [];
    const { id, role, visibility, createdAt } = message;
    if (role !== "user" && role !== "assistant") return [];
    if (visibility === "llm_only") return [];
    if (Predicate.isString(id) && PLUMBING_ID_PREFIXES.some((prefix) => id.startsWith(prefix))) {
      return [];
    }
    const text = droidMessageText(message["content"]);
    if (text.trim().length === 0) return [];
    return [{ role, text, createdAt: Predicate.isNumber(createdAt) ? createdAt : undefined }];
  });
}
