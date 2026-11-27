/**
 * `load_session` history holds Droid's `llm_only` plumbing (`context-<id>`, the user's private
 * injected context, and `__internal__system-reminder__<uuid>`); none of it may reach an event,
 * item, or snapshot. Turns are never rebuilt from the list: a user message may be a folded steer,
 * so rollbacks and forks go by the message ids on the provider turns t3 recorded.
 */
import type { OrchestrationV2ProviderRef } from "@t3tools/contracts";
import * as Predicate from "effect/Predicate";

import { droidMessageText } from "./protocol.ts";

const PLUMBING_ID_PREFIXES = ["context-", "__internal__system-reminder__"] as const;
// Droid adds these to a visible user message as text blocks of their own; they are its context, not
// the user's words.
const SYSTEM_REMINDER = /<system-reminder>[\s\S]*?<\/system-reminder>/g;

const isPlumbingId = (id: string): boolean =>
  PLUMBING_ID_PREFIXES.some((prefix) => id.startsWith(prefix));

const idOf = (value: unknown): string | undefined =>
  Predicate.isString(value) && value.length > 0 ? value : undefined;

export interface DroidHistoryMessage {
  readonly role: "user" | "assistant";
  readonly text: string;
  /** Epoch milliseconds, when Droid recorded one. */
  readonly createdAt: number | undefined;
}

/** One stored message as the user saw it, or `undefined` for plumbing and messages with no text. */
export function visibleDroidMessage(message: unknown): DroidHistoryMessage | undefined {
  if (!Predicate.isObject(message)) return undefined;
  const { id, role, visibility, createdAt } = message;
  if (role !== "user" && role !== "assistant") return undefined;
  if (visibility === "llm_only") return undefined;
  if (Predicate.isString(id) && isPlumbingId(id)) return undefined;
  const text = droidMessageText(message["content"]).replace(SYSTEM_REMINDER, "").trim();
  if (text.length === 0) return undefined;
  return { role, text, createdAt: Predicate.isNumber(createdAt) ? createdAt : undefined };
}

export function visibleDroidHistory(messages: unknown): ReadonlyArray<DroidHistoryMessage> {
  if (!Array.isArray(messages)) return [];
  return messages.flatMap((message) => {
    const visible = visibleDroidMessage(message);
    return visible === undefined ? [] : [visible];
  });
}

/**
 * Each recorded provider turn keeps the id of its first user message as `nativeTurnRef`. The
 * boundary is the earliest turn after the target, so a steer folded into the target stays with it.
 * `null` when no turn follows the target (nothing to discard), `undefined` when that turn has no
 * strong ref to rewind to.
 */
export function droidRollbackMessageId(input: {
  readonly target:
    | { readonly type: "thread_start" }
    | { readonly type: "provider_turn"; readonly providerTurn: { readonly ordinal: number } };
  readonly providerThreadTurns: ReadonlyArray<{
    readonly ordinal: number;
    readonly nativeTurnRef: OrchestrationV2ProviderRef | null;
  }>;
}): string | null | undefined {
  const boundaryOrdinal =
    input.target.type === "thread_start" ? 0 : input.target.providerTurn.ordinal;
  const ref = input.providerThreadTurns
    .filter((turn) => turn.ordinal > boundaryOrdinal && turn.nativeTurnRef !== null)
    .toSorted((a, b) => a.ordinal - b.ordinal)[0]?.nativeTurnRef;
  if (ref === undefined || ref === null) return null;
  return ref.strength === "strong" && ref.nativeId !== null ? ref.nativeId : undefined;
}

/** Every message id of a stored history, plumbing included. Ids only: no content leaves here. */
export function droidMessageIds(messages: unknown): ReadonlySet<string> {
  const ids = new Set<string>();
  if (!Array.isArray(messages)) return ids;
  for (const message of messages) {
    const id = Predicate.isObject(message) ? idOf(message["id"]) : undefined;
    if (id !== undefined) ids.add(id);
  }
  return ids;
}

/**
 * `execute_rewind` cuts the fork at the first later turn the stored history still holds. Recorded
 * turns include ones a rollback removed and compaction turns with no user message, so a turn counts
 * only with a strong ref whose id `sourceMessageIds` lists. `null` when no such turn follows or
 * there is no target (a fork from the head).
 */
export function droidForkRewindMessageId(input: {
  readonly targetOrdinal: number | undefined;
  readonly providerThreadTurns: ReadonlyArray<{
    readonly ordinal: number;
    readonly nativeTurnRef: OrchestrationV2ProviderRef | null;
  }>;
  readonly sourceMessageIds: ReadonlySet<string>;
}): string | null {
  const { targetOrdinal } = input;
  if (targetOrdinal === undefined) return null;
  for (const turn of input.providerThreadTurns.toSorted((a, b) => a.ordinal - b.ordinal)) {
    const ref = turn.nativeTurnRef;
    if (turn.ordinal <= targetOrdinal || ref === null) continue;
    if (ref.strength !== "strong" || ref.nativeId === null) continue;
    if (input.sourceMessageIds.has(ref.nativeId)) return ref.nativeId;
  }
  return null;
}
