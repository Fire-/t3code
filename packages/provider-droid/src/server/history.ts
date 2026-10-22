/**
 * Droid's stored history, as `load_session` returns it. Droid keeps its own plumbing there next to
 * the conversation: `context-<id>` (the user's private injected context, tens of thousands of
 * characters) and `__internal__system-reminder__<uuid>`, both `llm_only`. None of it may reach an
 * event, an item, or a snapshot, so it is dropped here and never copied.
 *
 * Only the visible messages are read from the list (`visibleDroidHistory`). Turns are never
 * rebuilt from it: a user message in the list may be a steer Droid folded into a running turn,
 * and spec-mode turns carry `llm_only` reminders of their own, so the list alone does not say
 * where a t3 turn begins. The provider turns t3 recorded already carry the message ids, so a
 * rollback reads those (`droidRollbackMessageId`), and a steer stays with the t3 turn it was sent
 * to. A fork reads the stored ids once, to tell the recorded turns the history still holds from
 * the ones a rollback removed (`droidForkRewindMessageId`).
 */
import type { OrchestrationV2ProviderRef } from "@t3tools/contracts";
import * as Predicate from "effect/Predicate";

import { droidMessageText } from "./protocol.ts";

const PLUMBING_ID_PREFIXES = ["context-", "__internal__system-reminder__"] as const;

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

export function visibleDroidHistory(messages: unknown): ReadonlyArray<DroidHistoryMessage> {
  if (!Array.isArray(messages)) return [];
  return messages.flatMap((message): ReadonlyArray<DroidHistoryMessage> => {
    if (!Predicate.isObject(message)) return [];
    const { id, role, visibility, createdAt } = message;
    if (role !== "user" && role !== "assistant") return [];
    if (visibility === "llm_only") return [];
    if (Predicate.isString(id) && isPlumbingId(id)) return [];
    const text = droidMessageText(message["content"]);
    if (text.trim().length === 0) return [];
    return [{ role, text, createdAt: Predicate.isNumber(createdAt) ? createdAt : undefined }];
  });
}

/**
 * The Droid message a t3 rollback rewinds to, read from the provider turns t3 recorded: each one
 * keeps the id of the user message that began it as its `nativeTurnRef`. The boundary is the
 * earliest turn after the target, so a steer folded into the target turn (it sits before the next
 * turn's message) stays with the target. Returns `null` when no turn follows the target (nothing
 * to discard) and `undefined` when that turn has no strong ref to rewind to.
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
 * The message `execute_rewind` takes to cut a fork just after the target turn: the user message of
 * the first later turn the stored history still holds. The recorded turns include ones a rollback
 * removed and compaction turns that began with no user message, so a turn only counts when it
 * has a strong ref whose id `sourceMessageIds` lists. `null` when no such turn follows (the fork
 * already ends at the target) or when there is no target (a fork from the head).
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
