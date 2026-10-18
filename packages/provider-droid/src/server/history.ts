/**
 * Droid's stored history, as `load_session` returns it. Droid keeps its own plumbing there next to
 * the conversation: `context-<id>` (the user's private injected context, tens of thousands of
 * characters) and `__internal__system-reminder__<uuid>`, both `llm_only`. None of it may reach an
 * event, an item, or a snapshot, so it is dropped here and never copied.
 *
 * Two readings of the same list: the visible messages (`visibleDroidHistory`), and the turns t3
 * saw (`droidHistoryTurns`, with `droidRewindMessageId` to pick a rewind point in them). A rollback
 * t3 asks for does not read the stored list: the provider turns t3 recorded already carry the
 * message ids (`droidRollbackMessageId`), and a steer Droid ran as a native turn of its own stays
 * with the t3 turn it belongs to. A fork reads the stored ids once, to tell the recorded turns
 * the history still holds from the ones a rollback removed (`droidForkRewindMessageId`).
 */
import type { OrchestrationV2ProviderRef } from "@t3tools/contracts";
import * as Predicate from "effect/Predicate";

import { DROID_TASK_COMPLETION_ID_PREFIX, droidMessageText } from "./protocol.ts";

const PLUMBING_ID_PREFIXES = ["context-", "__internal__system-reminder__"] as const;

/**
 * Droid trails a steer it folded into the running turn with this `llm_only` reminder. Mode
 * reminders ("Spec mode is active", "Spec mode has ended.") hang off real turns the same way, so
 * only this text marks a steer.
 */
const FOLDED_STEER_REMINDER = "A follow-up user message arrived while you were working";

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

export interface DroidHistoryTurn {
  /** The Droid user message that began the turn: its `nativeTurnRef`, and what `execute_rewind` takes. */
  readonly messageId: string;
  readonly text: string;
}

/**
 * The turns t3 saw in a stored history, oldest first, so a resumed session has rewind targets.
 * It mirrors what the live adapter records: each user message is a turn, a late steer included,
 * except a steer folded into a running turn. A background task's completion that ran as a turn
 * of its own is stored as an `llm_only` user message hanging off an assistant reply; one folded
 * into a running turn hangs off a tool result instead. Droid's other `llm_only` and `user_only`
 * messages (context, mode reminders, errors) are not turns.
 */
export function droidHistoryTurns(messages: unknown): ReadonlyArray<DroidHistoryTurn> {
  if (!Array.isArray(messages)) return [];
  const roleById = new Map<string, unknown>();
  const reminderByParent = new Map<string, string>();
  for (const message of messages) {
    if (!Predicate.isObject(message)) continue;
    const id = idOf(message["id"]);
    if (id !== undefined) roleById.set(id, message["role"]);
    const parentId = idOf(message["parentId"]);
    if (parentId !== undefined && message["visibility"] === "llm_only") {
      reminderByParent.set(
        parentId,
        `${reminderByParent.get(parentId) ?? ""}${droidMessageText(message["content"])}`,
      );
    }
  }
  return messages.flatMap((message): ReadonlyArray<DroidHistoryTurn> => {
    if (!Predicate.isObject(message) || message["role"] !== "user") return [];
    const messageId = idOf(message["id"]);
    if (messageId === undefined) return [];
    const text = droidMessageText(message["content"]);
    if (messageId.startsWith(DROID_TASK_COMPLETION_ID_PREFIX)) {
      const parentId = idOf(message["parentId"]);
      return parentId !== undefined && roleById.get(parentId) === "assistant"
        ? [{ messageId, text }]
        : [];
    }
    if (isPlumbingId(messageId)) return [];
    if (message["visibility"] === "llm_only" || message["visibility"] === "user_only") return [];
    if (reminderByParent.get(messageId)?.includes(FOLDED_STEER_REMINDER)) return [];
    return [{ messageId, text }];
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

export type DroidRollbackTarget =
  | { readonly type: "thread_start" }
  | { readonly type: "provider_turn"; readonly messageId: string };

/**
 * The message `execute_rewind` takes to end the conversation at `target`: the first turn after
 * it, or the first turn of all when rolling back to the start. A steer folded into the target
 * turn sits before that next turn, so it stays with the target. `undefined` when no turn follows
 * the target or the target is not in `turns`.
 */
export function droidRewindMessageId(
  turns: ReadonlyArray<DroidHistoryTurn>,
  target: DroidRollbackTarget,
): string | undefined {
  if (target.type === "thread_start") return turns[0]?.messageId;
  const index = turns.findIndex((turn) => turn.messageId === target.messageId);
  return index < 0 ? undefined : turns[index + 1]?.messageId;
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
