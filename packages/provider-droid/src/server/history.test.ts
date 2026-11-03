import { assert, describe, it } from "@effect/vitest";
import { ProviderDriverKind } from "@t3tools/contracts";

import {
  droidForkRewindMessageId,
  droidMessageIds,
  droidRollbackMessageId,
  visibleDroidHistory,
} from "./history.ts";

const text = (value: string) => [{ type: "text", text: value }];

const message = (
  id: string,
  role: string,
  parentId: string | null,
  content: string,
  visibility?: string,
) => ({ id, role, parentId, content: text(content), ...(visibility ? { visibility } : {}) });

describe("droidRollbackMessageId", () => {
  const ref = (nativeId: string | null, strength: "strong" | "weak" = "strong") => ({
    driver: "droid" as never,
    nativeId,
    strength,
  });
  const turn = (ordinal: number, nativeId: string | null, strength?: "strong" | "weak") => ({
    ordinal,
    nativeTurnRef: nativeId === null && strength === undefined ? null : ref(nativeId, strength),
  });
  const turns = [turn(3, "u3"), turn(1, "u1"), turn(2, "u2")];

  it("[VAL-HIST-002] rolling back to a turn rewinds to the earliest turn after it, whatever the list order", () => {
    assert.strictEqual(
      droidRollbackMessageId({
        target: { type: "provider_turn", providerTurn: { ordinal: 1 } },
        providerThreadTurns: turns,
      }),
      "u2",
    );
    assert.strictEqual(
      droidRollbackMessageId({
        target: { type: "provider_turn", providerTurn: { ordinal: 2 } },
        providerThreadTurns: turns,
      }),
      "u3",
    );
  });

  it("[VAL-HIST-003] rolling back to the thread start rewinds to the first turn", () => {
    assert.strictEqual(
      droidRollbackMessageId({ target: { type: "thread_start" }, providerThreadTurns: turns }),
      "u1",
    );
  });

  it("has nothing to discard when no turn follows the target", () => {
    assert.strictEqual(
      droidRollbackMessageId({
        target: { type: "provider_turn", providerTurn: { ordinal: 3 } },
        providerThreadTurns: turns,
      }),
      null,
    );
    assert.strictEqual(
      droidRollbackMessageId({ target: { type: "thread_start" }, providerThreadTurns: [] }),
      null,
    );
  });

  it("skips later turns that never captured a native ref and refuses a weak or blank one", () => {
    assert.strictEqual(
      droidRollbackMessageId({
        target: { type: "provider_turn", providerTurn: { ordinal: 1 } },
        providerThreadTurns: [turn(1, "u1"), turn(2, null), turn(3, "u3")],
      }),
      "u3",
    );
    assert.strictEqual(
      droidRollbackMessageId({
        target: { type: "provider_turn", providerTurn: { ordinal: 1 } },
        providerThreadTurns: [turn(1, "u1"), turn(2, "u2", "weak")],
      }),
      undefined,
    );
    assert.strictEqual(
      droidRollbackMessageId({
        target: { type: "provider_turn", providerTurn: { ordinal: 1 } },
        providerThreadTurns: [turn(1, "u1"), turn(2, null, "strong")],
      }),
      undefined,
    );
  });
});

describe("droidForkRewindMessageId [VAL-HIST-015]", () => {
  const ref = (nativeId: string, strength: "strong" | "weak" = "strong") => ({
    driver: ProviderDriverKind.make("droid"),
    nativeId,
    strength,
  });
  const turn = (ordinal: number, nativeId: string | null, strength?: "strong" | "weak") => ({
    ordinal,
    nativeTurnRef: nativeId === null ? null : ref(nativeId, strength),
  });
  const present = new Set(["u1", "u2", "u3", "u4"]);

  it("[VAL-HIST-015] forking from the head, or from the latest turn, drops nothing", () => {
    const turns = [turn(1, "u1"), turn(2, "u2")];
    assert.strictEqual(
      droidForkRewindMessageId({
        targetOrdinal: undefined,
        providerThreadTurns: turns,
        sourceMessageIds: present,
      }),
      null,
    );
    assert.strictEqual(
      droidForkRewindMessageId({
        targetOrdinal: 2,
        providerThreadTurns: turns,
        sourceMessageIds: present,
      }),
      null,
    );
  });

  it("[VAL-HIST-015] forking from an earlier turn rewinds to the first message after it", () => {
    assert.strictEqual(
      droidForkRewindMessageId({
        targetOrdinal: 1,
        providerThreadTurns: [turn(3, "u3"), turn(1, "u1"), turn(2, "u2")],
        sourceMessageIds: present,
      }),
      "u2",
    );
  });

  it("[VAL-HIST-015] turns the stored history no longer holds (rolled back) do not count as later turns", () => {
    // Turns 2 and 3 were rolled back: the live history holds u1 and u4 only.
    const live = new Set(["u1", "u4"]);
    const turns = [turn(1, "u1"), turn(2, "u2"), turn(3, "u3"), turn(4, "u4")];
    assert.strictEqual(
      droidForkRewindMessageId({
        targetOrdinal: 1,
        providerThreadTurns: turns,
        sourceMessageIds: live,
      }),
      "u4",
    );
    assert.strictEqual(
      droidForkRewindMessageId({
        targetOrdinal: 4,
        providerThreadTurns: turns,
        sourceMessageIds: live,
      }),
      null,
    );
    assert.strictEqual(
      droidForkRewindMessageId({
        targetOrdinal: 1,
        providerThreadTurns: [turn(1, "u1"), turn(2, "u2")],
        sourceMessageIds: new Set(["u1"]),
      }),
      null,
      "every later turn was rolled back, so the head already is the target",
    );
  });

  it("[VAL-HIST-015] turns without a strong Droid message id (a compaction turn, a weak ref) are skipped", () => {
    assert.strictEqual(
      droidForkRewindMessageId({
        targetOrdinal: 1,
        providerThreadTurns: [turn(1, "u1"), turn(2, null), turn(3, "u2", "weak"), turn(4, "u3")],
        sourceMessageIds: present,
      }),
      "u3",
    );
  });
});

describe("droidMessageIds [VAL-HIST-015]", () => {
  it("[VAL-HIST-015] lists every stored message id, plumbing included, and never any content", () => {
    const ids = droidMessageIds([
      message("context-u1", "user", null, "injected private context", "llm_only"),
      message("u1", "user", "context-u1", "hi"),
      message("__internal__system-reminder__x", "user", "u1", "reminder", "llm_only"),
      { role: "assistant", content: text("no id") },
      "not a message",
    ]);
    assert.deepStrictEqual([...ids].toSorted(), [
      "__internal__system-reminder__x",
      "context-u1",
      "u1",
    ]);
    assert.deepStrictEqual([...droidMessageIds(undefined)], []);
  });
});

describe("visibleDroidHistory", () => {
  it("keeps what the user saw: no plumbing, tool calls, or reminders Droid added to a message", () => {
    const reminder = "<system-reminder>\nTodo list is empty\n</system-reminder>";
    const history = visibleDroidHistory([
      message("context-u1", "user", null, "injected private context", "llm_only"),
      { id: "u1", role: "user", content: [...text("Fix the test"), ...text(reminder)] },
      { id: "u2", role: "user", content: text(reminder) },
      { id: "u3", role: "user", content: text(`${reminder}\n\nAnd the lint`) },
      message("u4", "user", "u3", "a task report", "llm_only"),
      { id: "a1", role: "assistant", content: [{ type: "tool_use", id: "t1", name: "Read" }] },
      { id: "u5", role: "user", content: [{ type: "tool_result", tool_use_id: "t1" }] },
      { id: "a2", role: "assistant", content: text("Fixed both"), createdAt: 7 },
    ]);
    assert.deepStrictEqual(history, [
      { role: "user", text: "Fix the test", createdAt: undefined },
      { role: "user", text: "And the lint", createdAt: undefined },
      { role: "assistant", text: "Fixed both", createdAt: 7 },
    ]);
  });
});
