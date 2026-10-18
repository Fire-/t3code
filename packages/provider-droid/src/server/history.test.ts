import { assert, describe, it } from "@effect/vitest";
import { ProviderDriverKind } from "@t3tools/contracts";

import {
  droidForkRewindMessageId,
  droidHistoryTurns,
  droidMessageIds,
  droidRewindMessageId,
  droidRollbackMessageId,
  type DroidHistoryTurn,
} from "./history.ts";

const text = (value: string) => [{ type: "text", text: value }];

const message = (
  id: string,
  role: string,
  parentId: string | null,
  content: string,
  visibility?: string,
) => ({ id, role, parentId, content: text(content), ...(visibility ? { visibility } : {}) });

/**
 * Trimmed from a live `load_session` history (probe 18): an auto turn, two spec turns (u2 carries
 * the spec reminder, u3 has an assistant parent), an auto turn after spec mode (u4, which carries
 * "Spec mode has ended." and has the same shape as a folded steer), an `end_of_turn` steer folded
 * into it, a background Task turn, and the task's completion running as a turn of its own.
 */
const P18_HISTORY = [
  message("context-u1", "user", null, "<system-reminder>context</system-reminder>", "llm_only"),
  message("u1", "user", "context-u1", "Remember APPLE."),
  message("a1", "assistant", "u1", "OK"),
  message("context-u2", "user", "a1", "<system-reminder>context</system-reminder>", "llm_only"),
  message("u2", "user", "context-u2", "Reply pong."),
  message(
    "__internal__system-reminder__s",
    "user",
    "u2",
    "<system-reminder>\nSpec mode is active. Do NOT edit files",
    "llm_only",
  ),
  message("a2", "assistant", "__internal__system-reminder__s", "pong"),
  message("u3", "user", "a2", "Reply ping."),
  message("a3", "assistant", "u3", "ping"),
  message("u4", "user", "a3", "Count to 60."),
  message(
    "__internal__system-reminder__e",
    "user",
    "u4",
    "<system-reminder>Spec mode has ended.</system-reminder>",
    "llm_only",
  ),
  message("a4", "assistant", "__internal__system-reminder__e", "1 2 3"),
  message("steer", "user", "a4", "Then reply QUEUED."),
  message(
    "__internal__system-reminder__f",
    "user",
    "steer",
    "<system-reminder>A follow-up user message arrived while you were working. Treat it as steering, not implicit cancellation.</system-reminder>",
    "llm_only",
  ),
  message("a5", "assistant", "__internal__system-reminder__f", "QUEUED"),
  message("u6", "user", "a5", "Launch a background task."),
  message("a6", "assistant", "u6", ""),
  message("tool6", "tool", "a6", "Task launched in background."),
  message("a6b", "assistant", "tool6", "LAUNCHED"),
  message(
    "task-completion:c1",
    "user",
    "a6b",
    "Background task completed.\ntask_id: t1",
    "llm_only",
  ),
  message("a7", "assistant", "task-completion:c1", "The task finished."),
  message("u8", "user", "a7", "Run sleep."),
  message("a8", "assistant", "u8", ""),
  message("tool8", "tool", "a8", "slept"),
  // Overlapping a running user turn, the completion folds in after a tool result.
  message(
    "task-completion:c2",
    "user",
    "tool8",
    "Background task completed.\ntask_id: t2",
    "llm_only",
  ),
  message("a8b", "assistant", "task-completion:c2", "DONE"),
  message("sys", "user", "a8b", "Authentication failed.", "user_only"),
];

const EXPECTED_TURN_IDS = ["u1", "u2", "u3", "u4", "u6", "task-completion:c1", "u8"];

describe("droidHistoryTurns", () => {
  it("[VAL-PLAN-011] a history with plan-mode turns and a folded steer keeps every real turn", () => {
    const turns = droidHistoryTurns(P18_HISTORY);
    assert.deepStrictEqual(
      turns.map((turn) => turn.messageId),
      EXPECTED_TURN_IDS,
    );
    assert.deepStrictEqual(turns[0], { messageId: "u1", text: "Remember APPLE." });
    assert.deepStrictEqual(turns[5], {
      messageId: "task-completion:c1",
      text: "Background task completed.\ntask_id: t1",
    });
  });

  it("[VAL-PLAN-011] spec turns and the first auto turn after spec are not mistaken for steers", () => {
    const ids = droidHistoryTurns(P18_HISTORY).map((turn) => turn.messageId);
    // u2: spec reminder child. u3: assistant parent in spec mode. u4: assistant parent and an
    // "ended" reminder, the shape of the folded steer.
    for (const id of ["u2", "u3", "u4"]) assert.include(ids, id);
    assert.notInclude(ids, "steer");
  });

  it("[VAL-PLAN-011] a steer is detected by the reminder text, even when the reminder is split", () => {
    const turns = droidHistoryTurns([
      message("u1", "user", null, "Long job."),
      message("a1", "assistant", "u1", "working"),
      message("s1", "user", "a1", "Also this."),
      message("r1a", "user", "s1", "<system-reminder>A follow-up user message", "llm_only"),
      message(
        "r1b",
        "user",
        "s1",
        " arrived while you were working.</system-reminder>",
        "llm_only",
      ),
      message("a2", "assistant", "r1b", "done"),
    ]);
    assert.deepStrictEqual(
      turns.map((turn) => turn.messageId),
      ["u1"],
    );
  });

  it("[VAL-PLAN-011] a changed reminder wording counts the steer as a turn rather than hiding a real one", () => {
    const turns = droidHistoryTurns([
      message("u1", "user", null, "Long job."),
      message("a1", "assistant", "u1", "working"),
      message("s1", "user", "a1", "Also this."),
      message(
        "r1",
        "user",
        "s1",
        "<system-reminder>Something else entirely</system-reminder>",
        "llm_only",
      ),
    ]);
    assert.deepStrictEqual(
      turns.map((turn) => turn.messageId),
      ["u1", "s1"],
    );
  });

  it("skips Droid's plumbing by id even without a visibility flag", () => {
    const turns = droidHistoryTurns([
      message("context-u1", "user", null, "private context"),
      message("__internal__system-reminder__x", "user", "context-u1", "a reminder"),
      message("u1", "user", "context-u1", "Hello."),
    ]);
    assert.deepStrictEqual(
      turns.map((turn) => turn.messageId),
      ["u1"],
    );
  });

  it("counts a task completion only when it hangs off an assistant reply", () => {
    const turns = droidHistoryTurns([
      message("u1", "user", null, "Go."),
      message("a1", "assistant", "u1", "ok"),
      message("task-completion:own", "user", "a1", "Background task completed.", "llm_only"),
      message("tool1", "tool", "a1", "result"),
      message("task-completion:folded", "user", "tool1", "Background task completed.", "llm_only"),
      message("task-completion:orphan", "user", null, "Background task completed.", "llm_only"),
    ]);
    assert.deepStrictEqual(
      turns.map((turn) => turn.messageId),
      ["u1", "task-completion:own"],
    );
  });

  it("ignores messages without an id, other roles, and a history that is not a list", () => {
    assert.deepStrictEqual(droidHistoryTurns(undefined), []);
    assert.deepStrictEqual(droidHistoryTurns({ messages: [] }), []);
    assert.deepStrictEqual(
      droidHistoryTurns([
        "not a message",
        { role: "user", content: text("no id") },
        { id: "", role: "user", content: text("empty id") },
        message("a1", "assistant", null, "assistant first"),
        message("u1", "user", null, "Hello."),
      ]).map((turn) => turn.messageId),
      ["u1"],
    );
  });
});

describe("droidRewindMessageId", () => {
  const turns: ReadonlyArray<DroidHistoryTurn> = droidHistoryTurns(P18_HISTORY);

  it("[VAL-PLAN-011] rolling back to a turn rewinds to the next turn, so folded steers stay with their turn", () => {
    const expectedNext = new Map<string, string | undefined>([
      ["u1", "u2"],
      ["u2", "u3"],
      ["u3", "u4"],
      // u4 ran the steer folded into it; the rewind point is the turn after the steer.
      ["u4", "u6"],
      ["u6", "task-completion:c1"],
      ["task-completion:c1", "u8"],
      ["u8", undefined],
    ]);
    assert.deepStrictEqual(
      turns.map((turn) => [
        turn.messageId,
        droidRewindMessageId(turns, { type: "provider_turn", messageId: turn.messageId }),
      ]),
      [...expectedNext],
    );
  });

  it("[VAL-PLAN-011] rolling back to the thread start rewinds to the first turn", () => {
    assert.strictEqual(droidRewindMessageId(turns, { type: "thread_start" }), "u1");
    assert.strictEqual(droidRewindMessageId([], { type: "thread_start" }), undefined);
  });

  it("has no rewind point for a message that is not a turn, such as a folded steer", () => {
    assert.strictEqual(
      droidRewindMessageId(turns, { type: "provider_turn", messageId: "steer" }),
      undefined,
    );
    assert.strictEqual(
      droidRewindMessageId(turns, { type: "provider_turn", messageId: "unknown" }),
      undefined,
    );
  });
});

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
