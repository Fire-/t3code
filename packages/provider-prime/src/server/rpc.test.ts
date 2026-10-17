import { assert, describe, it } from "@effect/vitest";
import * as Option from "effect/Option";

import { classifyPrimeEvent, decodePrimeState, isPrimeStateIdle } from "./rpc.ts";

// Shape observed from a live Prime get_state (research/dependency-readiness.md).
const liveIdleState = {
  model: { provider: "mistral", id: "ministral-3b-latest", contextWindow: 128000 },
  thinkingLevel: "off",
  isStreaming: false,
  isCompacting: false,
  steeringMode: "one-at-a-time",
  followUpMode: "one-at-a-time",
  sessionFile: "/tmp/sessions/01a14805-bd0a-7238-b7e5-953b564db16a.jsonl",
  sessionId: "01a14805-bd0a-7238-b7e5-953b564db16a",
  autoCompactionEnabled: true,
  messageCount: 0,
  sessionActions: { queuedCount: 0, steering: [], followUps: [] },
  goal: { active: false, status: "idle", tokensUsed: 0, timeUsedSeconds: 0, continuationsUsed: 0 },
};

describe("Prime RPC frames", () => {
  it.each([
    {
      type: "session_action_update",
      actions: {
        queuedCount: 1,
        steering: ["steer"],
        followUps: [],
        active: { kind: "session_command", phase: "running", label: "/goal status" },
      },
    },
    {
      type: "goal_update",
      goal: {
        active: true,
        status: "active",
        goalId: "goal-1",
        objective: "ship",
        tokensUsed: 10,
        timeUsedSeconds: 2,
        continuationsUsed: 1,
      },
    },
    {
      type: "rlm_child_update",
      child: { id: "child-1", label: "explore", status: "running", sessionDir: "/tmp/child" },
    },
    { type: "compaction_start", reason: "threshold" },
    { type: "compaction_end", reason: "manual", aborted: false, willRetry: false },
    {
      type: "auto_retry_start",
      attempt: 1,
      maxAttempts: 3,
      delayMs: 2000,
      errorMessage: "overloaded",
    },
    { type: "auto_retry_end", success: false, attempt: 3, finalError: "overloaded" },
    { type: "extension_error", extensionPath: "/ext.ts", event: "tool_call", error: "boom" },
    { type: "refine_complete", result: { summary: "done" } },
    { type: "refine_failed", error: "no model" },
    {
      type: "message_start",
      message: {
        role: "custom",
        customType: "goal_context",
        content: "[goal: continuation]",
        display: true,
        details: { kind: "continuation", goalId: "goal-1" },
      },
    },
    {
      type: "message_end",
      message: { role: "custom", customType: "autonomous_status", content: [], display: false },
    },
  ])("[VAL-LAUNCH-001] decodes Prime-only frame $type", (record) => {
    const decoded = classifyPrimeEvent(record);
    assert.equal(decoded._tag, "Prime");
    if (decoded._tag === "Prime") assert.deepEqual<unknown>(decoded.frame, record);
  });

  it("[VAL-LAUNCH-001] decodes an id-less error response as an orphan error", () => {
    assert.deepEqual(
      classifyPrimeEvent({
        type: "response",
        command: "prompt",
        success: false,
        error: "Agent has queued work",
      }),
      { _tag: "OrphanErrorResponse", command: "prompt", error: "Agent has queued work" },
    );
    assert.equal(
      classifyPrimeEvent({ type: "response", id: "t3-4", command: "prompt", success: false })._tag,
      "Passthrough",
    );
  });

  it("[VAL-LAUNCH-001] keeps Pi-shared events and assistant messages as passthrough frames", () => {
    assert.deepEqual(classifyPrimeEvent({ type: "agent_end", messages: [] }), {
      _tag: "Passthrough",
      type: "agent_end",
    });
    assert.deepEqual(
      classifyPrimeEvent({ type: "message_start", message: { role: "assistant", content: [] } }),
      { _tag: "Passthrough", type: "message_start" },
    );
  });

  it("[VAL-LAUNCH-001] reports unknown event types without failing", () => {
    assert.deepEqual(classifyPrimeEvent({ type: "future_prime_event", payload: 1 }), {
      _tag: "Unknown",
      type: "future_prime_event",
    });
    assert.deepEqual(classifyPrimeEvent({ payload: 1 }), { _tag: "Unknown", type: undefined });
  });

  it("[VAL-LAUNCH-001] reads Prime's get_state shape, where queued or active work is not idle", () => {
    const state = Option.getOrThrow(decodePrimeState(liveIdleState));
    assert.equal(state.sessionFile, liveIdleState.sessionFile);
    assert.isTrue(isPrimeStateIdle(state));
    assert.isFalse(
      isPrimeStateIdle({ ...state, sessionActions: { ...state.sessionActions!, queuedCount: 1 } }),
    );
    assert.isFalse(
      isPrimeStateIdle({
        ...state,
        sessionActions: {
          ...state.sessionActions!,
          active: { kind: "turn", phase: "running" },
        },
      }),
    );
    assert.isFalse(isPrimeStateIdle({ ...state, isCompacting: true }));
    assert.isTrue(Option.isNone(decodePrimeState({ sessionFile: "/x.jsonl" })));
  });
});
