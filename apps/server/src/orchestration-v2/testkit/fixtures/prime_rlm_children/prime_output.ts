import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertSemanticProjectionIntegrity,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  projectionFor,
} from "../shared.ts";
import { PRIME_RLM_CHILDREN_PROMPT } from "./input.ts";

const TERMINAL_CHILD_STATUSES = ["done", "error", "cancelled"];

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

/**
 * Prime reports each RLM child through `rlm_child_update` snapshots. Every
 * child must reach a terminal snapshot inside the parent's turn, and T3 must
 * show each one as a provider-native subagent with no child thread, owned by
 * the run that spawned it.
 */
export function assertPrimeRlmChildrenOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const frames = transcript.entries.flatMap((entry, index) =>
    entry.type === "runtime_exit"
      ? []
      : [{ index, direction: entry.type === "expect_outbound" ? "out" : "in", frame: entry.frame }],
  );
  const typeOf = (frame: unknown) => field(frame, "type");
  const prompts = frames.filter(
    (entry) => entry.direction === "out" && typeOf(entry.frame) === "prompt",
  );
  assert.deepEqual(
    prompts.map((entry) => field(entry.frame, "message")),
    [PRIME_RLM_CHILDREN_PROMPT],
  );
  const childUpdates = frames.filter(
    (entry) => entry.direction === "in" && typeOf(entry.frame) === "rlm_child_update",
  );
  const lastByChild = new Map<string, { index: number; status: unknown; done: boolean }>();
  for (const entry of childUpdates) {
    const child = field(entry.frame, "child");
    const id = field(child, "id");
    if (typeof id === "string") {
      const status = field(child, "status");
      const done = lastByChild.get(id)?.done === true || status === "done";
      lastByChild.set(id, { index: entry.index, status, done });
    }
  }
  assert.isAtLeast(lastByChild.size, 1, "Prime reported the spawned child");
  const lastAgentEnd = frames.findLast(
    (entry) => entry.direction === "in" && typeOf(entry.frame) === "agent_end",
  );
  assert.isDefined(lastAgentEnd);
  for (const [id, last] of lastByChild) {
    assert.include(TERMINAL_CHILD_STATUSES, last.status, `child ${id} ends terminal`);
    assert.isBelow(last.index, lastAgentEnd!.index, `child ${id} settles inside the turn`);
  }
  assert.isTrue(
    [...lastByChild.values()].some((last) => last.done),
    "the child finished",
  );

  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assert.lengthOf(projection.runs, 1);
  const run = projection.runs[0]!;
  assert.equal(run.status, "completed");
  assert.deepEqual(
    projection.providerTurns
      .filter((turn) => turn.runAttemptId === run.activeAttemptId)
      .map((turn) => turn.status),
    ["completed"],
    "the run settles exactly once",
  );

  assert.lengthOf(projection.subagents, lastByChild.size, "one subagent per child");
  for (const subagent of projection.subagents) {
    assert.equal(subagent.origin, "provider_native");
    assert.isNull(subagent.childThreadId);
    assert.equal(subagent.runId, run.id);
    assert.include(["completed", "failed", "cancelled"], subagent.status);
    assert.isNotNull(subagent.completedAt);
    const childId = [...lastByChild.keys()].find((id) =>
      (subagent.nativeTaskRef?.nativeId ?? "").endsWith(`:${id}`),
    );
    assert.isDefined(childId, "the subagent is keyed by the Prime child id");
    // A later "cancelled" is Prime's removal signal (rlm.delete_subagent), not a new outcome.
    if (lastByChild.get(childId!)?.done === true) assert.equal(subagent.status, "completed");
  }
  assert.isTrue(projection.subagents.some((subagent) => subagent.status === "completed"));
  const items = projection.turnItems.flatMap((item) => (item.type === "subagent" ? [item] : []));
  assert.sameMembers(
    items.map((item) => item.subagentId),
    projection.subagents.map((subagent) => subagent.id),
  );
  for (const item of items) {
    const subagent = projection.subagents.find((candidate) => candidate.id === item.subagentId);
    assert.equal(item.status, subagent?.status);
    assert.equal(item.origin, "provider_native");
    assert.isNull(item.childThreadId);
    assert.equal(item.runId, run.id);
  }
}
