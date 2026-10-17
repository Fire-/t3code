import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  projectionFor,
} from "../shared.ts";
import { PRIME_SESSION_COMMAND_TEXT } from "./input.ts";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

function isIdlePrimeState(data: unknown): boolean {
  const actions = field(data, "sessionActions");
  return (
    field(data, "isStreaming") === false &&
    field(data, "isCompacting") === false &&
    field(actions, "active") === undefined &&
    field(actions, "queuedCount") === 0
  );
}

/**
 * `/goal status` is a Prime session command with no agent run: no
 * `agent_start`/`agent_end`, only the session_command action, Prime's hidden
 * echo, a `goal_update` and the result row. The turn settles once, on an idle
 * `get_state` answered after the action cleared, the echo becomes no item, and
 * the result row becomes a system notice.
 */
export function assertPrimeSessionCommandOutput(
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
  assert.lengthOf(prompts, 1, "one prompt");
  assert.equal(field(prompts[0]!.frame, "message"), PRIME_SESSION_COMMAND_TEXT);
  const promptIndex = prompts[0]!.index;
  const after = frames.filter((entry) => entry.index > promptIndex);

  const inboundTypes = after.flatMap((entry) =>
    entry.direction === "in" ? [typeOf(entry.frame)] : [],
  );
  assert.notInclude(inboundTypes, "agent_start", "the command starts no agent run");
  assert.notInclude(inboundTypes, "agent_end", "the command emits no agent_end");

  const customTypes = after.flatMap((entry) => {
    const message = field(entry.frame, "message");
    return entry.direction === "in" &&
      typeOf(entry.frame) === "message_end" &&
      field(message, "role") === "custom"
      ? [field(message, "customType")]
      : [];
  });
  assert.include(customTypes, "session_slash_command", "Prime echoes the slash text");
  assert.include(customTypes, "session_slash_command_result");

  const actionUpdates = after.filter(
    (entry) => entry.direction === "in" && typeOf(entry.frame) === "session_action_update",
  );
  const activeKinds = actionUpdates.map((entry) =>
    field(field(field(entry.frame, "actions"), "active"), "kind"),
  );
  assert.include(activeKinds, "session_command", "the command runs as a session_command action");
  const clearing = actionUpdates.at(-1)!;
  assert.isUndefined(activeKinds.at(-1), "the last action update clears the command");

  const probes = after.filter(
    (entry) => entry.direction === "out" && typeOf(entry.frame) === "get_state",
  );
  const settleProbe = probes.at(-1);
  assert.isDefined(settleProbe, "the turn settles on a get_state probe");
  const settleAnswer = after.find(
    (entry) =>
      entry.direction === "in" &&
      typeOf(entry.frame) === "response" &&
      field(entry.frame, "command") === "get_state" &&
      field(entry.frame, "id") === field(settleProbe!.frame, "id"),
  );
  assert.isTrue(
    isIdlePrimeState(field(settleAnswer?.frame, "data")),
    "the settle probe answers idle",
  );
  assert.isAbove(settleAnswer!.index, clearing.index, "the turn settles after the action clears");

  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["completed"] });
  const projection = projectionFor(result, transcript.scenario);
  assert.deepEqual(
    projection.providerTurns.map((turn) => turn.status),
    ["completed"],
    "exactly one completed terminal",
  );
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assertUserMessagesInclude(projection, [PRIME_SESSION_COMMAND_TEXT]);
  assert.deepEqual(
    projection.turnItems
      .filter((item) => item.type !== "user_message")
      .filter((item) => JSON.stringify(item).includes(PRIME_SESSION_COMMAND_TEXT)),
    [],
    "Prime's echo of the slash text produces no item",
  );
  assert.deepEqual(
    projection.turnItems.flatMap((item) => (item.type === "system_notice" ? [item.message] : [])),
    ["No active goal."],
    "the result row is one system notice with its text",
  );
}
