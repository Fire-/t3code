import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import { projectionFor } from "../shared.ts";
import { assertSimpleOutput } from "./codex_output.ts";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function field(value: unknown, key: string): unknown {
  return isRecord(value) ? value[key] : undefined;
}

type PrimeFrame =
  | { readonly direction: "out"; readonly frame: unknown }
  | { readonly direction: "in"; readonly frame: unknown };

function primeFrames(transcript: ProviderReplayTranscript): ReadonlyArray<PrimeFrame> {
  return transcript.entries.flatMap((entry): ReadonlyArray<PrimeFrame> => {
    if (entry.type === "expect_outbound") return [{ direction: "out", frame: entry.frame }];
    if (entry.type === "emit_inbound") return [{ direction: "in", frame: entry.frame }];
    return [];
  });
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
 * Prime has no settled event: each turn settles on `agent_end` plus an idle
 * `get_state` probe. The recording must show that probe only after the run's
 * `agent_end` (no polling), its last answer idle, and the projection must hold
 * exactly one completed provider turn per prompt. Prime 1.0.0 rejects
 * `get_entries`, and a rejected session name would be rejected again, so
 * neither may appear more than the recording needs.
 */
export function assertPrimeTurnsSettleOnce(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
  expectedTurns: number,
) {
  const frames = primeFrames(transcript);
  const prompts = frames.filter(
    (entry) => entry.direction === "out" && field(entry.frame, "type") === "prompt",
  );
  assert.lengthOf(prompts, expectedTurns, "one prompt per turn");
  const outboundTypes = frames.flatMap((entry) =>
    entry.direction === "out" ? [field(entry.frame, "type")] : [],
  );
  assert.notInclude(outboundTypes, "get_entries");
  const sessionNames = frames.flatMap((entry) =>
    entry.direction === "out" && field(entry.frame, "type") === "set_session_name"
      ? [field(entry.frame, "name")]
      : [],
  );
  assert.lengthOf(
    sessionNames,
    new Set(sessionNames).size,
    "an unchanged session name is sent once",
  );

  let sawPrompt = false;
  let sawAgentEndSincePrompt = false;
  let settleProbes = 0;
  let lastProbeAnswer: unknown;
  const probeAnswers: Array<unknown> = [];
  for (const entry of frames) {
    const type = field(entry.frame, "type");
    if (entry.direction === "out" && type === "prompt") {
      if (sawPrompt) probeAnswers.push(lastProbeAnswer);
      sawPrompt = true;
      sawAgentEndSincePrompt = false;
      lastProbeAnswer = undefined;
    } else if (entry.direction === "in" && type === "agent_end") {
      sawAgentEndSincePrompt = true;
    } else if (sawPrompt && entry.direction === "out" && type === "get_state") {
      assert.isTrue(sawAgentEndSincePrompt, "get_state is probed only after the run's agent_end");
      settleProbes += 1;
    } else if (
      sawPrompt &&
      entry.direction === "in" &&
      type === "response" &&
      field(entry.frame, "command") === "get_state"
    ) {
      lastProbeAnswer = field(entry.frame, "data");
    }
  }
  probeAnswers.push(lastProbeAnswer);
  assert.isAtLeast(settleProbes, expectedTurns);
  for (const answer of probeAnswers) {
    assert.isTrue(isIdlePrimeState(answer), "each turn settles on an idle get_state answer");
  }

  const projection = projectionFor(result, transcript.scenario);
  assert.deepEqual(
    projection.providerTurns.map((turn) => turn.status),
    Array.from({ length: expectedTurns }, () => "completed"),
  );
}

export function assertPrimeSimpleOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertSimpleOutput(result, transcript);
  assertPrimeTurnsSettleOnce(result, transcript, 1);
}
