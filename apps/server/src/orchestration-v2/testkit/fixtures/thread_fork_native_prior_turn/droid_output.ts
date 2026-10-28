import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertSemanticProjectionIntegrity,
  THREAD_FORK_NATIVE_PRIOR_TURN_ALPHA_PROMPT,
  THREAD_FORK_NATIVE_PRIOR_TURN_BETA_PROMPT,
} from "../shared.ts";
import {
  assertDroidFrameCarriesCatalogEffort,
  assertDroidSessionThread,
  droidInitializedSessionId,
  droidRequests,
  droidResponseTo,
} from "../simple/droid_output.ts";
import { DROID_ASSISTANT_REPLIES_PROMPT } from "../thread_rollback/droid_input.ts";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

/**
 * Forking from the first of two source turns: Droid forks the source session, rewinds the copy to
 * before the second turn, and the thread moves to the rewound copy. The source keeps its session
 * and both turns, and the fork's answer holds only what the first turn said.
 */
export function assertDroidThreadForkNativePriorTurnOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const projections = [...result.projections.values()];
  assert.lengthOf(projections, 2, "a source thread and its fork");
  const source = projections.find((projection) =>
    projection.providerThreads.every((thread) => thread.forkedFrom == null),
  );
  const fork = projections.find((projection) =>
    projection.providerThreads.some((thread) => thread.forkedFrom != null),
  );
  assert.isDefined(source);
  assert.isDefined(fork);
  assertSemanticProjectionIntegrity(source!);
  assertSemanticProjectionIntegrity(fork!);

  assert.deepEqual(
    source!.runs.map((run) => run.status),
    ["completed", "completed"],
  );
  assert.deepEqual(
    fork!.runs.map((run) => run.status),
    ["completed"],
  );

  const sends = droidRequests(transcript, "add_user_message");
  assert.deepEqual(
    sends.map((send) => field(send.params, "text")),
    [
      THREAD_FORK_NATIVE_PRIOR_TURN_ALPHA_PROMPT,
      THREAD_FORK_NATIVE_PRIOR_TURN_BETA_PROMPT,
      DROID_ASSISTANT_REPLIES_PROMPT,
    ],
  );

  // load_session(source) -> fork_session -> execute_rewind(fork) -> load_session(rewound fork).
  const sourceSessionId = droidInitializedSessionId(transcript);
  const forks = droidRequests(transcript, "fork_session");
  assert.lengthOf(forks, 1, "the fork asks Droid to fork the source session once");
  const rewinds = droidRequests(transcript, "execute_rewind");
  assert.lengthOf(rewinds, 1, "forking before the latest turn rewinds the copy once");
  const loads = droidRequests(transcript, "load_session");
  assert.lengthOf(loads, 2, "the source is loaded to fork it, the rewound copy to host it");
  const [sourceLoad, finalLoad] = loads;
  assert.equal(field(sourceLoad!.params, "sessionId"), sourceSessionId);
  assert.isAbove(forks[0]!.index, sourceLoad!.index);
  assert.isAbove(rewinds[0]!.index, forks[0]!.index);
  assert.isAbove(finalLoad!.index, rewinds[0]!.index);
  assert.isAbove(forks[0]!.index, sends[1]!.index, "the fork comes after the source's two turns");
  assert.isBelow(finalLoad!.index, sends[2]!.index, "the fork's own turn runs on the rewound copy");
  const settingsAfterLoad = droidRequests(transcript, "update_session_settings").filter(
    (request) => request.index > finalLoad!.index && request.index < sends[2]!.index,
  );
  assert.lengthOf(
    settingsAfterLoad,
    1,
    "the rewound copy gets its settings before the fork's turn",
  );
  assertDroidFrameCarriesCatalogEffort(transcript, finalLoad!, settingsAfterLoad[0]!);

  const forkSessionId = field(droidResponseTo(transcript, forks[0]!), "newSessionId");
  const rewoundSessionId = field(droidResponseTo(transcript, rewinds[0]!), "newSessionId");
  assert.isString(forkSessionId);
  assert.isString(rewoundSessionId);
  assert.equal(field(rewinds[0]!.params, "sessionId"), forkSessionId);
  assert.equal(
    field(rewinds[0]!.params, "messageId"),
    field(sends[1]!.params, "messageId"),
    "the copy is rewound to before the turn after the fork point",
  );
  assert.equal(field(finalLoad!.params, "sessionId"), rewoundSessionId);
  assert.lengthOf(new Set([sourceSessionId, forkSessionId, rewoundSessionId]), 3);

  assertDroidSessionThread(source!, sourceSessionId);
  assertDroidSessionThread(fork!, String(rewoundSessionId));
  assert.equal(
    fork!.providerThreads[0]?.forkedFrom?.providerThreadId,
    source!.providerThreads[0]?.id,
  );
  assert.notEqual(fork!.providerThreads[0]?.id, source!.providerThreads[0]?.id);
  assert.equal(fork!.contextTransfers[0]?.resolution?.strategy, "native_fork");

  const answer = fork!.turnItems.findLast((item) => item.type === "assistant_message");
  assert.include(answer?.text, "fork boundary alpha");
  assert.notInclude(
    answer?.text,
    "fork boundary beta",
    "the fork holds only the history up to the fork point",
  );
}
