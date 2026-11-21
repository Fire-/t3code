import { assert, describe, it } from "@effect/vitest";
import { ProviderDriverKind } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { provideDeterministicTestRuntime } from "../DeterministicRuntime.ts";
import { DROID_MODEL_SELECTION, materializeFixtureInput } from "./shared.ts";

describe("materializeFixtureInput fork_thread", () => {
  it.effect("forks from the named run and runs the later steps on the fork thread", () =>
    Effect.gen(function* () {
      const materialized = yield* materializeFixtureInput({
        scenario: "fork_step",
        fixtureInput: {
          steps: [
            { type: "message", text: "first" },
            { type: "message", text: "second" },
            { type: "fork_thread", sourceRunIndex: 1 },
            { type: "message", text: "on the fork" },
          ],
        },
        driver: ProviderDriverKind.make("droid"),
        modelSelection: DROID_MODEL_SELECTION,
      }).pipe(Effect.provide(IdAllocator.layer), provideDeterministicTestRuntime);

      const [sourceThreadId, forkThreadId] = materialized.projectionThreadIds;
      assert.lengthOf(materialized.projectionThreadIds, 2);
      assert.notEqual(sourceThreadId, forkThreadId);
      assert.deepEqual(
        materialized.commands.map((command) => command.type),
        [
          "thread.create",
          "message.dispatch",
          "message.dispatch",
          "thread.fork",
          "message.dispatch",
        ],
      );

      const [create, first, second, fork, onFork] = materialized.commands;
      assert.equal(create?.type === "thread.create" && create.threadId, sourceThreadId);
      assert.equal(first?.type === "message.dispatch" && first.threadId, sourceThreadId);
      assert.equal(second?.type === "message.dispatch" && second.threadId, sourceThreadId);
      assert.equal(fork?.type === "thread.fork" && fork.sourceThreadId, sourceThreadId);
      assert.equal(fork?.type === "thread.fork" && fork.targetThreadId, forkThreadId);
      assert.equal(onFork?.type === "message.dispatch" && onFork.threadId, forkThreadId);

      const firstRunId = yield* Effect.service(IdAllocator.IdAllocatorV2).pipe(
        Effect.map((ids) => ids.derive.run({ threadId: sourceThreadId!, ordinal: 1 })),
        Effect.provide(IdAllocator.layer),
      );
      assert.deepEqual(fork?.type === "thread.fork" && fork.sourcePoint, {
        type: "run",
        runId: firstRunId,
      });

      const idleThreads = materialized.steps.flatMap((step) =>
        step.type === "await_thread_idle" ? [step.threadId] : [],
      );
      assert.deepEqual(idleThreads, [sourceThreadId, sourceThreadId, forkThreadId]);
    }),
  );
});
