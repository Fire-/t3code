import { assert, describe, it } from "@effect/vitest";
import { ProviderThreadId } from "@t3tools/contracts";
import { derive } from "@t3tools/provider-core/server/IdAllocator";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";

import { DROID_PROVIDER } from "./adapter.ts";
import {
  beginTurn,
  compactTurn,
  completed,
  ensure,
  isTerminal,
  layerTest,
  openItems,
  openRuntime,
  providerThreadUpdates,
  resume,
  runTurn,
  sessionStatuses,
  startTurn,
  terminals,
  THREAD_ID,
  type BegunTurn,
  type Events,
} from "./adapterHarness.testkit.ts";
import {
  FAKE_DROID_DEFAULT_SETTINGS,
  makeFakeDroid,
  type FakeDroidProcess,
} from "./fakeDroid.testkit.ts";

const TOOL_USE_ID = "chatcmpl-tool-0d8e2c1e5251402e8a35e8edbb902ac5";
const CHILD = "5fd3ffdd-4f3f-41c8-8873-e1fd3f6a1378";
const CHILD_THREAD = derive.threadFromProviderThread({
  driver: DROID_PROVIDER,
  nativeThreadId: CHILD,
});

const taskCall = (input: Record<string, unknown>) => ({
  type: "tool_call",
  toolUse: { type: "tool_use", id: TOOL_USE_ID, name: "Task", input },
});

const childSession = {
  type: "child_session_available",
  childSessionId: CHILD,
  toolUseId: TOOL_USE_ID,
  subagentType: "worker",
  description: "dig",
};

const taskResult = (content: string, isError = false) => ({
  type: "tool_result",
  toolUseId: TOOL_USE_ID,
  content,
  isError,
});

const AWAITED_REPORT = (report: string) =>
  `session_id: ${CHILD}\nUse this report as the source of record for the delegated work.\n\n${report}`;

const LAUNCH_ACKNOWLEDGEMENT =
  `Task launched in background.\ntask_id: ${CHILD}\nsession_id: ${CHILD}\n` +
  "subagent_type: worker\ndescription: dig\nThe task is running in a subagent session.";

/** What Droid injects when a background task ends: its CLI template, then the model's instructions. */
const completionReport = (word: "completed" | "stopped" | "error", output: string, failure = "") =>
  [
    `Background task ${word}.`,
    `task_id: ${CHILD}`,
    "type: worker",
    "reason: completed",
    "description: dig",
    ...(failure === "" ? [] : [`failure: ${failure}`]),
    `output: ${output}`,
    "",
    `Background task "dig" (${CHILD}) just ${word}. This is a new asynchronous completion event.`,
  ].join("\n");

const completionMessage = (text: string, taskTurnId = "3a1b2c3d") => ({
  type: "create_message",
  message: {
    id: `task-completion:${taskTurnId}`,
    role: "system",
    content: [{ type: "text", text }],
    visibility: "llm_only",
  },
});

const launchTask = Effect.fnUntraced(function* (
  process: FakeDroidProcess,
  options: { readonly background: boolean },
) {
  yield* process.emit(taskCall({}));
  yield* process.emit(
    taskCall({
      subagent_type: "worker",
      description: "dig",
      prompt: "Dig in.",
      ...(options.background ? { await: false } : {}),
    }),
  );
  yield* process.emit(childSession);
});

const subagentUpdates = (events: Events) =>
  events.flatMap((event) => (event.type === "subagent.updated" ? [event.subagent] : []));

const subagentItems = (events: Events) =>
  events.flatMap((event) =>
    event.type === "turn_item.updated" && event.turnItem.type === "subagent"
      ? [event.turnItem]
      : [],
  );

const rosters = (events: Events) =>
  providerThreadUpdates(events).map((thread) => thread.pendingBackgroundTasks ?? []);

const taskIds = (roster: ReadonlyArray<{ readonly taskId: string }>) =>
  roster.map((task) => task.taskId);

const indexOfEvent = (events: Events, predicate: (event: Events[number]) => boolean) =>
  events.findIndex(predicate);

const isSubagentWith = (status: string) => (event: Events[number]) =>
  event.type === "subagent.updated" && event.subagent.status === status;

/** The index of the first provider thread update that no longer lists any task, after one did. */
const clearedAt = (events: Events) => {
  const listed = indexOfEvent(
    events,
    (event) =>
      event.type === "provider_thread.updated" &&
      (event.providerThread.pendingBackgroundTasks?.length ?? 0) > 0,
  );
  return events.findIndex(
    (event, index) =>
      index > listed &&
      event.type === "provider_thread.updated" &&
      (event.providerThread.pendingBackgroundTasks?.length ?? 0) === 0,
  );
};

/** A turn that launched a background Task and finished: the subagent runs on with no turn open. */
const withBackgroundTask = Effect.gen(function* () {
  const fake = yield* makeFakeDroid();
  const begun = yield* beginTurn(fake, { runtimeMode: "auto" });
  yield* launchTask(begun.process, { background: true });
  yield* begun.process.emit(taskResult(LAUNCH_ACKNOWLEDGEMENT));
  yield* begun.process.emit(completed(1));
  yield* begun.opened.takeEvent(isTerminal);
  return { fake, ...begun };
});

const probes = (begun: BegunTurn) =>
  Effect.gen(function* () {
    const { runtime } = begun.opened;
    const other = { ...begun.providerThread, id: ProviderThreadId.make("provider-thread:other") };
    return {
      session: yield* runtime.hasPendingBackgroundWork!,
      thread: yield* runtime.hasPendingBackgroundWorkForThread!(begun.providerThread),
      otherThread: yield* runtime.hasPendingBackgroundWorkForThread!(other),
    };
  });

describe("DroidAdapterV2 awaited Task subagents", () => {
  it.effect(
    "[VAL-SUB-001] a Task call and its child session start a provider-native subagent, and the result completes it with the report and no framing lines",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { opened, process } = yield* beginTurn(fake, { runtimeMode: "auto" });

        yield* launchTask(process, { background: false });
        yield* opened.takeEvent(isSubagentWith("running"));
        const [started] = subagentUpdates(opened.all);
        assert.strictEqual(started?.origin, "provider_native");
        assert.strictEqual(started?.driver, "droid");
        assert.strictEqual(started?.prompt, "Dig in.");
        assert.strictEqual(started?.title, "dig");
        assert.strictEqual(started?.childThreadId, CHILD_THREAD);
        assert.deepStrictEqual(started?.nativeTaskRef, {
          driver: DROID_PROVIDER,
          nativeId: CHILD,
          strength: "strong",
        });
        assert.deepStrictEqual(
          rosters(opened.all).flat(),
          [],
          "an awaited subagent is not a pending background task",
        );

        yield* process.emit(taskResult(AWAITED_REPORT("Found the bug in a.ts.")));
        yield* process.emit(completed(1));
        yield* opened.takeEvent(isTerminal);

        assert.deepStrictEqual(
          subagentUpdates(opened.all).map((subagent) => [subagent.status, subagent.result]),
          [
            ["running", null],
            ["completed", "Found the bug in a.ts."],
          ],
        );
        const items = subagentItems(opened.all);
        assert.deepStrictEqual(
          items.map((item) => [item.status, item.type === "subagent" ? item.result : undefined]),
          [
            ["running", null],
            ["completed", "Found the bug in a.ts."],
          ],
        );
        assert.strictEqual(items[0]?.id, items[1]?.id, "one item for the whole subagent");
        assert.deepStrictEqual(items[1]?.nativeItemRef, started?.nativeTaskRef);
        assert.isNotNull(subagentUpdates(opened.all)[1]?.completedAt);
        assert.deepStrictEqual(
          opened.all.flatMap((event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool"
              ? [event.turnItem.toolName]
              : [],
          ),
          [],
          "the Task call is not also a dynamic_tool item",
        );
        assert.deepStrictEqual(openItems(opened.all), []);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-SUB-002] a duplicate Task call, a late child session, and a repeated result never reopen a finished subagent",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { opened, process } = yield* beginTurn(fake, { runtimeMode: "auto" });
        yield* launchTask(process, { background: false });
        yield* process.emit(taskResult(AWAITED_REPORT("done")));

        yield* process.emit(taskCall({ subagent_type: "worker", description: "dig", prompt: "x" }));
        yield* process.emit(taskCall({}));
        yield* process.emit(childSession);
        yield* process.emit(taskResult(AWAITED_REPORT("again")));
        yield* process.emit(taskResult("Error: late failure", true));
        yield* process.emit(completed(1));
        yield* opened.takeEvent(isTerminal);

        assert.deepStrictEqual(
          subagentUpdates(opened.all).map((subagent) => [subagent.status, subagent.result]),
          [
            ["running", null],
            ["completed", "done"],
          ],
        );
        assert.deepStrictEqual(
          subagentItems(opened.all).map((item) => item.status),
          ["running", "completed"],
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("[VAL-SUB-002] an awaited Task whose result is an error ends failed", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeDroid();
      const { opened, process } = yield* beginTurn(fake, { runtimeMode: "auto" });
      yield* launchTask(process, { background: false });
      yield* process.emit(taskResult("Error: the subagent crashed", true));
      yield* process.emit(completed(1));
      yield* opened.takeEvent(isTerminal);

      const last = subagentUpdates(opened.all).at(-1);
      assert.strictEqual(last?.status, "failed");
      assert.strictEqual(last?.result, "Error: the subagent crashed");
      assert.deepStrictEqual(openItems(opened.all), []);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  describe("[VAL-SUB-003] settles with the parent turn", () => {
    const openAtTurnEnd = (
      name: string,
      end: (begun: BegunTurn) => Effect.Effect<void>,
      expected: string,
      terminalStatus: string,
    ) =>
      it.effect(name, () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const begun = yield* beginTurn(fake, { runtimeMode: "auto" });
          yield* launchTask(begun.process, { background: false });
          yield* begun.opened.takeEvent(isSubagentWith("running"));

          yield* end(begun);
          const terminal = yield* begun.opened.takeEvent(isTerminal);

          assert.strictEqual(
            terminal.type === "turn.terminal" ? terminal.status : "",
            terminalStatus,
          );
          assert.strictEqual(subagentUpdates(begun.opened.all).at(-1)?.status, expected);
          assert.isBelow(
            indexOfEvent(begun.opened.all, isSubagentWith(expected)),
            indexOfEvent(begun.opened.all, isTerminal),
            "the subagent ends before the parent's terminal",
          );
          assert.deepStrictEqual(openItems(begun.opened.all), [], "nothing stays running");
          assert.strictEqual(terminals(begun.opened.all).length, 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
      );

    openAtTurnEnd(
      "a completed turn ends an open awaited subagent",
      ({ process }) => process.emit(completed(1)),
      "failed",
      "completed",
    );
    openAtTurnEnd(
      "a failed turn ends an open awaited subagent failed",
      ({ process }) => process.emit(completed(1, "model_rate_limited")),
      "failed",
      "failed",
    );
    openAtTurnEnd(
      "an interrupted turn ends an open awaited subagent interrupted",
      (begun) =>
        Effect.gen(function* () {
          yield* begun.opened.runtime.interruptTurn({
            providerThread: begun.providerThread,
            providerTurnId: begun.providerTurn.id,
          });
          yield* begun.process.emit(completed(1, "cancelled"));
        }).pipe(Effect.orDie),
      "interrupted",
      "interrupted",
    );
  });
});

describe("DroidAdapterV2 subagent child threads", () => {
  const onChildThread = (events: Events) =>
    events.flatMap((event) =>
      event.type === "message.updated" && event.message.threadId === CHILD_THREAD
        ? [[event.message.role, event.message.text]]
        : event.type === "node.updated" && event.node.threadId === CHILD_THREAD
          ? [[event.node.kind, event.node.status]]
          : [],
    );

  const isChildEnd = (event: Events[number]) =>
    event.type === "node.updated" &&
    event.node.threadId === CHILD_THREAD &&
    event.node.status === "completed";

  it.effect("a child session gets a child thread, filled from its transcript when it ends", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const home = yield* fs.makeTempDirectoryScoped({ prefix: "t3-droid-home-" });
      const folder = path.join(home, ".factory", "sessions", "-repo");
      yield* fs.makeDirectory(folder, { recursive: true });
      const records = [
        { type: "session_start", id: CHILD, callingSessionId: "parent-session" },
        ...[
          ["user", "Dig in."],
          ["assistant", "Found the bug in a.ts."],
        ].map(([role, text], index) => ({
          type: "message",
          id: `m${index}`,
          timestamp: `2027-01-15T08:00:0${index}.000Z`,
          message: { role, content: [{ type: "text", text }] },
        })),
      ];
      yield* fs.writeFileString(
        path.join(folder, `${CHILD}.jsonl`),
        records.map((record) => JSON.stringify(record)).join("\n"),
      );

      const fake = yield* makeFakeDroid();
      const { opened, process } = yield* beginTurn(fake, {
        runtimeMode: "auto",
        homeDirectory: home,
      });
      yield* launchTask(process, { background: false });
      const created = yield* opened.takeEvent((event) => event.type === "app_thread.created");
      if (created.type !== "app_thread.created") return assert.fail("no child thread");
      assert.strictEqual(created.appThread.id, CHILD_THREAD);
      assert.strictEqual(created.appThread.lineage.parentThreadId, THREAD_ID);

      yield* process.emit(taskResult(AWAITED_REPORT("Found the bug in a.ts.")));
      yield* process.emit(completed(1));
      yield* opened.takeEvent(isTerminal);

      assert.deepStrictEqual(onChildThread(opened.all), [
        ["root_turn", "running"],
        ["user", "Dig in."],
        ["assistant", "Found the bug in a.ts."],
        ["root_turn", "completed"],
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("a background child Droid kept no transcript of still shows its report", () =>
    Effect.gen(function* () {
      const { opened, process } = yield* withBackgroundTask;
      yield* process.emit(completionMessage(completionReport("completed", "sub-ok")));
      yield* opened.takeEvent(isChildEnd);

      assert.deepStrictEqual(onChildThread(opened.all), [
        ["root_turn", "running"],
        ["user", "Dig in."],
        ["assistant", "sub-ok"],
        ["root_turn", "completed"],
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});

describe("Pending background tasks are set on start and cleared on completion, failure, and process exit", () => {
  it.effect(
    "[VAL-SUB-005] start: a background Task lists a pending task before the turn ends, the subagent outlives the turn, and the runtime reports the work",
    () =>
      Effect.gen(function* () {
        const begun = yield* withBackgroundTask;
        const { opened } = begun;

        assert.strictEqual(opened.runtime.backgroundTasksEndWithProcess, true);
        const listed = rosters(opened.all).filter((roster) => roster.length > 0);
        assert.deepStrictEqual(listed[0], [
          { taskId: CHILD, kind: "subagent", description: "dig" },
        ]);
        const firstListed = indexOfEvent(
          opened.all,
          (event) =>
            event.type === "provider_thread.updated" &&
            (event.providerThread.pendingBackgroundTasks?.length ?? 0) > 0,
        );
        assert.isBelow(firstListed, indexOfEvent(opened.all, isTerminal));
        assert.isBelow(firstListed, indexOfEvent(opened.all, isSubagentWith("running")));

        const updates = rosters(opened.all);
        const afterListing = updates.slice(updates.findIndex((roster) => roster.length > 0));
        assert.isTrue(
          afterListing.every((roster) => taskIds(roster).join() === CHILD),
          "every thread update after the launch, the idle one after the terminal included, lists the task",
        );
        assert.strictEqual(providerThreadUpdates(opened.all).at(-1)?.status, "idle");

        assert.deepStrictEqual(
          subagentUpdates(opened.all).map((subagent) => subagent.status),
          ["running"],
          "the subagent is still running after the parent's terminal",
        );
        const [item] = subagentItems(opened.all);
        assert.strictEqual(item?.nativeItemRef?.nativeId, CHILD, "the roster id is the item's ref");
        assert.deepStrictEqual(yield* probes(begun), {
          session: true,
          thread: true,
          otherThread: false,
        });
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-SUB-006] completion: the report completes the subagent with its output and the thread stops listing the task before it",
    () =>
      Effect.gen(function* () {
        const begun = yield* withBackgroundTask;
        const { opened, process } = begun;

        yield* process.emit(completionMessage(completionReport("completed", "sub-ok")));
        yield* opened.takeEvent(isSubagentWith("completed"));

        const last = subagentUpdates(opened.all).at(-1);
        assert.strictEqual(last?.status, "completed");
        assert.strictEqual(last?.result, "sub-ok");
        assert.isNotNull(last?.completedAt);
        assert.strictEqual(last?.runId, subagentUpdates(opened.all)[0]?.runId, "it stays in run 1");
        assert.deepStrictEqual(rosters(opened.all).at(-1), []);
        assert.isBelow(
          clearedAt(opened.all),
          indexOfEvent(opened.all, isSubagentWith("completed")),
          "the roster clears first: the run's subscription closes on the subagent's terminal",
        );
        assert.deepStrictEqual(yield* probes(begun), {
          session: false,
          thread: false,
          otherThread: false,
        });
        assert.strictEqual(terminals(opened.all).length, 1, "the report starts no turn here");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("[VAL-SUB-006] a report for a task the adapter does not know changes nothing", () =>
    Effect.gen(function* () {
      const begun = yield* withBackgroundTask;
      const { opened, process } = begun;

      yield* process.emit(
        completionMessage(completionReport("completed", "x").replace(CHILD, "other-task")),
      );
      yield* process.emit(completionMessage(completionReport("completed", "sub-ok"), "second"));
      yield* opened.takeEvent(isSubagentWith("completed"));

      assert.deepStrictEqual(
        subagentUpdates(opened.all).map((subagent) => [subagent.status, subagent.result]),
        [
          ["running", null],
          ["completed", "sub-ok"],
        ],
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-SUB-006] a report that arrives during a later turn completes the subagent inside it, and the turn still ends once",
    () =>
      Effect.gen(function* () {
        const begun = yield* withBackgroundTask;
        const { opened, process, providerThread } = begun;

        yield* startTurn(opened.runtime, providerThread, { runOrdinal: 2, runtimeMode: "auto" });
        yield* process.emit(completionMessage(completionReport("completed", "folded")));
        yield* process.emit(completed(2));
        yield* opened.takeEvent((event) => isTerminal(event) && event.runOrdinal === 2);

        assert.strictEqual(subagentUpdates(opened.all).at(-1)?.result, "folded");
        assert.strictEqual(terminals(opened.all).filter((t) => t.runOrdinal === 2).length, 1);
        assert.deepStrictEqual(rosters(opened.all).at(-1), []);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-SUB-007] failure: an error report ends the subagent failed with the failure text and clears the entry",
    () =>
      Effect.gen(function* () {
        const begun = yield* withBackgroundTask;
        const { opened, process } = begun;

        yield* process.emit(
          completionMessage(
            completionReport(
              "error",
              "No output available",
              "The child process exited before completing the turn.",
            ),
          ),
        );
        yield* opened.takeEvent(isSubagentWith("failed"));

        const last = subagentUpdates(opened.all).at(-1);
        assert.strictEqual(last?.status, "failed");
        assert.strictEqual(last?.result, "The child process exited before completing the turn.");
        assert.deepStrictEqual(rosters(opened.all).at(-1), []);
        assert.deepStrictEqual(yield* probes(begun), {
          session: false,
          thread: false,
          otherThread: false,
        });
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("[VAL-SUB-007] a stopped report ends the subagent cancelled and clears the entry", () =>
    Effect.gen(function* () {
      const begun = yield* withBackgroundTask;
      const { opened, process } = begun;

      yield* process.emit(completionMessage(completionReport("stopped", "No output available")));
      yield* opened.takeEvent(isSubagentWith("cancelled"));

      const last = subagentUpdates(opened.all).at(-1);
      assert.strictEqual(last?.status, "cancelled");
      assert.isNull(last?.result);
      assert.deepStrictEqual(rosters(opened.all).at(-1), []);
      assert.deepStrictEqual(yield* probes(begun), {
        session: false,
        thread: false,
        otherThread: false,
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-SUB-007] a background Task whose launch fails ends failed and is never listed after",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const begun = yield* beginTurn(fake, { runtimeMode: "auto" });
        yield* launchTask(begun.process, { background: true });
        yield* begun.process.emit(taskResult("Error: could not launch", true));
        yield* begun.process.emit(completed(1));
        yield* begun.opened.takeEvent(isTerminal);

        assert.strictEqual(subagentUpdates(begun.opened.all).at(-1)?.status, "failed");
        assert.deepStrictEqual(rosters(begun.opened.all).at(-1), []);
        assert.deepStrictEqual(yield* probes(begun), {
          session: false,
          thread: false,
          otherThread: false,
        });
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-SUB-008] process exit: a crash while idle clears the entry and finishes the subagent",
    () =>
      Effect.gen(function* () {
        const begun = yield* withBackgroundTask;
        const { opened, process } = begun;

        yield* process.exit(1);
        yield* Deferred.await(opened.streamEnded);

        assert.deepStrictEqual(rosters(opened.all).at(-1), []);
        const last = subagentUpdates(opened.all).at(-1);
        assert.notStrictEqual(last?.status, "running");
        assert.strictEqual(last?.status, "failed");
        assert.isNotNull(last?.completedAt);
        assert.isBelow(
          clearedAt(opened.all),
          indexOfEvent(
            opened.all,
            (event) => event.type === "subagent.updated" && event.subagent.status === "failed",
          ),
        );
        assert.deepStrictEqual(yield* probes(begun), {
          session: false,
          thread: false,
          otherThread: false,
        });
        assert.strictEqual(terminals(opened.all).length, 1, "no turn was open to end");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-SUB-008] process exit: closing the session clears the entry and ends the subagent interrupted",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const sessionScope = yield* Scope.make();
        const opened = yield* openRuntime(fake, { runtimeMode: "auto", sessionScope });
        const thread = yield* ensure(opened, { runtimeMode: "auto" });
        const process = fake.current();
        yield* startTurn(opened.runtime, thread, { runtimeMode: "auto" });
        yield* opened.takeEvent((event) => event.type === "provider_turn.updated");
        yield* launchTask(process, { background: true });
        yield* process.emit(taskResult(LAUNCH_ACKNOWLEDGEMENT));
        yield* process.emit(completed(1));
        yield* opened.takeEvent(isTerminal);

        yield* Scope.close(sessionScope, Exit.void);
        yield* Deferred.await(opened.streamEnded);

        assert.deepStrictEqual(rosters(opened.all).at(-1), []);
        assert.strictEqual(subagentUpdates(opened.all).at(-1)?.status, "interrupted");
        assert.strictEqual(sessionStatuses(opened.all).at(-1), "stopped");
        assert.deepStrictEqual(
          [
            yield* opened.runtime.hasPendingBackgroundWork!,
            yield* opened.runtime.hasPendingBackgroundWorkForThread!(thread),
          ],
          [false, false],
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-SUB-008] process exit: a crash during a turn clears the entry and ends the subagent before the broken terminal",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { opened, process } = yield* beginTurn(fake, { runtimeMode: "auto" });
        yield* launchTask(process, { background: true });
        yield* process.emit(taskResult(LAUNCH_ACKNOWLEDGEMENT));
        yield* process.settle;

        yield* process.exit(1);
        const terminal = yield* opened.takeEvent(isTerminal);

        assert.strictEqual(
          terminal.type === "turn.terminal" ? terminal.threadDisposition : "",
          "broken",
        );
        const terminalAt = indexOfEvent(opened.all, isTerminal);
        assert.isBelow(clearedAt(opened.all), terminalAt);
        assert.isBelow(
          indexOfEvent(
            opened.all,
            (event) => event.type === "subagent.updated" && event.subagent.status === "failed",
          ),
          terminalAt,
        );
        assert.deepStrictEqual(rosters(opened.all).at(-1), []);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "[VAL-SUB-008] a stop that kills the process during a compaction clears the entry and ends the subagent interrupted before the turn's terminal",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({
          sessions: {
            "stored-session": {
              settings: FAKE_DROID_DEFAULT_SETTINGS,
              messages: [
                { id: "u1", role: "user", content: [{ type: "text", text: "first" }] },
                { id: "a1", role: "assistant", content: [{ type: "text", text: "ok" }] },
              ],
            },
          },
          overrides: { "droid.compact_session": () => "silent" },
        });
        yield* fake.deliverGroupSignals;
        const opened = yield* openRuntime(fake, { runtimeMode: "auto" });
        const thread = yield* resume(opened, "stored-session");
        const process = fake.current();
        yield* startTurn(opened.runtime, thread, { runtimeMode: "auto" });
        yield* launchTask(process, { background: true });
        yield* process.emit(taskResult(LAUNCH_ACKNOWLEDGEMENT));
        yield* process.emit(completed(1));
        yield* opened.takeEvent(isTerminal);
        assert.strictEqual(yield* opened.runtime.hasPendingBackgroundWork!, true);

        yield* compactTurn(opened.runtime, thread, { runOrdinal: 2, runtimeMode: "auto" });
        const compaction = yield* opened.takeEvent(
          (event) => event.type === "provider_turn.updated" && event.providerTurn.ordinal === 2,
        );
        if (compaction.type !== "provider_turn.updated") return assert.fail("no compaction turn");
        yield* opened.runtime.interruptTurn({
          providerThread: thread,
          providerTurnId: compaction.providerTurn.id,
        });
        yield* Deferred.await(opened.streamEnded);

        assert.deepStrictEqual(process.signals(), ["SIGTERM"], "the stop killed the process");
        const stoppedTurn = terminals(opened.all).filter((terminal) => terminal.runOrdinal === 2);
        assert.deepStrictEqual(
          stoppedTurn.map((terminal) => terminal.status),
          ["interrupted"],
        );
        assert.deepStrictEqual(rosters(opened.all).at(-1), []);
        assert.strictEqual(subagentUpdates(opened.all).at(-1)?.status, "interrupted");
        const secondTerminalAt = opened.all.findIndex(
          (event) => isTerminal(event) && event.runOrdinal === 2,
        );
        assert.isBelow(clearedAt(opened.all), secondTerminalAt);
        assert.strictEqual(sessionStatuses(opened.all).at(-1), "stopped");
        assert.deepStrictEqual(
          [
            yield* opened.runtime.hasPendingBackgroundWork!,
            yield* opened.runtime.hasPendingBackgroundWorkForThread!(thread),
          ],
          [false, false],
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "a new session on the same runtime ends the background tasks of the session it replaces",
    () =>
      Effect.gen(function* () {
        const begun = yield* withBackgroundTask;
        const { opened } = begun;

        const replaced = yield* ensure(opened, { runtimeMode: "auto" });

        assert.strictEqual(subagentUpdates(opened.all).at(-1)?.status, "interrupted");
        assert.deepStrictEqual(rosters(opened.all).at(-1), []);
        assert.deepStrictEqual(replaced.pendingBackgroundTasks, []);
        assert.deepStrictEqual(yield* opened.runtime.hasPendingBackgroundWork!, false);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("runs a second turn while the task is pending without dropping the entry", () =>
    Effect.gen(function* () {
      const begun = yield* withBackgroundTask;
      const { opened, fake, providerThread } = begun;

      yield* runTurn(opened, fake, providerThread, { runOrdinal: 2, runtimeMode: "auto" });

      const updates = rosters(opened.all);
      assert.isTrue(
        updates
          .slice(updates.findIndex((roster) => roster.length > 0))
          .every((roster) => taskIds(roster).join() === CHILD),
      );
      assert.strictEqual(yield* opened.runtime.hasPendingBackgroundWork!, true);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
