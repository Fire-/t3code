import { assert, describe, it } from "@effect/vitest";
import {
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import { derive } from "@t3tools/provider-core/server/IdAllocator";
import type { ProviderAdapterV2Event } from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";

import type { DroidEventContext, DroidNotification } from "./events.ts";
import {
  type DroidSubagent,
  type DroidSubagentUpdate,
  droidSubagentRoster,
  droidTaskCall,
  launchDroidSubagent,
  noteDroidTaskCall,
  settleDroidSubagent,
  settleDroidTaskResult,
} from "./subagents.ts";

const DROID = ProviderDriverKind.make("droid");
const T0 = DateTime.makeUnsafe(1_800_000_000_000);
const at = (offsetMs: number) => DateTime.makeUnsafe(1_800_000_000_000 + offsetMs);

const providerThread: OrchestrationV2ProviderThread = {
  id: ProviderThreadId.make("provider-thread-1"),
  driver: DROID,
  providerInstanceId: ProviderInstanceId.make("droid"),
  providerSessionId: null,
  appThreadId: null,
  ownerNodeId: null,
  nativeThreadRef: { driver: DROID, nativeId: "droid-session-1", strength: "strong" },
  nativeConversationHeadRef: null,
  status: "active",
  firstRunOrdinal: 1,
  lastRunOrdinal: 1,
  handoffIds: [],
  forkedFrom: null,
  pendingBackgroundTasks: [],
  contextUsage: null,
  nativeMetadata: null,
  goal: null,
  createdAt: T0,
  updatedAt: T0,
};

const providerTurn: OrchestrationV2ProviderTurn = {
  id: ProviderTurnId.make("provider-turn-1"),
  providerThreadId: providerThread.id,
  nodeId: NodeId.make("root-node-1"),
  runAttemptId: null,
  nativeTurnRef: { driver: DROID, nativeId: "user-message-1", strength: "strong" },
  ordinal: 1,
  status: "running",
  startedAt: T0,
  completedAt: null,
};

const ctx: DroidEventContext = {
  threadId: ThreadId.make("thread-1"),
  runId: RunId.make("run-1"),
  rootNodeId: NodeId.make("root-node-1"),
  providerThread,
  providerTurn,
  derive,
};

const TOOL_USE_ID = "chatcmpl-tool-00000000000000000000000000000001";
const CHILD = "00000000-0000-4000-8000-0000000000c1";

const taskCall = (input: Record<string, unknown>, toolUseId = TOOL_USE_ID): DroidNotification => ({
  type: "tool_call",
  toolUse: { type: "tool_use", id: toolUseId, name: "Task", input },
});
const childSession = (childSessionId = CHILD, toolUseId = TOOL_USE_ID): DroidNotification => ({
  type: "child_session_available",
  childSessionId,
  toolUseId,
  subagentType: "worker",
  description: "dig",
});
const taskResult = (
  content: unknown,
  isError = false,
  toolUseId = TOOL_USE_ID,
): DroidNotification => ({ type: "tool_result", toolUseId, content, isError });

const FRAMED_REPORT =
  `session_id: ${CHILD}\n` +
  "Use this report as the source of record for the delegated work.\n\n" +
  "The report.";

const AWAITED_INPUT = { subagent_type: "worker", description: "dig", prompt: "Dig in." };
const NOT_STOPPED = { interrupted: false, declined: false };

const noteCall = (known: DroidSubagent | undefined, notification: DroidNotification) => {
  const call = droidTaskCall(notification);
  assert.isDefined(call);
  return noteDroidTaskCall(known, ctx, call!);
};

const launched = (input: Record<string, unknown> = AWAITED_INPUT) => {
  const called = noteCall(noteCall(undefined, taskCall({})), taskCall(input));
  const update = launchDroidSubagent(called, ctx, childSession(), 101, at(10));
  assert.isDefined(update);
  return update!;
};

const ofType = <Type extends ProviderAdapterV2Event["type"]>(
  events: ReadonlyArray<ProviderAdapterV2Event>,
  type: Type,
) =>
  events.filter(
    (event): event is Extract<ProviderAdapterV2Event, { readonly type: Type }> =>
      event.type === type,
  );

const statusesOf = (update: DroidSubagentUpdate | undefined) =>
  (update?.events ?? []).flatMap((event) =>
    event.type === "subagent.updated"
      ? [event.subagent.status]
      : event.type === "turn_item.updated"
        ? [event.turnItem.status]
        : event.type === "node.updated"
          ? [event.node.status]
          : [],
  );

describe("droidTaskCall", () => {
  it("reads a Task call and ignores any other tool", () => {
    assert.deepStrictEqual(droidTaskCall(taskCall(AWAITED_INPUT)), {
      toolUseId: TOOL_USE_ID,
      input: AWAITED_INPUT,
    });
    assert.deepStrictEqual(droidTaskCall(taskCall({})), { toolUseId: TOOL_USE_ID, input: {} });
    assert.isUndefined(
      droidTaskCall({
        type: "tool_call",
        toolUse: { type: "tool_use", id: TOOL_USE_ID, name: "Read", input: {} },
      }),
    );
  });
});

describe("awaited Task subagent", () => {
  it("[VAL-SUB-001] starts at child_session_available as a provider-native subagent", () => {
    const update = launched();
    const [subagentEvent] = ofType(update.events, "subagent.updated");
    assert.isDefined(subagentEvent);
    const { subagent } = subagentEvent!;
    assert.strictEqual(subagent.origin, "provider_native");
    assert.strictEqual(subagent.driver, "droid");
    assert.strictEqual(subagent.status, "running");
    assert.strictEqual(subagent.prompt, "Dig in.");
    assert.strictEqual(subagent.title, "dig");
    assert.strictEqual(subagent.childThreadId, null);
    assert.strictEqual(subagent.runId, ctx.runId);
    assert.deepStrictEqual(subagent.nativeTaskRef, {
      driver: DROID,
      nativeId: CHILD,
      strength: "strong",
    });
    assert.isNull(subagent.completedAt);

    const [item] = ofType(update.events, "turn_item.updated");
    assert.strictEqual(item?.turnItem.type, "subagent");
    assert.strictEqual(item?.turnItem.status, "running");
    assert.strictEqual(item?.turnItem.ordinal, 101);
    assert.deepStrictEqual(item?.turnItem.nativeItemRef, subagent.nativeTaskRef);
    if (item?.turnItem.type === "subagent") {
      assert.strictEqual(item.turnItem.subagentId, subagent.id);
      assert.strictEqual(item.turnItem.origin, "provider_native");
    }
    assert.deepStrictEqual(
      ofType(update.events, "node.updated").map((event) => [event.node.kind, event.node.status]),
      [["subagent", "running"]],
    );
    assert.isTrue(update.surfaced);
    assert.deepStrictEqual(droidSubagentRoster([update.subagent]), []);
  });

  it("[VAL-SUB-001] completes with the report, minus the session id and source-of-record lines", () => {
    const running = launched();
    const done = settleDroidTaskResult(
      running.subagent,
      ctx,
      taskResult(FRAMED_REPORT),
      NOT_STOPPED,
      102,
      at(20),
    );
    assert.isDefined(done);
    const [subagentEvent] = ofType(done!.events, "subagent.updated");
    assert.strictEqual(subagentEvent?.subagent.status, "completed");
    assert.strictEqual(subagentEvent?.subagent.result, "The report.");
    assert.isNotNull(subagentEvent?.subagent.completedAt);
    const [item] = ofType(done!.events, "turn_item.updated");
    if (item?.turnItem.type !== "subagent") return assert.fail("expected a subagent item");
    assert.strictEqual(item.turnItem.result, "The report.");
    assert.strictEqual(item.turnItem.status, "completed");
    const [started] = ofType(running.events, "turn_item.updated");
    assert.strictEqual(item.turnItem.id, started?.turnItem.id);
    assert.strictEqual(item.turnItem.ordinal, started?.turnItem.ordinal);
    assert.isFalse(done!.surfaced);
  });

  it("[VAL-SUB-002] ends failed when the Task result is an error", () => {
    const failed = settleDroidTaskResult(
      launched().subagent,
      ctx,
      taskResult("Error: the subagent crashed", true),
      NOT_STOPPED,
      102,
      at(20),
    );
    assert.deepStrictEqual(statusesOf(failed), ["failed", "failed", "failed"]);
    assert.strictEqual(failed?.subagent.status, "failed");
  });

  it("ends an errored Task cancelled when the user declined it and interrupted when stopped", () => {
    const run = (stop: { readonly interrupted: boolean; readonly declined: boolean }) =>
      settleDroidTaskResult(
        launched().subagent,
        ctx,
        taskResult("Error: stopped", true),
        stop,
        102,
        at(20),
      )?.subagent.status;
    assert.strictEqual(run({ interrupted: false, declined: true }), "cancelled");
    assert.strictEqual(run({ interrupted: true, declined: true }), "interrupted");
  });

  it("[VAL-SUB-002] never returns to running once it has finished", () => {
    for (const result of [taskResult(FRAMED_REPORT), taskResult("Error: it failed", true)]) {
      const settled = settleDroidTaskResult(
        launched().subagent,
        ctx,
        result,
        NOT_STOPPED,
        102,
        at(20),
      )!.subagent;
      // A duplicate Task call keeps the finished record.
      assert.strictEqual(noteCall(settled, taskCall(AWAITED_INPUT)), settled);
      assert.strictEqual(noteCall(settled, taskCall({})), settled);
      assert.isUndefined(launchDroidSubagent(settled, ctx, childSession(), 103, at(30)));
      assert.isUndefined(
        settleDroidTaskResult(settled, ctx, taskResult(FRAMED_REPORT), NOT_STOPPED, 103, at(30)),
      );
      assert.isUndefined(
        settleDroidTaskResult(settled, ctx, taskResult("Error: x", true), NOT_STOPPED, 103, at(30)),
      );
      assert.isUndefined(settleDroidSubagent(settled, { status: "failed", result: null }, at(30)));
    }
  });

  it("does not start a subagent twice for a repeated child_session_available", () => {
    const running = launched();
    assert.isUndefined(launchDroidSubagent(running.subagent, ctx, childSession(), 102, at(11)));
  });

  it("waits for the Task input before it knows the Task is a background one", () => {
    const empty = noteCall(undefined, taskCall({}));
    assert.isFalse(empty.background);
    assert.strictEqual(empty.status, "pending");
    const full = noteCall(empty, taskCall({ ...AWAITED_INPUT, await: false }));
    assert.isTrue(full.background);
    assert.strictEqual(full.prompt, "Dig in.");
  });

  it("announces a Task whose result comes before any child session, so it is not lost", () => {
    const pending = noteCall(noteCall(undefined, taskCall({})), taskCall(AWAITED_INPUT));
    const failed = settleDroidTaskResult(
      pending,
      ctx,
      taskResult("Error: could not start", true),
      NOT_STOPPED,
      101,
      at(5),
    );
    assert.isDefined(failed);
    assert.isTrue(failed!.surfaced);
    assert.deepStrictEqual(statusesOf(failed), [
      "running",
      "running",
      "running",
      "failed",
      "failed",
      "failed",
    ]);
    const [subagentEvent] = ofType(failed!.events, "subagent.updated");
    assert.isNull(subagentEvent?.subagent.nativeTaskRef);
  });
});

describe("background Task subagent", () => {
  const backgroundInput = { ...AWAITED_INPUT, await: false };
  const launchedInBackground = () => launched(backgroundInput);

  it("[VAL-SUB-005] runs on after the launch acknowledgement and is listed in the roster", () => {
    const running = launchedInBackground();
    assert.isTrue(running.subagent.background);
    const acknowledged = settleDroidTaskResult(
      running.subagent,
      ctx,
      taskResult(`Task launched in background.\ntask_id: ${CHILD}\nsession_id: ${CHILD}`),
      NOT_STOPPED,
      102,
      at(20),
    );
    assert.isUndefined(acknowledged);
    assert.deepStrictEqual(droidSubagentRoster([running.subagent]), [
      { taskId: CHILD, kind: "subagent", description: "dig" },
    ]);
    const [item] = ofType(running.events, "turn_item.updated");
    assert.strictEqual(item?.turnItem.nativeItemRef?.nativeId, CHILD);
  });

  it("lists no awaited subagent and no finished background one", () => {
    const awaited = launched();
    const background = launchedInBackground();
    assert.deepStrictEqual(droidSubagentRoster([awaited.subagent]), []);
    const settled = settleDroidSubagent(
      background.subagent,
      { status: "completed", result: "done" },
      at(30),
    );
    assert.isDefined(settled);
    assert.deepStrictEqual(droidSubagentRoster([awaited.subagent, settled!.subagent]), []);
  });

  it("[VAL-SUB-007] ends failed when the launch itself fails", () => {
    const failed = settleDroidTaskResult(
      launchedInBackground().subagent,
      ctx,
      taskResult("Error: could not launch", true),
      NOT_STOPPED,
      102,
      at(20),
    );
    assert.strictEqual(failed?.subagent.status, "failed");
    assert.deepStrictEqual(droidSubagentRoster([failed!.subagent]), []);
  });

  it("settles on the frozen ids of the turn that launched it, whatever turn is running later", () => {
    const running = launchedInBackground();
    const settled = settleDroidSubagent(
      running.subagent,
      { status: "completed", result: "done" },
      at(60_000),
    );
    const [started] = ofType(running.events, "subagent.updated");
    const [ended] = ofType(settled!.events, "subagent.updated");
    assert.strictEqual(ended?.subagent.id, started?.subagent.id);
    assert.strictEqual(ended?.subagent.runId, started?.subagent.runId);
    assert.strictEqual(ended?.subagent.startedAt, started?.subagent.startedAt);
    assert.strictEqual(ended?.subagent.result, "done");
    const [node] = ofType(settled!.events, "node.updated");
    assert.strictEqual(node?.node.providerTurnId, providerTurn.id);
  });
});
