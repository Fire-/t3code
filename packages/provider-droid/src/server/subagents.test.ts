import { assert, describe, it } from "@effect/vitest";
import {
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  NodeId,
  ProjectId,
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
import { droidTranscriptEntries, type DroidTranscriptRecord } from "./sessionTranscripts.ts";
import {
  type DroidSubagent,
  type DroidSubagentParent,
  type DroidSubagentUpdate,
  droidChildThreadHistory,
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

const modelSelection = { instanceId: providerThread.providerInstanceId, model: "glm-5" };

const parentThread: OrchestrationV2AppThread = {
  createdBy: "user",
  creationSource: "web",
  id: ctx.threadId,
  projectId: ProjectId.make("project-1"),
  title: "Parent thread",
  providerInstanceId: providerThread.providerInstanceId,
  modelSelection,
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: providerThread.id,
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: ctx.threadId },
  forkedFrom: null,
  createdAt: T0,
  updatedAt: T0,
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  lastVisitedAt: null,
  deletedAt: null,
};

const PARENT: DroidSubagentParent = { appThread: parentThread, modelSelection, number: 1 };

const TOOL_USE_ID = "chatcmpl-tool-00000000000000000000000000000001";
const CHILD = "00000000-0000-4000-8000-0000000000c1";
const CHILD_THREAD = derive.threadFromProviderThread({ driver: DROID, nativeThreadId: CHILD });

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
  const update = launchDroidSubagent(called, ctx, childSession(), 101, at(10), PARENT);
  assert.isDefined(update);
  return update!;
};

const RESUME_TOOL_USE_ID = "chatcmpl-tool-00000000000000000000000000000002";

/** The parent continues the same child session with `resume`, under a new Task call. */
const resumed = () => {
  const input = { ...AWAITED_INPUT, description: "dig more", prompt: "Go on.", resume: CHILD };
  const called = noteCall(
    noteCall(undefined, taskCall({}, RESUME_TOOL_USE_ID)),
    taskCall(input, RESUME_TOOL_USE_ID),
  );
  const update = launchDroidSubagent(
    called,
    ctx,
    childSession(CHILD, RESUME_TOOL_USE_ID),
    103,
    at(40),
    PARENT,
  );
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

const subagentItemOf = (events: ReadonlyArray<ProviderAdapterV2Event>) =>
  ofType(events, "turn_item.updated").find((event) => event.turnItem.type === "subagent");

const onChildThread = (events: ReadonlyArray<ProviderAdapterV2Event>) =>
  events.filter(
    (event) =>
      (event.type === "node.updated" && event.node.threadId === CHILD_THREAD) ||
      (event.type === "message.updated" && event.message.threadId === CHILD_THREAD) ||
      (event.type === "turn_item.updated" && event.turnItem.threadId === CHILD_THREAD),
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
    assert.strictEqual(subagent.childThreadId, CHILD_THREAD);
    assert.strictEqual(subagent.runId, ctx.runId);
    assert.deepStrictEqual(subagent.nativeTaskRef, {
      driver: DROID,
      nativeId: CHILD,
      strength: "strong",
    });
    assert.isNull(subagent.completedAt);

    const item = subagentItemOf(update.events);
    assert.strictEqual(item?.turnItem.threadId, ctx.threadId);
    assert.strictEqual(item?.turnItem.status, "running");
    assert.strictEqual(item?.turnItem.ordinal, 101);
    assert.deepStrictEqual(item?.turnItem.nativeItemRef, subagent.nativeTaskRef);
    if (item?.turnItem.type === "subagent") {
      assert.strictEqual(item.turnItem.subagentId, subagent.id);
      assert.strictEqual(item.turnItem.origin, "provider_native");
      assert.strictEqual(item.turnItem.childThreadId, CHILD_THREAD);
    }
    assert.deepStrictEqual(
      ofType(update.events, "node.updated").map((event) => [event.node.kind, event.node.status]),
      [
        ["root_turn", "running"],
        ["subagent", "running"],
      ],
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
    const item = subagentItemOf(done!.events);
    if (item?.turnItem.type !== "subagent") return assert.fail("expected a subagent item");
    assert.strictEqual(item.turnItem.result, "The report.");
    assert.strictEqual(item.turnItem.status, "completed");
    const started = subagentItemOf(running.events);
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
    assert.deepStrictEqual(statusesOf(failed), ["failed", "failed", "failed", "failed"]);
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
      assert.isUndefined(launchDroidSubagent(settled, ctx, childSession(), 103, at(30), PARENT));
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
    assert.isUndefined(
      launchDroidSubagent(running.subagent, ctx, childSession(), 102, at(11), PARENT),
    );
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
    assert.isNull(subagentEvent?.subagent.childThreadId);
    assert.deepStrictEqual(ofType(failed!.events, "app_thread.created"), []);
  });
});

describe("Droid subagent child thread", () => {
  it("opens a read-only child thread holding the Task prompt", () => {
    const { events } = launched();
    const [created] = ofType(events, "app_thread.created");
    const [subagentEvent] = ofType(events, "subagent.updated");
    assert.isDefined(created);
    const child = created!.appThread;
    assert.strictEqual(child.id, CHILD_THREAD);
    assert.strictEqual(child.title, "dig");
    assert.strictEqual(child.creationSource, "provider");
    assert.isNull(child.activeProviderThreadId);
    assert.deepStrictEqual(child.lineage, {
      parentThreadId: parentThread.id,
      relationshipToParent: "subagent",
      rootThreadId: parentThread.id,
    });
    assert.deepStrictEqual(child.forkedFrom, { type: "node", nodeId: subagentEvent!.subagent.id });
    assert.deepStrictEqual(child.modelSelection, modelSelection);

    const [prompt] = ofType(events, "message.updated");
    assert.strictEqual(prompt?.message.threadId, CHILD_THREAD);
    assert.strictEqual(prompt?.message.role, "user");
    assert.strictEqual(prompt?.message.text, "Dig in.");
    const promptItem = ofType(events, "turn_item.updated").find(
      (event) => event.turnItem.type === "user_message",
    );
    assert.strictEqual(promptItem?.turnItem.threadId, CHILD_THREAD);
    assert.strictEqual(promptItem?.turnItem.ordinal, 100);
    if (promptItem?.turnItem.type === "user_message") {
      assert.strictEqual(promptItem.turnItem.senderThreadId, parentThread.id);
    }
    assert.strictEqual(
      events.findIndex((event) => event.type === "app_thread.created"),
      0,
      "the child thread exists before anything lands on it",
    );
  });

  it("ends the child thread's root with the subagent", () => {
    const done = settleDroidTaskResult(
      launched().subagent,
      ctx,
      taskResult(FRAMED_REPORT),
      NOT_STOPPED,
      102,
      at(20),
    );
    const roots = ofType(onChildThread(done!.events), "node.updated");
    assert.deepStrictEqual(
      roots.map((event) => [event.node.kind, event.node.status]),
      [["root_turn", "completed"]],
    );
    assert.isNotNull(roots[0]?.node.completedAt);
    assert.deepStrictEqual(ofType(done!.events, "app_thread.created"), []);
    assert.isBelow(
      done!.events.findIndex(
        (event) => event.type === "node.updated" && event.node.kind === "root_turn",
      ),
      done!.events.findIndex((event) => event.type === "subagent.updated"),
      "a background run stops listening once the subagent ends",
    );
  });

  it("keeps a resumed child's thread and first prompt, and adds the follow-up", () => {
    const first = launched();
    const again = resumed();
    const [created] = ofType(again.events, "app_thread.created");
    assert.isTrue(created?.createOnly, "a resume never renames the child thread");
    const prompts = [first, again].map((update) =>
      ofType(update.events, "message.updated").map((event) => event.message),
    );
    assert.deepStrictEqual(
      prompts.map((messages) => messages.map((message) => message.text)),
      [["Dig in."], ["Go on."]],
    );
    assert.notStrictEqual(prompts[0]![0]!.id, prompts[1]![0]!.id);
  });
});

describe("droidChildThreadHistory", () => {
  const stamp = (second: number) => `2027-01-15T08:00:0${second}.000Z`;
  const childRecords: ReadonlyArray<DroidTranscriptRecord> = [
    { type: "session_start", id: CHILD },
    {
      type: "message",
      id: "m1",
      timestamp: stamp(1),
      message: { role: "user", content: [{ type: "text", text: "Dig in." }] },
    },
    {
      type: "message",
      id: "m2",
      timestamp: stamp(2),
      message: {
        role: "assistant",
        content: [
          { type: "thinking", thinking: "Start with a.ts." },
          { type: "tool_use", id: "tool-1", name: "Read", input: { file_path: "/repo/a.ts" } },
        ],
      },
    },
    {
      type: "message",
      id: "m3",
      timestamp: stamp(3),
      message: {
        role: "user",
        content: [{ type: "tool_result", tool_use_id: "tool-1", content: "const a = 1;" }],
      },
    },
    {
      type: "message",
      id: "m4",
      timestamp: stamp(4),
      message: { role: "assistant", content: [{ type: "text", text: "Found the bug in a.ts." }] },
    },
  ];

  const settled = () =>
    settleDroidTaskResult(
      launched().subagent,
      ctx,
      taskResult(FRAMED_REPORT),
      NOT_STOPPED,
      102,
      at(20),
    )!.subagent;

  const itemsOf = (events: ReadonlyArray<ProviderAdapterV2Event>) =>
    ofType(events, "turn_item.updated").map((event) => [
      event.turnItem.type,
      event.turnItem.ordinal,
    ]);

  it("replays the child session after its prompt onto the child thread", () => {
    const events = droidChildThreadHistory(
      settled(),
      droidTranscriptEntries(childRecords),
      derive,
      at(30),
    );
    assert.strictEqual(onChildThread(events).length, events.length);
    assert.deepStrictEqual(itemsOf(events), [
      ["reasoning", 101],
      ["dynamic_tool", 102],
      ["assistant_message", 103],
    ]);
    assert.deepStrictEqual(
      ofType(events, "message.updated").map((event) => [event.message.role, event.message.text]),
      [["assistant", "Found the bug in a.ts."]],
    );
    const [reasoning] = ofType(events, "turn_item.updated");
    assert.strictEqual(DateTime.formatIso(reasoning!.turnItem.startedAt!), stamp(2));
  });

  it("shows the report when the transcript holds no reply", () => {
    for (const entries of [[], droidTranscriptEntries(childRecords.slice(0, 2))]) {
      const events = droidChildThreadHistory(settled(), entries, derive, at(30));
      assert.deepStrictEqual(
        ofType(events, "message.updated").map((event) => [event.message.role, event.message.text]),
        [["assistant", "The report."]],
      );
    }
  });

  it("leaves out every prompt, since each launch showed its own", () => {
    const resumedRecords: ReadonlyArray<DroidTranscriptRecord> = [
      ...childRecords,
      {
        type: "message",
        id: "m5",
        timestamp: stamp(5),
        message: {
          role: "user",
          content: [{ type: "text", text: "# Follow-up Instructions\n\nGo on." }],
        },
      },
      {
        type: "message",
        id: "m6",
        timestamp: stamp(6),
        message: { role: "assistant", content: [{ type: "text", text: "Went on." }] },
      },
    ];
    const events = droidChildThreadHistory(
      settled(),
      droidTranscriptEntries(resumedRecords),
      derive,
      at(30),
    );
    assert.deepStrictEqual(
      ofType(events, "message.updated").map((event) => [event.message.role, event.message.text]),
      [
        ["assistant", "Found the bug in a.ts."],
        ["assistant", "Went on."],
      ],
    );
  });

  it("gives each launch's stand-in report its own message", () => {
    const resumedSettled = settleDroidTaskResult(
      resumed().subagent,
      ctx,
      taskResult(FRAMED_REPORT),
      NOT_STOPPED,
      104,
      at(50),
    )!.subagent;
    const [first, again] = [settled(), resumedSettled].map((subagent) =>
      ofType(droidChildThreadHistory(subagent, [], derive, at(60)), "message.updated").map(
        (event) => event.message.id,
      ),
    );
    assert.lengthOf(first!, 1);
    assert.notDeepEqual(first, again);
  });

  it("keeps the ids of a replay, so reading the transcript again updates the same items", () => {
    const entries = droidTranscriptEntries(childRecords);
    const ids = (events: ReadonlyArray<ProviderAdapterV2Event>) =>
      ofType(events, "turn_item.updated").map((event) => event.turnItem.id);
    assert.deepStrictEqual(
      ids(droidChildThreadHistory(settled(), entries, derive, at(30))),
      ids(droidChildThreadHistory(settled(), entries, derive, at(40))),
    );
  });

  it("has nothing for a Task that never started a child session", () => {
    const pending = noteCall(noteCall(undefined, taskCall({})), taskCall(AWAITED_INPUT));
    const failed = settleDroidTaskResult(
      pending,
      ctx,
      taskResult("Error: could not start", true),
      NOT_STOPPED,
      101,
      at(5),
    )!.subagent;
    assert.deepStrictEqual(
      droidChildThreadHistory(failed, droidTranscriptEntries(childRecords), derive, at(30)),
      [],
    );
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
    assert.strictEqual(subagentItemOf(running.events)?.turnItem.nativeItemRef?.nativeId, CHILD);
  });

  it("runs in the background when Droid launched a Task called without `await` there", () => {
    const running = launched();
    const acknowledged = settleDroidTaskResult(
      running.subagent,
      ctx,
      taskResult(`Task launched in background.\ntask_id: ${CHILD}\nsession_id: ${CHILD}`),
      NOT_STOPPED,
      102,
      at(20),
    );
    assert.deepStrictEqual(statusesOf(acknowledged), []);
    assert.strictEqual(acknowledged?.subagent.status, "running");
    assert.deepStrictEqual(droidSubagentRoster([acknowledged!.subagent]), [
      { taskId: CHILD, kind: "subagent", description: "dig" },
    ]);
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
    const node = ofType(settled!.events, "node.updated").find(
      (event) => event.node.kind === "subagent",
    );
    assert.strictEqual(node?.node.providerTurnId, providerTurn.id);
  });
});
