import { assert, describe, it } from "@effect/vitest";
import {
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  NodeId,
  PlanId,
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

import {
  DROID_ZERO_COUNTS,
  type DroidEventContext,
  type DroidMapperState,
  createDroidTurnState,
  droidCompactionEvents,
  droidTurnTokenUsage,
  flushDroidStreams,
  mapDroidNotification,
  mapDroidProposedPlan,
  settleDroidTurn,
} from "./events.ts";

const DROID = ProviderDriverKind.make("droid");
const T0 = DateTime.makeUnsafe(1_800_000_000_000);
const at = (offsetMs: number) => DateTime.makeUnsafe(1_800_000_000_000 + offsetMs);

const providerThread = (
  overrides: Partial<OrchestrationV2ProviderThread> = {},
): OrchestrationV2ProviderThread => ({
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
  ...overrides,
});

const providerTurn = (id: string, ordinal = 1): OrchestrationV2ProviderTurn => ({
  id: ProviderTurnId.make(id),
  providerThreadId: ProviderThreadId.make("provider-thread-1"),
  nodeId: NodeId.make("root-node-1"),
  runAttemptId: null,
  nativeTurnRef: { driver: DROID, nativeId: "user-message-1", strength: "strong" },
  ordinal,
  status: "running",
  startedAt: T0,
  completedAt: null,
});

const context = (overrides: Partial<DroidEventContext> = {}): DroidEventContext => ({
  threadId: ThreadId.make("thread-1"),
  runId: RunId.make("run-1"),
  rootNodeId: NodeId.make("root-node-1"),
  providerThread: providerThread(),
  providerTurn: providerTurn("provider-turn-1"),
  derive,
  ...overrides,
});

type Notification = { readonly type: string } & Readonly<Record<string, unknown>>;

/** Feeds notifications through the mapper, returning the final state and each notification's events. */
const feed = (
  notifications: ReadonlyArray<Notification>,
  options: {
    readonly ctx?: DroidEventContext;
    readonly state?: DroidMapperState;
    readonly clockStepMs?: number;
  } = {},
) => {
  const ctx = options.ctx ?? context();
  let state = options.state ?? createDroidTurnState({ firstItemOrdinal: 101 });
  const perNotification: Array<ReadonlyArray<ProviderAdapterV2Event>> = [];
  notifications.forEach((notification, index) => {
    const result = mapDroidNotification(
      state,
      ctx,
      notification,
      at(index * (options.clockStepMs ?? 1)),
    );
    state = result.state;
    perNotification.push(result.events);
  });
  return { state, perNotification, events: perNotification.flat() };
};

const ofType = <Type extends ProviderAdapterV2Event["type"]>(
  events: ReadonlyArray<ProviderAdapterV2Event>,
  type: Type,
) =>
  events.filter(
    (event): event is Extract<ProviderAdapterV2Event, { readonly type: Type }> =>
      event.type === type,
  );

const turnItems = (events: ReadonlyArray<ProviderAdapterV2Event>) =>
  ofType(events, "turn_item.updated").map((event) => event.turnItem);

const MESSAGE_ID = "00000000-0000-4000-8000-0000000000a1";
const TOOL_ID = "chatcmpl-tool-0000000000000000000000000000000a";
const TOOL_ID_2 = "chatcmpl-tool-0000000000000000000000000000000b";

const textDelta = (textDelta: string, blockIndex = 0, messageId = MESSAGE_ID): Notification => ({
  type: "assistant_text_delta",
  messageId,
  blockIndex,
  textDelta,
});
const textComplete = (blockIndex = 0, messageId = MESSAGE_ID): Notification => ({
  type: "assistant_text_complete",
  messageId,
  blockIndex,
});
const thinkingDelta = (
  textDelta: string,
  blockIndex = 0,
  messageId = MESSAGE_ID,
): Notification => ({
  type: "thinking_text_delta",
  messageId,
  blockIndex,
  textDelta,
});
const thinkingComplete = (blockIndex = 0, messageId = MESSAGE_ID): Notification => ({
  type: "thinking_text_complete",
  messageId,
  blockIndex,
  durationMs: 66,
});
const toolCall = (name: string, input: unknown, id = TOOL_ID): Notification => ({
  type: "tool_call",
  toolUse: { type: "tool_use", id, name, input },
});
const toolResult = (content: unknown, isError = false, id = TOOL_ID): Notification => ({
  type: "tool_result",
  toolUseId: id,
  messageId: "00000000-0000-4000-8000-0000000000b1",
  content,
  isError,
});
const progress = (
  update: Readonly<Record<string, unknown>>,
  toolName = "Execute",
  id = TOOL_ID,
): Notification => ({
  type: "tool_progress_update",
  toolUseId: id,
  toolName,
  update: { type: "status", timestamp: 1_800_000_000_000, terminalId: "terminal-1", ...update },
});

describe("assistant text and reasoning [VAL-TURN-007]", () => {
  it("[VAL-TURN-007] streams deltas into one assistant_message item and finalizes it with the full text", () => {
    const { perNotification } = feed([textDelta("Hel"), textDelta("lo"), textComplete()]);
    const [first, second, completed] = perNotification;

    assert.deepStrictEqual(
      first?.map((event) => event.type),
      ["node.updated", "turn_item.updated", "message.updated"],
    );
    const streaming = turnItems(first ?? [])[0];
    assert.strictEqual(streaming?.type, "assistant_message");
    if (streaming?.type !== "assistant_message") return;
    assert.strictEqual(streaming.text, "Hel");
    assert.strictEqual(streaming.streaming, true);
    assert.strictEqual(streaming.status, "running");
    assert.strictEqual(streaming.completedAt, null);
    const streamingMessage = ofType(first ?? [], "message.updated")[0]?.message;
    assert.strictEqual(streamingMessage?.role, "assistant");
    assert.strictEqual(streamingMessage?.text, "Hel");
    assert.strictEqual(streamingMessage?.streaming, true);
    assert.strictEqual(streamingMessage?.id, streaming.messageId);

    assert.deepStrictEqual(
      second?.map((event) => event.type),
      ["turn_item.updated", "message.updated"],
      "only the first surfaced delta announces the node",
    );
    const grown = turnItems(second ?? [])[0];
    assert.strictEqual(grown?.type === "assistant_message" ? grown.text : undefined, "Hello");

    const final = turnItems(completed ?? [])[0];
    assert.strictEqual(final?.type, "assistant_message");
    if (final?.type !== "assistant_message") return;
    assert.strictEqual(final.text, "Hello");
    assert.strictEqual(final.streaming, false);
    assert.strictEqual(final.status, "completed");
    assert.notStrictEqual(final.completedAt, null);
    const finalMessage = ofType(completed ?? [], "message.updated")[0]?.message;
    assert.strictEqual(finalMessage?.text, "Hello");
    assert.strictEqual(finalMessage?.streaming, false);
    const node = ofType(completed ?? [], "node.updated")[0]?.node;
    assert.strictEqual(node?.kind, "assistant_message");
    assert.strictEqual(node?.status, "completed");
    assert.strictEqual(final.id, streaming.id);
    assert.strictEqual(final.messageId, streaming.messageId);
  });

  it("[VAL-TURN-007] attributes items and messages to the run, root node, provider thread and provider turn", () => {
    const ctx = context();
    const { events } = feed([textDelta("Hi"), textComplete()], { ctx });
    const item = turnItems(events)[0];
    assert.strictEqual(item?.threadId, ctx.threadId);
    assert.strictEqual(item?.runId, ctx.runId);
    assert.strictEqual(item?.providerThreadId, ctx.providerThread.id);
    assert.strictEqual(item?.providerTurnId, ctx.providerTurn.id);
    assert.strictEqual(item?.parentItemId, null);
    const node = ofType(events, "node.updated")[0]?.node;
    assert.strictEqual(node?.parentNodeId, ctx.rootNodeId);
    assert.strictEqual(node?.rootNodeId, ctx.rootNodeId);
    assert.strictEqual(node?.countsForRun, false);
    assert.strictEqual(node?.id, item?.nodeId);
    const message = ofType(events, "message.updated")[0]?.message;
    assert.strictEqual(message?.createdBy, "agent");
    assert.strictEqual(message?.creationSource, "provider");
    for (const event of events) assert.strictEqual("driver" in event ? event.driver : DROID, DROID);
  });

  it("[VAL-TURN-007] assigns increasing item ordinals starting at the turn's first ordinal", () => {
    const { events } = feed([
      thinkingDelta("hm", 0),
      textDelta("Hi", 1),
      toolCall("Execute", { command: "ls" }),
    ]);
    assert.deepStrictEqual(
      turnItems(events).map((item) => item.ordinal),
      [101, 102, 103],
    );
  });

  it("[VAL-TURN-007] does not create a visible assistant item for a whitespace-only text block", () => {
    const { events, state } = feed([textDelta("\n\n"), textComplete()]);
    assert.deepStrictEqual(events, []);

    const afterRecord = feed(
      [
        {
          type: "create_message",
          message: {
            id: MESSAGE_ID,
            role: "assistant",
            content: [
              { type: "thinking", thinking: "plan", signature: "reasoning_content" },
              { type: "text", text: "\n\n" },
              { type: "tool_use", id: TOOL_ID, name: "Create", input: {} },
            ],
          },
        },
      ],
      { state },
    );
    assert.deepStrictEqual(afterRecord.events, []);
  });

  it("[VAL-TURN-007] surfaces a block that starts with whitespace once real text arrives, with the full text", () => {
    const { perNotification } = feed([textDelta("\n\n"), textDelta("Done"), textComplete()]);
    assert.deepStrictEqual(perNotification[0], []);
    const streaming = turnItems(perNotification[1] ?? [])[0];
    assert.strictEqual(streaming?.type === "assistant_message" ? streaming.text : "", "\n\nDone");
    const final = turnItems(perNotification[2] ?? [])[0];
    assert.strictEqual(final?.type === "assistant_message" ? final.streaming : true, false);
  });

  it("[VAL-TURN-007] maps thinking deltas and their completion to a reasoning item without a message", () => {
    const { perNotification } = feed([
      thinkingDelta("Simple"),
      thinkingDelta(" task."),
      thinkingComplete(),
    ]);
    const streaming = turnItems(perNotification[1] ?? [])[0];
    assert.strictEqual(streaming?.type, "reasoning");
    if (streaming?.type !== "reasoning") return;
    assert.strictEqual(streaming.text, "Simple task.");
    assert.strictEqual(streaming.streaming, true);

    const final = turnItems(perNotification[2] ?? [])[0];
    assert.strictEqual(final?.type, "reasoning");
    if (final?.type !== "reasoning") return;
    assert.strictEqual(final.text, "Simple task.");
    assert.strictEqual(final.streaming, false);
    assert.strictEqual(final.status, "completed");
    assert.strictEqual(final.id, streaming.id);
    assert.strictEqual(ofType(perNotification.flat(), "message.updated").length, 0);
    assert.strictEqual(ofType(perNotification[0] ?? [], "node.updated")[0]?.node.kind, "reasoning");
  });

  it("[VAL-TURN-007] keeps a reasoning block and a text block of one message as separate items", () => {
    const { events } = feed([
      thinkingDelta("hmm", 0),
      thinkingComplete(0),
      textDelta("Answer", 1),
      textComplete(1),
    ]);
    const types = [...new Set(turnItems(events).map((item) => item.type))];
    assert.deepStrictEqual(types, ["reasoning", "assistant_message"]);
    assert.strictEqual(new Set(turnItems(events).map((item) => item.id)).size, 2);
  });

  it("[VAL-TURN-007] completes a streamed block when the committed assistant message arrives without a completion", () => {
    const { perNotification } = feed([
      textDelta("Created it."),
      {
        type: "create_message",
        message: {
          id: MESSAGE_ID,
          role: "assistant",
          modelId: "model-a",
          content: [{ type: "text", text: "Created it." }],
        },
      },
    ]);
    const final = turnItems(perNotification[1] ?? []);
    assert.strictEqual(final.length, 1);
    assert.strictEqual(final[0]?.status, "completed");
    assert.strictEqual(final[0]?.type === "assistant_message" ? final[0].streaming : true, false);
  });

  it("[VAL-TURN-007] creates a completed item from a committed text block that never streamed, once", () => {
    const record: Notification = {
      type: "create_message",
      message: {
        id: MESSAGE_ID,
        role: "assistant",
        content: [{ type: "text", text: "  Ready.  " }],
      },
    };
    const { perNotification, state } = feed([record]);
    const items = turnItems(perNotification[0] ?? []);
    assert.strictEqual(items.length, 1);
    assert.strictEqual(items[0]?.type === "assistant_message" ? items[0].text : "", "Ready.");
    assert.strictEqual(items[0]?.status, "completed");

    const replay = feed([record, textDelta("late"), textComplete()], { state });
    assert.deepStrictEqual(replay.perNotification[0], []);
    assert.deepStrictEqual(replay.perNotification[1], [], "a completed block ignores stray deltas");
  });

  it("[VAL-TURN-007] ignores user, system and llm_only committed messages", () => {
    const { events } = feed([
      {
        type: "create_message",
        message: { id: "context-1", role: "user", content: [], visibility: "llm_only" },
      },
      {
        type: "create_message",
        message: { id: "user-1", role: "user", content: [{ type: "text", text: "hello" }] },
      },
      {
        type: "create_message",
        message: {
          id: "hidden-1",
          role: "assistant",
          visibility: "llm_only",
          content: [{ type: "text", text: "internal" }],
        },
      },
    ]);
    assert.deepStrictEqual(events, []);
  });

  it("[VAL-TURN-007] records the model the assistant message actually ran on", () => {
    const { state } = feed([
      {
        type: "create_message",
        message: {
          id: MESSAGE_ID,
          role: "assistant",
          modelId: "model-b",
          apiProvider: "provider-x",
          reasoningEffort: "low",
          content: [{ type: "text", text: "Hi" }],
        },
      },
    ]);
    assert.deepStrictEqual(state.assistantModel, {
      modelId: "model-b",
      apiProvider: "provider-x",
      reasoningEffort: "low",
    });
  });

  it("[VAL-TURN-007] closes a retracted message's open block and starts the re-streamed message on a new item", () => {
    const { perNotification } = feed([
      textDelta("Partial"),
      { type: "assistant_message_retracted", messageId: MESSAGE_ID },
      textDelta("Fresh"),
      textComplete(),
    ]);
    const closed = turnItems(perNotification[1] ?? [])[0];
    assert.strictEqual(closed?.status, "completed");
    assert.strictEqual(closed?.type === "assistant_message" ? closed.text : "", "Partial");
    const first = turnItems(perNotification[0] ?? [])[0];
    const restarted = turnItems(perNotification[2] ?? [])[0];
    assert.strictEqual(restarted?.type === "assistant_message" ? restarted.text : "", "Fresh");
    assert.notStrictEqual(restarted?.id, first?.id);
    const final = turnItems(perNotification[3] ?? [])[0];
    assert.strictEqual(final?.id, restarted?.id);
    assert.strictEqual(final?.status, "completed");
  });

  it("[VAL-TURN-007] settling a turn finalizes streaming items that never got a completion", () => {
    const ctx = context();
    const streamed = feed([thinkingDelta("hm"), textDelta("Partial answer")], { ctx });
    const settled = settleDroidTurn(streamed.state, ctx, "interrupted", at(50));
    const items = turnItems(settled.events);
    assert.deepStrictEqual(items.map((item) => item.type).toSorted(), [
      "assistant_message",
      "reasoning",
    ]);
    for (const item of items) {
      assert.strictEqual(item.status, "completed");
      if (item.type === "assistant_message" || item.type === "reasoning") {
        assert.strictEqual(item.streaming, false);
      }
    }
    const message = ofType(settled.events, "message.updated")[0]?.message;
    assert.strictEqual(message?.text, "Partial answer");
    assert.strictEqual(message?.streaming, false);
    assert.deepStrictEqual(settleDroidTurn(settled.state, ctx, "interrupted", at(60)).events, []);
  });

  it("[VAL-TURN-007] throttles streaming updates when streamFlushMs is set and flushes the latest text", () => {
    const ctx = context({ streamFlushMs: 50 });
    const first = mapDroidNotification(
      createDroidTurnState({ firstItemOrdinal: 101 }),
      ctx,
      textDelta("a"),
      at(0),
    );
    assert.strictEqual(turnItems(first.events).length, 1);
    const second = mapDroidNotification(first.state, ctx, textDelta("b"), at(10));
    const third = mapDroidNotification(second.state, ctx, textDelta("c"), at(20));
    assert.deepStrictEqual(second.events, []);
    assert.deepStrictEqual(third.events, []);

    const flushed = flushDroidStreams(third.state, ctx, at(60));
    const items = turnItems(flushed.events);
    assert.strictEqual(items.length, 1);
    assert.strictEqual(items[0]?.type === "assistant_message" ? items[0].text : "", "abc");
    assert.strictEqual(items[0]?.type === "assistant_message" ? items[0].streaming : false, true);
    assert.deepStrictEqual(flushDroidStreams(flushed.state, ctx, at(70)).events, []);

    const beyondWindow = mapDroidNotification(flushed.state, ctx, textDelta("d"), at(200));
    assert.strictEqual(turnItems(beyondWindow.events).length, 1);

    const completed = mapDroidNotification(
      mapDroidNotification(beyondWindow.state, ctx, textDelta("e"), at(205)).state,
      ctx,
      textComplete(),
      at(206),
    );
    const final = turnItems(completed.events)[0];
    assert.strictEqual(final?.type === "assistant_message" ? final.text : "", "abcde");
    assert.strictEqual(final?.status, "completed");
  });
});

describe("tool calls and results [VAL-TURN-008]", () => {
  it("[VAL-TURN-008] maps the doubled Execute tool_call to one command_execution item updated in place", () => {
    const { perNotification, events } = feed([
      toolCall("Execute", {}),
      toolCall("Execute", { command: "git status --short", riskLevel: "low" }),
      toolResult("M README.md\n\n\n[Process exited with code 0]"),
    ]);
    const items = turnItems(events);
    assert.strictEqual(items.length, 3);
    assert.strictEqual(new Set(items.map((item) => item.id)).size, 1);
    assert.strictEqual(
      ofType(events, "node.updated").length,
      2,
      "one node for start, one at completion",
    );

    const [started, updated, completed] = items;
    assert.strictEqual(started?.type, "command_execution");
    assert.strictEqual(started?.type === "command_execution" ? started.input : null, "");
    assert.strictEqual(started?.status, "running");
    assert.strictEqual(
      updated?.type === "command_execution" ? updated.input : null,
      "git status --short",
    );
    assert.strictEqual(updated?.status, "running");
    assert.strictEqual(completed?.type, "command_execution");
    if (completed?.type !== "command_execution") return;
    assert.strictEqual(completed.input, "git status --short");
    assert.strictEqual(completed.status, "completed");
    assert.strictEqual(completed.output, "M README.md");
    assert.strictEqual(completed.exitCode, 0);
    assert.notStrictEqual(completed.completedAt, null);
    assert.strictEqual(completed.title, "Execute");
    assert.deepStrictEqual(
      perNotification.map((batch) => batch.map((event) => event.type)),
      [
        ["node.updated", "turn_item.updated"],
        ["turn_item.updated"],
        ["node.updated", "turn_item.updated"],
      ],
    );
    const completedNode = ofType(perNotification[2] ?? [], "node.updated")[0]?.node;
    assert.strictEqual(completedNode?.kind, "tool_call");
    assert.strictEqual(completedNode?.status, "completed");
  });

  it("[VAL-TURN-008] reports a non-zero exit code from the output footer without failing the item", () => {
    const { events } = feed([
      toolCall("Execute", { command: "false" }),
      toolResult("boom\n\n\n[Process exited with code 3]"),
    ]);
    const final = turnItems(events).at(-1);
    assert.strictEqual(final?.type === "command_execution" ? final.exitCode : undefined, 3);
    assert.strictEqual(final?.type === "command_execution" ? final.output : undefined, "boom");
    assert.strictEqual(final?.status, "completed");
  });

  it("[VAL-TURN-008] maps Create to file_change once the file path is known, without an item for the empty first frame", () => {
    const { perNotification, events } = feed([
      toolCall("Create", {}),
      toolCall("Create", { file_path: "/tmp/project/out.txt", content: "hello\n" }),
      toolResult('{"success":true,"file_path":"/tmp/project/out.txt"}'),
    ]);
    assert.deepStrictEqual(perNotification[0], []);
    const items = turnItems(events);
    assert.strictEqual(new Set(items.map((item) => item.id)).size, 1);
    const [running, completed] = items;
    assert.strictEqual(running?.type, "file_change");
    assert.strictEqual(running?.status, "running");
    assert.strictEqual(completed?.type, "file_change");
    if (completed?.type !== "file_change") return;
    assert.strictEqual(completed.fileName, "/tmp/project/out.txt");
    assert.strictEqual(completed.newStr, "hello\n");
    assert.strictEqual(completed.status, "completed");
  });

  it("[VAL-TURN-008] maps Edit to file_change with old/new text and line counts from the result diff", () => {
    const { events } = feed([
      toolCall("Edit", { file_path: "/tmp/project/a.ts", old_str: "one", new_str: "two" }),
      toolResult(
        JSON.stringify({
          success: true,
          file_path: "/tmp/project/a.ts",
          diffLines: [
            { type: "unchanged", content: "keep", lineNumber: { old: 1, new: 1 } },
            { type: "removed", content: "one", lineNumber: { old: 2 } },
            { type: "added", content: "two", lineNumber: { new: 2 } },
            { type: "added", content: "three", lineNumber: { new: 3 } },
          ],
        }),
      ),
    ]);
    const final = turnItems(events).at(-1);
    assert.strictEqual(final?.type, "file_change");
    if (final?.type !== "file_change") return;
    assert.strictEqual(final.fileName, "/tmp/project/a.ts");
    assert.strictEqual(final.oldStr, "one");
    assert.strictEqual(final.newStr, "two");
    assert.strictEqual(final.additions, 2);
    assert.strictEqual(final.deletions, 1);
  });

  it("[VAL-TURN-008] maps MultiEdit with a path to file_change and ApplyPatch without one to dynamic_tool", () => {
    const multi = feed([
      toolCall("MultiEdit", { file_path: "/tmp/project/b.ts", edits: [] }),
      toolResult("ok"),
    ]);
    assert.strictEqual(turnItems(multi.events).at(-1)?.type, "file_change");

    const patch = feed([
      toolCall("ApplyPatch", {}),
      toolCall("ApplyPatch", { input: "*** Begin Patch\n*** End Patch" }),
      toolResult("patched"),
    ]);
    const patchItems = turnItems(patch.events);
    assert.deepStrictEqual(
      patchItems.map((item) => [item.type, item.status]),
      [
        ["dynamic_tool", "running"],
        ["dynamic_tool", "completed"],
      ],
      "the empty first frame waits; the item type is fixed once the final input is known",
    );
    assert.strictEqual(new Set(patchItems.map((item) => item.id)).size, 1);
  });

  it("[VAL-TURN-008] gives a file tool whose result arrives before any input a dynamic_tool item", () => {
    const { events } = feed([toolCall("Edit", {}), toolResult("Error: no input", true)]);
    const items = turnItems(events);
    assert.deepStrictEqual(
      items.map((item) => [item.type, item.status]),
      [["dynamic_tool", "failed"]],
    );
  });

  it("[VAL-TURN-008] maps every other tool to dynamic_tool with its name, latest input and output", () => {
    for (const name of ["Read", "LS", "Grep", "Glob", "WebSearch", "FetchUrl", "TodoWrite"]) {
      const { events } = feed([
        toolCall(name, {}),
        toolCall(name, { file_path: "/tmp/project/a.ts" }),
        toolResult([{ type: "text", text: "contents" }]),
      ]);
      const items = turnItems(events);
      assert.strictEqual(items.length, 3, name);
      assert.strictEqual(new Set(items.map((item) => item.id)).size, 1, name);
      const final = items.at(-1);
      assert.strictEqual(final?.type, "dynamic_tool", name);
      if (final?.type !== "dynamic_tool") continue;
      assert.strictEqual(final.toolName, name);
      assert.deepStrictEqual(final.input, { file_path: "/tmp/project/a.ts" });
      assert.strictEqual(final.output, "contents");
      assert.strictEqual(final.status, "completed");
    }
  });

  it("names an MCP tool the way clients recognize T3 tools and integrations", () => {
    const final = (name: string) => {
      const { events } = feed([toolCall(name, { taskId: "task-1" }), toolResult("done")]);
      const item = turnItems(events).at(-1);
      assert.strictEqual(item?.type, "dynamic_tool", name);
      return item?.type === "dynamic_tool" ? item : undefined;
    };

    const t3 = final("t3-code___task_status");
    assert.strictEqual(t3?.toolName, "mcp__t3-code__task_status");
    assert.strictEqual(t3?.title, "mcp__t3-code__task_status");
    assert.isUndefined(t3?.toolSource, "the client renders T3 tools itself");

    const other = final("mini___slow_count");
    assert.strictEqual(other?.toolName, "mcp__mini__slow_count");
    assert.strictEqual(other?.title, "slow count");
    assert.deepStrictEqual(other?.toolSource, {
      key: "mcp:mini",
      name: "mini",
      kind: "integration",
    });
  });

  it("[VAL-TURN-008] shows a Script call as JavaScript titled by the tools it calls", () => {
    const script = [
      'const page = await tools.FetchUrl({ url: "https://example.com" });',
      'await tools.Execute({ command: "ls" });',
      'text(await tools.FetchUrl({ url: "https://example.org" }));',
    ].join("\n");
    const { events } = feed([
      toolCall("Script", {}),
      toolCall("Script", { script }),
      toolResult("ok\n[Script completed · 0.4 s elapsed · 3 calls · log: /tmp/x.log]"),
    ]);
    const items = turnItems(events);
    assert.strictEqual(new Set(items.map((item) => item.id)).size, 1);
    assert.deepStrictEqual(
      items.map((item) =>
        item.type === "dynamic_tool" ? [item.toolName, item.title, item.input] : [item.type],
      ),
      [
        ["js", "Script", {}],
        ["js", "Script: FetchUrl, Execute", { code: script }],
        ["js", "Script: FetchUrl, Execute", { code: script }],
      ],
    );
    const final = items.at(-1);
    assert.strictEqual(
      final?.type === "dynamic_tool" ? final.output : undefined,
      "ok\n[Script completed · 0.4 s elapsed · 3 calls · log: /tmp/x.log]",
    );
  });

  it("[VAL-SUB-001] leaves a Task call and its result to the subagent projection", () => {
    const { events } = feed([
      toolCall("Task", {}),
      toolCall("Task", { prompt: "dig", description: "dig", subagent_type: "worker" }),
      toolResult("session_id: child-1\nreport"),
    ]);
    assert.deepStrictEqual(turnItems(events), []);
    assert.deepStrictEqual(
      events.filter((event) => event.type === "node.updated"),
      [],
    );
  });

  it("[VAL-TURN-008] fails the item and keeps the error text when the tool_result has isError", () => {
    const { events } = feed([
      toolCall("Edit", { file_path: "/tmp/project/a.ts", old_str: "x", new_str: "y" }),
      toolResult("Error: Failed to edit file", true),
      toolCall("Read", { file_path: "/tmp/project/missing" }, TOOL_ID_2),
      toolResult("Error: file not found", true, TOOL_ID_2),
    ]);
    const finals = turnItems(events).filter((item) => item.status !== "running");
    assert.deepStrictEqual(
      finals.map((item) => [item.type, item.status]),
      [
        ["file_change", "failed"],
        ["dynamic_tool", "failed"],
      ],
    );
    const edit = finals[0];
    assert.strictEqual(
      edit?.type === "file_change" ? edit.diffStr : undefined,
      "Error: Failed to edit file",
    );
    const dynamic = finals[1];
    assert.strictEqual(
      dynamic?.type === "dynamic_tool" ? dynamic.output : undefined,
      "Error: file not found",
    );
    const failedNode = ofType(events, "node.updated")
      .map((event) => event.node)
      .filter((node) => node.status === "failed");
    assert.strictEqual(failedNode.length, 2);
  });

  describe("an error tool_result after Stop", () => {
    // Droid answers a cancelled tool with an error result, twice in a live run.
    const cancelled = "Tool execution cancelled by user";
    const running = [toolCall("Execute", { command: "sleep 60" })];
    const stopped = (state: DroidMapperState): DroidMapperState => ({
      ...state,
      interruptRequested: true,
    });

    it("[VAL-TURN-020] settles the item and node interrupted when an interrupt was requested", () => {
      const open = feed(running);
      const { events } = feed([toolResult(cancelled, true)], { state: stopped(open.state) });
      const settled = turnItems(events).at(-1);
      assert.strictEqual(settled?.type, "command_execution");
      assert.strictEqual(settled?.status, "interrupted");
      assert.deepStrictEqual(
        ofType(events, "node.updated").map((event) => event.node.status),
        ["interrupted"],
      );
    });

    it("[VAL-TURN-020] keeps a genuine tool error failed when no interrupt was requested", () => {
      const open = feed(running);
      const { events } = feed([toolResult("Error: command not found", true)], {
        state: open.state,
      });
      assert.strictEqual(turnItems(events).at(-1)?.status, "failed");
    });

    it("[VAL-TURN-020] still completes a successful result that races the interrupt", () => {
      const open = feed(running);
      const { events } = feed([toolResult("done")], { state: stopped(open.state) });
      assert.strictEqual(turnItems(events).at(-1)?.status, "completed");
    });

    it("[VAL-TURN-020] ignores the duplicate second error result", () => {
      const open = feed(running);
      const first = feed([toolResult(cancelled, true)], { state: stopped(open.state) });
      assert.strictEqual(turnItems(first.events).at(-1)?.status, "interrupted");
      const second = feed([toolResult(cancelled, true)], { state: first.state });
      assert.deepStrictEqual(second.events, []);
    });
  });

  it("[VAL-REQ-006] settles a tool whose permission was declined cancelled, not failed", () => {
    const open = feed([toolCall("Execute", { command: "rm -rf build" })]);
    const declined = { ...open.state, declinedToolUseIds: new Set([TOOL_ID]) };
    const { events } = feed([toolResult("Error: Tool execution declined by user", true)], {
      state: declined,
    });
    assert.strictEqual(turnItems(events).at(-1)?.status, "cancelled");
    assert.deepStrictEqual(
      ofType(events, "node.updated").map((event) => event.node.status),
      ["cancelled"],
    );
  });

  it("[VAL-TURN-008] keeps two concurrent tools on separate items", () => {
    const { events } = feed([
      toolCall("Read", { file_path: "/tmp/a" }, TOOL_ID),
      toolCall("Read", { file_path: "/tmp/b" }, TOOL_ID_2),
      toolResult("a", false, TOOL_ID),
      toolResult("b", false, TOOL_ID_2),
    ]);
    const items = turnItems(events);
    assert.strictEqual(new Set(items.map((item) => item.id)).size, 2);
    assert.deepStrictEqual(
      items.filter((item) => item.status === "completed").map((item) => item.ordinal),
      [101, 102],
    );
  });

  it("[VAL-TURN-008] ignores repeats after completion, results for unknown tools, and phase changes", () => {
    const { state, events } = feed([
      toolCall("Read", { file_path: "/tmp/a" }),
      {
        type: "tool_execution_phase_changed",
        toolUseId: TOOL_ID,
        toolName: "Read",
        phase: "executing",
      },
      toolResult("a"),
    ]);
    assert.strictEqual(turnItems(events).length, 2);
    const after = feed(
      [
        toolCall("Read", { file_path: "/tmp/a" }),
        toolResult("again"),
        toolResult("orphan", false, TOOL_ID_2),
      ],
      { state },
    );
    assert.deepStrictEqual(after.events, []);
  });

  it("[VAL-TURN-008] settling a turn closes tools that never produced a result", () => {
    const ctx = context();
    const open = feed(
      [
        toolCall("Execute", { command: "sleep 60" }),
        toolCall("Read", { file_path: "/tmp/a" }, TOOL_ID_2),
      ],
      { ctx },
    );
    const outcomes = {
      interrupted: "interrupted",
      cancelled: "cancelled",
      failed: "failed",
      completed: "failed",
    } as const;
    for (const [outcome, expected] of Object.entries(outcomes)) {
      const settled = settleDroidTurn(open.state, ctx, outcome as keyof typeof outcomes, at(100));
      const items = turnItems(settled.events);
      assert.deepStrictEqual(
        items.map((item) => item.status),
        [expected, expected],
        outcome,
      );
      assert.deepStrictEqual(
        ofType(settled.events, "node.updated").map((event) => event.node.status),
        [expected, expected],
        outcome,
      );
      for (const item of items) assert.notStrictEqual(item.completedAt, null);
      assert.deepStrictEqual(settleDroidTurn(settled.state, ctx, "failed", at(110)).events, []);
    }
  });
});

describe("command output deltas [VAL-TURN-009]", () => {
  const started = () =>
    feed([toolCall("Execute", {}), toolCall("Execute", { command: "make test" })]);

  it("[VAL-TURN-009] emits command_output.delta for the command item without persisting it", () => {
    const base = started();
    const commandItemId = turnItems(base.events)[0]?.id;
    assert.ok(commandItemId !== undefined);
    const ctx = context();
    const { perNotification } = feed(
      [
        progress({ text: "" }),
        progress({ text: "line 1", fullOutput: "line 1\n" }),
        progress({ text: "line 2", fullOutput: "line 1\nline 2\n" }),
        progress({ text: "", fullOutput: "line 1\nline 2\n", exitCode: 0, signal: null }),
      ],
      { ctx, state: base.state },
    );
    const all = perNotification.flat();
    assert.deepStrictEqual(
      all.map((event) => event.type),
      ["command_output.delta", "command_output.delta"],
      "no turn_item.updated or node.updated for progress frames",
    );
    const deltas = ofType(all, "command_output.delta");
    assert.deepStrictEqual(
      deltas.map((delta) => delta.chunk),
      ["line 1\n", "line 2\n"],
    );
    for (const delta of deltas) {
      assert.strictEqual(delta.itemId, commandItemId);
      assert.strictEqual(delta.threadId, ctx.threadId);
      assert.strictEqual(delta.driver, DROID);
    }
    assert.deepStrictEqual(perNotification[0], [], "the start frame carries no output");
    assert.deepStrictEqual(perNotification[3], [], "the end frame repeats output already sent");
  });

  it("[VAL-TURN-009] delivers an end-only fullOutput as one chunk and keeps the exit code for the final item", () => {
    const { events, state } = feed(
      [progress({ text: "", fullOutput: "ok\n", exitCode: 0, signal: null })],
      { state: started().state },
    );
    assert.deepStrictEqual(
      ofType(events, "command_output.delta").map((delta) => delta.chunk),
      ["ok\n"],
    );
    const result = feed([toolResult("ok")], { state });
    const final = turnItems(result.events).at(-1);
    assert.strictEqual(final?.type === "command_execution" ? final.output : undefined, "ok");
    assert.strictEqual(final?.type === "command_execution" ? final.exitCode : undefined, 0);
  });

  it("[VAL-TURN-009] falls back to the frame's text when no fullOutput is present", () => {
    const { events } = feed([progress({ text: "tick" })], { state: started().state });
    assert.deepStrictEqual(
      ofType(events, "command_output.delta").map((delta) => delta.chunk),
      ["tick"],
    );
  });

  it("[VAL-TURN-009] never re-sends output when fullOutput stops extending what was already sent", () => {
    const { perNotification } = feed(
      [
        progress({ text: "", fullOutput: "abcdef" }),
        progress({ text: "", fullOutput: "…cdefgh" }),
        progress({ text: "", fullOutput: "…cdefghij" }),
      ],
      { state: started().state },
    );
    assert.deepStrictEqual(
      perNotification.map((batch) => ofType(batch, "command_output.delta").map((d) => d.chunk)),
      [["abcdef"], [], ["ij"]],
    );
  });

  it("[VAL-TURN-009] ignores progress for other tools, unknown tools and completed commands", () => {
    const base = started();
    const mission = feed(
      [
        progress({ text: "x", fullOutput: "x" }, "StartMissionRun", TOOL_ID_2),
        progress({ text: "x", fullOutput: "x" }, "Execute", TOOL_ID_2),
      ],
      { state: base.state },
    );
    assert.deepStrictEqual(mission.events, []);

    const done = feed([toolResult("done")], { state: base.state });
    const late = feed([progress({ text: "late", fullOutput: "late" })], { state: done.state });
    assert.deepStrictEqual(late.events, []);

    const nonCommand = feed([toolCall("Read", { file_path: "/tmp/a" }, TOOL_ID_2)]);
    const read = feed([progress({ text: "r", fullOutput: "r" }, "Read", TOOL_ID_2)], {
      state: nonCommand.state,
    });
    assert.deepStrictEqual(read.events, []);
  });
});

describe("item ids [VAL-TURN-010]", () => {
  const stream: ReadonlyArray<Notification> = [
    thinkingDelta("hm", 0),
    thinkingDelta("m", 0),
    thinkingComplete(0),
    textDelta("Hel", 1),
    textDelta("lo", 1),
    textComplete(1),
    toolCall("Execute", {}),
    toolCall("Execute", { command: "ls" }),
    toolResult("a\n\n\n[Process exited with code 0]"),
  ];

  const identities = (events: ReadonlyArray<ProviderAdapterV2Event>) => {
    const byKind = new Map<string, Set<string>>();
    const add = (kind: string, id: string) =>
      byKind.set(kind, (byKind.get(kind) ?? new Set()).add(id));
    for (const item of turnItems(events)) {
      add(`item:${item.type}`, item.id);
      add(`node:${item.type}`, String(item.nodeId));
      if (item.type === "assistant_message") add("message", item.messageId);
    }
    return byKind;
  };

  it("[VAL-TURN-010] keeps one id per text segment, reasoning block and tool across all updates", () => {
    const { events } = feed(stream);
    const ids = identities(events);
    assert.deepStrictEqual(
      [...ids.keys()].toSorted(),
      [
        "item:assistant_message",
        "item:command_execution",
        "item:reasoning",
        "message",
        "node:assistant_message",
        "node:command_execution",
        "node:reasoning",
      ].toSorted(),
    );
    for (const [kind, set] of ids) assert.strictEqual(set.size, 1, kind);
    for (const message of ofType(events, "message.updated")) {
      assert.ok([...(ids.get("message") ?? [])].includes(message.message.id));
    }
    for (const node of ofType(events, "node.updated")) {
      assert.ok(
        [...ids.values()].some((set) => set.has(String(node.node.id))),
        "node ids match the item node ids",
      );
    }
  });

  it("[VAL-TURN-010] includes the provider turn id in every native item ref", () => {
    const ctx = context({ providerTurn: providerTurn("provider-turn-xyz") });
    const { events } = feed(stream, { ctx });
    const refs = [
      ...turnItems(events).map((item) => item.nativeItemRef),
      ...ofType(events, "node.updated").map((event) => event.node.nativeItemRef),
    ];
    assert.ok(refs.length > 0);
    for (const ref of refs) {
      assert.strictEqual(ref?.driver, DROID);
      const nativeId = ref?.nativeId ?? "";
      assert.ok(nativeId.includes("provider-turn-xyz"), nativeId);
    }
  });

  it("[VAL-TURN-010] gives distinct item, node and message ids to two provider turns fed identical native ids", () => {
    const first = feed(stream, {
      ctx: context({ providerTurn: providerTurn("provider-turn-1", 1) }),
    });
    const second = feed(stream, {
      ctx: context({
        providerThread: providerThread({ id: ProviderThreadId.make("provider-thread-2") }),
        providerTurn: providerTurn("provider-turn-2", 1),
      }),
    });
    const collect = (events: ReadonlyArray<ProviderAdapterV2Event>) =>
      new Set([
        ...turnItems(events).map((item) => item.id),
        ...ofType(events, "node.updated").map((event) => String(event.node.id)),
        ...ofType(events, "message.updated").map((event) => event.message.id),
      ]);
    const firstIds = collect(first.events);
    const secondIds = collect(second.events);
    assert.ok(firstIds.size > 0);
    assert.strictEqual([...firstIds].filter((id) => secondIds.has(id)).length, 0);
  });

  it("[VAL-TURN-010] derives the same ids when the same notifications arrive again at other times", () => {
    const ids = (clockStepMs: number) =>
      turnItems(feed(stream, { clockStepMs }).events).map((item) => item.id);
    assert.deepStrictEqual(ids(1), ids(500));
  });
});

describe("context usage [VAL-TURN-011]", () => {
  const usageNotification = (
    lastCall: Readonly<Record<string, number>>,
    totals: Readonly<Record<string, number>> = {
      inputTokens: 13_343,
      outputTokens: 125,
      cacheCreationTokens: 0,
      cacheReadTokens: 1_280,
      thinkingTokens: 0,
      factoryCredits: 1_665,
    },
  ): Notification => ({
    type: "session_token_usage_changed",
    sessionId: "droid-session-1",
    tokenUsage: totals,
    inclusiveTokenUsage: totals,
    lastCallTokenUsage: lastCall,
  });

  it("[VAL-TURN-011] sets contextUsage from a usage notification without touching the provider thread identity", () => {
    const ctx = context({ contextWindow: 200_000 });
    const { events } = feed(
      [usageNotification({ inputTokens: 13_343, outputTokens: 125, cacheReadTokens: 1_280 })],
      { ctx },
    );
    const updates = ofType(events, "provider_thread.updated");
    assert.strictEqual(updates.length, 1);
    const thread = updates[0]?.providerThread;
    assert.strictEqual(thread?.id, ctx.providerThread.id);
    assert.deepStrictEqual(thread?.nativeThreadRef, ctx.providerThread.nativeThreadRef);
    assert.strictEqual(thread?.contextUsage?.usedTokens, 13_343 + 1_280 + 125);
    assert.deepStrictEqual(thread?.contextUsage, {
      usedTokens: 14_748,
      maxTokens: 200_000,
      lastUsedTokens: 14_748,
      lastInputTokens: 14_623,
      lastOutputTokens: 125,
      lastCachedInputTokens: 1_280,
      totalProcessedTokens: 14_748,
      inputTokens: 14_623,
      outputTokens: 125,
      cachedInputTokens: 1_280,
    });
    assert.deepStrictEqual(
      events.map((event) => event.type),
      ["provider_thread.updated"],
    );
  });

  it("[VAL-TURN-011] lets the latest report win and keeps totals in step", () => {
    const { events } = feed([
      usageNotification({ inputTokens: 13_343, outputTokens: 125, cacheReadTokens: 1_280 }),
      usageNotification(
        { inputTokens: 361, outputTokens: 8, cacheReadTokens: 0 },
        {
          inputTokens: 13_704,
          outputTokens: 133,
          cacheCreationTokens: 0,
          cacheReadTokens: 1_280,
          thinkingTokens: 20,
        },
      ),
    ]);
    const usages = ofType(events, "provider_thread.updated").map(
      (event) => event.providerThread.contextUsage,
    );
    assert.strictEqual(usages.length, 2);
    assert.strictEqual(usages[1]?.usedTokens, 369);
    assert.strictEqual(usages[1]?.lastUsedTokens, 369);
    assert.strictEqual(usages[1]?.totalProcessedTokens, 13_704 + 1_280 + 133);
    assert.strictEqual(usages[1]?.reasoningOutputTokens, 20);
    assert.strictEqual(usages[1]?.lastCachedInputTokens, undefined);
  });

  it("[VAL-TURN-011] reports no usage for a notification without a model call to measure", () => {
    const { events } = feed([
      usageNotification({ inputTokens: 0, outputTokens: 0, cacheReadTokens: 0 }),
      { type: "session_token_usage_changed", sessionId: "droid-session-1" },
    ]);
    assert.deepStrictEqual(events, []);
  });

  it("[VAL-TURN-011] reads usage carried on agent_turn_completed when no per-call report was seen", () => {
    const ctx = context({ contextWindow: 200_000 });
    const { events, state } = feed(
      [
        {
          type: "agent_turn_completed",
          reason: "completed",
          turnId: "user-message-1",
          tokenUsage: {
            inputTokens: 900,
            outputTokens: 100,
            cacheCreationTokens: 0,
            cacheReadTokens: 300,
            thinkingTokens: 0,
          },
          cumulativeTokenUsage: {
            inputTokens: 900,
            outputTokens: 100,
            cacheCreationTokens: 0,
            cacheReadTokens: 300,
            thinkingTokens: 0,
          },
          childTokenUsage: {
            inputTokens: 0,
            outputTokens: 0,
            cacheCreationTokens: 0,
            cacheReadTokens: 0,
            thinkingTokens: 0,
          },
        },
      ],
      { ctx },
    );
    const thread = ofType(events, "provider_thread.updated")[0]?.providerThread;
    assert.strictEqual(thread?.id, ctx.providerThread.id);
    assert.deepStrictEqual(thread?.nativeThreadRef, ctx.providerThread.nativeThreadRef);
    assert.strictEqual(thread?.contextUsage?.usedTokens, 1_300);
    assert.strictEqual(thread?.contextUsage?.maxTokens, 200_000);
    assert.strictEqual(thread?.contextUsage?.totalProcessedTokens, 1_300);
    assert.strictEqual(state.turnTokenUsage?.usageStatus, "complete");
    assert.strictEqual(
      events.filter((event) => event.type === "turn.terminal").length,
      0,
      "terminal events belong to the adapter",
    );
  });

  it("[VAL-TURN-011] refreshes totals on completion but keeps the last call's context size", () => {
    const { events } = feed([
      usageNotification(
        { inputTokens: 500, outputTokens: 50, cacheReadTokens: 0 },
        {
          inputTokens: 500,
          outputTokens: 50,
          cacheCreationTokens: 0,
          cacheReadTokens: 0,
          thinkingTokens: 0,
        },
      ),
      {
        type: "agent_turn_completed",
        reason: "completed",
        turnId: "user-message-1",
        tokenUsage: {
          inputTokens: 700,
          outputTokens: 90,
          cacheCreationTokens: 0,
          cacheReadTokens: 0,
          thinkingTokens: 0,
        },
        cumulativeTokenUsage: {
          inputTokens: 700,
          outputTokens: 90,
          cacheCreationTokens: 0,
          cacheReadTokens: 0,
          thinkingTokens: 0,
        },
      },
    ]);
    const usages = ofType(events, "provider_thread.updated").map(
      (event) => event.providerThread.contextUsage,
    );
    assert.strictEqual(usages.length, 2);
    assert.strictEqual(usages[1]?.usedTokens, 550);
    assert.strictEqual(usages[1]?.totalProcessedTokens, 790);
  });

  it("[VAL-TURN-011] resumes from the thread's existing usage when seeded with it", () => {
    const seeded = createDroidTurnState({
      firstItemOrdinal: 101,
      contextUsage: { usedTokens: 4_000, totalProcessedTokens: 9_000, maxTokens: 200_000 },
    });
    const { events } = feed(
      [
        {
          type: "agent_turn_completed",
          reason: "cancelled",
          turnId: "user-message-1",
          tokenUsage: {
            inputTokens: 0,
            outputTokens: 0,
            cacheCreationTokens: 0,
            cacheReadTokens: 0,
            thinkingTokens: 0,
          },
        },
      ],
      { state: seeded },
    );
    assert.deepStrictEqual(events, [], "nothing new to report");
  });
});

describe("old follow-up 7: zero-usage first turn [VAL-TURN-012]", () => {
  const ZERO = {
    inputTokens: 0,
    outputTokens: 0,
    cacheCreationTokens: 0,
    cacheReadTokens: 0,
    thinkingTokens: 0,
  };
  const completed = (reason: string, extra: Record<string, unknown> = {}): Notification => ({
    type: "agent_turn_completed",
    reason,
    turnId: "user-message-1",
    tokenUsage: ZERO,
    cumulativeTokenUsage: ZERO,
    childTokenUsage: ZERO,
    ...extra,
  });

  it("[VAL-TURN-012] old follow-up 7: a zero-usage first turn of a fresh session reports exact zero, not unavailable", () => {
    for (const reason of ["cancelled", "process_exit", "completed"]) {
      const fresh = createDroidTurnState({
        firstItemOrdinal: 101,
        sessionUsage: DROID_ZERO_COUNTS,
      });
      const ctx = context({ contextWindow: 200_000 });
      const { events, state } = feed([completed(reason)], { ctx, state: fresh });
      assert.deepStrictEqual(
        state.turnTokenUsage,
        {
          usageScope: "main_agent",
          usageStatus: "complete",
          inputTokens: 0,
          outputTokens: 0,
          hasSubagents: false,
        },
        reason,
      );
      const thread = ofType(events, "provider_thread.updated")[0]?.providerThread;
      assert.deepStrictEqual(
        thread?.contextUsage,
        { usedTokens: 0, maxTokens: 200_000 },
        `${reason}: the exact count (zero) is reported instead of null`,
      );
    }
  });

  it("[VAL-TURN-012] old follow-up 7: process_exit after real calls reports the session's growth, not the zero terminator", () => {
    const fresh = createDroidTurnState({ firstItemOrdinal: 101, sessionUsage: DROID_ZERO_COUNTS });
    const { state } = feed(
      [
        {
          type: "session_token_usage_changed",
          sessionId: "droid-session-1",
          tokenUsage: {
            inputTokens: 1_000,
            outputTokens: 80,
            cacheCreationTokens: 0,
            cacheReadTokens: 500,
            thinkingTokens: 10,
          },
          lastCallTokenUsage: { inputTokens: 1_000, outputTokens: 80, cacheReadTokens: 500 },
        },
        completed("process_exit"),
      ],
      { state: fresh },
    );
    assert.deepStrictEqual(state.turnTokenUsage, {
      usageScope: "main_agent",
      usageStatus: "complete",
      inputTokens: 1_500,
      outputTokens: 80,
      cachedInputTokens: 500,
      reasoningTokens: 10,
      hasSubagents: false,
    });
  });

  it("[VAL-TURN-012] old follow-up 7: growth is measured against the session total at turn start on later turns", () => {
    const later = createDroidTurnState({
      firstItemOrdinal: 201,
      sessionUsage: {
        inputTokens: 4_000,
        outputTokens: 300,
        cacheCreationTokens: 0,
        cacheReadTokens: 2_000,
        thinkingTokens: 0,
      },
    });
    const { state } = feed(
      [
        {
          type: "session_token_usage_changed",
          sessionId: "droid-session-1",
          tokenUsage: {
            inputTokens: 4_600,
            outputTokens: 340,
            cacheCreationTokens: 0,
            cacheReadTokens: 2_000,
            thinkingTokens: 0,
          },
          lastCallTokenUsage: { inputTokens: 600, outputTokens: 40, cacheReadTokens: 2_000 },
        },
        completed("process_exit"),
      ],
      { state: later },
    );
    assert.strictEqual(state.turnTokenUsage?.usageStatus, "complete");
    assert.strictEqual(state.turnTokenUsage?.inputTokens, 600 + 0);
    assert.strictEqual(state.turnTokenUsage?.outputTokens, 40);
  });

  it("[VAL-TURN-012] old follow-up 7: only an unknown session baseline stays unavailable", () => {
    const resumed = createDroidTurnState({ firstItemOrdinal: 101 });
    const { events, state } = feed([completed("cancelled")], { state: resumed });
    assert.deepStrictEqual(state.turnTokenUsage, {
      usageScope: "main_agent",
      usageStatus: "unavailable",
      hasSubagents: false,
    });
    assert.deepStrictEqual(events, [], "an unknown count is not reported as zero");
  });

  it("[VAL-TURN-012] subtracts child usage from a reported turn total and flags subagents", () => {
    const usage = droidTurnTokenUsage(createDroidTurnState({ firstItemOrdinal: 1 }), {
      type: "agent_turn_completed",
      reason: "completed",
      tokenUsage: {
        inputTokens: 1_000,
        outputTokens: 200,
        cacheCreationTokens: 0,
        cacheReadTokens: 400,
        thinkingTokens: 30,
      },
      childTokenUsage: {
        inputTokens: 300,
        outputTokens: 50,
        cacheCreationTokens: 0,
        cacheReadTokens: 100,
        thinkingTokens: 5,
      },
    });
    assert.deepStrictEqual(usage, {
      usageScope: "main_agent",
      usageStatus: "complete",
      inputTokens: 700 + 300,
      outputTokens: 150,
      cachedInputTokens: 300,
      reasoningTokens: 25,
      hasSubagents: true,
    });
  });

  it("[VAL-TURN-012] marks usage partial when subtracting children clamps a field", () => {
    const usage = droidTurnTokenUsage(createDroidTurnState({ firstItemOrdinal: 1 }), {
      type: "agent_turn_completed",
      reason: "completed",
      tokenUsage: {
        inputTokens: 10,
        outputTokens: 5,
        cacheCreationTokens: 0,
        cacheReadTokens: 0,
        thinkingTokens: 0,
      },
      childTokenUsage: {
        inputTokens: 20,
        outputTokens: 1,
        cacheCreationTokens: 0,
        cacheReadTokens: 0,
        thinkingTokens: 0,
      },
    });
    assert.strictEqual(usage.usageStatus, "partial");
    assert.strictEqual(usage.hasSubagents, true);
  });

  describe("a t3 turn that folded in a steer sees one completion per native turn", () => {
    const counts = (inputTokens: number, outputTokens: number, cacheReadTokens = 0) => ({
      inputTokens,
      outputTokens,
      cacheCreationTokens: 0,
      cacheReadTokens,
      thinkingTokens: 0,
    });
    const nativeCompletion = (
      turnId: string,
      tokenUsage: ReturnType<typeof counts>,
      cumulativeTokenUsage: ReturnType<typeof counts>,
    ): Notification => ({
      type: "agent_turn_completed",
      reason: "completed",
      turnId,
      tokenUsage,
      cumulativeTokenUsage,
      childTokenUsage: ZERO,
    });

    it("[VAL-REQ-002] sums the usage of every native turn", () => {
      const fresh = createDroidTurnState({ firstItemOrdinal: 1, sessionUsage: DROID_ZERO_COUNTS });
      const { state } = feed(
        [
          nativeCompletion("user-message-1", counts(1_000, 80, 500), counts(1_000, 80, 500)),
          nativeCompletion("steer-message-1", counts(300, 20), counts(1_300, 100, 500)),
        ],
        { state: fresh },
      );
      assert.deepStrictEqual(state.turnTokenUsage, {
        usageScope: "main_agent",
        usageStatus: "complete",
        inputTokens: 1_500 + 300,
        outputTokens: 100,
        cachedInputTokens: 500,
        hasSubagents: false,
      });
    });

    it("[VAL-REQ-002] measures a follow-on native turn that reports nothing from the previous completion", () => {
      const fresh = createDroidTurnState({ firstItemOrdinal: 1, sessionUsage: DROID_ZERO_COUNTS });
      const { state } = feed(
        [
          nativeCompletion("user-message-1", counts(1_000, 80), counts(1_000, 80)),
          {
            type: "session_token_usage_changed",
            sessionId: "droid-session-1",
            tokenUsage: counts(1_400, 130),
            lastCallTokenUsage: counts(400, 50),
          },
          {
            type: "agent_turn_completed",
            reason: "completed",
            turnId: "steer-message-1",
            tokenUsage: ZERO,
            cumulativeTokenUsage: counts(1_400, 130),
            childTokenUsage: ZERO,
          },
        ],
        { state: fresh },
      );
      assert.strictEqual(state.turnTokenUsage?.usageStatus, "complete");
      assert.strictEqual(state.turnTokenUsage?.inputTokens, 1_000 + 400);
      assert.strictEqual(state.turnTokenUsage?.outputTokens, 80 + 50);
    });

    it("[VAL-REQ-002] keeps the sum partial when one native turn's usage is unknown", () => {
      const { state } = feed([
        {
          type: "agent_turn_completed",
          reason: "completed",
          turnId: "user-message-1",
          tokenUsage: counts(1_000, 80),
          childTokenUsage: ZERO,
        },
        {
          type: "agent_turn_completed",
          reason: "completed",
          turnId: "steer-message-1",
          tokenUsage: ZERO,
          childTokenUsage: ZERO,
        },
      ]);
      assert.strictEqual(state.turnTokenUsage?.usageStatus, "partial");
      assert.strictEqual(state.turnTokenUsage?.inputTokens, 1_000);
    });
  });

  it("[VAL-TURN-012] a turn that ends without any completion notification still gets exact usage when the baseline is known", () => {
    const fresh = createDroidTurnState({ firstItemOrdinal: 1, sessionUsage: DROID_ZERO_COUNTS });
    assert.strictEqual(droidTurnTokenUsage(fresh, undefined).usageStatus, "complete");
    assert.strictEqual(
      droidTurnTokenUsage(createDroidTurnState({ firstItemOrdinal: 1 }), undefined).usageStatus,
      "unavailable",
    );
  });
});

describe("notifications the content mapper does not own", () => {
  it("drops unknown notification types without events or an error, and reports the type for logging", () => {
    const ctx = context();
    const state = createDroidTurnState({ firstItemOrdinal: 101 });
    const result = mapDroidNotification(
      state,
      ctx,
      { type: "mission_progress_update", payload: { anything: true } },
      T0,
    );
    assert.deepStrictEqual(result.events, []);
    assert.strictEqual(result.unknownType, "mission_progress_update");
    assert.strictEqual(result.state, state);
  });

  it("leaves lifecycle notifications to the adapter without reporting them as unknown", () => {
    const state = createDroidTurnState({ firstItemOrdinal: 101 });
    for (const type of [
      "settings_updated",
      "droid_working_state_changed",
      "session_title_updated",
      "mcp_status_changed",
      "error",
      "llm_retry",
      "permission_resolved",
      "child_session_available",
      "session_compacted",
    ]) {
      const result = mapDroidNotification(state, context(), { type }, T0);
      assert.deepStrictEqual(result.events, [], type);
      assert.strictEqual(result.unknownType, undefined, type);
    }
  });

  it("drops malformed content notifications instead of throwing", () => {
    const { events } = feed([
      { type: "assistant_text_delta" },
      { type: "assistant_text_delta", messageId: MESSAGE_ID, blockIndex: "0", textDelta: "x" },
      { type: "thinking_text_delta", messageId: MESSAGE_ID, blockIndex: 0 },
      { type: "tool_call" },
      { type: "tool_call", toolUse: { id: "", name: "Read" } },
      { type: "tool_result" },
      { type: "tool_progress_update", update: "nope" },
      { type: "create_message" },
      { type: "assistant_message_retracted" },
    ]);
    assert.deepStrictEqual(events, []);
  });
});

describe("mapDroidProposedPlan", () => {
  const PLAN_ID = PlanId.make("plan-1");
  const ctx = context();
  const state = createDroidTurnState({ firstItemOrdinal: 101 });
  const markdown = "# Plan\n\n1. Do it.";

  it("[VAL-PLAN-003] a proposed plan is a completed plan node, an active artifact, and a finished item", () => {
    const result = mapDroidProposedPlan(
      state,
      ctx,
      { planId: PLAN_ID, sourceId: "tool-9", markdown, previous: undefined },
      at(5),
    );
    assert.deepStrictEqual(
      result.events.map((event) => event.type),
      ["node.updated", "plan.updated", "turn_item.updated"],
    );
    const nativeItemId = "provider-turn-1:plan:tool-9";
    const nodeId = derive.nodeFromProviderItem({ driver: DROID, nativeItemId });
    const [node, planEvent, itemEvent] = result.events;
    assert.strictEqual(node?.type === "node.updated" && node.node.id, nodeId);
    assert.strictEqual(node?.type === "node.updated" && node.node.kind, "plan");
    assert.strictEqual(node?.type === "node.updated" && node.node.status, "completed");
    assert.deepStrictEqual(planEvent?.type === "plan.updated" && planEvent.plan, {
      id: PLAN_ID,
      threadId: ThreadId.make("thread-1"),
      runId: RunId.make("run-1"),
      nodeId,
      kind: "proposed_plan",
      status: "active",
      markdown,
    });
    const item = itemEvent?.type === "turn_item.updated" ? itemEvent.turnItem : undefined;
    assert.strictEqual(item?.type, "proposed_plan");
    if (item?.type !== "proposed_plan") return;
    assert.strictEqual(item.planId, PLAN_ID);
    assert.strictEqual(item.markdown, markdown);
    assert.strictEqual(item.streaming, false);
    assert.strictEqual(item.status, "completed");
    assert.strictEqual(item.ordinal, 101);
    assert.strictEqual(item.nodeId, nodeId);
    assert.strictEqual(item.nativeItemRef?.nativeId, nativeItemId);
    assert.strictEqual(result.state.nextItemOrdinal, 102);
    assert.strictEqual(result.plan.id, PLAN_ID);
  });

  it("[VAL-PLAN-003] the previous proposal is superseded before the new one is published", () => {
    const first = mapDroidProposedPlan(
      state,
      ctx,
      { planId: PLAN_ID, sourceId: "tool-1", markdown: "# First", previous: undefined },
      at(1),
    );
    const second = mapDroidProposedPlan(
      first.state,
      ctx,
      {
        planId: PlanId.make("plan-2"),
        sourceId: "tool-2",
        markdown: "# Second",
        previous: first.plan,
      },
      at(2),
    );
    const plans = ofType(second.events, "plan.updated").map((event) => [
      event.plan.id,
      event.plan.status,
    ]);
    assert.deepStrictEqual(plans, [
      [PLAN_ID, "superseded"],
      [PlanId.make("plan-2"), "active"],
    ]);
    assert.strictEqual(second.state.nextItemOrdinal, 103);
  });
});

describe("model reroute notice [VAL-HIST-014]", () => {
  const REQUESTED = "claude-opus-5-5";
  const answered = (modelId: string | undefined, id = MESSAGE_ID, apiProvider?: string) =>
    ({
      type: "create_message",
      message: {
        id,
        role: "assistant",
        ...(modelId === undefined ? {} : { modelId }),
        ...(apiProvider === undefined ? {} : { apiProvider }),
        content: [{ type: "text", text: `answer ${id}` }],
      },
    }) satisfies Notification;
  const requestedState = () =>
    createDroidTurnState({ firstItemOrdinal: 101, requestedModelId: REQUESTED });
  const notices = (events: ReadonlyArray<ProviderAdapterV2Event>) =>
    turnItems(events).flatMap((item) => (item.type === "system_notice" ? [item] : []));

  it("[VAL-HIST-014] a reply from another model emits one system_notice naming it", () => {
    const { events } = feed([answered("gpt-5-codex", MESSAGE_ID, "openai")], {
      state: requestedState(),
    });
    const [notice, ...others] = notices(events);
    assert.strictEqual(others.length, 0);
    assert.isDefined(notice);
    assert.include(notice?.message, "gpt-5-codex");
    assert.include(notice?.message, REQUESTED);
    assert.include(notice?.message, "openai");
    assert.strictEqual(notice?.status, "completed");
    assert.include(String(notice?.id), "provider-turn-1");
    assert.strictEqual(
      ofType(events, "node.updated").find((event) => event.node.id === notice?.nodeId)?.node.kind,
      "system",
    );
  });

  it("[VAL-HIST-014] later messages from the same rerouted model add no more notices, another model adds one", () => {
    const { perNotification, state } = feed(
      [answered("gpt-5-codex", "m1"), answered("gpt-5-codex", "m2")],
      { state: requestedState() },
    );
    assert.strictEqual(notices(perNotification[0] ?? []).length, 1);
    assert.strictEqual(notices(perNotification[1] ?? []).length, 0);
    assert.strictEqual(
      turnItems(perNotification[1] ?? []).some((item) => item.type === "assistant_message"),
      true,
      "the reply itself still surfaces",
    );

    const next = feed([answered("gemini-3-pro", "m3")], { state });
    assert.strictEqual(notices(next.events).length, 1);
    assert.include(notices(next.events)[0]?.message, "gemini-3-pro");
  });

  it("[VAL-HIST-014] a reply from the requested model, or one that names no model, emits no notice", () => {
    const { events } = feed([answered(REQUESTED, "m1"), answered(undefined, "m2")], {
      state: requestedState(),
    });
    assert.deepStrictEqual(notices(events), []);
  });

  it("[VAL-HIST-014] the router picks its own model, so no request names one and nothing is a reroute", () => {
    const { events } = feed([answered("claude-sonnet-5")], {
      state: createDroidTurnState({ firstItemOrdinal: 101 }),
    });
    assert.deepStrictEqual(notices(events), []);
  });
});

describe("compaction item [VAL-HIST-009]", () => {
  const ctx = context();
  const startedAt = at(0);

  it("[VAL-HIST-009] a running compaction is a compaction item of the droid driver on a system node", () => {
    const events = droidCompactionEvents(
      ctx,
      { ordinal: 101, status: "running", startedAt },
      at(1),
    );
    const [item] = turnItems(events);
    assert.strictEqual(item?.type, "compaction");
    assert.strictEqual(item?.type === "compaction" ? item.driver : undefined, "droid");
    assert.strictEqual(item?.status, "running");
    assert.strictEqual(item?.completedAt, null);
    assert.strictEqual(item?.ordinal, 101);
    assert.include(String(item?.id), "provider-turn-1");
    assert.strictEqual(ofType(events, "node.updated")[0]?.node.kind, "system");
    assert.strictEqual(ofType(events, "node.updated")[0]?.node.status, "running");
  });

  it("[VAL-HIST-009] settling it keeps the id and ordinal and sets the closing status", () => {
    const [running] = turnItems(
      droidCompactionEvents(ctx, { ordinal: 101, status: "running", startedAt }, at(1)),
    );
    for (const status of ["completed", "failed", "cancelled"] as const) {
      const events = droidCompactionEvents(ctx, { ordinal: 101, status, startedAt }, at(2));
      const [item] = turnItems(events);
      assert.strictEqual(item?.id, running?.id);
      assert.strictEqual(item?.ordinal, 101);
      assert.strictEqual(item?.status, status);
      assert.isNotNull(item?.completedAt);
      assert.strictEqual(ofType(events, "node.updated")[0]?.node.status, status);
    }
  });
});
