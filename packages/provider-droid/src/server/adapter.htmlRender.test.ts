import { assert, describe, it } from "@effect/vitest";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import type { HtmlRenderReference } from "@t3tools/shared/htmlRender";
import { htmlRenderFromToolItem } from "@t3tools/shared/toolOutput";
import * as Effect from "effect/Effect";

import {
  beginTurn,
  completed,
  isTerminal,
  layerTest,
  THREAD_ID,
  type Events,
} from "./adapterHarness.testkit.ts";
import { makeFakeDroid } from "./fakeDroid.testkit.ts";

const PAGE_INPUT = { html: "<p>hello</p>", title: "Test page", height: 240 };

const pageReference = (attachmentId: string): HtmlRenderReference => ({
  attachmentId,
  title: PAGE_INPUT.title,
  height: PAGE_INPUT.height,
});

/** What T3's `html_render` does on the server: it publishes the page and keeps its reference. */
const publish = Effect.fnUntraced(function* (reference: HtmlRenderReference) {
  const sessions = yield* McpProviderSessions.McpProviderSessions;
  yield* sessions.recordHtmlRender(THREAD_ID, PAGE_INPUT, reference);
});

const toolCall = (id: string, name: string, input: Record<string, unknown>) => ({
  type: "tool_call",
  toolUse: { type: "tool_use", id, name, input },
});

const toolResult = (id: string, content: string, isError = false) => ({
  type: "tool_result",
  toolUseId: id,
  content,
  isError,
});

/** Droid reports a call made inside a Script only as a committed message with the tool use. */
const scriptCall = (
  scriptId: string,
  id: string,
  name: string,
  input: Record<string, unknown>,
) => ({
  type: "create_message",
  message: {
    id: `message-${id}`,
    role: "assistant",
    visibility: "user_only",
    content: [
      {
        type: "tool_use",
        id,
        name,
        input,
        scriptExecution: { runId: scriptId, outerToolUseId: scriptId },
      },
    ],
  },
});

const SCRIPT = "await tools.t3_code.html_render({ html, title, height });\ntext('posted');";

const finalItems = (events: Events) => {
  const latest = new Map<
    string,
    Extract<Events[number], { type: "turn_item.updated" }>["turnItem"]
  >();
  for (const event of events) {
    if (event.type === "turn_item.updated") latest.set(event.turnItem.id, event.turnItem);
  }
  return [...latest.values()];
};

const shownPages = (events: Events) =>
  finalItems(events).flatMap((item) => {
    if (item.type !== "dynamic_tool" || item.status !== "completed") return [];
    const page = htmlRenderFromToolItem(item);
    return page === undefined ? [] : [{ item, page }];
  });

describe("DroidAdapterV2 html_render pages", () => {
  for (const failed of [false, true]) {
    it.effect(
      `a page published from inside a Script shows on that call's own item under the Script, even when the Script ${failed ? "failed" : "succeeded"}`,
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const { opened, process } = yield* beginTurn(fake, { runtimeMode: "auto" });

          yield* process.emit(toolCall("script-1", "Script", { script: SCRIPT }));
          yield* process.emit(
            scriptCall("script-1", "inner-1", "t3-code___html_render", PAGE_INPUT),
          );
          yield* publish(pageReference("page-1"));
          yield* process.emit(
            toolResult(
              "inner-1",
              JSON.stringify({ htmlRender: pageReference("page-1"), message: "Shown." }),
            ),
          );
          yield* process.emit(
            toolResult("script-1", failed ? "Error: later step threw" : "posted", failed),
          );
          yield* process.emit(completed(1));
          yield* opened.takeEvent(isTerminal);

          const script = finalItems(opened.all).find(
            (item) => item.type === "dynamic_tool" && item.toolName === "js",
          );
          const shown = shownPages(opened.all);
          assert.deepStrictEqual(
            shown.map(({ item, page }) => [item.toolName, page]),
            [["mcp__t3-code__html_render", pageReference("page-1")]],
          );
          assert.strictEqual(shown[0]?.item.parentItemId, script?.id);
          assert.isAbove(shown[0]?.item.ordinal ?? -1, script?.ordinal ?? Infinity);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  }

  it.effect("a direct html_render call shows its page on its own item", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeDroid();
      const { opened, process } = yield* beginTurn(fake, { runtimeMode: "auto" });

      yield* process.emit(toolCall("render-1", "t3-code___html_render", PAGE_INPUT));
      yield* publish(pageReference("page-1"));
      yield* process.emit(
        toolResult(
          "render-1",
          JSON.stringify({ htmlRender: pageReference("page-1"), message: "Shown to the reader." }),
        ),
      );
      yield* process.emit(completed(1));
      yield* opened.takeEvent(isTerminal);

      assert.deepStrictEqual(
        shownPages(opened.all).map(({ item, page }) => [item.toolName, item.parentItemId, page]),
        [["mcp__t3-code__html_render", null, pageReference("page-1")]],
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
