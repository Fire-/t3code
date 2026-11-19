import { EnvironmentId, ThreadId, type AssistantCitation } from "@t3tools/contracts";
import { useState, type ComponentProps } from "react";
import { expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";

import "~/index.css";

import type { AssistantCitationSourceAnchor } from "~/lib/assistantTextSelection";
import { act, render } from "../../../test/render";
import { AssistantSelectionToolbar } from "./AssistantSelectionToolbar";

const threadRef = {
  environmentId: EnvironmentId.make("environment"),
  threadId: ThreadId.make("thread"),
};

function SelectionHarness(props: {
  onCite: (citation: AssistantCitation, sourceAnchor: AssistantCitationSourceAnchor) => boolean;
}) {
  const [viewport, setViewport] = useState<HTMLDivElement | null>(null);
  const [anchor, setAnchor] = useState<HTMLDivElement | null>(null);
  return (
    <>
      <div ref={setViewport} data-assistant-citation-viewport="true">
        <p
          data-assistant-citation-source="source"
          data-assistant-citation-environment="environment"
          data-assistant-citation-thread="thread"
        >
          hello world
        </p>
      </div>
      {/* The dock ChatView floats above the composer; the chip mounts in its anchor. */}
      <div data-composer-dock="true">
        <div ref={setAnchor} data-cite-chip-anchor="true" />
      </div>
      {viewport ? (
        <AssistantSelectionToolbar
          viewport={viewport}
          threadRef={threadRef}
          anchor={anchor}
          onCite={props.onCite}
        />
      ) : null}
    </>
  );
}

/** Android's long-press: contextmenu, then pointercancel, then the selection appears. */
function touchLongPress(target: Element) {
  target.dispatchEvent(
    new PointerEvent("pointerdown", { bubbles: true, pointerType: "touch", isPrimary: true }),
  );
  target.dispatchEvent(new MouseEvent("contextmenu", { bubbles: true }));
  target.dispatchEvent(
    new PointerEvent("pointercancel", { bubbles: true, pointerType: "touch", isPrimary: true }),
  );
}

function selectText(target: Element, start: number, end: number) {
  window.getSelection()?.setBaseAndExtent(target.firstChild!, start, target.firstChild!, end);
  document.dispatchEvent(new Event("selectionchange"));
}

function collapseSelection() {
  window.getSelection()?.removeAllRanges();
  document.dispatchEvent(new Event("selectionchange"));
}

async function renderHarness(onCite: ComponentProps<typeof SelectionHarness>["onCite"]) {
  const screen = await render(<SelectionHarness onCite={onCite} />);
  const text = screen.getByText("hello world").element();
  return {
    screen,
    text,
    [Symbol.asyncDispose]: () => screen[Symbol.asyncDispose](),
  };
}

it("shows a settled touch selection as a chip above the composer", async () => {
  const onCite = vi.fn(() => true);
  await using harness = await renderHarness(onCite);
  await act(() => {
    touchLongPress(harness.text);
    selectText(harness.text, 0, 5);
  });
  const chip = harness.screen.getByRole("button", { name: "Cite selection" });
  await expect.element(chip).toBeVisible();
  expect(chip.element().textContent).toBe("Cite");
  expect(chip.element().closest("[data-cite-chip-anchor]")).not.toBeNull();
});

it("hides the chip when the selection collapses", async () => {
  const onCite = vi.fn(() => true);
  await using harness = await renderHarness(onCite);
  await act(() => {
    touchLongPress(harness.text);
    selectText(harness.text, 0, 5);
  });
  const chip = harness.screen.getByRole("button", { name: "Cite selection" });
  await expect.element(chip).toBeVisible();
  await act(() => collapseSelection());
  await expect.element(chip).not.toBeInTheDocument();
});

it("keeps the chip while the timeline scrolls", async () => {
  const onCite = vi.fn(() => true);
  await using harness = await renderHarness(onCite);
  await act(() => {
    touchLongPress(harness.text);
    selectText(harness.text, 0, 5);
  });
  const chip = harness.screen.getByRole("button", { name: "Cite selection" });
  await expect.element(chip).toBeVisible();
  await act(() =>
    harness.text.closest("[data-assistant-citation-viewport]")!.dispatchEvent(new Event("scroll")),
  );
  await expect.element(chip).toBeInTheDocument();
});

it("still offers mouse selections as the popover by the selection, with no chip", async () => {
  const onCite = vi.fn(() => true);
  await using harness = await renderHarness(onCite);
  const rect = harness.text.getBoundingClientRect();
  await act(() => {
    harness.text.dispatchEvent(
      new PointerEvent("pointerdown", {
        bubbles: true,
        pointerType: "mouse",
        isPrimary: true,
        clientX: rect.left + 4,
        clientY: rect.top + 4,
      }),
    );
    selectText(harness.text, 0, 5);
    harness.text.dispatchEvent(
      new MouseEvent("mouseup", {
        bubbles: true,
        detail: 1,
        clientX: rect.left + 4,
        clientY: rect.top + 4,
      }),
    );
  });
  await expect
    .element(page.getByRole("button", { name: "Cite selection in composer" }))
    .toBeVisible();
  expect(harness.screen.container.querySelector("[data-cite-chip-anchor] button")).toBeNull();
});

it("cites from the chip without losing the selection first", async () => {
  let selectionAtCite = "";
  const onCite = vi.fn((_citation: AssistantCitation) => {
    selectionAtCite = window.getSelection()?.toString() ?? "";
    return true;
  });
  await using harness = await renderHarness(onCite);
  await act(() => {
    touchLongPress(harness.text);
    selectText(harness.text, 0, 5);
  });
  const chip = harness.screen.getByRole("button", { name: "Cite selection" });
  await expect.element(chip).toBeVisible();
  await act(() => {
    chip
      .element()
      .dispatchEvent(
        new PointerEvent("pointerdown", { bubbles: true, pointerType: "touch", isPrimary: true }),
      );
    chip.element().dispatchEvent(new MouseEvent("click", { bubbles: true, detail: 1 }));
  });
  expect(onCite).toHaveBeenCalledTimes(1);
  expect(onCite.mock.calls[0]![0]).toMatchObject({ text: "hello", messageId: "source" });
  expect(selectionAtCite).toBe("hello");
  await expect.element(chip).not.toBeInTheDocument();
});
