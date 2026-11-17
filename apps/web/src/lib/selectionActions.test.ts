import { describe, expect, it, vi } from "vite-plus/test";

import { observeSelectionActions, resolveSelectionActionPosition } from "./selectionActions";

/**
 * Node has no DOM, so the observer runs against a hand-rolled tree: listeners are
 * recorded per event type and replayed in capture (document), target (element) and
 * bubble (view) order, like a real event aimed at the element.
 */
function selectionActionsFixture() {
  vi.useFakeTimers();
  const frames = new Map<number, () => void>();
  type Recorded = (event: never) => void;
  const listeners = {
    document: new Map<string, Recorded[]>(),
    element: new Map<string, Recorded[]>(),
    view: new Map<string, Recorded[]>(),
  };
  const track = (owner: keyof typeof listeners) => (type: string, listener: Recorded) => {
    const recorded = listeners[owner].get(type) ?? [];
    recorded.push(listener);
    listeners[owner].set(type, recorded);
  };
  const untrack = (owner: keyof typeof listeners) => (type: string, listener: Recorded) => {
    const remaining = (listeners[owner].get(type) ?? []).filter((known) => known !== listener);
    if (remaining.length > 0) listeners[owner].set(type, remaining);
    else listeners[owner].delete(type);
  };
  const textNode = { closest: () => null };
  const actionTarget = { closest: () => null };
  const view = {
    addEventListener: track("view"),
    removeEventListener: untrack("view"),
    setTimeout: (handler: () => void, delay?: number) => setTimeout(handler, delay),
    clearTimeout: (id: number) => clearTimeout(id),
    requestAnimationFrame: (callback: () => void) => {
      const id = frames.size + 1;
      frames.set(id, callback);
      return id;
    },
    cancelAnimationFrame: (id: number) => {
      frames.delete(id);
    },
  };
  const document = {
    addEventListener: track("document"),
    removeEventListener: untrack("document"),
    activeElement: null,
    defaultView: view,
  };
  const element = {
    addEventListener: track("element"),
    removeEventListener: untrack("element"),
    ownerDocument: document,
    contains: (node: unknown) => node === textNode,
  };
  const actionElement = { contains: (node: unknown) => node === actionTarget };
  const onSelection = vi.fn();
  const onDismiss = vi.fn();
  const actions = observeSelectionActions({
    element: element as unknown as HTMLElement,
    getActionElement: () => actionElement as unknown as HTMLElement,
    restoreAfterScroll: true,
    onSelection,
    onDismiss,
  });
  const fire = (
    owner: keyof typeof listeners,
    event: Record<string, unknown> & { type: string },
  ) => {
    // The snapshot keeps iteration safe if a listener disposes the observer.
    for (const listener of (listeners[owner].get(event.type) ?? []).slice()) {
      listener({ target: textNode, isPrimary: true, button: 0, ...event } as never);
    }
  };
  return {
    actions,
    onSelection,
    onDismiss,
    actionTarget,
    // A real event aimed at the element crosses the document's capture listeners
    // before the element's own and ends at the view's bubble listeners.
    elementEvent: (event: Record<string, unknown> & { type: string }) => {
      fire("document", event);
      fire("element", event);
      fire("view", event);
    },
    documentEvent: (event: Record<string, unknown> & { type: string }) => fire("document", event),
    viewEvent: (event: Record<string, unknown> & { type: string }) => fire("view", event),
    runScheduledFrames: () => {
      for (const [id, callback] of Array.from(frames)) {
        frames.delete(id);
        callback();
      }
    },
    [Symbol.dispose]: () => {
      actions.dispose();
      vi.useRealTimers();
    },
  };
}

// Android's long-press: contextmenu, then pointercancel, then the selection changes.
function touchLongPress(fixture: ReturnType<typeof selectionActionsFixture>) {
  fixture.elementEvent({ type: "pointerdown", pointerType: "touch" });
  fixture.elementEvent({ type: "contextmenu" });
  fixture.elementEvent({ type: "pointercancel", pointerType: "touch" });
  fixture.actions.selectionChanged();
}

describe("observeSelectionActions", () => {
  it("offers a touch long-press selection once it settles", () => {
    using fixture = selectionActionsFixture();
    touchLongPress(fixture);
    vi.advanceTimersByTime(150);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).not.toHaveBeenCalled();
    vi.advanceTimersByTime(850);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).toHaveBeenCalledWith(null, true);
  });

  it("keeps re-offering a touch selection while its handles drag it", () => {
    using fixture = selectionActionsFixture();
    touchLongPress(fixture);
    vi.advanceTimersByTime(1000);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).toHaveBeenCalledTimes(1);
    // Dragging a selection handle reaches the page as selectionchange only.
    fixture.actions.selectionChanged();
    expect(fixture.onDismiss).toHaveBeenCalledWith("cancel");
    vi.advanceTimersByTime(150);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).toHaveBeenCalledTimes(1);
    fixture.actions.selectionChanged();
    vi.advanceTimersByTime(1000);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).toHaveBeenCalledTimes(2);
    expect(fixture.onSelection).toHaveBeenLastCalledWith(null, true);
  });

  it("offers nothing for a touch tap that selects nothing", () => {
    using fixture = selectionActionsFixture();
    fixture.elementEvent({ type: "pointerdown", pointerType: "touch" });
    fixture.elementEvent({ type: "pointerup", pointerType: "touch" });
    // Chrome follows a tap with compatibility mouse events.
    fixture.elementEvent({ type: "mouseup", detail: 1, clientX: 12, clientY: 8 });
    vi.advanceTimersByTime(1000);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).not.toHaveBeenCalled();
  });

  it("offers nothing for a touch scroll without a selection", () => {
    using fixture = selectionActionsFixture();
    fixture.elementEvent({ type: "pointerdown", pointerType: "touch" });
    fixture.elementEvent({ type: "pointercancel", pointerType: "touch" });
    fixture.elementEvent({ type: "scroll" });
    vi.advanceTimersByTime(1000);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).not.toHaveBeenCalled();
  });

  it("keeps a touch offer through a scroll", () => {
    using fixture = selectionActionsFixture();
    touchLongPress(fixture);
    vi.advanceTimersByTime(1000);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).toHaveBeenCalledTimes(1);
    const dismissals = fixture.onDismiss.mock.calls.length;
    fixture.elementEvent({ type: "scroll" });
    expect(fixture.onDismiss.mock.calls.length).toBe(dismissals);
    vi.advanceTimersByTime(1000);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).toHaveBeenCalledTimes(1);
  });

  it("keeps a touch offer through a tap inside the element", () => {
    using fixture = selectionActionsFixture();
    touchLongPress(fixture);
    vi.advanceTimersByTime(1000);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).toHaveBeenCalledTimes(1);
    const dismissals = fixture.onDismiss.mock.calls.length;
    fixture.elementEvent({ type: "pointerdown", pointerType: "touch" });
    expect(fixture.onDismiss.mock.calls.length).toBe(dismissals);
    // The selection the tap collapses is what withdraws the offer, at its own change.
    fixture.actions.selectionChanged();
    expect(fixture.onDismiss.mock.calls.length).toBeGreaterThan(dismissals);
    vi.advanceTimersByTime(1000);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).toHaveBeenCalledTimes(2);
    expect(fixture.onSelection).toHaveBeenLastCalledWith(null, true);
  });

  it("keeps a touch selection pending when its tap lands on the action element", () => {
    using fixture = selectionActionsFixture();
    touchLongPress(fixture);
    vi.advanceTimersByTime(1000);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).toHaveBeenCalledTimes(1);
    const dismissals = fixture.onDismiss.mock.calls.length;
    fixture.documentEvent({
      type: "pointerdown",
      pointerType: "touch",
      target: fixture.actionTarget,
    });
    expect(fixture.onDismiss.mock.calls.length).toBe(dismissals);
    expect(fixture.onSelection).toHaveBeenCalledTimes(1);
  });

  it("offers a touch long-press that closes the keyboard mid-gesture", () => {
    using fixture = selectionActionsFixture();
    // A long-press that blurs a focused text field closes Android's keyboard, and
    // the page resizes with it while the selection forms and settles.
    fixture.elementEvent({ type: "pointerdown", pointerType: "touch" });
    fixture.elementEvent({ type: "contextmenu" });
    fixture.viewEvent({ type: "resize" });
    fixture.elementEvent({ type: "pointercancel", pointerType: "touch" });
    fixture.actions.selectionChanged();
    fixture.viewEvent({ type: "resize" });
    vi.advanceTimersByTime(1000);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).toHaveBeenCalledWith(null, true);
  });

  it("keeps a touch offer through a resize", () => {
    using fixture = selectionActionsFixture();
    touchLongPress(fixture);
    vi.advanceTimersByTime(1000);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).toHaveBeenCalledTimes(1);
    const dismissals = fixture.onDismiss.mock.calls.length;
    fixture.viewEvent({ type: "resize" });
    expect(fixture.onDismiss.mock.calls.length).toBe(dismissals);
  });

  it("still dismisses a mouse offer on resize", () => {
    using fixture = selectionActionsFixture();
    fixture.elementEvent({ type: "pointerdown", pointerType: "mouse" });
    fixture.elementEvent({ type: "mouseup", detail: 1, clientX: 30, clientY: 20 });
    vi.advanceTimersByTime(1000);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).toHaveBeenCalledTimes(1);
    fixture.viewEvent({ type: "resize" });
    expect(fixture.onDismiss).toHaveBeenLastCalledWith("cancel");
  });

  it("still dismisses contextmenu for mouse gestures", () => {
    using fixture = selectionActionsFixture();
    fixture.elementEvent({ type: "pointerdown", pointerType: "mouse", button: 2 });
    fixture.elementEvent({ type: "contextmenu" });
    expect(fixture.onDismiss).toHaveBeenCalledWith("interaction");
    vi.advanceTimersByTime(1000);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).not.toHaveBeenCalled();
  });

  it("still dismisses pointercancel for mouse gestures", () => {
    using fixture = selectionActionsFixture();
    fixture.elementEvent({ type: "pointerdown", pointerType: "mouse" });
    fixture.elementEvent({ type: "pointercancel", pointerType: "mouse" });
    expect(fixture.onDismiss).toHaveBeenCalledWith("cancel");
    vi.advanceTimersByTime(1000);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).not.toHaveBeenCalled();
  });

  it("still offers mouse selections at the pointer", () => {
    using fixture = selectionActionsFixture();
    fixture.elementEvent({ type: "pointerdown", pointerType: "mouse" });
    fixture.elementEvent({ type: "mouseup", detail: 1, clientX: 30, clientY: 20 });
    vi.advanceTimersByTime(1000);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).toHaveBeenCalledWith({ x: 30, y: 20 }, false);
  });

  it("still dismisses a mouse offer on scroll and re-offers once it settles", () => {
    using fixture = selectionActionsFixture();
    fixture.elementEvent({ type: "pointerdown", pointerType: "mouse" });
    fixture.elementEvent({ type: "mouseup", detail: 1, clientX: 30, clientY: 20 });
    vi.advanceTimersByTime(1000);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).toHaveBeenCalledWith({ x: 30, y: 20 }, false);
    fixture.elementEvent({ type: "scroll" });
    expect(fixture.onDismiss).toHaveBeenCalledWith("cancel");
    vi.advanceTimersByTime(150);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).toHaveBeenLastCalledWith(null, false);
  });

  it("treats a pen long-press like touch", () => {
    using fixture = selectionActionsFixture();
    fixture.elementEvent({ type: "pointerdown", pointerType: "pen" });
    fixture.elementEvent({ type: "contextmenu" });
    fixture.elementEvent({ type: "pointercancel", pointerType: "pen" });
    fixture.actions.selectionChanged();
    vi.advanceTimersByTime(1000);
    fixture.runScheduledFrames();
    expect(fixture.onSelection).toHaveBeenCalledWith(null, true);
  });
});

describe("resolveSelectionActionPosition", () => {
  const bounds = { left: 0, top: 0, width: 800, height: 600 };
  const selectionRect = { right: 400, bottom: 300 };
  const viewport = { width: 800, height: 600 };

  it("still follows the pointer by default", () => {
    expect(
      resolveSelectionActionPosition({
        bounds,
        selectionRect,
        pointer: { x: 380, y: 280 },
        viewport,
      }),
    ).toEqual({ x: 380, y: 280 });
  });
});
