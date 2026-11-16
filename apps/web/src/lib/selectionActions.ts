export type SelectionActionPoint = { x: number; y: number };

export const SELECTION_MULTI_CLICK_INTERVAL_MS = 500;
const SELECTION_SCROLL_SETTLE_MS = 150;
// Touch selections keep changing under browser-owned drag handles, so their actions
// wait for the selection to settle before they appear.
const SELECTION_TOUCH_SETTLE_MS = 300;

export function resolveSelectionActionPosition(options: {
  bounds: { left: number; top: number; width: number; height: number };
  selectionRect: { right: number; bottom: number } | null;
  pointer: SelectionActionPoint | null;
  viewport: { width: number; height: number };
  belowSelection?: boolean;
}): SelectionActionPoint {
  const { bounds, selectionRect, pointer, viewport, belowSelection } = options;
  const underSelection = {
    x: selectionRect?.right ?? bounds.left + bounds.width - 140,
    y: selectionRect ? selectionRect.bottom + 4 : bounds.top + 12,
  };
  // Android floats its own Copy/Share menu above the selection, so a touch
  // placement stays below the text instead of following the pointer.
  const preferred = belowSelection ? underSelection : (pointer ?? underSelection);
  return {
    x: Math.max(
      8,
      Math.min(Math.max(bounds.left, preferred.x), bounds.left + bounds.width, viewport.width - 8),
    ),
    y: Math.max(
      8,
      Math.min(Math.max(bounds.top, preferred.y), bounds.top + bounds.height, viewport.height - 8),
    ),
  };
}

type SelectionRect = { left: number; top: number; right: number; bottom: number };

export function isSelectionRectVisible(rect: SelectionRect, clips: readonly SelectionRect[]) {
  let { left, top, right, bottom } = rect;
  for (const clip of clips) {
    left = Math.max(left, clip.left);
    top = Math.max(top, clip.top);
    right = Math.min(right, clip.right);
    bottom = Math.min(bottom, clip.bottom);
  }
  return right > left && bottom > top;
}

/**
 * Interactive popovers pass getActionElement to keep editing their fields from replacing the
 * captured source selection.
 */
export function observeSelectionActions({
  element,
  getActionElement,
  restoreAfterScroll = false,
  onSelection,
  onDismiss,
}: {
  element: HTMLElement;
  getActionElement?: () => HTMLElement | null;
  restoreAfterScroll?: boolean;
  onSelection: (pointer: SelectionActionPoint | null, touch: boolean) => void;
  onDismiss: (reason: "interaction" | "cancel") => void;
}) {
  const document = element.ownerDocument;
  const view = document.defaultView!;
  let pointerDown = false;
  let gestureActive = false;
  let touchGesture = false;
  let dismissed = false;
  let restorable = false;
  let pointer: SelectionActionPoint | null = null;
  let timer: number | null = null;
  let frame: number | null = null;

  const isActionTarget = (target: EventTarget | null) =>
    target !== null && (getActionElement?.()?.contains(target as Node) ?? false);

  const cancelPending = () => {
    if (timer !== null) view.clearTimeout(timer);
    if (frame !== null) view.cancelAnimationFrame(frame);
    timer = frame = null;
  };
  const cancel = () => {
    cancelPending();
    pointerDown = gestureActive = touchGesture = false;
    pointer = null;
    dismissed = true;
    restorable = false;
  };
  const dismiss = () => {
    cancel();
    onDismiss("cancel");
  };
  const onContextMenu = () => {
    // A touch long-press fires contextmenu while it selects text; that is not a
    // menu request.
    if (touchGesture) return;
    cancel();
    onDismiss("interaction");
  };
  const schedule = (delay: number, touch: boolean) => {
    restorable = true;
    cancelPending();
    timer = view.setTimeout(() => {
      timer = null;
      frame = view.requestAnimationFrame(() => {
        frame = null;
        onSelection(pointer, touch);
      });
    }, delay);
  };
  const onPointerDown = (event: PointerEvent) => {
    if (!event.isPrimary) return;
    if (isActionTarget(event.target)) {
      cancel();
      return;
    }
    cancelPending();
    touchGesture = false;
    pointerDown = event.button === 0;
    const inside = element.contains(event.target as Node);
    gestureActive = false;
    dismissed = true;
    restorable = false;
    pointer = null;
    if (inside) onDismiss("interaction");
    else if (getActionElement?.()) onDismiss("cancel");
  };
  const onSelectionStart = (event: PointerEvent) => {
    if (!event.isPrimary) return;
    const target = event.target as Element | null;
    if (target?.closest("button, [role=button]")) return;
    gestureActive = event.button === 0 && !event.defaultPrevented;
    // Pens drive the same long-press selection UI as fingers.
    touchGesture = gestureActive && (event.pointerType === "touch" || event.pointerType === "pen");
    dismissed = !gestureActive;
  };
  const onPointerUp = (event: PointerEvent) => {
    // Preventing pointerdown (for example on a toolbar) can suppress mouseup.
    if (event.isPrimary && event.button === 0) pointerDown = false;
  };
  const onMouseUp = (event: MouseEvent) => {
    if (event.button !== 0) return;
    pointerDown = false;
    if (!gestureActive) return;
    gestureActive = false;
    // A touch tap's compatibility mouseup must not speak for its gesture; touch
    // selections settle through selectionchange.
    if (touchGesture) return;
    pointer = { x: event.clientX, y: event.clientY };
    schedule(event.detail >= 2 ? SELECTION_MULTI_CLICK_INTERVAL_MS : 0, false);
  };
  const onPointerCancel = () => {
    // Android ends a long-press with pointercancel once it owns the selection.
    if (touchGesture) {
      pointerDown = gestureActive = false;
      return;
    }
    dismiss();
  };
  const onKeyDown = (event: KeyboardEvent) => {
    if (isActionTarget(event.target) && event.key !== "Escape") return;
    if (event.key === "Escape") {
      dismiss();
    } else if (!pointerDown) {
      cancelPending();
      gestureActive = false;
      pointer = null;
      dismissed = false;
    }
  };
  const onFocusIn = (event: FocusEvent) => {
    if (isActionTarget(event.target)) cancel();
    else if (!pointerDown && getActionElement?.()) dismiss();
  };
  const onScroll = () => {
    cancelPending();
    // Autoscroll during a drag must not cancel that gesture's eventual release.
    if (!pointerDown) {
      pointer = null;
      if (restoreAfterScroll && !dismissed && restorable)
        schedule(SELECTION_SCROLL_SETTLE_MS, touchGesture);
      else dismissed = true;
    }
    onDismiss("cancel");
  };

  document.addEventListener("pointerdown", onPointerDown, true);
  element.addEventListener("pointerdown", onSelectionStart);
  view.addEventListener("pointerup", onPointerUp);
  view.addEventListener("mouseup", onMouseUp);
  view.addEventListener("pointercancel", onPointerCancel);
  document.addEventListener("keydown", onKeyDown);
  document.addEventListener("focusin", onFocusIn);
  element.addEventListener("contextmenu", onContextMenu, true);
  element.addEventListener("scroll", onScroll, true);
  view.addEventListener("blur", dismiss);
  view.addEventListener("resize", dismiss);

  return {
    get pending() {
      return timer !== null || frame !== null;
    },
    cancel,
    selectionChanged() {
      if (isActionTarget(document.activeElement)) return;
      if (touchGesture) {
        // Browser-owned handles keep changing a touch selection; hide the actions
        // while they move and offer them once it settles.
        onDismiss("cancel");
        schedule(SELECTION_TOUCH_SETTLE_MS, true);
      } else if (pointerDown || gestureActive) {
        onDismiss("cancel");
      } else if (!dismissed && timer === null && frame === null) {
        schedule(0, false);
      }
    },
    dispose() {
      cancel();
      document.removeEventListener("pointerdown", onPointerDown, true);
      element.removeEventListener("pointerdown", onSelectionStart);
      view.removeEventListener("pointerup", onPointerUp);
      view.removeEventListener("mouseup", onMouseUp);
      view.removeEventListener("pointercancel", onPointerCancel);
      document.removeEventListener("keydown", onKeyDown);
      document.removeEventListener("focusin", onFocusIn);
      element.removeEventListener("contextmenu", onContextMenu, true);
      element.removeEventListener("scroll", onScroll, true);
      view.removeEventListener("blur", dismiss);
      view.removeEventListener("resize", dismiss);
    },
  };
}
