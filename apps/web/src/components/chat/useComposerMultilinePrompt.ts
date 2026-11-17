import { useState } from "react";
import { observeResize } from "../../lib/observeResize";

/**
 * The mobile collapsed composer display: none's the body, so the editor has no layout to
 * measure a range in. The width it would wrap at is still known from the body's laid-out
 * parent, and the editor's own text and font say whether it fits that width.
 */
function hiddenEditorTextWraps(editor: HTMLElement, body: HTMLElement): boolean | null {
  const host = body.parentElement;
  if (host === null || host.clientWidth === 0) return null;
  const text = editor.textContent;
  if (text === null || text.trim().length === 0) return false;
  const hostStyle = getComputedStyle(host);
  const bodyStyle = getComputedStyle(body);
  const lineWidth =
    host.clientWidth -
    Number.parseFloat(hostStyle.paddingLeft) -
    Number.parseFloat(hostStyle.paddingRight) -
    Number.parseFloat(bodyStyle.paddingLeft) -
    Number.parseFloat(bodyStyle.paddingRight);
  const editorStyle = getComputedStyle(editor);
  const context = document.createElement("canvas").getContext("2d");
  if (context === null) return null;
  context.font = `${editorStyle.fontStyle} ${editorStyle.fontWeight} ${editorStyle.fontSize} ${editorStyle.fontFamily}`;
  return context.measureText(text).width > lineWidth + 1;
}

function measureComposerMultilinePrompt(body: HTMLElement): boolean | null {
  const editor = body.querySelector<HTMLElement>('[data-testid="composer-editor"]');
  if (!editor) return null;
  if (editor.clientWidth === 0) return hiddenEditorTextWraps(editor, body);

  const bodyStyle = getComputedStyle(body);
  const expandedWidth =
    body.clientWidth -
    Number.parseFloat(bodyStyle.paddingLeft) -
    Number.parseFloat(bodyStyle.paddingRight);
  const lineHeight = Number.parseFloat(getComputedStyle(editor).lineHeight);
  const range = document.createRange();
  range.selectNodeContents(editor);
  const bounds = range.getBoundingClientRect();

  // Measure against expanded width to prevent inline actions oscillating collapse.
  return bounds.height > lineHeight + 1 || bounds.width > expandedWidth + 1;
}

/** Whether the prompt wraps, and the ref callback that measures it on the composer body. */
export function useComposerMultilinePrompt(): readonly [
  boolean,
  (body: HTMLElement | null) => (() => void) | undefined,
] {
  const [isMultiline, setIsMultiline] = useState(false);
  const observeBody = (body: HTMLElement | null) => {
    if (!body) return;
    // Measuring reads layout, so one measurement per frame runs after all of the frame's DOM
    // writes instead of forcing a layout after each mutation.
    let frame: number | null = null;
    const measure = () => {
      if (frame !== null) return;
      frame = requestAnimationFrame(() => {
        frame = null;
        const next = measureComposerMultilinePrompt(body);
        if (next !== null) setIsMultiline(next);
      });
    };
    measure();
    const editor = body.querySelector<HTMLElement>('[data-testid="composer-editor"]');
    const stopObserving = observeResize(editor ? [body, editor] : body, measure);
    const mutationObserver = new MutationObserver(measure);
    mutationObserver.observe(body, { childList: true, characterData: true, subtree: true });
    return () => {
      if (frame !== null) cancelAnimationFrame(frame);
      stopObserving();
      mutationObserver.disconnect();
    };
  };
  return [isMultiline, observeBody] as const;
}
