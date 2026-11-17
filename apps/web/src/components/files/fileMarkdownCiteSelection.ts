import type { AssistantTextSelector } from "~/lib/assistantTextSelection";
import { captureRenderedTextSelection } from "~/lib/assistantTextSelection";

import { resolveFileCiteLineRange } from "./fileMarkdownCite.logic";

export interface FileCiteSelection {
  /** 1-based, inclusive source lines the selection's blocks span. */
  readonly startLine: number;
  readonly endLine: number;
  /** The selected rendered text, with a little context around it. */
  readonly quote: AssistantTextSelector;
  /** The clamped selected range, for positioning the Cite popover. */
  readonly range: Range;
}

/** The nearest rendered block carrying source offsets, or null outside the offset-tagged tree. */
function findFileCiteSourceBlock(node: Node): HTMLElement | null {
  const element = node.nodeType === 1 ? (node as Element) : node.parentElement;
  return element?.closest<HTMLElement>("[data-source-start]") ?? null;
}

/**
 * Captures a selection inside rendered file markdown as a cite: the source lines come from
 * the offset-tagged blocks the selection touches, the quote from the rendered text stream.
 */
export function captureFileCiteSelection(
  container: HTMLElement,
  selection: Selection | null,
  sourceText: string,
): FileCiteSelection | null {
  const captured = captureRenderedTextSelection(container, selection);
  if (captured === null) return null;
  const startBlock = findFileCiteSourceBlock(captured.range.startContainer);
  const endBlock = findFileCiteSourceBlock(captured.range.endContainer);
  if (
    startBlock === null ||
    endBlock === null ||
    !container.contains(startBlock) ||
    !container.contains(endBlock)
  ) {
    return null;
  }
  const lines = resolveFileCiteLineRange(
    sourceText,
    Number(startBlock.dataset.sourceStart),
    Number(endBlock.dataset.sourceEnd),
  );
  if (lines === null) return null;
  return { ...lines, quote: captured.selector, range: captured.range };
}
