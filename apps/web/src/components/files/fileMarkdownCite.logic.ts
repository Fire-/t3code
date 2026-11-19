/**
 * Source offsets come from mdast positions on rendered blocks; a selection's line range is
 * the span from the first block's start to the last block's end. Offsets are UTF-16 offsets
 * into the file text as rendered (frontmatter is masked with spaces, never moved).
 */

/** The 1-based line holding the character at `offset`. */
export function lineNumberAtOffset(text: string, offset: number): number {
  const clamped = Math.max(0, Math.min(offset, text.length));
  let line = 1;
  for (
    let index = text.indexOf("\n");
    index !== -1 && index < clamped;
    index = text.indexOf("\n", index + 1)
  ) {
    line += 1;
  }
  return line;
}

/**
 * The source lines spanned by block offsets `[startOffset, endOffset)`. The end is exclusive,
 * so a block that ends in a line break does not spill onto the next line.
 */
export function resolveFileCiteLineRange(
  text: string,
  startOffset: number,
  endOffset: number,
): { startLine: number; endLine: number } | null {
  if (!Number.isSafeInteger(startOffset) || !Number.isSafeInteger(endOffset)) return null;
  const start = Math.max(0, Math.min(startOffset, endOffset));
  const end = Math.max(start, Math.max(startOffset, endOffset));
  if (end <= start) return null;
  return {
    startLine: lineNumberAtOffset(text, start),
    endLine: lineNumberAtOffset(text, Math.max(start, end - 1)),
  };
}
