import type { Element, Root } from "hast";
import type { MarkdownSourceBlock } from "@t3tools/shared/markdownSourceBlocks";

/** Blocks that can hold selectable text, tagged so a selection maps back to source lines. */
const SOURCE_OFFSET_TAGS = new Set([
  "p",
  "h1",
  "h2",
  "h3",
  "h4",
  "h5",
  "h6",
  "ul",
  "ol",
  "li",
  "blockquote",
  "pre",
  "table",
  "tr",
  "th",
  "td",
  "hr",
]);

const lettersAndDigits = (value: string) => value.replace(/[^\p{L}\p{N}]/gu, "");

/** The tagged blocks directly under `node`, looking through other elements such as `thead`. */
function blockElements(node: Root | Element, found: Element[] = []): Element[] {
  for (const child of node.children) {
    if (child.type !== "element" || child.tagName === "svg") continue;
    if (SOURCE_OFFSET_TAGS.has(child.tagName)) found.push(child);
    else blockElements(child, found);
  }
  return found;
}

/** The text of `node`'s text nodes, and the start offsets Words itself put on it and inside it. */
function evidence(node: Element, texts: string[] = [], starts: number[] = []) {
  const start = node.position?.start.offset;
  if (start !== undefined) starts.push(start);
  for (const child of node.children) {
    if (child.type === "text") {
      const text = lettersAndDigits(child.value);
      if (text) texts.push(text);
    } else if (child.type === "element" && child.tagName !== "svg") {
      evidence(child, texts, starts);
    }
  }
  return { texts, starts };
}

/**
 * Pairs the rendered blocks under `parent` with the source blocks of the same container, in
 * order. A rendered block takes the next source block with its tag whose source holds all its
 * text and every position Words gave it; raw HTML and blocks Words splits differently stay
 * untagged, so a selection in them cites the enclosing block instead of the wrong lines.
 */
function pair(
  parent: Root | Element,
  sources: ReadonlyArray<MarkdownSourceBlock>,
  source: string,
  normalized: Map<MarkdownSourceBlock, string>,
  offset: number,
) {
  let next = 0;
  for (const element of blockElements(parent)) {
    const { texts, starts } = evidence(element);
    const at = sources.findIndex((block, index) => {
      if (index < next || block.tagName !== element.tagName) return false;
      if (starts.some((start) => start < block.start || start >= block.end)) return false;
      let text = normalized.get(block);
      if (text === undefined) {
        text = lettersAndDigits(source.slice(block.start, block.end));
        normalized.set(block, text);
      }
      return texts.every((part) => text.includes(part));
    });
    if (at < 0) continue;
    const block = sources[at]!;
    next = at + 1;
    element.properties.dataSourceStart = String(offset + block.start);
    element.properties.dataSourceEnd = String(offset + block.end);
    if (block.children.length > 0) pair(element, block.children, source, normalized, offset);
  }
}

/**
 * Copies each block's source offsets onto it as `data-source-start` and `data-source-end`
 * (UTF-16 offsets into the file: `offset` plus the position in `source`). Only rendered file previews ask for this; chat messages keep
 * their markup unchanged.
 */
export function addMarkdownSourceOffsets(
  tree: Root,
  source: string,
  blocks: ReadonlyArray<MarkdownSourceBlock>,
  /** Where `source` starts in its file, for a piece of a longer file. */
  offset = 0,
) {
  pair(tree, blocks, source, new Map(), offset);
}
