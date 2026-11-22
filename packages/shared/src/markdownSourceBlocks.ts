import type { Nodes } from "mdast";
import remarkGfm from "remark-gfm";
import remarkParse from "remark-parse";
import { unified } from "unified";

/** A block as rendered HTML names it, with its UTF-16 source offsets (end exclusive). */
export interface MarkdownSourceBlock {
  readonly tagName: string;
  readonly start: number;
  readonly end: number;
  /** Blocks inside a container: quotes, lists, list items, tables and rows. */
  readonly children: ReadonlyArray<MarkdownSourceBlock>;
}

const parser = unified().use(remarkParse).use(remarkGfm);

function tagName(node: Nodes, headerRow: boolean): string | null {
  switch (node.type) {
    case "heading":
      return `h${node.depth}`;
    case "paragraph":
      return "p";
    case "blockquote":
      return "blockquote";
    case "list":
      return node.ordered ? "ol" : "ul";
    case "listItem":
      return "li";
    case "code":
      return "pre";
    case "table":
      return "table";
    case "tableRow":
      return "tr";
    case "tableCell":
      return headerRow ? "th" : "td";
    case "thematicBreak":
      return "hr";
    default:
      return null;
  }
}

function blocks(nodes: ReadonlyArray<Nodes>, headerRow = false): MarkdownSourceBlock[] {
  const found: MarkdownSourceBlock[] = [];
  for (const node of nodes) {
    const name = tagName(node, headerRow);
    const start = node.position?.start.offset;
    const end = node.position?.end.offset;
    if (name === null || start === undefined || end === undefined) continue;
    const children =
      node.type === "table"
        ? node.children.flatMap((row, index) => blocks([row], index === 0))
        : node.type === "tableRow"
          ? blocks(node.children, headerRow)
          : node.type === "blockquote" || node.type === "list" || node.type === "listItem"
            ? blocks(node.children)
            : [];
    found.push({ tagName: name, start, end, children });
  }
  return found;
}

/**
 * The blocks of `markdown` as CommonMark and GFM read them, for renderers that keep no source
 * positions on paragraphs, headings or tables.
 */
export function markdownSourceBlocks(markdown: string): MarkdownSourceBlock[] {
  return blocks(parser.parse(markdown).children);
}
