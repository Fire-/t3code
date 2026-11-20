import type { Element, Nodes } from "hast";
import type { Pluggable } from "unified";

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
  "thead",
  "tbody",
  "tr",
  "th",
  "td",
  "hr",
]);

/**
 * Copies each block element's mdast position onto it as `data-source-start` and
 * `data-source-end` attributes (character offsets into the markdown source). Only
 * rendered file previews turn this on; chat messages keep their markup unchanged.
 */
export const rehypeMarkdownSourceOffsets: Pluggable = function rehypeMarkdownSourceOffsets() {
  return (tree: Nodes) => {
    const visit = (node: Nodes) => {
      if (node.type === "element") {
        const element = node as Element;
        if (SOURCE_OFFSET_TAGS.has(element.tagName)) {
          const start = element.position?.start.offset;
          const end = element.position?.end.offset;
          if (start !== undefined && end !== undefined && end > start) {
            element.properties.dataSourceStart = String(start);
            element.properties.dataSourceEnd = String(end);
          }
        }
      }
      if ("children" in node) {
        for (const child of node.children) visit(child);
      }
    };
    visit(tree);
  };
};
