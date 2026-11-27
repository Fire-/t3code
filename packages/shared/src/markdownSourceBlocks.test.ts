import { describe, expect, it } from "vite-plus/test";

import { markdownSourceBlocks, type MarkdownSourceBlock } from "./markdownSourceBlocks.ts";

const outline = (blocks: ReadonlyArray<MarkdownSourceBlock>, source: string): unknown[] =>
  blocks.map((block) =>
    block.children.length > 0
      ? [block.tagName, outline(block.children, source)]
      : [block.tagName, source.slice(block.start, block.end)],
  );

describe("markdownSourceBlocks", () => {
  it("names blocks as rendered HTML does, with header cells as th", () => {
    const source =
      "## Hi\n\n> - a\n>   1. b\n\n| x | y |\n| - | - |\n| 1 | 2 |\n\n***\n\n[ref]: /x";
    expect(outline(markdownSourceBlocks(source), source)).toEqual([
      ["h2", "## Hi"],
      [
        "blockquote",
        [
          [
            "ul",
            [
              [
                "li",
                [
                  ["p", "a"],
                  ["ol", [["li", [["p", "b"]]]]],
                ],
              ],
            ],
          ],
        ],
      ],
      [
        "table",
        [
          [
            "tr",
            [
              ["th", "| x "],
              ["th", "| y |"],
            ],
          ],
          [
            "tr",
            [
              ["td", "| 1 "],
              ["td", "| 2 |"],
            ],
          ],
        ],
      ],
      ["hr", "***"],
    ]);
  });
});
