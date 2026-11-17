import { maskMarkdownFrontmatter } from "@t3tools/shared/markdownFrontmatter";
import { describe, expect, it } from "vite-plus/test";

import { lineNumberAtOffset, resolveFileCiteLineRange } from "./fileMarkdownCite.logic";

const document = [
  "# Title", // line 1, offsets 0-7
  "",
  "First paragraph", // line 3
  "spans two lines.", // line 4
  "",
  "- one", // line 6
  "- two", // line 7
  "",
  "```ts", // line 9
  "const a = 1;", // line 10
  "```", // line 11
  "",
  "| a | b |", // line 13
  "| - | - |", // line 14
  "| 1 | 2 |", // line 15
].join("\n");

function offsetOf(lineNumber: number, column = 0): number {
  const lines = document.split("\n");
  return lines.slice(0, lineNumber - 1).reduce((length, line) => length + line.length + 1, column);
}

describe("lineNumberAtOffset", () => {
  it("counts line breaks before the offset", () => {
    expect(lineNumberAtOffset(document, 0)).toBe(1);
    expect(lineNumberAtOffset(document, offsetOf(4, 3))).toBe(4);
    expect(lineNumberAtOffset(document, document.length)).toBe(15);
  });

  it("clamps offsets outside the text", () => {
    expect(lineNumberAtOffset(document, -5)).toBe(1);
    expect(lineNumberAtOffset(document, document.length + 100)).toBe(15);
    expect(lineNumberAtOffset("", 10)).toBe(1);
  });

  it("keeps carriage returns on their own line", () => {
    expect(lineNumberAtOffset("a\r\nb\r\n", 3)).toBe(2);
  });
});

describe("resolveFileCiteLineRange", () => {
  it("maps a selection inside one paragraph to its lines", () => {
    const start = document.indexOf("First paragraph");
    const end = document.indexOf("spans two lines.") + "spans two lines.".length;
    expect(resolveFileCiteLineRange(document, start, end)).toEqual({ startLine: 3, endLine: 4 });
  });

  it("maps a multi-block selection to the span from first start to last end", () => {
    const start = document.indexOf("First paragraph");
    const end = offsetOf(7) + "- two".length;
    expect(resolveFileCiteLineRange(document, start, end)).toEqual({ startLine: 3, endLine: 7 });
  });

  it("maps a selection inside one list item", () => {
    const start = document.indexOf("two", document.indexOf("- two"));
    expect(resolveFileCiteLineRange(document, start, start + 3)).toEqual({
      startLine: 7,
      endLine: 7,
    });
  });

  it("maps a code block selection to the fenced lines", () => {
    const start = document.indexOf("```ts");
    const end = document.indexOf("```", start + 5) + 3;
    expect(resolveFileCiteLineRange(document, start, end)).toEqual({ startLine: 9, endLine: 11 });
  });

  it("maps a table row selection without spilling past the last line", () => {
    const start = document.indexOf("| 1 | 2 |");
    expect(resolveFileCiteLineRange(document, start, start + 9)).toEqual({
      startLine: 15,
      endLine: 15,
    });
  });

  it("treats the end offset as exclusive at a line break", () => {
    const start = document.indexOf("```ts");
    const end = document.indexOf("\n", start); // end of the opening fence line
    expect(resolveFileCiteLineRange(document, start, end)).toEqual({ startLine: 9, endLine: 9 });
  });

  it("normalizes a reversed range and rejects degenerate offsets", () => {
    const start = document.indexOf("- one");
    const end = start + 5;
    expect(resolveFileCiteLineRange(document, end, start)).toEqual({ startLine: 6, endLine: 6 });
    expect(resolveFileCiteLineRange(document, 3, 3)).toBeNull();
    expect(resolveFileCiteLineRange(document, Number.NaN, 5)).toBeNull();
  });

  it("keeps frontmatter offsets aligned with the original file", () => {
    const source = "---\ntitle: Doc\n---\n\nBody text.\n";
    const masked = maskMarkdownFrontmatter(source);
    expect(masked.length).toBe(source.length);
    const bodyOffset = masked.indexOf("Body text.");
    expect(resolveFileCiteLineRange(source, bodyOffset, bodyOffset + "Body text.".length)).toEqual({
      startLine: 5,
      endLine: 5,
    });
  });
});
