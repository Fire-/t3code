import { describe, expect, it } from "vite-plus/test";

import { contextInsertionOffset, formatInlineContextReference } from "./composerContextReferences";

const chip = formatInlineContextReference({
  kind: "review-comment",
  contextId: "review-comment_first",
  label: "Alpha",
});

describe("contextInsertionOffset", () => {
  it("puts new context after a chip selected whole", () => {
    const prompt = `Look ${chip} here`;
    const start = prompt.indexOf(chip);
    expect(contextInsertionOffset(prompt, { start, end: start + chip.length })).toBe(
      start + chip.length,
    );
  });

  it("keeps a caret and a text selection at their start", () => {
    const prompt = `Look ${chip} here`;
    expect(contextInsertionOffset(prompt, { start: 2, end: 2 })).toBe(2);
    expect(contextInsertionOffset(prompt, { start: 0, end: 4 })).toBe(0);
    expect(contextInsertionOffset(prompt, { start: 0, end: prompt.length })).toBe(0);
  });
});
