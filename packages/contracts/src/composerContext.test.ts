import { describe, expect, it } from "vite-plus/test";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import {
  ASSISTANT_CITATION_CONTEXT_LENGTH,
  ASSISTANT_CITATION_MAX_TEXT_LENGTH,
} from "./assistantCitations.ts";
import {
  COMPOSER_CONTEXT_KINDS,
  ComposerContextRecord,
  OrchestrationMessageContext,
  ReviewCommentContextRecord,
} from "./composerContext.ts";

const decodeRecord = Schema.decodeUnknownOption(ComposerContextRecord);
const decodeReviewComment = Schema.decodeUnknownOption(ReviewCommentContextRecord);
const decodeContext = Schema.decodeUnknownSync(OrchestrationMessageContext);

const base = { version: 1, contextId: "ctx_1" } as const;

const knownRecords: Record<(typeof COMPOSER_CONTEXT_KINDS)[number], Record<string, unknown>> = {
  image: {
    ...base,
    kind: "image",
    label: "checkout-error.png",
    attachmentId: "att_1",
    name: "checkout-error.png",
    mimeType: "image/png",
    sizeBytes: 1234,
  },
  file: {
    ...base,
    kind: "file",
    label: "notes.txt",
    attachmentId: "att_2",
    name: "notes.txt",
    mimeType: "text/plain",
    sizeBytes: 12,
  },
  terminal: {
    ...base,
    kind: "terminal",
    label: "Terminal 1 lines 509-514",
    terminalId: "term-1",
    terminalLabel: "Terminal 1",
    lineStart: 509,
    lineEnd: 514,
    text: "error: boom\n  at main.ts:1",
  },
  element: {
    ...base,
    kind: "element",
    label: "<Button>",
    pageUrl: "http://localhost:3000/checkout",
    pageTitle: "Checkout",
    tagName: "button",
    selector: "#pay",
    htmlPreview: '<button id="pay">Pay</button>',
    componentName: "Button",
    source: { functionName: "Button", fileName: "Button.tsx", lineNumber: 12, columnNumber: 3 },
    styles: "color: red;",
  },
  "preview-annotation": {
    ...base,
    kind: "preview-annotation",
    label: "Checkout",
    annotationId: "ann_1",
    pageUrl: "http://localhost:3000/checkout",
    pageTitle: "Checkout",
    comment: "Make this bigger",
    targetSummary: "1 selected element",
    styleChanges: ["font-size: 20px"],
    elements: [
      {
        pageUrl: "http://localhost:3000/checkout",
        pageTitle: "Checkout",
        tagName: "button",
        selector: "#pay",
        htmlPreview: "<button>Pay</button>",
        componentName: "Button",
        source: null,
        styles: "",
      },
    ],
    screenshotContextId: "ctx_2",
  },
  "review-comment": {
    ...base,
    kind: "review-comment",
    label: "ChatComposer.tsx L4118",
    sectionId: "diff-1",
    sectionTitle: "Changes",
    filePath: "apps/web/src/ChatComposer.tsx",
    startIndex: 4118,
    endIndex: 4118,
    rangeLabel: "L4118",
    text: "Why is this here?",
    diff: "+ const x = 1;",
    fenceLanguage: "diff",
    pullRequest: {
      number: 42,
      title: "Improve context chips",
      url: "https://github.com/pingdotgg/t3code/pull/42",
      headBranch: "feat/context-chips",
      baseBranch: "main",
      state: "open",
      isDraft: false,
    },
  },
  mention: { ...base, kind: "mention", label: "@src/index.ts", path: "src/index.ts" },
  skill: { ...base, kind: "skill", label: "$pinchtab", name: "pinchtab" },
  thread: {
    ...base,
    kind: "thread",
    label: "Fix login flow",
    environmentId: "environment-1",
    threadId: "thread-1",
    title: "Fix login flow",
  },
  environment: {
    ...base,
    kind: "environment",
    label: "vps",
    environmentId: "environment-vps",
  },
  response: {
    ...base,
    kind: "response",
    label: "Reviewer (search) finished",
    citation: {
      version: 1,
      environmentId: "environment-1",
      threadId: "thread-1",
      messageId: "message-1",
      text: "Reviewer (search) finished and returned blocked on one finding.",
      start: 0,
      end: 63,
      prefix: "",
      suffix: " I read its report",
    },
    itemId: "turn-item-1",
    subagents: [
      { threadId: "thread-2", title: "review pr 412 (search)" },
      { threadId: "thread-3", title: "review pr 413", environmentId: "environment-2" },
    ],
  },
};

describe("ComposerContextRecord", () => {
  it("keeps unknown kinds with their payload", () => {
    const decoded = decodeRecord({
      ...base,
      kind: "future-thing",
      label: "Future",
      payload: { anything: [1, 2, 3] },
    });
    expect(Option.getOrThrow(decoded)).toEqual({
      version: 1,
      contextId: "ctx_1",
      kind: "future-thing",
      label: "Future",
      payload: { anything: [1, 2, 3] },
    });
  });

  it("does not let a malformed known kind slide through as unknown", () => {
    expect(Option.isNone(decodeRecord({ ...base, kind: "image", label: "x" }))).toBe(true);
  });

  it("bounds the serialized payload of future context kinds", () => {
    const unknown = { ...base, kind: "future-thing", label: "Future" };
    expect(Option.isSome(decodeRecord({ ...unknown, payload: "x".repeat(63_998) }))).toBe(true);
    expect(Option.isNone(decodeRecord({ ...unknown, payload: "x".repeat(63_999) }))).toBe(true);
    expect(Option.isNone(decodeRecord({ ...unknown, payload: '"'.repeat(32_000) }))).toBe(true);
    expect(
      decodeContext({
        version: 1,
        records: [
          { ...unknown, payload: "x".repeat(64_000) },
          { ...knownRecords.skill, contextId: "ctx_skill" },
        ],
      }).records,
    ).toEqual([{ ...knownRecords.skill, contextId: "ctx_skill" }]);
  });
});

describe("ReviewCommentContextRecord quote", () => {
  const reviewComment = {
    ...base,
    kind: "review-comment",
    label: "notes.md L3",
    sectionId: "file:notes.md",
    sectionTitle: "File comment",
    filePath: "notes.md",
    startIndex: 2,
    endIndex: 2,
    rangeLabel: "L3",
    text: "",
    diff: "quoted line",
  } as const;

  it("decodes with a quote", () => {
    const decoded = Option.getOrThrow(
      decodeReviewComment({
        ...reviewComment,
        quote: {
          exact: "quoted line",
          prefix: "before ",
          suffix: " after",
        },
      }),
    );
    expect(decoded.quote).toEqual({
      exact: "quoted line",
      prefix: "before ",
      suffix: " after",
    });
  });

  it("decodes records written before quotes existed", () => {
    const decoded = Option.getOrThrow(decodeReviewComment(reviewComment));
    expect(decoded.quote).toBeUndefined();
  });

  it("bounds the quoted text and its context", () => {
    expect(
      Option.isNone(
        decodeRecord({
          ...reviewComment,
          quote: {
            exact: "x".repeat(ASSISTANT_CITATION_MAX_TEXT_LENGTH + 1),
            prefix: "",
            suffix: "",
          },
        }),
      ),
    ).toBe(true);
    expect(
      Option.isNone(
        decodeRecord({
          ...reviewComment,
          quote: {
            exact: "x",
            prefix: "x".repeat(ASSISTANT_CITATION_CONTEXT_LENGTH + 1),
            suffix: "",
          },
        }),
      ),
    ).toBe(true);
    expect(
      Option.isNone(
        decodeRecord({
          ...reviewComment,
          quote: {
            exact: "x",
            prefix: "",
            suffix: "x".repeat(ASSISTANT_CITATION_CONTEXT_LENGTH + 1),
          },
        }),
      ),
    ).toBe(true);
    expect(
      Option.isNone(
        decodeRecord({ ...reviewComment, quote: { exact: "", prefix: "", suffix: "" } }),
      ),
    ).toBe(true);
  });
});

describe("OrchestrationMessageContext", () => {
  it("rejects aggregate context size even when each record is valid", () => {
    const record = {
      ...knownRecords["preview-annotation"],
      styleChangeDetails: Array.from({ length: 200 }, () => ({
        targetId: "target",
        selector: null,
        property: "content",
        previousValue: "x".repeat(8_000),
        value: "y".repeat(8_000),
      })),
    };
    expect(Option.isSome(decodeRecord(record))).toBe(true);
    expect(() => decodeContext({ version: 1, records: [record] })).not.toThrow();
    expect(() =>
      decodeContext({
        version: 1,
        records: Array.from({ length: 6 }, (_, index) => ({
          ...record,
          contextId: `ctx_${index}`,
        })),
      }),
    ).toThrow();
  });

  it("sends a message without the records the wire cannot carry", () => {
    const encodeOrchestrationMessageContext = Schema.encodeUnknownSync(
      Schema.toCodecJson(OrchestrationMessageContext),
    );

    const wire = encodeOrchestrationMessageContext({
      version: 1,
      records: [
        decodeContext({ version: 1, records: [knownRecords.terminal] }).records[0],
        {
          ...base,
          contextId: "ctx_2",
          kind: "future-kind",
          label: "x",
          payload: { count: Number.NaN },
        },
        { ...knownRecords["review-comment"], contextId: "ctx_3", fenceLanguage: undefined },
        { ...base, contextId: "ctx_4", kind: "future-kind", label: "y", payload: { n: 1n } },
      ],
    });
    expect(decodeContext(wire).records.map((record) => record.contextId)).toEqual(["ctx_1"]);
  });

  it("rejects oversized arrays before dropping malformed records", () => {
    expect(() =>
      decodeContext({ version: 1, records: Array.from({ length: 201 }, () => ({})) }),
    ).toThrow();
  });

  it("drops undecodable records and keeps valid siblings", () => {
    const context = decodeContext({
      version: 1,
      records: [
        knownRecords.skill,
        { version: 1, kind: "image" },
        { ...knownRecords.terminal, contextId: "ctx_2" },
      ],
    });
    expect(context.records.map((record) => record.kind)).toEqual(["skill", "terminal"]);
  });

  it("rejects duplicate normalized context identities", () => {
    expect(() =>
      decodeContext({
        version: 1,
        records: [knownRecords.skill, { ...knownRecords.terminal, contextId: " ctx_1 " }],
      }),
    ).toThrow();
  });
});
