import { EnvironmentId, MessageId, ThreadId } from "@t3tools/contracts";
import { serializeAssistantCitation } from "@t3tools/shared/assistantCitations";
import { describe, expect, it } from "vite-plus/test";

import { getQuestionAnswerPreview, getQuestionAnswerText } from "./userInput.js";

const citation = (text: string, comment?: string) =>
  serializeAssistantCitation({
    version: 1,
    environmentId: EnvironmentId.make("environment-local"),
    threadId: ThreadId.make("thread-1"),
    messageId: MessageId.make("message-1"),
    text,
    start: 0,
    end: text.length,
    prefix: "",
    suffix: "",
    ...(comment === undefined ? {} : { comment }),
  });

// Shape the web client submits for an answer holding a review-comment chip.
const answerWithReviewComment = [
  "See my note: [Review comment: notes.md L2; ref=review-comment_c1]",
  "",
  '<t3_context version="1">',
  '<context kind="review-comment" id="review-comment_c1">',
  "file: notes.md",
  "range: L2 (2-2)",
  "comment:",
  "  Rename this",
  "diff:",
  "  +line changed",
  "</context>",
  "</t3_context>",
].join("\n");

describe("question answer display", () => {
  it("keeps the provider context envelope out of answer history and preview", () => {
    const history = getQuestionAnswerText(answerWithReviewComment);
    const preview = getQuestionAnswerPreview({
      requestId: "request-1",
      answers: { q1: answerWithReviewComment },
      attachmentsByQuestionId: {},
      questionTextById: { q1: "What should change?" },
    });

    expect(history).toBe("See my note: [Review comment: notes.md L2]");
    expect(preview).toContain("See my note:");
    expect(preview).not.toContain("<t3_context");

    const typedXml =
      'Use this XML literally: <t3_context version="1"><context kind="review-comment" id="example">keep me</context></t3_context>';
    expect(getQuestionAnswerText(typedXml)).toBe(typedXml);
  });

  it("shows quoted responses in the preview as their text and comment, not their links", () => {
    const answer = `${citation("Missing ffmpeg looks like nothing wrong", "fix please")} ${citation(
      "genuine",
    )}  and the rest too`;
    const preview = getQuestionAnswerPreview({
      requestId: "request-1",
      answers: { q1: answer },
      attachmentsByQuestionId: {},
      questionTextById: { q1: "Which suggestions should I apply?" },
    });

    expect(preview).toBe("“Missing ffmpeg looks…” fix please “genuine” and the rest too");
  });
});
