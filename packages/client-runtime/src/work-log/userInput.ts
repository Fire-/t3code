import { type UserInputAttachmentAnswerPayload } from "@t3tools/contracts";
import { collectAssistantCitations } from "@t3tools/shared/assistantCitations";
import { composerContextProviderTextForDisplay } from "@t3tools/shared/composerContextReferences";

function record(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

export function getQuestionAnswerText(value: unknown): string {
  // Answers holding context chips are stored in provider form.
  if (typeof value === "string") return composerContextProviderTextForDisplay(value);
  if (Array.isArray(value)) return value.map(getQuestionAnswerText).filter(Boolean).join(", ");
  const nested = record(value);
  return nested ? getQuestionAnswerText(nested.answers) : "";
}

export function getQuestionTextPreview(answer: UserInputAttachmentAnswerPayload): string {
  return Object.values(answer.questionTextById ?? {})
    .map((text) => text.replace(/\s+/g, " ").trim())
    .filter(Boolean)
    .join(" · ");
}

// Short, so the user's own words after a quote stay on the one-line preview.
const PREVIEW_QUOTE_LENGTH = 24;

function previewQuote(text: string): string {
  const flat = text.replace(/\s+/g, " ").trim();
  const characters = Array.from(flat);
  if (characters.length <= PREVIEW_QUOTE_LENGTH) return flat;
  const cut = characters.slice(0, PREVIEW_QUOTE_LENGTH).join("");
  const wordEnd = cut.lastIndexOf(" ");
  return `${(wordEnd > 0 ? cut.slice(0, wordEnd) : cut).trimEnd()}…`;
}

function citationsAsPreviewText(text: string): string {
  let result = "";
  let cursor = 0;
  for (const { citation, start, end } of collectAssistantCitations(text)) {
    result += `${text.slice(cursor, start)}“${previewQuote(citation.text)}”`;
    if (citation.comment) result += ` ${citation.comment}`;
    cursor = end;
  }
  return result + text.slice(cursor);
}

export function getQuestionAnswerPreview(answer: UserInputAttachmentAnswerPayload): string {
  const answers = Object.values(answer.answers)
    .map((value) => citationsAsPreviewText(getQuestionAnswerText(value)))
    .filter(Boolean);
  const attachments = Object.values(answer.attachmentsByQuestionId)
    .flat()
    .map((attachment) => attachment.name);
  return (
    answers.length > 0
      ? answers.join(" · ")
      : attachments.length > 0
        ? attachments.join(", ")
        : Object.values(answer.questionTextById ?? {}).join(" · ")
  )
    .replace(/\s+/g, " ")
    .trim();
}

export function hasQuestionAnswer(answer: UserInputAttachmentAnswerPayload): boolean {
  return (
    Object.values(answer.answers).some(getQuestionAnswerText) ||
    Object.values(answer.attachmentsByQuestionId).some((attachments) => attachments.length > 0)
  );
}
