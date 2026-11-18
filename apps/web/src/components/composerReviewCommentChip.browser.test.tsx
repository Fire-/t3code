import { EnvironmentId, ProjectId } from "@t3tools/contracts";
import { useState } from "react";
import { userEvent } from "vite-plus/test/browser";
import { describe, expect, it, vi } from "vite-plus/test";

import "~/index.css";
import { act, render } from "../../test/render";
import {
  ComposerContextActionsContext,
  ComposerContextRecordsContext,
  ComposerContextReferenceChip,
} from "./composerContextPresentation";
import { composerContextRecordsFromDraft } from "./composerContextPresentation";
import { reviewCommentContextId } from "~/lib/composerContextRecords";
import { buildFileReviewComment, type ReviewCommentContext } from "~/reviewCommentContext";

const environmentId = EnvironmentId.make("files-cite-chip-test");

function fileComment(quote: boolean, text = ""): ReviewCommentContext {
  return buildFileReviewComment({
    id: "rc-1",
    filePath: "notes.md",
    startLine: 3,
    endLine: 3,
    text,
    contents: "First paragraph with selectable text.\n",
    ...(quote
      ? { quote: { exact: "selectable text", prefix: "First paragraph with ", suffix: "" } }
      : {}),
  });
}

function Harness(props: {
  comment: ReviewCommentContext;
  onEdit: (comment: ReviewCommentContext, text: string) => void;
}) {
  const [record] = useState(props.comment);
  const records = composerContextRecordsFromDraft({
    terminalContexts: [],
    reviewComments: [record],
  });
  const [actions] = useState({
    environmentId,
    projectId: ProjectId.make("project"),
    editReviewComment: props.onEdit,
    expandImage: () => {},
    expandVideo: () => {},
    openFile: () => {},
    openMention: () => {},
    openPullRequest: () => {},
  });
  return (
    <ComposerContextActionsContext value={actions}>
      <ComposerContextRecordsContext value={records}>
        <ComposerContextReferenceChip
          kind="review-comment"
          contextId={reviewCommentContextId(record.id)}
          label={record.filePath}
        />
        <button type="button">Outside</button>
      </ComposerContextRecordsContext>
    </ComposerContextActionsContext>
  );
}

function elementByLabel(label: string) {
  return document.querySelector(`[aria-label="${CSS.escape(label)}"]`);
}

describe("composer file comment chip", () => {
  it("labels a quoted file comment by its quote and offers a pencil only then", async () => {
    const onEdit = vi.fn();
    await using screen = await render(<Harness comment={fileComment(true)} onEdit={onEdit} />);
    const quoted = screen.container.querySelector("[data-slot=context-chip]")!;
    expect(quoted.textContent).toContain("selectable text");
    expect(quoted.textContent).not.toContain("notes.md");
    const pencil = elementByLabel("Add file comment");
    expect(pencil).not.toBeNull();
    const chip = elementByLabel("File comment, notes.md L3: selectable text. Edit comment");
    expect(chip).not.toBeNull();
  });

  it("opens the comment editor from a tap on the chip, showing the file, lines, and quote", async () => {
    const onEdit = vi.fn();
    await using _screen = await render(
      <Harness comment={fileComment(true, "First note")} onEdit={onEdit} />,
    );
    await act(() =>
      (
        elementByLabel("File comment, notes.md L3: selectable text. Edit comment") as HTMLElement
      ).click(),
    );
    const editor = document.querySelector<HTMLTextAreaElement>(
      'textarea[aria-label="Comment on lines L3"]',
    );
    expect(editor).not.toBeNull();
    expect(editor!.value).toBe("First note");
    const header = document.querySelector("[data-file-comment-editor-source]");
    expect(header?.textContent).toContain("notes.md");
    expect(header?.textContent).toContain("L3");
    expect(header?.textContent).toContain("selectable text");
  });

  it("keeps typed text when the pencil is pressed while the chip opened the editor", async () => {
    const onEdit = vi.fn();
    await using _screen = await render(
      <Harness comment={fileComment(true, "First note")} onEdit={onEdit} />,
    );
    await act(() =>
      (
        elementByLabel("File comment, notes.md L3: selectable text. Edit comment") as HTMLElement
      ).click(),
    );
    const editor = document.querySelector<HTMLTextAreaElement>(
      'textarea[aria-label="Comment on lines L3"]',
    )!;
    await act(async () => {
      editor.focus();
      Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value")!.set!.call(
        editor,
        "Half typed",
      );
      editor.dispatchEvent(new Event("input", { bubbles: true }));
    });
    await userEvent.click(elementByLabel("Edit file comment") as HTMLElement);
    const after = document.querySelector<HTMLTextAreaElement>(
      'textarea[aria-label="Comment on lines L3"]',
    );
    if (after) {
      expect(after.value).toBe("Half typed");
    } else {
      expect(onEdit).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ id: "rc-1" }),
        "Half typed",
      );
    }
  });

  it("keeps the plain file label and no pencil for a code-view comment chip", async () => {
    await using screen = await render(<Harness comment={fileComment(false)} onEdit={vi.fn()} />);
    expect(screen.container.textContent).toContain("notes.md L3");
    expect(elementByLabel("Edit file comment")).toBeNull();
    expect(elementByLabel("Review comment, notes.md L3. Show details")).not.toBeNull();
  });

  it("edits the comment from the pencil: Save upserts, Cancel keeps the old text", async () => {
    const onEdit = vi.fn();
    await using _screen = await render(
      <Harness comment={fileComment(true, "First note")} onEdit={onEdit} />,
    );
    await act(() => (elementByLabel("Edit file comment") as HTMLElement).click());
    const editor = document.querySelector<HTMLTextAreaElement>(
      'textarea[aria-label="Comment on lines L3"]',
    );
    expect(editor).not.toBeNull();
    expect(editor!.value).toBe("First note");
    await act(async () => {
      editor!.focus();
      Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value")!.set!.call(
        editor!,
        "Second note",
      );
      editor!.dispatchEvent(new Event("input", { bubbles: true }));
    });
    await act(() => {
      const save = [...document.querySelectorAll("button")].find(
        (button) => button.textContent?.trim() === "Save",
      )!;
      save.click();
    });
    expect(onEdit).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ id: "rc-1" }),
      "Second note",
    );
    expect(document.querySelector('textarea[aria-label="Comment on lines L3"]')).toBeNull();

    onEdit.mockClear();
    await act(() => (elementByLabel("Edit file comment") as HTMLElement).click());
    await act(() => {
      const cancel = [...document.querySelectorAll("button")].find(
        (button) => button.textContent?.trim() === "Cancel",
      )!;
      cancel.click();
    });
    expect(onEdit).not.toHaveBeenCalled();
  });

  it("saves typed text when the pencil editor is dismissed by an outside press, not by Escape", async () => {
    const onEdit = vi.fn();
    await using _screen = await render(
      <Harness comment={fileComment(true, "First note")} onEdit={onEdit} />,
    );
    const typeInEditor = async (value: string) => {
      const editor = document.querySelector<HTMLTextAreaElement>(
        'textarea[aria-label="Comment on lines L3"]',
      )!;
      await act(async () => {
        editor.focus();
        Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value")!.set!.call(
          editor,
          value,
        );
        editor.dispatchEvent(new Event("input", { bubbles: true }));
      });
    };

    await act(() => (elementByLabel("Edit file comment") as HTMLElement).click());
    await typeInEditor("Typed then tapped away");
    await userEvent.click(
      [...document.querySelectorAll("button")].find((button) => button.textContent === "Outside")!,
    );
    await vi.waitFor(() =>
      expect(document.querySelector('textarea[aria-label="Comment on lines L3"]')).toBeNull(),
    );
    expect(onEdit).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ id: "rc-1" }),
      "Typed then tapped away",
    );

    onEdit.mockClear();
    await act(() => (elementByLabel("Edit file comment") as HTMLElement).click());
    await typeInEditor("Typed then escaped");
    await userEvent.keyboard("{Escape}");
    await vi.waitFor(() =>
      expect(document.querySelector('textarea[aria-label="Comment on lines L3"]')).toBeNull(),
    );
    expect(onEdit).not.toHaveBeenCalled();
  });
});
