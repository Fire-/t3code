import {
  ASSISTANT_CITATION_MAX_TEXT_LENGTH,
  type EnvironmentId,
  type ProjectId,
  type ScopedThreadRef,
} from "@t3tools/contracts";
import { QuoteIcon } from "lucide-react";
import { useEffect, useLayoutEffect, useRef, useState, type ComponentProps } from "react";
import { createPortal } from "react-dom";

import { useComposerDraftStore, type DraftId } from "~/composerDraftStore";
import { useStableCallback } from "~/hooks/useStableCallback";
import {
  isSelectionRectVisible,
  observeSelectionActions,
  resolveSelectionActionPosition,
  type SelectionActionPoint,
} from "~/lib/selectionActions";
import { buildFileReviewComment, type ReviewCommentContext } from "~/reviewCommentContext";
import { DiffCommentAnnotation } from "../diffs/DiffCommentAnnotation";
import { Button } from "../ui/button";
import { captureFileCiteSelection, type FileCiteSelection } from "./fileMarkdownCiteSelection";
import { formatFileCommentRange, nextFileCommentId } from "./fileCommentAnnotations";

type CiteOffer = FileCiteSelection & {
  /** Null for a touch offer, which shows as the docked chip. */
  readonly position: SelectionActionPoint | null;
  /** The file text the captured lines map to, kept so a rewrite cannot skew them. */
  readonly sourceText: string;
};

type OpenComment = {
  readonly comment: ReviewCommentContext;
  /** Null when the form docks where the touch chip was. */
  readonly position: SelectionActionPoint | null;
};

/**
 * Offers Cite over text selected in rendered file markdown. Citing lands a file comment chip
 * in the composer draft, carrying the source lines and the quoted rendered text; a comment can
 * be typed right away, and an empty comment is allowed.
 */
export function FileMarkdownCite(props: {
  readonly viewport: HTMLElement | null;
  /** Where the touch chip and its comment form dock: the composer dock, or the file surface. */
  readonly anchor: HTMLElement | null;
  readonly filePath: string;
  readonly sourceText: string;
  readonly composerDraftTarget: ScopedThreadRef | DraftId;
  readonly environmentId: EnvironmentId;
  readonly projectId?: ProjectId | undefined;
}) {
  const [offer, setOffer] = useState<CiteOffer | null>(null);
  const [comment, setComment] = useState<OpenComment | null>(null);
  const chipRef = useRef<HTMLButtonElement>(null);
  const popoverButtonRef = useRef<HTMLButtonElement>(null);
  const popoverRef = useRef<HTMLDivElement>(null);
  const formRef = useRef<HTMLDivElement>(null);
  const actionsRef = useRef<ReturnType<typeof observeSelectionActions> | null>(null);
  const addReviewComment = useComposerDraftStore((store) => store.addReviewComment);
  const removeReviewComment = useComposerDraftStore((store) => store.removeReviewComment);

  // The Cite button clamps at the window edge; the comment form opens where the button was.
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- measures the popover and clamps its position inside the window before paint
  useLayoutEffect(() => {
    const popover = formRef.current ?? popoverRef.current;
    const position = comment?.position ?? offer?.position;
    if (!popover || !position) return;
    const rect = popover.getBoundingClientRect();
    popover.style.left = `${Math.max(8, Math.min(position.x, window.innerWidth - rect.width - 8))}px`;
    popover.style.top = `${Math.max(8, Math.min(position.y, window.innerHeight - rect.height - 8))}px`;
  });

  const update = useStableCallback((pointer: SelectionActionPoint | null, touch: boolean) => {
    const viewport = props.viewport;
    if (!viewport) return;
    // One comment form at a time: a new selection while it is open only dismisses the offer.
    if (comment !== null) {
      setOffer(null);
      return;
    }
    const sourceText = props.sourceText;
    const captured = captureFileCiteSelection(viewport, window.getSelection(), sourceText);
    if (
      captured === null ||
      !isSelectionRectVisible(captured.range.getBoundingClientRect(), [
        viewport.getBoundingClientRect(),
      ])
    ) {
      setOffer(null);
      return;
    }
    const rects = captured.range.getClientRects();
    setOffer({
      ...captured,
      sourceText,
      // A touch offer is the docked chip: Android's own menu floats over the selection and
      // its end handle sits right under it.
      position: touch
        ? null
        : resolveSelectionActionPosition({
            bounds: viewport.getBoundingClientRect(),
            selectionRect: rects.item(rects.length - 1) ?? captured.range.getBoundingClientRect(),
            pointer,
            viewport: { width: window.innerWidth, height: window.innerHeight },
          }),
    });
  });

  const getActionElement = useStableCallback(
    () => formRef.current ?? chipRef.current ?? popoverButtonRef.current,
  );

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- subscribes to document selectionchange and pointer gestures to track text selections in the preview
  useEffect(() => {
    const viewport = props.viewport;
    if (!viewport) return;
    const actions = observeSelectionActions({
      element: viewport,
      getActionElement,
      restoreAfterScroll: true,
      onSelection: update,
      onDismiss: () => setOffer(null),
    });
    actionsRef.current = actions;
    document.addEventListener("selectionchange", actions.selectionChanged);
    return () => {
      document.removeEventListener("selectionchange", actions.selectionChanged);
      actions.dispose();
      actionsRef.current = null;
    };
  }, [getActionElement, props.viewport, update]);

  if (!props.viewport) return null;

  const cite = (open: CiteOffer) => {
    const next = buildFileReviewComment({
      id: nextFileCommentId(),
      filePath: props.filePath,
      startLine: open.startLine,
      endLine: open.endLine,
      text: "",
      contents: open.sourceText,
      quote: { exact: open.quote.text, prefix: open.quote.prefix, suffix: open.quote.suffix },
    });
    addReviewComment(props.composerDraftTarget, next);
    window.getSelection()?.removeAllRanges();
    actionsRef.current?.cancel();
    setOffer(null);
    setComment({ comment: next, position: open.position });
    return true;
  };

  const finishComment = (text: string) => {
    if (!comment) return;
    addReviewComment(props.composerDraftTarget, { ...comment.comment, text });
    setComment(null);
  };

  const cancelComment = () => {
    if (!comment) return;
    removeReviewComment(props.composerDraftTarget, comment.comment.id);
    setComment(null);
  };

  if (comment) {
    const form = (
      <DiffCommentAnnotation
        environmentId={props.environmentId}
        projectId={props.projectId}
        ownerKey={JSON.stringify([props.composerDraftTarget, props.filePath, comment.comment.id])}
        kind="draft"
        allowEmpty
        rangeLabel={formatFileCommentRange(
          comment.comment.startIndex + 1,
          comment.comment.endIndex + 1,
        )}
        text=""
        onComment={finishComment}
        onCancel={cancelComment}
      />
    );
    if (comment.position === null) {
      return props.anchor === null
        ? null
        : createPortal(
            <div
              ref={formRef}
              className="pointer-events-auto w-[min(24rem,calc(100vw-2rem))] rounded-lg border border-border/70 bg-background p-1 shadow-lg"
            >
              {form}
            </div>,
            props.anchor,
          );
    }
    return createPortal(
      <div
        ref={formRef}
        className="fixed z-50 w-[min(24rem,calc(100vw-1rem))] rounded-lg border border-border/70 bg-background p-1 shadow-lg"
      >
        {form}
      </div>,
      document.body,
    );
  }

  if (!offer) return null;
  const tooLong = offer.quote.text.length > ASSISTANT_CITATION_MAX_TEXT_LENGTH;
  const dismiss = () => {
    actionsRef.current?.cancel();
    setOffer(null);
  };
  const citeButton = (extraProps: Partial<ComponentProps<typeof Button>>) => (
    <Button
      type="button"
      size="xs"
      variant="glass"
      disabled={tooLong}
      onPointerDown={(event) => event.preventDefault()}
      onClick={() => cite(offer)}
      onKeyDown={(event) => {
        event.stopPropagation();
        if (event.key === "Escape" && !event.nativeEvent.isComposing) {
          event.preventDefault();
          dismiss();
        }
      }}
      {...extraProps}
    >
      <QuoteIcon aria-hidden="true" className="size-3.5" />
      {tooLong ? "Shorten selection" : "Cite"}
    </Button>
  );

  if (offer.position === null) {
    return props.anchor === null
      ? null
      : createPortal(
          <div className="flex w-full justify-center py-1.5">
            {citeButton({
              ref: chipRef,
              "aria-label": tooLong ? "Selection is too long to cite" : "Cite selection",
              className: "pointer-events-auto",
            })}
          </div>,
          props.anchor,
        );
  }
  return createPortal(
    <div ref={popoverRef} className="fixed z-50 max-w-[calc(100vw-1rem)]">
      {citeButton({
        ref: popoverButtonRef,
        "aria-label": tooLong ? "Selection is too long to cite" : "Cite selection in composer",
      })}
    </div>,
    document.body,
  );
}
