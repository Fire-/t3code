import { ChevronDownIcon, PaperclipIcon } from "lucide-react";
import type { ReactNode } from "react";

import { Button } from "../ui/button";

export function WorkspaceFileDropOverlay() {
  return (
    <div
      className="pointer-events-none absolute inset-2 z-40 flex items-center justify-center rounded-2xl border-2 border-dashed border-primary/60 bg-primary/[0.035]"
      data-chat-workspace-drop-overlay="true"
    >
      <div
        role="status"
        className="flex items-center gap-2 rounded-full border border-primary/25 bg-background/95 px-4 py-2.5 text-sm font-medium text-foreground shadow-lg"
      >
        <PaperclipIcon className="size-4 text-primary" aria-hidden="true" />
        Drop files to attach
      </div>
    </div>
  );
}

/**
 * Floats controls `bottom` pixels up, clear of the composer and its attached banners, and
 * stacks them: the first child sits closest to the composer. The touch Cite chip and the
 * scroll-to-end button both live here, so they never cover each other.
 */
export function ComposerDock(props: { readonly bottom: number; readonly children: ReactNode }) {
  return (
    <div
      className="chat-composer-dock pointer-events-none absolute z-30 flex flex-col-reverse items-center"
      style={{ bottom: props.bottom }}
    >
      {props.children}
    </div>
  );
}

/** The summary names what waits at the end of the timeline, which is out of view while it shows. */
export function ScrollToEndButton(props: {
  readonly summary: string | null;
  readonly onScrollToEnd: () => void;
}) {
  return (
    <div className="pointer-events-none flex w-full justify-center py-1.5">
      <Button
        data-scroll-to-end="true"
        onPointerDown={(event) => event.preventDefault()}
        onClick={props.onScrollToEnd}
        className="pointer-events-auto"
        size="xs"
        variant="glass"
      >
        <ChevronDownIcon className="size-3.5" />
        Scroll to end
        {props.summary === null ? null : (
          <span className="text-muted-foreground">
            <span aria-hidden>· </span>
            {props.summary}
          </span>
        )}
      </Button>
    </div>
  );
}
