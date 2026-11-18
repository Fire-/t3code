import { DEFAULT_THEMES, preloadHighlighter } from "@pierre/diffs";
import { EnvironmentId, ThreadId, type ProjectReadFileResult } from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
import { cloneElement, StrictMode, type ReactNode } from "react";
import { describe, expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { act, fakeTimers, render } from "../../../test/render";
import { useComposerDraftStore } from "~/composerDraftStore";
import appCss from "~/index.css?inline";
import { isMacPlatform } from "~/lib/utils";
import {
  createWorkspaceFocusController,
  startWorkspaceFocus,
  workspaceAreaProps,
} from "~/lib/workspaceFocus";

const state = vi.hoisted(() => ({
  file: null as ProjectReadFileResult | null,
  optimistic: null as ProjectReadFileResult | null,
  writeFile: vi.fn(),
  assetUrl: { _tag: "Loading" } as import("~/assets/assetUrls").AssetUrlState,
  mintUrl: vi.fn<() => Promise<string | null>>(),
}));

vi.mock(import("@t3tools/client-runtime/state/runtime"), async (original) => ({
  ...(await original()),
  runAtomCommand: ((_registry: unknown, _command: unknown, request: unknown) =>
    state.writeFile(request)) as never,
}));
vi.mock(import("~/state/session"), async (original) => ({
  ...(await original()),
  readEnvironmentScope: () => true,
  useEnvironmentScope: () => true,
}));
vi.mock(import("~/state/filesystem"), async (original) => ({
  ...(await original()),
  useFilesystemReadAccess: () => ({ canReadFiles: true, isPending: false, error: null }),
}));
vi.mock(import("~/state/use-atom-command"), async (original) => ({
  ...(await original()),
  useAtomCommand: (() => state.writeFile) as never,
}));
vi.mock(import("~/state/server"), async (original) => {
  const actual = await original();
  const { Atom } = await import("effect/reactivity");
  const noConfig = Atom.make(null);
  return {
    ...actual,
    serverEnvironment: { ...actual.serverEnvironment, configValueAtom: (() => noConfig) as never },
  };
});
vi.mock(import("~/remoteOpen"), async (original) => ({
  ...(await original()),
  useRemoteOpenState: () => ({ mode: "local-exec" }) as never,
  useRemoteOpenResolution: () => ({ state: { mode: "local-exec" }, isResolved: true }) as never,
}));
vi.mock(import("~/hooks/useTheme"), async (original) => ({
  ...(await original()),
  useTheme: () => ({ resolvedTheme: "dark" }) as never,
}));
vi.mock(import("~/hooks/useWorkspaceMutationRefresh"), () => ({
  useWorkspaceMutationRefresh: vi.fn(),
}));
vi.mock(import("~/assets/assetUrls"), async (original) => ({
  ...(await original()),
  useAssetUrlRefresh: () => state.mintUrl,
  useAssetUrlState: () => state.assetUrl,
}));
// The Word engine compiles a large WebAssembly bundle; these tests only need the URL it loads.
vi.mock(import("./OfficeFilePreview"), () => ({
  OfficeFilePreview: ({ src }: { src: string }) => <div data-testid="office-preview">{src}</div>,
}));
vi.mock(import("./FileBreadcrumbs"), () => ({ FileBreadcrumbs: () => <></> }));
vi.mock(import("./FileBrowserPanel"), () => ({
  default: () => (
    <div role="tree">
      <button type="button" role="treeitem" data-item-path="src/">
        src
      </button>
      <button type="button" role="treeitem" data-item-path="server.ts">
        server.ts
      </button>
    </div>
  ),
}));
vi.mock(import("../DiffWorkerPoolProvider"), () => ({
  DiffWorkerPoolProvider: ({ children }: { children?: ReactNode }) => <>{children}</>,
}));
vi.mock(import("./projectFilesQueryState"), async (original) => ({
  ...(await original()),
  useProjectFileQuery: () => ({
    data: state.file,
    error: null,
    isPending: false,
    readError: null,
    isNotFile: false,
    isFile: true,
    refresh: vi.fn(),
  }),
  getOptimisticProjectFileQueryData: () => state.optimistic,
  getUnsavedProjectFileQueryData: () => null,
  setProjectFileQueryData: (
    _environmentId: EnvironmentId,
    _cwd: string,
    relativePath: string,
    contents: string,
  ) => {
    state.optimistic = { relativePath, contents, truncated: false, byteLength: contents.length };
  },
  confirmProjectFileQueryData: () => true,
}));

import FilePreviewPanel from "./FilePreviewPanel";

const environmentId = EnvironmentId.make("markdown-preview-test");
const threadRef = { environmentId, threadId: ThreadId.make("thread") };
function setup() {
  const keys = ["t3code.fileExplorerOpen", "t3code.renderMarkdown"];
  const previous = keys.map((key) => localStorage.getItem(key));
  localStorage.setItem(keys[0]!, "false");
  localStorage.setItem(keys[1]!, "true");
  state.file = null;
  state.optimistic = null;
  state.assetUrl = { _tag: "Loading" };
  let mints = 0;
  state.mintUrl.mockReset().mockImplementation(async () => `/assets/report.docx?mint=${++mints}`);
  state.writeFile.mockReset().mockResolvedValue(AsyncResult.success(undefined));
  return {
    [Symbol.dispose]() {
      for (const [index, key] of keys.entries()) {
        const value = previous[index];
        if (value == null) localStorage.removeItem(key);
        else localStorage.setItem(key, value);
      }
    },
  };
}

function markdownPreview(truncated: boolean, contents = "- [ ] Keep the rest of this file\n") {
  state.file = {
    relativePath: "tasks.md",
    contents,
    byteLength: truncated ? 1_200_000 : contents.length,
    truncated,
  };
  return filePreview("tasks.md");
}

function filePreview(relativePath: string, workspaceMutationId: string | null = null) {
  return (
    <FilePreviewPanel
      environmentId={environmentId}
      cwd="/workspace"
      projectName="Project"
      relativePath={relativePath}
      threadRef={threadRef}
      composerDraftTarget={threadRef}
      keybindings={[]}
      availableEditors={[]}
      openInShortcutEnabled={false}
      revealLine={null}
      revealRequestId={0}
      onOpenFile={vi.fn()}
      onNavigateFolder={vi.fn()}
      folderTrail={undefined}
      onPendingChange={vi.fn()}
      selectedFilePending={false}
      workspaceMutationId={workspaceMutationId}
    />
  );
}

function documentPreview(workspaceMutationId: string, fileRevision: string) {
  state.file = null;
  state.assetUrl = {
    _tag: "Success",
    url: "/assets/report.docx",
    expiresAt: Number.MAX_SAFE_INTEGER,
    fileRevision,
  };
  return filePreview("report.docx", workspaceMutationId);
}

function deepActiveElement() {
  let active = document.activeElement;
  while (active?.shadowRoot?.activeElement) active = active.shadowRoot.activeElement;
  return active;
}

describe("source file focus", () => {
  it("hands panel focus to the file's editor, which owns mod+f", async () => {
    using _settings = setup();
    const style = document.createElement("style");
    style.textContent = appCss;
    document.head.append(style);
    using _style = { [Symbol.dispose]: () => style.remove() };
    await preloadHighlighter({ themes: Object.values(DEFAULT_THEMES), langs: ["typescript"] });
    const panel = () => (
      <div {...workspaceAreaProps("panel", 1, "Workspace panel")}>
        <div data-right-panel-surface-content>{filePreview("server.ts")}</div>
      </div>
    );
    const editorHasFocus = () => expect(deepActiveElement()?.matches("[data-content]")).toBe(true);

    await using screen = await render(panel());
    const focus = createWorkspaceFocusController(document);
    using _focus = { [Symbol.dispose]: focus.dispose };
    expect(focus.focus("panel")).toBe(true);
    state.file = {
      relativePath: "server.ts",
      contents: "const server = 1;\n",
      byteLength: 18,
      truncated: false,
    };
    await screen.rerender(panel());
    await vi.waitFor(editorHasFocus);
    await userEvent.keyboard("x");
    await vi.waitFor(() => expect(deepActiveElement()?.textContent).toContain("xconst server"));

    (deepActiveElement() as HTMLElement).blur();
    expect(focus.focus("panel", false)).toBe(true);
    await vi.waitFor(editorHasFocus);
    await userEvent.keyboard(
      isMacPlatform(navigator.platform) ? "{Meta>}f{/Meta}" : "{Control>}f{/Control}",
    );
    await vi.waitFor(() => expect(deepActiveElement()).toBeInstanceOf(HTMLInputElement));
  });

  it("focuses the editor when a file row is clicked or activated with Enter", async () => {
    using _settings = setup();
    localStorage.setItem("t3code.fileExplorerOpen", "true");
    const style = document.createElement("style");
    style.textContent = appCss;
    document.head.append(style);
    using _style = { [Symbol.dispose]: () => style.remove() };
    await preloadHighlighter({ themes: Object.values(DEFAULT_THEMES), langs: ["typescript"] });
    state.file = {
      relativePath: "server.ts",
      contents: "const server = 1;\n",
      byteLength: 18,
      truncated: false,
    };
    await using screen = await render(
      <div {...workspaceAreaProps("panel", 1, "Workspace panel")} style={{ width: 900 }}>
        <div data-right-panel-surface-content>{filePreview("server.ts")}</div>
      </div>,
    );
    using _focus = {
      [Symbol.dispose]: startWorkspaceFocus({ subscribe: () => () => {} } as never),
    };
    const editorHasFocus = () => expect(deepActiveElement()?.matches("[data-content]")).toBe(true);
    // The explorer measures its parent row; a wrapper without a box would cap it at the minimum.
    await expect
      .element(screen.getByRole("separator", { name: "Resize file explorer" }))
      .toHaveAttribute("aria-valuemax", "560");
    const folderRow = screen.getByRole("treeitem", { name: "src" });
    const fileRow = screen.getByRole("treeitem", { name: "server.ts" });

    (folderRow.element() as HTMLElement).focus();
    await userEvent.keyboard("{Enter}");
    expect(document.activeElement).toBe(folderRow.element());

    await userEvent.click(fileRow);
    await vi.waitFor(editorHasFocus);

    (fileRow.element() as HTMLElement).focus();
    await userEvent.keyboard("{Enter}");
    await vi.waitFor(editorHasFocus);
  });
});

describe("source file scroll", () => {
  it("returns a file tab to where it was scrolled after another file was shown", async () => {
    using _settings = setup();
    const style = document.createElement("style");
    style.textContent = appCss;
    document.head.append(style);
    using _style = { [Symbol.dispose]: () => style.remove() };
    await preloadHighlighter({ themes: Object.values(DEFAULT_THEMES), langs: ["typescript"] });
    const sourceFile = (relativePath: string, lines: number) => {
      const contents = Array.from({ length: lines }, (_, i) => `const line${i} = ${i};`).join("\n");
      state.file = { relativePath, contents, byteLength: contents.length, truncated: false };
      // The app mounts under StrictMode, whose remount must not wipe the saved offset.
      return (
        <StrictMode>
          <div style={{ height: 400, display: "flex", flexDirection: "column" }}>
            {filePreview(relativePath)}
          </div>
        </StrictMode>
      );
    };
    const scroller = () => document.querySelector<HTMLElement>(".file-preview-virtualizer");

    await using screen = await render(sourceFile("a.ts", 2000));
    await vi.waitFor(() => expect(scroller()?.scrollHeight).toBeGreaterThan(5000));
    scroller()!.scrollTop = 4200;
    await vi.waitFor(() => expect(scroller()?.scrollTop).toBe(4200));

    await screen.rerender(sourceFile("b.ts", 1500));
    await vi.waitFor(() => expect(scroller()?.scrollTop).toBe(0));
    await screen.rerender(sourceFile("a.ts", 2000));

    await vi.waitFor(() => expect(scroller()?.scrollTop).toBe(4200));
  });
});

describe("rendered Markdown task saves", () => {
  it("keeps a truncated read read-only without queuing a replacement", async () => {
    using _settings = setup();
    await using screen = await render(markdownPreview(true));
    using _timers = fakeTimers();
    const checkbox = screen.container.querySelector<HTMLInputElement>('input[type="checkbox"]')!;
    expect(checkbox).not.toBeNull();
    expect(checkbox.disabled).toBe(true);
    expect(checkbox.readOnly).toBe(true);
    await act(async () => checkbox.click());
    await vi.advanceTimersByTimeAsync(500);
    expect(checkbox.checked).toBe(false);
    expect(state.optimistic).toBeNull();
    expect(state.writeFile).not.toHaveBeenCalled();
  });

  it("still toggles and saves tasks from a complete read", async () => {
    using _settings = setup();
    await using screen = await render(markdownPreview(false));
    using _timers = fakeTimers();
    const checkbox = screen.container.querySelector<HTMLInputElement>('input[type="checkbox"]')!;
    expect(checkbox).not.toBeNull();
    expect(checkbox.disabled).toBe(false);
    await act(async () => checkbox.click());
    await vi.advanceTimersByTimeAsync(500);
    expect(state.writeFile).toHaveBeenCalledExactlyOnceWith({
      environmentId,
      input: {
        cwd: "/workspace",
        relativePath: "tasks.md",
        contents: "- [x] Keep the rest of this file\n",
      },
    });
  });
});

describe("workspace document previews", () => {
  it("keeps an unchanged document loaded when another workspace change lands", async () => {
    using _settings = setup();
    await using screen = await render(documentPreview("mutation-1", "revision-1"));
    const preview = screen.getByTestId("office-preview");
    await expect.element(preview).toHaveTextContent("/assets/report.docx?mint=1");
    const loaded = preview.element();

    await screen.rerender(documentPreview("mutation-2", "revision-1"));

    expect(state.mintUrl, "an unchanged document re-minted its URL").toHaveBeenCalledOnce();
    expect(screen.container.contains(loaded), "an unchanged document was remounted").toBe(true);
  });

  it("keeps an unchanged document mounted while its revision check reloads", async () => {
    using _settings = setup();
    await using screen = await render(documentPreview("mutation-1", "revision-1"));
    const preview = screen.getByTestId("office-preview");
    await expect.element(preview).toHaveTextContent("/assets/report.docx?mint=1");
    const loaded = preview.element();

    state.assetUrl = { _tag: "Loading" };
    await screen.rerender(filePreview("report.docx", "mutation-1"));
    await screen.rerender(documentPreview("mutation-1", "revision-1"));

    expect(
      state.mintUrl,
      "a reloading revision check re-minted the document URL",
    ).toHaveBeenCalledOnce();
    expect(
      screen.container.contains(loaded),
      "a reloading revision check remounted the document",
    ).toBe(true);
  });

  it("reloads the document when its URL can no longer be minted", async () => {
    using _settings = setup();
    await using screen = await render(documentPreview("mutation-1", "revision-1"));
    const preview = screen.getByTestId("office-preview");
    await expect.element(preview).toHaveTextContent("/assets/report.docx?mint=1");

    state.assetUrl = { _tag: "Failure" };
    await screen.rerender(filePreview("report.docx", "mutation-2"));

    await expect.element(preview).toHaveTextContent("/assets/report.docx?mint=2");
  });

  it("loads the new bytes when the document itself changes", async () => {
    using _settings = setup();
    await using screen = await render(documentPreview("mutation-1", "revision-1"));
    const preview = screen.getByTestId("office-preview");
    await expect.element(preview).toHaveTextContent("/assets/report.docx?mint=1");

    await screen.rerender(documentPreview("mutation-2", "revision-2"));

    await expect.element(preview).toHaveTextContent("/assets/report.docx?mint=2");
  });
});

describe("rendered Markdown select all", () => {
  it("selects only the rendered file, including after a task toggle", async () => {
    using _settings = setup();
    await using screen = await render(
      <>
        <p>Transcript text from the chat thread.</p>
        {markdownPreview(false, "- [ ] Task item\n\nRendered file body.\n")}
      </>,
    );
    const expectFileOnlySelection = async () => {
      await userEvent.keyboard(
        isMacPlatform(navigator.platform) ? "{Meta>}a{/Meta}" : "{Control>}a{/Control}",
      );
      const selection = window.getSelection()?.toString() ?? "";
      expect(selection).toContain("Rendered file body.");
      expect(selection).not.toContain("Transcript text from the chat thread.");
    };

    await userEvent.click(screen.getByText("Rendered file body."));
    await expectFileOnlySelection();
    // A toggled checkbox keeps focus on an input inside the preview.
    await userEvent.click(screen.getByRole("checkbox"));
    await expectFileOnlySelection();
  });
});

describe("rendered Markdown cite", () => {
  const citeContents = [
    "# Cite heading",
    "",
    "First paragraph with selectable text.",
    "",
    "- list one",
    "- list two",
  ].join("\n");

  function citePreview() {
    state.file = {
      relativePath: "notes.md",
      contents: citeContents,
      byteLength: citeContents.length,
      truncated: false,
    };
    return filePreview("notes.md");
  }

  /** The mouse gesture that ends a drag selection, as the selection toolbar observes it. */
  function mouseSelect(target: Element, start: number, end: number) {
    const rect = target.getBoundingClientRect();
    target.dispatchEvent(
      new PointerEvent("pointerdown", {
        bubbles: true,
        pointerType: "mouse",
        isPrimary: true,
        clientX: rect.left + 4,
        clientY: rect.top + 4,
      }),
    );
    selectRenderedText(target, start, end);
    target.dispatchEvent(
      new MouseEvent("mouseup", {
        bubbles: true,
        detail: 1,
        clientX: rect.left + 4,
        clientY: rect.top + 4,
      }),
    );
  }

  function selectRenderedText(target: Element, start: number, end: number) {
    window.getSelection()?.setBaseAndExtent(target.firstChild!, start, target.firstChild!, end);
    document.dispatchEvent(new Event("selectionchange"));
  }

  /** Android's long-press: contextmenu, then pointercancel, then the selection appears. */
  function touchLongPress(target: Element) {
    target.dispatchEvent(
      new PointerEvent("pointerdown", { bubbles: true, pointerType: "touch", isPrimary: true }),
    );
    target.dispatchEvent(new MouseEvent("contextmenu", { bubbles: true }));
    target.dispatchEvent(
      new PointerEvent("pointercancel", { bubbles: true, pointerType: "touch", isPrimary: true }),
    );
  }

  function draftReviewComments() {
    return useComposerDraftStore.getState().getComposerDraft(threadRef)?.reviewComments ?? [];
  }

  it("cites a mouse selection as a file comment with lines and quote", async () => {
    using _settings = setup();
    using _draft = {
      [Symbol.dispose]: () => useComposerDraftStore.getState().clearComposerContent(threadRef),
    };
    await using screen = await render(citePreview());
    const paragraph = screen.getByText("First paragraph with selectable text.").element();
    await act(() => mouseSelect(paragraph, 21, 36));
    const cite = page.getByRole("button", { name: "Cite selection in composer" });
    await expect.element(cite).toBeVisible();
    await act(() => (cite.element() as HTMLElement).click());
    expect(draftReviewComments()).toEqual([
      expect.objectContaining({
        filePath: "notes.md",
        sectionId: "file:notes.md",
        startIndex: 2,
        endIndex: 2,
        rangeLabel: "L3",
        text: "",
        diff: "First paragraph with selectable text.",
        fenceLanguage: "md",
        quote: {
          exact: "selectable text",
          prefix: "te heading First paragraph with ",
          suffix: ". list one list two ",
        },
      }),
    ]);
    // The comment form opens where the Cite button was; an empty comment is allowed.
    const form = page.getByRole("textbox", { name: "Comment on lines L3" });
    await expect.element(form).toBeVisible();
    await userEvent.click(form);
    await userEvent.keyboard("Check this wording");
    await act(() =>
      (page.getByRole("button", { name: "Comment" }).element() as HTMLElement).click(),
    );
    expect(draftReviewComments()).toEqual([
      expect.objectContaining({ text: "Check this wording" }),
    ]);
  });

  it("offers a touch selection as the chip docked in the file surface", async () => {
    using _settings = setup();
    using _draft = {
      [Symbol.dispose]: () => useComposerDraftStore.getState().clearComposerContent(threadRef),
    };
    await using screen = await render(citePreview());
    const paragraph = screen.getByText("First paragraph with selectable text.").element();
    await act(() => {
      touchLongPress(paragraph);
      selectRenderedText(paragraph, 21, 36);
    });
    const chip = page.getByRole("button", { name: "Cite selection" });
    await expect.element(chip).toBeVisible();
    expect(chip.element().closest("[data-file-cite-dock]")).not.toBeNull();
    await act(() => {
      chip
        .element()
        .dispatchEvent(
          new PointerEvent("pointerdown", { bubbles: true, pointerType: "touch", isPrimary: true }),
        );
      chip.element().dispatchEvent(new MouseEvent("click", { bubbles: true, detail: 1 }));
    });
    expect(draftReviewComments()).toEqual([
      expect.objectContaining({
        startIndex: 2,
        endIndex: 2,
        quote: {
          exact: "selectable text",
          prefix: "te heading First paragraph with ",
          suffix: ". list one list two ",
        },
      }),
    ]);
    // Cancel takes the comment back out of the draft.
    await act(() =>
      (page.getByRole("button", { name: "Cancel" }).element() as HTMLElement).click(),
    );
    expect(draftReviewComments()).toEqual([]);
  });

  function openCitedForm(paragraph: Element, start = 21, end = 36) {
    return act(async () => mouseSelect(paragraph, start, end)).then(async () => {
      const cite = page.getByRole("button", { name: "Cite selection in composer" });
      await expect.element(cite).toBeVisible();
      await act(() => (cite.element() as HTMLElement).click());
      const form = page.getByRole("textbox", { name: "Comment on lines L3" });
      await expect.element(form).toBeVisible();
      return form;
    });
  }

  it("saves the typed comment and offers Cite again when new text is selected mid-form", async () => {
    using _settings = setup();
    using _draft = {
      [Symbol.dispose]: () => useComposerDraftStore.getState().clearComposerContent(threadRef),
    };
    await using screen = await render(citePreview());
    const paragraph = screen.getByText("First paragraph with selectable text.").element();
    const form = await openCitedForm(paragraph);
    await userEvent.click(form);
    await userEvent.keyboard("Check this wording");
    // A new long-press on other text settles the open form: typed text saved, chip kept.
    const lastItem = screen.getByText("list two").element();
    await act(() => {
      touchLongPress(lastItem);
      selectRenderedText(lastItem, 0, 8);
    });
    expect(draftReviewComments()).toEqual([
      expect.objectContaining({ text: "Check this wording", rangeLabel: "L3" }),
    ]);
    const chip = page.getByRole("button", { name: "Cite selection" });
    await expect.element(chip).toBeVisible();
    await act(() => {
      chip
        .element()
        .dispatchEvent(
          new PointerEvent("pointerdown", { bubbles: true, pointerType: "touch", isPrimary: true }),
        );
      chip.element().dispatchEvent(new MouseEvent("click", { bubbles: true, detail: 1 }));
    });
    expect(draftReviewComments()).toEqual([
      expect.objectContaining({ text: "Check this wording", rangeLabel: "L3" }),
      expect.objectContaining({ text: "", rangeLabel: "L6" }),
    ]);
  });

  it("saves the typed comment and keeps the chip when a press lands outside the form", async () => {
    using _settings = setup();
    using _draft = {
      [Symbol.dispose]: () => useComposerDraftStore.getState().clearComposerContent(threadRef),
    };
    await using screen = await render(citePreview());
    const paragraph = screen.getByText("First paragraph with selectable text.").element();
    const form = await openCitedForm(paragraph);
    await userEvent.click(form);
    await userEvent.keyboard("Check this wording");
    await userEvent.click(screen.getByText("Cite heading"));
    expect(draftReviewComments()).toEqual([
      expect.objectContaining({ text: "Check this wording" }),
    ]);
    await expect.element(form).not.toBeInTheDocument();
  });

  it("keeps the chip and drops unsaved text when the form closes on Escape", async () => {
    using _settings = setup();
    using _draft = {
      [Symbol.dispose]: () => useComposerDraftStore.getState().clearComposerContent(threadRef),
    };
    await using screen = await render(citePreview());
    const paragraph = screen.getByText("First paragraph with selectable text.").element();
    const form = await openCitedForm(paragraph);
    await userEvent.click(form);
    await userEvent.keyboard("Unsaved note");
    await userEvent.keyboard("{Escape}");
    expect(draftReviewComments()).toEqual([expect.objectContaining({ text: "" })]);
    await expect.element(form).not.toBeInTheDocument();
  });

  it("dismisses the mouse popover when the preview scrolls and re-offers once it settles", async () => {
    using _settings = setup();
    using _draft = {
      [Symbol.dispose]: () => useComposerDraftStore.getState().clearComposerContent(threadRef),
    };
    await using screen = await render(citePreview());
    const paragraph = screen.getByText("First paragraph with selectable text.").element();
    await act(() => mouseSelect(paragraph, 21, 36));
    const cite = page.getByRole("button", { name: "Cite selection in composer" });
    await expect.element(cite).toBeVisible();
    // The breadcrumbs strip scrolls too; the preview's own viewport is the one that scrolls.
    const scroller = screen.container
      .querySelector("[data-file-cite-dock]")!
      .parentElement!.querySelector<HTMLElement>("[data-slot=scroll-area-viewport]")!;
    await act(() => scroller.dispatchEvent(new Event("scroll")));
    await expect.element(cite).not.toBeInTheDocument();
    await vi.waitFor(() => expect.element(cite).toBeVisible());
  });

  it("raises the touch chip clear of Chrome's bottom search bar in the file surface dock", async () => {
    using _settings = setup();
    const style = document.createElement("style");
    style.textContent = appCss;
    document.head.append(style);
    using _style = { [Symbol.dispose]: () => style.remove() };
    await using screen = await render(citePreview());
    const paragraph = screen.getByText("First paragraph with selectable text.").element();
    await act(() => {
      touchLongPress(paragraph);
      selectRenderedText(paragraph, 21, 36);
    });
    const chip = page.getByRole("button", { name: "Cite selection" });
    await expect.element(chip).toBeVisible();
    const surface = screen.container.querySelector("[data-file-cite-dock]")!.parentElement!;
    const raised =
      surface.getBoundingClientRect().bottom - chip.element().getBoundingClientRect().bottom;
    expect(raised).toBeGreaterThanOrEqual(96);
  });

  function spaceUnderLastLine(screen: { container: HTMLElement }) {
    const scroller = screen.container
      .querySelector("[data-file-cite-dock]")!
      .parentElement!.querySelector<HTMLElement>("[data-slot=scroll-area-viewport]")!;
    const lastLine = page.getByText("list two").element();
    return (
      scroller.getBoundingClientRect().top +
      scroller.scrollHeight -
      scroller.scrollTop -
      lastLine.getBoundingClientRect().bottom
    );
  }

  it("leaves room under the last line for Chrome's bottom search bar on touch screens", async () => {
    using _settings = setup();
    const style = document.createElement("style");
    style.textContent = appCss;
    document.head.append(style);
    using _style = { [Symbol.dispose]: () => style.remove() };
    const realMatchMedia = window.matchMedia.bind(window);
    using _touch = vi
      .spyOn(window, "matchMedia")
      .mockImplementation((query) => realMatchMedia(query === "(pointer: coarse)" ? "all" : query));
    await using screen = await render(citePreview());
    await expect.element(page.getByText("list two")).toBeVisible();
    expect(spaceUnderLastLine(screen)).toBeGreaterThanOrEqual(96);
  });

  it("adds no room under the last line with a mouse", async () => {
    using _settings = setup();
    const style = document.createElement("style");
    style.textContent = appCss;
    document.head.append(style);
    using _style = { [Symbol.dispose]: () => style.remove() };
    await using screen = await render(citePreview());
    await expect.element(page.getByText("list two")).toBeVisible();
    expect(spaceUnderLastLine(screen)).toBeLessThan(48);
  });

  it("keeps the composer-docked touch chip sitting in its dock", async () => {
    using _settings = setup();
    const style = document.createElement("style");
    style.textContent = appCss;
    document.head.append(style);
    using _style = { [Symbol.dispose]: () => style.remove() };
    const composerDock = document.createElement("div");
    composerDock.setAttribute("data-composer-dock", "true");
    document.body.append(composerDock);
    using _dock = { [Symbol.dispose]: () => composerDock.remove() };
    state.file = {
      relativePath: "notes.md",
      contents: citeContents,
      byteLength: citeContents.length,
      truncated: false,
    };
    await using screen = await render(
      cloneElement(filePreview("notes.md"), { citeChipAnchor: composerDock }),
    );
    const paragraph = screen.getByText("First paragraph with selectable text.").element();
    await act(() => {
      touchLongPress(paragraph);
      selectRenderedText(paragraph, 21, 36);
    });
    const chip = page.getByRole("button", { name: "Cite selection" });
    await expect.element(chip).toBeVisible();
    expect(chip.element().closest("[data-composer-dock]")).toBe(composerDock);
    const offset = Math.abs(
      chip.element().getBoundingClientRect().bottom - composerDock.getBoundingClientRect().bottom,
    );
    expect(offset).toBeLessThan(24);
  });

  it("keeps the rendered preview filling the panel and scrolling", async () => {
    using _settings = setup();
    // The layout assertions need the real stylesheet, not just class names.
    const style = document.createElement("style");
    style.textContent = appCss;
    document.head.append(style);
    using _style = { [Symbol.dispose]: () => style.remove() };
    state.file = {
      relativePath: "notes.md",
      contents: `${citeContents}\n\n${"Filler paragraph.\n\n".repeat(40)}`,
      byteLength: 4_000,
      truncated: false,
    };
    await using screen = await render(
      <div style={{ height: 320, display: "flex", flexDirection: "column" }}>
        {filePreview("notes.md")}
      </div>,
    );
    const dock = screen.container.querySelector("[data-file-cite-dock]")!;
    const surface = dock.parentElement!;
    const scroller = surface.querySelector<HTMLElement>("[data-slot=scroll-area-viewport]")!;
    await vi.waitFor(() => expect(scroller.scrollHeight).toBeGreaterThan(scroller.clientHeight));
    expect(surface.getBoundingClientRect().height).toBeGreaterThan(250);
    expect(scroller.getBoundingClientRect().height).toBeCloseTo(
      surface.getBoundingClientRect().height,
      0,
    );
    scroller.scrollTop = 48;
    expect(scroller.scrollTop).toBe(48);
  });

  it("cites a selection across blocks as the span from first to last line", async () => {
    using _settings = setup();
    using _draft = {
      [Symbol.dispose]: () => useComposerDraftStore.getState().clearComposerContent(threadRef),
    };
    await using screen = await render(citePreview());
    const paragraph = screen.getByText("First paragraph with selectable text.").element();
    const lastItem = screen.getByText("list two").element();
    await act(() => {
      window.getSelection()?.setBaseAndExtent(paragraph.firstChild!, 21, lastItem.firstChild!, 8);
      document.dispatchEvent(new Event("selectionchange"));
      paragraph.dispatchEvent(new MouseEvent("mouseup", { bubbles: true, detail: 1 }));
    });
    const cite = page.getByRole("button", { name: "Cite selection in composer" });
    await expect.element(cite).toBeVisible();
    await act(() => (cite.element() as HTMLElement).click());
    expect(draftReviewComments()).toEqual([
      expect.objectContaining({
        startIndex: 2,
        endIndex: 5,
        rangeLabel: "L3 to L6",
        diff: ["First paragraph with selectable text.", "", "- list one", "- list two"].join("\n"),
      }),
    ]);
  });
});
