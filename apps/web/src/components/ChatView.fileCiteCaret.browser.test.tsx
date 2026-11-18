import { createMemoryHistory } from "@tanstack/react-router";
import type * as Context from "effect/Context";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";

import { AppRoot } from "~/AppRoot";
// The route loads ChatView lazily; importing it here keeps its first transform out of the timeout.
import "~/components/ChatView";
import { readComposerDraftPrompt, useComposerDraftStore } from "~/composerDraftStore";
import { __setClientSettingsForTests } from "~/hooks/useSettings";
import { reviewCommentContextId } from "~/lib/composerContextRecords";
import { buildFileReviewComment } from "~/reviewCommentContext";
import { getRouter } from "~/router";
import {
  ENVIRONMENT_ID,
  installFakeEnvironment,
  threadWithHistory,
  type FakeDriver,
} from "../../test/fakeEnvironment";
import { act, render } from "../../test/render";

// The socket is the only fake layer; see test/appScenarios.renders.tsx.
vi.mock("../../../../packages/client-runtime/src/connection/driver.ts", async (importOriginal) => {
  const [actual, { fakeDriver }, Layer] = await Promise.all([
    importOriginal<{ readonly ConnectionDriver: Context.Key<unknown, FakeDriver> }>(),
    import("../../test/fakeEnvironment"),
    import("effect/Layer"),
  ]);
  return { ...actual, layer: Layer.succeed(actual.ConnectionDriver, fakeDriver) };
});

vi.mock(import("~/rpc/requestLatencyState"), async (original) => ({
  ...(await original()),
  trackRpcRequestSent: () => {},
}));

const contents = "Alpha line\nBeta line\n";

function fileCite(id: string, line: number, exact: string) {
  return buildFileReviewComment({
    id,
    filePath: "notes.md",
    startLine: line,
    endLine: line,
    text: "",
    contents,
    quote: { exact, prefix: "", suffix: " line" },
  });
}

it("a file cite added after tapping a chip to edit it lands after that chip", async () => {
  const realMatchMedia = window.matchMedia.bind(window);
  using _touch = vi
    .spyOn(window, "matchMedia")
    .mockImplementation((query) => realMatchMedia(query === "(pointer: coarse)" ? "all" : query));
  await page.viewport(390, 844);
  const thread = threadWithHistory("file-cite-caret", 1);
  using _environment = installFakeEnvironment([thread]);
  __setClientSettingsForTests({
    ...DEFAULT_CLIENT_SETTINGS,
    onboardingCompletedAt: "2026-01-01T00:00:00.000Z",
  });
  const router = getRouter(
    createMemoryHistory({ initialEntries: [`/${ENVIRONMENT_ID}/${thread.id}`] }),
  );
  await router.load();
  await using _app = await render(<AppRoot router={router} />, {
    // AppRoot brings its own router.
    router: false,
  });
  const answer = page.getByText(`Answer 0 for ${thread.id}.`);
  await expect.element(answer).toBeVisible();
  const threadRef = scopeThreadRef(ENVIRONMENT_ID, thread.id);

  await act(() =>
    useComposerDraftStore.getState().addReviewComment(threadRef, fileCite("first", 1, "Alpha")),
  );
  // Tapping a chip to edit its comment selects that chip; then leave the composer as opening a
  // file does. The next cite belongs after the tapped chip, not in front of it.
  const editor = page.getByTestId("composer-editor");
  await expect.element(editor).toBeVisible();
  await userEvent.click(page.getByRole("button", { name: /^File comment, notes\.md L1: Alpha/ }));
  const commentBox = page.getByRole("textbox", { name: "Comment on lines L1" });
  await expect.element(commentBox).toBeVisible();
  await userEvent.click(commentBox);
  await userEvent.keyboard("nice");
  await userEvent.click(page.getByRole("button", { name: "Save" }));
  await expect.element(commentBox).not.toBeInTheDocument();
  await userEvent.click(answer);
  await vi.waitFor(() =>
    expect(document.querySelector('[data-chat-composer-mobile-collapsed="true"]')).not.toBeNull(),
  );

  await act(() =>
    useComposerDraftStore.getState().addReviewComment(threadRef, fileCite("second", 2, "Beta")),
  );
  const prompt = readComposerDraftPrompt(threadRef) ?? "";
  const first = prompt.indexOf(reviewCommentContextId("first"));
  const second = prompt.indexOf(reviewCommentContextId("second"));
  expect(first).toBeGreaterThanOrEqual(0);
  expect(second).toBeGreaterThan(first);
});
