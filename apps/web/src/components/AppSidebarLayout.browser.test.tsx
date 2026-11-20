import {
  createHashHistory,
  createRootRoute,
  createRoute,
  createRouter,
  Outlet,
  RouterProvider,
} from "@tanstack/react-router";
import type { ReactNode } from "react";
import { page, userEvent } from "vite-plus/test/browser";
import { describe, expect, it, vi } from "vite-plus/test";

import { render } from "../../test/render";
import { registerModelPickerContent } from "../modelPickerVisibility";
import { Dialog, DialogPopup, DialogTitle } from "./ui/dialog";

vi.mock(import("../state/server"), async (original) => {
  const actual = await original();
  const { Atom } = await import("effect/reactivity");
  const { compileResolvedKeybindingsConfig } = await import("@t3tools/shared/keybindings");
  const keybindings = compileResolvedKeybindingsConfig([
    { key: "mod+b", command: "sidebar.toggle", when: "!terminalFocus" },
  ]);
  return { ...actual, primaryServerKeybindingsAtom: Atom.make(keybindings) };
});
// The thread list needs live environments; the shortcut lives in the shell around it.
vi.mock(import("./Sidebar"), () => ({ default: (() => null) as never }));

import { AppSidebarLayout } from "./AppSidebarLayout";

describe("sidebar toggle shortcut", () => {
  it("leaves the key to a focused terminal when its rule excludes terminalFocus", async () => {
    await page.viewport(1280, 800);
    using _platform = vi.spyOn(navigator, "platform", "get").mockReturnValue("Linux x86_64");
    await using screen = await render(
      <AppSidebarLayout>
        <div data-terminal-owner="drawer">
          <textarea aria-label="Terminal input" />
        </div>
        <button type="button">Outside</button>
      </AppSidebarLayout>,
    );
    const wrapper = screen.container.querySelector('[data-slot="sidebar-wrapper"]')!;

    await userEvent.click(screen.getByRole("textbox", { name: "Terminal input" }));
    await userEvent.keyboard("{Control>}b{/Control}");
    expect(wrapper.getAttribute("data-sidebar-state")).toBe("expanded");

    await userEvent.click(screen.getByRole("button", { name: "Outside" }));
    await userEvent.keyboard("{Control>}b{/Control}");
    await vi.waitFor(() => expect(wrapper.getAttribute("data-sidebar-state")).toBe("collapsed"));
  });
});

// Android's back gesture pops real browser history, which a memory router never consults.
async function renderWithBrowserHistory(threadPage: ReactNode) {
  const startHref = window.location.href;
  const root = createRootRoute({
    component: () => (
      <AppSidebarLayout>
        <Outlet />
      </AppSidebarLayout>
    ),
  });
  const routeTree = root.addChildren([
    createRoute({ getParentRoute: () => root, path: "/", component: () => <p>Home page</p> }),
    createRoute({ getParentRoute: () => root, path: "/usage", component: () => <p>Usage page</p> }),
    createRoute({
      getParentRoute: () => root,
      path: "/$environmentId/$threadId",
      component: () => threadPage,
    }),
  ]);
  const router = createRouter({
    routeTree,
    history: createHashHistory(),
    disableGlobalCatchBoundary: true,
  });
  const screen = await render(<RouterProvider router={router} />, { router: false });
  await expect.element(page.getByText("Home page")).toBeVisible();
  return {
    router,
    sidebarState: () =>
      screen.container
        .querySelector('[data-slot="sidebar-wrapper"]')
        ?.getAttribute("data-sidebar-state"),
    async [Symbol.asyncDispose]() {
      await screen[Symbol.asyncDispose]();
      window.history.replaceState(window.history.state, "", startHref);
    },
  };
}

describe("back on a phone", () => {
  it("opens the sidebar from a thread, then goes back once it is open", async () => {
    await page.viewport(414, 896);
    await using app = await renderWithBrowserHistory(<p>Thread page</p>);
    await app.router.navigate({
      to: "/$environmentId/$threadId",
      params: { environmentId: "env-1", threadId: "thread-1" },
    });
    await expect.element(page.getByText("Thread page")).toBeVisible();
    expect(app.sidebarState()).toBe("collapsed");

    window.history.back();
    await vi.waitFor(() => expect(app.sidebarState()).toBe("expanded"));
    await vi.waitFor(() => expect(window.location.hash).toBe("#/env-1/thread-1"));
    await expect.element(page.getByText("Thread page")).toBeInTheDocument();

    window.history.back();
    await expect.element(page.getByText("Home page")).toBeInTheDocument();
  });

  it.each([
    { name: "a wide window", width: 1280, to: "thread" },
    { name: "an open model picker", width: 414, to: "thread", picker: true },
    { name: "an open dialog", width: 414, to: "thread", dialog: true },
    { name: "a page that is not a thread", width: 414, to: "usage" },
  ] as const)("leaves back alone on $name", async (testCase) => {
    await page.viewport(testCase.width, 896);
    await using app = await renderWithBrowserHistory(
      <>
        <p>Thread page</p>
        {"dialog" in testCase ? (
          <Dialog defaultOpen>
            <DialogPopup>
              <DialogTitle>Rename thread</DialogTitle>
            </DialogPopup>
          </Dialog>
        ) : null}
      </>,
    );
    const sidebarBefore = app.sidebarState();
    if (testCase.to === "usage") {
      await app.router.navigate({ to: "/usage" });
      await expect.element(page.getByText("Usage page")).toBeVisible();
    } else {
      await app.router.navigate({
        to: "/$environmentId/$threadId",
        params: { environmentId: "env-1", threadId: "thread-1" },
      });
      await expect.element(page.getByText("Thread page")).toBeVisible();
    }
    const unregisterPicker = "picker" in testCase ? registerModelPickerContent() : () => {};
    using _picker = { [Symbol.dispose]: unregisterPicker };
    if ("dialog" in testCase) {
      await expect.element(page.getByRole("dialog", { name: "Rename thread" })).toBeVisible();
    }

    window.history.back();
    await expect.element(page.getByText("Home page")).toBeInTheDocument();
    expect(app.sidebarState()).toBe(sidebarBefore);
  });
});
