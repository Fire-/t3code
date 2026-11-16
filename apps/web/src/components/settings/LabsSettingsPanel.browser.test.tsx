import { DEFAULT_SERVER_SETTINGS, type UnifiedSettings } from "@t3tools/contracts";
import type { ReactNode } from "react";
import { expect, it, vi } from "vite-plus/test";
import { userEvent } from "vite-plus/test/browser";

import { act, render } from "../../../test/render";

const state = vi.hoisted(() => ({
  settings: {} as UnifiedSettings,
  // Applies the patch, so a save behaves like the server round trip the row keys on.
  update: vi.fn((patch: Record<string, unknown>) => {
    Object.assign(state.settings, patch);
  }),
  signedIn: undefined as boolean | undefined,
  cloudConfigured: true,
}));

vi.mock(import("@clerk/react"), async (original) => ({
  ...(await original()),
  useAuth: (() => ({ isSignedIn: state.signedIn })) as never,
}));
vi.mock(import("../clerk/useT3ConnectAuthPrompt"), () => ({
  useT3ConnectAuthPrompt: () => ({ openAuthPrompt: vi.fn() }),
}));
vi.mock(import("~/cloud/publicConfig"), async (original) => ({
  ...(await original()),
  hasCloudPublicConfig: () => state.cloudConfigured,
}));
vi.mock(import("@tanstack/react-router"), async (original) => ({
  ...(await original()),
  Link: (({ children, ...props }: { readonly children?: ReactNode }) => (
    <a {...props}>{children}</a>
  )) as never,
}));
vi.mock(import("./SettingsScopeContext"), () => ({
  useSettingsScope: (() => ({
    scope: { kind: "all" },
    environments: [],
    connectedEnvironments: [],
    targets: [],
    target: null,
  })) as never,
  useOptionalSettingsScope: (() => null) as never,
}));
vi.mock(import("./useScopedSettings"), async (original) => ({
  ...(await original()),
  useScopedSettings: ((selector?: (settings: UnifiedSettings) => unknown) =>
    selector ? selector(state.settings) : state.settings) as never,
  useScopedSettingsMixed: () => false,
  useUpdateScopedSettings: () => state.update,
  useClearScopedSettings: () => vi.fn(),
}));
vi.mock(import("./settingsLayout"), async (original) => ({
  ...(await original()),
  SettingsPageContainer: (({ children }: { readonly children: ReactNode }) => children) as never,
  SettingsRow: ({
    title,
    description,
    status,
    control,
    children,
  }: {
    readonly title: ReactNode;
    readonly description?: ReactNode;
    readonly status?: ReactNode;
    readonly control?: ReactNode;
    readonly children?: ReactNode;
  }) => (
    <div>
      <h3>{title}</h3>
      {description}
      {status}
      {control}
      {children}
    </div>
  ),
  SettingResetButton: (() => null) as never,
}));
vi.mock(import("~/state/managedPolicy"), async (original) => ({
  ...(await original()),
  usePrimaryManagedPolicy: () => undefined,
}));

import { LabsSettingsPanel } from "./LabsSettingsPanel";

function setup(settings: Partial<typeof DEFAULT_SERVER_SETTINGS> = {}) {
  state.settings = { ...DEFAULT_SERVER_SETTINGS, ...settings } as UnifiedSettings;
  state.update.mockClear();
}

const switchFor = (container: HTMLElement) =>
  container.querySelector<HTMLElement>('[role="switch"][aria-label="Guest links"]');
const signInButton = (container: HTMLElement) =>
  [...container.querySelectorAll("button")].find((element) => element.textContent === "Sign in");

const addressInput = (container: HTMLElement) => {
  const found = container.querySelector<HTMLInputElement>('[aria-label="Public link address"]');
  expect(found).not.toBeNull();
  return found!;
};

async function typeAddress(container: HTMLElement, value: string) {
  await userEvent.fill(addressInput(container), value);
  await act(async () => addressInput(container).blur());
}

it("shows the sign-in wall only while no public link address is set", async () => {
  setup();
  state.signedIn = false;
  state.cloudConfigured = true;
  await using stack = new AsyncDisposableStack();
  const wall = stack.use(await render(<LabsSettingsPanel />, { router: false }));
  expect(signInButton(wall.container)).toBeDefined();
  expect(switchFor(wall.container)).toBeNull();

  setup({ guestSharePublicBaseUrl: "https://t3.example.com" });
  const row = stack.use(await render(<LabsSettingsPanel />, { router: false }));
  expect(signInButton(row.container)).toBeUndefined();
  expect(switchFor(row.container)).not.toBeNull();
  expect(addressInput(row.container).value).toBe("https://t3.example.com");
});

it("saves a public link address only when it is an HTTPS origin", async () => {
  setup();
  state.signedIn = true;
  state.cloudConfigured = false;
  await using screen = await render(<LabsSettingsPanel />, { router: false });
  await typeAddress(screen.container, "http://t3.example.com");
  expect(screen.container.querySelector('[role="alert"]')).not.toBeNull();
  expect(state.update).not.toHaveBeenCalled();

  await typeAddress(screen.container, "https://t3.example.com/");
  expect(screen.container.querySelector('[role="alert"]')).toBeNull();
  expect(state.update).toHaveBeenCalledWith({ guestSharePublicBaseUrl: "https://t3.example.com" });

  await typeAddress(screen.container, "");
  expect(state.update).toHaveBeenCalledWith({ guestSharePublicBaseUrl: null });
});
