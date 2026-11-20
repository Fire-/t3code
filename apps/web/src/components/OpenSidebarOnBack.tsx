import { useBlocker } from "@tanstack/react-router";

import { isCommandPaletteOpen } from "../commandPaletteBus";
import { isModelPickerOpen } from "../modelPickerVisibility";
import { resolveThreadRouteTarget } from "../threadRoutes";
import { useSidebar } from "./ui/sidebar";

// Base UI marks an open popup with data-open; the closed sidebar sheet stays mounted without it.
const OPEN_OVERLAY_SELECTOR = ["dialog", "alertdialog", "menu", "listbox"]
  .map((role) => `[role="${role}"][data-open]`)
  .join(", ");

function isOverlayOpen(): boolean {
  return (
    isModelPickerOpen() ||
    isCommandPaletteOpen() ||
    document.querySelector(OPEN_OVERLAY_SELECTOR) !== null
  );
}

/** On a phone, back from a thread opens the thread list before it leaves the thread. */
export function OpenSidebarOnBack() {
  const { isMobile, openMobile, setOpenMobile } = useSidebar();
  useBlocker({
    shouldBlockFn: ({ action, current }) => {
      if (action !== "BACK" || !isMobile || openMobile) return false;
      const params: Readonly<Record<string, string | undefined>> = current.params;
      // Whatever is open over the page keeps its own back behavior.
      if (!resolveThreadRouteTarget(params) || isOverlayOpen()) return false;
      setOpenMobile(true);
      return true;
    },
    enableBeforeUnload: false,
  });
  return null;
}
