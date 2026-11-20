import { useBlocker } from "@tanstack/react-router";

import { isCommandPaletteOpen } from "../commandPaletteBus";
import { isModelPickerOpen } from "../modelPickerVisibility";
import { resolveThreadRouteTarget } from "../threadRoutes";
import { useSidebar } from "./ui/sidebar";

// Base UI marks an open popup with data-open, the open sidebar sheet included.
const OPEN_OVERLAY_SELECTOR = ["dialog", "alertdialog", "menu", "listbox"]
  .map((role) => `[role="${role}"][data-open]:not([data-sidebar="sidebar"])`)
  .join(", ");

function isOverlayOpen(): boolean {
  return (
    isModelPickerOpen() ||
    isCommandPaletteOpen() ||
    document.querySelector(OPEN_OVERLAY_SELECTOR) !== null
  );
}

/** On a phone, back closes the open thread list, and on a thread opens it, instead of leaving. */
export function ToggleSidebarOnBack() {
  const { isMobile, openMobile, setOpenMobile } = useSidebar();
  useBlocker({
    shouldBlockFn: ({ action, current }) => {
      // Whatever is open over the page keeps its own back behavior.
      if (action !== "BACK" || !isMobile || isOverlayOpen()) return false;
      if (!openMobile) {
        const params: Readonly<Record<string, string | undefined>> = current.params;
        if (!resolveThreadRouteTarget(params)) return false;
      }
      setOpenMobile(!openMobile);
      return true;
    },
    enableBeforeUnload: false,
  });
  return null;
}
