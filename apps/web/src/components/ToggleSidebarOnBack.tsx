import { useBlocker } from "@tanstack/react-router";

import { isCommandPaletteOpen } from "../commandPaletteBus";
import { dismissContextMenu, isContextMenuOpen } from "../contextMenuFallback";
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
  const { isMobile, openMobile, setOpenMobile, closeMobileWithoutFocusReturn } = useSidebar();
  useBlocker({
    shouldBlockFn: ({ action, current }) => {
      if (action !== "BACK" || !isMobile) return false;
      // The fallback context menu is plain DOM, so the open-overlay check cannot see it.
      if (isContextMenuOpen()) {
        dismissContextMenu(false);
        return true;
      }
      // Whatever else is open over the page keeps its own back behavior.
      if (isOverlayOpen()) return false;
      if (openMobile) {
        closeMobileWithoutFocusReturn();
        return true;
      }
      const params: Readonly<Record<string, string | undefined>> = current.params;
      if (!resolveThreadRouteTarget(params)) return false;
      setOpenMobile(true);
      return true;
    },
    enableBeforeUnload: false,
  });
  return null;
}
