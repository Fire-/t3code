import { ProviderDriverKind } from "@t3tools/contracts";
import { defineProviderClient } from "@t3tools/provider-core/client";

import { DroidSettings } from "./settings.ts";

export const droidClient = defineProviderClient({
  driverKind: ProviderDriverKind.make("droid"),
  label: "Droid",
  settingsSchema: DroidSettings,
  icon: {
    viewBox: "0 0 24 24",
    fill: { light: "#0F0F0F", dark: "#F5F5F5" },
    paths: [
      {
        d: "M12 1a1.75 1.75 0 1 1 0 3.5a1.75 1.75 0 1 1 0-3.5ZM11.25 4.5h1.5V7h-1.5ZM6 7h12a3 3 0 0 1 3 3v7a3 3 0 0 1-3 3H6a3 3 0 0 1-3-3v-7a3 3 0 0 1 3-3ZM8.5 11.5a2 2 0 1 0 0 4a2 2 0 1 0 0-4ZM15.5 11.5a2 2 0 1 0 0 4a2 2 0 1 0 0-4Z",
        fillRule: "evenodd",
      },
    ],
  },
});
