import { ProviderDriverKind } from "@t3tools/contracts";
import { defineProviderClient } from "@t3tools/provider-core/client";

import { PrimeSettings } from "./settings.ts";

export const primeClient = defineProviderClient({
  driverKind: ProviderDriverKind.make("prime-agent"),
  label: "Prime",
  settingsSchema: PrimeSettings,
  icon: {
    viewBox: "0 0 24 24",
    fill: { light: "#0F0F0F", dark: "#F5F5F5" },
    paths: [
      {
        d: "M12 1.5L21.5269 7.05V18.15L12 23.7L2.47311 18.15V7.05L12 1.5ZM12 4.366L4.97311 8.433V15.687L12 19.754L19.0269 15.687V8.433L12 4.366Z",
        fillRule: "evenodd",
      },
      { d: "M8.9 12.06A3.1 3.1 0 1 0 15.1 12.06A3.1 3.1 0 1 0 8.9 12.06Z" },
    ],
  },
});
