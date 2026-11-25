import { describe, expect, it } from "vite-plus/test";

import { canShareWithGuest } from "./guestShareAvailability";

const open = { permitted: true, betaEnabled: true } as const;

describe("canShareWithGuest", () => {
  it("needs the beta on and a way for guests to arrive", () => {
    expect(
      canShareWithGuest({
        ...open,
        publicBaseUrl: null,
        connectorHostname: undefined,
        connectDenial: undefined,
      }),
    ).toBe(false);
    expect(
      canShareWithGuest({
        ...open,
        publicBaseUrl: null,
        connectorHostname: "prod-a1.t3coderelay.com",
        connectDenial: undefined,
      }),
    ).toBe(true);
    expect(
      canShareWithGuest({
        ...open,
        publicBaseUrl: "https://t3.example.com",
        connectorHostname: undefined,
        connectDenial: undefined,
      }),
    ).toBe(true);
  });

  it("keeps the beta switch and the permission authoritative", () => {
    for (const [permitted, betaEnabled] of [
      [false, true],
      [true, false],
    ] as const) {
      expect(
        canShareWithGuest({
          permitted,
          betaEnabled,
          publicBaseUrl: "https://t3.example.com",
          connectorHostname: "prod-a1.t3coderelay.com",
          connectDenial: undefined,
        }),
      ).toBe(false);
    }
  });

  it("applies a Connect policy denial only to the Connect path", () => {
    const connectDenial = "Example Corp's policy does not allow T3 Connect.";
    expect(
      canShareWithGuest({
        ...open,
        publicBaseUrl: null,
        connectorHostname: "prod-a1.t3coderelay.com",
        connectDenial,
      }),
    ).toBe(false);
    // A configured address does not arrive through T3 Connect, so the denial leaves it alone.
    expect(
      canShareWithGuest({
        ...open,
        publicBaseUrl: "https://t3.example.com",
        connectorHostname: undefined,
        connectDenial,
      }),
    ).toBe(true);
  });
});
