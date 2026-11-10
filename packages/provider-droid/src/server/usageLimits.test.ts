import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import { droidUsageResponsesToLimits, readDroidUsageLimits } from "./usageLimits.ts";

const checkedAt = "2026-11-05T00:00:00.000Z";

const fullBillingResponse = {
  usesTokenRateLimitsBilling: true,
  hasActiveSubscription: true,
  limits: {
    standard: {
      fiveHour: {
        usedPercent: 16,
        windowEnd: "2026-11-06T04:57:51.096Z",
        secondsRemaining: 15601,
      },
      weekly: {
        usedPercent: 5,
        windowEnd: "2026-11-12T06:50:00.793Z",
        secondsRemaining: 540731,
      },
      monthly: {
        usedPercent: 96,
        windowEnd: "2026-11-14T03:07:43.512Z",
        secondsRemaining: 700194,
      },
    },
    core: {
      fiveHour: { usedPercent: 0, windowEnd: null, secondsRemaining: null },
      weekly: { usedPercent: 0, windowEnd: null, secondsRemaining: null },
      monthly: { usedPercent: 0, windowEnd: null, secondsRemaining: null },
    },
  },
  overagePreference: "extraUsage",
  canManageOverage: true,
  extraUsageBalanceCents: 0,
  extraUsageAllowed: true,
};

const fullComputeResponse = {
  orgUsageMs: 6856947,
  limitMs: 288000000,
  overageAllowed: false,
  overagePreference: false,
  periodStart: "2026-10-13T20:23:51.000Z",
  periodEnd: "2026-11-13T08:00:00.000Z",
};

describe("Droid usage limits", () => {
  it("maps full billing and compute responses to four windows", () => {
    const limits = droidUsageResponsesToLimits(fullBillingResponse, fullComputeResponse, checkedAt);
    expect(limits.windows).toHaveLength(4);
    expect(limits.windows.slice(0, 3)).toEqual([
      {
        id: "fiveHour",
        kind: "session",
        label: "5-hour",
        usedPercent: 16,
        windowDurationMins: 300,
        resetsAt: "2026-11-06T04:57:51.096Z",
      },
      {
        id: "weekly",
        kind: "weekly",
        label: "Weekly",
        usedPercent: 5,
        windowDurationMins: 10080,
        resetsAt: "2026-11-12T06:50:00.793Z",
      },
      {
        id: "monthly",
        kind: "monthly",
        label: "Monthly",
        usedPercent: 96,
        resetsAt: "2026-11-14T03:07:43.512Z",
      },
    ]);
    const managedComputers = limits.windows[3];
    expect(managedComputers).toMatchObject({
      id: "managedComputers",
      kind: "other",
      label: "Managed computers",
      resetsAt: "2026-11-13T08:00:00.000Z",
    });
    expect(managedComputers?.usedPercent).toBeCloseTo(2.380884375);
    expect(managedComputers).not.toHaveProperty("windowDurationMins");
    expect(limits.externalUsage).toEqual({
      label: "Factory usage",
      url: "https://app.factory.ai/settings/usage",
    });
    expect(limits.unavailable).toBeUndefined();
  });

  it("omits resetsAt when Factory reports a null window end", () => {
    const limits = droidUsageResponsesToLimits(
      {
        limits: {
          standard: {
            fiveHour: { usedPercent: 16, windowEnd: null },
            weekly: { usedPercent: 5, windowEnd: "2026-11-12T06:50:00.793Z" },
            monthly: { usedPercent: 96, windowEnd: "2026-11-14T03:07:43.512Z" },
          },
        },
      },
      undefined,
      checkedAt,
    );
    expect(limits.windows).toHaveLength(3);
    const fiveHour = limits.windows.find((window) => window.id === "fiveHour");
    expect(fiveHour).toMatchObject({
      id: "fiveHour",
      kind: "session",
      label: "5-hour",
      usedPercent: 16,
      windowDurationMins: 300,
    });
    expect(fiveHour).not.toHaveProperty("resetsAt");
  });

  it.effect("requires FACTORY_API_KEY before any request", () =>
    Effect.gen(function* () {
      const limits = yield* readDroidUsageLimits({}).pipe(
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make(() => Effect.die("must not request limits without a key")),
        ),
      );
      expect(limits.unavailable?.reason).toBe("unsupported");
      expect(limits.unavailable?.message).toContain("FACTORY_API_KEY");
      expect(limits.windows).toEqual([]);
    }),
  );

  it.effect("keeps one endpoint's windows when the other fails", () =>
    Effect.gen(function* () {
      const requests: Array<{ readonly url: string; readonly authorization: string | undefined }> =
        [];
      const client = HttpClient.make((request) => {
        requests.push({ url: request.url, authorization: request.headers.authorization });
        return Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            request.url.endsWith("/api/billing/limits")
              ? new Response(JSON.stringify(fullBillingResponse), { status: 200 })
              : new Response(JSON.stringify({ error: "unavailable" }), { status: 500 }),
          ),
        );
      });
      const limits = yield* readDroidUsageLimits({ FACTORY_API_KEY: " test-key " }).pipe(
        Effect.provideService(HttpClient.HttpClient, client),
      );
      expect(limits.windows.map((window) => window.id)).toEqual(["fiveHour", "weekly", "monthly"]);
      expect(requests.map((request) => request.url).sort()).toEqual([
        "https://api.factory.ai/api/billing/limits",
        "https://api.factory.ai/api/organization/compute-usage",
      ]);
      expect(requests.map((request) => request.authorization)).toEqual([
        "Bearer test-key",
        "Bearer test-key",
      ]);
    }),
  );
});
