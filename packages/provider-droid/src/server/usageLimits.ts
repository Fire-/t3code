import type { ServerProviderUsageLimits, ServerProviderUsageWindow } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import {
  clampPercent,
  makeUnavailableUsageLimits,
  makeUsageLimits,
} from "@t3tools/provider-core/server/usageLimits";

const FACTORY_API_ORIGIN = "https://api.factory.ai";

const FactoryWindow = Schema.Struct({
  usedPercent: Schema.optional(Schema.Number),
  windowEnd: Schema.optional(Schema.NullOr(Schema.String)),
});

const FactoryBillingLimits = Schema.Struct({
  limits: Schema.optional(
    Schema.Struct({
      standard: Schema.optional(
        Schema.Struct({
          fiveHour: Schema.optional(FactoryWindow),
          weekly: Schema.optional(FactoryWindow),
          monthly: Schema.optional(FactoryWindow),
        }),
      ),
    }),
  ),
});

const FactoryComputeUsage = Schema.Struct({
  orgUsageMs: Schema.optional(Schema.Number),
  limitMs: Schema.optional(Schema.Number),
  periodEnd: Schema.optional(Schema.NullOr(Schema.String)),
});

const FACTORY_USAGE_LINK = {
  label: "Factory usage",
  url: "https://app.factory.ai/settings/usage",
} as const;

const STANDARD_WINDOWS = [
  { id: "fiveHour", kind: "session", label: "5-hour", windowDurationMins: 300 },
  { id: "weekly", kind: "weekly", label: "Weekly", windowDurationMins: 10_080 },
  { id: "monthly", kind: "monthly", label: "Monthly" },
] as const;

export function droidUsageResponsesToLimits(
  limits: typeof FactoryBillingLimits.Type | undefined,
  compute: typeof FactoryComputeUsage.Type | undefined,
  checkedAt: string,
): ServerProviderUsageLimits {
  const windows: ServerProviderUsageWindow[] = [];
  for (const definition of STANDARD_WINDOWS) {
    const window = limits?.limits?.standard?.[definition.id];
    if (window?.usedPercent === undefined || !Number.isFinite(window.usedPercent)) continue;
    windows.push({
      ...definition,
      usedPercent: clampPercent(window.usedPercent),
      ...(window.windowEnd ? { resetsAt: window.windowEnd } : {}),
    });
  }
  if (
    compute?.orgUsageMs !== undefined &&
    Number.isFinite(compute.orgUsageMs) &&
    compute.limitMs !== undefined &&
    compute.limitMs > 0
  ) {
    windows.push({
      id: "managedComputers",
      kind: "other",
      label: "Managed computers",
      usedPercent: clampPercent((compute.orgUsageMs / compute.limitMs) * 100),
      ...(compute.periodEnd ? { resetsAt: compute.periodEnd } : {}),
    });
  }
  return windows.length > 0
    ? { ...makeUsageLimits({ checkedAt, windows }), externalUsage: FACTORY_USAGE_LINK }
    : makeUnavailableUsageLimits({
        checkedAt,
        reason: "probeFailed",
        message: "Factory could not report usage limits.",
      });
}

export const readDroidUsageLimits = Effect.fn("readDroidUsageLimits")(function* (
  environment: NodeJS.ProcessEnv,
) {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const key = environment.FACTORY_API_KEY?.trim();
  if (!key) {
    return makeUnavailableUsageLimits({
      checkedAt,
      reason: "unsupported",
      message:
        "Add FACTORY_API_KEY to this provider's environment variables to show Factory usage.",
    });
  }
  return yield* Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const readEndpoint = <S extends Schema.Constraint>(schema: S, path: string) =>
      client
        .execute(
          HttpClientRequest.get(`${FACTORY_API_ORIGIN}${path}`).pipe(
            HttpClientRequest.bearerToken(key),
          ),
        )
        .pipe(
          Effect.flatMap(HttpClientResponse.filterStatusOk),
          Effect.flatMap(HttpClientResponse.schemaBodyJson(schema)),
          Effect.option,
          Effect.map(Option.getOrUndefined),
        );
    const [limits, compute] = yield* Effect.all(
      [
        readEndpoint(FactoryBillingLimits, "/api/billing/limits"),
        readEndpoint(FactoryComputeUsage, "/api/organization/compute-usage"),
      ],
      { concurrency: 2 },
    );
    return droidUsageResponsesToLimits(limits, compute, checkedAt);
  }).pipe(
    Effect.timeout("10 seconds"),
    Effect.orElseSucceed(() =>
      makeUnavailableUsageLimits({
        checkedAt,
        reason: "probeFailed",
        message: "Factory could not report usage limits.",
      }),
    ),
  );
});
