import { describe, expect, it } from "@effect/vitest";
import {
  ProviderInstanceId,
  resolveProviderInstanceEnabled,
  ServerSettings,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import { PrimeSettings } from "./settings.ts";

const decodePrimeSettings = Schema.decodeUnknownSync(PrimeSettings);
const decodeServerSettings = Schema.decodeUnknownSync(ServerSettings);

const PRIME_ID = ProviderInstanceId.make("prime-agent");
const SAVED_CONFIG = {
  binaryPath: "/x/prime-agent",
  customModels: ["mistral/zai-glm-5-3", { slug: "a/b", name: "B" }],
};
const savedSelection = (options?: ReadonlyArray<{ id: string; value: string }>) => ({
  instanceId: "prime-agent",
  model: "mistral/zai-glm-5-3",
  ...(options ? { options } : {}),
});

describe("PrimeSettings", () => {
  it("[VAL-SET-001] decodes the saved Prime block with binaryPath and customModels unchanged", () => {
    const settings = decodePrimeSettings(SAVED_CONFIG);
    expect(settings.binaryPath).toBe("/x/prime-agent");
    expect(settings.customModels).toEqual(["mistral/zai-glm-5-3", { slug: "a/b", name: "B" }]);
    expect(settings.launchArgs).toBe("");
  });

  it("[VAL-SET-002] leaves Prime disabled when enabled is unset", () => {
    expect(decodePrimeSettings({}).enabled).toBe(false);
    expect(decodePrimeSettings(SAVED_CONFIG).enabled).toBe(false);
    expect(decodePrimeSettings({ enabled: true }).enabled).toBe(true);
  });

  it("[VAL-SET-001] decodes ServerSettings carrying the saved prime-agent instance as enabled", () => {
    const settings = decodeServerSettings({
      providerInstances: {
        "prime-agent": { driver: "prime-agent", enabled: true, config: SAVED_CONFIG },
      },
    });
    const instance = settings.providerInstances[PRIME_ID];
    expect(instance?.driver).toBe("prime-agent");
    expect(instance?.config).toEqual(SAVED_CONFIG);
    expect(resolveProviderInstanceEnabled(instance!)).toBe(true);
  });

  it("[VAL-SET-002] resolves a prime-agent instance without enabled as disabled", () => {
    const settings = decodeServerSettings({
      providerInstances: { "prime-agent": { driver: "prime-agent", config: {} } },
    });
    expect(resolveProviderInstanceEnabled(settings.providerInstances[PRIME_ID]!)).toBe(false);
  });

  it("[VAL-SET-003] decodes saved Prime selections that carry the overlay's reasoningEffort option", () => {
    const settings = decodeServerSettings({
      defaultModelSelection: savedSelection([{ id: "reasoningEffort", value: "high" }]),
      textGenerationModelSelection: savedSelection([{ id: "reasoningEffort", value: "low" }]),
      sourceControlWriterModelSelection: savedSelection(),
    });
    expect(settings.defaultModelSelection).toEqual(
      savedSelection([{ id: "reasoningEffort", value: "high" }]),
    );
    expect(settings.textGenerationModelSelection).toEqual(
      savedSelection([{ id: "reasoningEffort", value: "low" }]),
    );
    expect(settings.sourceControlWriterModelSelection).toEqual(savedSelection());
  });
});
