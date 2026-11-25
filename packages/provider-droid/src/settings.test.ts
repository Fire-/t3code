import { describe, expect, it } from "@effect/vitest";
import {
  ProviderInstanceId,
  resolveProviderInstanceEnabled,
  ServerSettings,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import { DroidSettings } from "./settings.ts";

const decodeDroidSettings = Schema.decodeUnknownSync(DroidSettings);
const decodeServerSettings = Schema.decodeUnknownSync(ServerSettings);

const DROID_ID = ProviderInstanceId.make("droid");
const SAVED_CONFIG = { binaryPath: "droid", customModels: [] };
const SAVED_INSTANCE = { driver: "droid", enabled: true, config: SAVED_CONFIG };

const formAnnotation = (field: keyof typeof DroidSettings.fields) =>
  Schema.resolveAnnotationsKey(DroidSettings.fields[field])?.providerSettingsForm;

describe("DroidSettings", () => {
  it("[VAL-REG-005] decodes an empty config to binaryPath droid, no custom models, disabled", () => {
    expect(decodeDroidSettings({})).toEqual({
      enabled: false,
      binaryPath: "droid",
      customModels: [],
    });
  });

  it("[VAL-REG-005] falls back to the default binary when the saved path is blank", () => {
    expect(decodeDroidSettings({ binaryPath: "  " }).binaryPath).toBe("droid");
    expect(decodeDroidSettings({ binaryPath: " /opt/droid " }).binaryPath).toBe("/opt/droid");
  });

  it("[VAL-REG-005] shows binaryPath in the form and hides enabled and customModels", () => {
    expect(formAnnotation("binaryPath")?.hidden).not.toBe(true);
    expect(formAnnotation("binaryPath")?.placeholder).toBe("droid");
    expect(formAnnotation("customModels")?.hidden).toBe(true);
    expect(formAnnotation("enabled")?.hidden).toBe(true);
    expect(Schema.resolveAnnotations(DroidSettings)?.providerSettingsFormSchema?.order).toEqual([
      "binaryPath",
    ]);
  });

  it("[VAL-REG-005] adds no required key and no key beyond the saved block", () => {
    expect(Object.keys(DroidSettings.fields).toSorted()).toEqual([
      "binaryPath",
      "customModels",
      "enabled",
    ]);
    expect(() => decodeDroidSettings({})).not.toThrow();
  });

  it("[VAL-REG-003] decodes the saved config unchanged and keeps the explicit enabled flag", () => {
    const settings = decodeDroidSettings({ enabled: true, ...SAVED_CONFIG });
    expect(settings).toEqual({ enabled: true, binaryPath: "droid", customModels: [] });
  });

  it("[VAL-REG-004] leaves Droid disabled when enabled is unset", () => {
    expect(decodeDroidSettings(SAVED_CONFIG).enabled).toBe(false);
    expect(decodeDroidSettings({ enabled: true }).enabled).toBe(true);
  });

  it("[VAL-REG-003] decodes ServerSettings carrying the saved droid instance as enabled", () => {
    const settings = decodeServerSettings({ providerInstances: { droid: SAVED_INSTANCE } });
    const instance = settings.providerInstances[DROID_ID];
    expect(instance?.driver).toBe("droid");
    expect(instance?.enabled).toBe(true);
    expect(instance?.config).toEqual(SAVED_CONFIG);
    expect(resolveProviderInstanceEnabled(instance!)).toBe(true);
  });

  it("[VAL-REG-004] resolves a droid instance without enabled, and a missing block, as disabled", () => {
    const withoutFlag = decodeServerSettings({
      providerInstances: { droid: { driver: "droid", config: {} } },
    });
    expect(resolveProviderInstanceEnabled(withoutFlag.providerInstances[DROID_ID]!)).toBe(false);
    expect(decodeServerSettings({}).providerInstances[DROID_ID]).toBeUndefined();
  });
});
