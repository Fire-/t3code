import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  PROVIDER_DISPLAY_NAMES,
  ProviderDriverKind,
  secretSettingsFields,
} from "@t3tools/contracts";
import { makeProviderClientRegistry } from "@t3tools/provider-core/client";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { droidClient } from "./client.ts";
import { DroidSettings } from "./settings.ts";

const BROWSER_SAFE_SPECIFIER =
  /^(effect\/[\w/]+|@t3tools\/contracts|@t3tools\/provider-core\/client|\.\/settings\.ts)$/;

const importSpecifiers = Effect.fn("importSpecifiers")(function* (file: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const source = yield* fileSystem.readFileString(
    yield* path.fromFileUrl(new URL(file, import.meta.url)),
  );
  return [...source.matchAll(/from\s+"([^"]+)"/g)].map((match) => match[1]!);
});

describe("droidClient", () => {
  it("[VAL-REG-002] names the driver droid and labels it Droid", () => {
    expect(droidClient.driverKind).toBe(ProviderDriverKind.make("droid"));
    expect(droidClient.label).toBe("Droid");
    expect(PROVIDER_DISPLAY_NAMES[droidClient.driverKind]).toBe(droidClient.label);
    expect(droidClient.settingsSchema).toBe(DroidSettings);
  });

  it("[VAL-REG-016] carries a plain-data icon", () => {
    const { icon } = droidClient;
    expect(icon?.viewBox).toBe("0 0 24 24");
    expect(icon?.fill.light).toMatch(/^#[0-9A-Fa-f]{6}$/);
    expect(icon?.fill.dark).toMatch(/^#[0-9A-Fa-f]{6}$/);
    expect(icon?.paths.length).toBeGreaterThan(0);
    expect(JSON.parse(JSON.stringify(icon))).toEqual(icon);
  });

  it("[VAL-REG-010] declares no secret settings field", () => {
    expect(secretSettingsFields(droidClient.settingsSchema)).toEqual([]);
  });

  it("[VAL-REG-016] registers once without clashing with another driver", () => {
    expect(makeProviderClientRegistry([droidClient]).get(droidClient.driverKind)).toBe(droidClient);
  });

  it.effect("[VAL-REG-016] imports only browser-safe modules from the client entry", () =>
    Effect.gen(function* () {
      for (const file of ["./client.ts", "./settings.ts"]) {
        const specifiers = yield* importSpecifiers(file);
        expect(specifiers.length).toBeGreaterThan(0);
        for (const specifier of specifiers) {
          expect(specifier).toMatch(BROWSER_SAFE_SPECIFIER);
          expect(specifier.startsWith("node:")).toBe(false);
          expect(specifier).not.toContain("/server");
        }
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
