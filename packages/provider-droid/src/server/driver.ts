/**
 * Droid state (sessions, settings, auth) lives in the user's own `~/.factory`,
 * so continuation identity uses the default instance grouping.
 */
import { ProviderDriverKind } from "@t3tools/contracts";
import { DroidSettings } from "../settings.ts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import { ProviderDriverError } from "@t3tools/provider-core/server/errors";
import { DroidAdapterV2Driver, type DroidAdapterV2DriverEnv } from "./adapter.ts";
import { DroidAuthSignal } from "./authSignal.ts";
import {
  buildInitialDroidProviderSnapshot,
  checkDroidProviderStatus,
  enrichDroidSnapshot,
  withDroidAuthFailure,
} from "./status.ts";
import { makeDroidTextGeneration } from "./textGeneration.ts";
import { makeManagedServerProvider } from "@t3tools/provider-core/server/managedProvider";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "@t3tools/provider-core/server/driver";
import { withInstanceIdentity } from "@t3tools/provider-core/server/instanceIdentity";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import {
  makeCachedProviderMaintenanceResolution,
  makeManualOnlyProviderMaintenanceCapabilities,
} from "@t3tools/provider-core/server/maintenanceResolver";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "@t3tools/provider-core/server/snapshotSettings";

const decodeDroidSettings = Schema.decodeSync(DroidSettings);

const DRIVER_KIND = ProviderDriverKind.make("droid");
// Droid updates itself, so T3 offers no install or update command for it.
const MANUAL_MAINTENANCE = makeManualOnlyProviderMaintenanceCapabilities({
  provider: DRIVER_KIND,
  packageName: null,
});

export type DroidDriverEnv =
  | DroidAdapterV2DriverEnv
  | ProviderHost.ProviderHost
  | ChildProcessSpawner.ChildProcessSpawner
  | FileSystem.FileSystem
  | Path.Path
  | HttpClient.HttpClient
  | ProviderLatestVersions.ProviderLatestVersions;

export const DroidDriver: ProviderDriver<DroidSettings, DroidDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "Droid",
    supportsMultipleInstances: true,
  },
  configSchema: DroidSettings,
  defaultConfig: (): DroidSettings => decodeDroidSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const pathService = yield* Path.Path;
      const httpClient = yield* HttpClient.HttpClient;
      const latestVersions = yield* ProviderLatestVersions.ProviderLatestVersions;
      // Inherits the host environment, so FACTORY_API_KEY reaches Droid when it is set there.
      const processEnv = yield* mergeProviderInstanceEnvironment(environment);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        driverKind: DRIVER_KIND,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const effectiveConfig = { ...config, enabled } satisfies DroidSettings;
      const textGeneration = yield* makeDroidTextGeneration(effectiveConfig, processEnv);
      const resolveMaintenance = yield* makeCachedProviderMaintenanceResolution(
        Effect.succeed(MANUAL_MAINTENANCE),
      );

      // Set by a turn the adapter ends `model_authentication_failed`, cleared by the next completed
      // one. Either change re-probes so the snapshot shows it.
      const scope = yield* Effect.scope;
      const authFailed = yield* Ref.make(false);
      let refreshSnapshot: Effect.Effect<void> = Effect.void;
      const setAuthFailed = (next: boolean) =>
        Ref.getAndSet(authFailed, next).pipe(
          Effect.flatMap((previous) =>
            previous === next
              ? Effect.void
              : Effect.suspend(() => refreshSnapshot).pipe(
                  Effect.ignoreCause({ log: true }),
                  Effect.forkIn(scope),
                  Effect.asVoid,
                ),
          ),
        );
      const authSignal = {
        authenticationFailed: setAuthFailed(true),
        authenticated: setAuthFailed(false),
      };

      const orchestrationAdapter = yield* DroidAdapterV2Driver.create({
        instanceId,
        displayName,
        accentColor,
        environment,
        enabled,
        config,
      }).pipe(
        Effect.provideService(DroidAuthSignal, authSignal),
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: "Failed to build Droid orchestration adapter.",
              cause,
            }),
        ),
      );

      const snapshotSettings = yield* makeProviderSnapshotSettingsSource(effectiveConfig);
      const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<DroidSettings>>({
        resolveMaintenance,
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          buildInitialDroidProviderSnapshot(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider: checkDroidProviderStatus(effectiveConfig, processEnv).pipe(
          Effect.flatMap((draft) =>
            Ref.get(authFailed).pipe(
              Effect.map((failed) => (failed ? withDroidAuthFailure(draft) : draft)),
            ),
          ),
          Effect.map(stampIdentity),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.provideService(Path.Path, pathService),
        ),
        enrichSnapshot: ({ settings, snapshot: currentSnapshot, publishSnapshot }) =>
          resolveMaintenance().pipe(
            Effect.flatMap((maintenanceCapabilities) =>
              enrichDroidSnapshot({
                snapshot: currentSnapshot,
                maintenanceCapabilities,
                enableProviderUpdateChecks: settings.enableProviderUpdateChecks,
                publishSnapshot,
              }),
            ),
            Effect.provideService(HttpClient.HttpClient, httpClient),
            Effect.provideService(ProviderLatestVersions.ProviderLatestVersions, latestVersions),
          ),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: "Failed to build Droid snapshot.",
              cause,
            }),
        ),
      );

      refreshSnapshot = snapshot.refresh.pipe(Effect.asVoid);

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        orchestrationAdapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
};
