import { type EnvironmentId, managedCapabilityDenial } from "@t3tools/contracts";
import { Atom } from "effect/reactivity";

import { cloudLinkStateValueAtom } from "~/cloud/cloudLinkState";
import { environmentPresentations } from "~/state/presentation";
import { serverEnvironment } from "~/state/server";

/**
 * Whether this environment offers "Share with guest": its Guest links beta is on (Settings >
 * Labs) with a way for guests to arrive — the running T3 Connect tunnel or the configured public
 * link address. A policy that denies `connect` hides only the Connect path.
 */
export function canShareWithGuest(input: {
  readonly permitted: boolean;
  readonly betaEnabled: boolean;
  /** The configured public link address, or null when the machine uses T3 Connect instead. */
  readonly publicBaseUrl: string | null | undefined;
  /** The running tunnel's hostname, or undefined while the link state is unreadable. */
  readonly connectorHostname: string | undefined;
  /** Why a managed policy denies the `connect` capability, or undefined when it does not. */
  readonly connectDenial: string | undefined;
}): boolean {
  if (!input.permitted || !input.betaEnabled) return false;
  if (input.publicBaseUrl != null) return true;
  return input.connectorHostname !== undefined && input.connectDenial === undefined;
}

export const canShareWithGuestAtom = Atom.family((environmentId: EnvironmentId) =>
  Atom.make((get) => {
    const serverConfig = get(environmentPresentations.serverConfigsAtom).get(environmentId);
    const permitted = get(serverEnvironment.createGuestShare.permissionAtom(environmentId));
    const betaEnabled = serverConfig?.settings.guestSharingEnabled === true;
    const publicBaseUrl = serverConfig?.settings.guestSharePublicBaseUrl ?? null;
    // Reading the link state asks the environment, so skip it when the answer cannot matter.
    const connectorHostname =
      permitted && betaEnabled && publicBaseUrl === null
        ? get(cloudLinkStateValueAtom(environmentId))?.tunnelHostname
        : undefined;
    return canShareWithGuest({
      permitted,
      betaEnabled,
      publicBaseUrl,
      connectorHostname,
      connectDenial: managedCapabilityDenial(serverConfig?.managedPolicy, "connect"),
    });
  }).pipe(Atom.withLabel(`guest-share:available:${environmentId}`)),
);
