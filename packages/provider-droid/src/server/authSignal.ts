import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface DroidAuthSignalService {
  readonly authenticationFailed: Effect.Effect<void>;
  readonly authenticated: Effect.Effect<void>;
}

/**
 * How a turn tells the provider status whether Droid is signed in. The adapter fires
 * `authenticationFailed` when a turn ends `model_authentication_failed` and `authenticated` when a
 * later turn completes. The default does nothing, so an adapter built without a status keeps
 * working.
 */
export class DroidAuthSignal extends Context.Reference<DroidAuthSignalService>(
  "@t3tools/provider-droid/server/authSignal/DroidAuthSignal",
  {
    defaultValue: () => ({
      authenticationFailed: Effect.void,
      authenticated: Effect.void,
    }),
  },
) {}
