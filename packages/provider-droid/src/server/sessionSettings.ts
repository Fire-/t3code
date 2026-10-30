/**
 * The settings t3 pushes into a Droid session, and the record of what Droid is known to hold. Droid
 * fills every field the client leaves out from the user's own `~/.factory/settings.json` (it can
 * come back in spec mode on a non-default model), so each frame that carries settings carries all
 * of them, explicitly.
 */
import {
  DEFAULT_MODEL_BY_PROVIDER,
  ProviderDriverKind,
  RuntimeMode,
  type ModelSelection,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as Predicate from "effect/Predicate";

export type DroidAutonomyLevel = "off" | "low" | "medium" | "high";
export type DroidInteractionMode = "auto" | "spec";

const DROID_REASONING_EFFORT_OPTION_ID = "reasoningEffort";

const AUTONOMY_BY_RUNTIME_MODE = {
  "approval-required": "off",
  "auto-accept-edits": "low",
  auto: "medium",
  "full-access": "high",
} as const satisfies Record<RuntimeMode, DroidAutonomyLevel>;

const droidAutonomyLevel = (runtimeMode: RuntimeMode): DroidAutonomyLevel =>
  AUTONOMY_BY_RUNTIME_MODE[runtimeMode];

/**
 * The runtime mode a settings report puts the session at, when that is above `runtimeMode`.
 * Approving a prompt for the rest of the session raises Droid's autonomy to the prompt's tier.
 */
export const droidRaisedRuntimeMode = (
  runtimeMode: RuntimeMode,
  settings: unknown,
): RuntimeMode | undefined => {
  const level = Predicate.isObject(settings) ? settings["autonomyLevel"] : undefined;
  const reported = RuntimeMode.literals.find((mode) => AUTONOMY_BY_RUNTIME_MODE[mode] === level);
  return reported !== undefined &&
    RuntimeMode.literals.indexOf(reported) > RuntimeMode.literals.indexOf(runtimeMode)
    ? reported
    : undefined;
};

/**
 * Droid takes real model ids only: a selection that names no model, or the placeholder
 * `default`, is the model t3 configures as Droid's default. The literal only satisfies the
 * record's optional type.
 */
const DROID_DEFAULT_MODEL =
  DEFAULT_MODEL_BY_PROVIDER[ProviderDriverKind.make("droid")] ?? "claude-opus-5-5";

/** The router chooses its model per request and takes no reasoning effort. */
const DROID_ROUTER_MODEL_ID = "auto";

export const resolveDroidModelId = (model: string): string => {
  const trimmed = model.trim();
  return trimmed === "" || trimmed === "default" ? DROID_DEFAULT_MODEL : trimmed;
};

/**
 * The efforts a model entry offers and the one it starts at: its `defaultReasoningEffort` when it
 * lists that effort, else the first one listed. The router picks its own effort and offers none.
 * The model picker and the settings the adapter sends both read an entry through this, so what
 * t3 shows as the default is what t3 sends.
 */
export const droidReasoningEfforts = (entry: {
  readonly [key: string]: unknown;
}): { readonly efforts: ReadonlyArray<string>; readonly defaultEffort: string | undefined } => {
  const listed = entry["supportedReasoningEfforts"];
  if (entry["kind"] === "router" || !Array.isArray(listed)) {
    return { efforts: [], defaultEffort: undefined };
  }
  const efforts = listed.filter(Predicate.isString);
  const preferred = entry["defaultReasoningEffort"];
  return {
    efforts,
    defaultEffort:
      Predicate.isString(preferred) && efforts.includes(preferred) ? preferred : efforts[0],
  };
};

/** One entry of the model list `initialize_session` and `load_session` report. */
export interface DroidModelInfo {
  readonly id: string;
  readonly router: boolean;
  readonly efforts: ReadonlyArray<string>;
  readonly defaultEffort: string | undefined;
}

const nonEmptyString = (value: unknown): string | undefined =>
  Predicate.isString(value) && value.length > 0 ? value : undefined;

/** The models a session reports; `undefined` when the result carries no list. */
export const droidModelCatalog = (result: unknown): ReadonlyArray<DroidModelInfo> | undefined => {
  const listed = Predicate.isObject(result) ? result["availableModels"] : undefined;
  if (!Array.isArray(listed)) return undefined;
  return listed.flatMap((entry): ReadonlyArray<DroidModelInfo> => {
    if (!Predicate.isObject(entry)) return [];
    const id = nonEmptyString(entry["id"]) ?? nonEmptyString(entry["modelId"]);
    if (id === undefined) return [];
    return [{ id, router: entry["kind"] === "router", ...droidReasoningEfforts(entry) }];
  });
};

export interface DroidSessionSettings {
  readonly interactionMode: DroidInteractionMode;
  readonly autonomyLevel: DroidAutonomyLevel;
  readonly modelId: string;
  /** The router picks the model per request, so no request names a model and none can be a reroute. */
  readonly router: boolean;
  /** Absent when t3 has no effort to ask for; see `droidSessionSettings`. */
  readonly reasoningEffort: string | undefined;
}

/**
 * What t3 asks Droid for, from the thread's current selection alone. The effort is the selection's
 * own, else the selected model's catalog default from the model list the session reported. Nothing
 * remembers where an earlier effort came from, so the same selection always yields the same
 * settings. With no effort to ask for (the router, a model that lists none, or no model list yet)
 * the frame carries none and Droid keeps what it holds.
 */
export const droidSessionSettings = (input: {
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: "default" | "plan";
  readonly modelSelection: ModelSelection;
  /** The model list the session reported, when it reported one. */
  readonly models?: ReadonlyArray<DroidModelInfo> | undefined;
}): DroidSessionSettings => {
  const modelId = resolveDroidModelId(input.modelSelection.model);
  const listed = input.models?.find((model) => model.id === modelId);
  const router = modelId === DROID_ROUTER_MODEL_ID || listed?.router === true;
  const explicitEffort = getModelSelectionStringOptionValue(
    input.modelSelection,
    DROID_REASONING_EFFORT_OPTION_ID,
  );
  return {
    interactionMode: input.interactionMode === "plan" ? "spec" : "auto",
    autonomyLevel: droidAutonomyLevel(input.runtimeMode),
    modelId,
    router,
    reasoningEffort: router ? undefined : (explicitEffort ?? listed?.defaultEffort),
  };
};

/** The fields t3 sends in a settings frame. Droid's `settings` reports use the same names. */
const KNOWN_KEYS = [
  "interactionMode",
  "autonomyLevel",
  "modelId",
  "reasoningEffort",
  "specModeModelId",
  "specModeReasoningEffort",
] as const;

type DroidKnownKey = (typeof KNOWN_KEYS)[number];

/** What t3 knows Droid holds. A field nobody sent or reported is absent. */
export type DroidKnownSettings = Readonly<Partial<Record<DroidKnownKey, string>>>;

/** The settings fields of `initialize_session` and `update_session_settings`. */
export const droidSettingsParams = (settings: DroidSessionSettings): DroidKnownSettings => ({
  interactionMode: settings.interactionMode,
  autonomyLevel: settings.autonomyLevel,
  modelId: settings.modelId,
  ...(settings.reasoningEffort === undefined ? {} : { reasoningEffort: settings.reasoningEffort }),
  // Spec mode runs on its own model setting; keep it on the thread's model rather than whatever
  // the user's Droid settings name.
  ...(settings.interactionMode === "spec"
    ? {
        specModeModelId: settings.modelId,
        ...(settings.reasoningEffort === undefined
          ? {}
          : { specModeReasoningEffort: settings.reasoningEffort }),
      }
    : {}),
});

/**
 * True when sending `settings` would change nothing: every field the frame carries is what Droid
 * is known to hold. A field the frame leaves out (no effort to ask for) has nothing to differ.
 */
export const droidHoldsSettings = (
  known: DroidKnownSettings,
  settings: DroidSessionSettings,
): boolean => {
  const frame = droidSettingsParams(settings);
  return KNOWN_KEYS.every((key) => frame[key] === undefined || frame[key] === known[key]);
};

/**
 * `known` after Droid reported `settings` (an `initialize_session` or `load_session` result, or
 * a `settings_updated` notification). Fields the report leaves out stay as they were. A
 * notification never moves the interaction mode: t3 cancels `exit_spec_mode`, Droid stays in spec
 * mode whatever it announces afterwards, and t3 leaves spec mode only by sending a frame.
 */
export const withDroidReport = (
  known: DroidKnownSettings,
  settings: unknown,
  adopt: { readonly interactionMode: boolean },
): DroidKnownSettings => {
  if (!Predicate.isObject(settings)) return known;
  const next: Partial<Record<DroidKnownKey, string>> = { ...known };
  for (const key of KNOWN_KEYS) {
    const value = settings[key];
    if (Predicate.isString(value) && (key !== "interactionMode" || adopt.interactionMode)) {
      next[key] = value;
    }
  }
  return next;
};
