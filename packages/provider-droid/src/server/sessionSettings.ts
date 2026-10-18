/**
 * The settings t3 pushes into a Droid session. Droid fills every field the client leaves out from
 * the user's own `~/.factory/settings.json` (it can come back in spec mode on a non-default model),
 * so each frame that carries settings carries all of them, explicitly.
 */
import {
  DEFAULT_MODEL_BY_PROVIDER,
  ProviderDriverKind,
  type ModelSelection,
  type RuntimeMode,
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

/** One entry of the model list `initialize_session` and `load_session` report. */
export interface DroidModelInfo {
  readonly id: string;
  readonly router: boolean;
  readonly supportedReasoningEfforts: ReadonlyArray<string>;
  readonly defaultReasoningEffort: string | undefined;
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
    const efforts = entry["supportedReasoningEfforts"];
    return [
      {
        id,
        router: entry["kind"] === "router",
        supportedReasoningEfforts: Array.isArray(efforts) ? efforts.filter(Predicate.isString) : [],
        defaultReasoningEffort: nonEmptyString(entry["defaultReasoningEffort"]),
      },
    ];
  });
};

export interface DroidSessionSettings {
  readonly interactionMode: DroidInteractionMode;
  readonly autonomyLevel: DroidAutonomyLevel;
  readonly modelId: string;
  /** The router picks the model per request, so no request names a model and none can be a reroute. */
  readonly router: boolean;
  /** Absent when the selected model has no reasoning effort option, and for the router. */
  readonly reasoningEffort: string | undefined;
}

export const droidSessionSettings = (input: {
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: "default" | "plan";
  readonly modelSelection: ModelSelection;
  /** The model list the session reported, when it reported one. */
  readonly models?: ReadonlyArray<DroidModelInfo> | undefined;
  /** The settings the session holds now. */
  readonly previous?: DroidSessionSettings | undefined;
}): DroidSessionSettings => {
  const modelId = resolveDroidModelId(input.modelSelection.model);
  const listed = input.models?.find((model) => model.id === modelId);
  const router = modelId === DROID_ROUTER_MODEL_ID || listed?.router === true;
  const chosenEffort = getModelSelectionStringOptionValue(
    input.modelSelection,
    DROID_REASONING_EFFORT_OPTION_ID,
  );
  return {
    interactionMode: input.interactionMode === "plan" ? "spec" : "auto",
    autonomyLevel: droidAutonomyLevel(input.runtimeMode),
    modelId,
    router,
    // The router left no effort behind that the new model could inherit, so a model entered
    // from it without a chosen effort starts at its own default.
    reasoningEffort: router
      ? undefined
      : (chosenEffort ??
        (input.previous?.router === true ? listed?.defaultReasoningEffort : undefined)),
  };
};

/** The settings fields of `initialize_session` and `update_session_settings`. */
export const droidSettingsParams = (settings: DroidSessionSettings) => ({
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

export const sameDroidSessionSettings = (
  left: DroidSessionSettings,
  right: DroidSessionSettings,
): boolean =>
  left.interactionMode === right.interactionMode &&
  left.autonomyLevel === right.autonomyLevel &&
  left.modelId === right.modelId &&
  left.reasoningEffort === right.reasoningEffort;

const AUTONOMY_LEVELS = [
  "off",
  "low",
  "medium",
  "high",
] as const satisfies ReadonlyArray<DroidAutonomyLevel>;

const isDroidAutonomyLevel = (value: unknown): value is DroidAutonomyLevel =>
  AUTONOMY_LEVELS.some((level) => level === value);

/** The autonomy level a `settings_updated` notification reports, if it reports one. */
export const echoedDroidAutonomy = (notification: {
  readonly [key: string]: unknown;
}): DroidAutonomyLevel | undefined => {
  const settings = notification["settings"];
  const level = Predicate.isObject(settings) ? settings["autonomyLevel"] : undefined;
  return isDroidAutonomyLevel(level) ? level : undefined;
};
