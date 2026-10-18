/**
 * The settings t3 pushes into a Droid session. Droid fills every field the client leaves out from
 * the user's own `~/.factory/settings.json` (it can come back in spec mode on a non-default model),
 * so each frame that carries settings carries all of them, explicitly.
 */
import type { ModelSelection, RuntimeMode } from "@t3tools/contracts";
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

export interface DroidSessionSettings {
  readonly interactionMode: DroidInteractionMode;
  readonly autonomyLevel: DroidAutonomyLevel;
  readonly modelId: string;
  /** Absent when the selected model has no reasoning effort option. */
  readonly reasoningEffort: string | undefined;
}

export const droidSessionSettings = (input: {
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: "default" | "plan";
  readonly modelSelection: ModelSelection;
}): DroidSessionSettings => ({
  interactionMode: input.interactionMode === "plan" ? "spec" : "auto",
  autonomyLevel: droidAutonomyLevel(input.runtimeMode),
  modelId: input.modelSelection.model,
  reasoningEffort: getModelSelectionStringOptionValue(
    input.modelSelection,
    DROID_REASONING_EFFORT_OPTION_ID,
  ),
});

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
