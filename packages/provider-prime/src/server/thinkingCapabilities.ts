import { type ModelCapabilities, type ProviderOptionChoice } from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as Predicate from "effect/Predicate";

/**
 * Prime's full thinking ladder. Extra High (`xhigh`) and Max are opt-in per
 * model via `thinkingLevelMap`; advertising them globally makes
 * `set_thinking_level` fail on models that lack them.
 */
const PRIME_THINKING_LEVELS = ["off", "minimal", "low", "medium", "high", "xhigh", "max"] as const;

export type PrimeThinkingLevel = (typeof PRIME_THINKING_LEVELS)[number];

const PRIME_THINKING_LEVEL_LABELS: Record<PrimeThinkingLevel, string> = {
  off: "Off",
  minimal: "Minimal",
  low: "Low",
  medium: "Medium",
  high: "High",
  xhigh: "Extra High",
  max: "Max",
};

export const EMPTY_PRIME_MODEL_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

export function thinkingCapabilitiesForPrimeModel(
  model: unknown,
  defaultThinkingLevel: unknown,
): ModelCapabilities {
  const levels = supportedPrimeThinkingLevelsFromModel(model);
  if (levels.length === 0) return EMPTY_PRIME_MODEL_CAPABILITIES;
  const defaultLevel = clampPrimeThinkingLevel(defaultThinkingLevel, levels);
  return createModelCapabilities({
    optionDescriptors: [
      {
        id: "thinking",
        label: "Thinking",
        type: "select",
        options: levels.map(
          (level): ProviderOptionChoice => ({
            id: level,
            label: PRIME_THINKING_LEVEL_LABELS[level],
            ...(level === defaultLevel ? { isDefault: true } : {}),
          }),
        ),
      },
    ],
  });
}

function clampPrimeThinkingLevel(
  input: unknown,
  availableLevels: ReadonlyArray<PrimeThinkingLevel>,
): PrimeThinkingLevel | undefined {
  if (typeof input !== "string") return undefined;
  const requestedIndex = PRIME_THINKING_LEVELS.findIndex((level) => level === input);
  if (requestedIndex === -1) return undefined;
  const exact = availableLevels.find((level) => level === input);
  if (exact !== undefined) return exact;
  for (let index = requestedIndex + 1; index < PRIME_THINKING_LEVELS.length; index += 1) {
    const higher = availableLevels.find((level) => level === PRIME_THINKING_LEVELS[index]);
    if (higher !== undefined) return higher;
  }
  for (let index = requestedIndex - 1; index >= 0; index -= 1) {
    const lower = availableLevels.find((level) => level === PRIME_THINKING_LEVELS[index]);
    if (lower !== undefined) return lower;
  }
  return availableLevels[0];
}

export function supportedPrimeThinkingLevelsFromModel(
  model: unknown,
): ReadonlyArray<PrimeThinkingLevel> {
  if (recordField(model, "reasoning") !== true) return [];
  const thinkingLevelMap = thinkingLevelMapFromModel(model);
  return PRIME_THINKING_LEVELS.filter((level) => {
    const mapped = thinkingLevelMap?.[level];
    if (mapped === null) return false;
    if (level === "xhigh" || level === "max") return mapped !== undefined;
    return true;
  });
}

function thinkingLevelMapFromModel(model: unknown): Record<string, unknown> | undefined {
  const value = recordField(model, "thinkingLevelMap");
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  return value as Record<string, unknown>;
}

function recordField(input: unknown, key: string): unknown {
  return Predicate.isObject(input) ? input[key] : undefined;
}
