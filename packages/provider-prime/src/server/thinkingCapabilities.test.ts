import { assert, describe, it } from "@effect/vitest";

import { thinkingCapabilitiesForPrimeModel } from "./thinkingCapabilities.ts";

describe("thinkingCapabilitiesForPrimeModel", () => {
  it("gives every choice its real level id and tags Prime's default", () => {
    const capabilities = thinkingCapabilitiesForPrimeModel(
      {
        reasoning: true,
        thinkingLevelMap: { off: null, xhigh: "extra_high", max: null },
      },
      "xhigh",
    );
    const descriptors = capabilities.optionDescriptors ?? [];
    const thinking = descriptors[0];
    assert.equal(thinking?.id, "thinking");
    assert.equal(thinking?.type, "select");
    if (thinking?.type !== "select") return;
    assert.deepEqual(
      thinking.options.map((option) => [option.id, option.label, option.isDefault === true]),
      [
        ["minimal", "Minimal", false],
        ["low", "Low", false],
        ["medium", "Medium", false],
        ["high", "High", false],
        ["xhigh", "Extra High", true],
      ],
    );
  });

  it("clamps Prime's default to each model's supported levels", () => {
    const capabilities = thinkingCapabilitiesForPrimeModel(
      {
        reasoning: true,
        thinkingLevelMap: { xhigh: "extra_high", max: null },
      },
      "max",
    );
    const thinking = capabilities.optionDescriptors?.[0];
    assert.equal(thinking?.type, "select");
    if (thinking?.type !== "select") return;
    assert.deepInclude(thinking.options, {
      id: "xhigh",
      label: "Extra High",
      isDefault: true,
    });
    assert.notInclude(
      thinking.options.map((option) => option.id),
      "max",
    );
  });

  it("[VAL-SET-005] offers GLM 5.3 only low/high/max and Ministral no thinking option", () => {
    // thinkingLevelMap values from a live get_available_models response.
    const glm = thinkingCapabilitiesForPrimeModel(
      {
        provider: "mistral",
        id: "zai-glm-5-3",
        reasoning: true,
        thinkingLevelMap: {
          off: null,
          minimal: null,
          low: "low",
          medium: null,
          high: "high",
          xhigh: null,
          max: "max",
        },
      },
      "off",
    );
    const thinking = glm.optionDescriptors?.[0];
    assert.equal(thinking?.id, "thinking");
    assert.equal(thinking?.type, "select");
    if (thinking?.type !== "select") return;
    assert.deepEqual(
      thinking.options.map((option) => option.id),
      ["low", "high", "max"],
    );
    // An unsupported saved default clamps to the nearest supported level, never off.
    assert.deepEqual(
      thinking.options.filter((option) => option.isDefault).map((option) => option.id),
      ["low"],
    );

    const ministral = thinkingCapabilitiesForPrimeModel(
      { provider: "mistral", id: "ministral-3b-latest", reasoning: false },
      "off",
    );
    assert.deepEqual(ministral.optionDescriptors ?? [], []);
  });
});
