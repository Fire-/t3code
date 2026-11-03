import { assert, describe, it } from "@effect/vitest";
import {
  DEFAULT_MODEL_BY_PROVIDER,
  ProviderDriverKind,
  ProviderInstanceId,
  type ModelSelection,
} from "@t3tools/contracts";

import {
  droidHoldsSettings,
  droidModelCatalog,
  droidReasoningEfforts,
  droidSessionSettings,
  droidSettingsParams,
  withDroidReport,
  type DroidModelInfo,
} from "./sessionSettings.ts";
import { buildDroidModelsFromListModels } from "./status.ts";

const DEFAULT_MODEL = DEFAULT_MODEL_BY_PROVIDER[ProviderDriverKind.make("droid")];

const selected = (model: string, effort?: string): ModelSelection => ({
  instanceId: ProviderInstanceId.make("droid"),
  model,
  ...(effort === undefined ? {} : { options: [{ id: "reasoningEffort", value: effort }] }),
});

const settingsFor = (
  modelSelection: ModelSelection,
  extra: Partial<Parameters<typeof droidSessionSettings>[0]> = {},
) =>
  droidSessionSettings({
    runtimeMode: "approval-required",
    interactionMode: "default",
    modelSelection,
    ...extra,
  });

const LISTED: ReadonlyArray<DroidModelInfo> = [
  { id: "auto", router: true, efforts: [], defaultEffort: undefined },
  {
    id: "claude-opus-5-5",
    router: false,
    efforts: ["low", "medium", "high"],
    defaultEffort: "medium",
  },
  { id: "plain-model", router: false, efforts: [], defaultEffort: undefined },
];

describe("real model ids [VAL-HIST-012]", () => {
  it("[VAL-HIST-012] a default selection resolves to the configured default model id", () => {
    assert.isString(DEFAULT_MODEL);
    for (const model of ["default", "", "  default  "]) {
      const settings = settingsFor(selected(model, "high"));
      assert.strictEqual(settings.modelId, DEFAULT_MODEL);
      assert.notInclude(JSON.stringify(droidSettingsParams(settings)), '"default"');
    }
  });

  it("[VAL-HIST-012] a named model is sent as it is", () => {
    assert.strictEqual(settingsFor(selected("gpt-5-codex", "low")).modelId, "gpt-5-codex");
  });
});

describe("the router model [VAL-HIST-012]", () => {
  it("[VAL-HIST-012] the router gets no reasoning effort, in either mode", () => {
    for (const interactionMode of ["default", "plan"] as const) {
      const settings = settingsFor(selected("auto", "high"), { interactionMode });
      assert.strictEqual(settings.modelId, "auto");
      assert.strictEqual(settings.reasoningEffort, undefined);
      const params = droidSettingsParams(settings);
      assert.strictEqual(params.modelId, "auto");
      assert.notProperty(params, "reasoningEffort");
      assert.notProperty(params, "specModeReasoningEffort");
    }
  });

  it("[VAL-HIST-012] the model list can name another router", () => {
    const models = [{ ...LISTED[0]!, id: "router-x" }];
    assert.strictEqual(
      settingsFor(selected("router-x", "high"), { models }).reasoningEffort,
      undefined,
    );
    assert.strictEqual(settingsFor(selected("router-x", "high")).reasoningEffort, "high");
  });
});

describe("the effort sent to Droid [VAL-HIST-012]", () => {
  it("[VAL-HIST-012] an explicit effort wins over the model's catalog default", () => {
    assert.strictEqual(
      settingsFor(selected("claude-opus-5-5", "high"), { models: LISTED }).reasoningEffort,
      "high",
    );
  });

  it("[VAL-HIST-012] without an explicit effort it is the model's catalog default", () => {
    assert.strictEqual(
      settingsFor(selected("claude-opus-5-5"), { models: LISTED }).reasoningEffort,
      "medium",
    );
  });

  it("[VAL-HIST-012] a model that lists no efforts, the router, and an unknown list send none", () => {
    assert.strictEqual(
      settingsFor(selected("plain-model"), { models: LISTED }).reasoningEffort,
      undefined,
    );
    assert.strictEqual(
      settingsFor(selected("auto"), { models: LISTED }).reasoningEffort,
      undefined,
    );
    assert.strictEqual(settingsFor(selected("claude-opus-5-5")).reasoningEffort, undefined);
    assert.strictEqual(
      settingsFor(selected("not-listed"), { models: LISTED }).reasoningEffort,
      undefined,
    );
  });

  it("[VAL-HIST-012] the same selection always gives the same settings, whatever came before", () => {
    const direct = settingsFor(selected("claude-opus-5-5"), { models: LISTED });
    const again = settingsFor(selected("claude-opus-5-5"), { models: LISTED });
    assert.deepStrictEqual(direct, again);
    assert.deepStrictEqual(
      Object.keys(direct).toSorted(),
      ["autonomyLevel", "interactionMode", "modelId", "reasoningEffort", "router"],
      "the settings hold no record of where the effort came from",
    );
  });

  it("[VAL-HIST-012] spec mode carries the same effort in both effort fields", () => {
    assert.deepStrictEqual(
      droidSettingsParams(
        settingsFor(selected("claude-opus-5-5"), { models: LISTED, interactionMode: "plan" }),
      ),
      {
        interactionMode: "spec",
        autonomyLevel: "off",
        modelId: "claude-opus-5-5",
        reasoningEffort: "medium",
        specModeModelId: "claude-opus-5-5",
        specModeReasoningEffort: "medium",
      },
    );
    assert.deepStrictEqual(
      droidSettingsParams(
        settingsFor(selected("claude-opus-5-5", "high"), {
          models: LISTED,
          interactionMode: "plan",
        }),
      ),
      {
        interactionMode: "spec",
        autonomyLevel: "off",
        modelId: "claude-opus-5-5",
        reasoningEffort: "high",
        specModeModelId: "claude-opus-5-5",
        specModeReasoningEffort: "high",
      },
    );
  });
});

describe("the default effort rule is shared with the picker [VAL-HIST-012]", () => {
  const entries: ReadonlyArray<Record<string, unknown>> = [
    {
      id: "listed",
      supportedReasoningEfforts: ["low", "medium", "high"],
      defaultReasoningEffort: "high",
    },
    { id: "unlisted", supportedReasoningEfforts: ["off", "high"], defaultReasoningEffort: "ultra" },
    { id: "absent", supportedReasoningEfforts: ["xhigh", "max"] },
    { id: "mixed", supportedReasoningEfforts: ["low", 7, "high"], defaultReasoningEffort: 7 },
    { id: "empty", supportedReasoningEfforts: [] },
    { id: "none-listed" },
    {
      id: "auto",
      kind: "router",
      supportedReasoningEfforts: ["none"],
      defaultReasoningEffort: "none",
    },
  ];

  it("[VAL-HIST-012] the picker's default effort is the adapter's default effort for every entry", () => {
    const catalog = droidModelCatalog({ availableModels: entries });
    const pickerModels = buildDroidModelsFromListModels({ models: entries });
    assert.strictEqual(catalog?.length, entries.length);
    for (const info of catalog ?? []) {
      const descriptor = pickerModels.find((model) => model.slug === info.id)?.capabilities
        ?.optionDescriptors?.[0];
      const pickerDefault = descriptor?.type === "select" ? descriptor.currentValue : undefined;
      assert.strictEqual(pickerDefault, info.defaultEffort, `default effort of ${info.id}`);
    }
  });

  it("[VAL-HIST-012] the default is the entry's own when it lists it, else the first listed effort", () => {
    assert.deepStrictEqual(
      droidReasoningEfforts({
        supportedReasoningEfforts: ["low", "medium", "high"],
        defaultReasoningEffort: "high",
      }),
      { efforts: ["low", "medium", "high"], defaultEffort: "high" },
    );
    assert.strictEqual(
      droidReasoningEfforts({
        supportedReasoningEfforts: ["off", "high"],
        defaultReasoningEffort: "ultra",
      }).defaultEffort,
      "off",
    );
  });

  it("[VAL-HIST-012] the router, a missing list, and an empty list offer no effort", () => {
    const none = { efforts: [], defaultEffort: undefined };
    assert.deepStrictEqual(
      droidReasoningEfforts({ kind: "router", supportedReasoningEfforts: ["none"] }),
      none,
    );
    assert.deepStrictEqual(droidReasoningEfforts({}), none);
    assert.deepStrictEqual(droidReasoningEfforts({ supportedReasoningEfforts: [] }), none);
  });
});

describe("what Droid is known to hold [VAL-HIST-011]", () => {
  const held = droidSettingsParams(settingsFor(selected("claude-opus-5-5", "high")));

  it("[VAL-HIST-011] settings Droid already holds need no frame", () => {
    assert.isTrue(droidHoldsSettings(held, settingsFor(selected("claude-opus-5-5", "high"))));
  });

  it("[VAL-HIST-011] a different model, effort, autonomy, or interaction mode needs a frame", () => {
    const base = { models: LISTED };
    assert.isFalse(droidHoldsSettings(held, settingsFor(selected("plain-model"), base)));
    assert.isFalse(droidHoldsSettings(held, settingsFor(selected("claude-opus-5-5", "low"), base)));
    assert.isFalse(
      droidHoldsSettings(
        held,
        settingsFor(selected("claude-opus-5-5", "high"), { runtimeMode: "full-access" }),
      ),
    );
    assert.isFalse(
      droidHoldsSettings(
        held,
        settingsFor(selected("claude-opus-5-5", "high"), { interactionMode: "plan" }),
      ),
    );
  });

  it("[VAL-HIST-011] a cleared effort needs a frame carrying the catalog default, none when held", () => {
    const cleared = settingsFor(selected("claude-opus-5-5"), { models: LISTED });
    assert.strictEqual(cleared.reasoningEffort, "medium");
    assert.isFalse(droidHoldsSettings(held, cleared));
    assert.isTrue(
      droidHoldsSettings(
        droidSettingsParams(settingsFor(selected("claude-opus-5-5", "medium"))),
        cleared,
      ),
    );
  });

  it("[VAL-HIST-011] nothing known needs a frame, and so does a changed spec model or effort", () => {
    assert.isFalse(droidHoldsSettings({}, settingsFor(selected("claude-opus-5-5", "high"))));
    const spec = settingsFor(selected("claude-opus-5-5", "high"), { interactionMode: "plan" });
    const heldSpec = droidSettingsParams(spec);
    assert.isTrue(droidHoldsSettings(heldSpec, spec));
    assert.isFalse(
      droidHoldsSettings({ ...heldSpec, specModeReasoningEffort: "low" }, spec),
      "Droid swapped the spec effort",
    );
    assert.isFalse(
      droidHoldsSettings({ ...heldSpec, specModeModelId: "gpt-5-codex" }, spec),
      "Droid swapped the spec model",
    );
  });

  it("[VAL-HIST-011] the spec fields only matter in spec mode", () => {
    const auto = settingsFor(selected("claude-opus-5-5", "high"));
    assert.isTrue(
      droidHoldsSettings({ ...droidSettingsParams(auto), specModeModelId: "elsewhere" }, auto),
    );
  });

  it("[VAL-HIST-011] an effort Droid fills in where t3 sends none is not a difference", () => {
    const router = settingsFor(selected("auto"), { models: LISTED });
    assert.isTrue(
      droidHoldsSettings({ ...droidSettingsParams(router), reasoningEffort: "medium" }, router),
    );
    const plain = settingsFor(selected("plain-model"), { models: LISTED });
    assert.isTrue(
      droidHoldsSettings({ ...droidSettingsParams(plain), reasoningEffort: "medium" }, plain),
    );
  });

  it("[VAL-HIST-011] a report overlays the fields it carries and keeps the rest", () => {
    const next = withDroidReport(
      held,
      { autonomyLevel: "low", reasoningEffort: "medium", compactionThresholdCheckEnabled: true },
      { interactionMode: true },
    );
    assert.deepStrictEqual(next, { ...held, autonomyLevel: "low", reasoningEffort: "medium" });
  });

  it("[VAL-HIST-011] a report reads strings only, and a report that is not an object changes nothing", () => {
    assert.deepStrictEqual(
      withDroidReport(held, { modelId: 7, reasoningEffort: null }, { interactionMode: true }),
      held,
    );
    assert.strictEqual(withDroidReport(held, undefined, { interactionMode: true }), held);
    assert.strictEqual(withDroidReport(held, "settings", { interactionMode: true }), held);
  });

  it("[VAL-HIST-011] only a response moves the interaction mode, never a notification", () => {
    const report = { interactionMode: "spec", autonomyLevel: "high" };
    assert.deepStrictEqual(withDroidReport(held, report, { interactionMode: false }), {
      ...held,
      autonomyLevel: "high",
    });
    assert.deepStrictEqual(withDroidReport(held, report, { interactionMode: true }), {
      ...held,
      interactionMode: "spec",
      autonomyLevel: "high",
    });
  });
});

describe("droidModelCatalog", () => {
  it("[VAL-HIST-012] reads the model list a session reports, router kind and efforts included", () => {
    assert.deepStrictEqual(
      droidModelCatalog({
        availableModels: [
          {
            id: "auto",
            modelId: "auto",
            kind: "router",
            supportedReasoningEfforts: ["none"],
            defaultReasoningEffort: "none",
          },
          {
            modelId: "claude-opus-5-5",
            supportedReasoningEfforts: ["low", 7, "high"],
            defaultReasoningEffort: "high",
          },
          { id: "bare" },
          { displayName: "no id" },
          "junk",
        ],
      }),
      [
        { id: "auto", router: true, efforts: [], defaultEffort: undefined },
        { id: "claude-opus-5-5", router: false, efforts: ["low", "high"], defaultEffort: "high" },
        { id: "bare", router: false, efforts: [], defaultEffort: undefined },
      ],
    );
  });

  it("[VAL-HIST-012] a result without a model list reports none", () => {
    assert.strictEqual(droidModelCatalog({}), undefined);
    assert.strictEqual(droidModelCatalog({ availableModels: "nope" }), undefined);
    assert.strictEqual(droidModelCatalog(undefined), undefined);
  });
});
