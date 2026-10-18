import { assert, describe, it } from "@effect/vitest";
import {
  DEFAULT_MODEL_BY_PROVIDER,
  ProviderDriverKind,
  ProviderInstanceId,
  type ModelSelection,
} from "@t3tools/contracts";

import {
  droidModelCatalog,
  droidSessionSettings,
  droidSettingsParams,
  sameDroidSessionSettings,
  type DroidModelInfo,
} from "./sessionSettings.ts";

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
  {
    id: "auto",
    router: true,
    supportedReasoningEfforts: ["none"],
    defaultReasoningEffort: "none",
  },
  {
    id: "claude-opus-5-5",
    router: false,
    supportedReasoningEfforts: ["low", "medium", "high"],
    defaultReasoningEffort: "medium",
  },
  {
    id: "plain-model",
    router: false,
    supportedReasoningEfforts: [],
    defaultReasoningEffort: undefined,
  },
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

  it("[VAL-HIST-012] leaving the router sends the chosen effort, else the model's default effort", () => {
    const previous = settingsFor(selected("auto"), { models: LISTED });
    assert.strictEqual(
      settingsFor(selected("claude-opus-5-5", "high"), { models: LISTED, previous })
        .reasoningEffort,
      "high",
    );
    assert.strictEqual(
      settingsFor(selected("claude-opus-5-5"), { models: LISTED, previous }).reasoningEffort,
      "medium",
    );
  });

  it("[VAL-HIST-012] no default effort is invented when the target lists none or the list is unknown", () => {
    const previous = settingsFor(selected("auto"));
    assert.strictEqual(
      settingsFor(selected("plain-model"), { models: LISTED, previous }).reasoningEffort,
      undefined,
    );
    assert.strictEqual(
      settingsFor(selected("claude-opus-5-5"), { previous }).reasoningEffort,
      undefined,
    );
  });

  it("[VAL-HIST-012] a model that follows another model with no chosen effort carries no default effort", () => {
    const previous = settingsFor(selected("plain-model", "low"), { models: LISTED });
    assert.strictEqual(
      settingsFor(selected("claude-opus-5-5"), { models: LISTED, previous }).reasoningEffort,
      undefined,
    );
  });
});

describe("the restored default effort stays while the selection does [VAL-HIST-012]", () => {
  const restored = () =>
    settingsFor(selected("claude-opus-5-5"), {
      models: LISTED,
      previous: settingsFor(selected("auto"), { models: LISTED }),
    });

  it("[VAL-HIST-012] an identical selection computes the same settings, so no frame follows", () => {
    const first = restored();
    assert.strictEqual(first.reasoningEffort, "medium");
    const next = settingsFor(selected("claude-opus-5-5"), { models: LISTED, previous: first });
    assert.strictEqual(next.reasoningEffort, "medium");
    assert.isTrue(sameDroidSessionSettings(first, next));
  });

  it("[VAL-HIST-012] spec mode and the implement turn after it carry the restored effort", () => {
    const first = restored();
    const spec = settingsFor(selected("claude-opus-5-5"), {
      models: LISTED,
      previous: first,
      interactionMode: "plan",
    });
    assert.deepStrictEqual(droidSettingsParams(spec), {
      interactionMode: "spec",
      autonomyLevel: "off",
      modelId: "claude-opus-5-5",
      reasoningEffort: "medium",
      specModeModelId: "claude-opus-5-5",
      specModeReasoningEffort: "medium",
    });
    const implement = settingsFor(selected("claude-opus-5-5"), { models: LISTED, previous: spec });
    assert.strictEqual(implement.reasoningEffort, "medium");
    assert.strictEqual(implement.interactionMode, "auto");
  });

  it("[VAL-HIST-012] a chosen effort replaces it, and a later turn without one does not bring it back", () => {
    const chosen = settingsFor(selected("claude-opus-5-5", "high"), {
      models: LISTED,
      previous: restored(),
    });
    assert.strictEqual(chosen.reasoningEffort, "high");
    assert.strictEqual(
      settingsFor(selected("claude-opus-5-5"), { models: LISTED, previous: chosen })
        .reasoningEffort,
      undefined,
    );
  });

  it("[VAL-HIST-012] another model does not inherit it", () => {
    assert.strictEqual(
      settingsFor(selected("plain-model"), { models: LISTED, previous: restored() })
        .reasoningEffort,
      undefined,
    );
  });

  it("[VAL-HIST-012] a model that was never entered from the router stays without an effort", () => {
    const fresh = settingsFor(selected("claude-opus-5-5"), { models: LISTED });
    assert.strictEqual(fresh.reasoningEffort, undefined);
    assert.strictEqual(
      settingsFor(selected("claude-opus-5-5"), { models: LISTED, previous: fresh }).reasoningEffort,
      undefined,
    );
  });
});

describe("where the effort came from [VAL-HIST-012]", () => {
  const restored = () =>
    settingsFor(selected("claude-opus-5-5"), {
      models: LISTED,
      previous: settingsFor(selected("auto"), { models: LISTED }),
    });

  it("[VAL-HIST-012] an explicit effort equal to the model's default is not read as restored", () => {
    const explicit = settingsFor(selected("claude-opus-5-5", "medium"), { models: LISTED });
    assert.strictEqual(explicit.reasoningEffort, "medium");
    const dropped = settingsFor(selected("claude-opus-5-5"), {
      models: LISTED,
      previous: explicit,
    });
    assert.strictEqual(dropped.reasoningEffort, undefined);
    assert.isFalse(sameDroidSessionSettings(explicit, dropped));
  });

  it("[VAL-HIST-012] selecting the restored default explicitly changes nothing on the wire but takes ownership of it", () => {
    const first = restored();
    const explicit = settingsFor(selected("claude-opus-5-5", "medium"), {
      models: LISTED,
      previous: first,
    });
    assert.isTrue(sameDroidSessionSettings(first, explicit));
    const dropped = settingsFor(selected("claude-opus-5-5"), {
      models: LISTED,
      previous: explicit,
    });
    assert.strictEqual(dropped.reasoningEffort, undefined);
    assert.isFalse(sameDroidSessionSettings(explicit, dropped));
  });

  it("[VAL-HIST-012] the restored effort survives a return to the same model through the router only", () => {
    const viaRouter = settingsFor(selected("claude-opus-5-5"), {
      models: LISTED,
      previous: settingsFor(selected("auto"), { models: LISTED, previous: restored() }),
    });
    assert.strictEqual(viaRouter.reasoningEffort, "medium");
    const viaModel = settingsFor(selected("claude-opus-5-5"), {
      models: LISTED,
      previous: settingsFor(selected("plain-model"), { models: LISTED, previous: restored() }),
    });
    assert.strictEqual(viaModel.reasoningEffort, undefined);
  });

  it("[VAL-HIST-012] the origin of the effort never reaches the wire", () => {
    for (const settings of [restored(), settingsFor(selected("claude-opus-5-5", "medium"))]) {
      assert.deepStrictEqual(droidSettingsParams(settings), {
        interactionMode: "auto",
        autonomyLevel: "off",
        modelId: "claude-opus-5-5",
        reasoningEffort: "medium",
      });
    }
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
        {
          id: "auto",
          router: true,
          supportedReasoningEfforts: ["none"],
          defaultReasoningEffort: "none",
        },
        {
          id: "claude-opus-5-5",
          router: false,
          supportedReasoningEfforts: ["low", "high"],
          defaultReasoningEffort: "high",
        },
        {
          id: "bare",
          router: false,
          supportedReasoningEfforts: [],
          defaultReasoningEffort: undefined,
        },
      ],
    );
  });

  it("[VAL-HIST-012] a result without a model list reports none", () => {
    assert.strictEqual(droidModelCatalog({}), undefined);
    assert.strictEqual(droidModelCatalog({ availableModels: "nope" }), undefined);
    assert.strictEqual(droidModelCatalog(undefined), undefined);
  });
});
