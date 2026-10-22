import { describe } from "@effect/vitest";

import { DroidOrchestratorReplayHarness } from "../Adapters/DroidAdapterV2.testkit.ts";
import { itRunsReplayFixtures, runReplayFixture } from "./ProviderReplayFixtures.ts";

describe("Droid replay fixtures", () => {
  itRunsReplayFixtures("droid", (input) =>
    runReplayFixture({ ...input, harness: DroidOrchestratorReplayHarness }),
  );
});
