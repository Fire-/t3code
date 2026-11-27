import { describe } from "@effect/vitest";

import { PrimeOrchestratorReplayHarness } from "../Adapters/PrimeAdapterV2.testkit.ts";
import { itRunsReplayFixtures, runReplayFixture } from "./ProviderReplayFixtures.ts";

describe("Prime replay fixtures", () => {
  itRunsReplayFixtures("prime-agent", (input) =>
    runReplayFixture({ ...input, harness: PrimeOrchestratorReplayHarness }),
  );
});
