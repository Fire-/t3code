import { assert, describe, it } from "@effect/vitest";

import { primeAgentEndErrorMessage } from "./events.ts";

describe("primeAgentEndErrorMessage", () => {
  it("[VAL-TURN-009] takes the first non-blank errorMessage among agent_end messages", () => {
    const errorMessage =
      'Mistral API error (400): {"message":"Invalid request payload: Expected at most one leading ThinkChunk, got 2","code":"3051"}';
    assert.equal(
      primeAgentEndErrorMessage({
        type: "agent_end",
        messages: [
          { role: "user", content: "did something happen?" },
          { role: "assistant", content: [], errorMessage: "  " },
          { role: "assistant", content: [], stopReason: "error", errorMessage },
          { role: "assistant", content: [], stopReason: "error", errorMessage: "later error" },
        ],
      }),
      errorMessage,
    );
  });

  it.each([
    { name: "a clean stop", messages: [{ role: "assistant", content: [], stopReason: "stop" }] },
    { name: "no messages", messages: [] },
    {
      name: "a whitespace-only errorMessage",
      messages: [{ role: "assistant", errorMessage: "  " }],
    },
    { name: "a missing messages array", messages: undefined },
  ])("[VAL-TURN-009] gives no detail for $name", ({ messages }) => {
    assert.isUndefined(primeAgentEndErrorMessage({ type: "agent_end", messages }));
  });
});
