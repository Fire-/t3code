import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import { subagentInlineDetail, subagentResumedSince } from "./V2LifecycleRow.logic";

const failedAt = "2026-10-09T12:21:00.000Z";
const beforeFailure = DateTime.makeUnsafe("2026-10-09T12:05:00.000Z");
const afterFailure = DateTime.makeUnsafe("2026-10-09T12:48:00.000Z");

describe("subagentResumedSince", () => {
  it("is true while it runs even when the notification was delivered after the resume", () => {
    expect(
      subagentResumedSince(
        { status: "running", startedAt: afterFailure },
        "2026-10-09T12:50:00.000Z",
      ),
    ).toBe(true);
  });

  // regression: #18099 a resumed subagent's old failure notification row showed failed instead of resumed
  it("is true once a run started after the notification has settled", () => {
    expect(subagentResumedSince({ status: "completed", startedAt: afterFailure }, failedAt)).toBe(
      true,
    );
  });

  it("is false for the run the notification reported on", () => {
    expect(subagentResumedSince({ status: "failed", startedAt: beforeFailure }, failedAt)).toBe(
      false,
    );
  });
});

describe("subagentInlineDetail", () => {
  it("shows the prompt and report of a subagent that has no thread to open", () => {
    expect(
      subagentInlineDetail({
        hasThread: false,
        prompt: "  Find where models are labelled.  ",
        report: "\nShort answer: no.\n",
      }),
    ).toEqual({ prompt: "Find where models are labelled.", report: "Short answer: no." });
  });

  it("leaves a subagent with its own thread to that thread", () => {
    expect(subagentInlineDetail({ hasThread: true, prompt: "Prompt", report: "Report" })).toBe(
      null,
    );
  });

  it("drops the generic end-of-task line, and has nothing to show without text", () => {
    expect(
      subagentInlineDetail({
        hasThread: false,
        prompt: "Prompt",
        report: "Child task ended with status completed",
      }),
    ).toEqual({ prompt: "Prompt", report: null });
    expect(subagentInlineDetail({ hasThread: false, prompt: " ", report: null })).toBe(null);
  });
});
