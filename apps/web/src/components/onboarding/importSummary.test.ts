import { describe, expect, it } from "vite-plus/test";

import { summarizeImport } from "./importSummary";

describe("summarizeImport", () => {
  it("notes empty sessions without warning when every project imported", () => {
    expect(
      summarizeImport(
        [
          { importedCount: 3, skippedCount: 0, emptyCount: 1 },
          { importedCount: 2, skippedCount: 0 },
        ],
        true,
      ),
    ).toEqual({ importedThreadCount: 5, warning: "", note: "1 empty session skipped." });
  });

  it("says why threads were skipped across projects", () => {
    expect(
      summarizeImport(
        [
          {
            importedCount: 4,
            skippedCount: 2,
            emptyCount: 2,
            skippedReasons: [{ reason: "too-large", count: 2 }],
          },
          {
            importedCount: 0,
            skippedCount: 2,
            skippedReasons: [
              { reason: "changed", count: 1 },
              { reason: "too-large", count: 1 },
            ],
          },
        ],
        false,
      ),
    ).toEqual({
      importedThreadCount: 4,
      warning:
        "Imported 4 threads. 4 threads could not be imported (3 too large, 1 changed while importing). 2 empty sessions skipped.",
      note: "",
    });
  });

  it("keeps the bare count from servers that send no reasons", () => {
    expect(summarizeImport([{ importedCount: 0, skippedCount: 1 }], false).warning).toBe(
      "1 thread could not be imported.",
    );
  });
});
