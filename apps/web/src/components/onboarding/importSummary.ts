import type { AgentSessionImportResult, AgentSessionImportSkipReason } from "@t3tools/contracts";

export interface ImportSummary {
  readonly importedThreadCount: number;
  /** Set when some selected history did not import. */
  readonly warning: string;
  /** Detail for an import that otherwise succeeded. */
  readonly note: string;
}

const SKIP_REASON_LABELS: Record<AgentSessionImportSkipReason, string> = {
  "import-limit": "over the import limit",
  "too-large": "too large",
  changed: "changed while importing",
  unreadable: "unreadable",
  "other-folder": "started in another folder",
  failed: "failed to save",
};

const plural = (count: number, one: string, many: string) => `${count} ${count === 1 ? one : many}`;

export function summarizeImport(
  results: ReadonlyArray<AgentSessionImportResult>,
  allProjectsImported: boolean,
): ImportSummary {
  let importedThreadCount = 0;
  let skippedCount = 0;
  let emptyCount = 0;
  const reasons = new Map<AgentSessionImportSkipReason, number>();
  for (const result of results) {
    importedThreadCount += result.importedCount;
    skippedCount += result.skippedCount;
    emptyCount += result.emptyCount ?? 0;
    for (const { reason, count } of result.skippedReasons ?? []) {
      reasons.set(reason, (reasons.get(reason) ?? 0) + count);
    }
  }

  const emptyNote =
    emptyCount > 0 ? `${plural(emptyCount, "empty session", "empty sessions")} skipped.` : "";
  if (allProjectsImported) return { importedThreadCount, warning: "", note: emptyNote };

  // Servers that predate skip reasons send only the count.
  const why =
    reasons.size > 0
      ? ` (${Array.from(reasons, ([reason, count]) => `${count} ${SKIP_REASON_LABELS[reason]}`).join(", ")})`
      : "";
  const imported = `Imported ${plural(importedThreadCount, "thread", "threads")}.`;
  const notImported = `${plural(skippedCount, "thread", "threads")} could not be imported${why}.`;
  const warning =
    skippedCount > 0
      ? importedThreadCount > 0
        ? `${imported} ${notImported}`
        : notImported
      : importedThreadCount > 0
        ? `${imported} Some thread history could not be imported.`
        : "Could not import thread history.";
  return {
    importedThreadCount,
    warning: emptyNote === "" ? warning : `${warning} ${emptyNote}`,
    note: "",
  };
}
