import type { OrchestrationV2Subagent } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

const ACTIVE_SUBAGENT_STATUSES = new Set<OrchestrationV2Subagent["status"]>([
  "pending",
  "running",
  "waiting",
]);

/** Some providers report this when a child ends without a report of its own. */
export const GENERIC_CHILD_END = /^Child task ended with status\b/i;

/**
 * What a subagent row expands to. Providers that run subagents without a thread of their own
 * leave the prompt and report on the row, and touch screens cannot hover for its preview.
 */
export function subagentInlineDetail(input: {
  readonly hasThread: boolean;
  readonly prompt: string | null | undefined;
  readonly report: string | null | undefined;
}): { readonly prompt: string | null; readonly report: string | null } | null {
  if (input.hasThread) return null;
  const prompt = input.prompt?.trim() || null;
  const trimmedReport = input.report?.trim();
  const report = trimmedReport && !GENERIC_CHILD_END.test(trimmedReport) ? trimmedReport : null;
  return prompt === null && report === null ? null : { prompt, report };
}

/**
 * Whether a subagent started another run after a notification reported its
 * final outcome, so that outcome no longer describes it. Claude resumes a
 * finished subagent under the same id. An active subagent is always on a later
 * run, even when the notification was delivered late from the queue; a settled
 * one is on a later run when it started after the notification arrived.
 */
export function subagentResumedSince(
  agent: Pick<OrchestrationV2Subagent, "status" | "startedAt">,
  notifiedAt: string,
): boolean {
  if (ACTIVE_SUBAGENT_STATUSES.has(agent.status)) return true;
  if (agent.startedAt === null) return false;
  const notifiedAtMillis = Date.parse(notifiedAt);
  return (
    !Number.isNaN(notifiedAtMillis) && DateTime.toEpochMillis(agent.startedAt) > notifiedAtMillis
  );
}
