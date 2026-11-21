import { toServerClock } from "@t3tools/client-runtime/clock";
import type { EnvironmentId } from "@t3tools/contracts";
import { formatDuration } from "@t3tools/shared/orchestrationTimingCopy";
import { msg } from "gt-react";
import { m } from "~/i18n/messages";

function formatWorkingTimer(startIso: string, endIso: string): string | null {
  const startedAtMs = Date.parse(startIso);
  const endedAtMs = Date.parse(endIso);
  if (!Number.isFinite(startedAtMs) || !Number.isFinite(endedAtMs)) {
    return null;
  }

  const elapsedSeconds = Math.max(0, Math.floor((endedAtMs - startedAtMs) / 1000));
  if (elapsedSeconds < 60) {
    return m(msg("{seconds}s", { seconds: elapsedSeconds }));
  }

  return formatDuration(elapsedSeconds * 1_000, m);
}

/** Elapsed time to `pausedIso`, or to now on the thread environment's server clock. */
export function formatWorkingTimerNow(
  startIso: string,
  environmentId: EnvironmentId,
  pausedIso?: string,
): string {
  const endIso = pausedIso ?? new Date(toServerClock(environmentId, Date.now())).toISOString();
  return formatWorkingTimer(startIso, endIso) ?? "0s";
}
