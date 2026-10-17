import { primeRecordString as recordString, type PrimeRpcRecord } from "./rpc.ts";

function nonBlank(value: string | undefined): string | undefined {
  return value === undefined || value.trim().length === 0 ? undefined : value;
}

/** The detail Prime attached to a failed model call, if any message carries one. */
export function primeErrorMessage(message: unknown): string | undefined {
  return nonBlank(recordString(message, "errorMessage"));
}

/**
 * A model failure that never streamed (a provider 400 before the first token)
 * can reach T3 only through agent_end, so its messages are the last place to
 * find the provider's reason.
 */
export function primeAgentEndErrorMessage(agentEnd: PrimeRpcRecord): string | undefined {
  const messages = agentEnd["messages"];
  if (!Array.isArray(messages)) return undefined;
  for (const message of messages) {
    const errorMessage = primeErrorMessage(message);
    if (errorMessage !== undefined) return errorMessage;
  }
  return undefined;
}

export function primeAgentEndFailed(agentEnd: PrimeRpcRecord): boolean {
  const messages = agentEnd["messages"];
  return (
    Array.isArray(messages) &&
    messages.some((message) => recordString(message, "stopReason") === "error")
  );
}

/**
 * Prime refuses a prompt while it is busy (core/agent-session.ts). The refusal
 * concerns only the message it answers; the run that made Prime busy keeps going.
 */
export function isPrimeBusyRejection(error: string | undefined): boolean {
  return (
    error !== undefined &&
    (error.startsWith("Agent is already processing") ||
      error.startsWith("Agent has queued work") ||
      error.startsWith("Agent became busy before prompt delivery"))
  );
}
