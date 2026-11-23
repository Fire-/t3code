import type { OrchestrationV2ProjectedTurnItem, RuntimeRequestId } from "@t3tools/contracts";

/**
 * The reasoning that explains a pending question, for agents whose explanation lands in the
 * reasoning channel instead of a visible message: the nearest reasoning item of the run that
 * raised the question, with no visible text between it and the question.
 */
export function questionPreamble(
  rows: ReadonlyArray<OrchestrationV2ProjectedTurnItem>,
  requestId: RuntimeRequestId,
): string | null {
  const questionIndex = rows.findLastIndex(
    (row) => row.item.type === "user_input_request" && row.item.requestId === requestId,
  );
  const questionRow = questionIndex === -1 ? undefined : rows[questionIndex];
  if (questionRow === undefined) return null;
  const runId = questionRow.item.runId;
  for (let index = questionIndex - 1; index >= 0; index--) {
    const item = rows[index]?.item;
    if (item === undefined || item.runId !== runId) return null;
    if (item.type === "user_message" || item.type === "assistant_message") return null;
    if (item.type === "reasoning") {
      const text = item.text.trim();
      return text.length > 0 ? text : null;
    }
  }
  return null;
}
