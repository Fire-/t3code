/**
 * Pure mapping between Droid's inbound requests and t3's runtime requests: what to show for a
 * permission or question, and the exact reply Droid accepts for each answer.
 */
import type {
  OrchestrationV2UserInputQuestion,
  ProviderApprovalDecision,
  ProviderApprovalOption,
  ProviderRequestKind,
} from "@t3tools/contracts";
import * as Predicate from "effect/Predicate";

import { droidAskUserQuestionsFromParams, type DroidAskUserQuestion } from "./protocol.ts";

/** Droid's own option id for saying no; every permission prompt carries it. */
export const DROID_CANCEL_OPTION = "cancel";

/** The `confirmationType` of the permission Droid raises when its agent wants to leave spec mode. */
export const DROID_EXIT_SPEC_MODE = "exit_spec_mode";

const MAX_PROMPT_CHARS = 4_000;

/**
 * An option Droid offers only appears when it has a decision here. `proceed_new_session*` (moves
 * the process to a new session), `proceed_edit` (needs edited spec content), and
 * `proceed_report_false_positive` have no `ProviderApprovalDecision`, so t3 never offers them.
 */
const DECISION_BY_OPTION: Readonly<Record<string, ProviderApprovalDecision>> = {
  proceed_once: "accept",
  proceed_always: "acceptForSession",
  proceed_always_tools: "acceptForSession",
  proceed_always_server: "acceptForSession",
  proceed_auto_run_low: "acceptForSession",
  proceed_auto_run_medium: "acceptForSession",
  proceed_auto_run_high: "acceptForSession",
  proceed_always_file: "acceptAlways",
  proceed_auto_run: "acceptAlways",
  [DROID_CANCEL_OPTION]: "decline",
};

export interface DroidApprovalRequest {
  readonly requestKind: ProviderRequestKind;
  readonly confirmationType: string | undefined;
  readonly title: string;
  readonly prompt: string | undefined;
  readonly options: ReadonlyArray<ProviderApprovalOption>;
  /** The Droid option id offered for each affirmative decision in this request. */
  readonly optionIds: ReadonlyMap<ProviderApprovalDecision, string>;
  /** The offered cancel option, or the protocol's fixed id when Droid listed none. */
  readonly cancelOptionId: string;
  readonly toolUseIds: ReadonlyArray<string>;
}

const asTrimmed = (value: unknown): string | undefined => {
  if (!Predicate.isString(value)) return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
};

const field = (value: unknown, key: string): unknown =>
  Predicate.isObject(value) ? value[key] : undefined;

function requestKindOf(confirmationType: string | undefined): ProviderRequestKind {
  switch (confirmationType) {
    case "exec":
      return "command";
    case "create":
    case "edit":
    case "apply_patch":
      return "file-change";
    default:
      return "permission";
  }
}

function promptOf(entry: unknown, confirmationType: string | undefined): string | undefined {
  const details = field(entry, "details");
  const input = field(field(entry, "toolUse"), "input");
  const text = (() => {
    switch (confirmationType) {
      case "exec":
        return (
          asTrimmed(field(details, "fullCommand")) ??
          asTrimmed(field(details, "command")) ??
          asTrimmed(field(input, "command"))
        );
      case "create":
      case "edit":
      case "apply_patch":
        return (
          asTrimmed(field(details, "filePath")) ??
          asTrimmed(field(details, "fileName")) ??
          asTrimmed(field(input, "file_path"))
        );
      case "mcp_tool": {
        const server = asTrimmed(field(details, "serverName"));
        const tool =
          asTrimmed(field(details, "actualToolName")) ?? asTrimmed(field(details, "toolName"));
        return tool === undefined ? undefined : server === undefined ? tool : `${server}: ${tool}`;
      }
      default:
        return undefined;
    }
  })();
  return text?.slice(0, MAX_PROMPT_CHARS);
}

export function droidApprovalRequest(params: unknown): DroidApprovalRequest {
  const toolUses = field(params, "toolUses");
  const entries: ReadonlyArray<unknown> = Array.isArray(toolUses) ? toolUses : [];
  const first = entries[0];
  const confirmationType = asTrimmed(field(first, "confirmationType"));
  const toolUseIds = entries.flatMap((entry) => {
    const id = asTrimmed(field(field(entry, "toolUse"), "id"));
    return id === undefined ? [] : [id];
  });

  const offeredOptions = field(params, "options");
  const offered: ReadonlyArray<unknown> = Array.isArray(offeredOptions) ? offeredOptions : [];
  const options: Array<ProviderApprovalOption> = [];
  const optionIds = new Map<ProviderApprovalDecision, string>();
  let cancelOptionId = DROID_CANCEL_OPTION;
  const seen = new Set<ProviderApprovalDecision>();
  for (const option of offered) {
    const value = asTrimmed(field(option, "value"));
    const label = asTrimmed(field(option, "label"));
    const decision = value === undefined ? undefined : DECISION_BY_OPTION[value];
    if (value === undefined || label === undefined || decision === undefined) continue;
    if (seen.has(decision)) continue;
    seen.add(decision);
    options.push({ decision, label });
    if (value === DROID_CANCEL_OPTION) cancelOptionId = value;
    else optionIds.set(decision, value);
  }

  return {
    requestKind: requestKindOf(confirmationType),
    confirmationType,
    title: asTrimmed(field(field(first, "toolUse"), "name")) ?? "Droid permission request",
    prompt: promptOf(first, confirmationType),
    options,
    optionIds,
    cancelOptionId,
    toolUseIds,
  };
}

/**
 * `undefined` when the decision needs an option Droid did not offer: a made-up option id could
 * approve something the user never saw.
 */
export function droidPermissionReply(
  request: DroidApprovalRequest,
  decision: ProviderApprovalDecision,
): { readonly selectedOption: string } | undefined {
  if (decision === "decline" || decision === "cancel") {
    return { selectedOption: request.cancelOptionId };
  }
  const selectedOption = request.optionIds.get(decision);
  return selectedOption === undefined ? undefined : { selectedOption };
}

const nonBlank = (value: unknown): string | undefined =>
  Predicate.isString(value) && value.trim().length > 0 ? value : undefined;

/**
 * The plan an `exit_spec_mode` request carries. `details.plan` is the text Droid shows; the
 * ExitSpecMode tool input holds the same plan and stands in when the details lack it. The text is
 * kept exactly as written, because it is what the user will read and implement.
 */
export function droidExitSpecModePlan(params: unknown): string | undefined {
  const toolUses = field(params, "toolUses");
  const first = Array.isArray(toolUses) ? toolUses[0] : undefined;
  return (
    nonBlank(field(field(first, "details"), "plan")) ??
    nonBlank(field(field(field(first, "toolUse"), "input"), "plan"))
  );
}

export interface DroidQuestion extends DroidAskUserQuestion {
  readonly id: string;
}

export interface DroidUserInputRequest {
  readonly questions: ReadonlyArray<OrchestrationV2UserInputQuestion>;
  readonly native: ReadonlyArray<DroidQuestion>;
}

const droidQuestionId = (index: number): string => `question-${index}`;

export function droidUserInputRequest(params: unknown): DroidUserInputRequest {
  const native = droidAskUserQuestionsFromParams(params).map(
    (question): DroidQuestion => ({ ...question, id: droidQuestionId(question.index) }),
  );
  return {
    native,
    questions: native.map(
      (question): OrchestrationV2UserInputQuestion => ({
        id: question.id,
        header: question.topic ?? `Question ${question.index}`,
        question: question.question,
        options: question.options.map((label) => ({ label, description: label, value: label })),
        multiSelect: question.multiSelect,
      }),
    ),
  };
}

const answerText = (answer: unknown): string | undefined => {
  if (Predicate.isString(answer)) return answer.length > 0 ? answer : undefined;
  if (Array.isArray(answer)) {
    const parts = answer.filter(Predicate.isString).filter((part) => part.length > 0);
    return parts.length > 0 ? parts.join(", ") : undefined;
  }
  return undefined;
};

/** `undefined` when nothing was answered, which the caller treats as a dismissal. */
export function droidAskUserReply(
  request: DroidUserInputRequest,
  answers: Readonly<Record<string, unknown>> | undefined,
):
  | { readonly answers: ReadonlyArray<{ index: number; question: string; answer: string }> }
  | undefined {
  if (answers === undefined) return undefined;
  const replies = request.native.flatMap((question) => {
    const answer = answerText(answers[question.id]);
    return answer === undefined
      ? []
      : [{ index: question.index, question: question.question, answer }];
  });
  return replies.length === 0 ? undefined : { answers: replies };
}
