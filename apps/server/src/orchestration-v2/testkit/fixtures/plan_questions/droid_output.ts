import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertAllRuntimeRequestsResolved,
  assertBaseProjection,
  assertRuntimeRequestCounts,
  assertRuntimeRequestKinds,
  assertSemanticProjectionIntegrity,
  assertTurnItemTypes,
  assertUserMessagesInclude,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  DROID_MODEL_SELECTION,
  PLAN_QUESTIONS_PROMPT,
  projectionFor,
} from "../shared.ts";
import {
  adapterReplyTo,
  droidInboundRequests,
  droidRequests,
  droidSettingsFrameBefore,
} from "../simple/droid_output.ts";
import { planQuestionsInput } from "./input.ts";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

/** The answer the shared input carries under the key of Droid's first question. */
function fixtureAnswerFor(questionId: string): string {
  const answerStep = planQuestionsInput().steps.find(
    (step) => step.type === "answer_next_user_input_request",
  );
  const answer =
    answerStep?.type === "answer_next_user_input_request"
      ? answerStep.answers[questionId]
      : undefined;
  assert.isString(answer, `the plan_questions input has an answer for ${questionId}`);
  return answer as string;
}

/**
 * Droid asks through `droid.ask_user` even in spec mode. The adapter opens one `user_input`
 * request, and the answer goes back as `{answers: [{index, question, answer}]}` (the shape the
 * live probe verified), never as a cancellation.
 */
export function assertDroidPlanQuestionsOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const sends = droidRequests(transcript, "add_user_message");
  assert.lengthOf(sends, 1);
  const settings = droidSettingsFrameBefore(transcript, sends[0]!.index);
  assert.equal(field(settings, "interactionMode"), "spec", "plan mode runs Droid in spec mode");
  assert.equal(field(settings, "specModeModelId"), DROID_MODEL_SELECTION.model);

  const asks = droidInboundRequests(transcript, "ask_user");
  assert.lengthOf(asks, 1, "the model asks one question");
  const questions = field(asks[0]!.params, "questions");
  assert.isArray(questions);
  assert.lengthOf(questions as ReadonlyArray<unknown>, 1);
  const [question] = questions as ReadonlyArray<unknown>;
  const questionIndex = field(question, "index");
  assert.isNumber(questionIndex);
  const expectedAnswer = fixtureAnswerFor(`question-${String(questionIndex)}`);

  const reply = adapterReplyTo(transcript, asks[0]!);
  assert.deepEqual(
    reply,
    {
      answers: [
        { index: questionIndex, question: field(question, "question"), answer: expectedAnswer },
      ],
    },
    "the reply carries the fixture's answer in Droid's ask_user shape",
  );
  assert.isUndefined(field(reply, "cancelled"), "an answered question is not a cancellation");

  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["completed"] });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assertRuntimeRequestCounts(projection, { total: 1, resolved: 1 });
  assertRuntimeRequestKinds(projection, ["user_input"]);
  assertAllRuntimeRequestsResolved(projection);
  assertTurnItemTypes(projection, ["user_message", "user_input_request"]);
  assertUserMessagesInclude(projection, [PLAN_QUESTIONS_PROMPT]);

  const requestItems = projection.turnItems.flatMap((item) =>
    item.type === "user_input_request" ? [item] : [],
  );
  assert.lengthOf(requestItems, 1);
  assert.equal(requestItems[0]?.questions[0]?.id, `question-${String(questionIndex)}`);
  assert.equal(requestItems[0]?.requestId, projection.runtimeRequests[0]?.id);
  assert.equal(
    projection.runtimeRequests[0]?.answers?.[`question-${String(questionIndex)}`],
    expectedAnswer,
    "the resolved request records the answer the user gave",
  );
  assert.isFalse(
    projection.turnItems.some((item) => item.status === "failed" || item.type === "error"),
    "answering a question leaves no failed item",
  );
}
