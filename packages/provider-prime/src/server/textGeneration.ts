import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { TextGenerationError } from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import type { PrimeSettings } from "../settings.ts";

import {
  makePrimeRpcConnection,
  parsePrimeModelSlug,
  primeRecordField,
  primeRecordString,
  type PrimeRpcRecord,
} from "./rpc.ts";
import { buildPrimeRpcLaunch, resolvePrimeLaunchArgs } from "./mcpInjection.ts";
import { supportedPrimeThinkingLevelsFromModel } from "./thinkingCapabilities.ts";
import * as TextGenerationOperations from "@t3tools/provider-core/server/textGenerationOperations";

const PRIME_TIMEOUT_MS = 180_000;

const isTextGenerationError = Schema.is(TextGenerationError);

type RunOutcome =
  | { readonly _tag: "text"; readonly text: string }
  | { readonly _tag: "error"; readonly detail: string };

function assistantText(message: unknown): string {
  const content = primeRecordField(message, "content");
  if (Predicate.isString(content)) return content;
  if (!Array.isArray(content)) return "";
  return content
    .filter((part) => primeRecordString(part, "type") === "text")
    .map((part) => primeRecordString(part, "text") ?? "")
    .join("");
}

/**
 * Prime has no `agent_settled`; with `--no-session` nothing can keep the
 * session busy after the run, so `agent_end` (whose `messages` are the run's
 * transcript) is the end of the request.
 */
function outcomeFromAgentEnd(record: PrimeRpcRecord): RunOutcome {
  const messages = primeRecordField(record, "messages");
  const assistants = (Array.isArray(messages) ? messages : []).filter(
    (message) => primeRecordString(message, "role") === "assistant",
  );
  const failure = assistants.find(
    (message) => primeRecordString(message, "stopReason") === "error",
  );
  if (failure !== undefined) {
    const errorMessage = primeRecordString(failure, "errorMessage")?.trim();
    return { _tag: "error", detail: errorMessage || "Prime request failed." };
  }
  return { _tag: "text", text: assistantText(assistants.at(-1)).trim() };
}

export const makePrimeTextGeneration = Effect.fn("makePrimeTextGeneration")(function* (
  primeSettings: PrimeSettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  const runPrimeJson: TextGenerationOperations.Runner = (request) => {
    const { operation, cwd, prompt, modelSelection } = request;
    return Effect.gen(function* () {
      const resolvedLaunchArgs = resolvePrimeLaunchArgs(primeSettings.launchArgs);
      if (!resolvedLaunchArgs.ok) {
        return yield* new TextGenerationError({
          operation,
          detail: resolvedLaunchArgs.message,
        });
      }
      // `customModels` accepts arbitrary strings, so an unusable slug is
      // rejected rather than skipped: running Prime's default model here would
      // report success for a model the caller never asked for.
      const parsedModel =
        modelSelection.model === "default" ? null : parsePrimeModelSlug(modelSelection.model);
      if (modelSelection.model !== "default" && parsedModel === null) {
        return yield* new TextGenerationError({
          operation,
          detail: `Prime model '${modelSelection.model}' must use provider/model format.`,
        });
      }
      const launch = buildPrimeRpcLaunch({
        launchArgs: resolvedLaunchArgs.args,
        environment,
        mcpSession: undefined,
        extensionPath: undefined,
        ephemeral: true,
        disableExtensions: true,
        // Background naming/content helpers must never mutate the workspace.
        disableTools: true,
      });
      const connection = yield* makePrimeRpcConnection({
        command: primeSettings.binaryPath || "prime-agent",
        args: launch.args,
        cwd,
        env: launch.env,
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));

      const selectedModel =
        parsedModel === null
          ? undefined
          : yield* connection.request({
              type: "set_model",
              provider: parsedModel.provider,
              modelId: parsedModel.modelId,
            });

      // Saved selections may carry options from other drivers (the overlay's
      // `reasoningEffort`); only `thinking` applies, and only at a level the
      // model accepts, because set_thinking_level fails on unsupported levels.
      const thinking = getModelSelectionStringOptionValue(modelSelection, "thinking");
      if (thinking !== undefined) {
        const model =
          parsedModel === null
            ? primeRecordField(yield* connection.request({ type: "get_state" }), "model")
            : selectedModel;
        if (supportedPrimeThinkingLevelsFromModel(model).some((level) => level === thinking)) {
          yield* connection.request({ type: "set_thinking_level", level: thinking });
        }
      }

      yield* connection.request({ type: "prompt", message: prompt });
      const outcome = yield* Effect.gen(function* () {
        while (true) {
          const event = yield* Queue.take(connection.events);
          if (event["type"] === "agent_end") return outcomeFromAgentEnd(event);
          // The daemon path reports failures after acceptance as an error
          // response without an id.
          if (event["type"] === "response" && event["success"] === false) {
            const error = primeRecordField(event, "error");
            return {
              _tag: "error",
              detail:
                Predicate.isString(error) && error.trim() ? error.trim() : "Prime request failed.",
            } satisfies RunOutcome;
          }
        }
      });
      if (outcome._tag === "error") {
        return yield* new TextGenerationError({ operation, detail: outcome.detail });
      }
      if (!outcome.text) {
        return yield* new TextGenerationError({
          operation,
          detail: "Prime returned empty output.",
        });
      }
      return yield* TextGenerationOperations.decodeJsonReply(request, "Prime", outcome.text);
    }).pipe(
      Effect.timeoutOption(PRIME_TIMEOUT_MS),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(new TextGenerationError({ operation, detail: "Prime request timed out." })),
          onSome: (value) => Effect.succeed(value),
        }),
      ),
      Effect.mapError((cause) =>
        isTextGenerationError(cause)
          ? cause
          : new TextGenerationError({
              operation,
              detail: "Prime text generation failed.",
              cause,
            }),
      ),
      Effect.scoped,
    );
  };

  return TextGenerationOperations.fromRunner(
    "PrimeTextGeneration",
    TextGenerationOperations.withoutInvestigation("Prime", runPrimeJson),
  );
});
