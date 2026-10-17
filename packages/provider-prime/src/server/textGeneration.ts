import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { TextGenerationError } from "@t3tools/contracts";
import type { PrimeSettings } from "../settings.ts";

import { makePrimeRpcConnection, parsePrimeModelSlug } from "./rpc.ts";
import { buildPrimeRpcLaunch, resolvePrimeLaunchArgs } from "./mcpInjection.ts";
import * as TextGenerationOperations from "@t3tools/provider-core/server/textGenerationOperations";

const PRIME_TIMEOUT_MS = 180_000;

const isTextGenerationError = Schema.is(TextGenerationError);

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

      if (modelSelection.model !== "default") {
        // `customModels` accepts arbitrary strings, so an unusable slug is
        // rejected rather than skipped: running Prime's default model here would
        // report success for a model the caller never asked for.
        const parsed = parsePrimeModelSlug(modelSelection.model);
        if (parsed === null) {
          return yield* new TextGenerationError({
            operation,
            detail: `Prime model '${modelSelection.model}' must use provider/model format.`,
          });
        }
        yield* connection.request({
          type: "set_model",
          provider: parsed.provider,
          modelId: parsed.modelId,
        });
      }

      yield* connection.request({ type: "prompt", message: prompt });
      yield* Effect.gen(function* () {
        while (true) {
          const event = yield* Queue.take(connection.events);
          if (event["type"] === "agent_settled") return;
        }
      });
      const data = yield* connection.request({ type: "get_last_assistant_text" });
      const text =
        typeof data === "object" &&
        data !== null &&
        typeof (data as { text?: unknown }).text === "string"
          ? (data as { text: string }).text.trim()
          : "";
      if (!text) {
        return yield* new TextGenerationError({
          operation,
          detail: "Prime returned empty output.",
        });
      }
      return yield* TextGenerationOperations.decodeJsonReply(request, "Prime", text);
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
