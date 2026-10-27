import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { TextGenerationError } from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { extractJsonObject } from "@t3tools/shared/schemaJson";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import * as TextGenerationOperations from "@t3tools/provider-core/server/textGenerationOperations";
import { isCommandMissingCause } from "@t3tools/provider-core/server/snapshotProbe";
import { limitSection } from "@t3tools/provider-core/server/textGenerationUtils";

import type { DroidSettings } from "../settings.ts";
import { DROID_HOOKS_OFF_SETTINGS } from "./hooksOffSettings.ts";

const DROID_TIMEOUT_MS = 180_000;
/** The router picks its own effort, so it has no effort option. */
const DROID_ROUTER_MODEL = "auto";
const DETAIL_MAX_CHARS = 2_000;

const DroidExecResult = Schema.Struct({
  type: Schema.Literal("result"),
  is_error: Schema.Boolean,
  result: Schema.String,
});
const decodeDroidExecResult = Schema.decodeOption(Schema.fromJsonString(DroidExecResult));

/**
 * One read-only `droid exec -o json` per call, in place of a stream-jsonrpc session. The
 * prompt travels in a temp file because positional arguments hit command-line length
 * limits.
 */
export const makeDroidTextGenerationRunner = Effect.fn("makeDroidTextGenerationRunner")(function* (
  droidSettings: DroidSettings,
  environment: NodeJS.ProcessEnv,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const fileSystem = yield* FileSystem.FileSystem;
  const binary = droidSettings.binaryPath.trim() || "droid";
  // The child inherits the environment, so Droid's own error text could echo the key.
  const apiKey = environment["FACTORY_API_KEY"]?.trim();
  const droidText = (text: string) =>
    limitSection(apiKey ? text.replaceAll(apiKey, "[redacted]") : text, DETAIL_MAX_CHARS);

  const runDroid: TextGenerationOperations.Runner = (request) => {
    const { operation, prompt, modelSelection } = request;
    const model = modelSelection.model;
    const requestedEffort = getModelSelectionStringOptionValue(modelSelection, "reasoningEffort");
    const effort = model === DROID_ROUTER_MODEL ? undefined : requestedEffort;
    const failure = (detail: string, cause?: unknown) =>
      new TextGenerationError({ operation, detail, cause });

    const writeTempFile = (prefix: string, content: string, suffix?: string) =>
      fileSystem.makeTempFileScoped({ prefix: `t3code-droid-${prefix}-`, suffix }).pipe(
        Effect.tap((path) => fileSystem.writeFileString(path, content)),
        Effect.mapError((cause) => failure("Failed to write a temporary file for Droid.", cause)),
      );

    const collectText = <E>(stream: Stream.Stream<Uint8Array, E>) =>
      stream.pipe(
        Stream.decodeText(),
        Stream.runFold(
          () => "",
          (text, chunk) => text + chunk,
        ),
        Effect.mapError((cause) => failure("Failed to read Droid CLI output.", cause)),
      );

    const runCommand: Effect.Effect<
      { readonly stdout: string; readonly stderr: string; readonly exitCode: number },
      TextGenerationError,
      Scope.Scope
    > = Effect.gen(function* () {
      // Droid starts the MCP servers listed in `<cwd>/.factory/mcp.json`, and titles are
      // generated for any project automatically, so never run inside the project. The
      // prompt already carries everything the model needs.
      const workingDirectory = yield* fileSystem
        .makeTempDirectoryScoped({ prefix: "t3code-droid-cwd-" })
        .pipe(
          Effect.mapError((cause) =>
            failure("Failed to create a temporary directory for Droid.", cause),
          ),
        );
      const promptPath = yield* writeTempFile("prompt", prompt);
      const settingsPath = yield* writeTempFile("settings", DROID_HOOKS_OFF_SETTINGS, ".json");
      const spawnCommand = yield* resolveSpawnCommand(
        binary,
        [
          "exec",
          "-o",
          "json",
          "-m",
          model,
          ...(effort === undefined ? [] : ["-r", effort]),
          // Spec-mode flags always mirror the chosen model so a spec model configured in the
          // user's Droid settings can never take over the run.
          "--spec-model",
          model,
          ...(effort === undefined ? [] : ["--spec-reasoning-effort", effort]),
          "--cwd",
          workingDirectory,
          "-f",
          promptPath,
          // The prompts carry untrusted diff text, so allow no tool that reads or fetches
          // data (Read, FetchUrl, WebSearch, connectors). The flag rejects an empty value
          // and unknown ids, so TodoWrite is the harmless placeholder.
          "--only-tools",
          "TodoWrite",
          "--settings",
          settingsPath,
        ],
        { env: environment, extendEnv: true },
      );
      const child = yield* spawner
        .spawn(
          ChildProcess.make(spawnCommand.command, spawnCommand.args, {
            cwd: workingDirectory,
            env: environment,
            extendEnv: true,
            shell: spawnCommand.shell,
            // The prompt comes from -f; the child must never wait on stdin.
            stdin: "ignore",
            killSignal: "SIGTERM",
            forceKillAfter: "5 seconds",
          }),
        )
        .pipe(
          Effect.mapError((cause) =>
            isCommandMissingCause(cause)
              ? failure(
                  `Droid CLI (\`${binary}\`) was not found. Check the binary path in Droid settings.`,
                  cause,
                )
              : failure("Failed to start the Droid CLI process.", cause),
          ),
        );
      const [stdout, stderr, exitCode] = yield* Effect.all(
        [
          collectText(child.stdout),
          collectText(child.stderr),
          child.exitCode.pipe(
            Effect.mapError((cause) => failure("Failed to read the Droid CLI exit code.", cause)),
          ),
        ],
        { concurrency: "unbounded" },
      );
      return { stdout, stderr, exitCode };
    });

    return Effect.gen(function* () {
      // The scope closes (child killed, temp files removed) before a timeout is reported.
      const { stdout, stderr, exitCode } = yield* runCommand.pipe(
        Effect.scoped,
        Effect.timeoutOption(DROID_TIMEOUT_MS),
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.fail(failure("Droid CLI request timed out.")),
            onSome: (output) => Effect.succeed(output),
          }),
        ),
      );

      const envelope = decodeDroidExecResult(stdout);
      if (exitCode !== 0) {
        const message = Option.match(envelope, {
          onNone: () => (stderr.trim().length > 0 ? stderr.trim() : stdout.trim()),
          onSome: (value) => value.result.trim(),
        });
        return yield* failure(
          message.length > 0
            ? `Droid CLI command failed: ${droidText(message)}`
            : `Droid CLI exited with code ${exitCode}.`,
        );
      }
      if (Option.isNone(envelope)) {
        return yield* failure(
          stdout.trim().length > 0
            ? `Droid CLI returned unexpected output: ${droidText(stdout.trim())}`
            : "Droid CLI returned no output.",
        );
      }
      const { is_error: isError, result } = envelope.value;
      if (isError) {
        return yield* failure(`Droid CLI command failed: ${droidText(result.trim())}`);
      }

      const decodeOutput = Schema.decodeEffect(Schema.fromJsonString(request.outputSchema));
      return yield* decodeOutput(extractJsonObject(result)).pipe(
        Effect.mapError((cause) =>
          failure(
            `Droid CLI returned invalid structured output: ${droidText(result.trim())}`,
            cause,
          ),
        ),
      );
    });
  };

  return runDroid;
});

export const makeDroidTextGeneration = Effect.fn("makeDroidTextGeneration")(function* (
  droidSettings: DroidSettings,
  environment: NodeJS.ProcessEnv,
) {
  const runDroid = yield* makeDroidTextGenerationRunner(droidSettings, environment);
  return TextGenerationOperations.fromRunner(
    "DroidTextGeneration",
    TextGenerationOperations.withoutInvestigation("Droid", runDroid),
  );
});
