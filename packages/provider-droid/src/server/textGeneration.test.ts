import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  DEFAULT_TEXT_GENERATION_MODEL_BY_PROVIDER,
  ProviderDriverKind,
  ProviderInstanceId,
  type ModelSelection,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { createModelSelection } from "@t3tools/shared/model";
import { fallbackTextGenerationModelSelection } from "@t3tools/shared/serverSettings";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import {
  buildReviewGuidePrompt,
  buildThreadTitlePrompt,
} from "@t3tools/provider-core/server/textGenerationPrompts";
import { DroidSettings } from "../settings.ts";
import { DROID_HOOKS_OFF_SETTINGS } from "./hooksOffSettings.ts";
import type { OneShotMockScript } from "./oneShotMockAgent.mock.ts";
import { makeOneShotMockAgent } from "./oneShotMockAgent.testkit.ts";
import { makeDroidTextGeneration, makeDroidTextGenerationRunner } from "./textGeneration.ts";

const decodeDroidSettings = Schema.decodeSync(DroidSettings);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

const DROID_INSTANCE_ID = ProviderInstanceId.make("droid");

const modelSelection = (model: string, effort?: string): ModelSelection =>
  createModelSelection(
    DROID_INSTANCE_ID,
    model,
    effort === undefined ? undefined : [{ id: "reasoningEffort", value: effort }],
  );

const resultEnvelope = (result: string, isError = false) =>
  `${encodeJson({
    type: "result",
    subtype: isError ? "failure" : "success",
    is_error: isError,
    duration_ms: 12,
    num_turns: 1,
    result,
    session_id: "00000000-0000-4000-8000-000000000001",
  })}\n`;

const replyWith = (value: unknown): OneShotMockScript => ({
  stdout: resultEnvelope(encodeJson(value)),
});

const asString = (value: string | null | undefined): string =>
  value ?? assert.fail("expected a string");

const flagValue = (argv: ReadonlyArray<string>, flag: string): string => {
  const index = argv.indexOf(flag);
  assert.isAtLeast(index, 0, `${flag} is missing from ${argv.join(" ")}`);
  return asString(argv[index + 1]);
};

const REVIEW_GUIDE_INPUT = {
  number: 7,
  title: "Route reset credits through the hub",
  description: "Moves credit routing into the hub service.",
  baseBranch: "main",
  baseCommit: "0123456789abcdef0123456789abcdef01234567",
  settledReview: "(nothing)",
};

const REVIEW_GUIDE_REPLY = {
  stops: [
    {
      title: "Credits route through the hub",
      summary: "Credits used to be routed per account. The hub now owns routing.",
      attention: { kind: "contract", note: "The hub becomes the only owner of routing." },
      excerpts: [{ path: "hub.ts", side: "new", start: 3, end: 9, caption: "The new router" }],
      files: [{ path: "hub.test.ts", note: "covers the router" }],
      notes: [{ path: "hub.ts", line: 4, title: "Why a map", body: "Lookups stay constant time." }],
      questions: ["Why not keep the per-account router?"],
    },
  ],
};

const makeTextGeneration = (binaryPath: string, environment: NodeJS.ProcessEnv = {}) =>
  makeDroidTextGeneration(decodeDroidSettings({ binaryPath }), environment);

const makeHandle = (input: {
  readonly stdout?: string;
  readonly exitCode?: Effect.Effect<ChildProcessSpawner.ExitCode>;
  readonly kill?: () => Effect.Effect<void>;
}) =>
  ChildProcessSpawner.makeHandle({
    // Outside the valid PID range, so cleanup can never signal a real process.
    pid: ChildProcessSpawner.ProcessId(999_999_999),
    exitCode: input.exitCode ?? Effect.succeed(ChildProcessSpawner.ExitCode(0)),
    isRunning: Effect.succeed(false),
    kill: input.kill ?? (() => Effect.void),
    unref: Effect.succeed(Effect.void),
    stdin: Sink.drain,
    stdout:
      input.stdout === undefined
        ? Stream.empty
        : Stream.succeed(new TextEncoder().encode(input.stdout)),
    stderr: Stream.empty,
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });

const standardCommand = (command: ChildProcess.Command): ChildProcess.StandardCommand =>
  ChildProcess.isStandardCommand(command) ? command : assert.fail("unexpected pipeline");

it.layer(NodeServices.layer)("DroidTextGeneration", (it) => {
  it.effect(
    "[VAL-TXT-002][VAL-TXT-004] runs one droid exec outside the project, with a hooks-off settings file and the prompt in a file, and removes both files",
    () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const factoryHome = path.join(yield* HostProcess.HomeDirectory, ".factory");
        const mock = yield* makeOneShotMockAgent(replyWith({ title: "Fix login redirect" }));
        const textGeneration = yield* makeTextGeneration(mock.binaryPath);

        const result = yield* textGeneration.generateThreadTitle({
          cwd: mock.projectDir,
          message: "Fix the login redirect",
          modelSelection: modelSelection("claude-opus-5-5", "high"),
        });
        assert.equal(result.title, "Fix login redirect");

        const [entry, ...rest] = yield* mock.readLog;
        assert.isDefined(entry);
        assert.lengthOf(rest, 0);
        const workingDirectory = flagValue(entry.argv, "--cwd");
        const promptPath = asString(entry.promptFile.path);
        const settingsPath = asString(entry.settingsFile.path);
        assert.deepEqual(entry.argv, [
          "exec",
          "-o",
          "json",
          "-m",
          "claude-opus-5-5",
          "-r",
          "high",
          "--spec-model",
          "claude-opus-5-5",
          "--spec-reasoning-effort",
          "high",
          "--cwd",
          workingDirectory,
          "-f",
          promptPath,
          "--only-tools",
          "TodoWrite",
          "--settings",
          settingsPath,
        ]);

        // The fake droid was launched through the configured binaryPath, and the process
        // runs in the same directory it was told to use.
        assert.equal(path.basename(entry.cwd), path.basename(workingDirectory));
        for (const location of [workingDirectory, promptPath, settingsPath]) {
          assert.isTrue(
            path.relative(mock.projectDir, location).startsWith(".."),
            `${location} must be outside the project`,
          );
          assert.isTrue(
            path.relative(factoryHome, location).startsWith(".."),
            `${location} must be outside ~/.factory`,
          );
        }
        assert.notInclude(entry.argv, mock.projectDir);

        assert.equal(entry.settingsFile.content, DROID_HOOKS_OFF_SETTINGS);
        assert.deepEqual(decodeJson(asString(entry.settingsFile.content)), {
          hooks: { hooksDisabled: true },
        });
        const expectedPrompt = buildThreadTitlePrompt({
          message: "Fix the login redirect",
          previousTitle: undefined,
          linkedContext: undefined,
          attachments: undefined,
        }).prompt;
        assert.equal(entry.promptFile.content, expectedPrompt);
        assert.notInclude(entry.argv, expectedPrompt);

        assert.isFalse(yield* fileSystem.exists(promptPath));
        assert.isFalse(yield* fileSystem.exists(settingsPath));
        assert.isFalse(yield* fileSystem.exists(workingDirectory));
      }),
  );

  it.effect.each([
    {
      name: "a model with an effort mirrors both on the spec model",
      selection: modelSelection("claude-opus-5-5", "high"),
      flags: [
        "-m",
        "claude-opus-5-5",
        "-r",
        "high",
        "--spec-model",
        "claude-opus-5-5",
        "--spec-reasoning-effort",
        "high",
      ],
    },
    {
      name: "a model with no effort sends no effort flags",
      selection: modelSelection("claude-opus-5-5"),
      flags: ["-m", "claude-opus-5-5", "--spec-model", "claude-opus-5-5"],
    },
    {
      name: "the auto router sends no effort flags even when an effort is selected",
      selection: modelSelection("auto", "high"),
      flags: ["-m", "auto", "--spec-model", "auto"],
    },
  ])("[VAL-TXT-003] $name", ({ selection, flags }) =>
    Effect.gen(function* () {
      const mock = yield* makeOneShotMockAgent(replyWith({ branch: "fix-login" }));
      const textGeneration = yield* makeTextGeneration(mock.binaryPath);

      yield* textGeneration.generateBranchName({
        cwd: mock.projectDir,
        message: "Fix the login redirect",
        modelSelection: selection,
      });

      const [entry] = yield* mock.readLog;
      assert.isDefined(entry);
      assert.deepEqual(entry.argv.slice(3, 3 + flags.length), flags);
      const spec = entry.argv.indexOf("--spec-model");
      assert.equal(entry.argv[spec + 1], flagValue(entry.argv, "-m"));
      assert.equal(entry.argv.includes("-r"), flags.includes("-r"));
      assert.equal(
        entry.argv.includes("--spec-reasoning-effort"),
        flags.includes("--spec-reasoning-effort"),
      );
    }),
  );

  it.effect(
    "[VAL-REG-019] a text generation selection resolved without a configured model runs claude-haiku-5-5",
    () =>
      Effect.gen(function* () {
        const droid = ProviderDriverKind.make("droid");
        assert.equal(DEFAULT_TEXT_GENERATION_MODEL_BY_PROVIDER[droid], "claude-haiku-5-5");
        const selection = fallbackTextGenerationModelSelection(
          {},
          (_instanceId, driver) => driver === droid,
        );
        assert.deepEqual(selection, { instanceId: DROID_INSTANCE_ID, model: "claude-haiku-5-5" });

        const mock = yield* makeOneShotMockAgent(replyWith({ title: "Fix login redirect" }));
        const textGeneration = yield* makeTextGeneration(mock.binaryPath);
        yield* textGeneration.generateThreadTitle({
          cwd: mock.projectDir,
          message: "Fix the login redirect",
          modelSelection: selection ?? assert.fail("droid must be a text generation fallback"),
        });

        const [entry] = yield* mock.readLog;
        assert.isDefined(entry);
        assert.deepEqual(entry.argv.slice(3, 7), [
          "-m",
          "claude-haiku-5-5",
          "--spec-model",
          "claude-haiku-5-5",
        ]);
      }),
  );

  it.effect(
    "[VAL-TXT-006] generateThreadTitle, generateCommitMessage, generatePrContent, and generateBranchName decode and sanitize the reply",
    () =>
      Effect.gen(function* () {
        const selection = modelSelection("claude-haiku-5-5");
        const commitReply = yield* makeOneShotMockAgent(
          replyWith({
            subject: "  Fix the login redirect.  ",
            body: "\n  Send users back to where they started.\n",
            branch: "Fix Login Redirect!",
          }),
        );
        const commit = yield* (yield* makeTextGeneration(
          commitReply.binaryPath,
        )).generateCommitMessage({
          cwd: commitReply.projectDir,
          branch: "main",
          stagedSummary: "M src/login.ts",
          stagedPatch: "diff --git a/src/login.ts b/src/login.ts",
          includeBranch: true,
          modelSelection: selection,
        });
        assert.deepEqual(commit, {
          subject: "Fix the login redirect",
          body: "Send users back to where they started.",
          branch: "feature/fix-login-redirect",
        });

        const prReply = yield* makeOneShotMockAgent(
          replyWith({ title: "  Fix login redirect\nignored second line", body: "\nBody text\n" }),
        );
        const pr = yield* (yield* makeTextGeneration(prReply.binaryPath)).generatePrContent({
          cwd: prReply.projectDir,
          baseBranch: "main",
          headBranch: "feature/login",
          commitSummary: "Fix login",
          diffSummary: "1 file changed",
          diffPatch: "diff --git a/src/login.ts b/src/login.ts",
          modelSelection: selection,
        });
        assert.deepEqual(pr, { title: "Fix login redirect", body: "Body text" });

        const branchReply = yield* makeOneShotMockAgent(
          replyWith({ branch: "  Fix/Login Redirect!! " }),
        );
        const branch = yield* (yield* makeTextGeneration(
          branchReply.binaryPath,
        )).generateBranchName({
          cwd: branchReply.projectDir,
          message: "Fix the login redirect",
          modelSelection: selection,
        });
        assert.deepEqual(branch, { branch: "fix/login-redirect" });

        const titleReply = yield* makeOneShotMockAgent(
          replyWith({ title: '  "Fix   the login redirect"\nsecond line' }),
        );
        const title = yield* (yield* makeTextGeneration(titleReply.binaryPath)).generateThreadTitle(
          {
            cwd: titleReply.projectDir,
            message: "Fix the login redirect",
            modelSelection: selection,
          },
        );
        assert.deepEqual(title, { title: "Fix the login redirect" });
      }),
  );

  it.effect(
    "[VAL-TXT-001][VAL-TXT-006] the shared runner decodes a review guide, while generateReviewGuide refuses to run blind",
    () =>
      Effect.gen(function* () {
        const settings = decodeDroidSettings({});
        const prompt = buildReviewGuidePrompt(REVIEW_GUIDE_INPUT);
        const mock = yield* makeOneShotMockAgent(replyWith(REVIEW_GUIDE_REPLY));
        const runner = yield* makeDroidTextGenerationRunner(
          { ...settings, binaryPath: mock.binaryPath },
          {},
        );

        const guide = yield* runner({
          operation: "generateReviewGuide",
          cwd: mock.projectDir,
          modelSelection: modelSelection("claude-haiku-5-5"),
          ...prompt,
        });
        assert.deepEqual<unknown>(guide, REVIEW_GUIDE_REPLY);

        // The review guide asks the runner to investigate a checkout. The one-shot has
        // no tool that can, so the operation fails typed instead of answering blind.
        const textGeneration = yield* makeTextGeneration(mock.binaryPath);
        assert.sameMembers(Object.keys(textGeneration), [
          "generateCommitMessage",
          "generatePrContent",
          "generateBranchName",
          "generateThreadTitle",
          "generateReviewGuide",
        ]);
        const spawnsBefore = (yield* mock.readLog).length;
        const error = yield* Effect.flip(
          textGeneration.generateReviewGuide({
            ...REVIEW_GUIDE_INPUT,
            cwd: mock.projectDir,
            modelSelection: modelSelection("claude-haiku-5-5"),
          }),
        );
        assert.equal(error._tag, "TextGenerationError");
        assert.equal(error.operation, "generateReviewGuide");
        assert.include(error.detail, "cannot investigate");
        assert.lengthOf(yield* mock.readLog, spawnsBefore);
      }),
  );

  it.effect.each([
    {
      name: "a non-zero exit with a result envelope carries Droid's message",
      script: {
        stdout: resultEnvelope("Authentication failed. Please log in.", true),
        exitCode: 1,
      },
      detail: "Authentication failed. Please log in.",
    },
    {
      name: "a non-zero exit without an envelope carries stderr",
      script: { stderr: "droid: unexpected crash\n", exitCode: 2 },
      detail: "droid: unexpected crash",
    },
    {
      name: "a non-zero exit with no output names the exit code",
      script: { exitCode: 3 },
      detail: "exited with code 3",
    },
    {
      name: "is_error: true on a zero exit",
      script: { stdout: resultEnvelope("The model refused the request.", true) },
      detail: "The model refused the request.",
    },
    {
      name: "stdout that is not a result envelope",
      script: { stdout: "droid printed plain text\n" },
      detail: "droid printed plain text",
    },
    {
      name: "empty stdout",
      script: {},
      detail: "returned no output",
    },
    {
      name: "a result that is not JSON",
      script: { stdout: resultEnvelope("Here is a title, not JSON.") },
      detail: "invalid structured output",
    },
    {
      name: "JSON that does not match the operation's schema",
      script: { stdout: resultEnvelope(encodeJson({ heading: "wrong key" })) },
      detail: "invalid structured output",
    },
  ])("[VAL-TXT-007] $name returns a typed error and removes the temp files", ({ script, detail }) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const mock = yield* makeOneShotMockAgent(script);
      const textGeneration = yield* makeTextGeneration(mock.binaryPath);

      const error = yield* Effect.flip(
        textGeneration.generateThreadTitle({
          cwd: mock.projectDir,
          message: "Fix the login redirect",
          modelSelection: modelSelection("claude-haiku-5-5"),
        }),
      );

      assert.equal(error._tag, "TextGenerationError");
      assert.equal(error.operation, "generateThreadTitle");
      assert.include(error.detail, detail);
      const [entry, ...rest] = yield* mock.readLog;
      assert.isDefined(entry);
      assert.lengthOf(rest, 0);
      assert.isFalse(yield* fileSystem.exists(asString(entry.promptFile.path)));
      assert.isFalse(yield* fileSystem.exists(asString(entry.settingsFile.path)));
      assert.isFalse(yield* fileSystem.exists(flagValue(entry.argv, "--cwd")));
    }),
  );

  it.effect("[VAL-TXT-007] a missing binary returns a typed error", () =>
    Effect.gen(function* () {
      const mock = yield* makeOneShotMockAgent();
      const textGeneration = yield* makeTextGeneration(`${mock.projectDir}/no-such-droid`);

      const error = yield* Effect.flip(
        textGeneration.generateThreadTitle({
          cwd: mock.projectDir,
          message: "Fix the login redirect",
          modelSelection: modelSelection("claude-haiku-5-5"),
        }),
      );

      assert.equal(error._tag, "TextGenerationError");
      assert.include(error.detail, "was not found");
    }),
  );

  it.effect(
    "[VAL-TXT-008] a model Droid rejects fails with Droid's error after exactly one spawn",
    () =>
      Effect.gen(function* () {
        const rejection = "Model 'claude-haiku-5-5' is not available for this account.";
        const mock = yield* makeOneShotMockAgent({
          stdout: resultEnvelope(rejection, true),
          exitCode: 1,
        });
        const textGeneration = yield* makeTextGeneration(mock.binaryPath);

        const error = yield* Effect.flip(
          textGeneration.generateThreadTitle({
            cwd: mock.projectDir,
            message: "Fix the login redirect",
            modelSelection: modelSelection("claude-haiku-5-5"),
          }),
        );

        assert.equal(error._tag, "TextGenerationError");
        assert.include(error.detail, rejection);
        const log = yield* mock.readLog;
        assert.lengthOf(log, 1);
        assert.equal(flagValue(log[0]?.argv ?? [], "-m"), "claude-haiku-5-5");
      }),
  );

  it.effect("passes FACTORY_API_KEY through to the child and never echoes its value", () =>
    Effect.gen(function* () {
      const key = "fk-test-secret-value";
      const mock = yield* makeOneShotMockAgent({
        stdout: resultEnvelope(`Invalid API key ${key}`, true),
        exitCode: 1,
      });
      const textGeneration = yield* makeTextGeneration(mock.binaryPath, { FACTORY_API_KEY: key });

      const error = yield* Effect.flip(
        textGeneration.generateThreadTitle({
          cwd: mock.projectDir,
          message: "Fix the login redirect",
          modelSelection: modelSelection("claude-haiku-5-5"),
        }),
      );

      const [entry] = yield* mock.readLog;
      assert.isTrue(entry?.factoryApiKeySet);
      assert.include(error.detail, "Invalid API key");
      assert.notInclude(error.detail, key);
      assert.notInclude(error.message, key);
      assert.notInclude(String(error.cause), key);
    }),
  );

  it.effect(
    "[VAL-TXT-005] a call still pending at 179,999 ms fails at 180,000 ms after the child is killed and the temp files are removed",
    () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const spawned = yield* Deferred.make<void>();
        const killed = yield* Deferred.make<void>();
        const launched = yield* Ref.make<ChildProcess.StandardCommand | undefined>(undefined);
        const duringTheCall = yield* Ref.make<ReadonlyArray<boolean>>([]);
        const spawner = ChildProcessSpawner.make((command) => {
          const standard = standardCommand(command);
          const child = makeHandle({
            exitCode: Effect.never,
            kill: () => Deferred.succeed(killed, undefined).pipe(Effect.asVoid),
          });
          return Effect.acquireRelease(
            Effect.gen(function* () {
              yield* Ref.set(launched, standard);
              yield* Ref.set(
                duringTheCall,
                yield* Effect.all([
                  fileSystem.exists(flagValue(standard.args, "-f")),
                  fileSystem.exists(flagValue(standard.args, "--settings")),
                  fileSystem.exists(flagValue(standard.args, "--cwd")),
                ]),
              );
              yield* Deferred.succeed(spawned, undefined);
              return child;
            }),
            () => child.kill().pipe(Effect.ignore),
          );
        });
        const textGeneration = yield* makeTextGeneration("droid").pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        );

        const fiber = yield* textGeneration
          .generateThreadTitle({
            cwd: "/project",
            message: "Fix the login redirect",
            modelSelection: modelSelection("claude-haiku-5-5"),
          })
          .pipe(Effect.flip, Effect.forkChild);
        yield* Deferred.await(spawned);

        yield* TestClock.adjust(179_999);
        assert.isUndefined(fiber.pollUnsafe());
        assert.isFalse(yield* Deferred.isDone(killed));
        yield* TestClock.adjust(1);
        const error = yield* Fiber.join(fiber);

        assert.equal(error._tag, "TextGenerationError");
        assert.equal(error.operation, "generateThreadTitle");
        assert.include(error.detail, "timed out");
        assert.isTrue(yield* Deferred.isDone(killed), "the child must be killed");
        assert.deepEqual(yield* Ref.get(duringTheCall), [true, true, true]);
        const command = (yield* Ref.get(launched)) ?? assert.fail("droid was never spawned");
        assert.equal(command.command, "droid");
        assert.equal(command.options.stdin, "ignore");
        assert.equal(command.options.cwd, flagValue(command.args, "--cwd"));
        assert.isFalse(yield* fileSystem.exists(flagValue(command.args, "-f")));
        assert.isFalse(yield* fileSystem.exists(flagValue(command.args, "--settings")));
        assert.isFalse(yield* fileSystem.exists(flagValue(command.args, "--cwd")));
      }),
  );

  it.effect("[VAL-TXT-005] a hung real child process is killed when the call times out", () =>
    Effect.gen(function* () {
      const realSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const spawned = yield* Deferred.make<ChildProcessSpawner.ChildProcessHandle>();
      const spawner = ChildProcessSpawner.make((command) =>
        realSpawner.spawn(command).pipe(Effect.tap((child) => Deferred.succeed(spawned, child))),
      );
      const mock = yield* makeOneShotMockAgent({ hang: true });
      const textGeneration = yield* makeTextGeneration(mock.binaryPath).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );

      const fiber = yield* textGeneration
        .generateThreadTitle({
          cwd: mock.projectDir,
          message: "Fix the login redirect",
          modelSelection: modelSelection("claude-haiku-5-5"),
        })
        .pipe(Effect.flip, Effect.forkChild);
      const child = yield* Deferred.await(spawned);
      assert.isTrue(yield* child.isRunning);

      yield* TestClock.adjust(180_000);
      const error = yield* Fiber.join(fiber);

      assert.equal(error._tag, "TextGenerationError");
      assert.include(error.detail, "timed out");
      assert.isFalse(yield* child.isRunning);
    }),
  );
});
