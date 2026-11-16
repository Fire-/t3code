import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { writeFakeCli } from "@t3tools/provider-testing/fakeCli";

import { ONE_SHOT_MOCK_ENV, type OneShotMockScript } from "./oneShotMockAgent.mock.ts";

const MockFile = Schema.Struct({
  path: Schema.NullOr(Schema.String),
  content: Schema.NullOr(Schema.String),
});

const MockLogEntry = Schema.Struct({
  argv: Schema.Array(Schema.String),
  cwd: Schema.String,
  settingsFile: MockFile,
  promptFile: MockFile,
  factoryApiKeySet: Schema.Boolean,
});

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeMockLogLine = Schema.decodeUnknownSync(Schema.fromJsonString(MockLogEntry));

const mockModuleUrl = new URL("./oneShotMockAgent.mock.ts", import.meta.url).href;

/**
 * A fake `droid` that records one log entry per spawn (argv, cwd, and the settings and
 * prompt files as the child saw them) and then prints the scripted output. The real
 * `droid` is never spawned.
 */
export const makeOneShotMockAgent = Effect.fn("makeOneShotMockAgent")(function* (
  script: OneShotMockScript = {},
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-droid-oneshot-test-" });
  const projectDir = path.join(root, "project");
  yield* fileSystem.makeDirectory(projectDir);
  const logPath = path.join(root, "mock.log");
  const binaryPath = writeFakeCli({
    directory: path.join(root, "bin"),
    name: "droid",
    env: {
      [ONE_SHOT_MOCK_ENV.log]: logPath,
      [ONE_SHOT_MOCK_ENV.script]: encodeJson(script),
    },
    source: [
      `import { runOneShotMockAgent } from ${encodeJson(mockModuleUrl)};`,
      "runOneShotMockAgent();",
    ].join("\n"),
  });
  const readLog = Effect.gen(function* () {
    if (!(yield* fileSystem.exists(logPath))) return [];
    const raw = yield* fileSystem.readFileString(logPath);
    return raw
      .split("\n")
      .filter((line) => line.trim().length > 0)
      .map((line) => decodeMockLogLine(line));
  });
  return { binaryPath, projectDir, readLog };
});
