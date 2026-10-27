import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { execScriptSource, writeFakeCli } from "@t3tools/provider-testing/fakeCli";
import * as HostProcess from "@t3tools/shared/HostProcess";

export type RpcMockLogEntry = Record<string, unknown>;

const decodeLogLine = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);

/**
 * A fake `droid` whose `exec --input-format stream-jsonrpc ...` speaks the carried mock
 * agent's protocol (see the header of `rpcMockAgent.mock.ts` for its triggers and `DROID_MOCK_*`
 * switches). `env` is applied inside the child, on top of whatever the runtime passes.
 */
export const makeRpcMockAgent = Effect.fn("makeRpcMockAgent")(function* (
  env: Record<string, string> = {},
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const platform = yield* HostProcess.Platform;
  const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-droid-rpc-test-" });
  const projectDir = path.join(root, "project");
  yield* fileSystem.makeDirectory(projectDir);
  const logPath = path.join(root, "mock.log");
  const scriptPath = yield* path.fromFileUrl(new URL("./rpcMockAgent.mock.ts", import.meta.url));
  const binaryPath = writeFakeCli({
    directory: path.join(root, "bin"),
    name: "droid",
    env: { DROID_MOCK_LOG: logPath, ...env },
    source: execScriptSource({
      scriptPath,
      expectedArgs: [
        "exec",
        "--input-format",
        "stream-jsonrpc",
        "--output-format",
        "stream-jsonrpc",
      ],
    }),
    platform,
  });
  const readLog = fileSystem.readFileString(logPath).pipe(
    Effect.map((raw) =>
      raw
        .split("\n")
        .filter((line) => line.trim().length > 0)
        .map((line): RpcMockLogEntry => decodeLogLine(line)),
    ),
  );
  return { binaryPath, projectDir, readLog };
});
