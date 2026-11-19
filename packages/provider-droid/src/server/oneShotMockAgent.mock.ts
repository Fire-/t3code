// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off globalTimers:off - runs as the body of a fake `droid` child process, outside Effect.
import * as NodeFS from "node:fs";

/**
 * The body of the fake `droid` that one-shot (`droid exec -o json`) tests spawn. It only
 * imports Node built-ins because the child loads this file by URL, not through Effect.
 */
export const ONE_SHOT_MOCK_ENV = {
  log: "DROID_MOCK_LOG",
  script: "DROID_MOCK_SCRIPT",
} as const;

export interface OneShotMockScript {
  readonly stdout?: string;
  readonly stderr?: string;
  readonly exitCode?: number;
  /** Keep running until the parent kills the process. */
  readonly hang?: boolean;
}

export interface OneShotMockFile {
  readonly path: string | null;
  readonly content: string | null;
}

export interface OneShotMockLogEntry {
  readonly argv: ReadonlyArray<string>;
  readonly cwd: string;
  readonly settingsFile: OneShotMockFile;
  readonly promptFile: OneShotMockFile;
  /** Whether the variable reached the child; its value is never logged. */
  readonly factoryApiKeySet: boolean;
}

const flagValue = (argv: ReadonlyArray<string>, ...names: ReadonlyArray<string>) => {
  const index = argv.findIndex((arg) => names.includes(arg));
  return index < 0 ? null : (argv[index + 1] ?? null);
};

const readFile = (path: string | null): OneShotMockFile => {
  if (path === null) return { path, content: null };
  try {
    return { path, content: NodeFS.readFileSync(path, "utf8") };
  } catch {
    return { path, content: null };
  }
};

/** @public Imported by URL from the stub that `makeOneShotMockAgent` writes, never by a module. */
export function runOneShotMockAgent(): void {
  const argv = process.argv.slice(2);
  const logPath = process.env[ONE_SHOT_MOCK_ENV.log];
  const script: OneShotMockScript = JSON.parse(process.env[ONE_SHOT_MOCK_ENV.script] ?? "{}");

  // Read the files now: the parent removes them as soon as the call ends.
  const entry: OneShotMockLogEntry = {
    argv,
    cwd: process.cwd(),
    settingsFile: readFile(flagValue(argv, "--settings")),
    promptFile: readFile(flagValue(argv, "-f", "--file")),
    factoryApiKeySet: (process.env["FACTORY_API_KEY"] ?? "") !== "",
  };
  if (logPath !== undefined) {
    NodeFS.appendFileSync(logPath, `${JSON.stringify(entry)}\n`, "utf8");
  }

  if (script.hang === true) {
    // A pending timer keeps the event loop alive until SIGTERM.
    setInterval(() => undefined, 60_000);
    return;
  }
  NodeFS.writeSync(1, script.stdout ?? "");
  NodeFS.writeSync(2, script.stderr ?? "");
  process.exit(script.exitCode ?? 0);
}
