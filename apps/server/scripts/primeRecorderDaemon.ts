import * as Console from "effect/Console";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Net from "node:net";

const PRIME_DAEMON_PROTOCOL_NAME = "prime-agent.daemon";
const DEFAULT_STOP_BOUND = Duration.seconds(10);

export class PrimeRecorderDaemonStopError extends Schema.TaggedError<PrimeRecorderDaemonStopError>()(
  "PrimeRecorderDaemonStopError",
  {
    socketPath: Schema.String,
    supervisorPid: Schema.NullOr(Schema.Number),
    detail: Schema.String,
  },
) {
  override get message(): string {
    return `private daemon on ${this.socketPath} (supervisorPid ${this.supervisorPid ?? "unknown"}) did not stop: ${this.detail}`;
  }
}

interface StopOptions {
  /**
   * How long to wait for a socket next to a leftover lock, then for
   * `daemon_closing`, and again for the socket, lock and supervisor to go.
   */
  readonly bound?: Duration.Duration;
  readonly isProcessAlive?: (pid: number) => Effect.Effect<boolean>;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

const processAlive = (pid: number) =>
  Effect.sync(() => {
    try {
      process.kill(pid, 0);
      return true;
    } catch (error) {
      return (error as NodeJS.ErrnoException).code !== "ESRCH";
    }
  });

/**
 * Prime's own per-socket stop (`cli/daemon-launch.ts` `shutdownDaemonAndWait`):
 * read `daemon_hello`, send one `shutdown` command for that client, and wait
 * for `daemon_closing`. `prime-agent shutdown` would stop every daemon, the
 * user's included, so it is never used.
 */
const sendDaemonShutdown = (socketPath: string, onHello: (supervisorPid: number) => void) =>
  Effect.callback<{ readonly accepted: boolean }, string>((resume) => {
    const socket = Net.createConnection(socketPath);
    let buffer = "";
    let helloSeen = false;
    let accepted = false;
    let settled = false;
    const finish = (effect: Effect.Effect<{ readonly accepted: boolean }, string>) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      resume(effect);
    };
    socket.setEncoding("utf8");
    socket.on("data", (chunk: string) => {
      buffer += chunk;
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      for (const line of lines) {
        let message: unknown;
        try {
          message = JSON.parse(line);
        } catch {
          continue;
        }
        if (!isRecord(message)) continue;
        if (message.type === "daemon_hello" && !helloSeen) {
          const protocol = isRecord(message.protocol) ? message.protocol : {};
          if (typeof message.clientId !== "string" || typeof message.supervisorPid !== "number") {
            finish(Effect.fail("daemon_hello lacks clientId or supervisorPid"));
            return;
          }
          helloSeen = true;
          onHello(message.supervisorPid);
          const version = typeof protocol.version === "number" ? protocol.version : 7;
          socket.write(
            `${JSON.stringify({
              type: "command",
              id: "ctl_1",
              protocol: { name: PRIME_DAEMON_PROTOCOL_NAME, version },
              clientId: message.clientId,
              command: { type: "shutdown", id: "ctl_1" },
            })}\n`,
          );
        } else if (message.type === "response" && message.command === "shutdown") {
          accepted = message.success === true;
          if (!accepted) {
            finish(Effect.fail(`daemon refused shutdown: ${line}`));
            return;
          }
        } else if (message.type === "daemon_closing" && helloSeen) {
          finish(Effect.succeed({ accepted }));
          return;
        }
      }
    });
    socket.on("error", (error) => finish(Effect.fail(`daemon socket error: ${error.message}`)));
    // A close without daemon_closing is judged by the exit check that follows.
    socket.on("close", () =>
      finish(
        helloSeen
          ? Effect.succeed({ accepted })
          : Effect.fail("daemon socket closed before daemon_hello"),
      ),
    );
    return Effect.sync(() => socket.destroy());
  });

/**
 * Stops the recorder's private daemon and confirms its socket, lock and
 * supervisor are gone. Every way that can fall short fails with the socket
 * path and, once the daemon said hello, its supervisor pid. NotStarted means
 * neither the socket nor its lock exists.
 */
export const stopPrimeRecorderDaemon = (socketPath: string, options: StopOptions = {}) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const bound = options.bound ?? DEFAULT_STOP_BOUND;
    const isProcessAlive = options.isProcessAlive ?? processAlive;
    // Filled in by daemon_hello, so a later timeout still reports the pid.
    const daemon: { supervisorPid: number | null } = { supervisorPid: null };
    const stopError = (detail: string) =>
      new PrimeRecorderDaemonStopError({ socketPath, supervisorPid: daemon.supervisorPid, detail });
    const exists = (path: string) =>
      fs
        .exists(path)
        .pipe(Effect.mapError((error) => stopError(`checking ${path} failed: ${error.message}`)));

    const lockPath = `${socketPath}.lock`;

    // A lock without a socket is a daemon still starting (it may listen after
    // the recording gave up) or one that died uncleanly. Prime's lock is a
    // proper-lockfile directory (modes/daemon/daemon-socket.ts
    // acquireDaemonSocketPathLease), so it names no pid.
    const presence = Effect.gen(function* () {
      if (yield* exists(socketPath)) return "socket" as const;
      return (yield* exists(lockPath)) ? ("lock-only" as const) : ("none" as const);
    });
    const last: { state: "socket" | "lock-only" | "none" } = { state: "lock-only" };
    const resolved = yield* presence.pipe(
      Effect.tap((state) =>
        Effect.sync(() => {
          last.state = state;
        }),
      ),
      Effect.repeat({
        until: (state) => state !== "lock-only",
        schedule: Schedule.spaced(Duration.millis(100)),
      }),
      Effect.as(true),
      Effect.timeoutOrElse({ duration: bound, orElse: () => Effect.succeed(false) }),
    );
    if (!resolved) {
      return yield* stopError(
        `${lockPath} still present without a socket ${Duration.format(bound)} after the recording ended (the daemon may still be starting or died uncleanly)`,
      );
    }
    if (last.state === "none") return { _tag: "NotStarted" } as const;

    yield* sendDaemonShutdown(socketPath, (pid) => {
      daemon.supervisorPid = pid;
    }).pipe(
      Effect.timeoutOrElse({
        duration: bound,
        orElse: () => Effect.fail(`no daemon_closing within ${Duration.format(bound)}`),
      }),
      Effect.mapError(stopError),
    );
    const pid = daemon.supervisorPid;
    if (pid === null) return yield* stopError("daemon closed without a supervisorPid");

    const leftovers = Effect.gen(function* () {
      const left: Array<string> = [];
      if (yield* exists(socketPath)) left.push("socket file");
      if (yield* exists(`${socketPath}.lock`)) left.push("lock file");
      if (yield* isProcessAlive(pid)) left.push("supervisor process");
      return left;
    });
    let lastLeftovers: ReadonlyArray<string> = [];
    const stopped = yield* leftovers.pipe(
      Effect.tap((left) =>
        Effect.sync(() => {
          lastLeftovers = left;
        }),
      ),
      Effect.repeat({
        until: (left) => left.length === 0,
        schedule: Schedule.spaced(Duration.millis(100)),
      }),
      Effect.as(true),
      Effect.timeoutOrElse({ duration: bound, orElse: () => Effect.succeed(false) }),
    );
    if (!stopped) {
      return yield* stopError(
        `${lastLeftovers.join(", ")} still present ${Duration.format(bound)} after shutdown`,
      );
    }
    return { _tag: "Stopped", supervisorPid: pid } as const;
  });

/**
 * Runs a recording, then stops its private daemon on every exit path. The
 * recording directory is removed only once the daemon is confirmed gone; a
 * failed stop keeps it, prints what is needed to stop the daemon by hand, and
 * fails the run (alongside the recording's own failure, if any).
 */
export const withPrimeRecorderDaemonCleanup = <A, E, R>(
  recording: Effect.Effect<A, E, R>,
  input: {
    readonly socketPath: string;
    readonly root: string;
    readonly workspace: string;
    readonly invocations: ReadonlyArray<string>;
    readonly stopOptions?: StopOptions;
  },
) =>
  recording.pipe(
    Effect.onExit((exit) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        for (const invocation of input.invocations) yield* Console.log(`invocation: ${invocation}`);
        const result = yield* stopPrimeRecorderDaemon(input.socketPath, input.stopOptions).pipe(
          Effect.tapError((error) =>
            Console.error(
              [
                `private daemon: FAILED to stop: ${error.detail}`,
                `  socket: ${error.socketPath}`,
                `  supervisorPid: ${error.supervisorPid ?? "unknown (no daemon_hello)"}`,
                `  kept recording directory: ${input.root} (workspace ${input.workspace})`,
                "  Stop it with the per-socket shutdown message; never with `prime-agent shutdown`.",
                ...(Exit.isFailure(exit)
                  ? ["  The recording failed as well; both errors follow."]
                  : []),
              ].join("\n"),
            ),
          ),
        );
        yield* Console.log(
          result._tag === "NotStarted"
            ? `private daemon: none was started on ${input.socketPath}`
            : `private daemon: supervisorPid ${result.supervisorPid} exited; ${input.socketPath} and its lock removed`,
        );
        yield* fs.remove(input.root, { recursive: true, force: true }).pipe(Effect.ignore);
      }),
    ),
  );
