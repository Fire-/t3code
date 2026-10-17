import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import { TestClock } from "effect/testing";
import * as Net from "node:net";

import {
  PrimeRecorderDaemonStopError,
  stopPrimeRecorderDaemon,
  withPrimeRecorderDaemonCleanup,
} from "./primeRecorderDaemon.ts";

const BOUND = Duration.seconds(5);
const SUPERVISOR_PID = 424_242;

/**
 * - `accept`: answers like Prime and removes its socket before `daemon_closing`.
 * - `accept-keep-listening`: answers like Prime but its socket stays.
 * - `refuse`: answers the shutdown with `success: false`.
 * - `silent`: never answers the shutdown.
 */
type FakeDaemonBehavior = "accept" | "accept-keep-listening" | "refuse" | "silent";

interface FakeDaemon {
  readonly socketPath: string;
  readonly shutdownReceived: Deferred.Deferred<string>;
}

/** A short `/tmp` directory: macOS caps unix socket paths near 104 bytes. */
const shortTempDirectory = Effect.acquireRelease(
  Effect.flatMap(Effect.service(FileSystem.FileSystem), (fs) =>
    fs.makeTempDirectory({ directory: "/tmp", prefix: "pv2t-" }),
  ),
  (directory) =>
    Effect.flatMap(Effect.service(FileSystem.FileSystem), (fs) =>
      fs.remove(directory, { recursive: true, force: true }),
    ).pipe(Effect.ignore),
);

const fakeDaemon = (socketPath: string, behavior: FakeDaemonBehavior) =>
  Effect.gen(function* () {
    const shutdownReceived = yield* Deferred.make<string>();
    const connections = new Set<Net.Socket>();
    const server = Net.createServer((connection) => {
      connections.add(connection);
      connection.on("close", () => connections.delete(connection));
      connection.setEncoding("utf8");
      connection.write(
        `${JSON.stringify({
          type: "daemon_hello",
          clientId: "client_1",
          supervisorPid: SUPERVISOR_PID,
          protocol: { name: "prime-agent.daemon", version: 7 },
        })}\n`,
      );
      let buffer = "";
      connection.on("data", (chunk: string) => {
        buffer += chunk;
        const newline = buffer.indexOf("\n");
        if (newline === -1) return;
        const line = buffer.slice(0, newline);
        Deferred.doneUnsafe(shutdownReceived, Effect.succeed(line));
        switch (behavior) {
          case "accept":
            connection.write(`{"type":"response","command":"shutdown","success":true}\n`);
            server.close();
            connection.end(`{"type":"daemon_closing"}\n`);
            return;
          case "accept-keep-listening":
            connection.write(`{"type":"response","command":"shutdown","success":true}\n`);
            connection.end(`{"type":"daemon_closing"}\n`);
            return;
          case "refuse":
            connection.write(
              `{"type":"response","command":"shutdown","success":false,"error":"shutdown refused"}\n`,
            );
            return;
          case "silent":
            return;
        }
      });
    });
    yield* Effect.acquireRelease(
      Effect.callback<void, Error>((resume) => {
        server.once("error", (error) => resume(Effect.fail(error)));
        server.listen(socketPath, () => resume(Effect.void));
      }),
      () =>
        Effect.callback<void>((resume) => {
          for (const connection of connections) connection.destroy();
          if (!server.listening) return resume(Effect.void);
          server.close(() => resume(Effect.void));
        }),
    );
    return { socketPath, shutdownReceived } satisfies FakeDaemon;
  });

const supervisorAlive = (alive: boolean) =>
  Effect.gen(function* () {
    const checked = yield* Deferred.make<number>();
    const isProcessAlive = (pid: number) => Deferred.succeed(checked, pid).pipe(Effect.as(alive));
    return { checked, isProcessAlive };
  });

const withFakeDaemon = <A, E, R>(
  behavior: FakeDaemonBehavior,
  body: (daemon: FakeDaemon, directory: string) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const directory = yield* shortTempDirectory;
    const daemon = yield* fakeDaemon(`${directory}/d.sock`, behavior);
    return yield* body(daemon, directory);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

it.effect("[VAL-FIX-012] a daemon that accepts shutdown and exits is stopped", () =>
  withFakeDaemon("accept", (daemon) =>
    Effect.gen(function* () {
      const supervisor = yield* supervisorAlive(false);
      const result = yield* stopPrimeRecorderDaemon(daemon.socketPath, {
        bound: BOUND,
        isProcessAlive: supervisor.isProcessAlive,
      });
      assert.deepStrictEqual(result, { _tag: "Stopped", supervisorPid: SUPERVISOR_PID });
      const command = JSON.parse(yield* Deferred.await(daemon.shutdownReceived));
      assert.deepStrictEqual(command, {
        type: "command",
        id: "ctl_1",
        protocol: { name: "prime-agent.daemon", version: 7 },
        clientId: "client_1",
        command: { type: "shutdown", id: "ctl_1" },
      });
      assert.strictEqual(yield* Deferred.await(supervisor.checked), SUPERVISOR_PID);
    }),
  ),
);

it.effect("[VAL-FIX-012] no socket means no daemon was started", () =>
  Effect.gen(function* () {
    const directory = yield* shortTempDirectory;
    const result = yield* stopPrimeRecorderDaemon(`${directory}/none.sock`);
    assert.deepStrictEqual(result, { _tag: "NotStarted" });
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("[VAL-FIX-012] a refused shutdown fails with the socket path and supervisor pid", () =>
  withFakeDaemon("refuse", (daemon) =>
    Effect.gen(function* () {
      const error = yield* stopPrimeRecorderDaemon(daemon.socketPath, { bound: BOUND }).pipe(
        Effect.flip,
      );
      assert.instanceOf(error, PrimeRecorderDaemonStopError);
      assert.strictEqual(error.socketPath, daemon.socketPath);
      assert.strictEqual(error.supervisorPid, SUPERVISOR_PID);
      assert.include(error.detail, "shutdown refused");
      assert.include(error.message, daemon.socketPath);
      assert.include(error.message, String(SUPERVISOR_PID));
    }),
  ),
);

it.effect("[VAL-FIX-012] a socket error fails with the socket path", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* shortTempDirectory;
    // A plain file where the socket should be: connecting to it fails.
    const socketPath = `${directory}/d.sock`;
    yield* fs.writeFileString(socketPath, "");
    const error = yield* stopPrimeRecorderDaemon(socketPath, { bound: BOUND }).pipe(Effect.flip);
    assert.strictEqual(error.socketPath, socketPath);
    assert.isNull(error.supervisorPid);
    assert.include(error.detail, "daemon socket error");
    assert.include(error.message, socketPath);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("[VAL-FIX-012] a daemon that never answers shutdown fails once the bound passes", () =>
  withFakeDaemon("silent", (daemon) =>
    Effect.gen(function* () {
      const fiber = yield* stopPrimeRecorderDaemon(daemon.socketPath, { bound: BOUND }).pipe(
        Effect.flip,
        Effect.forkScoped,
      );
      yield* Deferred.await(daemon.shutdownReceived);
      yield* TestClock.adjust(BOUND);
      const error = yield* Fiber.join(fiber);
      assert.strictEqual(error.socketPath, daemon.socketPath);
      assert.strictEqual(error.supervisorPid, SUPERVISOR_PID);
      assert.include(error.detail, "no daemon_closing");
    }),
  ),
);

it.effect("[VAL-FIX-012] a supervisor still alive after the bound fails with its pid", () =>
  withFakeDaemon("accept", (daemon) =>
    Effect.gen(function* () {
      const supervisor = yield* supervisorAlive(true);
      const fiber = yield* stopPrimeRecorderDaemon(daemon.socketPath, {
        bound: BOUND,
        isProcessAlive: supervisor.isProcessAlive,
      }).pipe(Effect.flip, Effect.forkScoped);
      yield* Deferred.await(supervisor.checked);
      yield* TestClock.adjust(BOUND);
      const error = yield* Fiber.join(fiber);
      assert.strictEqual(error.socketPath, daemon.socketPath);
      assert.strictEqual(error.supervisorPid, SUPERVISOR_PID);
      assert.include(error.detail, "supervisor");
      assert.notInclude(error.detail, "socket file");
    }),
  ),
);

it.effect("[VAL-FIX-012] a socket still present after the bound fails", () =>
  withFakeDaemon("accept-keep-listening", (daemon) =>
    Effect.gen(function* () {
      const supervisor = yield* supervisorAlive(false);
      const fiber = yield* stopPrimeRecorderDaemon(daemon.socketPath, {
        bound: BOUND,
        isProcessAlive: supervisor.isProcessAlive,
      }).pipe(Effect.flip, Effect.forkScoped);
      yield* Deferred.await(supervisor.checked);
      yield* TestClock.adjust(BOUND);
      const error = yield* Fiber.join(fiber);
      assert.strictEqual(error.supervisorPid, SUPERVISOR_PID);
      assert.include(error.detail, "socket file");
    }),
  ),
);

it.effect(
  "[VAL-FIX-012] a failed stop after a failed recording keeps the workspace and reports both errors",
  () =>
    withFakeDaemon("refuse", (daemon, directory) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const root = `${directory}/recording`;
        yield* fs.makeDirectory(`${root}/ws`, { recursive: true });
        const exit = yield* withPrimeRecorderDaemonCleanup(Effect.fail("recording broke"), {
          socketPath: daemon.socketPath,
          root,
          workspace: `${root}/ws`,
          invocations: [],
          stopOptions: { bound: BOUND },
        }).pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(exit));
        if (!Exit.isFailure(exit)) return;
        const errors = exit.cause.reasons.filter(Cause.isFailReason).map((reason) => reason.error);
        assert.include(errors, "recording broke");
        const stopError = errors.find(Schema.is(PrimeRecorderDaemonStopError));
        assert.instanceOf(stopError, PrimeRecorderDaemonStopError);
        assert.strictEqual(stopError?.supervisorPid, SUPERVISOR_PID);
        assert.isTrue(yield* fs.exists(`${root}/ws`));
      }),
    ),
);

it.effect("[VAL-FIX-012] a failed stop after a successful recording still fails", () =>
  withFakeDaemon("refuse", (daemon, directory) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const root = `${directory}/recording`;
      yield* fs.makeDirectory(`${root}/ws`, { recursive: true });
      const error = yield* withPrimeRecorderDaemonCleanup(Effect.succeed("recorded"), {
        socketPath: daemon.socketPath,
        root,
        workspace: `${root}/ws`,
        invocations: [],
        stopOptions: { bound: BOUND },
      }).pipe(Effect.flip);
      assert.instanceOf(error, PrimeRecorderDaemonStopError);
      assert.isTrue(yield* fs.exists(`${root}/ws`));
    }),
  ),
);

it.effect(
  "[VAL-FIX-012] a stopped daemon lets the recording result through and removes the workspace",
  () =>
    withFakeDaemon("accept", (daemon, directory) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const root = `${directory}/recording`;
        yield* fs.makeDirectory(`${root}/ws`, { recursive: true });
        const supervisor = yield* supervisorAlive(false);
        const result = yield* withPrimeRecorderDaemonCleanup(Effect.succeed("recorded"), {
          socketPath: daemon.socketPath,
          root,
          workspace: `${root}/ws`,
          invocations: [],
          stopOptions: { bound: BOUND, isProcessAlive: supervisor.isProcessAlive },
        });
        assert.strictEqual(result, "recorded");
        assert.isFalse(yield* fs.exists(root));
      }),
    ),
);
