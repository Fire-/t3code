import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";

import {
  DAEMON_BACKGROUND_WORK_PATH,
  daemonBackgroundWorkHandler,
} from "./daemonBackgroundWork.ts";
import * as ThreadBackgroundLiveness from "./orchestration/ThreadBackgroundLiveness.ts";

const ask = Effect.fnUntraced(function* (remoteAddress: string | undefined) {
  const liveness = ThreadBackgroundLiveness.make();
  liveness.recordTaskLiveness({
    threadId: "thread",
    taskId: "child",
    taskType: "subagent",
    status: undefined,
    kind: "started",
  });
  const request = HttpServerRequest.fromWeb(
    new Request(`http://127.0.0.1:3773${DAEMON_BACKGROUND_WORK_PATH}`),
  ).modify({ remoteAddress: Option.fromUndefinedOr(remoteAddress) });
  const response = yield* daemonBackgroundWorkHandler.pipe(
    Effect.provideService(HttpServerRequest.HttpServerRequest, request),
    Effect.provideService(ThreadBackgroundLiveness.ThreadBackgroundLivenessService, liveness),
  );
  return HttpServerResponse.toWeb(response);
});

describe("daemon background-work route", () => {
  for (const address of ["127.0.0.1", "::ffff:127.0.0.1", "::1"]) {
    it.effect(`answers loopback caller ${address}`, () =>
      Effect.gen(function* () {
        const response = yield* ask(address);
        expect(response.status).toBe(200);
        expect(yield* Effect.promise(() => response.json())).toEqual({
          backgroundWorkThreads: 1,
        });
      }),
    );
  }

  for (const address of ["192.168.1.20", "::ffff:100.64.0.7", undefined]) {
    it.effect(`hides itself from caller ${address}`, () =>
      Effect.gen(function* () {
        const response = yield* ask(address);
        expect(response.status).toBe(404);
      }),
    );
  }
});
