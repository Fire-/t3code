import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";

import { ThreadBackgroundLivenessService } from "./orchestration/ThreadBackgroundLiveness.ts";

export const DAEMON_BACKGROUND_WORK_PATH = "/api/daemon/background-work";

const isLoopbackAddress = (address: string): boolean => {
  const ip = address.startsWith("::ffff:") ? address.slice("::ffff:".length) : address;
  return ip === "::1" || ip.startsWith("127.");
};

/**
 * `{backgroundWorkThreads}`: how many threads still have background work (subagents,
 * workflows, watch loops) running after their turn settled. `scripts/daemon.sh` asks
 * before an idle-gated restart, because this lives only in server memory and its database
 * check cannot see it. Loopback callers only; anyone else gets a 404.
 */
export const daemonBackgroundWorkHandler = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  if (!Option.exists(request.remoteAddress, isLoopbackAddress)) {
    return HttpServerResponse.text("Not Found", { status: 404 });
  }
  const liveness = yield* ThreadBackgroundLivenessService;
  return HttpServerResponse.jsonUnsafe(
    { backgroundWorkThreads: liveness.countThreadsWithLiveWork() },
    { headers: { "Cache-Control": "no-store" } },
  );
});

export const daemonBackgroundWorkRouteLayer = HttpRouter.add(
  "GET",
  DAEMON_BACKGROUND_WORK_PATH,
  daemonBackgroundWorkHandler,
);
