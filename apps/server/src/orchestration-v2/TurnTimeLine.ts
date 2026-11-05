import type { OrchestrationV2ConversationMessage } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { formatTurnTimeLine, prefixTurnTimeLine } from "@t3tools/shared/turnTimeLine";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";

/** The zone prompts are stamped in. Unset sends prompts exactly as written. */
export class TurnTimeZone extends Context.Reference<Effect.Effect<string> | undefined>(
  "t3/orchestration-v2/TurnTimeLine/TurnTimeZone",
  { defaultValue: () => undefined },
) {}

const validTimeZone = (zone: string | undefined) => {
  if (zone === undefined) return undefined;
  try {
    return Intl.DateTimeFormat("en-US", { timeZone: zone }).resolvedOptions().timeZone;
  } catch {
    return undefined;
  }
};

const runtimeTimeZone = () =>
  validTimeZone(Intl.DateTimeFormat().resolvedOptions().timeZone) ?? "UTC";

export const layerMachineTimeZone = Layer.effect(
  TurnTimeZone,
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const platform = yield* HostProcess.Platform;
    if (platform !== "darwin" && platform !== "linux") {
      return Effect.sync(runtimeTimeZone);
    }
    // A running Node process keeps the zone it started with (nodejs/node#33805).
    return fileSystem.readLink("/etc/localtime").pipe(
      Effect.map((target) => validTimeZone(target.split("zoneinfo/")[1]) ?? runtimeTimeZone()),
      Effect.orElseSucceed(runtimeTimeZone),
    );
  }),
);

type StampedMessage = Pick<
  OrchestrationV2ConversationMessage,
  "text" | "createdBy" | "scheduledTaskId" | "createdAt" | "timeZone"
>;

const stampsMessage = (message: StampedMessage) =>
  message.createdBy === "user" &&
  message.scheduledTaskId === undefined &&
  !message.text.trimStart().startsWith("/");

/** Prefixes when a person sent the message, in their device's zone, and how long after the latest earlier time. */
export const stampProviderText = Effect.fn("TurnTimeLine.stampProviderText")(function* <E>(input: {
  readonly timeZone: Effect.Effect<string> | undefined;
  readonly message: StampedMessage;
  readonly text: string;
  readonly previousTimes: Effect.Effect<ReadonlyArray<DateTime.Utc | null | undefined>, E>;
}) {
  if (input.timeZone === undefined || !stampsMessage(input.message)) return input.text;
  const sentAt = DateTime.toEpochMillis(input.message.createdAt);
  const earlier = (yield* input.previousTimes).flatMap((time) => {
    if (time == null) return [];
    const millis = DateTime.toEpochMillis(time);
    return millis < sentAt ? [millis] : [];
  });
  const line = formatTurnTimeLine({
    sentAt,
    previousAt: earlier.length === 0 ? undefined : Math.max(...earlier),
    timeZone: validTimeZone(input.message.timeZone) ?? (yield* input.timeZone),
  });
  return prefixTurnTimeLine(input.text, line);
});
