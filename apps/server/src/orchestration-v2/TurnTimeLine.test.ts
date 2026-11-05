import { assert, it } from "@effect/vitest";
import { ScheduledTaskId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";

import * as TurnTimeLine from "./TurnTimeLine.ts";

const at = DateTime.makeUnsafe;
const losAngeles = Effect.succeed("America/Los_Angeles");
const unread = Effect.die("previous times must not be read");

it.effect.each([
  {
    label: "after the latest earlier time",
    previousTimes: Effect.succeed([
      at("2026-09-04T11:50:00Z"),
      null,
      at("2026-09-04T12:00:00Z"),
      at("2026-09-04T12:02:30Z"),
      at("2026-09-04T12:05:00Z"),
    ]),
    sent: "[09-04 05:02 PDT +2m]\nhello",
  },
  {
    label: "without a gap first",
    previousTimes: Effect.succeed([]),
    sent: "[09-04 05:02 PDT]\nhello",
  },
  { label: "never for a slash command", text: "  /compact", previousTimes: unread, sent: "hello" },
  {
    label: "never for an agent message",
    message: { createdBy: "agent" as const },
    previousTimes: unread,
    sent: "hello",
  },
  {
    label: "never for a scheduled task",
    message: { scheduledTaskId: ScheduledTaskId.make("scheduled-task") },
    previousTimes: unread,
    sent: "hello",
  },
  { label: "never without a zone", timeZone: null, previousTimes: unread, sent: "hello" },
])("stamps the provider text $label", (row) =>
  Effect.gen(function* () {
    const sent = yield* TurnTimeLine.stampProviderText({
      timeZone: row.timeZone === null ? undefined : losAngeles,
      message: {
        text: row.text ?? "hello",
        createdBy: "user",
        createdAt: at("2026-09-04T12:02:30Z"),
        ...row.message,
      },
      text: "hello",
      previousTimes: row.previousTimes,
    });
    assert.equal(sent, row.sent);
  }),
);
