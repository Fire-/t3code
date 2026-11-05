import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import { formatTurnTimeLine, prefixTurnTimeLine, stripTurnTimeLine } from "./turnTimeLine.ts";

const at = (iso: string) => DateTime.toEpochMillis(DateTime.makeUnsafe(iso));

describe("formatTurnTimeLine", () => {
  it.each([
    { previous: undefined, line: "[11-04 15:48 PST]" },
    { previous: "2026-11-04T23:48:00Z", line: "[11-04 15:48 PST +0s]" },
    { previous: "2026-11-04T23:49:00Z", line: "[11-04 15:48 PST +0s]" },
    { previous: "2026-11-04T23:47:01Z", line: "[11-04 15:48 PST +59s]" },
    { previous: "2026-11-04T23:45:30Z", line: "[11-04 15:48 PST +2m]" },
    { previous: "2026-11-04T22:48:01Z", line: "[11-04 15:48 PST +59m]" },
    { previous: "2026-11-04T22:48:00Z", line: "[11-04 15:48 PST +1h]" },
    { previous: "2026-11-04T21:43:00Z", line: "[11-04 15:48 PST +2h5m]" },
    { previous: "2026-11-03T23:48:01Z", line: "[11-04 15:48 PST +23h59m]" },
    { previous: "2026-11-03T23:48:00Z", line: "[11-04 15:48 PST +1d]" },
    { previous: "2026-11-01T19:18:00Z", line: "[11-04 15:48 PST +3d4h]" },
  ])("measures the gap since $previous", ({ previous, line }) => {
    expect(
      formatTurnTimeLine({
        sentAt: at("2026-11-04T23:48:00Z"),
        previousAt: previous === undefined ? undefined : at(previous),
        timeZone: "America/Los_Angeles",
      }),
    ).toBe(line);
  });

  it.each([
    { sentAt: "2026-01-02T08:05:00Z", timeZone: "UTC", line: "[01-02 08:05 UTC]" },
    { sentAt: "2026-03-04T00:00:00Z", timeZone: "UTC", line: "[03-04 00:00 UTC]" },
    { sentAt: "2026-11-04T23:48:00Z", timeZone: "Asia/Tokyo", line: "[11-05 08:48 GMT+9]" },
    { sentAt: "2026-11-01T08:50:00Z", timeZone: "America/Los_Angeles", line: "[11-01 01:50 PDT]" },
  ])("writes $sentAt in $timeZone as $line", ({ sentAt, timeZone, line }) => {
    expect(formatTurnTimeLine({ sentAt: at(sentAt), timeZone })).toBe(line);
  });

  it("measures the gap from instants across a daylight saving change", () => {
    expect(
      formatTurnTimeLine({
        sentAt: at("2026-11-01T09:10:00Z"),
        previousAt: at("2026-11-01T08:50:00Z"),
        timeZone: "America/Los_Angeles",
      }),
    ).toBe("[11-01 01:10 PST +20m]");
  });
});

describe("stripTurnTimeLine", () => {
  it.each([
    { text: "[11-04 15:48 PST +2m]\nFix the build", stripped: "Fix the build" },
    { text: "[11-04 15:48 PST]\nFix the build", stripped: "Fix the build" },
    { text: "[11-05 08:48 GMT+9 +3d4h]\n", stripped: "" },
    {
      text: "[11-04 15:48 PST +2m]\n[11-04 15:50 PST +2m]\nFix",
      stripped: "[11-04 15:50 PST +2m]\nFix",
    },
    {
      text: "[11-04 15:48 PST +2m] Fix the build",
      stripped: "[11-04 15:48 PST +2m] Fix the build",
    },
    { text: "[x] done\n- [ ] next", stripped: "[x] done\n- [ ] next" },
    { text: "[11-04 15:48 PST +2x]\nFix", stripped: "[11-04 15:48 PST +2x]\nFix" },
    {
      text: "Fix\n[11-04 15:48 PST +2m]\nthe build",
      stripped: "Fix\n[11-04 15:48 PST +2m]\nthe build",
    },
  ])("strips only a leading stamp line from $text", ({ text, stripped }) => {
    expect(stripTurnTimeLine(text)).toBe(stripped);
  });

  it("removes the line prefixTurnTimeLine added", () => {
    const line = formatTurnTimeLine({
      sentAt: at("2026-11-04T23:48:00Z"),
      previousAt: at("2026-11-04T23:45:30Z"),
      timeZone: "America/Los_Angeles",
    });
    expect(stripTurnTimeLine(prefixTurnTimeLine("[ ] keep this\n", line))).toBe("[ ] keep this\n");
  });
});
