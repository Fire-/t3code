import { describe, expect, it } from "vite-plus/test";

import {
  decideResume,
  decideWifiCycle,
  MAX_RESUMES_PER_DAY,
  nextState,
  parseWifiDevice,
  WIFI_CYCLE_GAP_MINUTES,
  WIFI_DOWN_MINUTES,
} from "./upgrade-watch.mjs";

const MINUTE = 60_000;
// 14:00 PST on 2026-11-18; run ids are local dates.
const now = new Date("2026-11-18T22:00:00Z");
const config = { threadId: "mcp:upgrades", resume: true, wifiRescue: true };
const failedThread = {
  status: "failed",
  activeRunId: null,
  pendingRequestCount: 0,
  updatedAt: "2026-11-18T14:06:49Z",
};
const runningRun = { id: "2026-11-18", status: "running" };
const resumeInput = {
  config,
  online: { consecutive: 2 },
  run: runningRun,
  thread: failedThread,
  resumesToday: 0,
  now,
};

describe("decideResume", () => {
  it("resumes a run the network cut off once the network has answered twice", () => {
    expect(decideResume(resumeInput).resume).toBe(true);
  });

  it("waits while the network has answered only once", () => {
    expect(decideResume({ ...resumeInput, online: { consecutive: 1 } })).toEqual({
      resume: false,
      reason: "offline",
    });
  });

  it("resumes when today's scheduled turn failed before preflight started a run", () => {
    const yesterday = { id: "2026-11-17", status: "deployed" };
    expect(decideResume({ ...resumeInput, run: yesterday }).resume).toBe(true);
  });

  it("does not start tomorrow's run early over a turn that failed last night", () => {
    const afterMidnight = new Date("2026-11-19T08:30:00Z");
    const thread = { ...failedThread, updatedAt: "2026-11-19T06:00:00Z" };
    const run = { id: "2026-11-18", status: "deployed" };
    expect(decideResume({ ...resumeInput, now: afterMidnight, thread, run })).toEqual({
      resume: false,
      reason: "run 2026-11-18 is deployed",
    });
  });

  it("leaves a finished or blocked run alone", () => {
    for (const run of [
      { id: "2026-11-18", status: "deployed" },
      { id: "2026-11-18", status: "abandoned" },
      { id: "2026-11-17", status: "blocked" },
    ]) {
      expect(decideResume({ ...resumeInput, run }).resume).toBe(false);
    }
  });

  it("leaves a thread that is working, finished, or waiting on a question alone", () => {
    for (const thread of [
      { ...failedThread, status: "running", activeRunId: "run:1" },
      { ...failedThread, status: "completed" },
      { ...failedThread, pendingRequestCount: 1 },
    ]) {
      expect(decideResume({ ...resumeInput, thread }).resume).toBe(false);
    }
  });

  it("ignores a failure older than half a day", () => {
    const thread = { ...failedThread, updatedAt: "2026-11-18T09:00:00Z" };
    expect(decideResume({ ...resumeInput, thread }).reason).toBe("the failure is old");
  });

  it("stops after the daily limit", () => {
    expect(decideResume({ ...resumeInput, resumesToday: MAX_RESUMES_PER_DAY }).resume).toBe(false);
  });

  it("does nothing when switched off", () => {
    expect(decideResume({ ...resumeInput, config: { ...config, resume: false } }).resume).toBe(
      false,
    );
  });
});

const stuckWifi = {
  device: "en0",
  powered: true,
  hasAddress: false,
  defaultRouteInterface: null,
};
const wifiInput = {
  config,
  online: { ok: false },
  wifi: stuckWifi,
  downSince: now.getTime() - WIFI_DOWN_MINUTES * MINUTE,
  lastCycleAt: null,
  now,
};

describe("decideWifiCycle", () => {
  it("cycles Wi-Fi that has been on with no address and nothing else connected", () => {
    expect(decideWifiCycle(wifiInput).cycle).toBe(true);
  });

  it("never cycles while ethernet holds the default route", () => {
    const wifi = { ...stuckWifi, defaultRouteInterface: "en7" };
    expect(decideWifiCycle({ ...wifiInput, wifi })).toEqual({
      cycle: false,
      reason: "default route is on en7",
    });
  });

  it("leaves Wi-Fi alone when it has an address (the outage is past the router)", () => {
    const wifi = { ...stuckWifi, hasAddress: true };
    expect(decideWifiCycle({ ...wifiInput, wifi }).cycle).toBe(false);
  });

  it("leaves Wi-Fi alone when it is off, online, or switched off", () => {
    expect(decideWifiCycle({ ...wifiInput, wifi: { ...stuckWifi, powered: false } }).cycle).toBe(
      false,
    );
    expect(decideWifiCycle({ ...wifiInput, online: { ok: true } }).cycle).toBe(false);
    expect(decideWifiCycle({ ...wifiInput, config: { ...config, wifiRescue: false } }).cycle).toBe(
      false,
    );
  });

  it("waits until Wi-Fi has been down long enough, and between cycles", () => {
    expect(decideWifiCycle({ ...wifiInput, downSince: now.getTime() - MINUTE }).cycle).toBe(false);
    const recent = now.getTime() - (WIFI_CYCLE_GAP_MINUTES - 1) * MINUTE;
    expect(decideWifiCycle({ ...wifiInput, lastCycleAt: recent }).cycle).toBe(false);
    const old = now.getTime() - WIFI_CYCLE_GAP_MINUTES * MINUTE;
    expect(decideWifiCycle({ ...wifiInput, lastCycleAt: old }).cycle).toBe(true);
  });
});

describe("nextState", () => {
  it("counts consecutive answers and resets on a miss", () => {
    const once = nextState({}, { online: true, wifiDown: false, now });
    const twice = nextState(once, { online: true, wifiDown: false, now });
    expect(twice.online.consecutive).toBe(2);
    expect(nextState(twice, { online: false, wifiDown: false, now }).online.consecutive).toBe(0);
  });

  it("keeps the time Wi-Fi first went down until it recovers", () => {
    const first = nextState({}, { online: false, wifiDown: true, now });
    const later = new Date(now.getTime() + 5 * MINUTE);
    expect(nextState(first, { online: false, wifiDown: true, now: later }).wifiDownSince).toBe(
      now.getTime(),
    );
    expect(nextState(first, { online: true, wifiDown: false, now: later }).wifiDownSince).toBe(
      null,
    );
  });

  it("counts resumes per local day", () => {
    const resumed = nextState({}, { online: true, wifiDown: false, now, resumed: true });
    expect(resumed.resumes).toEqual({ date: "2026-11-18", count: 1 });
    const nextDay = new Date("2026-11-19T22:00:00Z");
    expect(nextState(resumed, { online: true, wifiDown: false, now: nextDay }).resumes).toEqual({
      date: "2026-11-19",
      count: 0,
    });
  });
});

it("finds the Wi-Fi device in networksetup's hardware port list", () => {
  const ports = [
    "Hardware Port: Thunderbolt Bridge",
    "Device: bridge0",
    "Ethernet Address: 36:00:00:00:00:00",
    "",
    "Hardware Port: Wi-Fi",
    "Device: en0",
    "Ethernet Address: 00:00:00:00:00:00",
  ].join("\n");
  expect(parseWifiDevice(ports)).toBe("en0");
  expect(parseWifiDevice("Hardware Port: Ethernet\nDevice: en7")).toBe(null);
});
