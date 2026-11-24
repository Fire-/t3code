#!/usr/bin/env node
// Run by daemon.sh every ~5 minutes; UPGRADING.md ("When a run dies") describes both jobs.
// macOS can leave Wi-Fi powered on but associated with nothing until someone toggles it, and
// a network drop kills the upgrade agent's provider connection mid-step.
// `--dry-run` prints what one check would do, without acting or saving its counters.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

export const ONLINE_CHECKS_NEEDED = 2;
export const MAX_RESUMES_PER_DAY = 2;
export const RECENT_FAILURE_HOURS = 12;
export const WIFI_DOWN_MINUTES = 10;
export const WIFI_CYCLE_GAP_MINUTES = 30;
const PROBE_URLS = ["https://github.com", "https://api.factory.ai"];
const PROBE_TIMEOUT_MS = 8_000;

const MINUTE = 60_000;
const localDate = (date) => date.toLocaleDateString("sv-SE");

export const resumeMessage = (now) =>
  [
    `upgrade-watch, ${now.toISOString()}: your last turn failed, most likely because the network dropped, and the network has answered ${ONLINE_CHECKS_NEEDED} checks in a row since.`,
    "",
    "Follow UPGRADING.md from preflight. If today's run is unfinished, preflight exits 4: run `preflight --resume` and continue from the first step that has not passed. A conflict resolution left half-done in the upgrade worktree was never checked: read it before you stage it.",
  ].join("\n");

/**
 * Whether to message the upgrade thread now.
 * `run` is the current state.json (or null), `thread` the `t3 thread show --json` thread.
 */
export function decideResume({ config, online, run, thread, resumesToday, now }) {
  if (!config.resume) return { resume: false, reason: "resume is off" };
  if (!config.threadId) return { resume: false, reason: "watch.json has no threadId" };
  if (online.consecutive < ONLINE_CHECKS_NEEDED) return { resume: false, reason: "offline" };
  if (!thread) return { resume: false, reason: "thread not found" };
  if (thread.status !== "failed" || thread.activeRunId)
    return { resume: false, reason: `thread is ${thread.status}` };
  if (thread.pendingRequestCount > 0)
    return { resume: false, reason: "thread is waiting on a question" };
  const failedAt = Date.parse(thread.updatedAt);
  if (!(now.getTime() - failedAt < RECENT_FAILURE_HOURS * 60 * MINUTE))
    return { resume: false, reason: "the failure is old" };
  // A run that is still going was cut off mid-step. Otherwise today's scheduled turn
  // failed before preflight could start a run, which leaves an earlier day's run as
  // current; a blocked run waits for fire either way.
  const today = localDate(now);
  const failedToday = localDate(new Date(failedAt)) === today;
  const unfinished =
    run === null ||
    run.status === "running" ||
    (failedToday && run.id < today && (run.status === "deployed" || run.status === "abandoned"));
  if (!unfinished) return { resume: false, reason: `run ${run.id} is ${run.status}` };
  if (resumesToday >= MAX_RESUMES_PER_DAY)
    return { resume: false, reason: `already resumed ${resumesToday} times today` };
  return { resume: true, reason: "thread failed and the network is back" };
}

/** Whether to power-cycle Wi-Fi now. `wifi` comes from readWifi(). */
export function decideWifiCycle({ config, online, wifi, downSince, lastCycleAt, now }) {
  if (!config.wifiRescue) return { cycle: false, reason: "wifiRescue is off" };
  if (online.ok) return { cycle: false, reason: "online" };
  if (!wifi.device) return { cycle: false, reason: "no Wi-Fi hardware port" };
  if (!wifi.powered) return { cycle: false, reason: "Wi-Fi is off" };
  if (wifi.hasAddress) return { cycle: false, reason: "Wi-Fi has an address" };
  if (wifi.defaultRouteInterface && wifi.defaultRouteInterface !== wifi.device)
    return { cycle: false, reason: `default route is on ${wifi.defaultRouteInterface}` };
  if (downSince === null || now.getTime() - downSince < WIFI_DOWN_MINUTES * MINUTE)
    return { cycle: false, reason: "not down long enough" };
  if (lastCycleAt !== null && now.getTime() - lastCycleAt < WIFI_CYCLE_GAP_MINUTES * MINUTE)
    return { cycle: false, reason: "cycled recently" };
  return { cycle: true, reason: "Wi-Fi is on with no address and nothing else is connected" };
}

/** The watcher's bookkeeping after one check, as plain data. */
export function nextState(prev, { online, wifiDown, now, resumed, cycled }) {
  const today = localDate(now);
  const resumes = prev.resumes?.date === today ? prev.resumes : { date: today, count: 0 };
  return {
    online: { consecutive: online ? (prev.online?.consecutive ?? 0) + 1 : 0 },
    wifiDownSince: wifiDown ? (prev.wifiDownSince ?? now.getTime()) : null,
    lastCycleAt: cycled ? now.getTime() : (prev.lastCycleAt ?? null),
    resumes: resumed ? { date: today, count: resumes.count + 1 } : resumes,
  };
}

// ── Side effects ────────────────────────────────────────────────────────────

const sh = (cmd, args, opts = {}) => {
  const r = NodeChildProcess.spawnSync(cmd, args, { encoding: "utf8", timeout: 30_000, ...opts });
  return { code: r.status ?? 1, out: (r.stdout ?? "").trim(), err: (r.stderr ?? "").trim() };
};

const readJson = (file) => {
  try {
    return JSON.parse(NodeFS.readFileSync(file, "utf8"));
  } catch {
    return null;
  }
};

async function probe() {
  const results = await Promise.all(
    PROBE_URLS.map((url) =>
      fetch(url, { method: "HEAD", signal: AbortSignal.timeout(PROBE_TIMEOUT_MS) }).then(
        () => true,
        () => false,
      ),
    ),
  );
  return results.every(Boolean);
}

export function parseWifiDevice(hardwarePorts) {
  const match = /Hardware Port: Wi-Fi\nDevice: (\S+)/.exec(hardwarePorts);
  return match ? match[1] : null;
}

function readWifi() {
  const device = parseWifiDevice(sh("/usr/sbin/networksetup", ["-listallhardwareports"]).out);
  if (!device) return { device: null };
  const powered = / On$/.test(sh("/usr/sbin/networksetup", ["-getairportpower", device]).out);
  const address = sh("/usr/sbin/ipconfig", ["getifaddr", device]).out;
  const route = sh("/sbin/route", ["-n", "get", "default"]);
  const iface = /interface: (\S+)/.exec(route.out);
  return {
    device,
    powered,
    // 169.254.x.x is the self-assigned address macOS falls back to with no DHCP.
    hasAddress: address !== "" && !address.startsWith("169.254."),
    defaultRouteInterface: route.code === 0 && iface ? iface[1] : null,
  };
}

async function cycleWifi(device) {
  const off = sh("/usr/sbin/networksetup", ["-setairportpower", device, "off"]);
  if (off.code !== 0) return off.err || off.out || `exit ${off.code}`;
  await new Promise((resolve) => setTimeout(resolve, 3_000));
  const on = sh("/usr/sbin/networksetup", ["-setairportpower", device, "on"]);
  return on.code === 0 ? null : on.err || on.out || `exit ${on.code}`;
}

function showThread(t3, threadId) {
  const r = sh(t3, ["thread", "show", "--json", "--messages", "1", threadId]);
  if (r.code !== 0) return null;
  try {
    return JSON.parse(r.out).thread ?? null;
  } catch {
    return null;
  }
}

async function main(dryRun) {
  const runtime = process.env.T3_RUNTIME_DIR ?? NodePath.join(NodeOS.homedir(), ".t3/code");
  const upgrades = process.env.T3_UPGRADE_DIR ?? NodePath.join(runtime, "upgrades");
  const config = readJson(NodePath.join(upgrades, "watch.json"));
  if (!config || (!config.resume && !config.wifiRescue)) return;
  const stateFile = NodePath.join(upgrades, "watch-state.json");
  const prev = readJson(stateFile) ?? {};
  const now = new Date();
  const log = (message) => console.log(`${now.toISOString()} ${message}`);

  const ok = await probe();
  const wifi = config.wifiRescue ? readWifi() : { device: null };
  const wifiDown = !ok && wifi.device !== null && wifi.powered && !wifi.hasAddress;

  const wifiDecision = decideWifiCycle({
    config,
    online: { ok },
    wifi,
    downSince: wifiDown ? (prev.wifiDownSince ?? now.getTime()) : null,
    lastCycleAt: prev.lastCycleAt ?? null,
    now,
  });
  const online = { consecutive: ok ? (prev.online?.consecutive ?? 0) + 1 : 0 };
  const t3 = NodePath.join(runtime, "bin/t3");
  const resumeDecision = () => {
    const current = NodePath.join(upgrades, "current");
    const runId = NodeFS.existsSync(current) ? NodeFS.readFileSync(current, "utf8").trim() : null;
    return decideResume({
      config,
      online,
      run: runId ? readJson(NodePath.join(upgrades, runId, "state.json")) : null,
      thread: config.threadId ? showThread(t3, config.threadId) : null,
      resumesToday: prev.resumes?.date === localDate(now) ? prev.resumes.count : 0,
      now,
    });
  };
  // Reports what this check would do, without acting or touching watch-state.json.
  if (dryRun) {
    console.log(JSON.stringify({ online: ok, wifi, wifiDecision, resume: resumeDecision() }));
    return;
  }
  if (ok !== (prev.online?.consecutive ?? 0) > 0)
    log(ok ? "network answered" : "network is not answering");

  let cycled = false;
  if (wifiDecision.cycle) {
    const error = await cycleWifi(wifi.device);
    cycled = true;
    log(error ? `Wi-Fi power cycle failed: ${error}` : `power-cycled Wi-Fi (${wifi.device})`);
  }

  let resumed = false;
  if (config.resume && config.threadId && online.consecutive >= ONLINE_CHECKS_NEEDED) {
    if (resumeDecision().resume) {
      const sent = sh(t3, ["thread", "send", config.threadId, "-"], {
        input: resumeMessage(now),
      });
      resumed = sent.code === 0;
      log(
        resumed
          ? `resumed the upgrade thread ${config.threadId}`
          : `could not message the upgrade thread: ${sent.err || sent.out}`,
      );
    }
  }

  NodeFS.writeFileSync(
    stateFile,
    `${JSON.stringify(nextState(prev, { online: ok, wifiDown, now, resumed, cycled }), null, 2)}\n`,
  );
}

if (import.meta.main) await main(process.argv.includes("--dry-run"));
