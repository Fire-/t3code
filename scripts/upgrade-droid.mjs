// Droid only updates itself from its own terminal UI, so a Droid that only T3 runs never
// updates. preflight updates it, keeps the version it replaced, and puts that one back when
// the new one has lost a method T3's adapter calls or lists no models.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

export const BACKUPS_KEPT = 2;
const API_VERSION = "1.0.0";
const PROTOCOL_VERSION = "1.269.0";
// Droid starts answering only after its startup, which waits on Factory's backend.
const PROBE_TIMEOUT_MS = 180_000;
// Droid sends these to T3, so asking Droid for them proves nothing.
const INBOUND = new Set([
  "droid.session_notification",
  "droid.request_permission",
  "droid.ask_user",
  "droid.pick_options",
]);
// zod rejects a string as params before anything runs: present, but invalid params.
const PRESENT = -32602;

const versionOf = (text) => text.match(/\d+\.\d+\.\d+[\w.-]*/)?.[0] ?? null;
export const parseUpdateCheck = (text) =>
  text.match(/Update available:\s*v?(\d+\.\d+\.\d+[\w.-]*)/)?.[1] ?? null;
export function compareVersions(a, b) {
  const pa = a.split(/[.-]/).map(Number);
  const pb = b.split(/[.-]/).map(Number);
  for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
    const d = (pa[i] || 0) - (pb[i] || 0);
    if (d) return d;
  }
  return 0;
}

/** The `droid.*` methods the adapter sends, read from its source so the list cannot go stale. */
export function adapterMethods(srcDir) {
  if (!NodeFS.existsSync(srcDir)) return [];
  const found = new Set();
  for (const file of NodeFS.readdirSync(srcDir, { recursive: true })) {
    if (!/\.ts$/.test(file) || /\.(test|mock|testkit)\.ts$/.test(file)) continue;
    const text = NodeFS.readFileSync(NodePath.join(srcDir, file), "utf8");
    for (const [, m] of text.matchAll(/"(droid\.[a-z_]+)"/g)) if (!INBOUND.has(m)) found.add(m);
  }
  return [...found].sort();
}

const run = (bin, args) => {
  const r = NodeChildProcess.spawnSync(bin, args, { encoding: "utf8", timeout: 600_000 });
  return { code: r.status ?? 1, out: `${r.stdout ?? ""}${r.stderr ?? ""}`.trim() };
};
const readState = (file) => {
  try {
    return { held: null, rejected: [], ...JSON.parse(NodeFS.readFileSync(file, "utf8")) };
  } catch {
    return { held: null, rejected: [] };
  }
};
const writeState = (file, state) => {
  NodeFS.mkdirSync(NodePath.dirname(file), { recursive: true });
  NodeFS.writeFileSync(file, `${JSON.stringify(state, null, 2)}\n`);
};
// Staged on the same filesystem so the rename leaves running droid processes on the old file.
function replaceBinary(from, bin) {
  const staged = NodePath.join(NodePath.dirname(bin), `.droid-restore-${process.pid}`);
  NodeFS.copyFileSync(from, staged);
  NodeFS.chmodSync(staged, 0o755);
  NodeFS.renameSync(staged, bin);
}
const backupPath = (backups, version) => NodePath.join(backups, `droid-${version}`);
function pruneBackups(backups) {
  const all = NodeFS.readdirSync(backups)
    .filter((f) => /^droid-\d/.test(f))
    .sort((a, b) => compareVersions(b.slice(6), a.slice(6)));
  for (const f of all.slice(BACKUPS_KEPT)) NodeFS.rmSync(NodePath.join(backups, f));
}

/** Asks one `droid exec` for every method without running any, plus a real `list_models`. */
function probe(bin, methods, cwd) {
  return new Promise((resolve) => {
    const child = NodeChildProcess.spawn(
      bin,
      ["exec", "--input-format", "stream-jsonrpc", "--output-format", "stream-jsonrpc"],
      { cwd, stdio: ["pipe", "pipe", "ignore"] },
    );
    const replies = new Map();
    const pending = new Set();
    let buf = "";
    let settled = false;
    const finish = (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      child.kill();
      const missing = methods.filter(
        (m) => m !== "droid.list_models" && replies.get(m)?.error?.code !== PRESENT,
      );
      const models = replies.get("droid.list_models")?.result?.models;
      resolve({ missing, models: Array.isArray(models) ? models.length : 0, error });
    };
    const timer = setTimeout(() => finish("did not answer in time"), PROBE_TIMEOUT_MS);
    child.on("error", (e) => finish(e.message));
    child.on("exit", () => finish(pending.size ? "exited before answering" : undefined));
    child.stdout.on("data", (chunk) => {
      buf += chunk;
      let i;
      while ((i = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, i).trim();
        buf = buf.slice(i + 1);
        let frame;
        try {
          frame = JSON.parse(line);
        } catch {
          continue;
        }
        if (frame.type !== "response" || !pending.has(frame.id)) continue;
        pending.delete(frame.id);
        replies.set(asks[Number(frame.id) - 1], frame);
        if (!pending.size) finish();
      }
    });
    const asks = [...new Set([...methods, "droid.list_models"])];
    for (const [i, method] of asks.entries()) {
      const id = String(i + 1);
      pending.add(id);
      const frame = {
        type: "request",
        jsonrpc: "2.0",
        factoryApiVersion: API_VERSION,
        factoryProtocolVersion: PROTOCOL_VERSION,
        id,
        method,
        params: method === "droid.list_models" ? {} : "__probe_invalid__",
      };
      child.stdin.write(`${JSON.stringify(frame)}\n`);
    }
  });
}

/**
 * Updates droid when a newer version is offered. Results: `current`, `held` (restored by hand
 * and not to be updated until released), `rejected` (the offered version was rolled back
 * before), `updated`, `rolled-back`, or `failed` (the update itself did not install).
 */
export async function updateDroid({ bin, backups, stateFile, methods, cwd }) {
  const version = versionOf(run(bin, ["--version"]).out);
  if (!version) return { status: "failed", reason: `cannot read the version of ${bin}` };
  const state = readState(stateFile);
  if (state.held) return { status: "held", version, ...state.held };
  const check = run(bin, ["update", "--check"]);
  const offered = parseUpdateCheck(check.out);
  if (!offered || compareVersions(offered, version) <= 0) return { status: "current", version };
  const rejected = state.rejected.find((r) => r.version === offered);
  if (rejected) return { status: "rejected", version, offered, reason: rejected.reason };

  NodeFS.mkdirSync(backups, { recursive: true });
  const backup = backupPath(backups, version);
  if (!NodeFS.existsSync(backup)) NodeFS.copyFileSync(bin, backup);
  NodeFS.chmodSync(backup, 0o755);
  const installed = run(bin, ["update", "-v", offered]);
  const now = versionOf(run(bin, ["--version"]).out);
  if (installed.code !== 0 || now !== offered) {
    if (now !== version) replaceBinary(backup, bin);
    return {
      status: "failed",
      version,
      offered,
      reason: `droid update -v ${offered} did not install: ${installed.out.split("\n").at(-1)}`,
    };
  }

  const result = await probe(bin, methods, cwd);
  // Without every answer, the unanswered methods only look missing.
  const problems = result.error
    ? [result.error]
    : [
        result.missing.length && `lacks ${result.missing.join(", ")}`,
        !result.models && "lists no models",
      ].filter(Boolean);
  if (problems.length) {
    replaceBinary(backup, bin);
    const reason = `droid ${offered} ${problems.join("; ")}`;
    state.rejected.push({ version: offered, at: new Date().toISOString(), reason });
    writeState(stateFile, state);
    const missing = result.error ? [] : result.missing;
    return { status: "rolled-back", from: version, to: offered, missing, reason };
  }
  pruneBackups(backups);
  return { status: "updated", from: version, to: offered, backup, models: result.models };
}

/** Puts a backed-up droid back (the newest by default) and holds updates until released. */
export function restoreDroid({ bin, backups, stateFile, version, reason }) {
  const current = versionOf(run(bin, ["--version"]).out);
  const candidates = NodeFS.existsSync(backups)
    ? NodeFS.readdirSync(backups)
        .filter((f) => /^droid-\d/.test(f))
        .map((f) => f.slice(6))
        .filter((v) => v !== current)
        .sort((a, b) => compareVersions(b, a))
    : [];
  const target = version ?? candidates[0];
  if (!target || !candidates.includes(target)) {
    throw new Error(
      `No backup of droid ${target ?? "(none)"} in ${backups}. Kept: ${candidates.join(", ") || "none"}.`,
    );
  }
  replaceBinary(backupPath(backups, target), bin);
  const state = readState(stateFile);
  const at = new Date().toISOString();
  if (current && !state.rejected.some((r) => r.version === current))
    state.rejected.push({ version: current, at, reason });
  state.held = { version: target, at, reason };
  writeState(stateFile, state);
  return { from: current, to: target };
}

/** One line for preflight's output, the report, and the reply. */
export function describeDroid(r) {
  switch (r.status) {
    case "current":
      return `Droid ${r.version} is current.`;
    case "updated":
      return `Droid updated ${r.from} -> ${r.to} (${r.models} models); ${r.from} is kept in ${r.backup}.`;
    case "rolled-back":
      return `Droid ${r.to} was put back to ${r.from}: ${r.reason}. Later runs skip ${r.to}.`;
    case "rejected":
      return `Droid stays on ${r.version}: ${r.offered} was rolled back before (${r.reason}).`;
    case "held":
      return `Droid is held at ${r.version} since ${r.at}: ${r.reason}. \`droid-release\` lets it update again.`;
    default:
      return `Droid update failed: ${r.reason}.`;
  }
}

export function releaseDroid({ stateFile }) {
  const state = readState(stateFile);
  const held = state.held;
  state.held = null;
  writeState(stateFile, state);
  return held;
}
