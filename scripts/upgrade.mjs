#!/usr/bin/env node
// Steps of the fork upgrade to upstream tip. UPGRADING.md is the runbook that says
// when to run each one. Every step records its outcome in the run's state.json and
// rewrites report.md beside it, so a run can be checked without trusting the agent.
import { spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import { createRequire } from "node:module";
import net from "node:net";
import os from "node:os";
import path from "node:path";

const HOME = os.homedir();
const RUNTIME = process.env.T3_RUNTIME_DIR ?? path.join(HOME, ".t3/code");
const UPGRADES = process.env.T3_UPGRADE_DIR ?? path.join(RUNTIME, "upgrades");
const LIVE_DB = path.join(RUNTIME, "userdata/statev2.sqlite");
const LIVE_PORT = Number(process.env.T3_PORT ?? 3773);
const GIT_OVERLAY = path.join(HOME, "code/git-overlay/git-overlay");
const FORK = "fork";
const BACKUPS_KEPT = 3;

const exec = (cmd, args, opts = {}) => {
  const r = spawnSync(cmd, args, { encoding: "utf8", maxBuffer: 1 << 30, ...opts });
  return { code: r.status ?? 1, out: (r.stdout ?? "").trimEnd(), err: (r.stderr ?? "").trimEnd() };
};
const git = (cwd, ...args) => exec("git", ["-C", cwd, ...args]);
const die = (message, code = 1) => {
  console.error(message);
  process.exit(code);
};
const gitOk = (cwd, ...args) => {
  const r = git(cwd, ...args);
  if (r.code !== 0) die(`git ${args.join(" ")} failed:\n${r.err || r.out}`);
  return r.out;
};

// The main checkout, wherever this copy of the script lives.
const MAIN = path.dirname(
  gitOk(import.meta.dirname, "rev-parse", "--path-format=absolute", "--git-common-dir"),
);
const WORKTREE = path.join(path.dirname(MAIN), "t3code-worktrees", "upgrade");

const now = () => new Date().toISOString();
const rev = (ref, cwd = MAIN) => gitOk(cwd, "rev-parse", ref);
const isAncestor = (a, b) => git(MAIN, "merge-base", "--is-ancestor", a, b).code === 0;
const short = (sha) => (sha ? sha.slice(0, 10) : "-");
const overlayName = (b) => b.replace(/^overlay\//, "");
const sha256 = (file) => createHash("sha256").update(fs.readFileSync(file)).digest("hex");
// `--note-file <path>` (`-` reads stdin) keeps apostrophes and newlines out of shell quoting.
const noteText = ([flag, ...rest], { bareWords = false } = {}) => {
  if (flag === "--note-file") {
    if (rest.length !== 1) die("--note-file takes one path, or - for stdin.");
    try {
      return fs.readFileSync(rest[0] === "-" ? 0 : rest[0], "utf8").trim();
    } catch (error) {
      die(`Cannot read the note: ${error.message}`);
    }
  }
  if (flag === "--note") return rest.join(" ").trim();
  return bareWords ? [flag, ...rest].join(" ").trim() : "";
};
const listOverlays = () =>
  gitOk(MAIN, "for-each-ref", "--format=%(refname:short)", "refs/heads/overlay/")
    .split("\n")
    .filter(Boolean);

function worktreesByBranch() {
  const map = new Map();
  let wt = null;
  for (const line of gitOk(MAIN, "worktree", "list", "--porcelain").split("\n")) {
    if (line.startsWith("worktree ")) wt = line.slice(9);
    if (line.startsWith("branch refs/heads/")) map.set(line.slice(18), wt);
  }
  return map;
}
const dirty = (cwd) => git(cwd, "status", "--porcelain", "--untracked-files=no").out;

// ---------------------------------------------------------------- run state

const currentFile = path.join(UPGRADES, "current");
const runDir = (id) => path.join(UPGRADES, id);

function loadRun() {
  if (!fs.existsSync(currentFile)) return null;
  const file = path.join(runDir(fs.readFileSync(currentFile, "utf8").trim()), "state.json");
  return fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, "utf8")) : null;
}

function save(state) {
  state.updatedAt = now();
  fs.mkdirSync(runDir(state.id), { recursive: true });
  fs.writeFileSync(
    path.join(runDir(state.id), "state.json"),
    `${JSON.stringify(state, null, 2)}\n`,
  );
  fs.writeFileSync(path.join(runDir(state.id), "report.md"), renderReport(state));
}

// Requests wait here for the next run, which takes them over and must close each one.
const inboxFile = path.join(UPGRADES, "inbox.json");
const readInbox = () => readJson(inboxFile) ?? { next: 1, requests: [] };
function writeInbox(inbox) {
  fs.mkdirSync(UPGRADES, { recursive: true });
  fs.writeFileSync(inboxFile, `${JSON.stringify(inbox, null, 2)}\n`);
}
const requestId = (id) => Number(String(id ?? "").replace(/^#/, ""));

function requireRun(...statuses) {
  const state = loadRun();
  if (!state) die("No upgrade run. Start one with: preflight");
  if (statuses.length && !statuses.includes(state.status)) {
    die(`Run ${state.id} is ${state.status}${state.blocked ? `: ${state.blocked}` : ""}.`, 3);
  }
  return state;
}

function record(state, name, status, detail = {}) {
  state.steps[name] = { status, at: now(), ...detail };
  save(state);
}

function requireSteps(state, ...names) {
  for (const name of names) {
    const s = state.steps[name]?.status;
    if (s !== "ok" && s !== "accepted") die(`Step '${name}' has not passed (${s ?? "not run"}).`);
  }
}

function invalidate(state, ...names) {
  for (const name of names) if (state.steps[name]) state.steps[name].status = "stale";
}

// Overlays a person should look at: hand-merged, hand-fixed, or changed in what they do.
const isFlagged = (o) =>
  o.status === "resolved" ||
  Boolean(o.fixes?.length) ||
  Boolean(o.intent && o.intent.result !== "same");

// Long commands write to the run's log instead of the agent's context.
function logged(state, name, cwd, cmd, args, env = process.env) {
  const log = path.join(runDir(state.id), "logs", `${name}.log`);
  fs.mkdirSync(path.dirname(log), { recursive: true });
  const fd = fs.openSync(log, "a");
  fs.writeSync(fd, `\n[${now()}] $ (${cwd}) ${cmd} ${args.join(" ")}\n`);
  const started = Date.now();
  const r = spawnSync(cmd, args, { cwd, stdio: ["ignore", fd, fd], env });
  fs.closeSync(fd);
  return {
    code: r.status ?? 1,
    log,
    seconds: Math.round((Date.now() - started) / 1000),
  };
}

const tail = (file, lines = 60) =>
  fs.readFileSync(file, "utf8").split("\n").slice(-lines).join("\n");

// ---------------------------------------------------------------- report

function renderReport(s) {
  const lines = [
    `# t3code upgrade ${s.id}`,
    "",
    `Status: **${s.status}**${s.blocked ? ` (${s.blocked})` : ""}. Updated ${s.updatedAt}.`,
    "",
    `Upstream: ${short(s.upstream.before)} -> ${short(s.upstream.after)} (${s.upstream.commits} commits).`,
    `local before: ${short(s.local.before)}; fork/local before: ${short(s.local.forkBefore)}.`,
    "",
  ];
  if (s.requests?.length) {
    lines.push("## Requests", "");
    for (const r of s.requests) {
      lines.push(`- #${r.id} **${r.status}** (asked ${r.at}): ${r.text.replaceAll("\n", "\n  ")}`);
      if (r.outcome) lines.push(`  - ${r.outcome.replaceAll("\n", "\n    ")}`);
    }
    lines.push("");
  }
  lines.push("## Steps", "", "| step | status | at | detail |", "|---|---|---|---|");
  for (const [name, st] of Object.entries(s.steps)) {
    const detail = [
      st.note,
      st.log && `log: ${st.log}`,
      st.seconds !== undefined && `${st.seconds}s`,
    ]
      .filter(Boolean)
      .join("; ");
    lines.push(`| ${name} | ${st.status} | ${st.at} | ${detail.replaceAll("|", "\\|")} |`);
  }
  lines.push(
    "",
    "## Overlays",
    "",
    "| overlay | sync | conflicts | fixes | intent | check |",
    "|---|---|---|---|---|---|",
  );
  for (const [b, o] of Object.entries(s.overlays)) {
    const intent = o.intent
      ? o.intent.result === "same"
        ? "same"
        : `${o.intent.result} (${o.intent.changedLines} lines)`
      : "-";
    lines.push(
      `| ${b} | ${o.status} | ${o.conflicts?.join(", ") || "-"} | ${o.fixes?.length ?? "-"} | ${intent} | ${o.check?.status ?? "-"} |`,
    );
  }
  for (const [b, r] of Object.entries(s.retired ?? {})) {
    lines.push(
      `| ${b} | retired: ${r.tag} at ${short(r.archive)}, its last change, on origin/main ${short(r.base)} | - | - | - | - |`,
    );
  }
  if (s.risks) {
    lines.push("", "## Upstream risk flags", "");
    lines.push(`- New migrations: ${s.risks.migrations.join(", ") || "none"}`);
    lines.push(
      `- Toolchain (engines/packageManager): ${s.risks.toolchain.join("; ") || "unchanged"}`,
    );
    lines.push(`- Lockfile changed: ${s.risks.lockfile ? "yes" : "no"}`);
    lines.push(`- Runtime external packages changed: ${s.risks.externals ? "yes" : "no"}`);
    for (const [b, files] of Object.entries(s.risks.overlap)) {
      if (files.length)
        lines.push(
          `- ${b} shares ${files.length} changed file(s) with upstream: ${files.join(", ")}`,
        );
    }
  }
  if (s.notes.length) {
    lines.push("", "## Decisions and notes", "");
    for (const n of s.notes)
      lines.push(`- ${n.at}${n.subject ? ` [${n.subject}]` : ""}: ${n.text}`);
  }
  if (s.build)
    lines.push(
      "",
      `Build: ${short(s.build.commit)}, bin.mjs sha256 ${s.build.sha256.slice(0, 16)}.`,
    );
  if (s.deploy) {
    lines.push(`Deploy: ${s.deploy.at}; DB backup ${s.deploy.backup}.`);
    for (const t of s.deploy.busy ?? []) lines.push(`- busy at deploy: ${t}`);
  }
  if (s.verify) {
    lines.push(
      `Verify: ${s.verify.result} at ${s.verify.at}${s.verify.detail ? `: ${s.verify.detail}` : ""}.`,
    );
    for (const t of s.verify.busy ?? []) lines.push(`- busy: ${t}`);
  }
  return `${lines.join("\n")}\n`;
}

// ---------------------------------------------------------------- helpers

function daemonStatus() {
  const r = exec("mise", ["run", "daemon-status"], { cwd: MAIN });
  const text = `${r.out}\n${r.err}`;
  return {
    running: /daemon: running/.test(text),
    listening: /server: listening/.test(text),
    text: text
      .split("\n")
      .filter((l) => !l.startsWith("[daemon-status]"))
      .join("\n"),
  };
}

const localTime = (iso) => new Date(iso).toLocaleString("sv-SE").slice(0, 16);

// The threads that keep the daemon from restarting T3, by the daemon's own `busy` rules.
// A thread waiting on a question stays busy until someone answers it, however long that takes.
function busyThreads() {
  const sql = fs.readFileSync(path.join(import.meta.dirname, "t3-busy-v2.sql"), "utf8");
  const start = sql.indexOf("WITH busy");
  const end = sql.indexOf("\nSELECT group_concat");
  if (start < 0 || end < start) return ["(cannot read the busy rules in t3-busy-v2.sql)"];
  const perThread = `
SELECT b.thread_id AS id, t.title AS title, group_concat(DISTINCT b.reason) AS reasons,
  (SELECT min(r.created_at) FROM orchestration_v2_projection_runtime_requests r
    WHERE r.thread_id = b.thread_id AND r.status = 'pending') AS questionSince
FROM busy b
JOIN orchestration_v2_projection_threads t ON t.thread_id = b.thread_id AND t.deleted_at IS NULL
GROUP BY b.thread_id;`;
  const r = exec("sqlite3", ["-json", "-readonly", "-cmd", ".timeout 2000", LIVE_DB], {
    input: `${sql.slice(start, end)}\n${perThread}`,
  });
  if (r.code !== 0) return [`(busy probe failed: ${r.err.slice(0, 160)})`];
  return (r.out ? JSON.parse(r.out) : []).map((t) => {
    const title = t.title.replace(/\s+/g, " ");
    const shown = title.length > 60 ? `${title.slice(0, 57)}...` : title;
    const question = t.questionSince
      ? `, waiting on a question since ${localTime(t.questionSince)}`
      : "";
    return `"${shown}" (${t.id}): ${t.reasons}${question}`;
  });
}

// Runs from apps/server, where these imports resolve, with the database path as its argument.
const MIGRATE_ONLY = `
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { runMigrations } from "./src/persistence/Migrations.ts";
const program = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe("PRAGMA foreign_keys = ON").unprepared;
  console.log("executed:", JSON.stringify(yield* runMigrations()));
});
NodeRuntime.runMain(program.pipe(Effect.provide(NodeSqliteClient.layer({ filename: process.argv[1] }))));
`;

// The node the main checkout pins, which may have moved since this process started.
const mainNode = () => exec("mise", ["which", "node"], { cwd: MAIN }).out || "node";

const readJson = (file) => (fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, "utf8")) : null);

// mise puts the main checkout's node_modules/.bin on PATH; checks in the worktree must use its own tools.
const worktreeEnv = () => ({
  ...process.env,
  PATH: [
    path.join(WORKTREE, "node_modules/.bin"),
    ...(process.env.PATH ?? "").split(":").filter((p) => !p.endsWith("node_modules/.bin")),
  ].join(":"),
});

function ensureWorktree() {
  if (!fs.existsSync(WORKTREE)) gitOk(MAIN, "worktree", "add", "--detach", WORKTREE, "origin/main");
  if (dirty(WORKTREE)) die(`${WORKTREE} has uncommitted changes:\n${dirty(WORKTREE)}`);
}

// One worktree serves every check, so dependencies install only when the lockfile changes.
function checkoutForCheck(state, ref) {
  ensureWorktree();
  gitOk(WORKTREE, "checkout", "-q", "--detach", ref);
  const marker = path.join(WORKTREE, "node_modules/.upgrade-lock-sha");
  const lockSha = sha256(path.join(WORKTREE, "pnpm-lock.yaml"));
  if (fs.existsSync(marker) && fs.readFileSync(marker, "utf8").trim() === lockSha) return;
  const r = logged(
    state,
    "install-worktree",
    WORKTREE,
    "pnpm",
    ["install", "--frozen-lockfile"],
    worktreeEnv(),
  );
  if (r.code !== 0) die(`pnpm install failed in ${WORKTREE}; see ${r.log}\n${tail(r.log, 30)}`);
  fs.writeFileSync(marker, `${lockSha}\n`);
}

// Zero-context hunks without line numbers: what the overlay changes, not where.
function normalizedPatch(base, tip) {
  return gitOk(MAIN, "diff", "-U0", "--no-color", "--histogram", base, tip)
    .split("\n")
    .filter((l) => !l.startsWith("index ") && !l.startsWith("@@"))
    .join("\n");
}

const pidAlive = (pid) => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

const portFree = (port) =>
  new Promise((resolve) => {
    const srv = net.createServer();
    srv.once("error", () => resolve(false));
    srv.listen(port, "127.0.0.1", () => srv.close(() => resolve(true)));
  });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function httpOk(url) {
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(5000) });
    return res.ok;
  } catch {
    return false;
  }
}

function sqliteRo(db, query) {
  const r = exec("sqlite3", ["-json", `file:${db}?mode=ro`, query]);
  if (r.code !== 0) die(`sqlite3 failed on ${db}: ${r.err}`);
  return r.out ? JSON.parse(r.out) : [];
}

// ---------------------------------------------------------------- steps

async function preflight(args) {
  fs.mkdirSync(UPGRADES, { recursive: true });
  let prev = loadRun();
  if (prev?.status === "deployed") {
    await verifyDeploy();
    prev = loadRun();
  }
  if (prev?.status === "blocked") die(`Run ${prev.id} is blocked: ${prev.blocked}`, 3);
  // Deploying again would push the unverified build into bak, the daemon's first fallback.
  if (prev?.status === "deployed") {
    const hours = Math.round((Date.now() - new Date(prev.deploy.at).getTime()) / 3_600_000);
    const busy = prev.verify?.busy?.length ? `\nBusy:\n- ${prev.verify.busy.join("\n- ")}` : "";
    die(
      `Run ${prev.id} deployed ${hours}h ago and is not live yet: ${prev.verify?.detail ?? "unverified"}${busy}`,
      5,
    );
  }
  if (prev?.status === "running") {
    if (args.includes("--resume")) return status();
    die(
      `Run ${prev.id} is still in progress (last update ${prev.updatedAt}). Resume it with --resume only if it is yours or abandoned.`,
      4,
    );
  }

  const problems = [];
  if (dirty(MAIN)) problems.push(`main checkout has uncommitted changes:\n${dirty(MAIN)}`);
  if (fs.existsSync(path.join(MAIN, ".git/MERGE_HEAD")))
    problems.push("main checkout is mid-merge");
  const branch = git(MAIN, "branch", "--show-current").out;
  if (branch !== "local")
    problems.push(`main checkout is on '${branch || "(detached)"}', expected local`);
  for (const [b, wt] of worktreesByBranch()) {
    if (b.startsWith("overlay/") && dirty(wt))
      problems.push(`${b} has uncommitted changes in ${wt}`);
  }
  const staging = gitOk(
    MAIN,
    "branch",
    "--list",
    "*-overlayed-on-top-of-*",
    "--format=%(refname:short)",
  );
  if (staging)
    problems.push(`unfinished git-overlay staging branches: ${staging.replaceAll("\n", ", ")}`);
  if (problems.length) die(`preflight failed:\n- ${problems.join("\n- ")}`);

  gitOk(MAIN, "fetch", "--quiet", "origin", "main");
  gitOk(MAIN, "fetch", "--quiet", FORK);
  const overlays = listOverlays();
  const behind = overlays.filter((b) => !isAncestor("origin/main", b));
  const inbox = readInbox();
  if (!behind.length && isAncestor("origin/main", "local") && !inbox.requests.length) {
    console.log(
      `Up to date: every overlay and local contain origin/main (${short(rev("origin/main"))}).`,
    );
    process.exit(10);
  }

  const daemon = daemonStatus();
  const today = new Date().toLocaleDateString("sv-SE");
  let id = today;
  for (let n = 2; fs.existsSync(runDir(id)); n++) id = `${today}-${n}`;
  const localBase = gitOk(MAIN, "merge-base", "local", "origin/main");
  const forkLocal = gitOk(MAIN, "ls-remote", FORK, "refs/heads/local").split(/\s/)[0] || null;
  const state = {
    id,
    status: "running",
    blocked: null,
    startedAt: now(),
    upstream: {
      before: localBase,
      after: rev("origin/main"),
      commits: Number(gitOk(MAIN, "rev-list", "--count", `${localBase}..origin/main`)),
    },
    local: { before: rev("local"), forkBefore: forkLocal },
    deployedBefore: readJson(path.join(RUNTIME, "dist/current/build-info.json")),
    daemon: { running: daemon.running, listening: daemon.listening },
    overlays: Object.fromEntries(overlays.map((b) => [b, { status: "pending" }])),
    requests: inbox.requests.map((r) => ({ ...r, status: "open" })),
    steps: {},
    notes: [],
  };
  if (prev && prev.status !== "verified") {
    state.notes.push({
      at: now(),
      subject: "previous",
      text: `previous run ${prev.id} ended ${prev.status}`,
    });
  }
  save(state);
  fs.writeFileSync(currentFile, `${id}\n`);
  writeInbox({ ...inbox, requests: [] });
  record(state, "preflight", "ok", {
    note: `${state.upstream.commits} upstream commits; ${behind.length}/${overlays.length} overlays behind; ${state.requests.length} requests; daemon ${daemon.running ? "running" : "NOT running"}`,
  });
  pruneOldRuns(id);
  console.log(
    `Run ${id}: ${state.upstream.commits} new upstream commits, ${behind.length} of ${overlays.length} overlays behind.`,
  );
  if (state.requests.length)
    console.log(
      `Requests for this run (close each with done or decline):\n${state.requests.map((r) => `- #${r.id}: ${r.text}`).join("\n")}`,
    );
  if (!daemon.running || !daemon.listening)
    console.log(`WARNING: daemon not healthy:\n${daemon.text}`);
  console.log(`Report: ${path.join(runDir(id), "report.md")}\nNext: plan`);
}

// Rehearsal copies of the database are the bulky part of a run; keep the newest few.
function pruneOldRuns(keep) {
  const runs = fs
    .readdirSync(UPGRADES)
    .filter((d) => /^\d{4}-\d{2}-\d{2}/.test(d) && d !== keep)
    .sort()
    .reverse();
  for (const id of runs.slice(4)) {
    const dir = path.join(runDir(id), "rehearsal");
    if (fs.existsSync(dir)) fs.rmSync(dir, { recursive: true, force: true });
  }
}

function plan() {
  const state = requireRun("running");
  requireSteps(state, "preflight");
  const tagged = [];
  for (const b of [...listOverlays(), "local"]) {
    const tag = `ember-backup/${state.id}/${overlayName(b)}-pre-upgrade`;
    if (git(MAIN, "rev-parse", "-q", "--verify", `refs/tags/${tag}`).code !== 0) {
      gitOk(MAIN, "tag", tag, b);
      tagged.push(tag);
    }
  }

  const patches = path.join(runDir(state.id), "patches");
  fs.mkdirSync(patches, { recursive: true });
  const upstreamFiles = new Set(
    gitOk(MAIN, "diff", "--name-only", state.upstream.before, state.upstream.after)
      .split("\n")
      .filter(Boolean),
  );
  const overlap = {};
  for (const b of listOverlays()) {
    const o = (state.overlays[b] ??= { status: "pending" });
    const base = gitOk(MAIN, "merge-base", b, "origin/main");
    o.before = rev(b);
    o.oldBase = base;
    fs.writeFileSync(path.join(patches, `${overlayName(b)}.before.diff`), normalizedPatch(base, b));
    const files = gitOk(MAIN, "diff", "--name-only", base, b).split("\n").filter(Boolean);
    o.files = files.length;
    overlap[b] = files.filter((f) => upstreamFiles.has(f));
    if (isAncestor("origin/main", b)) {
      o.status = "synced";
      continue;
    }
    const merge = git(MAIN, "merge-tree", "--write-tree", "--name-only", "origin/main", b);
    if (merge.code === 0) {
      o.status = "clean";
    } else {
      o.status = "conflict";
      o.conflicts = merge.out.split("\n\n")[0].split("\n").slice(1).filter(Boolean);
    }
  }

  const range = [state.upstream.before, state.upstream.after];
  const added = gitOk(MAIN, "diff", "--name-only", "--diff-filter=A", ...range).split("\n");
  const pkg = gitOk(MAIN, "diff", "-U0", ...range, "--", "package.json")
    .split("\n")
    .filter((l) => /^[+-]\s*"(node|pnpm|packageManager|engines)"/.test(l));
  state.risks = {
    migrations: added.filter(
      (f) => f.startsWith("apps/server/src/persistence/Migrations/") && !f.endsWith(".test.ts"),
    ),
    toolchain: pkg,
    lockfile: upstreamFiles.has("pnpm-lock.yaml"),
    externals: upstreamFiles.has("scripts/lib/cli-external-packages.ts"),
    overlap,
  };
  record(state, "plan", "ok", {
    note: `backup tags: ${tagged.length ? tagged.length : "already present"}`,
  });

  for (const [b, o] of Object.entries(state.overlays)) {
    console.log(
      `${o.status.padEnd(8)} ${b}${o.conflicts ? `  conflicts: ${o.conflicts.join(", ")}` : ""}`,
    );
  }
  const r = state.risks;
  console.log(
    `\nRisks: migrations ${r.migrations.length}, toolchain ${r.toolchain.length ? "CHANGED" : "same"}, lockfile ${r.lockfile ? "changed" : "same"}, externals ${r.externals ? "CHANGED" : "same"}`,
  );
  console.log("Next: sync");
}

function sync() {
  const state = requireRun("running");
  requireSteps(state, "plan");
  const r = logged(state, "sync", MAIN, "mise", ["run", "sync"]);
  const pending = [];
  for (const b of listOverlays()) {
    const o = state.overlays[b];
    if (isAncestor("origin/main", b)) {
      if (o.status !== "resolved") o.status = "synced";
      o.after = rev(b);
    } else {
      pending.push(b);
    }
  }
  record(state, "sync", pending.length ? "partial" : "ok", {
    log: r.log,
    note: pending.length ? `conflicting: ${pending.join(", ")}` : undefined,
  });
  if (pending.length) {
    console.log(`Synced all but ${pending.length}. Resolve each with: resolve <overlay>`);
    for (const b of pending)
      console.log(`  ${b}: ${state.overlays[b].conflicts?.join(", ") ?? ""}`);
    process.exit(2);
  }
  console.log("All overlays contain origin/main. Next: intent");
}

// git-overlay renames the branch, which fails while another worktree has it checked out.
function borrow(state, b) {
  const holder = worktreesByBranch().get(b);
  if (!holder || holder === WORKTREE) return;
  if (dirty(holder)) die(`${b} has uncommitted changes in ${holder}; leave it for fire.`);
  gitOk(holder, "switch", "-q", "--detach");
  state.overlays[b].heldBy = holder;
  save(state);
}

function giveBack(state, b) {
  const o = state.overlays[b];
  if (worktreesByBranch().get(b) === WORKTREE) gitOk(WORKTREE, "checkout", "-q", "--detach", b);
  if (!o.heldBy) return;
  const r = git(o.heldBy, "switch", "-q", b);
  if (r.code !== 0) console.log(`WARNING: could not reattach ${o.heldBy}: ${r.err}`);
  else delete o.heldBy;
}

function resolve([b]) {
  const state = requireRun("running");
  if (!state.overlays[b]) die(`Unknown overlay '${b}'.`);
  if (isAncestor("origin/main", b)) die(`${b} already contains origin/main.`);
  // Installed dependencies are what let the repo's pre-commit hook run on the merge commit.
  checkoutForCheck(state, "origin/main");
  borrow(state, b);
  const r = exec("bash", [GIT_OVERLAY, "origin/main", b], { cwd: WORKTREE });
  console.log(`${r.out}\n${r.err}`.trim());
  if (r.code === 0 && isAncestor("origin/main", b)) {
    console.log(`\n${b} merged without conflicts. Run: resolved ${b} --note "<what you checked>"`);
    return;
  }
  const unmerged = git(WORKTREE, "diff", "--name-only", "--diff-filter=U").out;
  console.log(
    `\nResolve in ${WORKTREE}:\n${unmerged}\nThen: git -C ${WORKTREE} add <files> && (cd ${WORKTREE} && bash ${GIT_OVERLAY} --continue)\nThen: resolved ${b} --note "<decision and why>"`,
  );
}

function resolved([b, ...args]) {
  const state = requireRun("running");
  const o = state.overlays[b];
  if (!o) die(`Unknown overlay '${b}'.`);
  const note = noteText(args);
  if (!note)
    die(
      'A decision note is required: resolved <overlay> --note "<decision and why>" (or --note-file <path>)',
    );
  if (!isAncestor("origin/main", b)) die(`${b} does not contain origin/main yet.`);
  if (git(WORKTREE, "diff", "--name-only", "--diff-filter=U").out)
    die(`${WORKTREE} still has unmerged files.`);
  const staging = gitOk(
    MAIN,
    "branch",
    "--list",
    "*-overlayed-on-top-of-*",
    "--format=%(refname:short)",
  );
  if (staging) die(`git-overlay staging branch still exists: ${staging}`);
  giveBack(state, b);
  o.status = "resolved";
  o.after = rev(b);
  state.notes.push({ at: now(), subject: b, text: note });
  save(state);
  const left = listOverlays().filter((x) => !isAncestor("origin/main", x));
  console.log(
    left.length
      ? `Still behind: ${left.join(", ")}`
      : "All overlays contain origin/main. Next: intent",
  );
}

// For an overlay that merged cleanly but no longer works against the new upstream.
function fix([b]) {
  const state = requireRun("running");
  if (!state.overlays[b]) die(`Unknown overlay '${b}'.`);
  if (!isAncestor("origin/main", b)) die(`${b} does not contain origin/main yet; use resolve.`);
  checkoutForCheck(state, b);
  borrow(state, b);
  gitOk(WORKTREE, "switch", "-q", b);
  console.log(
    `${b} is checked out in ${WORKTREE}.\nCommit the fix there, then: fixed ${b} --note "<what broke and how you fixed it>"`,
  );
}

function fixed([b, ...args]) {
  const state = requireRun("running");
  const o = state.overlays[b];
  if (!o) die(`Unknown overlay '${b}'.`);
  const note = noteText(args);
  if (!note)
    die(
      'A note is required: fixed <overlay> --note "<what broke and how you fixed it>" (or --note-file <path>)',
    );
  if (dirty(WORKTREE)) die(`${WORKTREE} has uncommitted changes; commit or drop them first.`);
  const since = o.after ?? o.before;
  const commits = gitOk(MAIN, "log", "--format=%h %s", `${since}..${b}`)
    .split("\n")
    .filter(Boolean);
  if (!commits.length) die(`${b} has no new commits since ${short(since)}.`);
  giveBack(state, b);
  o.fixes = [...(o.fixes ?? []), ...commits];
  o.after = rev(b);
  delete o.check;
  invalidate(state, "intent", "rebuild", "check-local");
  state.notes.push({ at: now(), subject: `fix ${b}`, text: `${note} (${commits.join("; ")})` });
  save(state);
  console.log(
    `Recorded ${commits.length} fix commit(s) on ${b}. Next: check ${b}, then intent and rebuild again.`,
  );
}

function intent() {
  const state = requireRun("running");
  const patches = path.join(runDir(state.id), "patches");
  const flagged = [];
  for (const b of listOverlays()) {
    const o = state.overlays[b];
    if (!isAncestor("origin/main", b)) die(`${b} does not contain origin/main yet.`);
    const name = overlayName(b);
    const beforeFile = path.join(patches, `${name}.before.diff`);
    const afterFile = path.join(patches, `${name}.after.diff`);
    fs.writeFileSync(afterFile, normalizedPatch("origin/main", b));
    const d = exec("git", ["diff", "--no-index", "--no-color", "-U1", beforeFile, afterFile]);
    const intentFile = path.join(patches, `${name}.intent.diff`);
    const changedLines = d.out.split("\n").filter((l) => /^[+-](?![+-]{2} )/.test(l)).length;
    let result = d.code === 0 ? "same" : "changed";
    if (!fs.readFileSync(afterFile, "utf8").trim()) result = "absorbed";
    if (result !== "same") {
      fs.writeFileSync(intentFile, `${d.out}\n`);
      flagged.push(b);
    }
    o.intent = { result, changedLines, file: result === "same" ? null : intentFile };
  }
  record(state, "intent", "ok", {
    note: flagged.length ? `review: ${flagged.join(", ")}` : "all overlays unchanged",
  });
  for (const b of listOverlays()) {
    const i = state.overlays[b].intent;
    console.log(
      `${i.result.padEnd(8)} ${b}${i.file ? `  ${i.changedLines} lines: ${i.file}` : ""}`,
    );
  }
  console.log(
    flagged.length
      ? `\nReview each changed overlay's intent diff, then note it, check it, and continue with: rebuild`
      : "\nNext: rebuild",
  );
}

function check([target]) {
  const state = requireRun("running");
  if (!target) die("Usage: check <overlay>|local|--flagged");
  const targets =
    target === "--flagged"
      ? Object.entries(state.overlays)
          .filter(([, o]) => isFlagged(o))
          .map(([b]) => b)
      : [target];
  let failed = 0;
  for (const t of targets) {
    if (t === "local") requireSteps(state, "rebuild");
    else if (!state.overlays[t]) die(`Unknown overlay '${t}'.`);
    const cwd = t === "local" ? MAIN : WORKTREE;
    if (t !== "local") checkoutForCheck(state, t);
    const vp = path.join(cwd, "node_modules/.bin/vp");
    const env = t === "local" ? process.env : worktreeEnv();
    const r = logged(
      state,
      `check-${overlayName(t)}`,
      cwd,
      vp,
      ["run", "check:changed", "origin/main"],
      env,
    );
    const result = {
      status: r.code === 0 ? "ok" : "failed",
      log: r.log,
      seconds: r.seconds,
      at: now(),
    };
    if (t === "local")
      record(state, "check-local", result.status, { log: r.log, seconds: r.seconds });
    else {
      state.overlays[t].check = result;
      save(state);
    }
    console.log(`${result.status.padEnd(6)} ${t} (${r.seconds}s) ${r.log}`);
    if (r.code !== 0) {
      failed++;
      console.log(tail(r.log, 40));
    }
  }
  if (!targets.length) console.log("No flagged overlays to check.");
  if (failed) process.exit(1);
}

function baseline(files) {
  const state = requireRun("running");
  if (!files.length) die("Usage: baseline <test files...>  (runs them on plain origin/main)");
  checkoutForCheck(state, "origin/main");
  const vp = path.join(WORKTREE, "node_modules/.bin/vp");
  const r = logged(state, "baseline", WORKTREE, vp, ["test", "run", ...files], worktreeEnv());
  const verdict =
    r.code === 0
      ? "pass on origin/main (so the failure is ours)"
      : "fail on origin/main too (upstream's)";
  state.notes.push({
    at: now(),
    subject: "baseline",
    text: `${files.join(" ")}: ${verdict}; log ${r.log}`,
  });
  save(state);
  console.log(`${verdict}\n${tail(r.log, 25)}`);
}

function rebuild() {
  const state = requireRun("running");
  const behind = listOverlays().filter((b) => !isAncestor("origin/main", b));
  if (behind.length) die(`Overlays still behind origin/main: ${behind.join(", ")}`);
  requireSteps(state, "intent");

  const steps = [
    ["rebuild-local", "mise", ["run", "rebuild-local"]],
    // Through mise, so a toolchain bump committed during this run is what installs.
    ["install", "mise", ["x", "--", "pnpm", "install", "--frozen-lockfile"]],
    ["build", "mise", ["run", "build"]],
    ["smoke-test", "mise", ["run", "smoke-test"]],
  ];
  for (const [name, cmd, args] of steps) {
    const r = logged(state, name, MAIN, cmd, args);
    if (r.code !== 0) {
      const midMerge = fs.existsSync(path.join(MAIN, ".git/MERGE_HEAD"));
      record(state, "rebuild", "failed", {
        log: r.log,
        note: midMerge ? `${name} stopped on a conflict between overlays` : `${name} failed`,
      });
      const unmerged = midMerge ? git(MAIN, "diff", "--name-only", "--diff-filter=U").out : "";
      die(
        `${name} failed (${r.seconds}s); see ${r.log}\n${tail(r.log, 40)}` +
          (midMerge
            ? `\n\nThe main checkout is mid-merge on local:\n${unmerged}\nSee "Conflicts between overlays" in UPGRADING.md.`
            : ""),
      );
    }
    if (name === "rebuild-local") {
      const markers = gitOk(MAIN, "diff", "origin/main", "local")
        .split("\n")
        .filter((l) => /^\+(<<<<<<<|>>>>>>>) /.test(l));
      if (markers.length) {
        record(state, "rebuild", "failed", { note: "conflict markers in local" });
        die(`Conflict markers in local:\n${markers.join("\n")}`);
      }
    }
    console.log(`ok ${name} (${r.seconds}s)`);
  }
  const bin = path.join(MAIN, "apps/server/dist/bin.mjs");
  const info = readJson(path.join(MAIN, "apps/server/dist/build-info.json"));
  state.build = { commit: rev("local"), sha256: sha256(bin), info };
  if (!info || info.commit !== state.build.commit || info.dirty) {
    record(state, "rebuild", "failed", { note: "build-info.json does not match a clean local" });
    die(`build-info.json says ${JSON.stringify(info)}, but local is ${short(state.build.commit)}.`);
  }
  record(state, "rebuild", "ok", { note: `local ${short(state.build.commit)}` });
  console.log("Next: check local");
}

function accept([name, ...args]) {
  const state = requireRun("running");
  const note = noteText(args);
  if (!name || !note)
    die(
      'Usage: accept <step|overlay> --note "<why the failure is acceptable>" (or --note-file <path>)',
    );
  const target = state.overlays[name] ? state.overlays[name].check : state.steps[name];
  if (target?.status !== "failed") die(`'${name}' has not failed.`);
  target.status = "accepted";
  state.notes.push({ at: now(), subject: `accept ${name}`, text: note });
  save(state);
  console.log(`Accepted ${name}.`);
}

async function rehearse() {
  const state = requireRun("running");
  requireSteps(state, "rebuild");
  if (state.rehearsal?.pid && pidAlive(state.rehearsal.pid))
    die("A rehearsal server is already running; rehearse-stop first.");
  // migrate-dev-db prunes even with huge limits, so new migrations also run on an exact copy.
  // Only migrations run there: a server on it would start fire's scheduled tasks.
  if (state.risks?.migrations.length) {
    const full = path.join(runDir(state.id), "rehearsal-full");
    fs.rmSync(full, { recursive: true, force: true });
    fs.mkdirSync(full, { recursive: true });
    const copy = path.join(full, "statev2.sqlite");
    const b = exec("sqlite3", [`file:${LIVE_DB}?mode=ro`, `.backup '${copy}'`]);
    if (b.code !== 0) die(`Copying the live database failed: ${b.err}`);
    const m = logged(state, "migrate-full", path.join(MAIN, "apps/server"), mainNode(), [
      "--input-type=module",
      "-e",
      MIGRATE_ONLY,
      copy,
    ]);
    fs.rmSync(full, { recursive: true, force: true });
    // The daemon gives a starting build T3_HEALTH_TIMEOUT (300s) before falling back.
    const slow = m.seconds > 120;
    record(state, "migrate-full", m.code === 0 && !slow ? "ok" : "failed", {
      log: m.log,
      seconds: m.seconds,
      note: `${state.risks.migrations.length} new migration(s) on an exact copy of the live database${slow ? "; too slow for the daemon's health check" : ""}`,
    });
    if (m.code !== 0 || slow) {
      die(
        `New migrations failed or took ${m.seconds}s on the live data; see ${m.log}\n${tail(m.log, 40)}`,
      );
    }
    console.log(`ok new migrations on an exact copy of the live database (${m.seconds}s)`);
  }

  // migrate-dev-db drops scheduled tasks, pending work, and auth sessions, so a
  // rehearsal server never runs fire's agents.
  const base = path.join(runDir(state.id), "rehearsal");
  fs.rmSync(base, { recursive: true, force: true });
  const db = logged(state, "rehearse-db", MAIN, mainNode(), [
    "apps/server/scripts/migrate-dev-db.ts",
    ...["--base-dir", base, "--source", LIVE_DB, "--projects", "12", "--threads-per-project", "12"],
  ]);
  if (db.code !== 0) {
    record(state, "rehearse", "failed", {
      log: db.log,
      note: "copying or migrating the database failed",
    });
    die(`migrate-dev-db failed; see ${db.log}\n${tail(db.log, 40)}`);
  }

  let port = 3790;
  while (port < 3800 && !(await portFree(port))) port++;
  if (port === 3800) die("No free port in 3790-3799.");
  const serverLog = path.join(runDir(state.id), "logs", "rehearsal-server.log");
  // A fresh log: an earlier rehearsal's pairing token in it would already be spent.
  const out = fs.openSync(serverLog, "w");
  const env = { ...process.env };
  delete env.T3CODE_HOME;
  const child = spawn(
    mainNode(),
    [
      path.join(MAIN, "apps/server/dist/bin.mjs"),
      "serve",
      "--base-dir",
      base,
      "--port",
      String(port),
      "--host",
      "127.0.0.1",
    ],
    { cwd: base, detached: true, stdio: ["ignore", out, out], env },
  );
  child.unref();
  state.rehearsal = { base, port, pid: child.pid, log: serverLog, startedAt: now() };
  save(state);

  const url = `http://127.0.0.1:${port}`;
  const started = Date.now();
  let pairing = null;
  while (Date.now() - started < 180_000) {
    if (!pidAlive(child.pid)) break;
    pairing ??= fs.readFileSync(serverLog, "utf8").match(/Pairing URL: (\S+)/)?.[1] ?? null;
    if (pairing && (await httpOk(url))) break;
    await sleep(1000);
  }
  const seconds = Math.round((Date.now() - started) / 1000);
  if (!pairing || !(await httpOk(url))) {
    record(state, "rehearse", "failed", {
      log: serverLog,
      seconds,
      note: "server did not come up",
    });
    die(
      `Rehearsal server did not come up in ${seconds}s; see ${serverLog}\n${tail(serverLog, 40)}`,
    );
  }
  fs.writeFileSync(path.join(base, "pairing-url"), `${pairing}\n`, { mode: 0o600 });
  const errors = fs
    .readFileSync(serverLog, "utf8")
    .split("\n")
    .filter((l) => /\b(ERROR|FATAL)\b/.test(l));
  record(state, "rehearse", "ok", {
    log: serverLog,
    seconds,
    note: `up on ${url}; ${errors.length} error lines in the server log`,
  });
  console.log(
    `Rehearsal server up on ${url} in ${seconds}s (pid ${child.pid}); ${errors.length} error lines.`,
  );
  if (errors.length) console.log(errors.slice(0, 15).join("\n"));
  console.log("Next: ui");
}

// Headless, because a browser pane the user is not looking at pauses animation frames,
// which leaves virtualized lists and entrance animations unrendered.
async function launchBrowser() {
  const require = createRequire(path.join(MAIN, "apps/server/package.json"));
  const { chromium } = require("playwright-core");
  try {
    return await chromium.launch({ headless: true });
  } catch {
    return await chromium.launch({ channel: "chrome", headless: true });
  }
}

async function ui() {
  const state = requireRun("running");
  requireSteps(state, "rehearse");
  const reh = state.rehearsal;
  if (!reh?.pid || !pidAlive(reh.pid)) die("The rehearsal server is not running; run rehearse.");
  const url = `http://127.0.0.1:${reh.port}`;
  const db = path.join(reh.base, "userdata/statev2.sqlite");
  const envId = fs.readFileSync(path.join(reh.base, "userdata/environment-id"), "utf8").trim();
  const live = "t.deleted_at IS NULL AND t.archived_at IS NULL";
  // The sidebar collapses settled threads, so only unsettled ones are visible on load.
  const unsettled = sqliteRo(
    db,
    `SELECT t.title FROM orchestration_v2_projection_threads t
     WHERE ${live} AND json_extract(t.payload_json, '$.settledAt') IS NULL
       AND json_extract(t.payload_json, '$.snoozedUntil') IS NULL LIMIT 20`,
  ).map((r) => r.title);
  // A thread with tool calls exercises the most rendering paths; its last user message proves the timeline rendered.
  const thread = sqliteRo(
    db,
    `SELECT t.thread_id AS id, t.title AS title,
       (SELECT json_extract(m.payload_json, '$.text') FROM orchestration_v2_projection_messages m
        WHERE m.thread_id = t.thread_id AND m.role = 'user' ORDER BY m.created_at DESC LIMIT 1) AS lastUser
     FROM orchestration_v2_projection_threads t
     WHERE ${live} AND EXISTS (SELECT 1 FROM orchestration_v2_projection_turn_items i
       WHERE i.thread_id = t.thread_id AND i.payload_json LIKE '%"tool%')
     ORDER BY t.updated_at DESC LIMIT 10`,
  )
    .map((t) => ({
      ...t,
      snippet: (t.lastUser ?? "").match(/[A-Za-z][A-Za-z ,']{14,40}[A-Za-z]/)?.[0],
    }))
    .find((t) => t.snippet);
  const shots = path.join(runDir(state.id), "screenshots");
  fs.mkdirSync(shots, { recursive: true });
  const pairing = fs.readFileSync(path.join(reh.base, "pairing-url"), "utf8").trim();

  const checks = [];
  const pageErrors = [];
  let consoleErrors = 0;
  const step = async (name, fn) => {
    try {
      await fn();
      checks.push({ name, ok: true });
    } catch (error) {
      checks.push({
        name,
        ok: false,
        detail: String(error.message ?? error)
          .split("\n")[0]
          .slice(0, 300),
      });
    }
  };
  let browser = null;
  try {
    browser = await launchBrowser();
    const page = await browser.newPage({ viewport: { width: 1440, height: 960 } });
    page.on("pageerror", (e) => pageErrors.push(String(e.message ?? e).slice(0, 200)));
    page.on("console", (m) => {
      if (m.type() === "error") consoleErrors++;
    });
    await step("pairing opens the app", () =>
      page.goto(pairing, { waitUntil: "load", timeout: 30_000 }),
    );
    await step("sidebar lists an unsettled thread", () =>
      unsettled.length
        ? page.waitForFunction(
            (titles) => titles.some((t) => document.body.innerText.includes(t)),
            unsettled,
            {
              timeout: 45_000,
            },
          )
        : page.getByText("Settled").first().waitFor({ state: "visible", timeout: 45_000 }),
    );
    await sleep(2000);
    await page.screenshot({ path: path.join(shots, "home.png") });
    if (thread) {
      await step("thread opens", () =>
        page.goto(`${url}/${envId}/${thread.id}`, { timeout: 30_000 }),
      );
      await step("thread timeline shows its last user message", () =>
        page.getByText(thread.snippet).first().waitFor({ state: "visible", timeout: 45_000 }),
      );
      await sleep(2000);
      await page.screenshot({ path: path.join(shots, "thread.png") });
    } else {
      checks.push({
        name: "a thread with tool calls exists",
        ok: false,
        detail: "none in the rehearsal copy",
      });
    }
    checks.push({
      name: "no uncaught page errors",
      ok: pageErrors.length === 0,
      detail: pageErrors.slice(0, 5).join(" | "),
    });
  } catch (error) {
    checks.push({
      name: "headless browser",
      ok: false,
      detail: String(error.message ?? error).slice(0, 300),
    });
  } finally {
    await browser?.close();
  }
  if (consoleErrors)
    state.notes.push({
      at: now(),
      subject: "ui",
      text: `${consoleErrors} console errors (not failing)`,
    });
  const ok = checks.every((c) => c.ok);
  record(state, "ui", ok ? "ok" : "failed", {
    note: checks
      .map((c) => `${c.ok ? "ok" : "FAIL"} ${c.name}${c.detail ? ` (${c.detail})` : ""}`)
      .join("; "),
    screenshots: shots,
  });
  for (const c of checks)
    console.log(`${c.ok ? "ok  " : "FAIL"} ${c.name}${c.detail ? `: ${c.detail}` : ""}`);
  console.log(`Screenshots: ${shots} (look at them before passing this step)\nNext: rehearse-stop`);
  if (!ok) process.exit(1);
}

async function rehearseStop() {
  const state = requireRun("running", "blocked");
  const reh = state.rehearsal;
  if (!reh?.pid || !pidAlive(reh.pid)) {
    console.log("No rehearsal server running.");
    return;
  }
  // Only the process this run started: guard against a recycled PID.
  const cmd = exec("ps", ["-o", "command=", "-p", String(reh.pid)]).out;
  if (!cmd.includes(reh.base))
    die(`PID ${reh.pid} is no longer the rehearsal server (${cmd}); not killing it.`);
  process.kill(reh.pid, "SIGTERM");
  for (let i = 0; i < 30 && pidAlive(reh.pid); i++) await sleep(1000);
  if (pidAlive(reh.pid)) process.kill(reh.pid, "SIGKILL");
  reh.stoppedAt = now();
  save(state);
  console.log("Rehearsal server stopped. Next: publish");
}

function publish() {
  const state = requireRun("running");
  requireSteps(state, "rebuild", "check-local", "rehearse", "ui");
  if (state.rehearsal?.pid && pidAlive(state.rehearsal.pid))
    die("Stop the rehearsal server first: rehearse-stop");
  for (const [b, o] of Object.entries(state.overlays)) {
    const s = o.check?.status;
    if (s === "failed")
      die(`${b} failed its check; fix it, or accept it with the reason it is upstream's.`);
    if (isFlagged(o) && s !== "ok" && s !== "accepted")
      die(`${b} is flagged and needs a check: check ${b}`);
  }
  const open = (state.requests ?? []).filter((r) => r.status === "open");
  if (open.length)
    die(`Close each request first with done or decline: ${open.map((r) => `#${r.id}`).join(", ")}`);
  const overlays = listOverlays();
  const behind = overlays.filter((b) => !isAncestor("origin/main", b));
  if (behind.length) die(`Overlays behind origin/main: ${behind.join(", ")}`);
  if (rev("local") !== state.build.commit) die("local moved since the build; rebuild first.");

  const pushOverlays = logged(state, "publish", MAIN, "git", ["push", FORK, ...overlays]);
  if (pushOverlays.code !== 0) {
    record(state, "publish", "failed", {
      log: pushOverlays.log,
      note: "overlay push failed (never force it)",
    });
    die(`Pushing overlays failed; see ${pushOverlays.log}\n${tail(pushOverlays.log, 20)}`);
  }
  const lease = state.local.forkBefore
    ? `--force-with-lease=local:${state.local.forkBefore}`
    : "--force-with-lease";
  const pushLocal = logged(state, "publish", MAIN, "git", ["push", lease, FORK, "local"]);
  if (pushLocal.code !== 0) {
    record(state, "publish", "failed", {
      log: pushLocal.log,
      note: "local push failed: fork/local moved since preflight",
    });
    die(`Pushing local failed; see ${pushLocal.log}\n${tail(pushLocal.log, 20)}`);
  }
  const retired = Object.entries(state.retired ?? {});
  if (retired.length) {
    const onFork = gitOk(MAIN, "ls-remote", FORK, ...retired.map(([b]) => `refs/heads/${b}`));
    const toDelete = retired.filter(([b]) => onFork.includes(`\trefs/heads/${b}`));
    const unknown = toDelete.filter(([, r]) => !r.forkTip).map(([b]) => b);
    if (unknown.length)
      die(`${unknown.join(", ")} appeared on ${FORK} after preflight; leave it for fire.`);
    // The lease deletes fork's overlay branch only if it is still the one retire archived.
    const pushRetired = logged(state, "publish", MAIN, "git", [
      "push",
      "--atomic",
      ...toDelete.map(([b, r]) => `--force-with-lease=refs/heads/${b}:${r.forkTip}`),
      FORK,
      ...retired.flatMap(([, r]) => [`refs/tags/${r.tag}`, `${r.archive}:refs/heads/${r.tag}`]),
      ...toDelete.map(([b]) => `:refs/heads/${b}`),
    ]);
    if (pushRetired.code !== 0) {
      record(state, "publish", "failed", {
        log: pushRetired.log,
        note: "pushing retired overlays failed",
      });
      die(`Pushing retired overlays failed; see ${pushRetired.log}\n${tail(pushRetired.log, 20)}`);
    }
  }
  const remote = new Map(
    gitOk(
      MAIN,
      "ls-remote",
      FORK,
      "refs/heads/local",
      "refs/heads/overlay/*",
      "refs/heads/retired/*",
      "refs/tags/retired/*",
    )
      .split("\n")
      .filter(Boolean)
      .map((l) => l.split("\t"))
      .map(([sha, ref]) => [ref, sha]),
  );
  const mismatched = [...overlays, "local"].filter((b) => remote.get(`refs/heads/${b}`) !== rev(b));
  for (const [b, r] of retired) {
    if (remote.has(`refs/heads/${b}`)) mismatched.push(`${b} (still there)`);
    if (remote.get(`refs/heads/${r.tag}`) !== r.archive) mismatched.push(`branch ${r.tag}`);
    if (remote.get(`refs/tags/${r.tag}^{}`) !== r.archive) mismatched.push(`tag ${r.tag}`);
  }
  if (mismatched.length) {
    record(state, "publish", "failed", { note: `fork does not match: ${mismatched.join(", ")}` });
    die(`Fork does not match after push: ${mismatched.join(", ")}`);
  }
  const retiredNote = retired.length ? `; retired ${retired.map(([b]) => b).join(", ")}` : "";
  record(state, "publish", "ok", {
    log: pushLocal.log,
    note: `fork/local ${short(rev("local"))}${retiredNote}`,
  });
  console.log(`Pushed ${overlays.length} overlays and local${retiredNote}. Next: deploy`);
}

function deploy() {
  const state = requireRun("running");
  requireSteps(state, "publish");
  const daemon = daemonStatus();
  if (!daemon.running)
    die(`The daemon is not running; deploying would not restart T3:\n${daemon.text}`);

  const backups = path.join(RUNTIME, "backups");
  fs.mkdirSync(backups, { recursive: true });
  const backup = path.join(backups, `statev2-pre-upgrade-${state.id}.sqlite`);
  const b = exec("sqlite3", [`file:${LIVE_DB}?mode=ro`, `.backup '${backup}'`]);
  if (b.code !== 0) die(`Database backup failed: ${b.err}`);
  const old = fs
    .readdirSync(backups)
    .filter((f) => /^statev2-pre-upgrade-.*\.sqlite$/.test(f))
    .sort()
    .reverse()
    .slice(BACKUPS_KEPT);
  for (const f of old) fs.rmSync(path.join(backups, f));

  // The runtime shim pins its own node; natives built for a newer node would not load in it.
  const vendorNode = exec("mise", ["where", "node"], { cwd: MAIN }).out;
  const runtimeNode = exec("readlink", [path.join(RUNTIME, "node.bin")]).out;
  if (vendorNode && !runtimeNode.startsWith(vendorNode)) {
    const g = logged(state, "deploy", MAIN, "mise", ["run", "generate-runtime"]);
    if (g.code !== 0) die(`generate-runtime failed; see ${g.log}`);
    state.notes.push({ at: now(), subject: "deploy", text: `runtime node moved to ${vendorNode}` });
  }

  const s = logged(state, "deploy", MAIN, "mise", ["run", "swap"]);
  if (s.code !== 0) {
    record(state, "deploy", "failed", { log: s.log, note: "swap failed; live build untouched" });
    die(`swap failed; see ${s.log}\n${tail(s.log, 30)}`);
  }
  const current = sha256(path.join(RUNTIME, "dist/current/bin.mjs"));
  if (current !== state.build.sha256) die("The swapped bin.mjs differs from the rehearsed build.");
  const busy = busyThreads();
  state.deploy = { at: now(), backup, sha256: current, busy };
  state.status = "deployed";
  record(state, "deploy", "ok", {
    log: s.log,
    note: "swapped; the daemon restarts T3 once it is idle",
  });
  console.log(
    "Deployed to dist/current. The daemon restarts T3 once it is idle; the next preflight (or verify-deploy) confirms it went live.",
  );
  if (busy.length) console.log(`Busy now (this thread is one of them):\n- ${busy.join("\n- ")}`);
}

async function verifyDeploy() {
  const state = loadRun();
  if (!state?.deploy) die("No deploy to verify.");
  const current = sha256(path.join(RUNTIME, "dist/current/bin.mjs"));
  const since = new Date(state.deploy.at);
  const pad = (n) => String(n).padStart(2, "0");
  const stamp = (d) =>
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  const log = fs.existsSync(path.join(RUNTIME, "daemon.log"))
    ? fs
        .readFileSync(path.join(RUNTIME, "daemon.log"), "utf8")
        .split("\n")
        .filter((l) => l.slice(0, 19) >= stamp(since))
    : [];
  const fellBack = log.find((l) =>
    /fallback|failed health check|giving up|Running on stable/i.test(l),
  );
  // The path dist/current is reused by every deploy, so only a server started after the swap runs it.
  const pid = exec("lsof", [`-tiTCP:${LIVE_PORT}`, "-sTCP:LISTEN"]).out.split("\n")[0];
  const argv = pid ? exec("ps", ["-o", "command=", "-p", pid]).out : "";
  const startedAt = pid ? new Date(exec("ps", ["-o", "lstart=", "-p", pid]).out.trim()) : null;
  const restarted = Boolean(startedAt && startedAt >= since && argv.includes("/dist/current/"));
  const http = await httpOk(`http://127.0.0.1:${LIVE_PORT}/`);
  let result;
  let detail = "";
  let busy = [];
  if (current !== state.deploy.sha256) {
    result = "replaced";
    detail = "dist/current changed since this deploy";
  } else if (fellBack) {
    result = "failed";
    detail = fellBack;
  } else if (restarted && http) {
    result = "live";
  } else {
    result = "pending";
    detail = !pid
      ? "T3 is not listening"
      : restarted
        ? "T3 not answering on its port"
        : "the daemon has not restarted T3 onto it; T3 has not been idle long enough";
    if (pid && !restarted) busy = busyThreads();
  }
  state.verify = { result, at: now(), detail, busy };
  if (result === "live" || result === "replaced") state.status = "verified";
  if (result === "failed") {
    state.status = "blocked";
    state.blocked = `deploy did not go live: ${detail}`;
  }
  save(state);
  console.log(`Deploy ${state.id}: ${result}${detail ? ` (${detail})` : ""}`);
  if (busy.length) console.log(`Busy:\n- ${busy.join("\n- ")}`);
  return result;
}

function note([subject, ...rest]) {
  const state = requireRun();
  const text = noteText(rest, { bareWords: true });
  if (!subject || !text) die('Usage: note <subject> "<text>" (or --note-file <path>)');
  state.notes.push({ at: now(), subject, text });
  save(state);
}

function block(rest) {
  const state = requireRun("running");
  const reason = noteText(rest, { bareWords: true });
  if (!reason) die('Usage: block "<reason>" (or --note-file <path>)');
  state.status = "blocked";
  state.blocked = reason;
  save(state);
  console.log(`Run ${state.id} blocked: ${reason}`);
}

// Overlays already synced stay synced; the next preflight starts a fresh run.
function abandon(rest) {
  const state = requireRun("running", "blocked");
  const why = noteText(rest, { bareWords: true });
  if (!why) die('Usage: abandon "<who decided and why>" (or --note-file <path>)');
  if (state.rehearsal?.pid && pidAlive(state.rehearsal.pid))
    die("Stop the rehearsal server first: rehearse-stop");
  state.notes.push({ at: now(), subject: "abandon", text: why });
  if (state.steps.publish?.status !== "ok") {
    for (const [b, r] of Object.entries(state.retired ?? {})) {
      const restored = git(MAIN, "branch", b, r.tip);
      git(MAIN, "tag", "-d", r.tag);
      state.notes.push({
        at: now(),
        subject: `retire ${b}`,
        text:
          restored.code === 0
            ? "undone: the run ended before publishing it"
            : `could not restore the branch: ${restored.err}`,
      });
    }
  }
  const open = (state.requests ?? []).filter((r) => r.status === "open");
  if (open.length) {
    const inbox = readInbox();
    writeInbox({
      ...inbox,
      requests: [...open.map(({ status: _, ...r }) => r), ...inbox.requests],
    });
    for (const r of open) r.status = "returned to the inbox";
  }
  state.status = "abandoned";
  save(state);
  console.log(
    `Run ${state.id} abandoned.${open.length ? ` ${open.length} open request(s) went back to the inbox.` : ""}`,
  );
}

// ---------------------------------------------------------------- requests

function ask(args) {
  const inbox = readInbox();
  const text = noteText(args, { bareWords: true });
  if (!text) {
    if (!inbox.requests.length) return console.log("The inbox is empty.");
    for (const r of inbox.requests) console.log(`#${r.id} (${r.at}): ${r.text}`);
    return;
  }
  const request = { id: inbox.next, at: now(), text };
  writeInbox({ next: inbox.next + 1, requests: [...inbox.requests, request] });
  console.log(`Request #${request.id} waits for the next run.`);
}

function unask([id]) {
  const inbox = readInbox();
  const kept = inbox.requests.filter((r) => r.id !== requestId(id));
  if (kept.length === inbox.requests.length)
    die(`No request #${requestId(id)} in the inbox. A run that took it over closes it.`);
  writeInbox({ ...inbox, requests: kept });
  console.log(`Removed request #${requestId(id)}.`);
}

function closeRequest(status, [id, ...rest]) {
  const state = requireRun("running");
  const r = state.requests?.find((x) => x.id === requestId(id));
  if (!r) die(`Run ${state.id} has no request #${requestId(id)}.`);
  const outcome = noteText(rest);
  if (!outcome)
    die(`A note is required: ${status === "done" ? "done" : "decline"} <id> --note "<...>"`);
  Object.assign(r, { status, outcome, closedAt: now() });
  save(state);
  console.log(`Request #${r.id} ${status}.`);
}

// Archives the overlay at its newest commit that still changed something, so the retired ref
// shows the change on the upstream it last applied to, not the empty tip upstream absorbed.
function retire([name, ...args]) {
  const state = requireRun("running");
  requireSteps(state, "plan");
  const note = noteText(args);
  if (!name || !note) die('Usage: retire <overlay> --note "<the request and who approved it>"');
  const b = name.startsWith("overlay/") ? name : `overlay/${name}`;
  if (!listOverlays().includes(b)) die(`No branch ${b}.`);
  const holder = worktreesByBranch().get(b);
  if (holder) die(`${b} is checked out in ${holder}; leave it for fire.`);
  const tip = rev(b);
  const forkTip = git(MAIN, "rev-parse", "-q", "--verify", `refs/remotes/${FORK}/${b}`).out || null;
  if (forkTip && !isAncestor(forkTip, tip))
    die(`${FORK}/${b} has commits ${b} does not; leave it for fire.`);
  const archive =
    gitOk(MAIN, "rev-list", "--topo-order", b, "^origin/main")
      .split("\n")
      .filter(Boolean)
      .find(
        (c) =>
          git(MAIN, "diff", "--quiet", gitOk(MAIN, "merge-base", c, "origin/main"), c).code !== 0,
      ) ?? tip;
  const base = gitOk(MAIN, "merge-base", archive, "origin/main");
  const tag = `retired/${b}`;
  if (git(MAIN, "rev-parse", "-q", "--verify", `refs/tags/${tag}`).code === 0)
    die(`Tag ${tag} already exists.`);
  gitOk(
    MAIN,
    "tag",
    "-a",
    tag,
    archive,
    "-m",
    `Retired ${b}: ${note}\n\nIts last change, on origin/main ${short(base)}. Branch tip at retirement: ${short(tip)} (upgrade run ${state.id}).`,
  );
  gitOk(MAIN, "branch", "-D", b);
  state.retired = { ...state.retired, [b]: { tag, archive, base, tip, forkTip, at: now() } };
  delete state.overlays[b];
  state.notes.push({ at: now(), subject: `retire ${b}`, text: note });
  invalidate(state, "rebuild", "check-local", "rehearse", "ui", "publish");
  save(state);
  console.log(
    `Retired ${b} as tag ${tag} at ${short(archive)} (its last change, on origin/main ${short(base)}).\npublish pushes the tag and a ${tag} branch to ${FORK} and deletes ${FORK}'s ${b}.`,
  );
}

function unblock(rest) {
  const state = requireRun("blocked");
  const why = noteText(rest, { bareWords: true });
  if (!why) die('Usage: unblock "<who decided what>" (or --note-file <path>)');
  state.notes.push({ at: now(), subject: "unblock", text: `${state.blocked} -> ${why}` });
  state.status = state.deploy ? "deployed" : "running";
  state.blocked = null;
  save(state);
  console.log(`Run ${state.id} is ${state.status} again.`);
}

function status() {
  const state = loadRun();
  console.log(
    state
      ? fs.readFileSync(path.join(runDir(state.id), "report.md"), "utf8")
      : "No upgrade run yet.",
  );
  const { requests } = readInbox();
  if (requests.length)
    console.log(
      `Inbox for the next run:\n${requests.map((r) => `- #${r.id}: ${r.text}`).join("\n")}`,
    );
}

const steps = {
  preflight,
  plan,
  sync,
  resolve,
  resolved,
  fix,
  fixed,
  intent,
  check,
  baseline,
  rebuild,
  accept,
  rehearse,
  ui,
  "rehearse-stop": rehearseStop,
  publish,
  deploy,
  "verify-deploy": verifyDeploy,
  note,
  block,
  unblock,
  abandon,
  status,
  ask,
  unask,
  done: (args) => closeRequest("done", args),
  decline: (args) => closeRequest("declined", args),
  retire,
};

const [name, ...args] = process.argv.slice(2);
if (!steps[name]) {
  die(
    `Usage: node scripts/upgrade.mjs <${Object.keys(steps).join("|")}> [args]\nSee UPGRADING.md.`,
  );
}
await steps[name](args);
