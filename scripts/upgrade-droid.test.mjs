import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vite-plus/test";

import {
  BACKUPS_KEPT,
  adapterMethods,
  compareVersions,
  describeDroid,
  parseUpdateCheck,
  releaseDroid,
  restoreDroid,
  updateDroid,
} from "./upgrade-droid.mjs";

const ADAPTER = ["droid.add_user_message", "droid.initialize_session", "droid.list_models"];

let dir;
let bin;
let releases;
let paths;

// A stand-in `droid`: `update -v` copies a release over itself, like the real installer.
const fakeDroid = ({ version, next = null, methods = ADAPTER, models = ["gemini-4-neon"] }) =>
  `#!/usr/bin/env node
const NodeFS = require("node:fs");
const [cmd, ...rest] = process.argv.slice(2);
const methods = ${JSON.stringify(methods)};
if (cmd === "--version") console.log(${JSON.stringify(version)});
else if (cmd === "update" && rest[0] === "--check")
  console.log(${JSON.stringify(next ? `! Update available: ${next}` : "Droid is up to date")});
else if (cmd === "update" && rest[0] === "-v") {
  NodeFS.copyFileSync(${JSON.stringify(releases)} + "/" + rest[1], process.argv[1]);
  console.log("Successfully updated to version " + rest[1]);
} else if (cmd === "exec") {
  let buf = "";
  process.stdin.on("data", (chunk) => {
    buf += chunk;
    let i;
    while ((i = buf.indexOf("\\n")) >= 0) {
      const req = JSON.parse(buf.slice(0, i));
      buf = buf.slice(i + 1);
      const reply = { jsonrpc: "2.0", type: "response", id: req.id };
      if (req.method === "droid.list_models" && methods.includes(req.method))
        reply.result = { models: ${JSON.stringify(models)}.map((id) => ({ id })) };
      else reply.error = { code: methods.includes(req.method) ? -32602 : -32601, message: "x" };
      process.stdout.write(JSON.stringify(reply) + "\\n");
    }
  });
}
`;

function writeRelease(version, opts = {}) {
  NodeFS.writeFileSync(NodePath.join(releases, version), fakeDroid({ version, ...opts }), {
    mode: 0o755,
  });
}
function install(version, opts = {}) {
  NodeFS.writeFileSync(bin, fakeDroid({ version, ...opts }), { mode: 0o755 });
}
const installedVersion = () =>
  NodeFS.readFileSync(bin, "utf8").match(/console\.log\("([^"]+)"\)/)[1];
const run = (extra = {}) => updateDroid({ ...paths, methods: ADAPTER, ...extra });

beforeEach(() => {
  dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "upgrade-droid-"));
  releases = NodePath.join(dir, "releases");
  NodeFS.mkdirSync(releases);
  NodeFS.mkdirSync(NodePath.join(dir, "bin"));
  bin = NodePath.join(dir, "bin", "droid");
  paths = {
    bin,
    backups: NodePath.join(dir, "backups"),
    stateFile: NodePath.join(dir, "droid.json"),
    cwd: dir,
  };
});
afterEach(() => NodeFS.rmSync(dir, { recursive: true, force: true }));

describe("parseUpdateCheck", () => {
  it("reads the offered version", () => {
    expect(parseUpdateCheck("Checking for updates...\n! Update available: 1.15.0\n")).toBe(
      "1.15.0",
    );
  });
  it("is null when droid is current", () => {
    expect(parseUpdateCheck("Checking for updates...\nDroid is up to date")).toBeNull();
  });
});

describe("compareVersions", () => {
  it("compares numerically", () => {
    expect(compareVersions("1.10.0", "1.9.0")).toBeGreaterThan(0);
    expect(compareVersions("1.9.0", "1.10.0")).toBeLessThan(0);
    expect(compareVersions("1.15.0", "1.15.0")).toBe(0);
  });
});

describe("adapterMethods", () => {
  it("collects the droid methods the adapter calls, not the ones Droid calls or tests use", () => {
    const src = NodePath.join(dir, "src");
    NodeFS.mkdirSync(src);
    NodeFS.writeFileSync(
      NodePath.join(src, "adapter.ts"),
      'send("droid.add_user_message"); on("droid.request_permission"); send("droid.add_user_message");',
    );
    NodeFS.writeFileSync(NodePath.join(src, "status.ts"), 'call("droid.list_models")');
    NodeFS.writeFileSync(NodePath.join(src, "adapter.test.ts"), 'send("droid.only_in_tests")');
    NodeFS.writeFileSync(NodePath.join(src, "rpcMockAgent.mock.ts"), 'send("droid.mocked")');
    expect(adapterMethods(src)).toEqual(["droid.add_user_message", "droid.list_models"]);
  });
  it("is empty without the adapter", () => {
    expect(adapterMethods(NodePath.join(dir, "missing"))).toEqual([]);
  });
});

describe("updateDroid", () => {
  it("leaves a current droid alone", async () => {
    install("1.15.0");
    const result = await run();
    expect(result).toMatchObject({ status: "current", version: "1.15.0" });
    expect(NodeFS.existsSync(paths.backups)).toBe(false);
  });

  it("updates, keeping the replaced version", async () => {
    install("1.10.0", { next: "1.15.0" });
    const before = NodeFS.readFileSync(bin, "utf8");
    writeRelease("1.15.0");
    const result = await run();
    expect(result).toMatchObject({ status: "updated", from: "1.10.0", to: "1.15.0" });
    expect(installedVersion()).toBe("1.15.0");
    expect(NodeFS.readFileSync(NodePath.join(paths.backups, "droid-1.10.0"), "utf8")).toBe(before);
  });

  it("puts the old version back when the new one lost an adapter method", async () => {
    install("1.10.0", { next: "1.15.0" });
    writeRelease("1.15.0", { methods: ["droid.initialize_session", "droid.list_models"] });
    const result = await run();
    expect(result).toMatchObject({
      status: "rolled-back",
      from: "1.10.0",
      to: "1.15.0",
      missing: ["droid.add_user_message"],
    });
    expect(installedVersion()).toBe("1.10.0");
    expect(NodeFS.statSync(bin).mode & 0o111).not.toBe(0);
  });

  it("puts the old version back when the new one lists no models", async () => {
    install("1.10.0", { next: "1.15.0" });
    writeRelease("1.15.0", { models: [] });
    const result = await run();
    expect(result).toMatchObject({ status: "rolled-back", missing: [] });
    expect(result.reason).toMatch(/no models/);
    expect(installedVersion()).toBe("1.10.0");
  });

  it("puts the old version back when the new one exits without answering", async () => {
    install("1.10.0", { next: "1.15.0" });
    NodeFS.writeFileSync(
      NodePath.join(releases, "1.15.0"),
      '#!/bin/sh\n[ "$1" = --version ] && echo 1.15.0\nexit 0\n',
      { mode: 0o755 },
    );
    const result = await run();
    expect(result).toMatchObject({
      status: "rolled-back",
      missing: [],
      reason: "droid 1.15.0 exited before answering",
    });
    expect(installedVersion()).toBe("1.10.0");
  });

  it("does not retry a version it rolled back, but tries the next one", async () => {
    install("1.10.0", { next: "1.15.0" });
    writeRelease("1.15.0", { methods: ["droid.list_models"] });
    await run();
    expect(await run()).toMatchObject({ status: "rejected", version: "1.10.0", offered: "1.15.0" });
    install("1.10.0", { next: "1.16.0" });
    writeRelease("1.16.0");
    expect(await run()).toMatchObject({ status: "updated", to: "1.16.0" });
  });

  it("keeps the newest backups only", async () => {
    let current = "1.10.0";
    for (const next of ["1.11.0", "1.12.0", "1.13.0", "1.14.0"]) {
      install(current, { next });
      writeRelease(next);
      expect(await run()).toMatchObject({ status: "updated", to: next });
      current = next;
    }
    const kept = NodeFS.readdirSync(paths.backups).sort();
    expect(kept).toHaveLength(BACKUPS_KEPT);
    expect(kept).toEqual(["droid-1.12.0", "droid-1.13.0"]);
  });
});

describe("restoreDroid and releaseDroid", () => {
  it("restores the newest backup and holds updates until released", async () => {
    install("1.10.0", { next: "1.15.0" });
    writeRelease("1.15.0", { next: "1.16.0" });
    await run();
    const restored = restoreDroid({ ...paths, reason: "turns hang on 1.15.0" });
    expect(restored).toMatchObject({ from: "1.15.0", to: "1.10.0" });
    expect(installedVersion()).toBe("1.10.0");
    expect(await run()).toMatchObject({ status: "held", version: "1.10.0" });
    releaseDroid(paths);
    expect(await run()).toMatchObject({ status: "rejected", offered: "1.15.0" });
  });

  it("restores a named version", async () => {
    install("1.10.0", { next: "1.11.0" });
    writeRelease("1.11.0", { next: "1.12.0" });
    await run();
    writeRelease("1.12.0");
    await run();
    expect(restoreDroid({ ...paths, version: "1.10.0", reason: "x" })).toMatchObject({
      to: "1.10.0",
    });
    expect(installedVersion()).toBe("1.10.0");
  });

  it("refuses a version it has no backup of", () => {
    install("1.15.0");
    expect(() => restoreDroid({ ...paths, version: "1.2.0", reason: "x" })).toThrow(/No backup/);
  });
});

describe("describeDroid", () => {
  it("names both versions and the cause of a rollback", () => {
    expect(
      describeDroid({
        status: "rolled-back",
        from: "1.10.0",
        to: "1.15.0",
        missing: ["droid.add_user_message"],
        reason: "droid 1.15.0 lacks droid.add_user_message",
      }),
    ).toBe(
      "Droid 1.15.0 was put back to 1.10.0: droid 1.15.0 lacks droid.add_user_message. Later runs skip 1.15.0.",
    );
  });
});
