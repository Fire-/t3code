#!/usr/bin/env node
// Copies the server bundle's runtime externals (scripts/lib/cli-external-packages.ts) from this
// pnpm install into a runtime tier. Unlike upstream's release archive it copies the built, patched
// packages this build was tested with, so a tier needs no registry and survives losing the vendor
// checkout. Usage: node runtime-natives.mjs <target-node_modules-dir>

import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import { selectCliRuntimeExternalDependencies } from "./lib/cli-external-packages.ts";

const vendorRoot = NodePath.resolve(import.meta.dirname, "..");
const serverDir = NodePath.join(vendorRoot, "apps/server");

const target = process.argv[2];
if (!target) {
  console.error("usage: node runtime-natives.mjs <target-node_modules-dir>");
  process.exit(1);
}
NodeFS.mkdirSync(target, { recursive: true });

// oxlint-disable-next-line t3code/no-global-process-runtime -- plain Node build script, no Effect runtime to inject
const PLATFORM = `${process.platform}-${process.arch}`;
const PLATFORM_RE =
  /(darwin|linux|win32|android|freebsd|openbsd|sunos|aix)-(arm64|x64|ia32|arm|universal|riscv64|loong64)/;
const isForeignPlatform = (name) => PLATFORM_RE.test(name) && !name.includes(PLATFORM);

const serverPackage = JSON.parse(
  NodeFS.readFileSync(NodePath.join(serverDir, "package.json"), "utf8"),
);
const SEEDS = Object.keys(selectCliRuntimeExternalDependencies(serverPackage.dependencies ?? {}));
if (SEEDS.length === 0) {
  console.error(
    "ERROR: apps/server declares no runtime externals; did cli-external-packages.ts change?",
  );
  process.exit(1);
}

// The tier is a flat node_modules, so each package name may resolve to only
// one directory across the whole closure.
const placed = new Map();

// pnpm links a package's dependencies as siblings in the node_modules
// directory that contains it, so resolving from there yields exactly the
// versions the vendor build used.
function addPackage(name, fromNodeModules, optional, parent) {
  const link = NodePath.join(fromNodeModules, name);
  if (!NodeFS.existsSync(link)) {
    if (optional) return;
    throw new Error(`${parent} depends on ${name}, which is not installed beside it`);
  }
  const pkgDir = NodeFS.realpathSync(link);
  const existing = placed.get(name);
  if (existing !== undefined) {
    if (existing !== pkgDir) {
      throw new Error(`two versions of ${name} in the closure:\n  ${existing}\n  ${pkgDir}`);
    }
    return;
  }
  placed.set(name, pkgDir);

  const dest = NodePath.join(target, name);
  NodeFS.mkdirSync(NodePath.dirname(dest), { recursive: true });
  NodeFS.cpSync(pkgDir, dest, { recursive: true, dereference: true, force: true });

  const ownNodeModules = pkgDir.slice(0, -(name.length + 1));
  const pkgJson = JSON.parse(NodeFS.readFileSync(NodePath.join(pkgDir, "package.json"), "utf8"));
  for (const dep of Object.keys(pkgJson.dependencies ?? {})) {
    addPackage(dep, ownNodeModules, false, name);
  }
  for (const dep of Object.keys(pkgJson.optionalDependencies ?? {})) {
    if (isForeignPlatform(dep)) continue;
    addPackage(dep, ownNodeModules, true, name);
  }
}

try {
  for (const seed of SEEDS)
    addPackage(seed, NodePath.join(serverDir, "node_modules"), false, "apps/server");
} catch (err) {
  console.error(`ERROR: ${err.message}`);
  process.exit(1);
}

// Packages such as node-pty ship prebuilds for every platform inside the
// package itself.
function pruneOtherPlatforms(dir, depth) {
  if (depth > 3) return;
  for (const entry of NodeFS.readdirSync(dir, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const full = NodePath.join(dir, entry.name);
    if (isForeignPlatform(entry.name)) {
      NodeFS.rmSync(full, { recursive: true, force: true });
    } else {
      pruneOtherPlatforms(full, depth + 1);
    }
  }
}
pruneOtherPlatforms(target, 0);

for (const seed of SEEDS) {
  if (!NodeFS.existsSync(NodePath.join(target, seed, "package.json"))) {
    console.error(`ERROR: incomplete copy of ${seed} in ${target}`);
    process.exit(1);
  }
}

console.log(`natives -> ${target} (${[...placed.keys()].join(", ")})`);
