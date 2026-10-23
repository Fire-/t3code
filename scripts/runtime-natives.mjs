#!/usr/bin/env node
// Computes and copies the t3 server bundle's runtime externals from the
// vendor pnpm install into a runtime tier.
//
// The server bundle (apps/server/dist) keeps the packages matched by
// scripts/lib/cli-external-packages.ts out of bin.mjs. Upstream's release
// archive (scripts/build-cli-archive.ts) installs the server's direct
// dependencies from that list; this does the same, but copies the built,
// patched packages this vendor build was tested against instead of
// reinstalling, so a tier survives deletion of the vendor directory and
// needs no registry.
//
// Usage: node runtime-natives.mjs <target-node_modules-dir>

import {
  readFileSync,
  existsSync,
  realpathSync,
  readdirSync,
  cpSync,
  rmSync,
  mkdirSync,
} from "node:fs";
import path from "node:path";

import { selectCliRuntimeExternalDependencies } from "./lib/cli-external-packages.ts";

const vendorRoot = path.resolve(import.meta.dirname, "..");
const serverDir = path.join(vendorRoot, "apps/server");

const target = process.argv[2];
if (!target) {
  console.error("usage: node runtime-natives.mjs <target-node_modules-dir>");
  process.exit(1);
}
mkdirSync(target, { recursive: true });

const PLATFORM = `${process.platform}-${process.arch}`;
const PLATFORM_RE =
  /(darwin|linux|win32|android|freebsd|openbsd|sunos|aix)-(arm64|x64|ia32|arm|universal|riscv64|loong64)/;
const isForeignPlatform = (name) => PLATFORM_RE.test(name) && !name.includes(PLATFORM);

const serverPackage = JSON.parse(readFileSync(path.join(serverDir, "package.json"), "utf8"));
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
  const link = path.join(fromNodeModules, name);
  if (!existsSync(link)) {
    if (optional) return;
    throw new Error(`${parent} depends on ${name}, which is not installed beside it`);
  }
  const pkgDir = realpathSync(link);
  const existing = placed.get(name);
  if (existing !== undefined) {
    if (existing !== pkgDir) {
      throw new Error(`two versions of ${name} in the closure:\n  ${existing}\n  ${pkgDir}`);
    }
    return;
  }
  placed.set(name, pkgDir);

  const dest = path.join(target, name);
  mkdirSync(path.dirname(dest), { recursive: true });
  cpSync(pkgDir, dest, { recursive: true, dereference: true, force: true });

  const ownNodeModules = pkgDir.slice(0, -(name.length + 1));
  const pkgJson = JSON.parse(readFileSync(path.join(pkgDir, "package.json"), "utf8"));
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
    addPackage(seed, path.join(serverDir, "node_modules"), false, "apps/server");
} catch (err) {
  console.error(`ERROR: ${err.message}`);
  process.exit(1);
}

// Packages such as node-pty ship prebuilds for every platform inside the
// package itself.
function pruneOtherPlatforms(dir, depth) {
  if (depth > 3) return;
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const full = path.join(dir, entry.name);
    if (isForeignPlatform(entry.name)) {
      rmSync(full, { recursive: true, force: true });
    } else {
      pruneOtherPlatforms(full, depth + 1);
    }
  }
}
pruneOtherPlatforms(target, 0);

for (const seed of SEEDS) {
  if (!existsSync(path.join(target, seed, "package.json"))) {
    console.error(`ERROR: incomplete copy of ${seed} in ${target}`);
    process.exit(1);
  }
}

console.log(`natives -> ${target} (${[...placed.keys()].join(", ")})`);
