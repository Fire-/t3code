#!/usr/bin/env node
// Computes and copies the t3 server bundle's native runtime dependency
// closure from the vendor pnpm store into a runtime tier.
//
// The server bundle (apps/server/dist) externalizes a handful of packages;
// everything else is packed into bin.mjs and its chunks. Packages are
// copied from the store as BUILT, PATCHED binaries (each package's whole
// store node_modules level, so sibling deps like ffi-rs come along) so a
// tier is byte-identical to what its build was tested against and
// survives deletion of the vendor directory.
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

const vendorRoot = path.resolve(import.meta.dirname, "..");
const serverNM = path.join(vendorRoot, "apps/server/node_modules");
const store = path.join(vendorRoot, "node_modules/.pnpm");

const target = process.argv[2];
if (!target) {
  console.error("usage: node runtime-natives.mjs <target-node_modules-dir>");
  process.exit(1);
}
mkdirSync(target, { recursive: true });

// Seeds: what the bundle imports at runtime (verified by booting a bare
// dist copy and iterating on ERR_MODULE_NOT_FOUND). Extend if a future
// upstream build externalizes more.
const SEEDS = ["node-pty", "@ff-labs/fff-node", "bufferutil", "utf-8-validate", "msgpackr-extract"];

const PLATFORM = `${process.platform}-${process.arch}`;
const PLATFORM_RE =
  /(darwin|linux|win32|android|freebsd|openbsd|sunos|aix)-(arm64|x64|ia32|arm|universal|riscv64|loong64)/;

function warn(msg) {
  console.warn(`WARNING: ${msg}`);
}

function findStoreNm(name) {
  const key = name.replace("/", "+");
  const candidates = readdirSync(store)
    .filter((d) => d.startsWith(`${key}@`))
    .sort();
  if (candidates.length === 0) {
    throw new Error(`package not found in pnpm store: ${name}`);
  }
  if (candidates.length > 1) {
    warn(
      `multiple store variants for ${name}: [${candidates.join(", ")}]; picking ${candidates[0]}`,
    );
  }
  return path.join(store, candidates[0], "node_modules");
}

function nmLevelOf(pkgDir) {
  // Walk up to the store's node_modules level (scoped packages like
  // @ff-labs/fff-node add depth beyond one dirname).
  let dir = path.dirname(pkgDir);
  for (let i = 0; i < 4 && path.basename(dir) !== "node_modules"; i++) {
    dir = path.dirname(dir);
  }
  if (path.basename(dir) !== "node_modules") {
    throw new Error(`could not find store node_modules level above ${pkgDir}`);
  }
  return dir;
}

function resolvePkgDir(name, hintNm) {
  const directLink = path.join(serverNM, name);
  if (existsSync(directLink)) return realpathSync(directLink);
  if (hintNm) {
    const sibling = path.join(hintNm, name);
    if (existsSync(sibling)) return realpathSync(sibling);
  }
  return realpathSync(path.join(findStoreNm(name), name));
}

const visited = new Set();
const copiedNmLevels = new Set();

function addPackage(name, hintNm) {
  if (visited.has(name)) return;
  visited.add(name);

  const pkgDir = resolvePkgDir(name, hintNm);
  const nmLevel = nmLevelOf(pkgDir);

  // Copy the whole store node_modules level (the package plus any
  // siblings pnpm placed next to it), dereferencing symlinks, merging
  // into the target.
  if (!copiedNmLevels.has(nmLevel)) {
    copiedNmLevels.add(nmLevel);
    cpSync(nmLevel, target, { recursive: true, dereference: true, force: true });
  }

  const pkgJson = JSON.parse(readFileSync(path.join(pkgDir, "package.json"), "utf8"));
  const deps = {
    ...(pkgJson.dependencies ?? {}),
    ...(pkgJson.optionalDependencies ?? {}),
  };
  for (const dep of Object.keys(deps)) {
    // Skip platform binaries for other platforms (napi-rs optional deps).
    if (PLATFORM_RE.test(dep) && !dep.includes(PLATFORM)) continue;
    addPackage(dep, nmLevel);
  }
}

try {
  for (const seed of SEEDS) addPackage(seed, null);
} catch (err) {
  console.error(`ERROR: ${err.message}`);
  process.exit(1);
}

// Prune other platforms' binaries dragged in by store-level copies
// (napi-rs optional deps for every platform sit as siblings, often under
// scope dirs like @yuuang/ or @ff-labs/). Bounded walk.
function pruneOtherPlatforms(dir, depth) {
  if (depth > 3) return;
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const full = path.join(dir, entry.name);
    if (PLATFORM_RE.test(entry.name) && !entry.name.includes(PLATFORM)) {
      rmSync(full, { recursive: true, force: true });
    } else {
      pruneOtherPlatforms(full, depth + 1);
    }
  }
}
pruneOtherPlatforms(target, 0);

// Sanity: every seed must exist as a real (non-dangling) directory.
for (const seed of SEEDS) {
  if (!existsSync(path.join(target, seed, "package.json"))) {
    console.error(`ERROR: incomplete copy of ${seed} in ${target}`);
    process.exit(1);
  }
}

console.log(`natives -> ${target} (${[...visited].join(", ")})`);
