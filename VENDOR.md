# T3 Code Local Development (Vendored)

This is a **vendored clone** of `https://github.com/pingdotgg/t3code` with local
patches maintained via git-overlay branches. The goal is to stay close to
upstream `main` while carrying our own infra config.

## Architecture

### Branch Structure

```
origin/main               ← upstream (never commit here)
├── overlay/dev-infra     ← .mise.toml, VENDOR.md
└── main                  ← local integration branch (merge commits ONLY)
```

**Rule #1: Never commit directly to `main`.** It must remain merge-commits-only.
All real work lives on `overlay/*` branches; `main` is rebuilt from them, which
is what makes `git reset --hard origin/main` safe _inside the mise tasks_.

**Rule #2: One concern per overlay.** Don't mix unrelated changes.

**Rule #3: Apply `overlay/dev-infra` first** in merge order — it contains
`.mise.toml` (node/pnpm toolchain + PATH) and this file, which every other
workflow depends on.

## Creating a New Overlay

```bash
# 1. Create branch from origin/main
git checkout -b overlay/my-feature origin/main

# 2. Make changes, commit with conventional commits
git add <specific-files-only>
git commit -m "fix(scope): description"

# 3. Overlay onto origin/main (creates single merge commit on clean base)
bash ~/code/git-overlay/git-overlay origin/main overlay/my-feature

# 4. Rebuild local and build
mise run update
```

Overlay branches are **independent topics**, each synced onto `origin/main`
itself — never stacked on each other. The `local` branch is the integration
branch and is rebuilt from scratch on every build:
`origin/main` + every `overlay/*` branch, merged in alphabetical order
(`dev-infra` before `prime-agent`). rerere remembers conflict resolutions
between rebuilds.

When two overlays need the same change, cherry-pick one commit into each and
keep its subject identical. Merging one overlay into another, or branching
from one, is stacking even when it only shares a single commit; if an overlay
needs part of another, split that part into its own commit and cherry-pick it.
`mise run check-overlays` (run first by `rebuild-local`) fails if two overlays
contain the very same commit, and matches cherry-picked copies by subject and
fails if their patches differ; amend every copy together or none.

## Mise Tasks

| Task                                          | Purpose                                                                                                                                                                                |
| --------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `mise run sync`                               | Fetch origin/main, re-overlay `overlay/*` branches that lack it, return to original branch; overlays that would conflict are left untouched and listed, so fix those by hand and rerun |
| `mise run rebuild-local`                      | Reset `local` to origin/main, re-merge all `overlay/*` branches, end on `local`                                                                                                        |
| `mise run check-overlays`                     | Fail if overlays stack (share the very same commit) or a commit cherry-picked into several overlays differs between them                                                               |
| `mise run build`                              | Build all workspace packages                                                                                                                                                           |
| `mise run smoke-test`                         | Boot-check the built server bundle (`bin.mjs --version`)                                                                                                                               |
| `mise run swap`                               | Promote the build to `~/.t3/code/dist/current` (shifts current→bak)                                                                                                                    |
| `mise run swap-back`                          | Restore the previous build from bak to current                                                                                                                                         |
| `mise run setup-stable`                       | Pin the current bak as the stable fallback tier                                                                                                                                        |
| `mise run generate-runtime`                   | (Re)generate the `~/.t3/code` shim, AGENTS.md, and tier scaffold                                                                                                                       |
| `mise run update`                             | rebuild-local + install + build + smoke-test + swap                                                                                                                                    |
| `mise run upgrade`                            | sync + rebuild-local + install + build + smoke-test + swap                                                                                                                             |
| `mise run serve`                              | Run the built server from the vendor tree (dev mode)                                                                                                                                   |
| `mise run daemon`                             | Start the self-updating daemon (detached, survives everything)                                                                                                                         |
| `mise run daemon-stop`                        | Stop the daemon and the server it supervises                                                                                                                                           |
| `mise run daemon-status`                      | Daemon, server, tier, and busy-session health at a glance                                                                                                                              |
| `mise run daemon-log` / `server-log` / `logs` | Tail daemon log / server log / both                                                                                                                                                    |

Tasks chain sequentially via `&&` — mise `depends` runs tasks in parallel,
which races the git operations. Don't convert these to `depends`.

`git reset --hard` is only permitted inside `mise run rebuild-local` — it is
safe there because `local` is rebuilt from scratch and overlays carry all
unique work. Never run it manually. `rebuild-local` deliberately ends on
`local`: after a rebuild you are always where integration commits belong,
and direct `main` commits become structurally impossible to stumble into.
Topic commits go on their `overlay/*` branch, never on `local`.

After each deploy, push to the `fork` remote (`fire-/t3code`): `overlay/*`
branches normally (they only fast-forward), and `local` with
`git push --force-with-lease fork local`. `local` is rebuilt from scratch, so
it never fast-forwards; the pushed copy is also the only remote record of the
merge conflict resolutions, since rerere's cache stays local.
Never force-push an `overlay/*` branch.

## Runtime (~/.t3/code)

The installed runtime lives outside the vendor dir and survives
`rm -rf ~/vendor/t3code`. The vendor tree is for development; the runtime is
what actually serves.

```
~/.t3/code/
  bin/t3           <- runner on PATH (tier walk: current -> bak -> stable)
  dist/current/    <- active build: bin.mjs + chunks + client/ + node_modules/
  dist/bak/        <- previous build (auto-created by swap)
  dist/stable/     <- pinned known-good (setup-stable)
  userdata/        <- server data (T3CODE_HOME; statev2.sqlite, plus the frozen v1 state.sqlite)
```

- `bin/t3` is a generated script (by `scripts/generate-runtime.sh`) that
  exports `T3CODE_HOME=~/.t3/code` and execs the first tier containing a
  `bin.mjs`. It uses a baked absolute node 24 path (the server needs
  `node:sqlite`); re-run `mise run generate-runtime` after toolchain changes.
- Each tier's `node_modules/` holds the packages the bundle keeps external:
  the same server dependencies upstream's release archive installs
  (`scripts/lib/cli-external-packages.ts`: @cursor/sdk, @ff-labs/fff-node,
  @napi-rs/keyring, node-pty, playwright-core, undici) plus their
  dependency closure. `scripts/runtime-natives.mjs` copies them BUILT and
  PATCHED from this vendor install, so a tier runs exactly what was tested
  and needs no registry. `swap` boots the staged tier before promoting it.
- Daily flow: `mise run upgrade` (updates the runtime via swap), phone pairs
  against the running server. Fallback to the stable tier: `rm -rf
~/.t3/code/dist/current && cp -R ~/.t3/code/dist/stable
~/.t3/code/dist/current` (or `mise run swap-back` if bak is healthy).
- On PATH via the labeled `~/.zshrc` block:

  ```bash
  ### t3code:start
  # t3 code runtime (current/bak/stable tiers)
  export PATH="$HOME/.t3/code/bin:$PATH"
  ### t3code:end
  ```

Note: ~/.zshrc is a symlink into ~/Dropbox/dotfiles — edit the symlink
target, never `sed -i` the link.

### Self-updating daemon

`mise run daemon` starts a detached supervisor (`scripts/daemon.sh`,
`nohup`+`disown`, reparented to launchd) that:

- restarts the server on crash (3→6→12s backoff, then tier fallback)
- watches `dist/current/bin.mjs` mtime — `swap` and `swap-back` both
  change it, and within 30s the daemon restarts onto that build, **but
  only when idle** for 3 consecutive checks (~90s). The idle probe,
  `scripts/t3-busy-v2.sql`, reads `statev2.sqlite` read-only and counts
  threads with an active or queued run, a pending approval or question, a
  provider background task, a running provider subagent or tool call, or
  a running effect. It fails closed: a probe error counts as busy. A v1
  server (before the cutover, or after a forced swap-back) gets the old
  v1 probe instead.
- falls back `current → bak → stable` if a build fails its health check
  (up to `T3_HEALTH_TIMEOUT`, default 300s, while the process stays
  alive), skipping pre-v2 builds once `statev2.sqlite` exists
- re-execs itself when `scripts/daemon.sh` changes (adopting the live
  server via a port-scoped `lsof` lookup — zero downtime)

This intentionally replaces `t3 service install` (launchd): launchd cannot
express idle-gated restarts. Running both would fight — don't.

Tune via env: `T3_PORT` (default 3773), `T3_HOST` (default `0.0.0.0` — all
interfaces, matching opencode's `*:80` posture and surviving DHCP changes;
set `127.0.0.1` for loopback-only), `T3_RUNTIME_DIR` (default `~/.t3/code`),
`T3_HEALTH_TIMEOUT` (seconds, default 300), `T3_ALLOW_V1_FALLBACK=1`
(permit pre-v2 tiers after the cutover; also unblocks `swap-back`).

### Orchestration v2 cutover

The first v2 server boot snapshots `userdata/state.sqlite` into
`userdata/statev2.sqlite` and imports threads from it; it never writes
`state.sqlite` again. On this machine's ~800 MB database the snapshot,
migrations, and import took about 2 seconds. To roll back,
`T3_ALLOW_V1_FALLBACK=1 mise run swap-back` onto the last v1 build: it
serves `state.sqlite` as it was at the cutover, so anything done in v2 is
missing there. Going forward again reuses the existing `statev2.sqlite`,
so anything done in v1 meanwhile is not re-imported (move `statev2.sqlite`
aside first to re-import from scratch). `swap-back` and `setup-stable`
refuse pre-v2 builds once `statev2.sqlite` exists.

Never `export T3CODE_HOME` ambiently. t3 treats it as an explicit base dir,
which flips dev-mode state from `<base>/dev` to `<base>/userdata` — i.e. an
ambient value can point dev experiments at a live production DB. The shim and
daemon already pin it per-process; base dirs are: `~/.t3` (vendor-dir CLI
default), `~/.t3/code` (this runtime), `<repo>/dev` (vendor dev runs).

## First-Time Setup

```bash
mise trust          # once, mise prompts on untrusted configs
mise install        # node 24 + pnpm 11.10.0 from .mise.toml
pnpm install        # deps + upstream's patchedDependencies in patches/
mise run build
```

`node_modules/.bin` (vp, oxlint, vitest, tsdown, ...) is on PATH inside this
directory via mise, so the project-pinned `vp` is always what runs.

## Build and Run

```bash
mise run build                                   # build everything
mise run serve                                   # http://127.0.0.1:3773 (default)
mise run serve -- --host 0.0.0.0                 # LAN / WireGuard access
mise run serve -- --port 8080 --no-browser       # any CLI flags pass through
```

Mobile app pairs against the server directly (QR/pairing URL printed at
startup) over LAN or WireGuard; `--tailscale-serve` is also available.

## Testing

```bash
pnpm tc        # typecheck (workspace)
pnpm test      # tests (workspace)
pnpm lint      # lint
```

## Upstream vs. Ours

If typecheck, tests, or builds fail in code that no overlay branch touches,
they are upstream's issues — do not fix, track, or document them here. Only
failures introduced by overlay changes are ours to fix.

Exception: when such a failure exposes a real product bug (not just a broken
or flaky test), it may get its own small fix overlay, but only with the user's
explicit approval. Fix the code, not the test, and drop the overlay once
upstream fixes it.

The `patches/` directory and `patchedDependencies` are **upstream's own**
pnpm patch mechanism — our overlays never touch it.

## Commit Hooks

The repo's pre-commit hook (`vp staged` via `.vite-hooks/`) runs `vp fmt` on
all staged files.

- **Never bypass hooks.** `--no-verify` is forbidden.
- Known failure mode: `Failed to load configuration file. Failed to parse
config: expected value at line 1 column 1`. This means `node_modules/oxfmt`
  is a **stale broken symlink** (vp's core resolves `oxfmt` by walking up to
  the root `node_modules/`, and a version bump that prunes the old store
  entry orphans the link). Fix by repointing it:

  ```bash
  ln -sfn "$(cd node_modules && ls -d .pnpm/oxfmt@*/node_modules/oxfmt | tail -1)" node_modules/oxfmt
  ```

  The target must be relative to `node_modules/` (where the link lives), so
  list from inside it; a `node_modules/.pnpm/...` target resolves to
  `node_modules/node_modules/...` and stays broken. `vp lint` resolves
  `oxlint` the same way; repoint `node_modules/oxlint` with the same command
  (s/oxfmt/oxlint/) if it fails the same way.

  Then re-run the commit. It is an environment state, not a content problem.

## Critical Git Rules

Adapted from `~/vendor/prime-agent/VENDOR.md` — multiple agents may share
this worktree.

### Forbidden

These destroy work or violate the overlay model:

- `git commit` on `main` — merge-commits-only
- `git add -A` / `git add .` — stages files from other overlays or agents
- `git checkout .` / `git clean -fd` — destroys uncommitted changes
- `git stash` without checking what's stashed — may capture other agents' work
- `git commit --no-verify` — bypasses required checks; never allowed
- `git pull` on `main` — use `mise run upgrade` instead; a plain pull stacks
  local merge commits awkwardly on top of upstream

### Safe Workflow

```bash
# 1. Check status
git status

# 2. Make sure you're on the right overlay branch
git branch --show-current   # should be overlay/*, NOT main

# 3. Add ONLY files you changed
git add .mise.toml

# 4. Commit
git commit -m "chore(infra): description"

# 5. Rebuild and build
mise run update
```

### If You Accidentally Edit on `main`

```bash
git stash                       # note what you stashed
git checkout overlay/the-right-one
git stash pop
git add <specific-files>
git commit -m "type(scope): description"
mise run rebuild-local           # local gets it via the overlay
```

## Notes for Agents

- This repo is **not** our project — it's a vendored upstream. Don't refactor
  upstream code unless necessary for our patch.
- The `git-overlay` tool lives at `~/code/git-overlay/git-overlay` (installed
  as `git overlay`); the mise tasks call it by path.
- `mise run sync` preserves your current branch after overlaying.
- `mise run serve` requires a prior `mise run build`; it runs
  `apps/server/dist/bin.mjs`.
- T3 Connect (relay/mobile push) needs Clerk + relay infra we don't run;
  direct pairing and WireGuard/LAN access work out of the box.
