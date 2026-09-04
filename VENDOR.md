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

# 4. Rebuild main and build
mise run update
```

When adding an overlay, also add it to the `rebuild-main` task in `.mise.toml`
below `overlay/dev-infra` (merge order matters).

## Mise Tasks

| Task                    | Purpose                                                                                   |
| ----------------------- | ----------------------------------------------------------------------------------------- |
| `mise run sync`         | Fetch origin/main, re-overlay all `overlay/*` branches onto it, return to original branch |
| `mise run rebuild-main` | Reset `main` to origin/main, merge overlays (dev-infra first)                             |
| `mise run build`        | Build all workspace packages                                                              |
| `mise run update`       | rebuild-main + pnpm install + build (quick rebuild)                                       |
| `mise run upgrade`      | sync + rebuild-main + pnpm install + build (full upstream sync)                           |
| `mise run serve`        | Run the built server (extra args pass through)                                            |

Tasks chain sequentially via `&&` — mise `depends` runs tasks in parallel,
which races the git operations. Don't convert these to `depends`.

`git reset --hard` is only permitted inside `mise run rebuild-main` — it is safe
there because `main` carries no unique work. Never run it manually.

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

The `patches/` directory and `patchedDependencies` are **upstream's own**
pnpm patch mechanism — our overlays never touch it.

## Commit Hooks

The repo's pre-commit hook (`vp staged` via `.vite-hooks/`) runs `vp fmt` on
all staged files.

- **Never bypass hooks.** `--no-verify` is forbidden.
- Known quirk: on a fresh install, the first `vp fmt` invocations can fail
  with `Failed to load configuration file. Failed to parse config: expected
value at line 1 column 1` while the vp core's config handshake warms up.
  This is a transient environment state, not a content problem — re-run the
  commit; it succeeds once warm. If it persists, debug before bypassing
  anything.

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
mise run rebuild-main           # main gets it via the overlay
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
