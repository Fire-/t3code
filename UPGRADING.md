# Upgrading the fork

An agent brings every `overlay/*` branch and `local` up to upstream `origin/main`, rebuilds, rehearses the build on a copy of real data, publishes to `fork`, and deploys it. A T3 scheduled task runs this daily in the "t3code upgrades" thread; the scheduled prompt says who to escalate to. Read [VENDOR.md](VENDOR.md) first: the overlay model and its git rules apply here.

Every step is a command of `scripts/upgrade.mjs`, run from the main checkout:

```sh
cd ~/vendor/t3code && node scripts/upgrade.mjs <step> [args]
```

Each step records its outcome in `~/.t3/code/upgrades/<run>/state.json` and rewrites `report.md` beside it. Logs, patch comparisons, and screenshots go in the same folder. Steps refuse to run before the steps they depend on have passed. The report is how fire checks your work, so every judgment call goes into it through a `--note`. Write any note with an apostrophe or a line break to a file in the run folder and pass `--note-file <path>` instead (`-` reads stdin): the shell your harness runs may be macOS's bash 3.2, which cannot parse a heredoc inside `"$(...)"`. Build and test steps write to log files and print only a summary and the tail of a failure; give them a 30-minute command timeout.

## The run

1. `preflight`. Confirms the previous deploy went live, checks that no worktree has uncommitted changes, and fetches. Exit codes:
   - `10`: up to date. Reply "t3code is up to date" and stop.
   - `3`: the run is blocked. If it was already escalated, say it is still waiting and stop. Otherwise escalate.
   - `4`: a run is in progress. It is this thread's own unfinished run: `preflight --resume`, then continue from the first step that has not passed.
   - `5`: the previous deploy is not live yet, usually because T3 has not been idle. It lists the busy threads; one waiting on a question stays busy until someone answers it. Stop, and name any thread other than this one in your reply. Escalate if it is more than 20 hours old.
   - `1`: something is in the way, such as uncommitted changes in a worktree. Report it and stop. Never clean, stash, or commit other people's work.
2. `plan`. Tags every overlay and `local` (`ember-backup/<run>/<name>-pre-upgrade`), predicts conflicts, and flags upstream risks: new migrations, toolchain changes, lockfile and external-package changes, and files both upstream and an overlay changed. A toolchain change: see [Toolchain](#toolchain).
3. `sync`. Re-overlays every overlay that merges cleanly. Exit `2` lists the ones that conflict.
4. For each conflicting overlay: `resolve <overlay>`. Fix the conflicts in `~/vendor/t3code-worktrees/upgrade`, `git add` the files, and finish with `bash ~/code/git-overlay/git-overlay --continue` from that worktree. Then `resolved <overlay> --note "<what conflicted, what you kept from each side, and why>"`. Follow the [conflict rules](#conflict-rules).
5. `intent`. Compares each overlay's change before and after the sync, ignoring line numbers. Read every intent diff it lists. A diff from upstream moving code around is fine. A diff that changes what the overlay does needs a reason. Record what you found: `note <overlay> "<text>"`.
6. `check --flagged`. Runs `vp run check:changed origin/main` on every overlay that was hand-merged, hand-fixed, or changed in intent.
7. `rebuild`. Rebuilds `local`, installs, builds, smoke-tests, and checks for leftover conflict markers.
8. `check local`. The same checks on the combined build. This catches overlays that merged cleanly but broke against the new upstream.
9. `rehearse`. If upstream added migrations, it first runs them on an exact copy of the live database. Then it copies recent projects and threads into the run folder and starts the new build on them, on a port in 3790-3799.
10. `ui`. Opens the rehearsal server in a headless browser, checks that the sidebar and a thread with tool calls render without uncaught page errors, and saves screenshots. Open the screenshots and look at them: a page can pass the checks and still be broken.
11. `rehearse-stop`.
12. `publish`. Pushes the overlays (never forced) and `local` (with a lease) to `fork`, then confirms the remote matches.
13. `deploy`. Backs up the live database to `~/.t3/code/backups`, swaps the build into `dist/current`, and checks it is the rehearsed build. The daemon restarts T3 onto it once T3 is idle. This thread's own turn keeps T3 busy, so the restart happens after you finish; the next `preflight` (or `verify-deploy`) confirms it. `deploy` lists the threads busy right now; a thread waiting on a question holds the restart until someone answers it, so name it in your reply.
14. Reply with the summary below and stop.

## Failures: ours or upstream's

VENDOR.md's [Upstream vs. Ours](VENDOR.md#upstream-vs-ours) decides this. Failures in code no overlay touches are upstream's: never fix them.

- A failing test: `baseline <test files>` runs it on plain `origin/main`. If it fails there too, it is upstream's: `accept <step|overlay> --note "<why>"`.
- A failure in a file an overlay changes is ours. `fix <overlay>` checks the overlay out in the upgrade worktree. Make the smallest change that restores the overlay's behavior on the new upstream, commit it there (`type(scope): subject`, one concern per overlay, never `--no-verify`), and run `fixed <overlay> --note "<what broke and how you fixed it>"`. Then `check <overlay>`, `intent`, `rebuild`, and `check local` again.
- A test that fails once and passes on a rerun: note it as flaky with the test name.
- A failed `ui` check where the screenshots show the page is fine (for example, a long title the check could not match): `accept ui --note "<what the screenshot shows>"`.

## Conflict rules

In the upgrade worktree's merge, "ours" is upstream (`origin/main`) and "theirs" is the overlay. Go ahead without asking when:

- Upstream renamed, moved, or reformatted code the overlay changes. Reapply the overlay's change to the new code.
- Both sides added different things in the same place, such as a provider each in the same list, or new fields in a contracts schema. Keep both.
- Both sides changed different dependencies in a `package.json`. Keep both. If both changed the same dependency, take upstream's version unless pinning it is the overlay's purpose.
- `pnpm-lock.yaml`: never merge it by hand. Resolve every `package.json` first, then `git checkout origin/main -- pnpm-lock.yaml`, run `pnpm install --lockfile-only` in the upgrade worktree, and `git add` the result. Its diff against `origin/main` should only add the overlay's own packages.
- A generated file, such as a model manifest: merge both sides' entries, or rerun its generator if it has one.
- Upstream now does what the overlay did. Take upstream's version and drop the overlay's copy. Say so in the note and in your reply, so fire can retire the overlay. Never delete a branch.
- The overlay needs a small compile fix for an upstream API change, such as a renamed import or a new required argument with an obvious value.

Block and escalate when:

- Upstream rewrote or removed what the overlay patches, so keeping the overlay's behavior needs a new design.
- Your resolution would change what the overlay does.
- A migration conflicts, or an overlay's migration and upstream's want the same number.
- The resolution would rename, retype, or remove a field either side already had in a `packages/contracts` schema.
- A rehearsal shows broken behavior that you cannot trace to one overlay.
- You are unsure. A day's delay costs little; a wrong merge deployed to fire's daily driver costs a lot.

### Conflicts between overlays

Overlays are merged into `local` one at a time (alphabetically), so two overlays that change the same lines can conflict there even after each merged cleanly with upstream. `rebuild` then stops with the main checkout mid-merge on `local`. Resolve the files there by the same rules, keeping both overlays' behavior, `git add` them, and `git commit --no-edit`. rerere records the resolution. Then run `rebuild` again: it rebuilds `local` from scratch and rerere replays your resolution. Record it with `note local "<files and what you kept>"`. If `pnpm install --frozen-lockfile` fails because the merged lockfile is out of date, block: `local` takes merge commits only.

## Toolchain

When `plan` reports a changed `engines` or `packageManager` in `package.json`, update the pins in `.mise.toml` to match: `fix overlay/dev-infra`, edit, commit `chore(infra): track upstream's node <version>`, `fixed overlay/dev-infra --note "..."`, then `mise install` in the main checkout before `rebuild`. `deploy` regenerates the runtime shim when the node path moves. A version mise cannot install is a reason to block.

## Blocking and escalating

`block "<reason>"` stops the run. Every later step and preflight refuses until someone runs `unblock "<who decided what>"`, or `abandon "<why>"` to drop the run. Overlays already synced stay synced either way.

To escalate, block first, then send one message to the thread the scheduled prompt names. Include the run id, the report path, the question, the options you see, and the one you recommend. Then stop. The answer arrives as a new message in this thread: unblock with what was decided, and continue.

## If the deploy goes wrong

The daemon falls back on its own (current, then bak) when a build fails its health check, and `verify-deploy` then blocks the run. To go back by hand: `mise run swap-back`. The database backup from the deploy is `~/.t3/code/backups/statev2-pre-upgrade-<run>.sqlite`; restoring it loses everything since the deploy, so that is fire's call, never yours. The `ember-backup/<run>/*` tags hold every branch as it was before the run.

## The reply

Keep it short. fire reads it on a phone.

```
t3code upgrade <run>: deployed | up to date | blocked
- upstream: <n> commits (<old>..<new>)
- overlays: <n> clean; resolved: <names>; fixed: <names>; absorbed: <names>
- accepted failures: <step: reason> | none
- deploy: waiting for T3 to go idle; the next run confirms it[; held by "<thread>", waiting on a question since <time>]
- report: ~/.t3/code/upgrades/<run>/report.md
```
