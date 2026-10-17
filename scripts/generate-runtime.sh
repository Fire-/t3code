#!/usr/bin/env bash
# Generator for the t3 code runtime environment outside the vendor dir.
# Creates ~/.t3/code/ with tiered dist/ (current/bak/stable) and the t3
# runner shim.
#
# Architecture:
#   ~/.t3/code/bin/t3              <- runner (on PATH via ~/.zshrc block)
#   ~/.t3/code/dist/current/       <- active build (bin.mjs + chunks + client/)
#     node_modules/                <- the 5 native deps (see runtime-natives.sh)
#   ~/.t3/code/dist/bak/           <- previous build (shifted on swap)
#   ~/.t3/code/dist/stable/        <- pinned known-good (setup-stable)
#   ~/.t3/code/userdata/           <- server data (T3CODE_HOME points here)
#
# Idempotent: safe to re-run; regenerates bin/t3 and AGENTS.md, populates
# current only if empty.
#
# Usage: bash scripts/generate-runtime.sh

set -euo pipefail

VENDOR_DIR="${VENDOR_DIR:-$HOME/vendor/t3code}"
RUNTIME_DIR="${RUNTIME_DIR:-$HOME/.t3/code}"
BIN_DIR="$RUNTIME_DIR/bin"
DIST_DIR="$RUNTIME_DIR/dist"
VENDOR_DIST="$VENDOR_DIR/apps/server/dist"

echo "=== t3 code Runtime Generator ==="
echo "Vendor:  $VENDOR_DIR"
echo "Runtime: $RUNTIME_DIR"
echo ""

if [[ ! -f "$VENDOR_DIST/bin.mjs" ]]; then
	echo "ERROR: server bundle not found at $VENDOR_DIST/bin.mjs"
	echo "Run 'mise run build' first."
	exit 1
fi

mkdir -p "$BIN_DIR" "$DIST_DIR"/{current,bak,stable}

# --- Resolve the mise-managed node 24 binary (t3 requires node:sqlite) ---
NODE_BIN="$(cd "$VENDOR_DIR" && mise where node)/bin/node"
if [[ ! -x "$NODE_BIN" ]]; then
	echo "ERROR: could not resolve node via mise in $VENDOR_DIR" >&2
	exit 1
fi
echo "Node:     $NODE_BIN ($("$NODE_BIN" --version))"

# Stable symlink so the daemon and shim never depend on mise at runtime.
ln -sf "$NODE_BIN" "$RUNTIME_DIR/node.bin"

# --- Generate runner script ---
cat >"$BIN_DIR/t3" <<RUNNER_EOF
#!/usr/bin/env bash
# t3 code runtime runner
# Tries dist tiers: current -> bak -> stable
# Each tier is self-contained (bin.mjs + native node_modules).

set -euo pipefail

RUNTIME_DIR="\${T3_RUNTIME_DIR:-\$HOME/.t3/code}"
DIST_DIR="\$RUNTIME_DIR/dist"
export T3CODE_HOME="\$RUNTIME_DIR"

tier=""
for t in current bak stable; do
	if [[ -f "\$DIST_DIR/\$t/bin.mjs" ]]; then
		tier="\$t"
		break
	fi
done

if [[ -z "\$tier" ]]; then
	echo "ERROR: no runtime tier found in \$DIST_DIR" >&2
	echo "Run 'mise run build && mise run swap' from ${VENDOR_DIR}" >&2
	exit 1
fi

exec "$NODE_BIN" "\$DIST_DIR/\$tier/bin.mjs" "\$@"
RUNNER_EOF
chmod +x "$BIN_DIR/t3"
echo "Generated: $BIN_DIR/t3"

# --- Generate runtime AGENTS.md ---
cat >"$RUNTIME_DIR/AGENTS.md" <<AGENTS_EOF
# t3 code Runtime

Self-contained runtime for the t3 code server. Separate from the vendored
source directory ($VENDOR_DIR) and survives even if that directory is
deleted.

## Structure

\`\`\`
~/.t3/code/
  bin/t3              <- Runner on PATH (tier walk: current -> bak -> stable)
  dist/current/       <- Active build (bin.mjs + chunks + client/ + natives)
  dist/bak/           <- Previous build (auto-created on swap)
  dist/stable/        <- Pinned known-good (manual via 'mise run setup-stable')
  userdata/           <- Server data (T3CODE_HOME; state.sqlite lives in userdata/state.sqlite)
\`\`\`

The runner exports T3CODE_HOME=~/.t3/code, so every t3 subcommand
(pair, project, auth, service) reads and writes the same data.

## Tier Fallback

\`bin/t3\` tries tiers in order:
1. \`current/\` — active build
2. \`bak/\` — previous build (auto-created on swap)
3. \`stable/\` — pinned known-good (manual via \`mise run setup-stable\`)

If a tier's bin.mjs is missing the runner falls through to the next.

## Recovery

If a build breaks and the server won't start:

\`\`\`bash
# Option 1: rollback to previous build
cd ~/vendor/t3code && mise run swap-back

# Option 2: promote the pinned known-good tier
rm -rf ~/.t3/code/dist/current
cp -R ~/.t3/code/dist/stable ~/.t3/code/dist/current

# Option 3: rebuild and re-swap
cd ~/vendor/t3code && mise run build && mise run smoke-test && mise run swap
\`\`\`

## Serving

\`\`\`bash
t3 --port 8100 --host 0.0.0.0 --no-browser   # LAN/WireGuard
t3 pair                                      # mint pairing token for the phone
t3 service --help                            # background service management
\`\`\`

Port 80 belongs to opencode — never bind it here. The daemon binds 0.0.0.0:3773
(all interfaces, same posture as opencode on :80, survives DHCP changes); narrow
it with T3_HOST=127.0.0.1 for loopback-only, or use --tailscale-serve.

## Daemon

The server is normally supervised by the self-updating daemon (do NOT also
run \`t3 service install\` — two supervisors would fight):

\`\`\`bash
cd ~/vendor/t3code && mise run daemon
\`\`\`

It restarts the server on crash, picks up new builds promoted by
\`mise run swap\` when no thread session is running (idle gate:
3 consecutive checks ~90s apart), and falls back current -> bak -> stable
if a build will not boot. It re-execs itself when its script changes.
Manage with: \`mise run daemon-status\`, \`daemon-stop\`, \`daemon-log\`,
\`server-log\`, \`logs\`. Logs live in this directory: daemon.log (daemon,
local time) and daemon.log.server (server, UTC).

## Updating

\`\`\`bash
cd ~/vendor/t3code
mise run upgrade   # sync upstream, rebuild main, build, smoke-test, swap
\`\`\`

## Node

The runner execs a baked absolute node path ($NODE_BIN, node 24 — the
server needs node:sqlite). Re-run \`mise run generate-runtime\` after
changing the mise toolchain.
AGENTS_EOF
echo "Generated: $RUNTIME_DIR/AGENTS.md"

# --- Populate current tier if empty ---
if [[ ! -f "$DIST_DIR/current/bin.mjs" ]]; then
	echo "Populating current tier from vendor build..."
	mkdir -p "$DIST_DIR/current"
	cp -R "$VENDOR_DIST/." "$DIST_DIR/current/"
	node "$VENDOR_DIR/scripts/runtime-natives.mjs" "$DIST_DIR/current/node_modules"
fi

echo ""
echo "=== Done ==="
echo "PATH block (add to ~/.zshrc if not present):"
echo "  export PATH=\"$BIN_DIR:\$PATH\""
echo ""
echo "Tiers:"
for t in current bak stable; do
	if [[ -f "$DIST_DIR/$t/bin.mjs" ]]; then
		echo "  $t: $(du -sh "$DIST_DIR/$t" | cut -f1) ok"
	else
		echo "  $t: empty"
	fi
done
