#!/usr/bin/env bash
# t3 code self-updating daemon.
#
# Supervises the t3 server from the tiered runtime at ~/.t3/code:
#   - restarts on crash (exponential backoff, then tier fallback)
#   - detects a different build in dist/current (swap and swap-back both
#     change bin.mjs mtime) and restarts onto it, but ONLY when no thread
#     session is running
#     (idle gate: 3 consecutive idle checks)
#   - falls back current -> bak -> stable if a build won't boot, skipping
#     pre-v2 builds once the data has moved to statev2.sqlite
#   - re-execs itself when this script changes (adopting the live server)
#
# Idle probe reads the live server's SQLite database directly (read-only,
# WAL). Orchestration v2 servers use userdata/statev2.sqlite and persist
# runs, pending requests, background tasks and subagents there, so
# scripts/t3-busy-v2.sql covers everything. On its first boot v2 copies the
# v1 state.sqlite and never writes it again, so the database is picked by
# which file the running server has open: a v1 server (before the cutover,
# or after a deliberate swap-back) still gets the v1 probe, which also asks
# that build's loopback-only /api/daemon/background-work.
#
# Started via: mise run daemon  (nohup + disown, survives everything)

set -euo pipefail

# ── Configuration ──────────────────────────────────────────────────────────
RUNTIME_DIR="${T3_RUNTIME_DIR:-$HOME/.t3/code}"
DIST="$RUNTIME_DIR/dist"
DB_V1="$RUNTIME_DIR/userdata/state.sqlite"
DB_V2="$RUNTIME_DIR/userdata/statev2.sqlite"
PID_FILE="$RUNTIME_DIR/daemon.pid"
LOG_FILE="$RUNTIME_DIR/daemon.log"
SERVER_LOG="$RUNTIME_DIR/daemon.log.server"
PORT="${T3_PORT:-3773}"
# 0.0.0.0 (not a literal LAN IP): follows DHCP interface changes, and matches
# opencode's *:80 posture. Override with T3_HOST if you want a narrower bind.
HOST="${T3_HOST:-0.0.0.0}"
# Paired session lifetime in days (consumed by the server at session-issue
# time; unset/invalid inside the server falls back to its 30-day default).
export T3CODE_SESSION_TTL="${T3CODE_SESSION_TTL:-180}"
HEALTH_URL="http://127.0.0.1:$PORT/"
BACKGROUND_WORK_URL="http://127.0.0.1:$PORT/api/daemon/background-work"
SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
BUSY_SQL_V2="$(dirname "$SELF")/t3-busy-v2.sql"

# Tuning
CRASH_BACKOFF=3
MAX_CRASH_RETRIES=3
BIN_CHECK_INTERVAL=30
IDLE_THRESHOLD=3 # consecutive idle checks before restart (~90s)
# The first v2 boot copies state.sqlite and migrates it before the listener
# comes up, so allow minutes while the process is alive; a crash fails fast.
HEALTH_TIMEOUT="${T3_HEALTH_TIMEOUT:-300}"

# ── State ──────────────────────────────────────────────────────────────────
server_pid=""
last_binary_mtime=0
consecutive_idle=0
busy_reason=""
crash_count=0
# A self-re-exec passes the baseline of the server it leaves running.
handoff_baseline=""
if [[ "${1:-}" == "--binary-baseline" && "${2:-}" =~ ^[0-9]+$ ]]; then
	handoff_baseline="$2"
fi

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*" >&2; }

# ── Server management ──────────────────────────────────────────────────────
start_server() {
	local tier="$1"
	# binary_changed compares against the build the server was last started
	# from. Starting a fallback tier leaves it on the failed current build,
	# so that build is not retried until current changes again.
	if [[ "$tier" == "current" ]]; then
		last_binary_mtime=$(current_binary_mtime)
	fi
	T3CODE_HOME="$RUNTIME_DIR" \
		"$NODE_BIN" "$DIST/$tier/bin.mjs" --port "$PORT" --host "$HOST" --no-browser \
		>"$SERVER_LOG" 2>&1 &
	server_pid=$!
	log "Started server from tier '$tier' (PID $server_pid) on $HOST:$PORT"
}

stop_server() {
	if [[ -n "${server_pid:-}" ]] && kill -0 "$server_pid" 2>/dev/null; then
		log "Stopping server (PID $server_pid)..."
		kill "$server_pid" 2>/dev/null || true
		# v2 shutdown releases the managed tunnel (up to 10s) and closes
		# provider sessions; a kill -9 skips its restart bookkeeping.
		local i=0
		while kill -0 "$server_pid" 2>/dev/null && [[ $i -lt 150 ]]; do
			sleep 0.2
			i=$((i + 1))
		done
		if kill -0 "$server_pid" 2>/dev/null; then
			log "Force killing server (PID $server_pid)..."
			kill -9 "$server_pid" 2>/dev/null || true
			wait "$server_pid" 2>/dev/null || true
		fi
		log "Server stopped"
	fi
	server_pid=""
}

# Adopt an already-running server on OUR port (left by a previous daemon
# instance during self-re-exec, or started manually). Port-scoped via lsof
# so we never adopt or kill a server on a different port.
adopt_server() {
	local pid
	pid=$(lsof -tiTCP:"$PORT" -sTCP:LISTEN 2>/dev/null | head -1 || true)
	if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
		server_pid="$pid"
		return 0
	fi
	return 1
}

# Epoch seconds when a PID started, or nothing when ps/date can't tell.
process_start_epoch() {
	local started
	started=$(LC_ALL=C ps -o lstart= -p "$1" 2>/dev/null || true)
	read -r started <<<"$started" # ps pads lstart with trailing spaces
	[[ -n "$started" ]] || return 0
	LC_ALL=C date -j -f '%a %b %d %T %Y' "$started" +%s 2>/dev/null || true
}

# ── Health & idle checks ───────────────────────────────────────────────────
health_check() {
	local started=$SECONDS
	while ((SECONDS - started < HEALTH_TIMEOUT)); do
		# --max-time: the server can bind the port before it serves; without
		# a timeout curl blocks forever on the accepted-but-stalled socket.
		if curl -sf --max-time 3 "$HEALTH_URL" -o /dev/null 2>/dev/null; then
			return 0
		fi
		kill -0 "$server_pid" 2>/dev/null || return 1
		sleep 2
	done
	return 1
}

# v1 builds never open statev2.sqlite, and v2 stops writing state.sqlite
# after its first boot, so the live server's open files name its database.
server_db() {
	if [[ -n "${server_pid:-}" ]] && lsof -p "$server_pid" -Fn 2>/dev/null | grep -q '/statev2\.sqlite$'; then
		echo "$DB_V2"
	else
		echo "$DB_V1"
	fi
}

is_idle() {
	local busy
	if [[ "$(server_db)" == "$DB_V2" ]]; then
		# Fail closed: a broken probe must not restart over live work.
		if ! busy=$(sqlite3 -readonly -cmd '.timeout 2000' "$DB_V2" <"$BUSY_SQL_V2" 2>&1); then
			busy_reason="busy probe failed: ${busy:0:160}"
			return 1
		fi
		if [[ -n "$busy" ]]; then
			busy_reason="threads busy ($busy)"
			return 1
		fi
		return 0
	fi
	# v1: any thread session in starting/running state means a provider
	# session is active. A missing/unreadable DB counts as idle (server may
	# be mid-migration).
	busy=$(sqlite3 "$DB_V1" \
		"SELECT COUNT(*) FROM projection_thread_sessions WHERE status IN ('starting','running');" \
		2>/dev/null || echo 0)
	if [[ "$busy" != "0" ]]; then
		busy_reason="thread session active"
		return 1
	fi
	# A build without the endpoint, or a server still starting, gives no
	# count: fall back to the database check alone.
	local threads
	threads=$(curl -sf --max-time 3 "$BACKGROUND_WORK_URL" 2>/dev/null |
		sed -nE 's/.*"backgroundWorkThreads":([0-9]+).*/\1/p' || true)
	if [[ -n "$threads" && "$threads" != "0" ]]; then
		busy_reason="$threads thread(s) with background work"
		return 1
	fi
	return 0
}

current_binary_mtime() { stat -f %m "$DIST/current/bin.mjs" 2>/dev/null || echo 0; }

# A pre-v2 build reads state.sqlite, which is frozen at the v2 cutover.
tier_is_v1() { ! grep -qs 'statev2.sqlite' "$DIST/$1"/*.mjs; }

# Any change counts, not just a newer mtime: swap-back moves the older bak
# build into current. A missing bin.mjs (mid-swap) is not a build.
binary_changed() {
	local mtime
	mtime=$(current_binary_mtime)
	[[ "$mtime" -gt 0 && "$mtime" -ne "$last_binary_mtime" ]]
}

# ── Fallback chain ─────────────────────────────────────────────────────────
try_tier() {
	local tier="$1"
	log "Trying tier '$tier'..."
	start_server "$tier"
	if health_check; then
		log "tier '$tier' is healthy"
		return 0
	fi
	log "tier '$tier' failed health check"
	stop_server
	return 1
}

fallback() {
	log "Initiating fallback chain..."
	stop_server
	local tier
	for tier in bak stable; do
		if [[ -f "$DIST/$tier/bin.mjs" ]]; then
			# Falling back to a v1 build would silently serve pre-upgrade data.
			if [[ -f "$DB_V2" ]] && tier_is_v1 "$tier" && [[ "${T3_ALLOW_V1_FALLBACK:-0}" != 1 ]]; then
				log "Skipping tier '$tier': pre-v2 build, data lives in statev2.sqlite (T3_ALLOW_V1_FALLBACK=1 overrides)"
				continue
			fi
			if try_tier "$tier"; then
				crash_count=0
				if [[ "$tier" == "stable" ]]; then
					log "WARNING: Running on stable fallback. current and bak builds are broken."
				fi
				return 0
			fi
		else
			log "No tier '$tier' available"
		fi
	done
	log "CRITICAL: All fallbacks exhausted. Daemon giving up."
	log "Fix the build, run 'mise run upgrade' from ~/vendor/t3code, then 'mise run daemon'."
	rm -f "$PID_FILE"
	exit 1
}

# ── Main loop ──────────────────────────────────────────────────────────────
cleanup() {
	log "Daemon shutting down..."
	stop_server
	# Only remove the PID file if it is still ours - a successor daemon
	# (self-re-exec) may have already written its own PID.
	if [[ -f "$PID_FILE" ]] && [[ "$(cat "$PID_FILE" 2>/dev/null || echo)" == "$$" ]]; then
		rm -f "$PID_FILE"
	fi
	exit 0
}
trap cleanup SIGTERM SIGINT

# Check for existing daemon (special-case $$ so a self-re-exec passes).
if [[ -f "$PID_FILE" ]]; then
	existing=$(cat "$PID_FILE" 2>/dev/null || echo "")
	if [[ -n "$existing" ]] && [[ "$existing" != "$$" ]] && kill -0 "$existing" 2>/dev/null; then
		log "Daemon already running (PID $existing). Exiting."
		exit 1
	fi
	log "Stale PID file found, removing"
	rm -f "$PID_FILE"
fi

if [[ ! -f "$DIST/current/bin.mjs" ]]; then
	log "ERROR: no build at $DIST/current/bin.mjs"
	log "Run 'mise run build && mise run swap' from ~/vendor/t3code first."
	exit 1
fi

# The node binary is resolved at generation time (node 24 required).
NODE_BIN="$RUNTIME_DIR/node.bin"
if [[ ! -x "$NODE_BIN" ]]; then
	NODE_BIN="$(cd ~/vendor/t3code 2>/dev/null && mise where node)/bin/node"
fi
if [[ ! -x "$NODE_BIN" ]]; then
	log "ERROR: could not resolve node 24 (tried $RUNTIME_DIR/node.bin and mise)"
	exit 1
fi

echo $$ >"$PID_FILE"
log "Daemon started (PID $$) - $HOST:$PORT, check interval ${BIN_CHECK_INTERVAL}s"

last_self_mtime=$(stat -f %m "$SELF" 2>/dev/null || echo 0)

# Adopt a running server (self-re-exec case) or start fresh.
adopt_stale=0
if adopt_server; then
	log "Found running server on port $PORT (PID $server_pid) - adopting it"
	adopt_stale=1 # verify below whether it matches the current tier's build
	last_binary_mtime=$(current_binary_mtime)
	if [[ -n "$handoff_baseline" ]]; then
		# Self-re-exec: the previous instance knows which build this server
		# runs, so a swap or swap-back that landed meanwhile still restarts,
		# and a server on a fallback tier is not sent back to a failed build.
		last_binary_mtime=$handoff_baseline
	else
		# A server we know nothing about (its daemon died, or a manual start)
		# cannot be running a build newer than itself.
		server_started=$(process_start_epoch "$server_pid")
		if [[ "$server_started" =~ ^[0-9]+$ ]] && [[ "$server_started" -lt "$last_binary_mtime" ]]; then
			last_binary_mtime=$server_started
		fi
	fi
	if binary_changed; then
		log "Adopted server is not on the current build - restart pending (idle-gated)"
	fi
else
	start_server current
fi

if ! health_check; then
	log "Initial health check failed, trying fallback..."
	fallback
elif [[ $adopt_stale -eq 1 ]]; then
	log "Adopted server is healthy; swap (if pending) happens when idle"
else
	log "Server healthy"
fi

while true; do
	# Interruptible sleep: SIGTERM lands in ~0s instead of up to 30s.
	sleep "$BIN_CHECK_INTERVAL" &
	wait $! || true

	# Crash recovery
	if [[ -z "$server_pid" ]] || ! kill -0 "$server_pid" 2>/dev/null; then
		crash_count=$((crash_count + 1))
		if [[ $crash_count -gt $MAX_CRASH_RETRIES ]]; then
			log "Max crash retries ($MAX_CRASH_RETRIES) exceeded"
			fallback
			continue
		fi
		log "Server crashed, restarting in ${CRASH_BACKOFF}s (attempt $crash_count/$MAX_CRASH_RETRIES)"
		sleep "$CRASH_BACKOFF"
		CRASH_BACKOFF=$((CRASH_BACKOFF * 2))
		start_server current
		if health_check; then
			log "Server recovered after crash"
			crash_count=0
			CRASH_BACKOFF=3
		else
			log "Server failed to start after crash"
			stop_server
		fi
		continue
	fi
	crash_count=0
	CRASH_BACKOFF=3

	# Self-update: re-exec when this script changes (server is adopted by
	# the new instance - zero downtime).
	self_mtime=$(stat -f %m "$SELF" 2>/dev/null || echo 0)
	if [[ "$self_mtime" -gt "$last_self_mtime" ]]; then
		if bash -n "$SELF" 2>/dev/null; then
			log "Daemon script changed - re-execing with new code (server stays up)"
			exec bash "$SELF" --binary-baseline "$last_binary_mtime"
		else
			log "Daemon script changed but failed syntax check - keeping current code"
			last_self_mtime=$self_mtime
		fi
	fi

	# New build detected -> idle-gated restart.
	if binary_changed; then
		log "New build detected in current tier, checking for idle..."
		if is_idle; then
			consecutive_idle=$((consecutive_idle + 1))
			if [[ $consecutive_idle -ge $IDLE_THRESHOLD ]]; then
				log "Idle for $consecutive_idle checks - restarting onto new build..."
				stop_server
				start_server current
				if health_check; then
					log "New build healthy"
				else
					log "New build failed health check, initiating fallback..."
					fallback
				fi
				consecutive_idle=0
			else
				log "Idle check $consecutive_idle/$IDLE_THRESHOLD"
			fi
		else
			log "Server busy ($busy_reason), will retry next cycle"
			consecutive_idle=0
		fi
	else
		consecutive_idle=0
	fi
done
