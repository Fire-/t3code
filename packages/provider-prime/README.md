# Prime provider

Prime is a fork of the Pi integration, but every `prime-agent --mode rpc` process, including
`--no-session` probes and text generation, is a thin client of Prime's default daemon: the same
daemon the user's own Prime terminal sessions use. T3 never picks a daemon socket and never stops
the daemon. It ends a session with SIGTERM, which completes the client-owned worker; a SIGKILL
before that leaves the worker in Prime's 30 s disconnect grace, and resuming the session fails
until the grace ends. Prime cancels a session's schedules when its worker stops, and RPC mode cannot
reattach to a running worker, so agent-created schedules live only as long as T3 holds the process.
Active ones pin the idle release rather than registering pending background tasks, so they never
block a restart. Recordings and probes must use a private `--daemon-socket` and stop it with that
socket's own shutdown message; `prime-agent shutdown` stops every daemon, including the user's.
See the [Prime adapter](./src/server/adapter.ts).

Prime has no `agent_settled`, and its `agent_end` does not mean idle: a goal, autonomous run,
queued follow-up, compaction, or pending auto-retry keeps the session working after it, and
command-only turns such as `/goal status` never emit one. A Prime turn therefore settles only after
`agent_end` or a finished session action, followed by one `get_state` showing no streaming,
compaction, active session action, or queued work. Prime answers idle during an auto-retry delay,
so a pending retry also holds the turn. The probe runs only on those transitions; polling it would
put a request on every idle Prime session. Work Prime starts after a turn settles becomes a
continuation turn.
