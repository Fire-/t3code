# Droid provider

Droid runs the user's own `droid` install against the user's `~/.factory`, which T3 never writes.
Droid fills any session setting a client leaves out from that directory's `settings.json`, so an
unsent interaction mode or model can come back as the user's spec mode on another model. Every
settings frame therefore carries the interaction mode, autonomy, model, and effort explicitly, and
each spawn gets a temporary `--settings` file that turns the user's hooks off. See the
[session settings](./src/server/sessionSettings.ts).

A Droid thread's `nativeThreadRef` is the live session id. Rewind and fork create a new session. A
compaction keeps its session unless Droid returns a different id, which the adapter then follows.
The adapter hands the new ref back (an event, or the thread it returns from
rollback and fork), and the orchestrator persists it and resumes that id after a restart. The ref
only moves forward to a session the adapter has loaded. A spec handoff also creates a session, but
it starts empty, so the adapter logs it and keeps the thread on its own session. See the
[adapter](./src/server/adapter.ts).

Droid review guides are unsupported on purpose. `droid exec --only-tools` still offers
`ExitSpecMode` when the user's settings start sessions in spec mode, and calling it wrote a spec
file into the checkout. Droid also starts MCP servers that the checkout declares, and no `exec`
flag turns that off. See [text generation](./src/server/textGeneration.ts).
