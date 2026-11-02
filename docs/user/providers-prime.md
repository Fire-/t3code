# Prime

T3 Code can use your existing Prime coding agent installation while keeping Prime's models,
sign-in, extensions, skills, and native session history.

## Set Up Prime

1. Install Prime on the machine running the T3 Code server. T3 Code supports Prime 0.9.8 or newer.

   ```sh
   curl -fsSL https://app.primeintellect.ai/prime-agent/install.sh | sh
   ```

2. Sign in. Run `prime-agent` in a terminal and use `/login`, or set `PRIME_API_KEY` in the Prime
   instance's environment variables in T3 Code and mark it as sensitive.
3. Open T3 Code Settings, enable Prime, and refresh the provider.

If `prime-agent` is not on the server's `PATH`, set Prime's binary path to the full path of the
executable. The installer places it in `.prime/agent/bin` in your home directory.

Launch arguments are available for options such as `--model`, `--thinking`, `--no-skills`, or an
extra `-e` extension. `--provider` must be paired with `--model`. T3 Code rejects launch arguments
that change Prime's execution mode, select a session, set the working directory, or choose a daemon
socket, because T3 owns those parts of the process lifecycle.

On a remote or headless server, connect to that machine over SSH, run `prime-agent`, and sign in
with `/login`, or set `PRIME_API_KEY` in the instance's settings.

T3 Code runs Prime through the same background daemon that Prime uses in your terminal. Avoid
`prime-agent shutdown` while T3 Code threads are working, because it stops every Prime session,
including T3 Code's. Update Prime with `prime-agent update`.

## What Carries Over

T3 Code discovers the models Prime reports, named by model provider and model, such as
`mistral/zai-glm-5-3`, and exposes each model's supported thinking levels. The thinking picker
marks the active model's configured level as its default. **Prime default** uses whichever model
Prime is configured to use. Generated titles, commit messages, pull request text, and branch names
use the selected Prime model and its thinking level when the model supports that level.

Prime's `/compact`, `/refine`, `/goal`, `/autonomous`, and `/reload` appear in the composer's slash
menu with the commands that Prime extensions add. Prime skills appear in the `$` menu. A goal or
autonomous run continues as a series of turns in the same thread, each with its own checkpoint, and
a message you send during a goal steers the running work. Child agents that Prime starts appear in
the subagent UI with their progress; they cannot be opened as T3 Code threads.

Prime loads its normal user and project extensions. Blocking `select`, `confirm`, `input`, and
`editor` dialogs appear in the T3 Code composer.

The agent can create schedules and heartbeats. While T3 Code keeps the thread's Prime session open,
each one that fires continues the thread as a new turn, and an active schedule keeps an idle session
open for up to four hours. Prime ends a session's schedules when the session stops. They therefore
end when T3 Code restarts, when it releases an idle session, and when you change the permission
mode. Resuming the thread does not bring them back, so ask Prime to create them again.

Threads use Prime's native session files for resume, rollback, and forks within the same Prime
instance. Switching providers uses portable conversation context. Turns that start with your
message roll back exactly, and so do command-only turns such as `/goal status`. Prime cannot roll
back or fork past a turn it started by itself without a user message. When the first turn a
rollback would discard, or the first turn after a fork point, began with something other than a
user message, T3 Code shows an error and leaves the thread unchanged. That covers a heartbeat
continuation, a `/goal <objective>` run, turning autonomous mode on or off, a Prime notice, or agent
output Prime started by itself. To undo such a turn, roll back to an earlier turn that began with
your message.

Prime cannot write review guides. Its built-in Python tool can change files, so T3 Code cannot keep
the investigation read-only. Choose another provider's model as the review guide model in
**Settings → Source Control**.

## Permission Modes

T3 Code applies the composer permission mode through a blocking tool hook it adds to Prime. Prime's
built-in tool runs Python cells, and one cell can both run commands and change files, so each cell
is treated as a command. Its approval prompt shows the code Prime wants to run.

- **Supervised** asks before every Python cell, file change, and extension tool. Read-only tools
  continue.
- **Auto-accept edits** allows dedicated edit and write tools, which Prime does not include by
  default, but still asks before every Python cell and extension tool.
- **Full access** allows tools without T3 Code approval prompts.

The **Auto** option is not shown for Prime. Threads that already stored Auto behave as Supervised.

Changing the mode restarts the Prime provider session and resumes the same native conversation. The
policy covers Prime tool calls. It is not an operating-system sandbox, and it does not cover code
that a Prime extension runs outside a tool call.

## Troubleshooting

- If Prime is unavailable, confirm that the configured binary runs on the server machine and that
  `prime-agent --version` succeeds, then refresh the provider in Settings.
- If Prime reports that it is not signed in, run `prime-agent` and use `/login`, or set
  `PRIME_API_KEY`. An API key for a model provider alone does not count as a Prime sign-in.
- If no models appear, open Prime directly and confirm its model configuration. When discovery
  cannot complete, T3 Code keeps the models it last found and the **Prime default** model.
- If Prime was stopped abruptly, it can keep the thread's session open for about 30 seconds. T3 Code
  waits for it. If the session stays open longer, T3 Code starts a new Prime session and carries the
  conversation over as context.
- If a schedule stopped firing, T3 Code or the thread's Prime session probably restarted. Ask Prime
  to create the schedule again.
- If a project extension or skill is missing, confirm that Prime loads it when you run
  `prime-agent` in that project, then refresh the provider or start a fresh provider session.
