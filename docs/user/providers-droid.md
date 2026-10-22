# Droid

T3 Code can use your existing [Droid](https://factory.ai) CLI installation while keeping Droid's
models, Factory sign-in, skills, MCP servers, and native session history.

## Set up Droid

1. Install the [Droid CLI](https://docs.factory.ai/cli/getting-started/quickstart) on the machine
   running the T3 Code server. T3 Code supports Droid 0.244.0 or newer.

   ```sh
   curl -fsSL https://app.factory.ai/cli | sh
   ```

2. Sign in. Run `droid` in a terminal and follow the browser sign-in, or set `FACTORY_API_KEY`
   to a key from your [Factory API key settings](https://app.factory.ai/settings/api-keys). Set it
   in the server's environment, or in the Droid instance's environment variables in T3 Code and
   mark it as sensitive.
3. Open **Settings → Providers**, select the environment, and enable **Droid**. Droid is off until
   you enable it. Then refresh the provider.

If `droid` is not on the server's `PATH`, set Droid's binary path to the full path of the
executable. On a remote or headless server, connect to that machine over SSH and run `droid` there,
or use `FACTORY_API_KEY`.

T3 Code cannot check Droid's sign-in ahead of time. If a message fails to authenticate, the
provider card asks you to sign in. Sign in as above, then send the message again.

Droid updates itself, so T3 Code does not install or update it. Run `droid update` to update it
on demand.

## Models and reasoning

T3 Code lists the models Droid reports, grouped by model provider, and exposes each model's
reasoning levels. The reasoning picker marks Droid's default for the model, and that default is what
T3 Code uses. Droid's `auto` router picks a model for each request and has no reasoning level. New
threads start on Claude Opus 5.5, so choose a cheaper model when you want to spend fewer credits.

The number after a model's name, such as `Opus 5.5 (1.6x)`, is Factory's credit multiplier. It shows
how fast the model uses your Factory plan compared with a 1x model, so a 2x model uses it about
twice as fast and a 0.04x model uses very little. A label such as `50% off` shows a current
promotion. The router has no multiplier because it bills at the model it picks.

You can switch models between messages in one thread. If Droid answers with a different model than
the one you chose, the thread shows a notice. To use a model that Droid accepts but does not list,
add it under **Custom models** in the Droid instance settings.

T3 Code sends the model, reasoning level, permission mode, and plan or build mode with every
thread. The defaults in your own Droid settings do not apply to T3 Code threads.

## Permission modes

T3 Code maps each [permission mode](./permission-modes.md) to a Droid autonomy level. Droid enforces
the level itself, and a change applies from your next message.

| Mode                  | Droid autonomy | Droid does without asking                                       |
| --------------------- | -------------- | --------------------------------------------------------------- |
| **Supervised**        | Off            | Read-only actions. It asks before edits and commands.           |
| **Auto-accept edits** | Low            | File edits and safe, non-destructive commands.                  |
| **Auto**              | Medium         | The above, plus installs, builds, tests, and local git commits. |
| **Full access**       | High           | The above, plus pushes, deployments, and long-running work.     |

Approval prompts offer the choices Droid offers. **Auto** is Droid's Medium level, not an AI
reviewer. Droid's own safety checks still apply in **Full access**.

## Plan mode

Switch the composer to **Plan** or send `/plan` to use Droid's spec mode. Droid researches the task
and can ask you questions, then shows a plan in the thread. It changes no files while planning, and
T3 Code leaves no spec file in your project. Planning runs on the thread's model, not the spec model
from your Droid settings.

Choose **Implement** to start a normal turn with the thread's permission mode.

## Text generation

T3 Code can use Droid for thread titles, commit messages, pull request text, and branch names. The
default model is `claude-haiku-5-5`, which costs very little. T3 Code uses it when you choose Droid
for text generation and when Droid is the only provider you have enabled. If your Factory account
does not include Haiku, these requests fail with Droid's error. Choose another model in
**Settings → General → Text generation**, or in **Settings → Source Control** for source control
text.

Droid cannot write review guides because it cannot investigate a checkout read-only, so choose
another provider's model as the review guide model in **Settings → Source Control**.

## Working with Droid threads

Each thread is a Droid session, so it also appears in Droid's own session list. T3 Code resumes it
after a restart. If Droid cannot resume a session, T3 Code starts a new one and carries the
conversation over as context.

Rolling back a thread and forking it, from the latest turn or an earlier one, use Droid's own
session, so Droid remembers the conversation up to that turn. Send `/compact` or use the context
meter to compact a conversation. Droid cannot stop a compaction partway, so **Stop** during one
restarts Droid and keeps the conversation as it was.

Droid's Task subagents appear in the thread with their progress and results. You cannot open them as
threads. A background subagent ends if Droid stops, such as when T3 Code restarts. When one finishes
while the thread is idle, Droid's follow-up appears as a new turn. Droid's questions appear in the
composer like those of other providers.

Droid loads its skills, custom droids, `AGENTS.md`, and MCP servers as it does in a terminal. They
do not appear in T3 Code's slash or `$` menus.

## Limits

- Droid missions and Droid usage limits are not supported. **Usage** does not include Droid. Check
  your Factory usage in the Factory app or with `/limits` in Droid.
- T3 Code starts Droid with hooks turned off, so hooks from your Droid settings do not run in T3
  Code threads.
- The context meter shows tokens used without a percentage, because Droid does not report a context
  size.

## Troubleshooting

- If Droid is unavailable, confirm that `droid --version` works on the server machine, then refresh
  the provider in Settings.
- If Droid reports that it could not authenticate, run `droid` and sign in, or set
  `FACTORY_API_KEY`. The next completed turn clears the warning.
- If the model list looks incomplete, T3 Code keeps the models it last found. Refresh the provider,
  and confirm that Droid can list models when you run it directly.
- If Droid sends nothing for 10 minutes while no tool is running and no question is waiting, T3 Code
  ends the turn with an error. Send another message to start Droid again.
