# Guest links

A guest link lets a teammate follow one thread in their browser and ask the agent questions, with no
account. They see your messages and the agent's replies as they stream. Each message they send waits
until you send or dismiss it.

## Share a thread

Guest links are a beta. Turn on **Settings → Labs → Guest links** for the machine that runs the
thread. Guests reach that machine through [T3 Connect](./remote-access.md) (sign in and link it,
and the link points at its T3 Connect address) or through an HTTPS address you run yourself; see
[Without T3 Connect](#without-t3-connect). The machine must stay online while the guest uses it.
While the setting is off, every guest link on that machine stops working; links that have not
expired work again when you turn it back on.

1. Right-click the thread in the sidebar and choose **Share with guest**. It is also in the thread
   menu and the command palette.
2. Copy the link and send it to one person. It works in one browser and expires after 24 hours.
   Opening it again in that browser takes them back to the thread.
3. When they send a message, it appears at the end of the thread and the sidebar shows **Request**.
   Choose **Send** to run it as a normal turn, or **Dismiss**. Files they attached show with the
   message, and **Send** includes them.

Choose **Stop sharing** to end the link at once. Archiving the thread, creating a new link,
unlinking the machine from T3 Connect, or changing or clearing its public link address also ends
it. You can send or dismiss guest messages from the mobile app.

## Without T3 Connect

You can run the public HTTPS address yourself instead of using T3 Connect. Any tunnel or reverse
proxy that forwards HTTPS to this machine's server works; a Cloudflare named tunnel on a domain
you control is a good fit, and the guest stream already sends the heartbeat Cloudflare's edge
needs. T3 does not run or manage the tunnel — it only stores the address.

1. Run your tunnel or reverse proxy to the server's HTTP port, with the public hostname on HTTPS.
2. In **Settings → Labs → Guest links**, set **Public link address** to that HTTPS origin, such
   as `https://t3.example.com`. An address with a path is refused: the guest page and the guest
   API use paths relative to the origin, and the guest cookie is scoped to `/api/guest`.
3. Create guest links as usual. They point at your address. Changing or clearing the address stops
   every link that was made for the old one; setting the same address back lets links that have not
   expired work again. To end a link for good, choose **Stop sharing**.

The proxy must keep paths unchanged (no path-rewriting prefix), pass `Set-Cookie` responses
through, stream `text/event-stream` responses unbuffered, and send `X-Forwarded-Proto: https`.

### What a guest's browser requests

Exactly these paths, nothing else — the guest page builds no app connection, opens no WebSocket,
calls no other `/api/*` route, and loads no script or font from anywhere but `/assets`:

- `/guest` — the guest page, served by the app's SPA fallback.
- `/assets/*` — the app's hashed JavaScript, CSS, and font files.
- `/api/guest/*` — the ten guest routes: `session`, `redeem`, `messages`, `stream` (SSE), `item`,
  `history`, `command-output` (SSE), `render`, `attachment`, `media`. Attachments and HTML renders
  the guest opens come from here too.
- `/favicon.ico`, `/favicon-16x16.png`, `/favicon-32x32.png`, `/apple-touch-icon.png`,
  `/icon-192x192.png`, `/icon-512x512.png`, `/manifest.webmanifest` — icons and the manifest.
  Optional; nothing breaks without them.

With a Cloudflare tunnel, allow exactly those paths in the ingress rules:

```yaml
# ~/.cloudflared/config.yml — guest paths only; every other path answers 404.
ingress:
  - hostname: t3.example.com
    path: ^/guest$
    service: http://localhost:3773
  - hostname: t3.example.com
    path: ^/assets/
    service: http://localhost:3773
  - hostname: t3.example.com
    path: ^/api/guest/
    service: http://localhost:3773
  - hostname: t3.example.com
    path: ^/(favicon\.ico|favicon-16x16\.png|favicon-32x32\.png|apple-touch-icon\.png|icon-192x192\.png|icon-512x512\.png|manifest\.webmanifest)$
    service: http://localhost:3773
  - service: http_status:404
```

Without a path allowlist, the tunnel exposes the whole app, exactly as the T3 Connect tunnel does
today. Every route stays protected by its own sign-in, pairing codes, and scoped sessions, but the
sign-in pages, the environment descriptor, and the MCP OAuth endpoints become publicly reachable.
The allowlist takes those off the public internet.

## What the guest sees

The guest sees the thread's recent history and everything after, in the same timeline you see, as
it streams: messages and their files, the agent's replies with the images they show from your
machine, its reasoning, plans, questions, and its work, with command output when they expand a
command. Their composer shows the thread's model, but they cannot change it. They do not see diffs, and they cannot act on the thread: no forking,
reverting, opening files on your machine, or answering the agent for you.

Anything the agent read or printed can show, such as a value from a file. Share only threads you
are comfortable showing.

## Guest attachments

Guests can attach images and other files to a message: use the paperclip, paste, or drop them on
the message box. A message holds up to 4 files and 10 MB in total. Images must be PNG, JPEG, GIF, or
WebP. While messages wait for you, their files can total 20 MB; the guest can attach more once you
send or dismiss some.

Dismissing a message, ending the link, or archiving the thread deletes the files of messages that
still wait. A sent message keeps its files, like any message in the thread.

The guest's text and files run with the thread's current permission mode once you send them. Check
them before you choose **Send**, as you would anything pasted from a chat.
