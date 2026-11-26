# Guest links

Share a single thread with a teammate in their browser—no account required. Guests see messages and agent activity stream live, and any message they send waits for your approval before running.

## Prerequisites and sharing

- Enable **Settings → Labs → Guest links** on the host machine (turning it off pauses all active guest links on that machine).
- Link the host to [T3 Connect](./remote-access.md) and keep it online while the guest uses the link, or serve it through an HTTPS address you run yourself (see [Without T3 Connect](#without-t3-connect)).

1. Open the thread's menu (or command palette / mobile long-press) and choose **Share with guest**.
2. Send the copied link to one person (works in one browser and expires after 24 hours).
3. Guest messages appear at the end of the thread (**Request** in the sidebar). Choose **Send** to run as a normal turn (in the thread's current permission mode—review text and files first) or **Dismiss**.

Choose **Stop sharing** anytime to revoke immediately. Archiving the thread, generating a new link, unlinking T3 Connect, or changing or clearing the public link address also ends the link. The mobile app can send, dismiss, and stop sharing too.

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

## What guests can see and attach

- **Visible**: Thread messages, files, agent replies, images, reasoning, plans, questions, and command output. Anything the agent printed is visible—only share threads you're comfortable exposing.
- **Restricted**: No diffs, file browser, model switching, forking, reverting, or answering approvals/questions.
- **Attachments**: Up to 4 files (10 MB total) per message (PNG, JPEG, GIF, WebP images + other files), up to 20 MB total across pending unapproved messages. Dismissing or ending the link deletes pending files.
