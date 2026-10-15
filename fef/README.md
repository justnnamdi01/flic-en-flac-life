# Flic en Flac Life — self-hosted server

This runs **Flic en Flac Life** on your own domain for any player, with no claude.ai account needed.
One small Node process does everything: it serves the game **and** runs the realtime backend
(the live room + the players/mail database) over the same port.

```
fef/
├─ server.js              the whole backend (Node + ws)
├─ package.json
├─ Dockerfile
├─ data.json             created on first run — your saved worlds (back this up)
└─ public/
   ├─ index.html         the game
   └─ net-adapter.js     connects the game to this server (loaded before the game)
```

## How it fits together

The game was written to ask its host for three things. This server provides all three so the
game behaves exactly as it does on claude.ai:

| The game asks for… | This server gives it… |
| --- | --- |
| `claude.use("user")` | a server-issued player id and signed key (stored per browser), plus optional logins |
| `claude.use("room")` | live presence — everyone's position, chat and actions, 8× a second |
| `claude.use("db")`   | two collections: `players` (saves + leaderboard) and `mail` (money transfers) |

`public/net-adapter.js` implements that interface over a WebSocket and is loaded **before** the
game, so nothing in the game itself had to change.

---

## 1. Run it locally (2 minutes)

You need Node 18+.

```bash
cd fef
npm install
npm start
```

Open **http://localhost:8080**. Open it in a second browser window too — you'll see the other
player walking around. Progress is saved to `data.json`.

---

## 2. Put it on the internet

You need a server (any cheap VPS — DigitalOcean, Hetzner, AWS Lightsail, etc.) and your domain
pointing at it. The one thing that matters: on an **https** page the browser only allows a
**secure** WebSocket (`wss://`), so you must serve the site over HTTPS. The easiest way is to put
**Caddy** in front — it gets and renews the TLS certificate automatically.

### Option A — Caddy reverse proxy (recommended)

Run the app (it listens on `localhost:8080`):

```bash
cd fef && npm install && PORT=8080 node server.js
```

Install Caddy, then create a `Caddyfile`:

```
flicenflaclife.com {
    reverse_proxy localhost:8080
}
```

```bash
caddy run
```

That's it — Caddy terminates TLS and transparently upgrades the WebSocket. Visit
`https://flicenflaclife.com`. (Use your real domain; point its DNS `A` record at the server's IP
first. Cloudflare in front is fine — turn on "WebSockets" in its Network settings, which is on by
default.)

To keep it running after you log out, use a process manager:

```bash
npm install -g pm2
pm2 start server.js --name fef
pm2 save && pm2 startup
```

### Option B — Docker

```bash
cd fef
docker build -t fef .
docker run -d --restart unless-stopped -p 8080:8080 -v fef-data:/data --name fef fef
```

Then put Caddy (or your existing reverse proxy) in front for HTTPS exactly as above. The `-v
fef-data:/data` mount keeps saves across restarts.

### Nginx instead of Caddy

If you already run Nginx, the only extra bit is the WebSocket upgrade headers:

```nginx
location / {
    proxy_pass http://localhost:8080;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
}
```

Use Certbot for the certificate.

---

## 3. Updating the game

When you change the game, replace **`public/index.html`** and restart (or redeploy). To rebuild it
from the artifact's HTML, wrap that HTML in a normal page and load the adapter first — the top of
the current `index.html` shows the exact wrapper to keep (the `<meta>` tags, the small `<style>`,
and `<script src="net-adapter.js"></script>` **before** the game's own markup).

---

## 4. Your data

- Everything lives in **`data.json`** (or the `/data` volume in Docker): every player's save,
  balance and the leaderboard.
- **Back it up.** A simple cron copy is enough to start:
  `0 * * * * cp /path/to/fef/data.json /path/to/backups/data-$(date +\%H).json`
- Restore by stopping the server, replacing `data.json`, and starting again.

---

## 5. Before you open it to strangers — read this

This MVP trusts the client, which is fine for friends but not for a public launch. The important
gaps (see your launch checklist for the full list):

1. **Player identity.** The server gives every browser an id plus a signed key, and only accepts
   writes from the holder of that key, so saves and money can't be hijacked. Players can add an
   optional username + password (Phone → Settings → Login) to play the same character on another
   device. What to do for launch:
   - Set **`AUTH_SECRET`** to a long random string in your host's environment variables. Without
     it the server writes `auth-secret.txt` next to `data.json`. Keep it on the same persistent
     disk: if the secret is lost, every player's key stops working.
   - Browsers from before keys existed may claim their old id once. When your players have moved
     over, set **`ALLOW_LEGACY_IDS=0`**.
   - Never deploy or share your local `data.json` / `auth-secret.txt` (`.dockerignore` excludes
     them from Docker images).
2. **Money is client-authoritative.** The game tells the server its balance; a determined player
   could edit it. For a real economy, validate money, purchases and transfers on the server.
3. **No chat moderation or admin tools yet.** Public chat needs a filter, report/block, and a way
   for you to mute/ban. Add this before going wide.
4. **Keep the casino virtual** (in-game Rupees only) and gate the game behind an **18+ notice** —
   it has gambling, drinking, zaza, theft and cheating as fictional mechanics.
5. **Legal:** add a privacy policy, terms, and a contact for abuse reports. You're in Mauritius, so
   the Data Protection Act 2017 applies; GDPR too if you get EU players.

## 6. Scaling note

One process keeps all state in memory and broadcasts the room to every tab, so it's great up to a
few hundred concurrent players on a small VPS. Beyond that, move `data.json` to Postgres or Redis,
run several instances behind a load balancer with sticky sessions, and shard players into separate
worlds/rooms.
