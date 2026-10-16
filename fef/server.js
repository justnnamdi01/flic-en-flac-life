// Flic en Flac Life — self-hosted backend (hardened for real players)
// Implements exactly what the game asks the page host for:
//   claude.use("user") -> { id() }        a stable player id
//   claude.use("room") -> presence/peers  live multiplayer
//   claude.use("db")   -> players + mail  saves, leaderboard, money transfers
// One Node process serves the static game AND the realtime backend over the same port.
//
// SAFETY MODEL (read before launch):
//   - A tab can only WRITE its own players/<uid> document, only ADD to `mail`, and only
//     DELETE a `mail` document addressed to itself. It can never overwrite another player.
//   - Every write is rate-limited and size-capped; messages above 64 KB are dropped by ws.
//   - Identity is server-issued: every browser gets a uid plus a signed key (HMAC with a server
//     secret). hello must present a valid key, so nobody can claim someone else's uid.
//     Optional username + password logins (scrypt-hashed) let a player move to another device.
//   - Money is still client-authoritative (the game reports its own balance); see README.

const http = require("http");          // static file server
const fs = require("fs");              // read files and persist the database
const path = require("path");          // safe path joins
const crypto = require("crypto");      // random ids
const { WebSocketServer } = require("ws"); // the realtime transport

const PORT = process.env.PORT || 8080;                 // the port to listen on
const PUBLIC = path.join(__dirname, "public");         // where the game files live
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, "data.json"); // where saves persist
const MIME = { ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".css": "text/css", ".json": "application/json", ".png": "image/png", ".jpg": "image/jpeg", ".svg": "image/svg+xml", ".ico": "image/x-icon", ".glb": "model/gltf-binary",".webmanifest": "application/manifest+json" };

// ---------- safety limits ----------
const MAX_DOC = 32 * 1024;   // biggest player save we accept (bytes of JSON)
const MAX_MAIL = 2 * 1024;   // biggest money-transfer message
const MAX_PRESENCE = 4 * 1024; // biggest presence patch
const MAIL_CAP = 5000;       // keep at most this many mail documents (prune oldest)
const MAX_SUBS = 12;         // subscriptions per tab
const RATE_CAP = 40;         // write-token bucket size
const RATE_REFILL = 4;       // tokens added per second

function sizeOk(data, max) { try { return JSON.stringify(data).length <= max; } catch (e) { return false; } } // reject oversized or circular data
function allow(me) { // token-bucket rate limiter per connection
  const now = Date.now(); me.tokens = Math.min(RATE_CAP, me.tokens + (now - me.last) / 1000 * RATE_REFILL); me.last = now; // refill over time
  if (me.tokens < 1) return false; me.tokens -= 1; return true; // spend one token per write
}
function validMail(d) { // a money transfer must look right before we store it
  return d && typeof d.to === "string" && d.to.length > 0 && d.to.length <= 64 && Number.isFinite(+d.amt) && +d.amt >= 1 && +d.amt <= 10000000; // recipient id + sane amount
}
function cleanMail(d) { return { to: String(d.to).slice(0, 64), from: String(d.from || "Someone").slice(0, 24), amt: Math.floor(+d.amt), at: Date.now() }; } // store only known fields, server timestamp
function pruneMail() { // keep the mail collection from growing without bound
  const ids = Object.keys(db.mail); if (ids.length <= MAIL_CAP) return; // still small enough
  ids.map((id) => [id, (db.mail[id] && db.mail[id].at) || 0]).sort((a, b) => a[1] - b[1]).slice(0, ids.length - MAIL_CAP).forEach(([id]) => delete db.mail[id]); // drop the oldest
}

// ---------- the little Firestore-like database (collections of documents) ----------
const dict = (o) => Object.assign(Object.create(null), o || {}); // a lookup table with no prototype (so "__proto__" is just a key)
let db = { players: {}, mail: {}, homes: dict(), auth: { bound: dict(), accounts: dict(), names: dict() }, invites: dict(), news: [] }; // public collections, plus private login data, home invites and town gossip
try { const loaded = JSON.parse(fs.readFileSync(DATA_FILE, "utf8")); db.players = loaded.players || {}; db.mail = loaded.mail || {}; db.news = Array.isArray(loaded.news) ? loaded.news.slice(-60) : []; db.homes = dict(loaded.homes); const a = loaded.auth || {}; db.auth = { bound: dict(a.bound), accounts: dict(a.accounts), names: dict(a.names) }; db.invites = dict(); for (const k in loaded.invites || {}) db.invites[k] = dict(loaded.invites[k]); } catch (e) {} // load last state (known collections only)
const PUBLIC_COLS = ["players", "mail", "homes"];      // the only collections a tab may read (homes: which player lives at which address)
let saveTimer = null;                                  // debounce disk writes
const BACKUP_DIR = path.join(path.dirname(DATA_FILE), "backups"); // daily copies of everyone's progress, kept for two weeks
function backupDaily() { try { fs.mkdirSync(BACKUP_DIR, { recursive: true }); const f = path.join(BACKUP_DIR, "data-" + new Date().toISOString().slice(0, 10) + ".json"); if (!fs.existsSync(f)) fs.writeFileSync(f, JSON.stringify(db)); const old = fs.readdirSync(BACKUP_DIR).filter((n) => /^data-d{4}-d{2}-d{2}.json$/.test(n)).sort().slice(0, -14); for (const n of old) fs.unlinkSync(path.join(BACKUP_DIR, n)); } catch (e) { console.warn("backup failed", e.message); } }
setTimeout(backupDaily, 60000); setInterval(backupDaily, 3600000); // a minute after start, then hourly (one file per day)
function persist() { clearTimeout(saveTimer); saveTimer = setTimeout(() => { fs.writeFile(DATA_FILE, JSON.stringify(db), () => {}); }, 400); } // write at most ~2x/sec

// ---------- player identity: signed keys, plus optional logins ----------
const SECRET_FILE = process.env.AUTH_SECRET_FILE || path.join(path.dirname(DATA_FILE), "auth-secret.txt"); // kept next to the saves
let SECRET = process.env.AUTH_SECRET || "";            // set AUTH_SECRET in production, or let the server make one
if (!SECRET) { try { SECRET = fs.readFileSync(SECRET_FILE, "utf8").trim(); } catch (e) {} }
if (!SECRET) { SECRET = crypto.randomBytes(32).toString("hex"); try { fs.writeFileSync(SECRET_FILE, SECRET, { mode: 0o600 }); } catch (e) { console.warn("Could not save " + SECRET_FILE + ": every player gets a new identity after a restart. Set AUTH_SECRET.", e.message); } }
const ALLOW_LEGACY = process.env.ALLOW_LEGACY_IDS !== "0"; // let browsers from before keys keep their old id; set ALLOW_LEGACY_IDS=0 once your players have moved over
const UID_RE = /^[a-z0-9]{6,40}$/, NAME_RE = /^[a-z0-9_]{3,20}$/; // what ids and usernames look like
function sign(uid) { return crypto.createHmac("sha256", SECRET).update(uid).digest("base64url"); } // the key that proves you own a uid
function keyOk(uid, key) { if (typeof key !== "string") return false; const a = Buffer.from(sign(uid)), b = Buffer.from(key); return a.length === b.length && crypto.timingSafeEqual(a, b); } // constant-time check
function hashPass(pass, salt) { return new Promise((ok, no) => crypto.scrypt(pass, salt, 32, { N: 16384, r: 8, p: 1 }, (e, k) => (e ? no(e) : ok(k)))); } // slow on purpose
const loginFails = new Map();                          // username -> { n, until }: lock a name for a minute after 5 wrong passwords
function whoIs(m) { // work out a connecting tab's uid from its hello; bound[uid] is 1 once its key is sent, 2 once the browser has used it
  const id = String(m.uid || "");
  if (UID_RE.test(id) && keyOk(id, m.token)) { if (db.auth.bound[id] !== 2) { db.auth.bound[id] = 2; persist(); } return id; } // returning player with a valid key
  if (ALLOW_LEGACY && UID_RE.test(id) && !m.token && db.auth.bound[id] !== 2) { if (!db.auth.bound[id]) { db.auth.bound[id] = 1; persist(); } return id; } // a browser from before keys existed claims its old id (again, if the key never reached it)
  const fresh = crypto.randomBytes(12).toString("hex"); db.auth.bound[fresh] = 2; persist(); return fresh; // no id, a wrong key, or someone else's id: a brand-new player (its key is required from now on)
}
// ---------- homes: one private address per player, entered only by its owner and invited guests ----------
const parties = new Map();                             // owner uid -> time their house party ends (anyone may come in until then)
const homeOf = (uid) => { for (const i in db.homes) if (db.homes[i].uid === uid) return +i; return -1; }; // a player's address number, or -1
function canVisit(owner, guest) { // may this guest be inside this owner's home (or their house party)?
  if (!owner || owner === guest) return true;           // your own place
  if ((parties.get(owner) || 0) > Date.now()) return true; // a party is on: open house
  const inv = db.invites[owner]; return !!(inv && inv[guest] > Date.now()); // an invite that has not run out
}
function roomOwner(p) { // whose private room a presence says it is in ("" for public places)
  const where = typeof p.in === "string" ? p.in : "";
  if (where.startsWith("home:")) return where.slice(5); // someone's home
  if (where === "houseparty") return typeof p.ph === "string" ? p.ph : ""; // someone's house party
  return "";
}
function tellUid(uid, obj) { for (const [, p] of peers) if (p.uid === uid) send(p.ws, obj); } // every open tab of one player
function kickGuest(owner, guest) { // send a guest out of someone's home right away
  for (const [, p] of peers) if (p.uid === guest && roomOwner(p.presence || {}) === owner) { p.presence.in = ""; p.presence.ph = ""; peersDirty = true; send(p.ws, { t: "kicked", owner }); }
}
function homes(ws, me, m) { // claim an address, invite or uninvite a guest, open your house for a party, list your invites
  const reply = (o) => send(ws, Object.assign({ t: "homes", rid: m.rid }, o));
  if (m.t === "home.claim") { // everyone gets the lowest free address, once
    let i = homeOf(me.uid);
    if (i < 0) { if (!allow(me)) return reply({ ok: false, error: "Slow down a little." }); i = 0; while (db.homes[i]) i++; db.homes[i] = { uid: me.uid, at: Date.now() }; persist(); fanout("homes"); }
    return reply({ ok: true, index: i });
  }
  if (m.t === "home.invites") { // who has invited me, and until when
    const list = []; for (const owner in db.invites) { const until = db.invites[owner][me.uid]; if (until > Date.now()) list.push({ owner, until }); }
    return reply({ ok: true, list });
  }
  if (!allow(me)) return reply({ ok: false, error: "Slow down a little." });
  const to = String(m.to || "");
  if (m.t === "home.invite") { // let one player in for a few hours
    if (!UID_RE.test(to) || to === me.uid) return reply({ ok: false, error: "You can't invite that player." });
    const until = Date.now() + Math.min(24, Math.max(1, Number(m.hours) || 3)) * 3600000, inv = db.invites[me.uid] || (db.invites[me.uid] = dict());
    for (const g in inv) if (inv[g] < Date.now()) delete inv[g]; // tidy old ones
    if (Object.keys(inv).length >= 50) return reply({ ok: false, error: "You have too many open invites. Cancel some first." });
    inv[to] = until; persist(); tellUid(to, { t: "invited", owner: me.uid, until }); return reply({ ok: true, until });
  }
  if (m.t === "home.uninvite") { // take an invite back (and walk them out if they are inside)
    const inv = db.invites[me.uid]; if (inv) delete inv[to]; persist(); if (!canVisit(me.uid, to)) kickGuest(me.uid, to); return reply({ ok: true });
  }
  if (m.t === "home.party") { // open house while a party runs (at most 15 minutes from now)
    const until = Math.min(Date.now() + 15 * 60000, Number(m.until) || 0); if (until > Date.now()) parties.set(me.uid, until); else parties.delete(me.uid);
    return reply({ ok: true });
  }
  return reply({ ok: false, error: "Unknown request." });
}
setInterval(() => { for (const [owner, until] of parties) if (until <= Date.now()) { parties.delete(owner); for (const [, p] of peers) if (p.uid && roomOwner(p.presence || {}) === owner && !canVisit(owner, p.uid)) kickGuest(owner, p.uid); } }, 15000); // parties end: uninvited guests go home

async function account(ws, me, m) { // create a login for this character, or log in to another one
  const reply = (o) => send(ws, Object.assign({ t: "account", rid: m.rid }, o));
  if (!allow(me) || !allow(me)) return reply({ ok: false, error: "Slow down a little." }); // costs two write tokens
  const name = String(m.name || "").trim().toLowerCase(), pass = String(m.pass || "");
  if (!NAME_RE.test(name)) return reply({ ok: false, error: "Usernames are 3-20 letters, numbers or _." });
  if (pass.length < 8 || pass.length > 200) return reply({ ok: false, error: "Passwords need at least 8 characters." });
  try {
    if (m.t === "account.register") {
      const taken = () => db.auth.accounts[name] ? "That username is taken." : db.auth.names[me.uid] ? "This character already has a login." : ""; // one login per character, one character per login
      if (taken()) return reply({ ok: false, error: taken() });
      const salt = crypto.randomBytes(16).toString("hex"), hash = (await hashPass(pass, salt)).toString("hex");
      if (taken()) return reply({ ok: false, error: taken() }); // someone may have taken it while we hashed
      db.auth.accounts[name] = { uid: me.uid, salt, hash, at: Date.now() }; db.auth.names[me.uid] = name; persist();
      return reply({ ok: true, name });
    }
    const f = loginFails.get(name); if (f && f.until > Date.now()) return reply({ ok: false, error: "Too many tries. Wait a minute and try again." });
    const acc = db.auth.accounts[name], got = await hashPass(pass, acc ? acc.salt : "no-such-user"); // same work whether or not the name exists
    if (!acc || !crypto.timingSafeEqual(got, Buffer.from(acc.hash, "hex"))) {
      const n = (f ? f.n : 0) + 1; if (loginFails.size > 10000) loginFails.clear(); loginFails.set(name, { n, until: n >= 5 ? Date.now() + 60000 : 0 });
      return reply({ ok: false, error: "Wrong username or password." });
    }
    loginFails.delete(name); return reply({ ok: true, name, uid: acc.uid, token: sign(acc.uid) }); // the browser switches to this character
  } catch (e) { return reply({ ok: false, error: "Something went wrong. Try again." }); }
}

// ---------- the HTTP side: serve the game from /public ----------
const server = http.createServer((req, res) => {
  if (req.method !== "GET" && req.method !== "HEAD") { res.writeHead(405); return res.end("method not allowed"); } // read-only static server
  let url = decodeURIComponent((req.url || "/").split("?")[0]);   // the path only
  if (url === "/api/stats") { res.writeHead(200, { "content-type": "application/json", "cache-control": "no-cache" }); return res.end(JSON.stringify({ total: playedCount() })); } // the welcome screen's population
  if (url === "/") url = "/index.html";                           // default page
  const file = path.join(PUBLIC, path.normalize(url).replace(/^(\.\.[\/\\])+/, "")); // block path escapes
  if (file !== PUBLIC && !file.startsWith(PUBLIC + path.sep)) { res.writeHead(403); return res.end("forbidden"); } // stay inside public/
  fs.readFile(file, (err, buf) => {
    if (err) { res.writeHead(404, { "content-type": "text/plain" }); return res.end("not found"); } // missing file
    res.writeHead(200, { "content-type": MIME[path.extname(file)] || "application/octet-stream", "cache-control": "no-cache", "x-content-type-options": "nosniff" }); // serve it
    if (file === path.join(PUBLIC, "index.html")) { const host = String(req.headers["x-forwarded-host"] || req.headers.host || "").split(",")[0].trim(), proto = String(req.headers["x-forwarded-proto"] || "http").split(",")[0].trim() === "https" ? "https" : "http"; buf = Buffer.from(buf.toString("utf8").split("__SITE__").join(/^[a-z0-9.\-]+(:\d+)?$/i.test(host) ? proto + "://" + host : "")); } // link previews need the full address of the share picture
    res.end(buf);
  });
});

// ---------- the realtime side: one WebSocket per open game tab ----------
const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 }); // share the HTTP port; drop messages above 64 KB
const peers = new Map();                               // peerId -> { ws, uid, presence, tokens, last }
const subs = new Map();                                // ws -> [ { subId, col, limit, where } ]
let peersDirty = false;                                // did the room change since the last broadcast?

function send(ws, obj) { if (ws && ws.readyState === 1) { try { ws.send(JSON.stringify(obj)); } catch (e) {} } } // one message (an "away" player has no socket)
const AWAY_MS = 5 * 60000;                             // a player whose connection drops (phone screen off) stays in the world as "away" this long
function roomList() { const out = []; for (const [peer, p] of peers) if (p.uid) out.push({ peer, kind: "viewer", presence: p.away ? Object.assign({}, p.presence, { away: 1, mv: false }) : p.presence || {} }); return out; } // everyone in town (away players marked)
function playedCount() { return Object.keys(db.players).length; } // everyone who has ever made a character

// ---------- town gossip: cheaters and people caught doing something bad get exposed to everyone ----------
const EXPOSE = { // what each kind of scandal says ({o} the offender, {v} the victim)
  cheat: ["💔", "{o} was caught cheating on {v}!"], sneak: ["🫣", "{o} got caught with a sneak link behind {v}'s back!"],
  pickpocket: ["🚓", "{o} was caught pickpocketing {v}!"], robbery: ["🚔", "Police tracked down {o} for robbing {v}!"],
  bag: ["👜", "{o} was caught snatching a tourist's bag on the beach!"], zaza: ["🚬", "{o} got busted smoking zaza at the res!"],
  race: ["🏁", "{o} got busted street racing on Coastal Road!"], fridge: ["🍗", "{o} was caught raiding the shared fridge at SLR!"],
  permit: ["👮", "{o} was caught without a valid permit!"]
};
const cleanName = (v) => String(v || "").replace(/[<>\n\r\t]/g, "").trim().slice(0, 16); // names are shown as text, but keep them short and tidy
function expose(me, m) { // a player was caught: tell the whole town and remember it
  const t = EXPOSE[m.kind]; if (!t) return;
  const now = Date.now(); if (now - (me.exposedAt || 0) < 20000) return; me.exposedAt = now; // at most one scandal every 20 seconds per tab
  const doc = db.players[me.uid] || {}, who = cleanName(doc.name || (me.presence || {}).name) || "Someone", vic = cleanName(m.victim) || "their partner";
  const item = { id: crypto.randomBytes(6).toString("hex"), kind: m.kind, icon: t[0], text: t[1].replace("{o}", who).replace("{v}", vic), who, uid: me.uid, vuid: UID_RE.test(String(m.vuid || "")) ? String(m.vuid) : "", at: now };
  db.news.push(item); if (db.news.length > 60) db.news.splice(0, db.news.length - 60); persist(); // keep the latest 60
  for (const [, p] of peers) send(p.ws, { t: "news", item }); // everyone online sees it now
}
function broadcastPeers() { const list = roomList(), total = playedCount(); for (const [, p] of peers) send(p.ws, { t: "peers", peers: list, total }); } // tell every tab who is here
setInterval(() => { const now = Date.now(); for (const [id, p] of peers) if (p.away && now - p.away > AWAY_MS) { peers.delete(id); peersDirty = true; } }, 15000); // away too long: they have really gone
setInterval(() => { for (const [, p] of peers) { if (!p.ws || p.away) continue; if (p.alive === false) { try { p.ws.terminate(); } catch (e) {} continue; } p.alive = false; try { p.ws.ping(); } catch (e) {} } }, 25000); // a heartbeat: sockets that stop answering are closed (and their player turns "away")
setInterval(() => { if (peersDirty) { peersDirty = false; broadcastPeers(); } }, 100); // at most ~10 room updates a second

// read a whole collection as the game expects: [{ id, data }]
function docsOf(col, limit, where) {
  if (!PUBLIC_COLS.includes(col)) return [];           // never hand out login data
  let rows = Object.entries(db[col] || {}).map(([id, data]) => ({ id, data })); // every doc
  if (where) rows = rows.filter((r) => r.data && r.data[where.field] === where.value); // a simple field == value filter
  if (limit) rows = rows.slice(0, limit);               // cap the count
  return rows;
}
function pushSnap(ws, sub) { send(ws, { t: "snap", id: sub.subId, docs: docsOf(sub.col, sub.limit, sub.where) }); } // send a subscription its data
function fanout(col) { for (const [ws, list] of subs) for (const sub of list) if (sub.col === col) pushSnap(ws, sub); } // tell every watcher of this collection

wss.on("connection", (ws) => {
  const peerId = crypto.randomBytes(8).toString("hex");  // this tab's id
  peers.set(peerId, { ws, uid: "", presence: {}, tokens: RATE_CAP, last: Date.now(), alive: true }); // register it (presence + uid fill in later)
  ws.on("pong", () => { const p = peers.get(peerId); if (p) p.alive = true; }); // still there
  subs.set(ws, []);                                      // no db subscriptions yet
  peersDirty = true;                                     // the room changed
  const helloTimer = setTimeout(() => { const me = peers.get(peerId); if (me && !me.uid) ws.close(); }, 10000); // must say hello within 10 s

  ws.on("message", (raw) => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; } // ignore junk
    const me = peers.get(peerId); if (!me) return;        // gone already
    if (m.t === "hello") { // who this tab is: checked against its key, then welcomed (once per connection)
      if (me.uid) return; clearTimeout(helloTimer); me.uid = whoIs(m);
      for (const [id, p] of peers) if (p.away && p.uid === me.uid) { if (!Object.keys(me.presence).length) me.presence = p.presence; peers.delete(id); } peersDirty = true; // back from "away": take your old spot
      send(ws, { t: "welcome", peer: peerId, uid: me.uid, token: sign(me.uid), account: db.auth.names[me.uid] || "", news: db.news.slice(-25) }); return; // its id, key, login name and the latest gossip
    }
    if (!me.uid) return;                                  // nothing else until hello
    if (m.t === "expose") { expose(me, m); return; }      // someone got caught
    if (m.t === "account.register" || m.t === "account.login") { account(ws, me, m); return; } // logins
    if (m.t === "relay") { // a game message for one other player (football moves, chess moves, challenges)
      const to = String(m.to || ""), now = Date.now(); me.rt = Math.min(60, (me.rt == null ? 60 : me.rt) + (now - (me.rl || now)) / 1000 * 30); me.rl = now; // its own allowance: about 30 a second
      if (!UID_RE.test(to) || to === me.uid || me.rt < 1 || !sizeOk(m.d, 2048)) return; me.rt -= 1; // bad target, too fast or too big: dropped
      tellUid(to, { t: "relay", from: me.uid, d: m.d }); return; // passed straight on, never stored
    }
    if (m.t === "home.claim" || m.t === "home.invites" || m.t === "home.invite" || m.t === "home.uninvite" || m.t === "home.party") { homes(ws, me, m); return; } // homes and invites
    if (m.t === "presence") { if (sizeOk(m.patch, MAX_PRESENCE)) { // merge a small presence patch
      me.presence = Object.assign(me.presence || {}, m.patch || {}); me.presence.uid = me.uid; // nobody can pose as another player
      const owner = roomOwner(me.presence); if (!canVisit(owner, me.uid)) { me.presence.in = ""; me.presence.ph = ""; send(ws, { t: "kicked", owner }); } // no invite: not allowed in
      peersDirty = true; } return; }
    if (m.t === "doc.set") { // only your own players/<uid>, nothing else
      const parts = String(m.path || "").split("/"), col = parts[0], id = parts[1]; // which document
      if (col === "players" && id && id === me.uid && allow(me) && sizeOk(m.data, MAX_DOC)) { db.players[id] = m.data; persist(); fanout("players"); } // save your own record
      send(ws, { t: "ack", rid: m.rid }); return; // always ack so the client never hangs
    }
    if (m.t === "col.add") { // only valid mail (money transfers) may be added
      if (m.col === "mail" && allow(me) && sizeOk(m.data, MAX_MAIL) && validMail(m.data)) { const id = crypto.randomBytes(10).toString("hex"); db.mail[id] = cleanMail(m.data); pruneMail(); persist(); fanout("mail"); } // store a cleaned copy
      send(ws, { t: "ack", rid: m.rid }); return;
    }
    if (m.t === "doc.delete") { // you may only clear mail addressed to you
      if (m.col === "mail" && db.mail[m.id] && db.mail[m.id].to === me.uid && allow(me)) { delete db.mail[m.id]; persist(); fanout("mail"); } // remove your claimed transfer
      send(ws, { t: "ack", rid: m.rid }); return;
    }
    if (m.t === "sub") { const list = subs.get(ws); if (list && list.length < MAX_SUBS) { const sub = { subId: m.id, col: String(m.col || "").slice(0, 32), limit: Math.min(1000, +m.limit || 0), where: m.where || null }; list.push(sub); pushSnap(ws, sub); } return; } // watch a collection (capped)
    if (m.t === "unsub") { subs.set(ws, (subs.get(ws) || []).filter((s) => s.subId !== m.id)); return; } // stop watching
  });

  ws.on("close", () => { clearTimeout(helloTimer); subs.delete(ws); const p = peers.get(peerId); // the connection dropped
    if (p && p.uid && typeof (p.presence || {}).x === "number") { p.away = Date.now(); p.ws = null; } else peers.delete(peerId); peersDirty = true; }); // a player who was in town stays as "away" for a while; others just leave
  ws.on("error", () => {});                               // never crash on a socket error
});

server.listen(PORT, () => console.log("Flic en Flac Life running on http://localhost:" + PORT)); // go
