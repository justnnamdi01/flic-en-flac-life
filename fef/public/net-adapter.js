// net-adapter.js — makes the self-hosted backend look exactly like the page host the game expects.
// Load this BEFORE the game script. It defines window.claude = { use } so the game's
// claude.use("user" | "room" | "db") calls work against your own server over WebSocket.
(function () {
  "use strict";
  // this browser's player id and the signed key that proves it; the server issues both on first visit
  var uid = "", token = "", accountName = "";
  try { uid = localStorage.getItem("fef_uid") || ""; token = localStorage.getItem("fef_token") || ""; } catch (e) {}
  function remember(id, key) { uid = id; token = key; try { localStorage.setItem("fef_uid", id); localStorage.setItem("fef_token", key); } catch (e) {} } // keep them for next time

  var proto = location.protocol === "https:" ? "wss:" : "ws:";   // secure socket on https pages
  var URL = proto + "//" + location.host;                         // same origin as the page
  var ws = null, open = false, myPeer = "";                       // the socket, its state, and this tab's id
  var readyQ = [];                                                // callbacks waiting for the first connection
  function whenReady() { return new Promise(function (res) { if (open) res(); else readyQ.push(res); }); } // resolves once connected

  var peersCb = null, peersCloseCb = null;                        // the room listener
  var snaps = {};                                                 // active db subscriptions: id -> { col, limit, where, cb }
  var pending = {};                                               // db writes waiting for an ack: rid -> { resolve, reject, timer }
  var ridN = 0, subN = 0;                                         // counters for request ids and subscription ids

  var myPres = null, retry = 0;                                   // everything you have told the room (re-sent after a reconnect), and the reconnect timer
  function reconnectNow() { if (open || (ws && ws.readyState <= 1)) return; clearTimeout(retry); connect(); } // the screen came back: reconnect at once
  document.addEventListener("visibilitychange", function () { if (!document.hidden) reconnectNow(); }); // phone unlocked or tab shown again
  window.addEventListener("online", reconnectNow); window.addEventListener("pageshow", reconnectNow); // network back, or page restored
  function connect() {
    try { ws = new WebSocket(URL); } catch (e) { setTimeout(connect, 2000); return; } // retry if the socket cannot be created
    ws.onopen = function () { ws.send(JSON.stringify({ t: "hello", uid: uid, token: token || undefined })); }; // identify with our key
    ws.onmessage = function (ev) {
      var m; try { m = JSON.parse(ev.data); } catch (e) { return; }
      if (m.t === "welcome") { myPeer = m.peer; remember(m.uid, m.token); accountName = m.account || ""; open = true; if (myPres) try { ws.send(JSON.stringify({ t: "presence", patch: myPres })); } catch (e) {} /* back online: tell everyone where you are again */ for (var id in snaps) sendSub(id); var q = readyQ; readyQ = []; q.forEach(function (r) { r(); }); } // accepted: the server says who we are, then restore subscriptions
      else if (m.t === "account" || m.t === "homes") { var ar = asks[m.rid]; if (ar) { clearTimeout(ar.timer); delete asks[m.rid]; ar.resolve(m); } } // an answer to a login or home request
      else if (m.t === "relay") { relayCbs.forEach(function (cb) { try { cb(m.from, m.d); } catch (e) {} }); } // a game message from another player
      else if (m.t === "invited" || m.t === "kicked") { (homeCbs[m.t] || []).forEach(function (cb) { try { cb(m); } catch (e) {} }); } // someone invited you, or you were sent out of a home
      else if (m.t === "peers") { if (peersCb) peersCb({ total: m.total | 0, peers: m.peers.map(function (p) { return { peer: p.peer, presence: p.presence, kind: p.kind || "viewer", isMe: p.peer === myPeer, sameTab: p.peer === myPeer }; }) }); } // room update (and how many people have ever played)
      else if (m.t === "snap") { var s = snaps[m.id]; if (s && s.cb) s.cb({ docs: m.docs.map(function (d) { return { id: d.id, data: function () { return d.data; } }; }) }); } // a collection's data
      else if (m.t === "ack") { var pr = pending[m.rid]; if (pr) { clearTimeout(pr.timer); pr.resolve(); delete pending[m.rid]; } } // a write landed
    };
    ws.onclose = function () { open = false; if (peersCloseCb) peersCloseCb(); clearTimeout(retry); retry = setTimeout(connect, 1500); }; // drop to solo and reconnect
    ws.onerror = function () { try { ws.close(); } catch (e) {} };
  }
  connect();

  function req(obj) { // a db write that resolves when the server acknowledges it (so .then/.catch behave)
    return whenReady().then(function () {
      return new Promise(function (resolve, reject) {
        var rid = ++ridN; obj.rid = rid;
        pending[rid] = { resolve: resolve, reject: reject, timer: setTimeout(function () { delete pending[rid]; reject(new Error("timeout")); }, 8000) };
        try { ws.send(JSON.stringify(obj)); } catch (e) { clearTimeout(pending[rid].timer); delete pending[rid]; reject(e); }
      });
    });
  }
  var asks = {};                                                  // login requests waiting for an answer: rid -> { resolve, timer }
  var homeCbs = {};                                               // listeners for "invited" and "kicked"
  var relayCbs = [];                                              // listeners for game messages from other players
  function ask(obj) { // a request whose answer comes back as a message (resolves with { ok, error, ... })
    return whenReady().then(function () {
      return new Promise(function (resolve) {
        var rid = ++ridN; obj.rid = rid;
        asks[rid] = { resolve: resolve, timer: setTimeout(function () { delete asks[rid]; resolve({ ok: false, error: "No answer from the server. Try again." }); }, 15000) };
        try { ws.send(JSON.stringify(obj)); } catch (e) { clearTimeout(asks[rid].timer); delete asks[rid]; resolve({ ok: false, error: "Not connected." }); }
      });
    });
  }
  function sendSub(id) { var s = snaps[id]; if (open && s) { try { ws.send(JSON.stringify({ t: "sub", id: id, col: s.col, limit: s.limit, where: s.where })); } catch (e) {} } } // start watching a collection
  function subscribe(col, limit, where, cb) { var id = "s" + (++subN); snaps[id] = { col: col, limit: limit, where: where, cb: cb }; whenReady().then(function () { sendSub(id); }); return function () { delete snaps[id]; if (open) try { ws.send(JSON.stringify({ t: "unsub", id: id })); } catch (e) {} }; } // returns an unsubscribe function

  function collection(col) {
    return {
      limit: function (n) { return { onSnapshot: function (cb) { return subscribe(col, n, null, cb); } }; },               // .limit(n).onSnapshot
      where: function (f, op, v) { return { onSnapshot: function (cb) { return subscribe(col, 0, { field: f, value: v }, cb); } }; }, // .where(f,"==",v).onSnapshot
      onSnapshot: function (cb) { return subscribe(col, 0, null, cb); },                                                   // whole-collection watch
      add: function (data) { return req({ t: "col.add", col: col, data: data }); },                                       // add a new document
      doc: function (id) { return { set: function (data) { return req({ t: "doc.set", path: col + "/" + id, data: data }); }, delete: function () { return req({ t: "doc.delete", col: col, id: id }); } }; } // one document in the collection
    };
  }
  function docRef(path) { return { set: function (data) { return req({ t: "doc.set", path: path, data: data }); } }; }     // db.doc("players/<uid>")

  // the object the game asks for
  window.claude = {
    use: function (name) {
      if (name === "user") return Promise.resolve({ id: function () { return whenReady().then(function () { return uid; }); } }); // who you are
      if (name === "room") return Promise.resolve({ onPeers: function (cb, closeCb) { peersCb = cb; peersCloseCb = closeCb; }, presence: function (patch) { myPres = Object.assign(myPres || {}, patch); if (open) try { ws.send(JSON.stringify({ t: "presence", patch: patch })); } catch (e) {} return Promise.resolve(); } }); // live players (remembered, so a reconnect restores you)
      if (name === "db") return Promise.resolve({ collection: collection, doc: docRef }); // the shared database
      return Promise.resolve(null); // anything else: not provided (the game falls back gracefully)
    },
    account: { // optional username + password, so a character can be played on other devices
      name: function () { return accountName; },                                  // this character's login, or "" if none
      register: function (name, pass) { return ask({ t: "account.register", name: name, pass: pass }).then(function (r) { if (r.ok) accountName = r.name; return r; }); }, // add a login to this character
      login: function (name, pass) { return ask({ t: "account.login", name: name, pass: pass }).then(function (r) { if (r.ok) remember(r.uid, r.token); return r; }); } // switch this browser to that character (reload afterwards)
    },
    relay: { // quick messages to one other player, for two-player games
      send: function (to, d) { if (open) try { ws.send(JSON.stringify({ t: "relay", to: to, d: d })); } catch (e) {} }, // to their player id
      on: function (cb) { relayCbs.push(cb); }                                                                        // cb(fromUid, data)
    },
    homes: { // private homes: your address, and who may come in
      claim: function () { return ask({ t: "home.claim" }); },                                   // your address number (given once, kept forever)
      invites: function () { return ask({ t: "home.invites" }); },                               // who has invited you: { list: [{ owner, until }] }
      invite: function (to, hours) { return ask({ t: "home.invite", to: to, hours: hours }); },  // let a player in for a few hours
      uninvite: function (to) { return ask({ t: "home.uninvite", to: to }); },                    // take it back
      party: function (until) { return ask({ t: "home.party", until: until }); },                // open house until then (0 to close)
      on: function (type, cb) { (homeCbs[type] = homeCbs[type] || []).push(cb); }                 // "invited" or "kicked"
    }
  };
})();
