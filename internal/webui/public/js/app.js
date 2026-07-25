// Entry point: owns timing and wiring, delegates state to store.js and
// rendering to ui/*.js.

import * as api from "./api.js";
import * as store from "./store.js";
import { connect } from "./sse.js";
import { paintStatus, paintPhases, paintInstance } from "./ui/status.js";
import { mountAuth, mountSession, paintAuth } from "./ui/auth.js";
import { paintUsers, paintStream } from "./ui/users.js";
import {
  mountSend,
  paintRecipients,
  paintOffers,
  paintTransfers,
} from "./ui/transfers.js";

const PHASES = [
  "Skeleton and shutdown",
  "Auth",
  "SSE control plane",
  "Transfer state machine",
  "Offer, accept, deny",
  "The relay",
  "Progress",
  "Distributed",
  "Bus comparison",
  "spud CLI",
  "Ops and load",
  "Resume",
  "WebRTC",
];

const CURRENT_PHASE = 6;
const POLL_MS = 3000;

const el = {
  status: document.querySelector("[data-status]"),
  phases: document.querySelector("[data-phases]"),
  instance: document.querySelector("[data-instance]"),
  online: document.querySelector("[data-online]"),
  offers: document.querySelector("[data-offers]"),
  send: document.querySelector("[data-send]"),
  transfers: document.querySelector("[data-transfers]"),
  auth: {
    section: document.querySelector("[data-auth]"),
    strip: document.querySelector("[data-session]"),
    whoami: document.querySelector("[data-whoami]"),
  },
};

mountAuth(el.auth.section);
mountSession(el.auth.strip);
paintPhases(el.phases, PHASES, CURRENT_PHASE);

// Files picked for a Transfer, held until the server says the Recipient has
// attached. Nothing is read from them until then.
const outbound = new Map();

mountSend(el.send, (id, files) => outbound.set(id, files));

store.subscribe((state) => {
  paintInstance(el.instance, state.instance);
  paintAuth(el.auth, state.self);
  paintStatus(el.status, state.health);

  el.online.classList.toggle("hidden", !state.self);
  el.send.classList.toggle("hidden", !state.self);
  paintUsers(el.online, state.users, state.self);
  paintStream(el.online, state);
  paintRecipients(el.send, state.users, state.self);

  const transfers = store.transferList();
  // Accepting is what opens the download: the GET parks, and the server tells
  // the Sender to start once it has.
  paintOffers(el.offers, transfers, state.self, state.streamId, (t) =>
    api.receivePayload(t.id),
  );
  paintTransfers(el.transfers, transfers, state.self);
});

// One Stream while signed in, none while signed out.
let es = null;
let suspended = false;

function openStream() {
  es = connect(
    {
      snapshot: store.applySnapshot,
      "user.online": store.userOnline,
      "user.offline": store.userOffline,

      // Both of these carry the whole Transfer; everything after is a patch.
      "transfer.created": store.transferFull,
      "transfer.offered": store.transferFull,
      "transfer.accepted": (e) =>
        store.transferPatch({ id: e.id, state: "accepted", acceptedByStream: e.byStream }),
      "transfer.denied": (e) => store.transferPatch({ id: e.id, state: "denied" }),

      // The Recipient is parked. Now, and only now, the bytes may move.
      "transfer.ready": async (e) => {
        const files = outbound.get(e.id);
        if (!files) return; // another tab of ours is the one holding them
        outbound.delete(e.id);
        try {
          await api.sendPayload(e.id, files);
        } catch (err) {
          console.error("send failed", err);
        }
      },

      "transfer.canceled": (e) => store.transferPatch({ id: e.id, state: "canceled" }),
      "transfer.progress": (e) =>
        store.transferPatch({
          id: e.id,
          state: "streaming",
          bytesRelayed: e.bytes,
          bytesPerSec: e.bytesPerSec,
        }),
      "transfer.completed": (e) =>
        store.transferPatch({ id: e.id, state: "completed", bytesRelayed: e.bytes }),
      "transfer.failed": (e) =>
        store.transferPatch({
          id: e.id,
          state: "failed",
          failureReason: e.reason,
          bytesRelayed: e.bytesRelayed,
        }),
      // This instance is going away. Reopening lands on whichever instance the
      // entry point picks next, and the new Stream arrives with a fresh
      // snapshot, so nothing has to be replayed.
      "server.draining": () => {
        suspended = true;
        closeStream();
        setTimeout(() => {
          suspended = false;
          syncStream(store.get());
        }, 500);
      },
    },
    (connected) => store.set({ connected }),
  );
}

function closeStream() {
  es?.close();
  es = null;
  store.set({ connected: false, users: [], streamId: null });
}

function syncStream({ self }) {
  if (self && !es && !suspended) openStream();
  if (!self && es) closeStream();
}

store.subscribe(syncStream);

async function poll() {
  store.set({ health: await api.health() });
}

// Who am I? A session cookie survives a reload, so ask before drawing.
store.set({ self: await api.me() });
poll();

// Don't poll a tab nobody is looking at; catch up the moment it returns.
setInterval(() => {
  if (!document.hidden) poll();
}, POLL_MS);
document.addEventListener("visibilitychange", () => {
  if (!document.hidden) poll();
});
