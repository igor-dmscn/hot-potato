// Entry point: owns timing and wiring, delegates state to store.js and
// rendering to ui/*.js.

import * as api from "./api.js";
import * as store from "./store.js";
import { connect } from "./sse.js";
import { paintInstance } from "./ui/status.js";
import { mountAuth, mountSession, paintAuth } from "./ui/auth.js";
import { paintUsers, paintStream } from "./ui/users.js";
import { mountSend, paintOffers, paintTransfers } from "./ui/transfers.js";

const el = {
  instance: document.querySelector("[data-instance]"),
  online: document.querySelector("[data-online]"),
  offers: document.querySelector("[data-offers]"),
  transfers: document.querySelector("[data-transfers]"),
  auth: {
    section: document.querySelector("[data-auth]"),
    strip: document.querySelector("[data-session]"),
    whoami: document.querySelector("[data-whoami]"),
  },
};

mountAuth(el.auth.section);
mountSession(el.auth.strip);

// Files picked for a Transfer, held until the server says the Recipient has
// attached. Nothing is read from them until then.
const outbound = new Map();

// The pickers live outside any section, so the whole document is their root.
const send = mountSend(document, (id, files) => outbound.set(id, files));

store.subscribe((state) => {
  paintInstance(el.instance, state.instance);
  paintAuth(el.auth, state.self);

  el.online.classList.toggle("hidden", !state.self);
  // The snapshot puts you in your own list (docs/protocol.md); a row you cannot
  // send to is not worth drawing, so drop yourself here.
  paintUsers(el.online, state.users.filter((u) => u.id !== state.self?.id), send);
  paintStream(el.online, state);

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

// Who am I? A session cookie survives a reload, so ask before drawing.
store.set({ self: await api.me() });
