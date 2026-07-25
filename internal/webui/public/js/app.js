// Entry point: owns timing and wiring, delegates state to store.js and
// rendering to ui/*.js.

import * as api from "./api.js";
import * as store from "./store.js";
import { paintStatus, paintPhases, paintInstance } from "./ui/status.js";
import { mountAuth, mountSession, paintAuth } from "./ui/auth.js";

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

const CURRENT_PHASE = 1;
const POLL_MS = 3000;

const el = {
  status: document.querySelector("[data-status]"),
  phases: document.querySelector("[data-phases]"),
  instance: document.querySelector("[data-instance]"),
  auth: {
    section: document.querySelector("[data-auth]"),
    strip: document.querySelector("[data-session]"),
    whoami: document.querySelector("[data-whoami]"),
  },
};

mountAuth(el.auth.section);
mountSession(el.auth.strip);
paintPhases(el.phases, PHASES, CURRENT_PHASE);

store.subscribe((state) => {
  paintInstance(el.instance, state.instance);
  paintAuth(el.auth, state.self);
  paintStatus(el.status, state.health);
});

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
