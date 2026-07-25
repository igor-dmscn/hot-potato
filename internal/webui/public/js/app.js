// Entry point: owns state and timing, delegates rendering to ui.js.

import { health } from "./api.js";
import { paintStatus, paintPhases, paintInstance } from "./ui.js";

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

const CURRENT_PHASE = 0;
const POLL_MS = 3000;

const statusCard = document.querySelector("[data-status]");

async function poll() {
  paintStatus(statusCard, await health());
}

paintInstance(document.querySelector("[data-instance]"));
paintPhases(document.querySelector("[data-phases]"), PHASES, CURRENT_PHASE);
poll();

// Don't poll a tab nobody is looking at; catch up the moment it returns.
setInterval(() => {
  if (!document.hidden) poll();
}, POLL_MS);
document.addEventListener("visibilitychange", () => {
  if (!document.hidden) poll();
});
