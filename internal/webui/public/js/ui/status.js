// Rendering only. No fetching, no timers — those belong to app.js.

const DOT = {
  up: "bg-emerald-500",
  down: "bg-rose-500",
  unknown: "bg-stone-300 dark:bg-stone-700",
};

/** Paint the server status card from an api.health() result. */
export function paintStatus(root, result) {
  const state = !result.reachable ? "down" : result.ok ? "up" : "down";

  root.querySelector("[data-dot]").className = `size-2.5 rounded-full ${DOT[state]}`;
  root.querySelector("[data-verdict]").textContent = !result.reachable
    ? "Unreachable"
    : result.ok
      ? "Healthy"
      : `Unhealthy (${result.status})`;
  root.querySelector("[data-latency]").textContent = result.reachable ? `${result.ms} ms` : "—";
  root.querySelector("[data-checked]").textContent = new Date().toLocaleTimeString();
}

/** Show which origin this tab is talking to. Becomes the instance ID in phase 7. */
export function paintInstance(el, instance) {
  el.textContent = instance;
}
