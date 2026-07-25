// Rendering only. No fetching, no timers — those belong to app.js.

/** Show which origin this tab is talking to. Becomes the instance ID in phase 7. */
export function paintInstance(el, instance) {
  el.textContent = instance;
}
