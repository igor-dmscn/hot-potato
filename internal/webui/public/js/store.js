// The harness's entire state, in one place, with a subscribe callback. This is
// not a framework — it is forty lines that stop the UI modules reading each
// other's DOM.

const state = {
  self: null, // the signed-in user, or null
  instance: location.host,
  health: { reachable: false, ok: false, ms: 0 },
};

const listeners = new Set();

export function get() {
  return state;
}

export function set(patch) {
  Object.assign(state, patch);
  for (const fn of listeners) fn(state);
}

export function subscribe(fn) {
  listeners.add(fn);
  fn(state);
  return () => listeners.delete(fn);
}
