// The harness's entire state, in one place, with a subscribe callback. This is
// not a framework — it is fifty lines that stop the UI modules reading each
// other's DOM.

const state = {
  self: null, // the signed-in user, or null
  instance: location.host,
  streamId: null,
  connected: false,
  users: [], // everyone online, from the snapshot then the deltas
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

// The control plane's three presence events, applied to the picture. A snapshot
// replaces it wholesale; the deltas edit it.

export function applySnapshot(snap) {
  set({
    self: snap.self,
    instance: snap.instance,
    streamId: snap.streamId,
    users: snap.users ?? [],
  });
}

export function userOnline(user) {
  if (state.users.some((u) => u.id === user.id)) return;
  set({ users: [...state.users, user].sort(byName) });
}

export function userOffline(user) {
  set({ users: state.users.filter((u) => u.id !== user.id) });
}

const byName = (a, b) => a.displayName.localeCompare(b.displayName);
