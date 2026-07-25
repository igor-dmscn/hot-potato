// The harness's entire state, in one place, with a subscribe callback. This is
// not a framework — it is fifty lines that stop the UI modules reading each
// other's DOM.

const state = {
  self: null, // the signed-in user, or null
  instance: location.host,
  streamId: null,
  connected: false,
  users: [], // everyone online, from the snapshot then the deltas
  transfers: {}, // by id — both directions
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
    // A snapshot replaces the picture: anything not in it is over and gone.
    transfers: Object.fromEntries((snap.transfers ?? []).map((t) => [t.id, t])),
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

// Transfers. transfer.created and transfer.offered carry the whole thing;
// everything after them is a patch keyed by id.

export function transferFull(t) {
  set({ transfers: { ...state.transfers, [t.id]: t } });
}

export function transferPatch(patch) {
  const existing = state.transfers[patch.id];
  // A patch for something we never saw means our picture is stale — the next
  // reconnect will bring a snapshot, so ignore it rather than invent a row.
  if (!existing) return;
  set({ transfers: { ...state.transfers, [patch.id]: { ...existing, ...patch } } });
}

/** Everything this User is sending or being offered, newest last. */
export function transferList() {
  return Object.values(state.transfers).sort((a, b) => a.createdAt.localeCompare(b.createdAt));
}
