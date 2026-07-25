// The online list, and the little dot that says whether the Stream is live.

/** Paint everyone online, marking which one is you. */
export function paintUsers(root, users, self) {
  root.querySelector("[data-count]").textContent = users.length;

  const list = root.querySelector("[data-list]");
  if (users.length === 0) {
    const empty = document.createElement("li");
    empty.className = "text-sm text-stone-500 dark:text-stone-400";
    empty.textContent = "nobody yet — open a second browser";
    list.replaceChildren(empty);
    return;
  }

  list.replaceChildren(
    ...users.map((u) => {
      const li = document.createElement("li");
      li.className = "flex items-baseline justify-between gap-3 py-1.5";

      const name = document.createElement("span");
      name.className = "text-sm";
      name.textContent = u.displayName + (self && u.id === self.id ? " (you)" : "");

      const id = document.createElement("span");
      id.className = "font-mono text-xs text-stone-400 dark:text-stone-600";
      id.textContent = u.id;

      li.append(name, id);
      return li;
    }),
  );
}

/** The stream indicator: connected, or reconnecting. */
export function paintStream(root, { connected, streamId }) {
  const dot = root.querySelector("[data-streamdot]");
  const label = root.querySelector("[data-streamlabel]");

  dot.className = `size-2 rounded-full ${connected ? "bg-emerald-500" : "bg-amber-500"}`;
  label.textContent = connected ? (streamId ?? "live") : "reconnecting…";
}
