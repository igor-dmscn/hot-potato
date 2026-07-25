// The online list, and the little dot that says whether the Stream is live.

/**
 * Paint everyone online. `users` is already everyone but you — you cannot send
 * to yourself, so you are not a row. `onSend(userId, kind)` opens the picker.
 */
export function paintUsers(root, users, onSend) {
  root.querySelector("[data-count]").textContent = users.length;

  const list = root.querySelector("[data-list]");
  if (users.length === 0) {
    const empty = document.createElement("li");
    empty.className = "text-sm text-stone-500 dark:text-stone-400";
    empty.textContent = "nobody else yet — open a second browser";
    list.replaceChildren(empty);
    return;
  }

  list.replaceChildren(
    ...users.map((u) => {
      const li = document.createElement("li");
      li.className = "flex flex-wrap items-center justify-between gap-3 py-2";

      const who = document.createElement("div");
      who.className = "min-w-0";
      const name = document.createElement("p");
      name.className = "truncate text-sm";
      name.textContent = u.displayName;
      const id = document.createElement("p");
      id.className = "truncate font-mono text-xs text-stone-400 dark:text-stone-600";
      id.textContent = u.id;
      who.append(name, id);

      const send = document.createElement("div");
      send.className = "flex gap-2";
      for (const kind of ["file", "folder"]) {
        const button = document.createElement("button");
        button.className =
          "rounded border border-stone-300 px-2.5 py-1 font-mono text-xs hover:bg-stone-100 dark:border-stone-700 dark:hover:bg-stone-900";
        button.textContent = `send ${kind}`;
        button.addEventListener("click", () => onSend(u.id, kind));
        send.append(button);
      }

      li.append(who, send);
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
