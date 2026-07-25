// The send form, the incoming offer cards, and the list of Transfers in flight.

import * as api from "../api.js";
import * as store from "../store.js";

const STATE_STYLE = {
  pending: "text-amber-600 dark:text-amber-400",
  accepted: "text-sky-600 dark:text-sky-400",
  streaming: "text-sky-600 dark:text-sky-400",
  completed: "text-emerald-600 dark:text-emerald-400",
  denied: "text-stone-500 dark:text-stone-400",
  expired: "text-stone-500 dark:text-stone-400",
  canceled: "text-stone-500 dark:text-stone-400",
  failed: "text-rose-600 dark:text-rose-400",
};

export function bytes(n) {
  if (n < 1024) return `${n} B`;
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v < 10 ? 1 : 0)} ${units[i]}`;
}

/**
 * Wire the send form. `onOffer` is handed the picked files and the created
 * Transfer, so the phase that can actually upload them has somewhere to hook in.
 */
export function mountSend(section, onOffer) {
  const form = section.querySelector("[data-sendform]");
  const error = section.querySelector("[data-senderror]");
  const fileInput = form.querySelector("[data-file]");
  const folderInput = form.querySelector("[data-folder]");
  const chosen = section.querySelector("[data-chosen]");

  let files = [];
  let kind = "file";

  const describe = () => {
    if (files.length === 0) {
      chosen.textContent = "nothing picked";
      return null;
    }
    const totalBytes = files.reduce((sum, f) => sum + f.size, 0);
    // A folder's name comes from the first entry's relative path; a file is
    // just itself.
    const name =
      kind === "folder"
        ? (files[0].webkitRelativePath || files[0].name).split("/")[0]
        : files[0].name;
    chosen.textContent = `${name} — ${files.length} file(s), ${bytes(totalBytes)}`;
    return { name, kind, totalBytes, entryCount: files.length };
  };

  fileInput.addEventListener("change", () => {
    kind = "file";
    files = [...fileInput.files];
    folderInput.value = "";
    describe();
  });
  folderInput.addEventListener("change", () => {
    kind = "folder";
    files = [...folderInput.files];
    fileInput.value = "";
    describe();
  });

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    error.classList.add("hidden");
    const payload = describe();
    const to = form.querySelector("[data-to]").value;
    if (!payload || !to) {
      error.textContent = "pick a recipient and a file";
      error.classList.remove("hidden");
      return;
    }
    const button = form.querySelector("button[type=submit]");
    button.disabled = true;
    try {
      const created = await api.createTransfer(
        to,
        payload.name,
        payload.kind,
        payload.totalBytes,
        payload.entryCount,
      );
      onOffer(created.id, files, payload);
      form.reset();
      files = [];
      describe();
    } catch (err) {
      error.textContent = err.message;
      error.classList.remove("hidden");
    } finally {
      button.disabled = false;
    }
  });
}

/** Keep the recipient picker in step with who is online. */
export function paintRecipients(section, users, self) {
  const select = section.querySelector("[data-to]");
  const previous = select.value;
  select.replaceChildren(
    ...users.map((u) => {
      const option = document.createElement("option");
      option.value = u.id;
      option.textContent = self && u.id === self.id ? `${u.displayName} (you)` : u.displayName;
      return option;
    }),
  );
  if (users.some((u) => u.id === previous)) select.value = previous;
}

/**
 * Paint the offers waiting on this User's decision. `onAccept` is called with
 * the Transfer so the download can be started by whoever knows how.
 */
export function paintOffers(root, transfers, self, streamId, onAccept) {
  const incoming = transfers.filter((t) => t.state === "pending" && t.recipient === self?.id);
  root.classList.toggle("hidden", incoming.length === 0);

  root.querySelector("[data-list]").replaceChildren(
    ...incoming.map((t) => {
      const card = document.createElement("li");
      card.className =
        "flex flex-wrap items-center justify-between gap-4 rounded-lg border border-amber-300 bg-amber-50/60 p-4 dark:border-amber-800 dark:bg-amber-950/30";

      const what = document.createElement("div");
      what.className = "min-w-0";
      const name = document.createElement("p");
      name.className = "truncate text-sm font-medium";
      name.textContent = t.payload.name;
      const meta = document.createElement("p");
      meta.className = "font-mono text-xs text-stone-500 dark:text-stone-400";
      meta.textContent = `${t.payload.kind} · ${bytes(t.payload.totalBytes)} · from ${t.sender}`;
      what.append(name, meta);

      const buttons = document.createElement("div");
      buttons.className = "flex gap-2";

      const accept = document.createElement("button");
      accept.className =
        "rounded bg-stone-900 px-3 py-1.5 text-sm font-medium text-stone-50 hover:bg-stone-700 dark:bg-stone-100 dark:text-stone-900";
      accept.textContent = "Accept";
      accept.addEventListener("click", async () => {
        accept.disabled = true;
        try {
          await api.acceptTransfer(t.id, streamId);
          onAccept(t);
        } catch (err) {
          console.error("accept", err);
          accept.disabled = false;
        }
      });

      const deny = document.createElement("button");
      deny.className =
        "rounded border border-stone-300 px-3 py-1.5 text-sm hover:bg-stone-100 dark:border-stone-700 dark:hover:bg-stone-900";
      deny.textContent = "Deny";
      deny.addEventListener("click", () => api.denyTransfer(t.id).catch(console.error));

      buttons.append(accept, deny);
      card.append(what, buttons);
      return card;
    }),
  );
}

/** Paint every Transfer this User is party to, whichever way it is going. */
export function paintTransfers(root, transfers, self) {
  root.classList.toggle("hidden", transfers.length === 0);

  root.querySelector("[data-list]").replaceChildren(
    ...transfers
      .slice()
      .reverse()
      .map((t) => {
        const row = document.createElement("li");
        row.className = "space-y-2 py-3";

        const top = document.createElement("div");
        top.className = "flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1";

        const left = document.createElement("p");
        left.className = "min-w-0 truncate text-sm";
        const direction = t.sender === self?.id ? "→" : "←";
        const other = t.sender === self?.id ? t.recipient : t.sender;
        left.textContent = `${direction} ${t.payload.name} (${other})`;

        const right = document.createElement("p");
        right.className = `font-mono text-xs ${STATE_STYLE[t.state] ?? ""}`;
        right.textContent = t.failureReason ? `${t.state}: ${t.failureReason}` : t.state;

        top.append(left, right);
        row.append(top);

        // A progress bar while the bytes are moving, and after, so a completed
        // Transfer still shows what it moved.
        if (t.state === "streaming" || t.state === "completed" || t.bytesRelayed > 0) {
          const done = Math.min(t.bytesRelayed ?? 0, t.payload.totalBytes);
          const track = document.createElement("div");
          track.className = "h-1.5 overflow-hidden rounded-full bg-stone-200 dark:bg-stone-800";
          const bar = document.createElement("div");
          bar.className = `h-full ${t.state === "failed" ? "bg-rose-500" : "bg-emerald-500"}`;
          bar.style.width = `${((done / t.payload.totalBytes) * 100).toFixed(1)}%`;
          track.append(bar);

          const numbers = document.createElement("p");
          numbers.className = "font-mono text-xs text-stone-500 dark:text-stone-400";
          const rate = t.bytesPerSec ? ` · ${bytes(t.bytesPerSec)}/s` : "";
          numbers.textContent = `${bytes(done)} of ${bytes(t.payload.totalBytes)}${rate}`;

          row.append(track, numbers);
        }

        if (!["completed", "denied", "expired", "canceled", "failed"].includes(t.state)) {
          const cancel = document.createElement("button");
          cancel.className =
            "font-mono text-xs text-stone-500 underline decoration-dotted hover:text-stone-900 dark:hover:text-stone-100";
          cancel.textContent = "cancel";
          cancel.addEventListener("click", () => api.cancelTransfer(t.id).catch(console.error));
          row.append(cancel);
        }

        return row;
      }),
  );
}
