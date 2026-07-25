// Rendering only. No fetching, no timers — those belong to app.js.

const DOT = {
  up: "bg-emerald-500",
  down: "bg-rose-500",
  unknown: "bg-stone-300 dark:bg-stone-700",
};

/** Paint the server status card from an api.health() result. */
export function paintStatus(root, result) {
  const dot = root.querySelector("[data-dot]");
  const verdict = root.querySelector("[data-verdict]");
  const latency = root.querySelector("[data-latency]");
  const checked = root.querySelector("[data-checked]");

  const state = !result.reachable ? "down" : result.ok ? "up" : "down";

  dot.className = `size-2.5 rounded-full ${DOT[state]}`;
  verdict.textContent = !result.reachable
    ? "Unreachable"
    : result.ok
      ? "Healthy"
      : `Unhealthy (${result.status})`;
  latency.textContent = result.reachable ? `${result.ms} ms` : "—";
  checked.textContent = new Date().toLocaleTimeString();
}

/** Render the roadmap, marking which phase is live. */
export function paintPhases(root, phases, currentIndex) {
  root.replaceChildren(
    ...phases.map((title, i) => {
      const li = document.createElement("li");
      const done = i < currentIndex;
      const now = i === currentIndex;

      li.className = [
        "flex gap-3 rounded px-2 py-1",
        now ? "bg-stone-200/60 font-medium dark:bg-stone-800/60" : "",
        done ? "text-stone-400 dark:text-stone-600" : "",
        !now && !done ? "text-stone-500 dark:text-stone-400" : "",
      ]
        .filter(Boolean)
        .join(" ");

      const num = document.createElement("span");
      num.className = "font-mono text-xs tabular-nums pt-0.5";
      num.textContent = String(i).padStart(2, "0");

      const label = document.createElement("span");
      label.textContent = title;

      li.append(num, label);
      return li;
    }),
  );
}

/** Show which origin this tab is talking to. Becomes the real instance ID in phase 7. */
export function paintInstance(el) {
  el.textContent = location.host;
}
