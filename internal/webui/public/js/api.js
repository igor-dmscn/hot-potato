// Every call the harness makes to the backend lives here, so the UI modules
// never touch fetch directly. Grows one function per phase.

/** Probe GET /healthz. Never throws — a dead server is a result, not an error. */
export async function health() {
  const started = performance.now();
  try {
    const res = await fetch("/healthz", { cache: "no-store" });
    return {
      reachable: true,
      ok: res.ok,
      status: res.status,
      ms: Math.round(performance.now() - started),
    };
  } catch (err) {
    return {
      reachable: false,
      ok: false,
      status: 0,
      ms: Math.round(performance.now() - started),
      error: String(err),
    };
  }
}
