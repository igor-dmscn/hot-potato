// Every call the harness makes to the backend lives here, so the UI modules
// never touch fetch directly.

/** The server's error envelope, thrown. */
export class ApiError extends Error {
  constructor(status, code, message) {
    super(message || code);
    this.status = status;
    this.code = code;
  }
}

/**
 * One JSON request. Content-Type: application/json is not decoration — the
 * server rejects state-changing requests without it, which is half of its CSRF
 * defence, so it goes on every call including the ones with no body.
 */
async function send(method, path, body) {
  const res = await fetch(path, {
    method,
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (res.status === 204) return null;

  const text = await res.text();
  const parsed = text ? JSON.parse(text) : null;
  if (!res.ok) {
    throw new ApiError(res.status, parsed?.error ?? "unknown", parsed?.message);
  }
  return parsed;
}

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

export const signup = (email, displayName, password) =>
  send("POST", "/api/signup", { email, displayName, password });

export const login = (email, password) => send("POST", "/api/login", { email, password });

export const logout = () => send("POST", "/api/logout");

/** POST /api/transfers — propose one. Returns {id, expiresAt}. */
export const createTransfer = (to, name, kind, totalBytes, entryCount) =>
  send("POST", "/api/transfers", { to, name, kind, totalBytes, entryCount });

export const acceptTransfer = (id, streamId) =>
  send("POST", `/api/transfers/${encodeURIComponent(id)}/accept`, { streamId });

export const denyTransfer = (id) => send("POST", `/api/transfers/${encodeURIComponent(id)}/deny`);

export const cancelTransfer = (id) =>
  send("POST", `/api/transfers/${encodeURIComponent(id)}/cancel`);

/** GET /api/me. Returns null when signed out rather than throwing. */
export async function me() {
  try {
    return await send("GET", "/api/me");
  } catch (err) {
    if (err instanceof ApiError && err.status === 401) return null;
    throw err;
  }
}
