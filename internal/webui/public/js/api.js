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

/**
 * POST /d/{id} — the data plane. Only legal once transfer.ready has arrived:
 * before that the Recipient has not parked and the server answers 409.
 *
 * The third argument to append is what puts the relative path in each part's
 * filename. Without it the folder structure is lost, and every file lands in
 * the root of the archive.
 */
export async function sendPayload(id, files) {
  const form = new FormData();
  for (const f of files) {
    form.append("files", f, f.webkitRelativePath || f.name);
  }
  const res = await fetch(`/d/${encodeURIComponent(id)}`, { method: "POST", body: form });
  if (res.status === 204) return;

  const text = await res.text();
  const parsed = text ? JSON.parse(text) : null;
  throw new ApiError(res.status, parsed?.error ?? "unknown", parsed?.message);
}

/**
 * Start the download. A plain navigation is the right tool: the request parks
 * until the Sender attaches, then arrives with Content-Disposition and the
 * browser saves it — no blob, no memory held in the page.
 */
export function receivePayload(id) {
  window.location.assign(`/d/${encodeURIComponent(id)}`);
}

/** GET /api/me. Returns null when signed out rather than throwing. */
export async function me() {
  try {
    return await send("GET", "/api/me");
  } catch (err) {
    if (err instanceof ApiError && err.status === 401) return null;
    throw err;
  }
}
