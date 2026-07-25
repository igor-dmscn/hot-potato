// The signup / login card and the "signed in as …" strip.

import * as api from "../api.js";
import * as store from "../store.js";

const ACTIVE = "bg-stone-200 text-stone-900 dark:bg-stone-800 dark:text-stone-100";
const IDLE = "text-stone-500 dark:text-stone-400";

/** Wire the auth form once. Re-rendering is store.subscribe's job. */
export function mountAuth(section) {
  const form = section.querySelector("[data-authform]");
  const error = section.querySelector("[data-autherror]");
  const displayName = section.querySelector("[data-displayname]");
  const label = section.querySelector("[data-submitlabel]");
  const tabs = [...section.querySelectorAll("[data-mode]")];
  let mode = "login";

  const paintMode = () => {
    for (const t of tabs) {
      t.className = `rounded px-2 py-1 ${t.dataset.mode === mode ? ACTIVE : IDLE}`;
    }
    displayName.classList.toggle("hidden", mode === "login");
    displayName.querySelector("input").required = mode === "signup";
    label.textContent = mode === "login" ? "Sign in" : "Create account";
    error.classList.add("hidden");
  };

  for (const t of tabs) {
    t.addEventListener("click", () => {
      mode = t.dataset.mode;
      paintMode();
    });
  }

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const data = new FormData(form);
    const button = form.querySelector("button[type=submit]");
    button.disabled = true;
    error.classList.add("hidden");
    try {
      const self =
        mode === "login"
          ? await api.login(data.get("email"), data.get("password"))
          : await api.signup(data.get("email"), data.get("displayName"), data.get("password"));
      form.reset();
      store.set({ self });
    } catch (err) {
      // The server's message is written for a human; show it verbatim.
      error.textContent = err.message;
      error.classList.remove("hidden");
    } finally {
      button.disabled = false;
    }
  });

  paintMode();
}

/** Wire the sign-out button in the header. */
export function mountSession(strip) {
  strip.querySelector("[data-signout]").addEventListener("click", async () => {
    await api.logout();
    store.set({ self: null });
  });
}

/** Show the form or the identity strip, never both. */
export function paintAuth({ section, strip, whoami }, self) {
  section.classList.toggle("hidden", Boolean(self));
  strip.classList.toggle("hidden", !self);
  whoami.textContent = self ? `${self.displayName} <${self.email}>` : "";
}
