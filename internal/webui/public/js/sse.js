// One EventSource per tab.
//
// There is no reconnect logic here on purpose: the browser reconnects by
// itself, the server's `retry:` hint sets the backoff, and every reconnection
// opens with a fresh snapshot — so there is nothing to replay and no
// Last-Event-ID to track (ADR 0003).

/**
 * Open the control-plane stream. handlers maps an event name to a function
 * taking the parsed payload; `onState` reports connected / disconnected.
 */
export function connect(handlers, onState) {
  const es = new EventSource("/events");

  es.addEventListener("open", () => onState(true));
  // EventSource fires `error` both for a failed connection and for a closed
  // one it is about to retry. Either way the UI is stale until `open`.
  es.addEventListener("error", () => onState(false));

  for (const [name, fn] of Object.entries(handlers)) {
    es.addEventListener(name, (e) => {
      try {
        fn(JSON.parse(e.data));
      } catch (err) {
        console.error(`bad ${name} payload`, e.data, err);
      }
    });
  }
  return es;
}
