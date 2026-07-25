// A peer-to-peer data plane.
//
// The control plane above this file is untouched: the same offer, the same
// accept, the same events, the same snapshot. Only the bytes go somewhere else.
// Everything awkward in here is a thing the server was doing for free.

import * as api from "./api.js";

// The same 64 KiB the server's relay uses, so the comparison is like for like.
const CHUNK = 64 * 1024;

// A high-water mark for the data channel's send buffer. On the HTTP data plane
// backpressure needed no code at all — the copy blocked on the Recipient's
// socket, which stopped draining the Sender's body. Here it is a manual
// watermark, and getting it wrong means the sending tab's memory grows until it
// dies.
const HIGH_WATER = 8 * 1024 * 1024;
const LOW_WATER = 1 * 1024 * 1024;

/**
 * No ICE servers by default: host candidates only, which works on a LAN and on
 * localhost and nowhere else. A real deployment needs STUN to discover its
 * public address, and TURN — a relay you have to run and pay for — for the
 * connections STUN cannot rescue. That is the first bill this design sends you
 * for not passing the bytes through the server.
 */
const ICE_SERVERS = [];

function connection() {
  return new RTCPeerConnection({ iceServers: ICE_SERVERS });
}

/** Anything the peers say to each other goes through the server verbatim. */
const send = (id, signal) => api.signalTransfer(id, signal).catch(console.error);

/**
 * The Sender's half. Called when the Recipient says it is ready, and resolves
 * when every byte has been handed to the data channel and acknowledged.
 */
export function sendOverWebRTC(id, files, onProgress) {
  return new Promise((resolve, reject) => {
    const pc = connection();
    const channel = pc.createDataChannel("payload", { ordered: true });
    channel.binaryType = "arraybuffer";
    channel.bufferedAmountLowThreshold = LOW_WATER;

    pc.addEventListener("icecandidate", (e) => {
      if (e.candidate) send(id, { candidate: e.candidate });
    });
    pc.addEventListener("connectionstatechange", () => {
      if (pc.connectionState === "failed") {
        // No TURN, no traversal. The server cannot tell you this happened,
        // because the server is not involved.
        reject(new Error("the peer connection failed — a TURN relay would be needed here"));
      }
    });

    channel.addEventListener("open", async () => {
      try {
        await pump(channel, files, onProgress);
        resolve();
      } catch (err) {
        reject(err);
      } finally {
        channel.close();
        pc.close();
      }
    });

    // The Sender makes the offer, because the Recipient has already said it is
    // listening.
    pc.createOffer()
      .then((offer) => pc.setLocalDescription(offer))
      .then(() => send(id, { description: pc.localDescription }))
      .catch(reject);

    // Handed back so app.js can feed in what arrives on the Stream.
    resolve.peer = pc;
    sendOverWebRTC.peers.set(id, pc);
  });
}
sendOverWebRTC.peers = new Map();

/** Feed a signal from the counterparty into an open peer connection. */
export async function acceptSignal(id, signal) {
  const pc = sendOverWebRTC.peers.get(id) ?? receiveOverWebRTC.peers.get(id);
  if (!pc) return;

  if (signal.description) {
    await pc.setRemoteDescription(signal.description);
    if (signal.description.type === "offer") {
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      send(id, { description: pc.localDescription });
    }
  } else if (signal.candidate) {
    try {
      await pc.addIceCandidate(signal.candidate);
    } catch (err) {
      // A candidate arriving before the description is normal.
      console.debug("ice candidate", err);
    }
  }
}

/**
 * The Recipient's half. Announces itself, then waits for the data channel and
 * assembles what comes down it.
 */
export function receiveOverWebRTC(id, transfer, onProgress) {
  return new Promise((resolve, reject) => {
    const pc = connection();
    receiveOverWebRTC.peers.set(id, pc);

    pc.addEventListener("icecandidate", (e) => {
      if (e.candidate) send(id, { candidate: e.candidate });
    });
    pc.addEventListener("connectionstatechange", () => {
      if (pc.connectionState === "failed") {
        reject(new Error("the peer connection failed — a TURN relay would be needed here"));
      }
    });

    pc.addEventListener("datachannel", (e) => {
      const channel = e.channel;
      channel.binaryType = "arraybuffer";

      // Held in memory, in the tab, in full. The server streamed straight into
      // the response and never held more than 64 KiB; a browser has no
      // equivalent of that without the File System Access API.
      const chunks = [];
      let received = 0;

      channel.addEventListener("message", (msg) => {
        if (typeof msg.data === "string") {
          if (msg.data === "done") {
            const blob = new Blob(chunks, { type: "application/octet-stream" });
            save(blob, transfer.payload.kind === "folder"
              ? `${transfer.payload.name}.zip`
              : transfer.payload.name);
            channel.close();
            pc.close();
            // The Recipient reports, because the Recipient is the one that knows
            // what arrived. The server has to take its word for it.
            api.signalTransfer(id, null, { bytes: received }).then(resolve, reject);
          }
          return;
        }
        chunks.push(msg.data);
        received += msg.data.byteLength;
        onProgress(received);
      });
    });

    // "I am listening." This is also what makes the server mark the Recipient
    // attached, using the same transition the HTTP data plane uses.
    send(id, { ready: true });
  });
}
receiveOverWebRTC.peers = new Map();

/** Push every file down the channel, respecting the watermark. */
async function pump(channel, files, onProgress) {
  let sent = 0;
  for (const file of files) {
    const reader = file.stream().getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      for (let at = 0; at < value.byteLength; at += CHUNK) {
        const slice = value.buffer.slice(at, Math.min(at + CHUNK, value.byteLength));
        await drain(channel);
        channel.send(slice);
        sent += slice.byteLength;
        onProgress(sent);
      }
    }
  }
  await drain(channel, 0);
  channel.send("done");
}

/** Wait until the send buffer has drained below the watermark. */
function drain(channel, threshold = HIGH_WATER) {
  if (channel.bufferedAmount <= threshold) return Promise.resolve();
  return new Promise((resolve) => {
    const check = () => {
      if (channel.bufferedAmount <= LOW_WATER) {
        channel.removeEventListener("bufferedamountlow", check);
        resolve();
      }
    };
    channel.addEventListener("bufferedamountlow", check);
  });
}

/** Save a blob to disk. The HTTP path got this from Content-Disposition. */
function save(blob, name) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.click();
  URL.revokeObjectURL(url);
}
