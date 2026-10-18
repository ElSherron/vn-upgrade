/*
 * voicechat.js: drop-in voice chat for browser games.
 *
 *   const voice = new VoiceChat({ url: "ws://localhost:8080", room: "match-42", name: "Ada" });
 *   voice.on("speaking", (id, isSpeaking) => showMicIcon(id, isSpeaking));
 *   await voice.join();          // call from a click/tap (browsers require it)
 *   voice.startTalking();        // push-to-talk down
 *   voice.stopTalking();         // push-to-talk up
 *
 * Needs HTTPS (or localhost) for microphone access.
 */
(function (global) {
  const WORKLET_SOURCE = `
    class Capture extends AudioWorkletProcessor {
      constructor() { super(); this.buf = new Float32Array(1024); this.n = 0; }
      process(inputs) {
        const ch = inputs[0][0];
        if (!ch) return true;
        for (let i = 0; i < ch.length; i++) {
          this.buf[this.n++] = ch[i];
          if (this.n === this.buf.length) { this.port.postMessage(this.buf.slice()); this.n = 0; }
        }
        return true;
      }
    }
    registerProcessor("capture", Capture);`;

  class VoiceChat {
    constructor(options = {}) {
      this.opts = {
        url: "ws://localhost:8080",
        room: "lobby",
        name: "Player",
        mode: "ptt",            // "ptt" (push-to-talk) or "open" (voice activity)
        sampleRate: 16000,      // network audio rate (voice quality, ~32 KB/s while talking)
        jitter: 0.12,           // playback buffer in seconds
        vadThreshold: 0.02,     // open-mic sensitivity (lower = more sensitive)
        vadHangoverMs: 300,     // keep transmitting this long after you stop speaking
        proximity: false,       // fade voices with distance (see setPosition)
        maxDistance: 50,        // distance at which voices become silent
        ...options,
      };
      this.handlers = {};
      this.peers = new Map();   // id -> peer state
      this.id = null;
      this.muted = false;       // your mic
      this.deafened = false;    // all incoming audio
      this.transmitting = false;
      this.level = 0;           // current mic level (0..1) for UI meters
      this._lastVoice = -Infinity;
      this._preroll = [];
      this._listener = { x: 0, y: 0 };
      this._leaving = false;
    }

    // ----- events: joined, peerJoin, peerLeave, speaking, disconnected, error -----
    on(event, fn) { (this.handlers[event] = this.handlers[event] || []).push(fn); return this; }
    _emit(event, ...args) { (this.handlers[event] || []).forEach((fn) => fn(...args)); }

    // ----- lifecycle -----
    async join() {
      try {
        this.ctx = new (global.AudioContext || global.webkitAudioContext)();
        await this.ctx.resume(); // unlocks playback

        this.stream = await navigator.mediaDevices.getUserMedia({
          audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true },
        });

        const url = URL.createObjectURL(new Blob([WORKLET_SOURCE], { type: "application/javascript" }));
        await this.ctx.audioWorklet.addModule(url);
        const mic = this.ctx.createMediaStreamSource(this.stream);
        const node = new AudioWorkletNode(this.ctx, "capture");
        node.port.onmessage = (e) => this._onMic(e.data);
        mic.connect(node);
      } catch (err) {
        const message = err.name === "NotAllowedError" ? "Microphone access denied" : "Audio setup failed: " + err.message;
        this._emit("error", message);
        throw new Error(message);
      }
      this._leaving = false;
      return this._connect();
    }

    leave() {
      this._leaving = true;
      this._stopTx();
      if (this.ws) this.ws.close();
      if (this.stream) this.stream.getTracks().forEach((t) => t.stop());
      if (this.ctx) this.ctx.close();
      this.peers.clear();
    }

    _connect() {
      return new Promise((resolve, reject) => {
        const ws = (this.ws = new WebSocket(this.opts.url));
        ws.binaryType = "arraybuffer";
        const timeout = setTimeout(() => reject(new Error("Connection timed out")), 8000);

        ws.onopen = () => ws.send(JSON.stringify({ type: "join", room: this.opts.room, name: this.opts.name }));
        ws.onmessage = (e) => {
          if (e.data instanceof ArrayBuffer) {
            const id = new DataView(e.data).getUint16(0, true);
            this._play(id, new Int16Array(e.data, 2));
            return;
          }
          const msg = JSON.parse(e.data);
          if (msg.type === "welcome") {
            clearTimeout(timeout);
            this.id = msg.id;
            this.peers.clear();
            msg.peers.forEach((p) => this._addPeer(p.id, p.name));
            this._emit("joined", { id: this.id, peers: this.getPeers() });
            resolve(this.id);
          } else if (msg.type === "peer_join") {
            this._emit("peerJoin", this._publicPeer(this._addPeer(msg.id, msg.name)));
          } else if (msg.type === "peer_leave") {
            const p = this.peers.get(msg.id);
            if (p) { p.gain.disconnect(); this.peers.delete(msg.id); this._emit("peerLeave", this._publicPeer(p)); }
          } else if (msg.type === "talk_start" || msg.type === "talk_end") {
            const p = this.peers.get(msg.id);
            if (p) {
              p.speaking = msg.type === "talk_start";
              p.nextTime = 0;
              this._emit("speaking", p.id, p.speaking);
            }
          } else if (msg.type === "error") {
            clearTimeout(timeout);
            this._emit("error", msg.message);
            reject(new Error(msg.message));
          }
        };
        ws.onclose = () => {
          this._stopTx(false);
          if (this._leaving) return;
          this._emit("disconnected");
          setTimeout(() => this._connect().catch(() => {}), 2000); // auto-reconnect
        };
        ws.onerror = () => ws.close();
      });
    }

    // ----- sending -----
    startTalking() { if (this.opts.mode === "ptt" && !this.muted) this._startTx(); }
    stopTalking() { if (this.opts.mode === "ptt") this._stopTx(); }
    setMode(mode) { this._stopTx(); this.opts.mode = mode; }
    setMuted(muted) { this.muted = muted; if (muted) this._stopTx(); }
    setDeafened(deafened) { this.deafened = deafened; this.peers.forEach((p) => this._applyGain(p)); }

    _startTx() {
      if (this.transmitting || !this.ws || this.ws.readyState !== WebSocket.OPEN) return;
      this.transmitting = true;
      this.ws.send(JSON.stringify({ type: "talk_start" }));
      this._preroll.forEach((c) => this._sendAudio(c)); // avoids clipping the first syllable in open-mic mode
      this._preroll = [];
      this._emit("speaking", this.id, true);
    }

    _stopTx(notify = true) {
      if (!this.transmitting) return;
      this.transmitting = false;
      if (notify && this.ws && this.ws.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify({ type: "talk_end" }));
      this._emit("speaking", this.id, false);
    }

    _onMic(float32) {
      let sum = 0;
      for (let i = 0; i < float32.length; i++) sum += float32[i] * float32[i];
      this.level = Math.sqrt(sum / float32.length);

      if (this.muted) return;

      if (this.opts.mode === "open") {
        const now = performance.now();
        if (this.level > this.opts.vadThreshold) this._lastVoice = now;
        const active = now - this._lastVoice < this.opts.vadHangoverMs;
        if (active && !this.transmitting) this._startTx();
        else if (!active && this.transmitting) this._stopTx();
        if (!this.transmitting) {
          this._preroll.push(float32);
          if (this._preroll.length > 3) this._preroll.shift();
          return;
        }
      }
      if (this.transmitting) this._sendAudio(float32);
    }

    _sendAudio(float32) {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN || this.ws.bufferedAmount > 64 * 1024) return;
      const down = this._downsample(float32, this.ctx.sampleRate, this.opts.sampleRate);
      const pcm = new Int16Array(down.length);
      for (let i = 0; i < down.length; i++) {
        const s = Math.max(-1, Math.min(1, down[i]));
        pcm[i] = s < 0 ? s * 0x8000 : s * 0x7fff;
      }
      this.ws.send(pcm.buffer);
    }

    _downsample(input, inRate, outRate) {
      if (inRate === outRate) return input;
      const ratio = inRate / outRate;
      const out = new Float32Array(Math.floor(input.length / ratio));
      for (let i = 0; i < out.length; i++) {
        const start = Math.floor(i * ratio);
        const end = Math.min(Math.floor((i + 1) * ratio), input.length);
        let sum = 0;
        for (let j = start; j < end; j++) sum += input[j];
        out[i] = end > start ? sum / (end - start) : 0;
      }
      return out;
    }

    // ----- receiving -----
    _addPeer(id, name) {
      if (this.peers.has(id)) return this.peers.get(id);
      const gain = this.ctx.createGain();
      gain.connect(this.ctx.destination);
      const peer = { id, name, speaking: false, volume: 1, muted: false, x: 0, y: 0, gain, nextTime: 0 };
      this.peers.set(id, peer);
      this._applyGain(peer);
      return peer;
    }

    _play(id, pcm) {
      const peer = this.peers.get(id);
      if (!peer || this.deafened || peer.muted) return;

      const floats = new Float32Array(pcm.length);
      for (let i = 0; i < pcm.length; i++) floats[i] = pcm[i] / 32768;
      const buffer = this.ctx.createBuffer(1, floats.length, this.opts.sampleRate);
      buffer.copyToChannel(floats, 0);
      const src = this.ctx.createBufferSource();
      src.buffer = buffer;
      src.connect(peer.gain);

      // Chain chunks back-to-back per speaker so playback is gapless
      let t = peer.nextTime;
      if (t < this.ctx.currentTime + 0.02) t = this.ctx.currentTime + this.opts.jitter;
      src.start(t);
      peer.nextTime = t + buffer.duration;
    }

    // ----- per-player controls -----
    setPeerVolume(id, volume) { const p = this.peers.get(id); if (p) { p.volume = volume; this._applyGain(p); } }
    setPeerMuted(id, muted) { const p = this.peers.get(id); if (p) { p.muted = muted; this._applyGain(p); } }

    // ----- proximity chat: call these from your game loop when positions change -----
    setPosition(x, y) { this._listener = { x, y }; this.peers.forEach((p) => this._applyGain(p)); }
    setPeerPosition(id, x, y) { const p = this.peers.get(id); if (p) { p.x = x; p.y = y; this._applyGain(p); } }

    _applyGain(p) {
      let level = this.deafened || p.muted ? 0 : p.volume;
      if (this.opts.proximity) {
        const d = Math.hypot(p.x - this._listener.x, p.y - this._listener.y);
        level *= Math.max(0, 1 - d / this.opts.maxDistance);
      }
      p.gain.gain.setTargetAtTime(level, this.ctx.currentTime, 0.05);
    }

    // ----- info for your UI -----
    _publicPeer(p) { return { id: p.id, name: p.name, speaking: p.speaking, volume: p.volume, muted: p.muted }; }
    getPeers() { return [...this.peers.values()].map((p) => this._publicPeer(p)); }
  }

  global.VoiceChat = VoiceChat;
})(window);
