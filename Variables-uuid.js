const CFG = {
  id: '',

  chunk: 64 * 1024,
  dnPack: 32 * 1024,
  dnTail: 512,
  dnMs: 0,

  upPack: 16 * 1024,
  upQMax: 256 * 1024,

  maxED: 8 * 1024,
  concur: 4
};

const hex = c => (c > 64 ? c + 9 : c) & 0xF;

const dec = new TextDecoder();

/* =========================
   UUID
========================= */

const parseUUID = uuid => {
  const out = new Uint8Array(16);

  for (let i = 0, p = 0, c, h; i < 16; i++) {
    c = uuid.charCodeAt(p++);
    c === 45 && (c = uuid.charCodeAt(p++));

    h = hex(c);

    c = uuid.charCodeAt(p++);
    c === 45 && (c = uuid.charCodeAt(p++));

    out[i] = (h << 4) | hex(c);
  }

  return out;
};

let uuidBytes = new Uint8Array(16);

const setUUID = uuid => {
  CFG.id = uuid;
  uuidBytes = parseUUID(uuid);
};

const matchID = c => {
  for (let i = 0; i < 16; i++) {
    if (c[i + 1] !== uuidBytes[i]) return false;
  }

  return true;
};

/* =========================
   Export
========================= */

export default {
  async fetch(req, env) {

    // 从 Worker Variables 读取 UUID
    if (env.UUID && env.UUID !== CFG.id) {
      setUUID(env.UUID);
    }

    return req.headers.get('Upgrade')?.toLowerCase() === 'websocket'
      ? ws(req)
      : new Response('Hello world!');
  }
};

/* =========================
   地址
========================= */

const addr = (t, b) =>
  t === 1
    ? `${b[0]}.${b[1]}.${b[2]}.${b[3]}`
    : t === 3
      ? dec.decode(b)
      : `[${Array.from(
          { length: 8 },
          (_, i) =>
            ((b[i * 2] << 8) | b[i * 2 + 1]).toString(16)
        ).join(':')}]`;

const parseAddr = (b, o, t) => {
  const l =
    t === 3
      ? b[o++]
      : t === 1
        ? 4
        : t === 4
          ? 16
          : null;

  if (l === null) return null;

  const n = o + l;

  return n > b.length
    ? null
    : {
        targetAddrBytes: b.subarray(o, n),
        dataOffset: n
      };
};

const vless = c => {
  if (c.length < 24 || !matchID(c)) return null;

  let o = 19 + c[17];

  const p = (c[o] << 8) | c[o + 1];

  let t = c[o + 2];

  if (t !== 1) t += 1;

  const a = parseAddr(c, o + 3, t);

  return a
    ? {
        addrType: t,
        ...a,
        port: p
      }
    : null;
};

/* =========================
   connect
========================= */

const sprout = (
  f,
  h,
  p,
  s = f.connect({
    hostname: h,
    port: p
  })
) => s.opened.then(() => s);

const raceSprout = (f, h, p) => {

  if (!f?.connect)
    return Promise.reject(
      new Error('connect unavailable')
    );

  if (CFG.concur <= 1)
    return sprout(f, h, p);

  const ts = Array(CFG.concur)
    .fill()
    .map(() => sprout(f, h, p));

  return Promise.any(ts).then(w => {

    ts.forEach(t =>
      t.then(
        s => s !== w && s.close(),
        () => {}
      )
    );

    return w;
  });
};

/* =========================
   Queue
========================= */

const mkQ = (
  cap,
  qCap = cap,
  itemsMax = Math.max(1, qCap >> 8)
) => {

  let q = [];
  let h = 0;
  let qB = 0;
  let buf = null;

  const trim = () => {
    h > 32 &&
      h * 2 >= q.length &&
      ((q = q.slice(h)), (h = 0));
  };

  const take = () => {

    if (h >= q.length) return null;

    const d = q[h];

    q[h++] = undefined;

    qB -= d.byteLength;

    trim();

    return d;
  };

  return {

    get bytes() {
      return qB;
    },

    get size() {
      return q.length - h;
    },

    get empty() {
      return h >= q.length;
    },

    clear() {
      q = [];
      h = 0;
      qB = 0;
    },

    sow(d) {

      const n = d?.byteLength || 0;

      if (!n) return 1;

      if (
        qB + n > qCap ||
        q.length - h >= itemsMax
      )
        return 0;

      q.push(d);

      qB += n;

      return 1;
    },

    bundle(d) {

      d ||= take();

      if (
        !d ||
        h >= q.length ||
        d.byteLength >= cap
      )
        return [d, 0];

      let n = d.byteLength;

      let e = h;

      while (e < q.length) {

        const x = q[e];

        const nn = n + x.byteLength;

        if (nn > cap) break;

        n = nn;

        e++;
      }

      if (e === h) return [d, 0];

      const out = (buf ||= new Uint8Array(cap));

      out.set(d);

      for (let o = d.byteLength; h < e;) {

        const x = q[h];

        q[h++] = undefined;

        qB -= x.byteLength;

        out.set(x, o);

        o += x.byteLength;
      }

      trim();

      return [out.subarray(0, n), 1];
    }
  };
};

/* =========================
   Downlink
========================= */

const mkDn = w => {

  const cap = CFG.dnPack;

  const tail = CFG.dnTail;

  const low = Math.max(4096, tail << 3);

  let pb = new Uint8Array(cap);

  let p = 0;

  let tp = 0;

  let mq = 0;

  let gen = 0;

  let qk = 0;

  let qr = 0;

  const reap = () => {

    tp && clearTimeout(tp);

    tp = 0;

    mq = 0;

    if (!p) return;

    w.send(pb.subarray(0, p).slice());

    pb = new Uint8Array(cap);

    p = 0;

    qr = 0;
  };

  const ripen = () => {

    if (tp || mq) return;

    mq = 1;

    qk = gen;

    queueMicrotask(() => {

      mq = 0;

      if (!p || tp) return;

      if (cap - p < tail) return reap();

      tp = setTimeout(() => {

        tp = 0;

        if (!p) return;

        if (cap - p < tail) return reap();

        if (
          qr < 2 &&
          (gen !== qk || p < low)
        ) {
          qr++;

          qk = gen;

          return ripen();
        }

        reap();

      }, Math.max(CFG.dnMs, 1));
    });
  };

  return {

    send(u) {

      let o = 0;

      let n = u?.byteLength || 0;

      if (!n) return;

      while (o < n) {

        if (!p && n - o >= cap) {

          const m = Math.min(cap, n - o);

          w.send(
            o || m !== n
              ? u.subarray(o, o + m)
              : u
          );

          o += m;

          continue;
        }

        const m = Math.min(cap - p, n - o);

        pb.set(u.subarray(o, o + m), p);

        p += m;

        o += m;

        gen++;

        if (
          p === cap ||
          cap - p < tail
        )
          reap();
        else
          ripen();
      }
    },

    reap
  };
};

/* =========================
   Stream
========================= */

const mill = async (rd, w) => {

  const r = rd.getReader({
    mode: 'byob'
  });

  const tx = mkDn(w);

  let buf = new ArrayBuffer(CFG.chunk);

  try {

    for (;;) {

      const {
        done,
        value: v
      } = await r.read(
        new Uint8Array(
          buf,
          0,
          CFG.chunk
        )
      );

      if (done) break;

      if (!v?.byteLength) continue;

      if (
        v.byteLength >=
        (CFG.chunk >> 1)
      ) {

        tx.reap();

        w.send(v);

        buf = new ArrayBuffer(CFG.chunk);

      } else {

        tx.send(v.slice());

        buf = v.buffer;
      }
    }

    tx.reap();

  } catch {} finally {

    try {
      tx.reap();
    } catch {}

    try {
      r.releaseLock();
    } catch {}
  }
};

/* =========================
   WebSocket
========================= */

const ws = async req => {

  const [client, server] =
    Object.values(new WebSocketPair());

  server.accept({
    allowHalfOpen: true
  });

  server.binaryType = 'arraybuffer';

  const fetcher = req.fetcher;

  const edStr =
    req.headers.get(
      'sec-websocket-protocol'
    );

  const ed =
    edStr &&
    edStr.length <=
      (CFG.maxED * 4) / 3 + 4
      ? Uint8Array.fromBase64(edStr, {
          alphabet: 'base64url'
        })
      : null;

  let curW = null;

  let sock = null;

  let closed = false;

  let busy = false;

  const uq = mkQ(
    CFG.upPack,
    CFG.upQMax,
    CFG.upQMax >> 8
  );

  const wither = () => {

    if (closed) return;

    closed = true;

    uq.clear();

    try {
      curW?.releaseLock();
    } catch {}

    try {
      sock?.close();
    } catch {}

    try {
      server.close();
    } catch {}
  };

  const toU8 = d =>
    d instanceof Uint8Array
      ? d
      : ArrayBuffer.isView(d)
        ? new Uint8Array(
            d.buffer,
            d.byteOffset,
            d.byteLength
          )
        : new Uint8Array(d);

  const sow = d => {

    const u = toU8(d);

    const n = u.byteLength;

    if (!n) return 1;

    if (uq.sow(u)) return 1;

    wither();

    return 0;
  };

  const thresh = async () => {

    if (busy || closed) return;

    busy = true;

    try {

      for (;;) {

        if (closed) break;

        if (!sock) {

          const [d] = uq.bundle();

          if (!d) break;

          const r = vless(d);

          if (!r) throw wither();

          server.send(
            new Uint8Array([d[0], 0])
          );

          const host = addr(
            r.addrType,
            r.targetAddrBytes
          );

          const port = r.port;

          const payload =
            d.subarray(r.dataOffset);

          sock = await raceSprout(
            fetcher,
            host,
            port
          );

          if (!sock)
            throw wither();

          curW =
            sock.writable.getWriter();

          const [first] =
            uq.bundle(payload);

          first?.byteLength &&
            (await curW.write(first));

          mill(
            sock.readable,
            server
          ).finally(() => wither());

          continue;
        }

        const [d] = uq.bundle();

        if (!d) break;

        await curW.write(d);
      }

    } catch {

      wither();

    } finally {

      busy = false;

      !uq.empty &&
        !closed &&
        queueMicrotask(thresh);
    }
  };

  if (ed && sow(ed))
    thresh();

  server.addEventListener(
    'message',
    e => {
      closed ||
        (sow(e.data) && thresh());
    }
  );

  server.addEventListener(
    'close',
    () => wither()
  );

  server.addEventListener(
    'error',
    () => wither()
  );

  return new Response(null, {
    status: 101,
    webSocket: client,
    headers: {
      'Sec-WebSocket-Extensions': ''
    }
  });
};
