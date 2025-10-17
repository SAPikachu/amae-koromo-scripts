const { spawn } = require("child_process");

const { COUCHDB_URL } = require("./couchStorage");

async function stream(url, callback) {
  const p = spawn("dotnet/EfficientRowStreamer/bin/Release/net7.0/linux-x64/publish/EfficientRowStreamer", [], {
    env: {
      ...process.env,
      TARGET_URL: url,
    },
    shell: false,
    stdio: ["ignore", "pipe", "inherit"],
  });
  let readableCallbacks = [];
  p.stdout.on("readable", () => {
    const cb = readableCallbacks;
    readableCallbacks = [];
    cb.forEach((x) => x());
  });
  const closeCallbacks = new Set();
  p.stdout.on("close", () => closeCallbacks.forEach((x) => x()));
  async function readExact(numBytes, allowEof) {
    let buf = null;
    let remaining = numBytes;
    for (;;) {
      const res = p.stdout.read(remaining);
      if (res) {
        if (res.length === numBytes) {
          return res.toString("utf8");
        }
        if (!buf) {
          buf = Buffer.allocUnsafe(numBytes);
        }
        remaining -= res.copy(buf, numBytes - remaining);
        if (remaining === 0) {
          return buf.toString("utf8");
        }
        continue;
      }
      if (res === "" || p.exitCode !== null || p.killed || p.stdout.destroyed) {
        if (allowEof) {
          return null;
        }
        throw new Error("Unexpected end of stream");
      }
      await new Promise((resolve, reject) => {
        closeCallbacks.add(reject);
        readableCallbacks.push(() => {
          resolve();
          closeCallbacks.delete(reject);
        });
      });
    }
  }
  let timeoutToken;
  let latestTs = new Date().getTime();
  const timeoutTick = function () {
    if (p.exitCode !== null) {
      return;
    }
    if (new Date().getTime() - latestTs > 60000) {
      p.kill();
      p.stdout.destroy(new Error("timeout"));
    }
    timeoutToken = setTimeout(timeoutTick, 5000);
  };
  timeoutTick();
  for (;;) {
    const length = await readExact(10, true);
    if (!length) {
      break;
    }
    const parsedLen = parseInt(length.trim(), 10);
    if (!parsedLen) {
      throw new Error("Invalid chunk length: " + length);
    }
    const data = await readExact(parsedLen + 2);
    try {
      await callback(JSON.parse(data));
    } catch (e) {
      // console.log(`[${length}]`, data, data.length);
      throw e;
    }
    latestTs = new Date().getTime();
  }
  await new Promise((resolve, reject) => {
    if (p.exitCode !== null) {
      resolve();
    }
    const t = setTimeout(() => reject(new Error("Timeout when waiting for process to exit")), 30000);
    t.unref();
    p.once("exit", () => {
      resolve();
      clearTimeout(t);
    });
  });
  clearTimeout(timeoutToken);
  if (p.killed || p.exitCode !== 0) {
    throw new Error("Process didn't exit cleanly");
  }
}
function streamView(docName, viewName, params, callback) {
  return stream(
    `${COUCHDB_URL}${params._suffix || ""}/_design/${docName}/_view/${viewName}?${Object.entries(params)
      .map(([k, v]) => `${k}=${encodeURIComponent(typeof v === "object" ? JSON.stringify(v) : v)}`)
      .join("&")}`,
    callback
  );
}
function streamAllDocs(params, callback) {
  return stream(
    `${params._url || COUCHDB_URL}${params._suffix || ""}/_all_docs?${Object.entries(params)
      .map(([k, v]) => `${k}=${encodeURIComponent(typeof v === "object" ? JSON.stringify(v) : v)}`)
      .join("&")}`,
    callback
  );
}

module.exports.streamView = streamView;
module.exports.streamAllDocs = streamAllDocs;
// vim: sw=2:ts=2:expandtab:fdm=syntax
