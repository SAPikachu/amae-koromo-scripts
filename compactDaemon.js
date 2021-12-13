const { promisify } = require("util");
const os = require("os");

const redis = require("redis");
const assert = require("assert");
const axios = require("axios").default;

const { wrappedRun } = require("./entryPoint");
const { CouchStorage } = require("./couchStorage");
const { COUCHDB_USER, COUCHDB_PASSWORD, COUCHDB_PROTO, PLAYER_SERVERS, REDIS_HOST, REDIS_PASSWORD } = require("./env");

async function withRetry(func, num = 5, retryInterval = 5000) {
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      return await func();
    } catch (e) {
      console.log(e);
      if (num <= 0 || e.status === 403) {
        throw e;
      }
      console.log(`Retrying (${num})`);
      await new Promise((r) => setTimeout(r, Math.random() * retryInterval));
    }
    num--;
  }
}

Promise.allSettled =
  Promise.allSettled ||
  ((promises) =>
    Promise.all(
      promises.map((p) =>
        p
          .then((v) => ({
            status: "fulfilled",
            value: v,
          }))
          .catch((e) => ({
            status: "rejected",
            reason: e,
          }))
      )
    ));

async function compact({ dbName }) {
  const m = /^p(\d+)_0*(\d+)$/.exec(dbName);
  assert(m);
  const [, mode] = m;
  const URL_BASE = `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${PLAYER_SERVERS[mode]}`;
  const s = new CouchStorage({
    uri: `${URL_BASE}/${dbName}`,
    skipSetup: true,
  });
  await withRetry(() => axios.put(`${URL_BASE}/${dbName}/_revs_limit`, "1"));
  await withRetry(() => s._db.compact({ interval: 200 }));
  for (const view of ["basic", "extended"]) {
    await withRetry(() => axios.post(`${URL_BASE}/${dbName}/_compact/${view}`, {}));
    await new Promise((res) => setTimeout(res, 200));
    while (
      (await withRetry(() => axios.get(`${URL_BASE}/${dbName}/_design/${view}/_info`))).data.view_index
        .compact_running !== false
    ) {
      await new Promise((res) => setTimeout(res, 200));
    }
  }
  s._db.close().catch(() => {});
}

function getCPUInfo() {
  const cpus = os.cpus();

  let user = 0;
  let nice = 0;
  let sys = 0;
  let idle = 0;
  let irq = 0;

  for (const cpu in cpus) {
    if (!cpus.hasOwnProperty(cpu)) continue;
    user += cpus[cpu].times.user;
    nice += cpus[cpu].times.nice;
    sys += cpus[cpu].times.sys;
    irq += cpus[cpu].times.irq;
    idle += cpus[cpu].times.idle;
  }

  const total = user + nice + sys + idle + irq;

  return {
    idle: idle,
    total: total,
  };
}

async function main() {
  const redisClientRaw = redis.createClient({
    host: REDIS_HOST,
    password: REDIS_PASSWORD,
    retry_unfulfilled_commands: true,
  });
  const redisClient = {
    zrevrange: promisify(redisClientRaw.zrevrange.bind(redisClientRaw)),
    zrem: promisify(redisClientRaw.zrem.bind(redisClientRaw)),
    del: promisify(redisClientRaw.del.bind(redisClientRaw)),
    sadd: promisify(redisClientRaw.sadd.bind(redisClientRaw)),
    rename: promisify(redisClientRaw.rename.bind(redisClientRaw)),
  };
  const running = {};
  function getNumRunning() {
    return (
      Object.keys(running)
        .map((x) => running[x])
        .reduce((a, b) => a + b, 0) || 0
    );
  }
  let concurrency = 1;
  let onComplete = null;
  let cpuInfo = getCPUInfo();
  let lastIdleCheck = new Date().getTime();
  let downCooldown = 0;
  let upCooldown = 0;
  function doCompact(dbName, force) {
    const m = /^p(\d+)_0*(\d+)$/.exec(dbName);
    assert(m);
    const [, mode] = m;
    const server = PLAYER_SERVERS[mode];
    if (running[server] && !force) {
      return false;
    }
    running[server] = (running[server] || 0) + 1;
    console.log(dbName);
    Promise.all([
      redisClient.zrem("compactQueue", dbName),
      redisClient.sadd("compactIgnore", dbName),
      compact({ dbName }),
    ])
      .then(() => {
        running[server]--;
        if (!running[server]) {
          delete running[server];
        }
        onComplete();
      })
      .catch((e) => {
        console.error(e);
        process.exit(1);
      });
    return true;
  }
  for (;;) {
    const ts = new Date().getTime();
    if (ts - lastIdleCheck >= 1000) {
      const newCpuInfo = getCPUInfo();
      const idlePercent = (newCpuInfo.idle - cpuInfo.idle) / (newCpuInfo.total - cpuInfo.total);
      lastIdleCheck = ts;
      cpuInfo = newCpuInfo;

      if (upCooldown > 0) {
        upCooldown--;
      }
      if (downCooldown > 0) {
        downCooldown--;
      }
      if (idlePercent < 0.08 + concurrency * 0.02) {
        if (concurrency > 1 && downCooldown <= 0) {
          concurrency--;
          downCooldown = 2;
          upCooldown = 10;
          console.log("Concurrency:", concurrency);
        }
      } else if (idlePercent > 0.2) {
        if (concurrency < Math.min(Math.floor(idlePercent * 10) + 1, 7) && upCooldown <= 0) {
          if (getNumRunning() < concurrency) {
            setTimeout(() => onComplete(), 1100);
          } else {
            concurrency++;
            upCooldown = 10;
            downCooldown = 0;
            console.log("Concurrency:", concurrency);
          }
        }
      }
    }
    const items = await redisClient.zrevrange("compactQueue", 0, 100);
    if (!items.length) {
      await redisClient.rename("compactQueueAlt", "compactQueue").catch(() => {});
      await redisClient.del("compactIgnore");
      await new Promise((res) => setTimeout(res, 5000));
      continue;
    }
    const droppedItems = [];
    await new Promise((resolve) => {
      onComplete = resolve;
      while (getNumRunning() < concurrency && items.length) {
        const dbName = items.shift();
        if (!doCompact(dbName)) {
          droppedItems.push(dbName);
          continue;
        }
      }
      assert(getNumRunning());
      while (getNumRunning() < concurrency && droppedItems.length) {
        const dbName = droppedItems.shift();
        doCompact(dbName, true);
      }
    });
  }
}

if (require.main === module) {
  wrappedRun(main);
}
// vim: sw=2:ts=2:expandtab:fdm=syntax
