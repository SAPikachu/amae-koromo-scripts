const { wrappedRun } = require("./entryPoint");

const { promisify } = require("util");

const moment = require("moment");
const assert = require("assert");
const redis = require("redis");
const axios = require("axios").default;

const { CouchStorage } = require("./couchStorage");
const { createFinalReducer } = require("./dbExtension");
const { COUCHDB_USER, COUCHDB_PASSWORD, COUCHDB_PROTO, PLAYER_SERVERS, REDIS_HOST, REDIS_PASSWORD } = require("./env");

const SUFFIXES = {
  9: null,
  12: "_stats",
  16: "_stats",
  22: "_sanma_stats",
  24: "_sanma_stats",
  26: "_sanma_stats",
  15: "_e4_stats",
  11: "_e4_stats",
  8: null,
  25: "_e3_stats",
  23: "_e3_stats",
  21: "_e3_stats",
};

const MODE_GROUPS = {};

[
  [12, 16],
  [22, 24, 26],
  [21, 23, 25],
  [11, 15]
].forEach((group) => group.forEach((id) => (MODE_GROUPS[id] = group)));

async function withRetry(func, num = 5, retryInterval = 5000) {
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      return await func();
    } catch (e) {
      if (num <= 0 || e.status === 403) {
        throw e;
      }
      console.log(e);
      console.log(`Retrying (${num})`);
      await new Promise((r) => setTimeout(r, Math.random() * retryInterval));
    }
    num--;
  }
}

async function saveStats({ storage, id, mode, stats, timestamp }) {
  assert(stats.basic && stats.extended);
  assert(timestamp);
  const key = `${id}-${mode}`;
  let doc = {};
  try {
    doc = await storage.db.get(key);
  } catch (e) {
    if (!e.status || e.status !== 404) {
      throw e;
    }
  }
  Object.assign(doc, {
    _id: key,
    type: "stats",
    account_id: id,
    mode_id: mode,
    timestamp,
    updated: moment.utc().valueOf(),
    ...stats,
  });
  while (true) {
    try {
      return await storage.db.put(doc);
    } catch (e) {
      if (e.type !== "request-timeout") {
        throw e;
      }
    }
  }
}

async function getPlayerStats(id, mode, type) {
  assert(mode.toString() !== "0");
  assert(["basic", "extended"].includes(type));
  try {
    const resp = await axios.get(
      `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${
        PLAYER_SERVERS[mode.toString()]
      }/p${mode}_${id.toString().padStart(10, "0")}/_design/${type}/_view/${type}?reduce=true`
    );
    return resp.data.rows[0].value;
  } catch (e) {
    if (!e.response || e.response.status !== 404) {
      throw e;
    }
    return null;
  }
}

async function run() {
  const basicReduce = await createFinalReducer("_meta_basic", "_design/player_stats_2", "player_stats");
  const extendedReduce = await createFinalReducer("_meta_extended", "_design/player_extended_stats", "player_stats");
  const stateDocName = "cacheStats3";
  const redisClient = redis.createClient({
    host: REDIS_HOST,
    password: REDIS_PASSWORD,
    retry_unfulfilled_commands: true,
  });
  const zadd = promisify(redisClient.zadd.bind(redisClient));
  const bzpopmin = promisify(redisClient.bzpopmin.bind(redisClient));
  const zrem = promisify(redisClient.zrem.bind(redisClient));
  const storages = {};
  for (;;) {
    const resp = await bzpopmin(stateDocName, 0);
    assert(resp);
    const dbName = resp[1];
    await zadd(stateDocName, new Date().getTime(), dbName);
    const m = /^p(\d+)_0*(\d+)$/.exec(dbName);
    assert(m);
    const [, updatedMode, id] = m;
    if (SUFFIXES[updatedMode] === null) {
      await zrem(stateDocName, dbName);
      continue;
    }
    assert(MODE_GROUPS[updatedMode]);
    assert(SUFFIXES[updatedMode]);
    console.log(id, updatedMode);
    if (!storages[SUFFIXES[updatedMode]]) {
      storages[SUFFIXES[updatedMode]] = new CouchStorage({ suffix: SUFFIXES[updatedMode] });
    }
    const targetStorage = storages[SUFFIXES[updatedMode]];
    const allStats = {
      basic: [],
      extended: [],
    };
    for (const mode of MODE_GROUPS[updatedMode]) {
      let basic = await withRetry(() => getPlayerStats(id, mode, "basic"));
      if (!basic) {
        if (mode.toString() !== updatedMode.toString()) {
          continue;
        }
        await new Promise((res) => setTimeout(res, 5000));
        basic = await withRetry(() => getPlayerStats(id, mode, "basic"));
        assert(!!basic, "Extended doc exists but basic doc doesn't exist.");
      }
      const extended = await withRetry(() => getPlayerStats(id, mode, "extended"));
      assert(!!extended);
      allStats.basic.push(basic);
      allStats.extended.push(extended);
      if (mode.toString() === updatedMode.toString()) {
        await withRetry(() =>
          saveStats({
            storage: targetStorage,
            id,
            mode,
            stats: { basic, extended },
            timestamp: basic.latest_timestamp * 1000,
          })
        );
      }
    }
    assert(allStats.basic.length);
    assert(allStats.extended.length);
    const reducedStats = {
      basic: basicReduce(allStats.basic),
      extended: extendedReduce(allStats.extended),
    };
    await withRetry(() =>
      saveStats({
        storage: targetStorage,
        id,
        mode: 0,
        stats: reducedStats,
        timestamp: reducedStats.basic.latest_timestamp * 1000,
      })
    );
    await zrem(stateDocName, dbName);
  }
}

async function main() {
  await run();
}

if (require.main === module) {
  wrappedRun(main);
}
// vim: sw=2:ts=2:expandtab:fdm=syntax
