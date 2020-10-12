const { wrappedRun } = require("./entryPoint");

const moment = require("moment");
const assert = require("assert");
const axios = require("axios").default;

const { CouchStorage } = require("./couchStorage");
const { createFinalReducer } = require("./dbExtension");
const { COUCHDB_USER, COUCHDB_PASSWORD, COUCHDB_PROTO, PLAYER_SERVERS } = require("./env");

const STATS_SERVER = process.env.STATS_SERVER || "127.0.0.1:5985";

const SUFFIXES = {
  "12": "_stats",
  "16": "_stats",
}

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
  assert(stats.basic || stats.extended);
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
      `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${PLAYER_SERVERS[mode.toString()]}/p${mode}_${id.toString().padStart(10, "0")}/_design/${type}/_view/${type}?reduce=true`,
    );
    return resp.data.rows[0].value;
  } catch (e) {
    if (!e.response || e.response.status !== 404) {
      throw e;
    }
    return null;
  }
}

async function main() {
  const stateStorage = new CouchStorage({
    uri: `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${STATS_SERVER}/state`,
    skipSetup: false,
  });
  const stateDoc = await stateStorage.getDocWithDefault("cacheStats");
  let seq = stateDoc ? stateDoc.value : "now";
  const basicReduce = await createFinalReducer("_meta_basic", "_design/player_stats_2", "player_stats");
  const extendedReduce = await createFinalReducer("_meta_extended", "_design/player_extended_stats", "player_stats");
  const storages = {}
  for (;;) {
    const resp = await axios.get(
      `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${STATS_SERVER}/_db_updates?feed=longpoll&timeout=30000&limit=25&since=${seq}`,
      { timeout: 60000 }
    );
    let updated = false;
    for (const entry of resp.data.results) {
      if (entry.type !== "updated") {
        continue;
      }
      const m = /^p(\d+)_0*(\d+)$/.exec(entry.db_name);
      if (!m) {
        continue;
      }
      updated = true;
      const [, updatedMode, id] = m;
      console.log(id, updatedMode);
      if (!storages[updatedMode]) {
        assert(SUFFIXES[updatedMode]);
        storages[updatedMode] = new CouchStorage({ suffix: SUFFIXES[updatedMode] });
      }
      const targetStorage = storages[updatedMode];
      const allStats = {
        basic: [],
        extended: [],
      };
      for (const mode of [12, 16]) {
        const basic = await withRetry(() => getPlayerStats(id, mode, "basic"));
        if (!basic) {
          assert(mode.toString() !== updatedMode.toString());
          continue;
        }
        const extended = await withRetry(() => getPlayerStats(id, mode, "extended"));
        assert(!!extended);
        allStats.basic.push(basic);
        allStats.extended.push(extended);
        if (mode.toString() === updatedMode.toString()) {
          await withRetry(() => saveStats({storage: targetStorage, id, mode, stats: {basic, extended}, timestamp: basic.latest_timestamp * 1000}));
        }
      }
      assert(allStats.basic.length);
      assert(allStats.extended.length);
      const reducedStats = {
        basic: basicReduce(allStats.basic),
        extended: extendedReduce(allStats.extended),
      };
      await withRetry(() => saveStats({storage: targetStorage, id, mode: 0, stats: reducedStats, timestamp: reducedStats.basic.latest_timestamp * 1000}));
    }
    seq = resp.data.last_seq;
    if (updated) {
      // Saving state will trigger another useless update, so only save when there is actual update
      await withRetry(() => stateStorage.saveDoc({ _id: "cacheStats", value: seq, timestamp: (new Date()).getTime() }));
    }
  }
}

if (require.main === module) {
  wrappedRun(main);
}
// vim: sw=2:ts=2:expandtab:fdm=syntax
