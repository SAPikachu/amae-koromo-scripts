const { wrappedRun } = require("./entryPoint");
const { streamAllDocs } = require("./streamView");
const { createFinalReducer } = require("./dbExtension");

const assert = require("assert");
const _ = require("lodash");
const Db = require("better-sqlite3");

function calcNumGames(row) {
  const accum = row.doc.basic.accum;
  const numGames = accum.slice(0, accum.length - 1).reduce((a, b) => a + b, 0);
  return numGames;
}

function getDb(suffix) {
  const db = new Db(`stat.cache/cache${suffix}.sqlite3`);
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = OFF");
  db.exec(`
  CREATE TABLE IF NOT EXISTS state (
    key TEXT NOT NULL PRIMARY KEY ON CONFLICT REPLACE,
    value TEXT NOT NULL
  );
  `);
  const state = Object.freeze({
    get: (key) => JSON.parse(db.prepare("SELECT value FROM state WHERE key = ?").get(key)?.value || "null"),
    set: (key, value) => db.prepare("INSERT INTO state VALUES (?, ?)").run(key, JSON.stringify(value)),
  });
  let insertStm;
  return Object.freeze({
    db,
    state,
    insert(row) {
      if (!insertStm) {
        insertStm = db.prepare("INSERT INTO items VALUES (?, ?, ?, ?)");
      }
      insertStm.run(parseInt(row.doc.account_id, 10), row.doc.mode_id, calcNumGames(row), JSON.stringify(row));
    },
    numRows() {
      return db.prepare("SELECT COUNT(*) FROM items").pluck(true).get();
    },
    verifyNumRows() {
      const oldNumRows = state.get("recordedNumRows");
      const newNumRows = this.numRows();
      assert(typeof newNumRows === "number");
      if (oldNumRows && newNumRows < oldNumRows * 0.99) {
        throw new Error("Unexpected loss of rows");
      }
      console.error(`[${suffix}] verifyNumRows: ${oldNumRows} -> ${newNumRows}`);
      state.set("recordedNumRows", newNumRows);
    },
    clear() {
      db.exec(`
        BEGIN TRANSACTION;
        DELETE FROM state WHERE key = 'updated';
        DROP TABLE IF EXISTS items;
        CREATE TABLE items (
          player_id INTEGER NOT NULL,
          mode_id INTEGER NOT NULL,
          num_games INTEGER NOT NULL,
          data TEXT NOT NULL,
          PRIMARY KEY (player_id, mode_id)
        ) STRICT;
        END TRANSACTION;
      `);
    },
    isFresh() {
      const updated = state.get("updated");
      return updated && updated > Date.now() - 6 * 60 * 60 * 1000;
    },
    _forEachRaw(callback, sql, ...args) {
      for (const row of db.prepare(sql).iterate(...args)) {
        callback(JSON.parse(row.data));
      }
    },
    forEach(callback) {
      return this._forEachRaw(callback, "SELECT data FROM items");
    },
    forEachWithoutMerged(callback) {
      return this._forEachRaw(callback, "SELECT data FROM items WHERE mode_id != 0");
    },
    forEachNumGames(callback, numGames) {
      return this._forEachRaw(callback, "SELECT data FROM items WHERE num_games >= ?", numGames);
    },
  });
}

async function allStatsFromMain(suffix, callback) {
  const basicReduce = await createFinalReducer("_meta_basic", "_design/player_stats_2", "player_stats");
  const extendedReduce = await createFinalReducer("_meta_extended", "_design/player_extended_stats", "player_stats");
  let playerDocs = [];

  function reducePlayerDocs() {
    const modeDocs = playerDocs.filter((x) => x.mode_id !== 0);
    assert(modeDocs.length);
    const docId = `${modeDocs[0].account_id}-0`;
    const merged = {
      _id: docId,
      account_id: modeDocs[0].account_id,
      type: "stats",
      mode_id: 0,
      timestamp: _(modeDocs).map("timestamp").max(),
      updated: Date.now(),
      basic: basicReduce(modeDocs.map((x) => x.basic)),
      extended: extendedReduce(modeDocs.map((x) => x.extended)),
    };
    callback({ id: docId, key: docId, value: {}, doc: merged });
    playerDocs = [];
  }
  await streamAllDocs({ _suffix: suffix, include_docs: true }, (item) => {
    if (item.doc.type !== "stats") {
      return;
    }
    assert(item.doc.account_id);
    assert(item.doc.mode_id !== undefined);
    item.doc.mode_id = parseInt(String(item.doc.mode_id), 10);
    if (item.doc.mode_id === 0) {
      return;
    }

    /*
    const currentId = playerDocs[0]?.account_id;
    if (currentId && item.doc.account_id !== currentId) {
      reducePlayerDocs();
    }
    playerDocs.push(item.doc);
    */
    callback(item);
  });
  if (playerDocs.length) {
    reducePlayerDocs();
  }
}

async function allStatsFromMainAndBuildCache(suffix, callback) {
  const cacheDb = getDb(suffix);
  cacheDb.clear();
  cacheDb.db.exec("BEGIN TRANSACTION");
  const ts = Date.now();
  await allStatsFromMain(suffix, (item) => {
    cacheDb.insert(item);
    callback(item);
  });
  cacheDb.verifyNumRows();
  cacheDb.state.set("updated", ts);
  cacheDb.db.exec(`
    CREATE INDEX IF NOT EXISTS items__num_games ON items (
      num_games
    );
  `);
  cacheDb.db.exec("END TRANSACTION");
  cacheDb.db.close();
}

async function allStats(suffix, callback) {
  const cacheDb = getDb(suffix);
  if (cacheDb.isFresh()) {
    cacheDb.forEach(callback);
    cacheDb.db.close();
    return;
  }
  cacheDb.db.close();
  await allStatsFromMainAndBuildCache(suffix, callback);
}

async function allStatsWithoutMerged(suffix, callback) {
  const cacheDb = getDb(suffix);
  if (cacheDb.isFresh()) {
    cacheDb.forEachWithoutMerged(callback);
    cacheDb.db.close();
    return;
  }
  cacheDb.db.close();
  await allStatsFromMainAndBuildCache(suffix, (item) => {
    if (item.doc.mode_id === 0) {
      return;
    }
    callback(item);
  });
}

async function numGames(suffix, num, callback) {
  const cacheDb = getDb(suffix);
  if (cacheDb.isFresh()) {
    cacheDb.forEachNumGames(callback, num);
    cacheDb.db.close();
    return;
  }
  cacheDb.db.close();
  return await allStats(suffix, (row) => {
    if (calcNumGames(row) < num) {
      return;
    }
    callback(row);
  });
}

async function test() {
  await allStats("_stats", ({ doc }) => {
    assert(doc.account_id);
  });
  await allStatsWithoutMerged("_stats", ({ doc }) => {
    assert(doc.account_id);
    assert(doc.mode_id !== 0);
  });
}

if (require.main === module) {
  wrappedRun(test);
} else {
  module.exports = {
    allStats,
    allStatsWithoutMerged,
    numGames,
  };
}
