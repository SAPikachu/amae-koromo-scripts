const { wrappedRun } = require("./entryPoint");

const { CouchStorage, generateCompressedId } = require("./couchStorage");
const { streamView, streamAllDocs } = require("./streamView");

const moment = require("moment");
const assert = require("assert");
const Db = require("better-sqlite3");
const { default: axios } = require("axios");

const sourceDbs = {
  "": ["_basic", "_gold_basic", "_e4_basic"],
  _sanma: ["_sanma_basic", "_e3_basic"],
};

function getProperSuffix(suffix) {
  return (process.env.DB_SUFFIX || "") + suffix;
}

function extractTopBottom(playerDeltas, numPlayers = 20) {
  const entries = Object.entries(playerDeltas).map(([id, info]) => ({ id: parseInt(id), ...info }));
  entries.sort((a, b) => a.delta - b.delta);
  const bottom = entries.slice(0, numPlayers).filter((x) => x.delta < 0);
  const top = entries.slice(entries.length - numPlayers).filter((x) => x.delta > 0);
  top.reverse();
  return {
    top,
    bottom,
  };
}

const mapLevel = function (rawLevel) {
  return {
    id: rawLevel[0],
    score: rawLevel[1],
    delta: rawLevel[2],
  };
};

async function enrichPlayers(nicknamesStorage, data) {
  async function forEachPlayer(func, obj = data) {
    if (obj.id) {
      await func(obj);
      return;
    }
    for (const value of Object.values(obj)) {
      if (value && typeof value === "object") {
        await forEachPlayer(func, value);
      }
    }
  }
  await forEachPlayer(async (x) => {
    const statDoc = await nicknamesStorage.db.get(`${x.id.toString().padStart(10, "0")}`);
    assert(statDoc.nickname);
    let modeObj;
    for (const suffix of sourceDbs[process.env.DB_SUFFIX || ""]) {
      const key = suffix.replace("_basic", "") || "_";
      const currentModeObj = statDoc.modes[key];
      if (!currentModeObj) {
        continue;
      }
      if (!modeObj || modeObj.timestamp < currentModeObj.timestamp) {
        modeObj = currentModeObj;
      }
    }
    assert(modeObj);
    assert(modeObj.level);
    Object.assign(x, {
      nickname: statDoc.nickname,
      level: mapLevel(modeObj.level),
    });
  });
  return data;
}

async function generateDeltaRanking() {
  const db = new Db(`deltaRanking.cache/deltaRanking${process.env.DB_SUFFIX || ""}.sqlite3`);
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = OFF");
  db.exec(`
  CREATE TABLE IF NOT EXISTS delta_entries (
    start_time INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    mode_id INTEGER NOT NULL CHECK (mode_id > 0),
    delta INTEGER NOT NULL,
    PRIMARY KEY (start_time, player_id) ON CONFLICT IGNORE
  ) STRICT, WITHOUT ROWID;
  CREATE TABLE IF NOT EXISTS state (
    key TEXT NOT NULL PRIMARY KEY ON CONFLICT REPLACE,
    value TEXT NOT NULL
  );
  `);
  const state = Object.freeze({
    get: (key) => JSON.parse(db.prepare("SELECT value FROM state WHERE key = ?").get(key)?.value || "null"),
    set: (key, value) => db.prepare("INSERT INTO state VALUES (?, ?)").run(key, JSON.stringify(value)),
  });
  const insertDeltaEntry = db.prepare("INSERT OR IGNORE INTO delta_entries VALUES (?, ?, ?, ?)");
  const now = moment.utc();
  const limit = now.clone().subtract(28, "days");
  const cutoff = moment.utc(state.get("updated") || limit).subtract(15, "minutes");
  for (const suffix of sourceDbs[process.env.DB_SUFFIX || ""]) {
    db.exec("BEGIN TRANSACTION");
    await streamView(
      "updated",
      "updated",
      {
        startkey: [cutoff.valueOf()],
        reduce: false,
        include_docs: true,
        _suffix: suffix,
      },
      ({ doc }) => {
        if (!doc) {
          throw new Error("Server error, exiting");
          db.close();
          process.exit(1);
        }
        if (!doc.uuid) {
          return;
        }
        var players = {};
        doc.accounts.forEach(function (x) {
          x.seat = x.seat || 0;
          players[x.seat] = { player: x };
        });
        doc.result.players.forEach(function (x) {
          x.seat = x.seat || 0;
          players[x.seat].result = x;
        });
        var playerList = Object.keys(players).map(function (x) {
          return players[x];
        });
        playerList.forEach(function (x) {
          insertDeltaEntry.run(doc.start_time, x.player.account_id, doc.config.meta.mode_id, x.result.grading_score);
        });
      }
    );
    db.exec("END TRANSACTION");
  }
  state.set("updated", now.valueOf());
  db.pragma("wal_checkpoint");
  const nicknamesStorage = new CouchStorage({ suffix: "_nicknames" });
  const targetStorage = new CouchStorage({ suffix: getProperSuffix("_aggregates") });
  let allModes;
  for (const [docId, days] of [
    ["4w", 28],
    ["1w", 7],
    ["3d", 3],
    ["1d", 1],
  ]) {
    const cutoff = now.clone().subtract(days, "days");
    db.exec(`
      DROP TABLE IF EXISTS delta_aggregate;
      CREATE TEMP TABLE delta_aggregate (
        mode_id INTEGER NOT NULL,
        player_id INTEGER NOT NULL,
        delta INTEGER NOT NULL,
        count INTEGER NOT NULL,
        PRIMARY KEY (mode_id, delta, player_id)
      ) STRICT, WITHOUT ROWID;
    `);
    const selectTop = db.prepare(
      `
      SELECT player_id AS id, delta
      FROM delta_aggregate
      WHERE mode_id = ? AND delta > 0
      ORDER BY delta DESC
      LIMIT 20`
    );
    const selectBottom = db.prepare(
      `
      SELECT player_id AS id, delta
      FROM delta_aggregate
      WHERE mode_id = ? AND delta < 0
      ORDER BY delta ASC
      LIMIT 20`
    );
    const selectCount = db.prepare(
      `
      SELECT player_id AS id, count AS delta
      FROM delta_aggregate
      WHERE mode_id = ?
      ORDER BY delta DESC
      LIMIT 20`
    );
    db.prepare(
      `
      WITH source AS (
        SELECT mode_id, player_id, delta FROM delta_entries WHERE start_time BETWEEN ? AND ?
        UNION ALL
        SELECT 0 AS mode_id, player_id, delta FROM source WHERE mode_id != 0
      )
      INSERT INTO delta_aggregate
      SELECT mode_id, player_id, SUM(delta), COUNT(*)
      FROM source
      WHERE true
      GROUP BY mode_id, player_id;`
    ).run(cutoff.unix(), now.unix());
    if (!allModes) {
      allModes = db.prepare("SELECT DISTINCT mode_id FROM delta_aggregate").raw(true).all().flat();
    }
    const buckets = {};
    for (const modeId of allModes) {
      buckets[modeId] = await enrichPlayers(nicknamesStorage, {
        top: selectTop.all(modeId),
        bottom: selectBottom.all(modeId),
        num_games: selectCount.all(modeId),
      });
    }
    await targetStorage.saveDoc({
      _id: "player_delta_ranking_" + docId,
      type: "deltaRanking",
      updated: now.valueOf(),
      data: buckets,
      cache: 450,
    });
  }
  db.prepare("DELETE FROM delta_entries WHERE start_time < ?").run(limit.unix());
  // db.exec("VACUUM");
  db.close();
}

async function main() {
  await generateDeltaRanking();
}

if (require.main === module) {
  wrappedRun(main);
} else {
  module.exports = {
    generateDeltaRanking,
  };
}
// vim: sw=2:ts=2:expandtab:fdm=syntax
