const { wrappedRun } = require("./entryPoint");

const { CouchStorage } = require("./couchStorage");
const { streamView } = require("./streamView");

const moment = require("moment");
const assert = require("assert");

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

async function generateDeltaRanking(docId, days) {
  const now = moment.utc();
  const cutoff = moment.utc(now).subtract(days, "days");
  const buckets = { 0: {} };
  const bucketsNumGames = { 0: {} };
  for (const suffix of sourceDbs[process.env.DB_SUFFIX || ""]) {
    await streamView(
      "player_delta",
      "player_delta",
      {
        startkey: [cutoff.unix()],
        endkey: [now.unix()],
        reduce: false,
        _suffix: suffix,
      },
      ({ key: [, playerId], value: { mode, delta } }) => {
        buckets[mode] = buckets[mode] || {};
        buckets[mode][playerId] = buckets[mode][playerId] || { delta: 0 };
        buckets[0][playerId] = buckets[0][playerId] || { delta: 0 };
        buckets[mode][playerId].delta += delta;
        buckets[0][playerId].delta += delta;
        bucketsNumGames[mode] = bucketsNumGames[mode] || {};
        bucketsNumGames[mode][playerId] = bucketsNumGames[mode][playerId] || { delta: 0 };
        bucketsNumGames[0][playerId] = bucketsNumGames[0][playerId] || { delta: 0 };
        bucketsNumGames[mode][playerId].delta += 1;
        bucketsNumGames[0][playerId].delta += 1;
      }
    );
  }
  const nicknamesStorage = new CouchStorage({ suffix: "_nicknames" });
  for (const [modeId, data] of Object.entries(buckets)) {
    buckets[modeId] = await enrichPlayers(nicknamesStorage, extractTopBottom(data));
  }
  for (const [modeId, data] of Object.entries(bucketsNumGames)) {
    bucketsNumGames[modeId] = await enrichPlayers(nicknamesStorage, extractTopBottom(data));
    buckets[modeId].num_games = bucketsNumGames[modeId].top;
  }
  const targetStorage = new CouchStorage({ suffix: getProperSuffix("_aggregates") });
  await targetStorage.saveDoc({
    _id: "player_delta_ranking_" + docId,
    type: "deltaRanking",
    updated: now.valueOf(),
    data: buckets,
  });
  await targetStorage.saveDoc({
    _id: "player_num_games_ranking_" + docId,
    type: "numGamesRanking",
    updated: now.valueOf(),
    data: bucketsNumGames,
  });
}

async function main() {
  await generateDeltaRanking("1w", 7);
  await generateDeltaRanking("4w", 28);
}

if (require.main === module) {
  wrappedRun(main);
} else {
  module.exports = {
    generateDeltaRanking,
  };
}
// vim: sw=2:ts=2:expandtab:fdm=syntax
