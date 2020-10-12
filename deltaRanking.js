const { wrappedRun } = require("./entryPoint");

const { CouchStorage } = require("./couchStorage");
const { streamView } = require("./streamView");

const moment = require("moment");
const assert = require("assert");

function extractTopBottom (playerDeltas, numPlayers = 20) {
  const entries = Object.entries(playerDeltas).map(([id, info]) => ({id: parseInt(id), ...info}));
  entries.sort((a, b) => a.delta - b.delta);
  const bottom = entries.slice(0, numPlayers).filter(x => x.delta < 0);
  const top = entries.slice(entries.length - numPlayers).filter(x => x.delta > 0);
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

async function enrichPlayers(statsStorage, modeId, data) {
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
    const statDoc = await statsStorage.db.get(`${x.id}-${modeId}`);
    assert.equal(statDoc.account_id.toString(), x.id.toString());
    assert.equal(statDoc.mode_id.toString(), modeId.toString());
    Object.assign(x, {
      nickname: statDoc.basic.nickname,
      level: mapLevel(statDoc.basic.level),
    });
  });
  return data;
}

async function generateDeltaRanking (docId, days) {
  const now = moment.utc();
  const cutoff = moment.utc(now).subtract(days, "days");
  const buckets = { 0: {} };
  await streamView(
    "player_delta", "player_delta",
    {
      startkey: [cutoff.unix()],
      endkey: [now.unix()],
      reduce: false,
      _suffix: "_basic",
    },
    ({ key: [, playerId], value: { mode, delta }}) => {
      buckets[mode] = buckets[mode] || {};
      buckets[mode][playerId] = buckets[mode][playerId] || { delta: 0 };
      buckets[0][playerId] = buckets[0][playerId] || { delta: 0 };
      buckets[mode][playerId].delta += delta;
      buckets[0][playerId].delta += delta;
    }
  );
  // const storage = new CouchStorage({mode: MODE_GAME});
  const statsStorage = new CouchStorage({ suffix: "_stats" });
  for (const [modeId, data] of Object.entries(buckets)) {
    buckets[modeId] = await enrichPlayers(statsStorage, parseInt(modeId), extractTopBottom(data));
  }
  const targetStorage = new CouchStorage({suffix: "_aggregates"});
  await targetStorage.saveDoc({
    _id: docId,
    type: "deltaRanking",
    updated: now.valueOf(),
    data: buckets,
  });
}

async function main () {
  await generateDeltaRanking("player_delta_ranking_1w", 7);
  await generateDeltaRanking("player_delta_ranking_4w", 28);
}

if (require.main === module) {
  wrappedRun(main);
} else {
  module.exports = {
    generateDeltaRanking,
  };
}
// vim: sw=2:ts=2:expandtab:fdm=syntax
