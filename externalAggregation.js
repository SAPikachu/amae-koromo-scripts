require("ts-node").register({
  dir: __dirname + "/web",
  compilerOptions: {
    module: "commonjs",
    target: "es2018",
  },
});

const { wrappedRun } = require("./entryPoint");

const { CouchStorage } = require("./couchStorage");
const { streamView } = require("./streamView");

const assert = require("assert");

const RANK_DELTA = [15, 5, -5, -15];
const MODE_DELTA = {
  12: [110, 55, 0, 0],
  16: [120, 60, 0, 0],
};

const webMetadata = require("./web/src/data/types/metadata.ts");

function calculateDeltaPoint(score, rank, mode, skipTrimming) {
  const result =
    (skipTrimming ? (x) => x : Math.ceil)((score - 25000) / 1000 + RANK_DELTA[rank]) +
    MODE_DELTA[mode.toString()][rank];
  return result;
}
function calculateExpectedGamePoint(metadata, mode) {
  const ranks = metadata.accum.slice(0, 4);
  metadata.rank_rates = ranks.map(function (x) {
    return x / metadata.count;
  });
  metadata.rank_avg_score = metadata.score_accum.map(function (x, i) {
    return Math.round((x / ranks[i]) * 100);
  });
  const rankDeltaPoints = metadata.rank_avg_score.map((score, rank) => calculateDeltaPoint(score, rank, mode));
  const rankWeightedPoints = rankDeltaPoints.map((point, rank) => point * metadata.rank_rates[rank]);
  metadata.rank_weighted_points = rankWeightedPoints;
  const expectedGamePoint = rankWeightedPoints.reduce((a, b) => a + b, 0);
  return expectedGamePoint;
}
function estimateStableLevel2(metadata, mode) {
  metadata = { ...metadata };
  mode = mode || (metadata.level[0] % 1000 < 600 && metadata.level[1] + metadata.level[2] < 9000 ? 12 : 16);
  const estimatedPoints = calculateExpectedGamePoint(metadata, mode);
  const result = estimatedPoints / (metadata.rank_rates[3] * 15) - 10;
  return result;
}
function calculatePointEfficiency(metadata, mode) {
  metadata = { ...metadata };
  mode = mode || (metadata.level[0] % 1000 < 600 && metadata.level[1] + metadata.level[2] < 9000 ? 12 : 16);
  const estimatedPoints = calculateExpectedGamePoint(metadata, mode);
  return estimatedPoints;
}
const expectedGamePointByRank = (rank) => (metadata, mode) => {
  metadata = { ...metadata };
  mode = mode || 12;
  calculateExpectedGamePoint(metadata, mode);
  return calculateDeltaPoint(metadata.rank_avg_score[rank], rank, mode, true);
};
const sum = (x) => x.reduce((a, b) => a + b, 0);
const convertMetadata = function (value) {
  "use strict";
  var convert_level = function (value) {
    return {
      id: value[0],
      score: value[1],
      delta: value[2],
    };
  };
  var ranks = value.accum.slice(0, 4);
  var result = {
    count: sum(ranks),
    level: value.level && convert_level(value.level),
    max_level: value.max_level && convert_level(value.max_level),
  };
  if (result.level && Math.floor(result.level.id / 10000) === 2) {
    ranks = ranks.slice(0, 3);
  }
  result.rank_rates = ranks.map(function (x) {
    return x / result.count;
  });
  if (value.score_accum) {
    result.rank_avg_score = value.score_accum.slice(0, ranks.length).map(function (x, i) {
      return Math.round((x / ranks[i]) * 100);
    });
  }
  result.avg_rank =
    sum(
      ranks.map(function (x, index) {
        return x * (index + 1);
      })
    ) / result.count;
  result.negative_rate = value.accum[4] / result.count;
  return result;
};

const SETTINGS = {
  default: {
    aggregates: "_aggregates",
    stats: "_stats",
    extraRankings: {
      rank4: { valueFunc: (x) => x.accum[3] / x.count, sort: "asc" },
      rank123: (x) => (x.accum[0] + x.accum[1] + x.accum[2]) / x.count,
      stable_level: estimateStableLevel2,
      point_efficiency: calculatePointEfficiency,
      expected_game_point_0: { valueFunc: expectedGamePointByRank(0), sort: "desc" },
      expected_game_point_1: { valueFunc: expectedGamePointByRank(1), sort: "desc" },
      expected_game_point_2: { valueFunc: expectedGamePointByRank(2), sort: "desc" },
      expected_game_point_3: { valueFunc: expectedGamePointByRank(3), sort: "desc" },
    },
  },
  sanma: {
    aggregates: "_sanma_aggregates",
    stats: "_sanma_stats",
    extraRankings: {
      rank3: { valueFunc: (x) => x.accum[2] / x.count, sort: "asc" },
      stable_level: {
        valueFunc: (metadata, mode) => {
          if (mode.toString() === "0") {
            return 0;
          }
          const converted = convertMetadata(metadata);
          const estimatedLevel = webMetadata.PlayerMetadata.estimateStableLevel(
            converted,
            parseInt(mode.toString(), 10)
          );
          const m = /^(.)(\d)?(\+|-)? ?\((-?[0-9.]+)\)$/.exec(estimatedLevel);
          let key = "初士杰豪圣魂".indexOf(m[1]) * 100000;
          key += parseInt(m[2] || "0", 10) * 10000;
          key += { "+": 6000, "-": 0 }[m[3]] || 3000;
          key += parseFloat(m[4]);
          return key;
        },
        sort: "desc",
      },
    },
  },
  e3: {
    aggregates: "_e3_aggregates",
    stats: "_e3_stats",
  },
};
SETTINGS.e3.extraRankings = SETTINGS.sanma.extraRankings;

const RANKINGS = {
  num_games: (x) => x.count,
  rank1: (x) => x.accum[0] / x.count,
  rank12: (x) => (x.accum[0] + x.accum[1]) / x.count,
  win: { valueFunc: (x) => x.extended.和 / x.extended.count, sort: "desc" },
  win_lose_diff: { valueFunc: (x) => (x.extended.和 - x.extended.放铳) / x.extended.count, sort: "desc" },
  lose: { valueFunc: (x) => x.extended.放铳 / x.extended.count, sort: "asc" },
  win_rev: { valueFunc: (x) => x.extended.和 / x.extended.count, sort: "asc" },
  lose_rev: { valueFunc: (x) => x.extended.放铳 / x.extended.count, sort: "desc" },
  里宝率: { valueFunc: (x) => x.extended.里宝 / x.extended.立直和了, sort: "desc" },
  一发率: { valueFunc: (x) => x.extended.一发 / x.extended.立直和了, sort: "desc" },
  被炸率: { valueFunc: (x) => x.extended.被炸 / x.extended.被自摸, sort: "asc" },
  里宝率_rev: { valueFunc: (x) => x.extended.里宝 / x.extended.立直和了, sort: "asc" },
  一发率_rev: { valueFunc: (x) => x.extended.一发 / x.extended.立直和了, sort: "asc" },
  被炸率_rev: { valueFunc: (x) => x.extended.被炸 / x.extended.被自摸, sort: "desc" },
  avg_rank: {
    valueFunc: (x) =>
      x.accum
        .slice(0, x.accum.length - 1)
        .map((n, i) => (n / x.count) * (i + 1))
        .reduce((a, b) => a + b, 0),
    sort: "asc",
  },
  max_level: (x) => (x.max_level[0] % 10000) * 1000000 + x.max_level[1] + x.max_level[2],
};

async function generateRateRanking() {
  const setting = SETTINGS[process.env.SETTING || "default"];
  Object.assign(RANKINGS, setting.extraRankings);
  const storage = new CouchStorage({ suffix: setting.aggregates || "INVALID" });
  const nicknameStorage = new CouchStorage({ suffix: "_nicknames" });
  const stats = [];
  const timestamp = new Date().getTime();
  await streamView(
    "num_games",
    "num_games",
    { startkey: 300, include_docs: true, _suffix: setting.stats },
    ({ doc }) => {
      const val = doc.basic;
      val.extended = doc.extended;
      val.count = val.accum.slice(0, 4).reduce((a, b) => a + b, 0);
      assert(val.count >= 300);
      stats.push({ key: [doc.account_id, doc.mode_id], value: val });
    }
  );
  assert(stats.length);

  for (const key of Object.keys(RANKINGS)) {
    const settings = typeof RANKINGS[key] === "function" ? { valueFunc: RANKINGS[key], sort: "desc" } : RANKINGS[key];
    const items = stats.map((x) => ({
      key: x.key,
      value: {
        ...x.value,
        rank_key: settings.valueFunc(x.value, x.key[1]),
        id: x.key[0],
      },
    }));
    if (settings.sort === "asc") {
      items.sort((a, b) => a.value.rank_key - b.value.rank_key);
    } else {
      items.sort((a, b) => b.value.rank_key - a.value.rank_key);
    }
    const groups = {};
    for (const item of items) {
      const modeKey = item.key[1].toString();
      groups[modeKey] = groups[modeKey] || [];
      groups[modeKey].push(item.value);
    }
    for (const x of Object.keys(groups)) {
      groups[x] = groups[x].slice(0, 100);
      for (const item of groups[x]) {
        const nicknameDoc = await nicknameStorage.db.get(item.id.toString().padStart(10, "0"));
        let level = item.level;
        let timestamp = 0;
        for (const mode of Object.values(nicknameDoc.modes)) {
          if (mode.timestamp < timestamp) {
            continue;
          }
          if (Math.floor(mode.level[0] / 10000) !== Math.floor(level[0] / 10000)) {
            continue;
          }
          level = mode.level;
          timestamp = mode.timestamp;
        }
        item.level = level;
      }
    }
    await storage.saveDoc({
      _id: `career_ranking_${key}`,
      type: "careerRanking",
      version: 1,
      data: groups,
      updated: timestamp,
    });
  }
}

if (require.main === module) {
  wrappedRun(generateRateRanking);
} else {
  module.exports = {
    generateRateRanking,
  };
}

// vim: sw=2:ts=2:expandtab:fdm=syntax
