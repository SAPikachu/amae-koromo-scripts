const { wrappedRun } = require("./entryPoint");

const { CouchStorage } = require("./couchStorage");
const { streamView } = require("./streamView");

const assert = require("assert");

const RANK_DELTA = [15, 5, -5, -15];
const MODE_DELTA = {
  "12": [110, 55, 0, 0],
  "16": [120, 60, 0, 0]
};


function calculateDeltaPoint (score, rank, mode) {
  const result = Math.ceil((score - 25000) / 1000 + RANK_DELTA[rank]) + MODE_DELTA[mode.toString()][rank];
  return result;
}
function calculateExpectedGamePoint (metadata, mode) {
  const rankDeltaPoints = metadata.rank_avg_score.map((score, rank) =>
    calculateDeltaPoint(score, rank, mode)
  );
  const rankWeightedPoints = rankDeltaPoints.map((point, rank) => point * metadata.rank_rates[rank]);
  const expectedGamePoint = rankWeightedPoints.reduce((a, b) => a + b, 0);
  return expectedGamePoint;
}
function estimateStableLevel2 (metadata, mode) {
  metadata = {...metadata};
  mode = mode || (((metadata.level[0] % 1000) < 600 && metadata.level[1] + metadata.level[2] < 9000) ? 12 : 16);
  const ranks = metadata.accum.slice(0, 4);
  metadata.rank_rates = ranks.map(function (x) { return x / metadata.count; });
  metadata.rank_avg_score = metadata.score_accum.map(function (x, i) {
    return Math.round(x / ranks[i] * 100);
  });
  const estimatedPoints = calculateExpectedGamePoint(metadata, mode);
  const result = estimatedPoints / (metadata.rank_rates[3] * 15) - 10;
  return result;
}

const RANKINGS = {
  num_games: (x) => x.count,
  rank1: (x) => x.accum[0] / x.count,
  rank4: {valueFunc: (x) => x.accum[3] / x.count, sort: "asc"},
  rank12: (x) => (x.accum[0] + x.accum[1]) / x.count,
  rank123: (x) => (x.accum[0] + x.accum[1] + x.accum[2]) / x.count,
  stable_level: estimateStableLevel2,
  win: {valueFunc: (x) => x.extended.和 / x.extended.count, sort: "desc"},
  lose: {valueFunc: (x) => x.extended.放铳 / x.extended.count, sort: "asc"},
  里宝率: {valueFunc: (x) => x.extended.里宝 / x.extended.立直和了, sort: "desc"},
  被炸率: {valueFunc: (x) => x.extended.被炸 / x.extended.被自摸, sort: "asc"},
  avg_rank: {valueFunc: (x) => x.accum.slice(0, 4).map((n, i) => n / x.count * (i + 1)).reduce((a, b) => a + b, 0), sort: "asc"},
};

async function generateRateRanking () {
  const storage = new CouchStorage({suffix: "_aggregates"});
  const stats = [];
  const timestamp = new Date().getTime();
  await streamView(
    "num_games",
    "num_games",
    {startkey: 300, include_docs: true, _suffix: "_stats"},
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
    const settings = typeof RANKINGS[key] === "function" ? {valueFunc: RANKINGS[key], sort: "desc"} : RANKINGS[key];
    const items = stats.map(x => ({
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
    Object.keys(groups).forEach(x => groups[x] = groups[x].slice(0, 100));
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
