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

const webLevel = require("./web/src/data/types/level.ts");

const assert = require("assert");

const SETTINGS = {
  default: {
    aggregates: "_aggregates",
    stats: ["_stats", "_e4_stats"],
  },
  sanma: {
    aggregates: "_sanma_aggregates",
    stats: ["_sanma_stats", "_e3_stats"],
  },
};
const RANKINGS = {
  max_level_global: (x) => {
    const level = new webLevel.Level(x.max_level[0]);
    const score = x.max_level[1] + x.max_level[2];
    const adjustedLevel = level.getAdjustedLevel(score);
    const adjustedScore = adjustedLevel.isSame(level)
      ? level.getVersionAdjustedScore(score)
      : adjustedLevel.getStartingPoint();
    return (
      (adjustedLevel._majorRank * 100 + adjustedLevel._minorRank) * 1000000 + adjustedScore + 1 / x.latest_timestamp
    );
  },
};

function addField(dst, src, name) {
  assert(dst[name] !== undefined);
  assert(src[name] !== undefined);
  if (Array.isArray(dst[name])) {
    dst[name].forEach((_, i) => (dst[name][i] += src[name][i]));
  } else {
    dst[name] += src[name];
  }
}

async function generateMaxLevelRanking() {
  const setting = SETTINGS[process.env.SETTING || "default"];
  const storage = new CouchStorage({ suffix: setting.aggregates || "INVALID" });
  const nicknameStorage = new CouchStorage({ suffix: "_nicknames" });
  const stats = [];
  const timestamp = new Date().getTime();
  for (const suffix of setting.stats) {
    await streamView("num_games", "num_games", { include_docs: true, _suffix: suffix }, ({ doc }) => {
      if (!doc) {
        console.error("Streaming view failed");
        process.exit(1);
      }
      if (!doc.basic || !doc.extended) {
        console.log(`Invalid doc: ${doc._id}`, doc);
        return;
      }
      if (!doc.basic.max_level || doc.basic.max_level[0] % 10000 < 500) {
        return;
      }
      const val = doc.basic;
      val.count = val.accum.slice(0, 4).reduce((a, b) => a + b, 0);
      val.extended = { count: doc.extended.count };
      stats.push({ key: [doc.account_id, doc.mode_id], value: val });
    });
  }
  assert(stats.length);

  for (const key of Object.keys(RANKINGS)) {
    for (const minGames of [undefined, 600, 1000, 2500, 5000, 10000]) {
      if (global.gc) {
        global.gc();
      }

      const suffix = minGames ? `_${minGames}` : "";
      const settings = typeof RANKINGS[key] === "function" ? { valueFunc: RANKINGS[key], sort: "desc" } : RANKINGS[key];
      const items = stats
        .filter((x) => !minGames || x.value.count >= minGames)
        .map((x) => ({
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
        if (modeKey !== "0") {
          continue;
        }
        groups[modeKey] = groups[modeKey] || [];
        groups[modeKey].push(item.value);
      }
      for (const x of Object.keys(groups)) {
        const dedup = {};
        let numItems = 0;
        groups[x] = groups[x].filter((item) => {
          if (dedup[item.id]) {
            ["accum", "score_accum", "count"].forEach((key) => addField(dedup[item.id], item, key));
            dedup[item.id].extended.count += item.extended.count;
            if (item.latest_timestamp > dedup.latest_timestamp) {
              dedup[item.id].level = item.level;
              dedup[item.id].nickname = item.nickname;
            }
            return false;
          }
          if (numItems >= 100) {
            return false;
          }
          dedup[item.id] = item;
          numItems++;
          return true;
        });
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
        _id: `career_ranking_${key}${suffix}`,
        type: "careerRanking",
        version: 1,
        data: groups,
        updated: timestamp,
      });
    }
  }
}

if (require.main === module) {
  wrappedRun(generateMaxLevelRanking);
} else {
  module.exports = {
    generateMaxLevelRanking,
  };
}

// vim: sw=2:ts=2:expandtab:fdm=syntax
