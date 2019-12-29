const { wrappedRun } = require("./entryPoint");

const moment = require("moment");

const { CouchStorage } = require("./couchStorage");
const { streamView } = require("./streamView");

const LEVEL_MAX_POINTS = [20, 80, 200, 600, 800, 1000, 1200, 1400, 2000, 2800, 3200, 3600, 4000, 6000, 9000];
const PLAYER_RANKS = "初士杰豪圣魂";

class Level {
  constructor (levelId) {
    const realId = levelId % 10000;
    this._majorRank = Math.floor(realId / 100);
    this._minorRank = realId % 100;
  }
  getMaxPoint () {
    return LEVEL_MAX_POINTS[(this._majorRank - 1) * 3 + this._minorRank - 1];
  }
  getNextLevel () {
    if (this._majorRank === PLAYER_RANKS.length) {
      return this;
    }
    let majorRank = this._majorRank;
    let minorRank = this._minorRank + 1;
    if (minorRank > 3) {
      majorRank++;
      minorRank = 1;
    }
    return (majorRank * 100 + minorRank);
  }
  getPreviousLevel () {
    if (this._majorRank === 1 && this._minorRank === 1) {
      return this;
    }
    let majorRank = this._majorRank;
    let minorRank = this._minorRank - 1;
    if (minorRank < 1) {
      majorRank--;
      minorRank = 3;
    }
    return (majorRank * 100 + minorRank);
  }
  getAdjustedLevelId (score) {
    const maxPoints = this.getMaxPoint();
    const level = this;
    if (maxPoints && score >= maxPoints) {
      return this.getNextLevel();
    } else if (score < 0) {
      if (!maxPoints || level._majorRank === 1 || (level._majorRank === 2 && level._minorRank === 1)) {
        score = 0;
      } else {
        return this.getPreviousLevel();
      }
    }
    return this._majorRank * 100 + this._minorRank;
  }
}

function getAdjustedLevelId (rawLevel) {
  return new Level(rawLevel[0]).getAdjustedLevelId(rawLevel[1] + rawLevel[2]) + 10000;
}

function merge (result, next) {
  Object.keys(next).forEach(function (key) {
    if (typeof next[key] === "object") {
      return;
    }
    if (!(key in result)) {
      result[key] = 0;
    }
    if (key.indexOf("最大") === 0) {
      result[key] = Math.max(result[key], next[key]);
    } else {
      result[key] += next[key];
    }
  });
  return result;
}

async function main () {
  const buckets = {};
  await streamView(
    "all_stats",
    "all_stats",
    {_suffix: "_stats", include_docs: true},
    ({ doc }) => {
      const levelId = getAdjustedLevelId(doc.basic.level);
      const mode = doc.mode_id;
      if (!buckets[mode]) {
        buckets[mode] = {};
      }
      if (!buckets[mode][levelId]) {
        buckets[mode][levelId] = { accum: [0, 0, 0, 0, 0], num_players: 0 };
      }
      doc.basic.accum.forEach((x, i) => buckets[mode][levelId].accum[i] += x);
      buckets[mode][levelId].num_players++;
      merge(buckets[mode][levelId], doc.extended);
    }
  );
  const storage = new CouchStorage({suffix: "_aggregates"});
  await storage.saveDoc({
    _id: "global_statistics",
    type: "globalStatistics",
    updated: moment.utc().valueOf(),
    data: buckets,
  });

}

if (require.main === module) {
  wrappedRun(main);
}
// vim: sw=2:ts=2:expandtab:fdm=syntax
