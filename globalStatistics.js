const { wrappedRun } = require("./entryPoint");

const moment = require("moment");

const { CouchStorage } = require("./couchStorage");
const { streamView } = require("./streamView");

function getProperSuffix(suffix) {
  return (process.env.DB_SUFFIX || "") + suffix;
}

const LEVEL_MAX_POINTS = [20, 80, 200, 600, 800, 1000, 1200, 1400, 2000, 2800, 3200, 3600, 4000, 6000, 9000];
const LEVEL_KONTEN = 7;
const LEVEL_MAX_POINT_KONTEN = 2000;
const PLAYER_RANKS = "初士杰豪圣魂";

class Level {
  constructor(levelId) {
    const realId = levelId % 10000;
    this._majorRank = Math.floor(realId / 100);
    this._minorRank = realId % 100;
    this._numPlayerId = Math.floor(levelId / 10000);
  }
  isKonten() {
    return this._majorRank >= LEVEL_KONTEN - 1;
  }
  getMaxPoint() {
    if (this.isKonten()) {
      return LEVEL_MAX_POINT_KONTEN;
    }
    return LEVEL_MAX_POINTS[(this._majorRank - 1) * 3 + this._minorRank - 1];
  }

  getNextLevel() {
    const level = this.getVersionAdjustedLevel();
    let majorRank = level._majorRank;
    let minorRank = level._minorRank + 1;
    if (minorRank > 3 && !level.isKonten()) {
      majorRank++;
      minorRank = 1;
    }
    if (majorRank === LEVEL_KONTEN - 1) {
      majorRank = LEVEL_KONTEN;
    }
    return new Level(level._numPlayerId * 10000 + majorRank * 100 + minorRank);
  }
  getPreviousLevel() {
    if (this._majorRank === 1 && this._minorRank === 1) {
      return this;
    }
    const level = this.getVersionAdjustedLevel();
    let majorRank = level._majorRank;
    let minorRank = level._minorRank - 1;
    if (minorRank < 1) {
      majorRank--;
      minorRank = 3;
    }
    if (majorRank === LEVEL_KONTEN - 1) {
      majorRank = LEVEL_KONTEN - 2;
    }
    return new Level(level._numPlayerId * 10000 + majorRank * 100 + minorRank);
  }
  getVersionAdjustedLevel() {
    if (this._majorRank !== LEVEL_KONTEN - 1) {
      return this;
    }
    return new Level(this._numPlayerId * 10000 + LEVEL_KONTEN * 100 + 1);
  }
  getVersionAdjustedScore(score) {
    if (this._majorRank === LEVEL_KONTEN - 1) {
      return Math.ceil(score / 100) * 10 + 200;
    }
    return score;
  }
  getAdjustedLevelId(score) {
    score = this.getVersionAdjustedScore(score);
    let level = this.getVersionAdjustedLevel();
    let maxPoints = level.getMaxPoint();
    if (maxPoints && score >= maxPoints) {
      level = level.getNextLevel();
      // maxPoints = level.getMaxPoint();
      // score = level.getStartingPoint();
    } else if (score < 0) {
      if (!maxPoints || level._majorRank === 1 || (level._majorRank === 2 && level._minorRank === 1)) {
        score = 0;
      } else {
        level = level.getPreviousLevel();
        // maxPoints = level.getMaxPoint();
        // score = level.getStartingPoint();
      }
    }
    return level._majorRank * 100 + level._minorRank;
  }
}

function getAdjustedLevelId(rawLevel) {
  const base = Math.floor(rawLevel[0] / 10000) * 10000;
  return new Level(rawLevel[0]).getAdjustedLevelId(rawLevel[1] + rawLevel[2]) + base;
}

function merge(result, next, debugId) {
  Object.keys(next).forEach(function (key) {
    if (typeof next[key] === "object") {
      return;
    }
    if (!(key in result)) {
      // console.log("New key:", key, debugId);
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

async function main() {
  const buckets = {};
  const bucketsYear = {};
  const buckets500 = {};
  const yearCutoff = moment().subtract(1, "year").valueOf();

  function processDoc({ mode, basic, extended, buckets, debugId }) {
    const levelId = getAdjustedLevelId(basic.level);
    if (!buckets[mode]) {
      buckets[mode] = {};
    }
    if (!buckets[mode][levelId]) {
      buckets[mode][levelId] = { accum: [0, 0, 0, 0, 0], num_players: 0 };
    }
    basic.accum.forEach((x, i) => (buckets[mode][levelId].accum[i] += x));
    buckets[mode][levelId].num_players++;
    merge(buckets[mode][levelId], extended, debugId);
  }
  await streamView("all_stats", "all_stats", { _suffix: getProperSuffix("_stats"), include_docs: true }, ({ doc }) => {
    const mode = doc.mode_id;
    processDoc({ mode, basic: doc.basic, extended: doc.extended, buckets, debugId: doc._id });
    if (doc.stats_year?.basic && doc.stats_year?.extended && doc.updated >= yearCutoff) {
      processDoc({
        mode,
        basic: doc.stats_year.basic,
        extended: doc.stats_year.extended,
        buckets: bucketsYear,
        debugId: doc._id,
      });
    }
    if (doc.stats_500?.basic && doc.stats_500?.extended) {
      processDoc({
        mode,
        basic: doc.stats_500.basic,
        extended: doc.stats_500.extended,
        buckets: buckets500,
        debugId: doc._id,
      });
    }
  });
  await new Promise((res) => setTimeout(res, 10000));
  const storage = new CouchStorage({ suffix: getProperSuffix("_aggregates") });
  await storage.saveDoc({
    _id: "global_statistics",
    type: "globalStatistics",
    updated: moment.utc().valueOf(),
    data: buckets,
  });
  await storage.saveDoc({
    _id: "global_statistics_year",
    type: "globalStatistics",
    updated: moment.utc().valueOf(),
    data: bucketsYear,
  });
  await storage.saveDoc({
    _id: "global_statistics_500",
    type: "globalStatistics",
    updated: moment.utc().valueOf(),
    data: buckets500,
  });
  await storage.saveDoc({
    _id: `global_statistics-${new Date().toISOString().slice(0, 10)}`,
    type: "globalStatistics",
    updated: moment.utc().valueOf(),
    data: buckets,
  });
}

if (require.main === module) {
  wrappedRun(main);
}
// vim: sw=2:ts=2:expandtab:fdm=syntax
