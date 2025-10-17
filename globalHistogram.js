require("ts-node").register({
  dir: __dirname + "/web",
  compilerOptions: {
    module: "commonjs",
    target: "es2018",
  },
});

const { wrappedRun } = require("./entryPoint");

const assert = require("assert");
const { incrsummary } = require("@stdlib/stats-incr");
const sp = require("streaming-percentiles");
const _ = require("lodash");
const moment = require("moment");

const { CouchStorage } = require("./couchStorage");
const { allStatsWithoutMerged } = require("./statIterator");

const { createRenderer } = require("./dbExtension");

const webLevel = require("./web/src/data/types/level.ts");

const sourceDbs = {
  "": ["_stats", "_e4_stats", "_gold_stats"],
  _sanma: ["_sanma_stats", "_e3_stats"],
};

function getProperSuffix(suffix) {
  return (process.env.DB_SUFFIX || "") + suffix;
}

function getAdjustedLevelId(rawLevel) {
  const base = Math.floor(rawLevel[0] / 10000) * 10000;
  const adjustedLevel = new webLevel.Level(rawLevel[0]).getAdjustedLevel(rawLevel[1] + rawLevel[2]);
  return (adjustedLevel.isKonten() ? 799 : adjustedLevel._majorRank * 100 + adjustedLevel._minorRank) + base;
}

class Histogram {
  static DEFAULT_NUM_BINS = 120;

  constructor(min, max, numBins = Histogram.DEFAULT_NUM_BINS) {
    assert(!isNaN(min));
    assert(!isNaN(max));
    assert(min < max, `min ${min} is not less than max ${max}`);
    this.min = min;
    this.max = max;
    this.bins = new Array(numBins).fill(0);
  }
  insert(value) {
    if (typeof value !== "number" || isNaN(value)) {
      return;
    }
    if (!(value >= this.min && value <= this.max)) {
      console.log(`Value ${value} is out of range [${this.min}, ${this.max}]`);
    }
    assert(value >= this.min && value <= this.max);
    const bin =
      value === this.max
        ? this.bins.length - 1
        : Math.floor(((value - this.min) / (this.max - this.min)) * this.bins.length);
    this.bins[bin]++;
  }
  toJSON() {
    return {
      min: this.min,
      max: this.max,
      bins: this.bins,
    };
  }
}
class Metric {
  constructor(getter) {
    if (typeof getter === "string") {
      getter = _.property(getter);
    }
    this.getter = getter;
    this.summary = incrsummary();
    this.percentiles = new sp.CKMS_UQ(0.01);
    this.histogramFull = null;
    this.histogramClamped = null;
  }
  update(obj) {
    assert(!this.histogramFull);
    if (obj.count < 100) {
      return;
    }
    const value = this.getter(obj);
    if (typeof value !== "number" || isNaN(value)) {
      return;
    }
    this.summary(value);
    this.percentiles.insert(value);
  }
  updateHistogram(obj) {
    if (obj.count < 100) {
      return;
    }
    const value = this.getter(obj);
    if (typeof value !== "number" || isNaN(value)) {
      return;
    }
    if (!this.histogramFull) {
      const summary = this.summary();
      if (summary.min >= summary.max) {
        return;
      }
      const isDiscrete =
        Number.isInteger(summary.range) && Number.isInteger(summary.sum) && summary.range < Histogram.DEFAULT_NUM_BINS;
      this.histogramFull = new Histogram(
        summary.min,
        isDiscrete ? summary.max + 1 : summary.max,
        Math.min(summary.count, isDiscrete ? summary.range + 1 : Histogram.DEFAULT_NUM_BINS)
      );
      if (summary.count > Histogram.DEFAULT_NUM_BINS * 2) {
        const clippedMin = this.percentiles.quantile(0.02);
        const clippedMax = this.percentiles.quantile(0.98);
        if (clippedMin < clippedMax && !isDiscrete) {
          this.histogramClamped = new Histogram(clippedMin, clippedMax);
        }
      }
    }
    this.histogramFull.insert(value);
    if (this.histogramClamped && this.histogramClamped.min <= value && value <= this.histogramClamped.max) {
      this.histogramClamped.insert(value);
    }
  }
  toJSON() {
    const summary = this.summary();
    return {
      mean: summary.mean,
      histogramFull: this.histogramFull ? this.histogramFull.toJSON() : undefined,
      histogramClamped: this.histogramClamped ? this.histogramClamped.toJSON() : undefined,
    };
  }
}

const METRICS = [];

class MetricGroup {
  constructor() {
    this._metrics = {};
    for (const name of METRICS) {
      this._metrics[name] = new Metric(name);
    }
  }
  update(obj) {
    for (const name of Object.keys(obj)) {
      if (!(name in this._metrics)) {
        if (typeof obj[name] !== "number") {
          continue;
        }
        this._metrics[name] = new Metric(name);
      }
    }
    for (const name of Object.keys(this._metrics)) {
      this._metrics[name].update(obj);
    }
  }
  updateHistogram(obj) {
    for (const name of Object.keys(this._metrics)) {
      this._metrics[name].updateHistogram(obj);
    }
  }
  toJSON() {
    const ret = {};
    for (const name of Object.keys(this._metrics)) {
      ret[name] = this._metrics[name].toJSON();
    }
    return ret;
  }
}

async function main() {
  console.log("Fetching design docs...");
  const render = await createRenderer();
  // const extendedReduce = await createFinalReducer("_meta_extended", "_design/player_extended_stats", "player_stats");

  const buckets = {};
  console.log("Generating summary...");
  for (const db of sourceDbs[process.env.DB_SUFFIX || ""]) {
    await allStatsWithoutMerged(db, ({ doc }) => {
      assert(doc.mode_id);
      if (!doc.mode_id) {
        return;
      }
      const levelId = getAdjustedLevelId(doc.basic.level);
      if (!new webLevel.Level(levelId).isAllowedMode(doc.mode_id)) {
        return;
      }
      if (!buckets[doc.mode_id]) {
        buckets[doc.mode_id] = {
          0: new MetricGroup(),
        };
      }
      const bucket = buckets[doc.mode_id];
      if (!bucket[levelId]) {
        bucket[levelId] = new MetricGroup();
      }
      const id = doc.account_id;
      const rendered = JSON.parse(
        render("list", "player_extended_stats", [{ key: [id, id, id], value: doc.extended }]).body
      );
      rendered.对局数 = doc.basic.accum.slice(0, doc.basic.accum.length - 1).reduce((a, b) => a + b, 0);
      rendered.局收支 =
        ((doc.basic.score_accum.reduce((a, b) => a + b, 0) -
          (Math.floor(doc.basic.level[0] / 10000) === 1 ? 250 : 350) * rendered.对局数) /
          rendered.count) *
        100;
      delete rendered.id;
      bucket["0"].update(rendered);
      bucket[levelId].update(rendered);
    });
  }
  console.log("Generating histograms...");
  for (const db of sourceDbs[process.env.DB_SUFFIX || ""]) {
    await allStatsWithoutMerged(db, ({ doc }) => {
      assert(doc.mode_id);
      if (!doc.mode_id) {
        return;
      }
      assert(doc.mode_id);
      const bucket = buckets[doc.mode_id];
      const levelId = getAdjustedLevelId(doc.basic.level);
      if (!new webLevel.Level(levelId).isAllowedMode(doc.mode_id)) {
        return;
      }
      const id = doc.account_id;
      const rendered = JSON.parse(
        render("list", "player_extended_stats", [{ key: [id, id, id], value: doc.extended }]).body
      );
      rendered.对局数 = doc.basic.accum.slice(0, doc.basic.accum.length - 1).reduce((a, b) => a + b, 0);
      rendered.局收支 =
        ((doc.basic.score_accum.reduce((a, b) => a + b, 0) -
          (Math.floor(doc.basic.level[0] / 10000) === 1 ? 250 : 350) * rendered.对局数) /
          rendered.count) *
        100;
      delete rendered.id;
      bucket["0"].updateHistogram(rendered);
      // bucket[levelId].updateHistogram(rendered);
    });
  }

  const storage = new CouchStorage({ suffix: getProperSuffix("_aggregates") });
  await storage.saveDoc({
    _id: "global_histogram",
    type: "globalHistogram",
    cache: 86400,
    updated: moment.utc().valueOf(),
    data: buckets,
  });
  console.log("Done");
}

if (require.main === module) {
  wrappedRun(main);
}
// vim: sw=2:ts=2:expandtab:fdm=syntax
