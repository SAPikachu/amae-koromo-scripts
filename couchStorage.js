const PouchDB = require("pouchdb");
// const xz = require("xz");
const assert = require("assert");
const moment = require("moment");

const BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const bs62 = require('base-x')(BASE62);

const { COUCHDB_USER, COUCHDB_PASSWORD, COUCHDB_URL } = require("./env");

const MODE_GAME = "GAME";

class CouchStorage {
  constructor ({ uri = COUCHDB_URL, timeout = 60000, suffix = "", mode = CouchStorage.DEFAULT_MODE, skipSetup = true } = {}) {
    assert(!mode || mode === MODE_GAME);
    this._timeout = timeout;
    this._mode = mode
    if (mode === MODE_GAME) {
      this._db = new PouchDB(uri + suffix + "_basic", {
        fetch: this._fetch.bind(this),
        skip_setup: skipSetup,
      });
      this._dbExtended = new PouchDB(uri + suffix + "_extended", {
        fetch: this._fetch.bind(this),
        skip_setup: skipSetup,
      });
    } else {
      this._db = new PouchDB(uri + suffix, {
        fetch: this._fetch.bind(this),
        skip_setup: skipSetup,
      });
    }
    this._savedDefinitions = {};
  }
  _fetch (url, opts) {
    opts.timeout = opts.timeout || this._timeout;
    return PouchDB.fetch(url, opts);
  }
  /**
   *
   * @param {*} gameInfo
   * @param {Buffer} recordData
   *//*
  async compressData (raw) {
    const compressor = new xz.Compressor({ preset: 8 });
    const compressedData = Buffer.concat([
      await compressor.updatePromise(raw),
      await compressor.finalPromise(),
    ]);
    compressor.engine.close();
    return compressedData;
  }*/
  generateCompressedId (uuid, startTime) {
    assert(typeof startTime === "number");
    assert(startTime < 0x0ffffffff, "startTime is out of range");
    const m = /^(?:\d{6}-)?([0-9a-f]{8})/i.exec(uuid);
    assert(m, "Invalid UUID");
    const buf = Buffer.allocUnsafe(8);
    buf.writeUInt32BE(startTime, 0);
    buf.write(m[1], 4, 4, "hex");
    return bs62.encode(buf);
  }
  getIdForDoc (doc) {
    assert(doc.start_time);

    if (doc.uuid) {
      return this.generateCompressedId(doc.uuid, doc.start_time);
    } else if (doc.game && doc.game._id) {
      return "r-" + this.generateCompressedId(doc.game._id, doc.start_time);
    }
    throw new Error("Unrecognized doc");
  }
  async saveGame (gameInfo, version, batch) {
    assert(this._mode === MODE_GAME);
    assert(gameInfo.uuid);
    if (gameInfo.toJSON) {
      gameInfo = gameInfo.toJSON();
    }
    await this.saveDoc({
      _id: this.getIdForDoc(gameInfo),
      version: 2,
      data_version: version,
      updated: moment.utc().valueOf(),
      ...gameInfo,
    }, batch);
  }
  async getDocWithDefault (id, defaultValue = undefined) {
    try {
      return await this._db.get(id);
    } catch (e) {
      if (!e.status || e.status !== 404) {
        throw e;
      }
    }
    return defaultValue;
  }
  async saveDoc (doc, batch) {
    let db = this._db;
    if (this._mode === MODE_GAME && doc.type === "roundData") {
      db = this._dbExtended;
    }
    let rev = undefined;
    if (!batch) {
      try {
        const existingRecord = await db.get(doc._id);
        rev = existingRecord._rev;
      } catch (e) {
        if (!e.status || e.status !== 404) {
          throw e;
        }
      }
    }
    try {
      await db.put({
        ...doc,
        _rev: rev,
      }, { batch: batch ? "ok" : undefined });
    } catch (e) {
      if (!e.status || e.status !== 409) {
        throw e;
      }
      console.warn(`Conflict while saving doc ${doc._id}, retrying`);
      return await this.saveDoc(doc);
    }
  }
  async ensureDataDefinition (version, rawDefinition) {
    if (this._savedDefinitions[version]) {
      return;
    }
    const key = `dataDefinition-${version}`;
    try {
      await this._db.get(key);
    } catch (e) {
      if (e.status !== 404) {
        throw e;
      }
      await this._db.put({
        _id: key,
        version,
        defintion: rawDefinition,
      });
    }
    this._savedDefinitions[version] = true;
    return;
  }
  async findNonExistentRecordsFast (docs) {
    assert(docs.every(x => x.uuid && x.start_time));
    const generatedIds = docs.map(x => ({id: this.getIdForDoc(x), doc: x}));
    while (true) {
      try {
        const resp = await this._db.allDocs({
          keys: generatedIds.map(x => x.id),
        });
        const resp2 = await this._dbExtended.allDocs({
          keys: generatedIds.map(x => "r-" + x.id),
        });
        const respSet = new Set(resp.rows.concat(resp2.rows).filter(x => x.id && !x.error && x.value && !x.value.deleted).map(x => x.id));
        return generatedIds.filter(x => !respSet.has(x.id) || !respSet.has("r-" + x.id)).map(x => x.doc);
      } catch (e) {
        console.error("findNonExistentRecords:", e);
        await new Promise((res) => setTimeout(res, 10000));
      }
    }
  }
  async findNonExistentRecords (ids) {
    assert(this._mode === MODE_GAME);
    const idSet = new Set(ids);
    while (true) {
      try {
        const existingResp = await this._db.query("default/valid_ids", { keys: ids });
        const existingResp2 = await this._dbExtended.query("have_valid_round_data/have_valid_round_data", { keys: ids });
        const existingSetPart = new Set(existingResp.rows.map(x => x.key));
        existingResp2.rows.map(x => x.key).filter(x => existingSetPart.has(x)).forEach(x => idSet.delete(x));
        return Array.from(idSet);
      } catch (e) {
        console.error("findNonExistentRecords:", e);
        await new Promise((res) => setTimeout(res, 10000));
      }
    }
  }
  async getLatestRecord () {
    const resp = await this._db.query("default/by_time", {
      limit: 1,
      descending: true,
      include_docs: true,
      reduce: false,
    });
    return resp.rows[0].doc;
  }
  async getRecordData (uuid) {
    const doc = await this._db.get(uuid);
    const dataDefinition = await this._db.get(`dataDefinition-${doc.data_version}`);
    return {
      dataDefinition: dataDefinition.defintion,
      game: doc,
    };
  }
  async saveRoundData (game, rounds, batch) {
    assert(this._mode === MODE_GAME);
    const newDoc = {
      version: 6,
      type: "roundData",
      game: { _id: game.uuid },
      start_time: game.start_time,
      mode_id: game.config.meta.mode_id,
      accounts: game.accounts.map(x => x.account_id),
      levels: game.accounts.map(x => x.level.id),
      data: rounds,
      updated: moment.utc().valueOf(),
    };
    newDoc._id = this.getIdForDoc(newDoc);
    await this.saveDoc(newDoc, batch);
  }
  async triggerViewRefresh () {
    assert(this._mode === MODE_GAME);
    const promises = [];
    const oldTimeout = this._timeout;
    this._timeout = 5000;
    for (const view of [
      "default/by_time",
      "default/valid_ids",
      "have_valid_round_data/have_valid_round_data",
      "player_stats_2/player_stats",
      "nicknames/nicknames",
      "player_extended_stats/player_stats",
      // "player_extended_stats_test/player_stats",
      "rank_rate_by_seat/rank_rate_by_seat",
      "updated_players/updated_players",
      "fan_stats/fan_stats",
      "highlight_games/highlight_games",
    ]) {
      for (const level of [undefined, 1, 2, 3]) {
        for (const db of [this._db, this._dbExtended]) {
          promises.push(db.query(view, {
            limit: 1,
            stale: "update_after",
            group_level: level,
          }).catch(e => {
            if (
              e.reason !== "Invalid use of grouping on a map view." &&
              e.type !== "request-timeout" &&
              e.reason !== "missing_named_view" &&
              e.reason !== "missing" &&
              e.reason !== "deleted"
            ) {
              console.log("triggerViewRefresh:", e);
            }
          }));
        }
      }
    }
    try {
      await Promise.all(promises);
    } finally {
      this._timeout = oldTimeout;
    }
  }
  get db () {
    return this._db;
  }
}

Object.assign(exports, { CouchStorage, MODE_GAME, COUCHDB_URL, COUCHDB_USER, COUCHDB_PASSWORD });

// vim: sw=2:ts=2:expandtab:fdm=syntax
