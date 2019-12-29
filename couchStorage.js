const PouchDB = require("pouchdb");
const xz = require("xz");
const assert = require("assert");
const moment = require("moment");

const { COUCHDB_USER, COUCHDB_PASSWORD, COUCHDB_URL } = require("./env");


class CouchStorage {
  constructor ({ uri = COUCHDB_URL, timeout = 30000, suffix = "" } = {}) {
    this._db = new PouchDB(uri + suffix, {
      fetch (url, opts) {
        opts.timeout = opts.timeout || timeout;
        return PouchDB.fetch(url, opts);
      }
    });
    this._savedDefinitions = {};
  }
  /**
   *
   * @param {*} gameInfo
   * @param {Buffer} recordData
   */
  async compressData (raw) {
    const compressor = new xz.Compressor({ preset: 8 });
    const compressedData = Buffer.concat([
      await compressor.updatePromise(raw),
      await compressor.finalPromise(),
    ]);
    compressor.engine.close();
    return compressedData;
  }
  async saveGame (gameInfo, version) {
    assert(gameInfo.uuid);
    if (gameInfo.toJSON) {
      gameInfo = gameInfo.toJSON();
    }
    await this.saveDoc({
      _id: gameInfo.uuid,
      version: 2,
      data_version: version,
      updated: moment.utc().valueOf(),
      ...gameInfo,
    });
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
  async saveDoc (doc) {
    let rev = undefined;
    try {
      const existingRecord = await this._db.get(doc._id);
      rev = existingRecord._rev;
    } catch (e) {
      if (!e.status || e.status !== 404) {
        throw e;
      }
    }
    try {
      await this._db.put({
        ...doc,
        _rev: rev,
      });
    } catch (e) {
      if (!e.status || e.status !== 409) {
        throw e;
      }
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
  async findNonExistentRecords (ids) {
    const idSet = new Set(ids);
    const existingResp = await this._db.query("default/valid_ids", { keys: ids });
    const existingResp2 = await this._db.query("have_valid_round_data/have_valid_round_data", { keys: ids });
    const existingSetPart = new Set(existingResp.rows.map(x => x.key));
    existingResp2.rows.map(x => x.key).filter(x => existingSetPart.has(x)).forEach(x => idSet.delete(x));
    return Array.from(idSet);
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
  async saveRoundData (game, rounds) {
    const newDoc = {
      _id: `roundData-${game.uuid}`,
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
    await this.saveDoc(newDoc);
  }
  async triggerViewRefresh () {
    for (const view of [
      "default/by_time",
      "have_valid_round_data/have_valid_round_data",
      "player_stats_2/player_stats",
      "nicknames/nicknames",
      "player_extended_stats/player_stats",
      "player_extended_stats_test/player_stats",
      "rank_rate_by_seat/rank_rate_by_seat",
      "updated_players/updated_players",
      "fan_stats/fan_stats",
      "highlight_games/highlight_games",
    ]) {
      for (const level of [undefined, 1, 2, 3]) {
        try {
          await this._db.query(view, {
            limit: 1,
            stale: "update_after",
            group_level: level,
          });
        } catch (e) {
          if (e.reason !== "Invalid use of grouping on a map view.") {
            console.log("triggerViewRefresh:", e);
          }
        }
      }
    }
  }
  get db () {
    return this._db;
  }
}

Object.assign(exports, { CouchStorage, COUCHDB_URL, COUCHDB_USER, COUCHDB_PASSWORD });

// vim: sw=2:ts=2:expandtab:fdm=syntax
