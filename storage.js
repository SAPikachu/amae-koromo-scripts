const { Storage } = require("@google-cloud/storage");
const GCS_BUCKET = "amae-koromo";

class DataStorage {
  constructor () {
    const storage = new Storage();
    const bucket = storage.bucket(GCS_BUCKET);
    this._bucket = bucket;
  }
  async get (name, defaultValue = undefined) {
    let resp;
    try {
      resp = await this._bucket.file(name).download();
    } catch (e) {
      return defaultValue;
    }
    return JSON.parse(resp[0].toString("utf8"));
  }
  async getRaw (name, defaultValue = undefined) {
    let resp;
    try {
      resp = await this._bucket.file(name).download();
    } catch (e) {
      return defaultValue;
    }
    return resp[0];
  }
  async set (name, value) {
    return await this._bucket.file(name).save(Buffer.from(JSON.stringify(value), "utf8"), {
      resumable: false,
      contentType: "application/json",
      metadata: {
        cacheControl: "public, max-age=60",
      },
    });
  }
  async setRaw (name, value) {
    return await this._bucket.file(name).save(value, {
      resumable: false,
      contentType: "application/octet-stream",
    });
  }
}
exports.DataStorage = DataStorage;
