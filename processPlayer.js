const { wrappedRun } = require("./entryPoint");
const { CouchStorage, MODE_GAME } = require("./couchStorage");
const { createMapper, createLiveDocGetter } = require("./dbExtension");
const { COUCHDB_USER, COUCHDB_PASSWORD, COUCHDB_PROTO, PLAYER_SERVERS } = require("./env");
const STATE_SERVER = process.env.STATE_SERVER || "couchdb-jt:5985";

function fromEntries(iterable) {
  return [...iterable].reduce((obj, [key, val]) => {
    obj[key] = val;
    return obj;
  }, {});
}

class DocMapper {
  constructor(mapFunctions) {
    this._mapFunctions = mapFunctions;
  }
  static async create() {
    return new DocMapper({
      basic: await createMapper("_meta_basic", "_design/player_stats_2", "player_stats"),
      extended: await createMapper("_meta_extended", "_design/player_extended_stats", "player_stats"),
    });
  }
  process(basic, extended) {
    if (basic.uuid !== extended.game._id) {
      throw new Error(`Mismatched doc: ${basic.uuid} vs ${extended.game._id}`);
    }
    const playerData = {};
    for (const [[playerId, mode], mappedObj] of this._mapFunctions.basic(basic)) {
      if (!mode) {
        continue;
      }
      playerData[playerId] = {
        mode,
        basic: mappedObj,
      };
    }
    for (const [[playerId, mode], mappedObj] of this._mapFunctions.extended(extended)) {
      if (!mode) {
        continue;
      }
      if (!playerData[playerId]) {
        throw new Error(`Unexpected player ${playerId} in extended data`);
      }
      playerData[playerId].extended = mappedObj;
    }
    return playerData;
  }
}

function needUpdate(existing, target) {
  return Object.keys(target).some((k) => {
    if (!(k in existing)) {
      return true;
    }
    const value = target[k];
    if (typeof value !== typeof existing[k]) {
      return true;
    }
    if (typeof value === "object") {
      return needUpdate(existing[k], value);
    }
    return existing[k] !== value;
  });
}

async function withRetry(func, num = 5, retryInterval = 5000) {
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      return await func();
    } catch (e) {
      if (num <= 0 || e.status === 403) {
        throw e;
      }
      console.log(e);
      console.log(`Retrying (${num})`);
      await new Promise((r) => setTimeout(r, Math.random() * retryInterval));
    }
    num--;
  }
}

let designDocIsLatest = {};

async function uploadPlayer(playerId, data, basic, extended, designDocs) {
  const dbName = `p${data.mode}_${playerId.toString().padStart(10, "0")}`;
  const storage = new CouchStorage({
    uri: `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${PLAYER_SERVERS[data.mode]}/${dbName}`,
    skipSetup: false,
  });
  await withRetry(() =>
    storage.saveDoc(
      {
        _id: basic._id,
        game: basic.uuid,
        start_time: basic.start_time,
        updated: extended.updated,
        ...data,
      },
      true
    )
  );
  if (!designDocIsLatest[dbName]) {
    for (const [key, doc] of Object.entries(designDocs)) {
      const docName = `_design/${key}`;
      const existingDoc = await storage.getDocWithDefault(docName);
      const docNeedUpdate = !existingDoc || needUpdate(existingDoc, doc);
      if (docNeedUpdate) {
        console.log(`Updating design doc ${key} for:`, dbName);
        await withRetry(() => storage.saveDoc({ _id: docName, ...doc }));
      }
    }
    storage._db.viewCleanup(() => {});
    designDocIsLatest[dbName] = true;
  }
  await storage._db.close();
}

async function main() {
  const mapper = await DocMapper.create();
  const sourceStorage = new CouchStorage({ suffix: process.env.DB_SUFFIX, mode: MODE_GAME });
  const stateStorage = new CouchStorage({
    uri: `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${STATE_SERVER}/state`,
    skipSetup: false,
  });
  const stateDoc = await stateStorage.getDocWithDefault("seq");
  let seq = stateDoc ? stateDoc.value : "";
  const getDesignDocs = await createLiveDocGetter("_meta_ext", "player_docs");
  let designDocs = getDesignDocs();
  for (;;) {
    const batch = await sourceStorage._dbExtended.changes({
      since: seq,
      live: false,
      include_docs: true,
      limit: 25,
      batch_size: 25,
      seq_interval: 25,
      timeout: 30000,
      heartbeat: false,
      query_params: {
        feed: "longpoll",
      },
    });
    if (seq === batch.last_seq) {
      continue;
    }
    const extendedDocs = batch.results.map((x) => x.doc).filter((x) => x.type === "roundData");
    if (extendedDocs.length) {
      const keys = extendedDocs.map((x) => x._id.replace(/^r-/, ""));
      const basicResp = await sourceStorage._db.allDocs({
        include_docs: true,
        keys,
      });
      const basicDocs = fromEntries(basicResp.rows.map((x) => [x.doc._id, x.doc]));
      for (const extended of extendedDocs) {
        const basic = basicDocs[extended._id.replace(/^r-/, "")];
        if (!basic) {
          throw new Error(`No basic doc for ${extended._id}`);
        }
        console.log(basic.uuid, basic._id);
        const processedData = mapper.process(basic, extended);
        const newDesignDocs = getDesignDocs();
        if (newDesignDocs._rev !== designDocs._rev) {
          console.log("Design doc updated");
          designDocIsLatest = {};
        }
        designDocs = newDesignDocs;
        for (const [playerId, data] of Object.entries(processedData)) {
          await uploadPlayer(playerId, data, basic, extended, newDesignDocs.docs);
        }
      }
    }
    seq = batch.last_seq;
    await stateStorage.saveDoc({ _id: "seq", value: seq, timestamp: new Date().getTime() });
  }
}

if (require.main === module) {
  wrappedRun(main);
}
// vim: sw=2:ts=2:expandtab:fdm=syntax
