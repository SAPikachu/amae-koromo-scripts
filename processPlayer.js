const { promisify } = require("util");

const redis = require("redis");
const assert = require("assert");
const axios = require("axios").default;

const { wrappedRun } = require("./entryPoint");
const { CouchStorage, MODE_GAME } = require("./couchStorage");
const { createMapper, createLiveDocGetter } = require("./dbExtension");
const { Throttler, DummyThrottler } = require("./throttler");
const { COUCHDB_USER, COUCHDB_PASSWORD, COUCHDB_PROTO, PLAYER_SERVERS, REDIS_HOST, REDIS_PASSWORD } = require("./env");

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

async function uploadPlayer({ playerId, data, basic, extended, designDocs, logTag, redisClient }) {
  assert(playerId);
  assert(data);
  assert(basic);
  assert(extended);
  assert(designDocs);
  assert(logTag);
  assert(redisClient);
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
        console.log(logTag, `Updating design doc ${key} for:`, dbName);
        await withRetry(() => storage.saveDoc({ _id: docName, ...doc }));
      }
    }
    await axios.put(
      `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${PLAYER_SERVERS[data.mode]}/${dbName}/_revs_limit`,
      "1"
    );
    storage._db.viewCleanup(() => {});
    designDocIsLatest[dbName] = true;
  }
  await storage._db.close();
  if (!process.env.SINGLE_SOURCE) {
    const ignore = await redisClient.sismember("compactIgnore", dbName);
    await redisClient.zincrby(!ignore ? "compactQueue" : "compactQueueAlt", 1 + Math.random() * 0.01, dbName);

    await redisClient.zadd("cacheStats3", new Date().getTime(), dbName);
  }
}

async function updateNickname({ playerId, nicknameStorage, basic, data, dbSuffix }) {
  assert(playerId);
  assert(nicknameStorage);
  assert(basic);
  assert(data);
  assert(dbSuffix || dbSuffix === "");
  const paddedId = playerId.toString().padStart(10, "0");
  const nicknameDoc = await withRetry(() => nicknameStorage.getDocWithDefault(paddedId, { _id: paddedId }));
  const modeKey = dbSuffix || "_";
  nicknameDoc.modes = nicknameDoc.modes || {};
  if (nicknameDoc.modes[modeKey] && nicknameDoc.modes[modeKey].timestamp > basic.start_time) {
    return;
  }
  if (!nicknameDoc.timestamp || basic.start_time > nicknameDoc.timestamp) {
    nicknameDoc.normalized_name = data.basic.nickname.toLowerCase().replace(/(^\s+|\s+$)/g, "");
    nicknameDoc.nickname = data.basic.nickname;
    nicknameDoc.timestamp = basic.start_time;
  }
  nicknameDoc.modes[modeKey] = {
    level: data.basic.level,
    timestamp: basic.start_time,
  };
  await withRetry(() => nicknameStorage.saveDoc(nicknameDoc));
}

async function run({ mapper, dbSuffix, stateServer, logTag, throttler, nicknameStorage, redisClient, getDesignDocs }) {
  assert(mapper);
  assert(stateServer);
  assert(logTag);
  assert(nicknameStorage);
  assert(redisClient);
  assert(throttler);
  assert(getDesignDocs);
  const sourceStorage = new CouchStorage({ suffix: dbSuffix, mode: MODE_GAME });
  const stateStorage = new CouchStorage({
    uri: `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${stateServer}/state`,
    skipSetup: false,
  });
  const stateDoc = await stateStorage.getDocWithDefault("seq");
  let seq = stateDoc ? stateDoc.value : "";
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
    const lowPriorityKeys = extendedDocs
      .filter(function (x) {
        assert(x.start_time);
        return x.start_time * 1000 < new Date().getTime() - 48 * 60 * 60 * 1000;
      })
      .map((x) => x._id.replace(/^r-/, ""));
    if (extendedDocs.length) {
      const keys = extendedDocs
        .map((x) => x._id.replace(/^r-/, ""))
        .filter((x) => {
          if (lowPriorityKeys.includes(x)) {
            console.log(`[${logTag}] Low priority:`, x);
            // return false;
          }
          return true;
        });
      const throttlerId = await throttler.waitNext();
      const basicResp = await sourceStorage._db.allDocs({
        include_docs: true,
        keys,
      });
      throttler.complete(throttlerId);
      const basicDocs = fromEntries(basicResp.rows.map((x) => [x.doc._id, x.doc]));
      for (const extended of extendedDocs) {
        const basic = basicDocs[extended._id.replace(/^r-/, "")];
        if (!basic) {
          throw new Error(`[${logTag}] No basic doc for ${extended._id}`);
        }
        console.log(logTag, basic.uuid, basic._id);
        const processedData = mapper.process(basic, extended);
        const newDesignDocs = getDesignDocs();
        if (newDesignDocs._rev !== designDocs._rev) {
          console.log("Design doc updated");
          designDocIsLatest = {};
        }
        designDocs = newDesignDocs;
        for (const [playerId, data] of Object.entries(processedData)) {
          const throttlerId = await throttler.waitNext();
          await uploadPlayer({ playerId, data, basic, extended, designDocs: newDesignDocs.docs, logTag, redisClient });
          await updateNickname({ playerId, nicknameStorage, basic, data, dbSuffix });
          throttler.complete(throttlerId);
        }
      }
    }
    seq = batch.last_seq;
    await stateStorage.saveDoc({ _id: "seq", value: seq, timestamp: new Date().getTime() });
  }
}
async function main() {
  const mapper = await DocMapper.create();
  const getDesignDocs = await createLiveDocGetter("_meta_ext", "player_docs");
  const nicknameStorage = new CouchStorage({ suffix: "_nicknames" });
  const redisClientRaw = redis.createClient({
    host: REDIS_HOST,
    password: REDIS_PASSWORD,
    retry_unfulfilled_commands: true,
  });
  const redisClient = {
    zincrby: promisify(redisClientRaw.zincrby.bind(redisClientRaw)),
    zadd: promisify(redisClientRaw.zadd.bind(redisClientRaw)),
    sismember: promisify(redisClientRaw.sismember.bind(redisClientRaw)),
  };
  const promises = [];
  const settings = {
    jt: {
      dbSuffix: "",
      stateServer: PLAYER_SERVERS[12],
      logTag: "[JT]",
    },
    gold: {
      dbSuffix: "_gold",
      stateServer: PLAYER_SERVERS[9],
      logTag: "[G ]",
    },
    sanma: {
      dbSuffix: "_sanma",
      stateServer: PLAYER_SERVERS[24],
      logTag: "[S ]",
    },
    e4: {
      dbSuffix: "_e4",
      stateServer: PLAYER_SERVERS[15],
      logTag: "[E4]",
    },
    e3: {
      dbSuffix: "_e3",
      stateServer: PLAYER_SERVERS[25],
      logTag: "[E3]",
    },
  };
  if (process.env.SINGLE_SOURCE) {
    assert(settings[process.env.SINGLE_SOURCE]);
    promises.push(
      run({
        ...settings[process.env.SINGLE_SOURCE],
        mapper,
        getDesignDocs,
        nicknameStorage,
        redisClient,
        throttler: new DummyThrottler(),
      })
    );
  } else {
    const throttler = new Throttler(0);
    Object.keys(settings)
      .filter((key) => !settings[key]._disabled)
      .forEach((key) =>
        promises.push(run({ ...settings[key], mapper, getDesignDocs, nicknameStorage, redisClient, throttler }))
      );
  }
  await Promise.all(promises);
}

if (require.main === module) {
  wrappedRun(main);
}
// vim: sw=2:ts=2:expandtab:fdm=syntax
