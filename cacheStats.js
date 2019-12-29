const { wrappedRun } = require("./entryPoint");

const moment = require("moment");
const assert = require("assert");

const { CouchStorage } = require("./couchStorage");
const { streamView } = require("./streamView");

const VIEWS = {
  basic: "player_stats_2",
  extended: "player_extended_stats",
};

async function saveStats ({storage, id, mode, stats, timestamp}) {
  assert(stats.basic || stats.extended);
  assert(timestamp);
  const key = `${id}-${mode}`;
  let doc = {};
  try {
    doc = await storage.db.get(key);
  } catch (e) {
    if (!e.status || e.status !== 404) {
      throw e;
    }
  }
  Object.assign(doc, {
    _id: key,
    type: "stats",
    account_id: id,
    mode_id: mode,
    timestamp,
    updated: moment.utc().valueOf(),
    ...stats,
  });
  await storage.db.put(doc);
}

async function fullSyncType (type, timestamp) {
  assert(VIEWS[type]);
  console.log("Full sync: " + type);
  const storage = new CouchStorage({suffix: "_stats"});
  const rowBuffer = [];
  const runSave = async function () {
    while (rowBuffer.length) {
      const { key: [id, mode], value } = rowBuffer.shift();
      await saveStats({
        storage,
        id,
        mode,
        stats: {[type]: value},
        timestamp,
      });
    }
  };
  let savedError = null;
  await streamView(VIEWS[type], "player_stats", {group_level: 2}, (row) => {
    if (savedError) {
      throw savedError;
    }
    rowBuffer.push(row);
    if (rowBuffer.length === 1) {
      runSave().catch(e => {
        console.error(e);
        savedError = e;
      });
    }
  });
  console.log("Finishing buffered rows...");
  while (rowBuffer.length) {
    if (savedError) {
      throw savedError;
    }
    await new Promise(res => setTimeout(res, 1000));
  }
}

async function getPlayerStats (storage, id, mode, type) {
  assert(VIEWS[type]);
  const resp = await storage.db.query(VIEWS[type] + "/player_stats", {
    group_level: 2,
    startkey: [id, mode],
    endkey: [id, mode, {}],
    limit: 1,
  });
  const row = resp.rows[0];
  assert(row.key[0] === id);
  assert(row.key[1] === mode);
  return row.value;
}

async function syncPlayer ({sourceStorage, targetStorage, id, mode, timestamp}) {
  for (const selectedMode of [mode, 0]) {
    await saveStats({
      storage: targetStorage,
      id,
      mode: selectedMode,
      stats: {
        basic: await getPlayerStats(sourceStorage, id, selectedMode, "basic"),
        extended: await getPlayerStats(sourceStorage, id, selectedMode, "extended"),
      },
      timestamp,
    });
  }
}
async function getLastTimestamp (targetStorage) {
  const lastTimestampResp = await targetStorage.db.query("updated_timestamp/updated_timestamp", {
    descending: true,
    limit: 1,
  });
  const lastTimestamp = lastTimestampResp.rows[0].key;
  assert(lastTimestamp);
  return lastTimestamp;
}

async function fullSync () {
  const targetStorage = new CouchStorage({suffix: "_stats"});
  const timestamp = await getLastTimestamp(targetStorage);
  await fullSyncType("basic", timestamp);
  await fullSyncType("extended", timestamp);
}

async function main () {
  if (process.env.FULL_SYNC) {
    await fullSync();
    return;
  }
  const targetStorage = new CouchStorage({suffix: "_stats"});
  const lastTimestamp = await getLastTimestamp(targetStorage);
  const pendingDocs = [];
  await streamView(
    "updated_players", "updated_players",
    {startkey: lastTimestamp + 1},
    ({ key, value }) => {
      for (const accountId of value.accounts) {
        const pendingSyncKey = `${accountId}-${value.mode_id}`;
        pendingDocs.push({
          _id: `pendingSync-${pendingSyncKey}`,
          type: "pendingSync",
          key: pendingSyncKey,
          value: {
            id: accountId,
            mode: value.mode_id,
            timestamp: key,
          },
        });
      }
    }
  );
  for (const doc of pendingDocs) {
    await targetStorage.saveDoc(doc);
  }
  const needUpdate = [];
  await streamView(
    "pending_sync", "pending_sync",
    {_suffix: "_stats", include_docs: true},
    (row) => {
      needUpdate.push(row);
    }
  );
  const sourceStorage = new CouchStorage();
  for (const { value, doc } of needUpdate) {
    assert(value);
    console.log(value);
    await syncPlayer({...value, sourceStorage, targetStorage});
    assert(doc);
    try {
      await targetStorage.db.remove(doc);
    } catch (e) {
      if (!e.status || (e.status !== 404 && e.status !== 409)) {
        throw e;
      }
    }
  }
}

if (require.main === module) {
  wrappedRun(main);
}
// vim: sw=2:ts=2:expandtab:fdm=syntax
