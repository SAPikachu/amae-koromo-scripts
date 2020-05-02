const { wrappedMain, wrappedRun } = require("./entryPoint");

const rp = require("request-promise");
const assert = require("assert");
const moment = require("moment");
const _ = require("lodash");
const compareVersion = require("node-version-compare");

const { DataStorage } = require("./storage");
const { createMajsoulConnection } = require("./majsoul");
const { CouchStorage } = require("./couchStorage");
const { iterateLocalData } = require("./localData");
const { calcShanten } = require("./shanten");

function convertGameInfo (raw) {
  return {
    modeId: raw.game_config.meta.mode_id,
    uuid: raw.uuid,
    startTime: raw.start_time,
    players: raw.seat_list.map((accountId) => {
      const player = raw.players.filter((x) => x.account_id === accountId)[0];
      return {
        accountId,
        nickname: player.nickname,
        level: player.level.id,
      };
    }),
  };
}

function convertGameRecord (raw) {
  return {
    // modeId: raw.config.meta.mode_id,
    uuid: raw.uuid,
    /*
    startTime: raw.start_time,

    endTime: raw.end_time,
    players: raw.accounts.map((account) => {
      return {
        accountId: account.account_id,
        nickname: account.nickname,
        level: account.level.id,
        score: raw.result.players.filter(x => x.seat === account.seat)[0].part_point_1,
      };
    }),*/
  };
}

async function fetchLiveGames (conn) {
  const game216 = await conn.rpcCall(".lq.Lobby.fetchGameLiveList", {
    filter_id: 216,
  });
  const game212 = await conn.rpcCall(".lq.Lobby.fetchGameLiveList", {
    filter_id: 212,
  });
  return game216.live_list.concat(game212.live_list).map(convertGameInfo);
}

function groupBy (list, keyGetter) {
  const map = new Map();
  list.forEach((item) => {
    const key = keyGetter(item);
    const collection = map.get(key);
    if (!collection) {
      map.set(key, [item]);
    } else {
      collection.push(item);
    }
  });
  return map;
}

async function withRetry (func, num = 20, retryInterval = 5000) {
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      return await func();
    } catch (e) {
      if (num <= 0 || e.status === 403) {
        throw e;
      }
      console.log(`Retrying (${num})`);
      await new Promise(r => setTimeout(r, retryInterval));
    }
    num--;
  }
}

function buildRecordData ({ data, dataDefinition, game }) {
  const root = require("protobufjs").Root.fromJSON(dataDefinition);
  const wrapper = root.nested.lq.Wrapper;
  const msg = wrapper.decode(data);
  const typeObj = root.lookupType(msg.name);
  const payload = typeObj.decode(msg.data);
  const rounds = [];
  let 振听 = null;
  let numDiscarded = null;
  let lastDiscardSeat = null;
  for (const itemBuf of payload.records) {
    const item = wrapper.decode(itemBuf);
    if ([".lq.RecordDealTile"].includes(item.name)) {
      continue;
    }
    const itemPayload = root.lookupType(item.name).decode(item.data);
    if (item.name === ".lq.RecordNewRound") {
      assert([3, 4].includes(itemPayload.scores.length));
      rounds.push([0, 1, 2, 3].slice(0, itemPayload.scores.length).map(seat => ({
        ...(itemPayload[`tiles${seat}`].length === 14 ? {
          亲: true,
          牌山: itemPayload.paishan,
        } : {}),
        手牌: itemPayload[`tiles${seat}`],
        起手向听: calcShanten(itemPayload[`tiles${seat}`]),
      })));
      振听 = Array(rounds[rounds.length - 1].length).fill(false);
      numDiscarded = 0;
      lastDiscardSeat = null;
      assert(rounds[rounds.length - 1].filter(x => x.亲).length === 1);
      assert([3, 4].includes(rounds[rounds.length - 1].length));
      continue;
    }
    const curRound = rounds[rounds.length - 1];
    assert(curRound);
    const numPlayers = curRound.length;
    assert([3, 4].includes(numPlayers));
    switch (item.name) {
    case ".lq.RecordChiPengGang":
      curRound[itemPayload.seat].副露 = (curRound[itemPayload.seat].副露 || 0) + 1;
      break;
    case ".lq.RecordDiscardTile":
      // console.log(itemPayload);
      lastDiscardSeat = itemPayload.seat;
      振听 = itemPayload.zhenting;
      if ((!curRound[itemPayload.seat].立直) && (itemPayload.is_liqi || itemPayload.is_wliqi)) {
        curRound[itemPayload.seat].立直 = numDiscarded / numPlayers + 1;
        if (振听[itemPayload.seat]) {
          curRound[itemPayload.seat].振听立直 = true;
        }
      }
      if (itemPayload.is_wliqi) {
        curRound[itemPayload.seat].W立直 = true;
      }
      numDiscarded++;
      break;
    case ".lq.RecordNoTile":
      if (itemPayload.liujumanguan) {
        itemPayload.scores.forEach(x => curRound[x.seat].流满 = true);
      }
      itemPayload.players.forEach((x, seat) => {
        curRound[seat].流听 = x.tingpai;
      });
      break;
    case ".lq.RecordHule":
      itemPayload.hules.forEach((x) => {
        curRound[x.seat].和 = [
          itemPayload.delta_scores[x.seat] - (x.liqi ? 1000 : 0),
          _.flatten(x.fans.map(x => Array(x.val).fill(x.id))),
          numDiscarded / numPlayers + 1,
        ];
        if (!x.zimo && curRound[x.seat].和[0] < Math.max(0, x.point_rong - 1500)) {
          // 一炮多响 + 包牌
          console.log(itemPayload);
          assert(itemPayload.hules.length === 2);
          const info = itemPayload.hules.filter(other => other.yiman && other.seat !== x.seat)[0];
          assert(info);
          curRound[x.seat].和[0] += info.point_rong / 2;
          curRound[x.seat].包牌 = info.point_rong / 2;
        }
        const numLosingPlayers = itemPayload.delta_scores.filter(x => x < 0).length;
        if (x.zimo) {
          assert(itemPayload.hules.length === 1);
          assert(numLosingPlayers === (numPlayers - 1) || itemPayload.hules[0].yiman);
          curRound[x.seat].自摸 = true;
          if (振听[x.seat]) {
            curRound[x.seat].振听自摸 = true;
          }
          if (numLosingPlayers === 1) {
            itemPayload.delta_scores.forEach((score, seat) => {
              if (score < 0) {
                curRound[seat].包牌 = Math.abs(score);
              }
            });
          }
        } else {
          assert([1, 2].includes(numLosingPlayers));
          itemPayload.delta_scores.forEach((score, seat) => {
            if (score < 0) {
              if (numLosingPlayers === 1) {
                assert(seat === lastDiscardSeat);
              } else {
                assert(itemPayload.hules.some(x => x.yiman));
              }
              curRound[seat][seat === lastDiscardSeat ? "放铳" : "包牌"] = Math.abs(score);
            }
          });
        }
      });
      break;
    case ".lq.RecordBaBei":
    case ".lq.RecordAnGangAddGang":
      lastDiscardSeat = itemPayload.seat;
      break;
    case ".lq.RecordLiuJu":
      curRound.forEach(x => x.途中流局 = itemPayload.type);
      break;
    default:
      console.log(game.uuid);
      console.log(item.name);
      delete itemPayload.operation;
      console.log(itemPayload);
      assert(false);
    }
  }
  return rounds;
}
async function processRecordDataForGameId (store, uuid, recordData) {
  const rawRecordInfo = {
    ...(await store.getRecordData(uuid)),
    data: recordData,
  };
  const rounds = buildRecordData(rawRecordInfo);
  // console.log(rawRecordInfo.game.uuid);
  await store.saveRoundData(rawRecordInfo.game, rounds);
}

async function processGames (conn, ids, storageParams = {}) {
  const store = new CouchStorage(storageParams);
  let filteredIds = [];
  while (ids.length) {
    filteredIds = filteredIds.concat(await store.findNonExistentRecords(ids.slice(0, 100)));
    ids = ids.slice(100);
  }
  ids = filteredIds;
  if (!ids.length) {
    return;
  }
  ids.sort();
  for (const id of ids) {
    console.log(id);
    let resp;
    try {
      resp = await conn.rpcCall(".lq.Lobby.fetchGameRecord", { game_uuid: id });
    } catch (e) {
      console.log(e);
      console.log("Reconnecting");
      await new Promise(r => setTimeout(r, 1000));
      // eslint-disable-next-line require-atomic-updates
      conn.reconnect();
      await conn.waitForReady();
      resp = await conn.rpcCall(".lq.Lobby.fetchGameRecord", { game_uuid: id });
    }
    if (!resp.data_url && !(resp.data && resp.data.length)) {
      console.log(`No data in response: ${id}`);
      continue;
    }
    const recordData = resp.data_url ? await withRetry(() => rp({uri: resp.data_url, encoding: null, timeout: 5000})) : resp.data;
    console.log("Saving");
    // const compressedRecordData = await store.compressData(recordData);
    // await withRetry(() => dataStore.setRaw(`recordData/${id}.lzma2`, compressedRecordData));
    await withRetry(() => store.saveGame(resp.head, conn._codec.version));
    await withRetry(() => store.ensureDataDefinition(conn._codec.version, conn._codec.rawDefinition));
    await withRetry(() => processRecordDataForGameId(store, id, recordData));
    await new Promise(r => setTimeout(r, 1000));
  }
  await store.triggerViewRefresh();
}

async function syncToCouchDb () {
  const storage = new DataStorage();
  const store = new CouchStorage();
  const latestDoc = await store.getLatestRecord();
  let date = moment.unix(latestDoc.start_time).subtract(36, "hours").startOf("day");
  const conn = await createMajsoulConnection();
  if (!conn) {
    return;
  }
  try {
    while (date.isSameOrBefore(moment())) {
      const ids = Object.keys(await storage.get(`records/${date.format("YYMMDD")}.json`));
      await processGames(conn, ids);
      date = date.add(1, "day");
    }
  } finally {
    conn.close();
  }
  await store.triggerViewRefresh();
}

async function loadLocalData () {
  const store = new CouchStorage();
  const dataDefs = await store._db.allDocs({
    include_docs: true,
    startkey: "dataDefinition-",
    endkey: "dataDefinition-\uffff",
  });
  const ver = dataDefs.rows.map(x => x.doc.version).sort(compareVersion).reverse()[0];
  const groups = {
    normal: {
      store,
      items: [],
    },
    gold: {
      store: new CouchStorage({suffix: "_2_9"}),
      items: [],
    },
    sanma: {
      store: new CouchStorage({suffix: "_12"}),
      items: [],
    },
  };
  const processLoadedData = async function() {
    for (const group of Object.values(groups)) {
      let items = group.items;
      group.items = [];
      const itemStore = group.store;
      const filteredItems = [];
      while (items.length) {
        const chunk = items.slice(0, 100);
        items = items.slice(100);
        const filteredIds = new Set(await itemStore.findNonExistentRecords(chunk.map(x => x.id)));
        for (const item of chunk) {
          if (filteredIds.has(item.id)) {
            filteredItems.push(item);
          }
        }
      }
      for (const item of filteredItems) {
        console.log(`Saving ${item.id}`);
        const recordData = item.getRecordData();
        await withRetry(() => itemStore.saveGame(item.data, ver));
        await withRetry(() => processRecordDataForGameId(itemStore, item.id, recordData));
      }
      await itemStore.triggerViewRefresh();
    }
  };
  await iterateLocalData(async function (item) {
    try {
      item.data = item.getData();
    } catch (e) {
      console.error(`Failed to parse ${item.id}:`, e);
      return;
    }
    if (item.data.config.category !== 2) {
      return;
    }
    if ([12, 16].includes(item.data.config.meta.mode_id)) {
      groups.normal.items.push(item);
    } else if ([9].includes(item.data.config.meta.mode_id)) {
      groups.gold.items.push(item);
    } else if ([22, 24, 26].includes(item.data.config.meta.mode_id)) {
      groups.sanma.items.push(item);
    }
    if (Object.values(groups).some(x => x.items.length > 1000)) {
      await processLoadedData();
    }
  });
  await processLoadedData();
}

async function syncContest (contestId, dbSuffix) {
  const conn = await createMajsoulConnection();
  if (!conn) {
    return;
  }
  try {
    let resp = await conn.rpcCall(".lq.Lobby.fetchCustomizedContestByContestId", {
      contest_id: contestId,
    });
    const realId = resp.contest_info.unique_id;
    let nextIndex = undefined;
    const idLog = {};
    while (true) {
      resp = await conn.rpcCall(".lq.Lobby.fetchCustomizedContestGameRecords", {
        unique_id: realId,
        last_index: nextIndex,
      });
      for (const game of resp.record_list) {
        idLog[game.uuid] = true;
      }

      if (!resp.next_index || !resp.record_list.length) {
        break;
      }
      nextIndex = resp.next_index;
    }
    await processGames(conn, Object.keys(idLog), {suffix: dbSuffix});
  } finally {
    conn.close();
  }
}

async function main () {
  if (process.env.EXTERNAL_AGGREGATION) {
    throw new Error("Moved to separate script");
  }
  if (process.env.SYNC_COUCHDB) {
    return await syncToCouchDb();
  }
  if (process.env.LOAD_LOCAL_DATA) {
    return await loadLocalData();
  }
  if (process.env.SYNC_CONTEST) {
    // return await syncContest(511652, "_jinja");
    return await syncContest(903356, "_jinja");
  }
  if (process.env.UPDATE_AGV) {
    throw new Error("Moved to separate script");
  }
  const storage = new DataStorage();
  const oldLiveGames = await storage.get("live.json");
  let pendingIds = await storage.get("pending_ids.json", []);
  const conn = await createMajsoulConnection();
  if (!conn) {
    return;
  }
  try {
  /*
  resp = await conn.rpcCall(".lq.Lobby.fetchGameRecord", {
    game_uuid: "190823-64f22e47-7a34-4720-977f-2767eae35700",
  });
  const details = proto.decodeDataMessage(resp.data);
  const lastOp = details.payload.records[details.payload.records.length - 1];
  resp = await conn.rpcCall(".lq.Lobby.fetchGameRecordsDetail", {
    uuid_list: ["190823-64f22e47-7a34-4720-977f-2767eae35700"],
  });
*/
    const liveGames = await fetchLiveGames(conn);
    const newGameIds = new Set(liveGames.map(x => x.uuid));
    const finishedGameIds = oldLiveGames.map(x => x.uuid).filter(x => !newGameIds.has(x));
    pendingIds = Array.from(new Set(finishedGameIds.concat(pendingIds)));
    await storage.set("pending_ids.json", pendingIds);
    let gameRecords = [];
    if (pendingIds.length > 0) {
      const resp = await conn.rpcCall(".lq.Lobby.fetchGameRecordsDetail", {
        uuid_list: pendingIds,
      });
      gameRecords = resp.record_list.map(convertGameRecord);
      const groupedRecords = groupBy(gameRecords, x => x.uuid.split("-")[0]);
      for (const [time, records] of groupedRecords) {
        const fileName = `records/${time}.json`;
        const existingRecords = await storage.get(fileName, {});
        for (const record of records) {
          existingRecords[record.uuid] = record;
        }
        await storage.set(fileName, existingRecords);
      }
    }
    const newRecordsIdSet = new Set(gameRecords.map(x => x.uuid));
    pendingIds.sort();
    await storage.set("pending_ids.json", pendingIds.filter(x => !newRecordsIdSet.has(x)));
    await storage.set("live.json", liveGames);
    if (newRecordsIdSet.size > 0) {
      await processGames(conn, Array.from(newRecordsIdSet));
    }
    return `${gameRecords.length} records saved`;
  } finally {
    conn.close();
  }
}

if (require.main === module) {
  wrappedRun(main);
} else {
  exports["amae-koromo"] = wrappedMain(main);
  exports.processRecordDataForGameId = processRecordDataForGameId;
}
// vim: sw=2:ts=2:expandtab:fdm=syntax
