const { wrappedRun } = require("./entryPoint");

const fs = require("fs");
const path = require("path");
const rp = require("request-promise");

const { createMajsoulConnection } = require("./majsoul");

const MODES = [216, 215, 225, 226, 224, 223, 212, 211, 208, 209, 221, 222];
const OUTPUT_DIR = process.env.OUTPUT_DIR || path.resolve("livegames");

function writeFile (fileNameParts, data) {
  const target = path.join(OUTPUT_DIR, ...fileNameParts);
  fs.mkdirSync(path.dirname(target), { recursive: true });
  fs.writeFileSync(path.join(target), data);
}

/**
 *  * Shuffles array in place.
 *   * @param {Array} a items An array containing the items.
 *    */
function shuffle(a) {
  var j, x, i;
  for (i = a.length - 1; i > 0; i--) {
    j = Math.floor(Math.random() * (i + 1));
    x = a[i];
    a[i] = a[j];
    a[j] = x;
  }
  return a;
}

async function main () {
  const deadline = (new Date()).getTime() + 1000 * 170;
  let timeoutToken = null;
  const resetWatchdog = function () {
    if (timeoutToken) {
      clearTimeout(timeoutToken);
    }
    if ((new Date()).getTime() > deadline) {
      console.warn("Deadline exceeded");
      process.exit(1);
    }
    timeoutToken = setTimeout(() => {
      console.warn("Unexpected timeout");
      process.exit(1);
    }, 20000);
  };
  resetWatchdog();
  const conn = await createMajsoulConnection().catch(e => {
    clearTimeout(timeoutToken);
    setTimeout(() => process.exit(1), 100);
    return Promise.reject(e);
  });
  if (!conn) {
    clearTimeout(timeoutToken);
    return;
  }
  const liveGames = {};
  try {
    for (const mode of shuffle(MODES)) {
      resetWatchdog();
      const resp = await conn.rpcCall(".lq.Lobby.fetchGameLiveList", {
        filter_id: mode,
      });
      console.log(`Mode: ${mode}, Live: ${resp.live_list.length}`);
      for (const game of resp.live_list) {
        if (game.start_time < (new Date()).getTime() / 1000 - 60 * 60 * 5) {
          // console.log(game.uuid, game.start_time, (new Date()).getTime() / 1000 - 60 * 60 * 5);
          if (Math.random() > 0.05) {
            continue;
          }
        }
        liveGames[game.uuid] = game;
        writeFile([
          game.uuid.split("-")[0],
          mode.toString(),
          game.uuid + ".json",
        ], JSON.stringify(game));
      }
    }
    if (process.env.LIST_ONLY) {
      return;
    }
    const pendingPromises = [];
    const recurseFillData = async function (dir) {
      if (dir !== OUTPUT_DIR && !/^\d+$/.test(path.basename(dir))) {
        return;
      }
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      const ts = (new Date()).getTime();
      for (const ent of entries) {
        ent.mtimeDelta = fs.statSync(path.join(dir, ent.name)).mtimeMs - ts;
        if (ent.isDirectory()) {
          ent.sortKey = Math.random();
        } else {
          // Favor older games up to 3h, then sort randomly
          ent.sortKey = Math.max(ent.mtimeDelta, -1000 * 60 * 60 * 3) + Math.random() * 10000;
        }
      }
      entries.sort((a, b) => a.sortKey - b.sortKey);
      for (const ent of entries) {
        if (ent.isDirectory()) {
          await recurseFillData(path.join(dir, ent.name));
          continue;
        }
        if (path.extname(ent.name) === ".json") {
          const id = path.parse(ent.name).name;
          if (id in liveGames) {
            continue;
          }
          if (ent.mtimeDelta > -50000) {
            continue;
          }
          resetWatchdog();
          const resp = await conn.rpcCall(".lq.Lobby.fetchGameRecord", { game_uuid: id });
          if (!resp.data && !resp.data_url) {
            continue;
          }
          pendingPromises.push((async function () {
            if (!resp.head) {
              return;
            }
            const recordData = (resp.data_url && (!resp.data || !resp.data.length)) ? await rp({uri: resp.data_url, encoding: null, timeout: 5000}).catch((e) => console.warn(`Failed to download data for ${id}:`, resp)): resp.data;
            if (!recordData || !recordData.length) {
              return;
            }
            console.log(resp.head.uuid);
            const game = resp.head;
            writeFile([
              "records",
              game.uuid.split("-")[0],
              game.config.mode.mode.toString(),
              (game.config.meta.mode_id || game.config.meta.contest_uid || game.config.meta.room_id).toString(),
              game.uuid + ".json",
            ], JSON.stringify(resp.head));
            writeFile([
              "records",
              game.uuid.split("-")[0],
              game.config.mode.mode.toString(),
              (game.config.meta.mode_id || game.config.meta.contest_uid || game.config.meta.room_id).toString(),
              game.uuid + ".recordData",
            ], recordData);
            try {
              fs.unlinkSync(path.join(dir, ent.name));
            } catch(e) {
              console.warn("Error when deleting file: ", e);
            }
          })());
        }
      }
    };
    await recurseFillData(OUTPUT_DIR);
    resetWatchdog();
    await Promise.all(pendingPromises);
  } finally {
    conn.close();
    clearTimeout(timeoutToken);
  }
  process.exit(0);
}

if (require.main === module) {
  wrappedRun(main);
}
// vim: sw=2:ts=2:expandtab:fdm=syntax
