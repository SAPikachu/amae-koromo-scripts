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

async function main () {
  const deadline = (new Date()).getTime() + 1000 * 110; // 110 seconds
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
  const conn = await createMajsoulConnection();
  if (!conn) {
    clearTimeout(timeoutToken);
    return;
  }
  const liveGames = {};
  try {
    for (const mode of MODES) {
      resetWatchdog();
      const resp = await conn.rpcCall(".lq.Lobby.fetchGameLiveList", {
        filter_id: mode,
      });
      console.log(`Mode: ${mode}, Live: ${resp.live_list.length}`);
      for (const game of resp.live_list) {
        liveGames[game.uuid] = true;
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
      for (const ent of fs.readdirSync(dir, { withFileTypes: true }).reverse()) {
        if (ent.isDirectory()) {
          await recurseFillData(path.join(dir, ent.name));
          continue;
        }
        if (path.extname(ent.name) === ".json") {
          const id = path.parse(ent.name).name;
          if (id in liveGames) {
            continue;
          }
          resetWatchdog();
          const resp = await conn.rpcCall(".lq.Lobby.fetchGameRecord", { game_uuid: id });
          if (!resp.data && !resp.data_url) {
            continue;
          }
          pendingPromises.push((async function () {
            const recordData = (resp.data_url && (!resp.data || !resp.data.length)) ? await rp({uri: resp.data_url, encoding: null, timeout: 5000}).catch((e) => console.warn(`Failed to download data for ${id}:`, e)): resp.data;
            if (!resp.head || !recordData || !recordData.length) {
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
            fs.unlinkSync(path.join(dir, ent.name));
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
