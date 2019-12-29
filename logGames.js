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
  const conn = await createMajsoulConnection();
  if (!conn) {
    return;
  }
  const liveGames = {};
  try {
    for (const mode of MODES) {
      const resp = await conn.rpcCall(".lq.Lobby.fetchGameLiveList", {
        filter_id: mode,
      });
      for (const game of resp.live_list) {
        liveGames[game.uuid] = true;
        writeFile([
          game.uuid.split("-")[0],
          mode.toString(),
          game.uuid + ".json",
        ], JSON.stringify(game));
      }
    }
    const recurseFillData = async function (dir) {
      if (dir !== OUTPUT_DIR && !/^\d+$/.test(path.basename(dir))) {
        return;
      }
      for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
        if (ent.isDirectory()) {
          await recurseFillData(path.join(dir, ent.name));
          continue;
        }
        if (path.extname(ent.name) === ".json") {
          const id = path.parse(ent.name).name;
          if (id in liveGames) {
            continue;
          }
          const resp = await conn.rpcCall(".lq.Lobby.fetchGameRecord", { game_uuid: id });
          if (!resp.data && !resp.data_url) {
            continue;
          }
          const recordData = resp.data_url ? await rp({uri: resp.data_url, encoding: null, timeout: 5000}) : resp.data;
          if (!resp.head || !recordData.length) {
            continue;
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
        }
      }
    };
    await recurseFillData(OUTPUT_DIR);
  } finally {
    conn.close();
  }
}

if (require.main === module) {
  wrappedRun(main);
}
// vim: sw=2:ts=2:expandtab:fdm=syntax
