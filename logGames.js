const { wrappedRun } = require("./entryPoint");

const fs = require("fs");
const path = require("path");
const rp = require("request-promise");

const { createMajsoulConnection } = require("./majsoul");

const MODES = [216, 215, 225, 226, 224, 223, 212, 211, 208, 209, 221, 222];
const OUTPUT_DIR = process.env.OUTPUT_DIR || path.resolve("livegames");

function writeFile(fileNameParts, data) {
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

const DEFAULT_MTIME_CUTOFF = 130000;
async function main() {
  let deadline = new Date().getTime() + 1000 * 230;
  let timeoutToken = null;
  const resetWatchdog = function () {
    if (timeoutToken) {
      clearTimeout(timeoutToken);
    }
    if (new Date().getTime() > deadline) {
      console.warn("Deadline exceeded");
      process.exit(1);
    }
    timeoutToken = setTimeout(() => {
      console.warn("Unexpected timeout");
      process.exit(1);
    }, 25000);
  };
  resetWatchdog();
  const conn = await createMajsoulConnection().catch((e) => {
    clearTimeout(timeoutToken);
    console.warn("Exiting in 500ms");
    setTimeout(() => process.exit(1), 500);
    return Promise.reject(e);
  });
  if (!conn) {
    clearTimeout(timeoutToken);
    // process.exit(0);
    return;
  }
  const liveGames = {};
  try {
    if (process.env.LIST_ONLY) {
      while (true) {
        const startTime = Date.now();
        for (const mode of shuffle(MODES)) {
          resetWatchdog();
          const resp = await conn.rpcCall(".lq.Lobby.fetchGameLiveList", {
            filter_id: mode,
          });
          console.log(`Mode: ${mode}, Live: ${resp.live_list.length}`);
          for (const game of resp.live_list) {
            if (game.start_time < new Date().getTime() / 1000 - 60 * 60 * 5) {
              // console.log(game.uuid, game.start_time, (new Date()).getTime() / 1000 - 60 * 60 * 5);
              if (Math.random() > /*0.05*/ 0) {
                continue;
              }
            }
            liveGames[game.uuid] = game;
            writeFile([game.uuid.split("-")[0], mode.toString(), game.uuid + ".json"], JSON.stringify(game));
          }
        }
        if (!process.env.LIST_ONLY) {
          break;
        } else {
          deadline += 60000;
        }
        const elapsed = Date.now() - startTime;
        if (elapsed < 15000) {
          const sleepTime = 15000 - elapsed;
          console.log(`Sleeping for ${sleepTime}ms`);
          await new Promise((resolve) => setTimeout(resolve, sleepTime));
        }
        /*
      if (process.env.LIST_ONLY) {
        // process.exit(0);
        return;
      }*/
      }
    }
    const pendingPromises = [];
    const recurseFillData = async function (dir) {
      if (dir !== OUTPUT_DIR && !/^\d+$/.test(path.basename(dir))) {
        return;
      }
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      const ts = new Date().getTime();
      for (const ent of entries) {
        try {
          ent.mtimeDelta = fs.statSync(path.join(dir, ent.name)).mtimeMs - ts;
        } catch (e) {
          // Probably deleted by other process, this will be ignored below
          ent.mtimeDelta = 0;
        }
        if (ent.isDirectory()) {
          ent.sortKey = Math.random();
        } else {
          // Favor older games up to 3h, then sort randomly
          ent.sortKey = Math.max(ent.mtimeDelta, -1000 * 60 * 60 * 3) + Math.random() * 10000;
        }
      }
      // entries.sort((a, b) => a.sortKey - b.sortKey);
      shuffle(entries);
      const MTIME_CUTOFF = (parseInt(process.env.MTIME_CUTOFF, 10) || DEFAULT_MTIME_CUTOFF) * -1;
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
          try {
            ent.mtimeDelta = fs.statSync(path.join(dir, ent.name)).mtimeMs - ts;
          } catch (e) {
            // Probably deleted by other process
            continue;
          }
          if (ent.mtimeDelta > MTIME_CUTOFF) {
            // console.log("MTIME_CUTOFF", ent.name, ent.mtimeDelta, MTIME_CUTOFF);
            if (MTIME_CUTOFF * -1 > DEFAULT_MTIME_CUTOFF) {
              const resp = await conn.rpcCall(".lq.Lobby.fetchGameLiveInfo", {
                game_uuid: id,
              });
              resetWatchdog();
              if (resp.live_head) {
                // console.log("Still running:", id);
                try {
                  fs.statSync(path.join(dir, ent.name));
                } catch (e) {
                  // Probably deleted by other process
                  continue;
                }
                fs.writeFileSync(path.join(dir, ent.name), JSON.stringify(resp.live_head));
                process.stdout.write(".");
              }
            }
            continue;
          }
          resetWatchdog();
          try {
            fs.statSync(path.join(dir, ent.name));
          } catch (e) {
            // Probably deleted by other process
            continue;
          }
          const startTime = Date.now();
          async function throttle() {
            if (Date.now() - startTime < 1000) {
              await new Promise((resolve) => setTimeout(resolve, 1000 - (Date.now() - startTime)));
            }
          }
          const resp = await conn.rpcCall(".lq.Lobby.fetchGameRecord", {
            game_uuid: id,
            client_version_string: conn.clientVersionString,
          });
          if ((!resp.data && !resp.data_url) || !resp.head) {
            let resp = await conn.rpcCall(".lq.Lobby.fetchOBToken", { uuid: id });
            if (resp.token && resp.create_time && resp.create_time > Date.now() / 1000 - 60 * 60 * 6) {
              console.log("OB:", id);
              fs.utimesSync(path.join(dir, ent.name), new Date(), new Date(Date.now() + 1000 * 60 * 30));
              await throttle();
              continue;
            }
            resp = await conn.rpcCall(".lq.Lobby.fetchGameLiveInfo", { game_uuid: id });
            if (resp.live_head) {
              console.log("Still running:", id);
              fs.writeFileSync(path.join(dir, ent.name), JSON.stringify(resp.live_head));
              fs.utimesSync(path.join(dir, ent.name), new Date(), new Date(Date.now() + 1000 * 60 * 30));
            } else {
              let mtimeDeltaAdjusted = ent.mtimeDelta;
              try {
                const liveInfo = JSON.parse(fs.readFileSync(path.join(dir, ent.name), { encoding: "utf8" }));
                if (liveInfo.start_time) {
                  mtimeDeltaAdjusted = liveInfo.start_time * 1000 - ts;
                  fs.utimesSync(
                    path.join(dir, ent.name),
                    new Date(),
                    new Date(
                      Date.now() + Math.min(Math.max(Math.abs(mtimeDeltaAdjusted), 1000 * 60 * 15), 1000 * 60 * 60)
                    )
                  );
                }
              } catch (e) {
                // Ignore
              }
              console.log(resp.live_head, resp.error.code || resp.error, Math.round(mtimeDeltaAdjusted / 1000), id);
              if (resp.error && resp.error.code === 1801 && mtimeDeltaAdjusted < -1000 * 60 * 60 * 24 * 3) {
                console.log("Deleting...");
                fs.unlink(path.join(dir, ent.name), () => {});
              }
            }
            await throttle();
            continue;
          }
          // pendingPromises.push(
          await (async function ({ id, ent, resp, dir }) {
            const recordData =
              resp.data_url && (!resp.data || !resp.data.length)
                ? await rp({ uri: resp.data_url, encoding: null, timeout: 5000 }).catch(() =>
                    console.warn(`Failed to download data for ${id}:`, resp)
                  )
                : resp.data;
            if (!recordData || !recordData.length) {
              console.log("No data:", id);
              return;
            }
            console.log(resp.head.uuid);
            const game = resp.head;
            writeFile(
              [
                "records",
                game.uuid.split("-")[0],
                game.config.mode.mode.toString(),
                (game.config.meta.mode_id || game.config.meta.contest_uid || game.config.meta.room_id).toString(),
                game.uuid + ".json",
              ],
              JSON.stringify(resp.head)
            );
            writeFile(
              [
                "records",
                game.uuid.split("-")[0],
                game.config.mode.mode.toString(),
                (game.config.meta.mode_id || game.config.meta.contest_uid || game.config.meta.room_id).toString(),
                game.uuid + ".recordData",
              ],
              recordData
            );
            try {
              fs.unlinkSync(path.join(dir, ent.name));
            } catch (e) {
              // console.warn("Error when deleting file: ", e);
            }
          })({ id, ent, resp, dir });
          await throttle();
        }
      }
    };
    await recurseFillData(OUTPUT_DIR);
    resetWatchdog();
    if (pendingPromises.length > 0) {
      await Promise.all(pendingPromises);
    }
    resetWatchdog();
    deadline += 60000;
    console.log("Sleeping...");
    await new Promise((resolve) => setTimeout(resolve, 19000));
  } finally {
    conn.close();
    clearTimeout(timeoutToken);
  }
  // process.exit(0);
}

if (require.main === module) {
  wrappedRun(main);
}
// vim: sw=2:ts=2:expandtab:fdm=syntax
