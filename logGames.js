const { wrappedRun } = require("./entryPoint");

const fs = require("fs");
const path = require("path");
const rp = require("request-promise");
const lockfile = require("proper-lockfile");

const { createMajsoulConnection } = require("./majsoul");

const MODES = [216, 215, 225, 226, 224, 223, 212, 211, 208, 209, 221, 222];
const RECORD_MODES = [16, 15, 25, 26, 24, 23, 12, 11, 8, 9, 21, 22];
const OUTPUT_DIR = process.env.OUTPUT_DIR || path.resolve("livegames");

const LOCK_DIR = "/run/lock/amae-koromo.lock/";

const SECOND = 1000;
const MINUTE = SECOND * 60;
const HOUR = MINUTE * 60;
const DAY = HOUR * 24;

function once(func) {
  let called = false;
  return function () {
    if (called) {
      return;
    }
    called = true;
    return func.apply(this, arguments);
  };
}

function tryLock(id) {
  fs.mkdirSync(LOCK_DIR, { recursive: true });
  try {
    return once(
      lockfile.lockSync(id, {
        stale: 10 * 60 * 1000,
        lockfilePath: path.join(LOCK_DIR, path.parse(id).name),
      })
    );
  } catch (e) {
    return null;
  }
}

function writeFile(fileNameParts, data, mtime) {
  const target = path.join(OUTPUT_DIR, ...fileNameParts);
  fs.mkdirSync(path.dirname(target), { recursive: true });
  const targetTemp = target + "." + Date.now() + Math.random().toString().slice(2) + ".tmp";
  const fd = fs.openSync(targetTemp, "w");
  fs.writeSync(fd, data);
  fs.fsyncSync(fd);
  fs.closeSync(fd);
  if (mtime) {
    fs.utimesSync(targetTemp, mtime, mtime);
  }
  fs.renameSync(targetTemp, target);
  // fs.writeFileSync(path.join(target), data);
}

let pendingDb;
function logPendingRecord(fileNameParts) {
  if (pendingDb === null) {
    return;
  }
  if (!pendingDb) {
    try {
      pendingDb = new require("better-sqlite3")(path.join(OUTPUT_DIR, "pending2.sqlite3"), { timeout: 15000 });
      process.on("exit", () => pendingDb.close());
      pendingDb.pragma("journal_mode = WAL");
      pendingDb.pragma("synchronous = NORMAL");
      pendingDb.pragma("busy_timeout = 5000");
      pendingDb.exec(`
        CREATE TABLE IF NOT EXISTS pending (
          path TEXT NOT NULL,
          updated INT NOT NULL
        ) STRICT;
      `);
    } catch (e) {
      pendingDb = null;
      console.warn("Failed to initialize pending db:", e);
      return;
    }
  }
  pendingDb.prepare("INSERT INTO pending VALUES (?, ?)").run(path.join(OUTPUT_DIR, ...fileNameParts), Date.now());
}

const getIdLogDb = (function () {
  let db;
  const getDb = function () {
    if (db === null) {
      return db;
    }
    if (!db) {
      try {
        db = new require("better-sqlite3")(path.join(OUTPUT_DIR, "idlog.sqlite3"), { timeout: 15000 });
        process.on("exit", () => db.close());
        db.pragma("journal_mode = WAL");
        db.pragma("synchronous = NORMAL");
        db.exec(`
        CREATE TABLE IF NOT EXISTS idlog (
          id TEXT NOT NULL PRIMARY KEY ON CONFLICT IGNORE,
          start_time INT NOT NULL
        ) STRICT;
        CREATE INDEX IF NOT EXISTS idlog__start_time ON idlog (
          start_time
        );
      `);
        db.prepare("DELETE FROM idlog WHERE start_time < ?").run(Math.floor((Date.now() - DAY * 90) / 1000));
      } catch (e) {
        db = null;
        console.warn("Failed to initialize IDLog db:", e);
        return;
      }
    }
    return db;
  };
  return function () {
    const db = getDb();
    const queryStmt = db ? db.prepare("SELECT start_time FROM idlog WHERE id = ? LIMIT 1") : null;
    const insertStmt = db ? db.prepare("INSERT INTO idlog (id, start_time) VALUES (?, ?)") : null;
    return {
      available: () => !!db,
      seen: (id) => {
        if (!queryStmt) {
          return false;
        }
        return !!queryStmt.get(id);
      },
      insert: (id, startTime) => {
        if (insertStmt) {
          try {
            insertStmt.run(id, startTime);
          } catch (e) {
            console.error("Failed to insert entry to IDLog:", e);
          }
        }
      },
    };
  };
})();

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
  deadline = new Date().getTime() + SECOND * 230;
  const liveGames = {};
  const releaseLocks = [];
  try {
    if (process.env.LIST_ONLY) {
      const idlog = getIdLogDb();
      while (true) {
        const startTime = Date.now();
        for (const mode of shuffle(MODES)) {
          resetWatchdog();
          const tsBefore = Date.now();
          const resp = await conn.rpcCall(".lq.Lobby.fetchGameLiveList", {
            filter_id: mode,
          });
          const tsAfterRequest = Date.now();
          let timeWriteFile = 0;
          for (const game of resp.live_list) {
            if (!/\d{6}-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/.test(game.uuid)) {
              console.warn("Invalid game ID:", game.uuid);
              continue;
            }
            if (game.start_time < new Date().getTime() / 1000 - 60 * 60 * 1 && idlog.seen(game.uuid)) {
              continue;
            }
            const fileNameParts = [game.uuid.split("-")[0], mode.toString(), game.uuid + ".json"];
            const ts = new Date(Math.max(game.start_time * 1000 + 20 * MINUTE, Date.now() + 6 * MINUTE));
            if (game.start_time < new Date().getTime() / 1000 - 60 * 60 * 36) {
              // 36h ago, stale
              continue;
            }
            liveGames[game.uuid] = game;
            const tsBeforeWriteFile = Date.now();
            writeFile(fileNameParts, JSON.stringify(game), ts);
            timeWriteFile += Date.now() - tsBeforeWriteFile;
            idlog.insert(game.uuid, game.start_time || Math.floor(Date.now() / 1000));
          }
          console.log(
            `${mode}/${resp.live_list.length}/${tsAfterRequest - tsBefore}/${
              Date.now() - tsAfterRequest
            }/${timeWriteFile}`
          );
        }
        if (!process.env.LIST_ONLY) {
          break;
        } else {
          deadline += 60000;
        }
        const elapsed = Date.now() - startTime;
        if (elapsed < 7000) {
          const sleepTime = 7000 - elapsed;
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
    if (process.env.MAJSOUL_URL_BASE === "https://mahjongsoul.game.yo-star.com/") {
      console.log("int server disabled");
      await new Promise((resolve) => setTimeout(resolve, 30000));
      return;
    }
    const pendingPromises = [];
    let locked = 0,
      skipped = 0,
      notFinished = 0,
      completed = 0;
    const ents = [];
    const printStats = () => console.log(`L=${locked}/S=${skipped}/NF=${notFinished}/C=${completed}/T=${ents.length}`);
    process.on("exit", printStats);
    const MTIME_CUTOFF = (parseInt(process.env.MTIME_CUTOFF, 10) || DEFAULT_MTIME_CUTOFF) * -1;
    const PRIORITY_CUTOFF = 1000 * 60 * 60 * 24 * 365 * 1;
    const ts = new Date().getTime();
    let startTime = Date.now() - 1000;
    async function throttle() {
      if (Date.now() - startTime < 1000) {
        await new Promise((resolve) => setTimeout(resolve, 1000 - (Date.now() - startTime)));
      }
      startTime = Date.now();
    }
    function updateTimestamp(dir, ent, minutes = 15) {
      fs.utimesSync(path.join(dir, ent.name), new Date(), new Date(Date.now() + MINUTE * minutes));
    }
    async function processEntry(ent) {
      const dir = ent._dir;
      const id = path.parse(ent.name).name;
      const fullName = path.join(dir, ent.name);
      const releaseLock = tryLock(fullName);
      if (!releaseLock) {
        skipped++;
        return;
      }
      locked++;
      try {
        const stat = fs.statSync(fullName);
        ent.mtimeDelta = stat.mtimeMs - ts;
        ent.birthtimeDelta = stat.birthtimeMs - ts;
      } catch (e) {
        // Probably deleted by other process
        return;
      }
      if (ent.mtimeDelta > MTIME_CUTOFF && !ent.prioritized) {
        releaseLock();
        skipped++;
        return;
      }
      releaseLocks.push(releaseLock);
      await throttle();
      resetWatchdog();
      const realStart = Date.now();
      let resp = await conn.rpcCall(".lq.Lobby.fetchGameRecord", {
        game_uuid: id,
        client_version_string: conn.clientVersionString,
      });
      if ((!resp.data && !resp.data_url) || !resp.head) {
        const head = resp.head;
        const errorCode = resp.error?.code || resp.error;
        if (ent.birthtimeDelta > -HOUR * 6) {
          console.log(
            `R E${typeof errorCode === "number" ? errorCode : JSON.stringify(errorCode)} ${(
              (Date.now() - realStart) /
              1000
            ).toFixed(1)}s ${(ent.birthtimeDelta / HOUR).toFixed(1)}h ${id}`
          );
          updateTimestamp(dir, ent);
          notFinished++;
          return;
        }
        let mtimeDeltaAdjusted = ent.mtimeDelta;
        console.log(
          head,
          errorCode,
          Math.round(mtimeDeltaAdjusted / 1000),
          (ent.birthtimeDelta / DAY).toFixed(1) + "d",
          id
        );
        if (errorCode === 511 && ent.birthtimeDelta < -HOUR * 48) {
          console.log(`Deleting ${id}...`);
          fs.unlink(fullName, () => {});
        } else {
          updateTimestamp(dir, ent, 60);
        }
        notFinished++;
        return;
      }
      if (resp.head.end_time && Date.now() - resp.head.end_time * 1000 < 5 * MINUTE) {
        console.log("FW:", id);
        fs.utimesSync(
          fullName,
          new Date(resp.head.end_time * 1000 - PRIORITY_CUTOFF + MINUTE * 5.5),
          new Date(Date.now() + MINUTE * 5)
        );
        notFinished++;
        releaseLock();
        return;
      }
      if (!RECORD_MODES.includes(resp?.head?.config?.meta?.mode_id)) {
        console.warn(`Unexpected mode ${resp?.head?.config?.meta?.mode_id}: $${id}`);
        fs.unlink(fullName, () => {});
        notFinished++;
        return;
      }
      // pendingPromises.push(
      await (async function ({ id, ent, resp, dir }) {
        const game = resp.head;
        if (!/\d{6}-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/.test(game.uuid)) {
          console.warn("Invalid game ID:", game.uuid);
          return;
        }
        const recordData =
          resp.data_url && (!resp.data || !resp.data.length)
            ? await rp({
                uri: resp.data_url,
                encoding: null,
                timeout: 5000,
              }).catch(() => console.warn(`Failed to download data for ${id}:`, resp))
            : resp.data;
        if (!recordData || !recordData.length) {
          console.log("No data:", id);
          const diff = resp.head.end_time ? Date.now() - resp.head.end_time * 1000 : 0;
          if (diff < 15 * MINUTE) {
            fs.utimesSync(
              fullName,
              new Date(Date.now() - PRIORITY_CUTOFF + MINUTE * 15),
              new Date(Date.now() + MINUTE * 15)
            );
          } else if (diff > 3 * DAY) {
            console.log(`Deleting ${id}...`);
            fs.unlink(fullName, () => {});
          } else {
            updateTimestamp(dir, ent, 60 * 8);
          }
          notFinished++;
          return;
        }
        const diff = Date.now() / 1000 - game.end_time;
        console.log(
          `+${Math.floor(diff / 3600)}:${Math.floor((diff % 3600) / 60)
            .toString()
            .padStart(2, "0")}`,
          `${((Date.now() - realStart) / 1000).toFixed(1)}s`,
          (game.config.meta.mode_id || game.config.meta.contest_uid || game.config.meta.room_id)
            .toString()
            .padStart(2, " "),
          resp.head.uuid
        );
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
        logPendingRecord([
          "records",
          game.uuid.split("-")[0],
          game.config.mode.mode.toString(),
          (game.config.meta.mode_id || game.config.meta.contest_uid || game.config.meta.room_id).toString(),
          game.uuid,
        ]);
        try {
          fs.unlinkSync(fullName);
        } catch (e) {
          // console.warn("Error when deleting file: ", e);
        }
        completed++;
      })({ id, ent, resp, dir });
    }
    const recurseFillData = async function (dir) {
      if (dir !== OUTPUT_DIR && !/^\d+$/.test(path.basename(dir))) {
        return;
      }
      if (ents.length > 200000) {
        return;
      }
      let entries;
      try {
        entries = fs.readdirSync(dir, { withFileTypes: true });
      } catch (e) {
        console.error("recurseFillData: error when calling readdirSync:", e);
        return;
      }
      for (const ent of entries) {
        try {
          const stat = fs.statSync(path.join(dir, ent.name));
          ent.mtimeDelta = stat.mtimeMs - ts;
          ent.atimeDelta = stat.atimeMs - ts;
          ent.birthtimeDelta = stat.birthtimeMs - ts;
        } catch (e) {
          // Probably deleted by other process, this will be ignored below
          ent.mtimeDelta = 0;
          ent.atimeDelta = 0;
          ent.birthtimeDelta = 0;
        }
        if (ent.isDirectory()) {
          ent.sortKey = Math.random();
        } else if (ent.atimeDelta < -PRIORITY_CUTOFF) {
          // console.log(`P A ${ent.name}`);
          ent.sortKey = ent.atimeDelta;
          ent.prioritized = true;
        } else if (ent.mtimeDelta < -PRIORITY_CUTOFF) {
          // console.log(`P M ${ent.name}`);
          ent.sortKey = ent.mtimeDelta;
          ent.prioritized = true;
        } else {
          // Favor older games up to 2h, then sort randomly
          ent.sortKey = Math.max(
            Math.max(ent.mtimeDelta, ent.birthtimeDelta + 20 * MINUTE),
            -1000 * 60 * 60 * 2 - Math.random() * 1000
          );
        }
      }
      // entries.sort((a, b) => a.sortKey - b.sortKey);
      shuffle(entries);
      for (const ent of entries) {
        if (ent.isDirectory()) {
          await recurseFillData(path.join(dir, ent.name));
          continue;
        }
        if (path.extname(ent.name) === ".json") {
          const id = path.parse(ent.name).name;
          if (id in liveGames) {
            skipped++;
            continue;
          }
          ent._dir = dir;
          // await processEntry(ent);
          ents.push(ent);
        }
      }
    };
    await recurseFillData(OUTPUT_DIR);
    ents.sort((a, b) => a.sortKey - b.sortKey);
    // shuffle(ents);
    resetWatchdog();
    for (const ent of ents) {
      await processEntry(ent);
    }
    resetWatchdog();
    if (pendingPromises.length > 0) {
      await Promise.all(pendingPromises);
    }
    resetWatchdog();
    deadline += 600000;
    process.off("exit", printStats);
    printStats();
    console.log("Sleeping...");
    for (;;) {
      resetWatchdog();
      await new Promise((resolve) => setTimeout(resolve, 10000 + 5000 * Math.random()));
      if (Math.random() < 0.5) {
        break;
      }
    }
    releaseLocks.forEach((x) => x());
    releaseLocks.splice(0, releaseLocks.length);
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
