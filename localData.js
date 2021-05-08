"use strict";

const fs = require("fs");
const path = require("path");
const chokidar = require("chokidar");

const RECORDS_DIR = process.env.RECORDS_DIR || "records";
const DEFAULT_BASE = process.env.LOCAL_DATA_BASE || path.join(process.env.HOME, "livegames");

async function iterateLocalData(
  callback,
  baseDir = DEFAULT_BASE,
  cutoffSeconds = parseInt(process.env.LOCAL_DATA_CUTOFF)
) {
  const unfinishedIds = {};
  (function fillUnfinishedIds(dir) {
    for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
      const id = path.parse(ent.name).name;
      if (ent.isDirectory()) {
        if (/^\d+$/.test(id)) {
          fillUnfinishedIds(path.join(dir, ent.name));
        }
        continue;
      }
      if (/^\d{6}-.*\.json$/.test(ent.name)) {
        unfinishedIds[id] = true;
      }
    }
  })(baseDir);
  const cutoff = cutoffSeconds ? new Date().getTime() - cutoffSeconds * 1000 : 0;
  await (async function loadData(dir) {
    for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
      const id = path.parse(ent.name).name;
      if (ent.isDirectory()) {
        if (/^\d+$/.test(id)) {
          await loadData(path.join(dir, ent.name));
        }
        continue;
      }
      if (cutoff && fs.statSync(path.join(dir, ent.name)).mtimeMs < cutoff) {
        continue;
      }
      if (/^\d{6}-.*\.json$/.test(ent.name)) {
        await callback({
          id,
          getData: () => JSON.parse(fs.readFileSync(path.join(dir, ent.name), { encoding: "utf-8" })),
          getRecordData: () => fs.readFileSync(path.join(dir, id + ".recordData")),
        });
      }
    }
  })(path.join(baseDir, RECORDS_DIR));
}

async function watchLiveData(callback, baseDir = DEFAULT_BASE) {
  let watcher;
  function handle(p) {
    const basename = path.basename(p);
    if (/^\d{6}-.*\.recordData$/.test(basename)) {
      // watcher.unwatch(p);
      const components = path.parse(p);
      const id = components.name;
      let data;
      try {
        data = JSON.parse(fs.readFileSync(path.join(components.dir, id + ".json"), { encoding: "utf-8" }));
      } catch (e) {
        return;
      }
      callback({
        id,
        getData: () => data,
        getRecordData: () => fs.readFileSync(p),
      });
    }
  }
  watcher = chokidar
    .watch(path.join(baseDir, RECORDS_DIR), {
      persistent: true,
      awaitWriteFinish: true,
      ignoreInitial: true,
      ignored: ["**/*.json", "**/2004*", "**/200501", "**/200502", "**/200503"],
    })
    .on("add", handle)
    .on("change", handle);
  await new Promise(() => {});
}

async function watchLiveData2(callback, baseDir = DEFAULT_BASE) {
  const watchers = {};
  const debouncers = {};
  function handle(eventType, p) {
    if (p.endsWith(".recordData")) {
      if (p in debouncers) {
        clearTimeout(debouncers[p]);
      }
      debouncers[p] = setTimeout(function () {
        try {
          clearTimeout(debouncers[p]);
          delete debouncers[p];
          const components = path.parse(p);
          const id = components.name;
          let data;
          try {
            data = JSON.parse(fs.readFileSync(path.join(components.dir, id + ".json"), { encoding: "utf-8" }));
          } catch (e) {
            return;
          }
          callback({
            id,
            getData: () => data,
            getRecordData: () => fs.readFileSync(p),
          });
        } catch (e) {
          console.error(e);
        }
      }, 2000);
    } else if (!p.includes(".")) {
      let isNew = false;
      try {
        if (fs.statSync(p).isDirectory()) {
          isNew = addDir(p);
        }
      } catch (e) {
        if (watchers[p]) {
          watchers[p].close();
          delete watchers[p];
        }
      }
      if (isNew) {
        setTimeout(function () {
          const files = fs.readdirSync(p, { withFileTypes: true });
          if (files.length > 20) {
            return;
          }
          for (const ent of files) {
            handle("add", path.join(p, ent.name));
          }
        }, 2000);
      }
    }
  }
  function addDir(dir) {
    if (watchers[dir]) {
      return false;
    }
    console.log(`Watching ${dir}`);
    try {
      watchers[dir] = fs
        .watch(
          dir,
          {
            persistent: true,
            encoding: "utf-8",
          },
          (t, p) => handle(t, path.join(dir, p))
        )
        .on("error", (e) => console.error(e));
      for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
        if (!ent.isDirectory()) {
          continue;
        }
        addDir(path.join(dir, ent.name));
      }
    } catch (e) {
      console.error("Failed to add watcher:", e);
    }
    return true;
  }
  addDir(path.join(baseDir, RECORDS_DIR));
  await new Promise(() => {});
}
exports.iterateLocalData = iterateLocalData;
exports.watchLiveData = watchLiveData2;
exports.DEFAULT_BASE = DEFAULT_BASE;

if (require.main === module) {
  watchLiveData2((item) => {
    console.log(item.id);
    item.getRecordData();
  });
}
