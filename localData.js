"use strict";

const fs = require("fs");
const path = require("path");

const RECORDS_DIR = "records";

async function iterateLocalData (callback, baseDir = process.env.LOCAL_DATA_BASE || path.join(process.env.HOME, "livegames")) {
  const unfinishedIds = {};
  (function fillUnfinishedIds (dir) {
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
  await (async function loadData (dir) {
    for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
      const id = path.parse(ent.name).name;
      if (ent.isDirectory()) {
        if (/^\d+$/.test(id)) {
          await loadData(path.join(dir, ent.name));
        }
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

exports.iterateLocalData = iterateLocalData;

if (require.main === module) {
  iterateLocalData((item) => {
    console.log(item.id);
    item.getRecordData();
  });
}
