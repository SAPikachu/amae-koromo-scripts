const { wrappedRun } = require("./entryPoint");

const fs = require("fs");
const path = require("path");

const { CouchStorage, MODE_GAME } = require("./couchStorage");

function fromEntries (iterable) {
  return [...iterable].reduce((obj, [key, val]) => {
    obj[key] = val;
    return obj;
  }, {});
}

function processDesignObject (obj, dir = process.env.OUTPUT_DIR) {
  const result = Object.entries(obj).map(([key, value]) => {
    if (key[0] === "_") {
      return undefined;
    }
    if (typeof value === "object" && value.length === undefined) {
      return [key, processDesignObject(value, path.join(dir, key))];
    } else if (typeof value === "string") {
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }
      const extension = value.indexOf("\n") > -1 ? ".js" : "";
      fs.writeFileSync(path.join(dir, key + extension), value);
      return undefined;
    }
    return [key, value];
  }).filter(x => x && x[1] !== undefined);
  if (!result.length) {
    return undefined;
  }
  return fromEntries(result);
}

async function main () {
  fs.mkdirSync(path.join(process.env.OUTPUT_DIR, "_raw/basic/_design"), { recursive: true });
  fs.mkdirSync(path.join(process.env.OUTPUT_DIR, "_raw/extended/_design"), { recursive: true });
  const storage = new CouchStorage({mode: MODE_GAME});
  for (const { doc } of (await storage.db.allDocs({
    include_docs: true,
    startkey: "_design/",
    endkey: "_design/\uffff",
  })).rows) {
    fs.writeFileSync(path.join(process.env.OUTPUT_DIR, "_raw/basic", doc._id), JSON.stringify(doc, undefined, 2));
    processDesignObject(doc, path.join(process.env.OUTPUT_DIR, doc._id));
  }
  for (const { doc } of (await storage._dbExtended.allDocs({
    include_docs: true,
    startkey: "_design/",
    endkey: "_design/\uffff",
  })).rows) {
    fs.writeFileSync(path.join(process.env.OUTPUT_DIR, "_raw/extended", doc._id), JSON.stringify(doc, undefined, 2));
    processDesignObject(doc, path.join(process.env.OUTPUT_DIR, doc._id));
  }
}

if (require.main === module) {
  wrappedRun(main);
} else {
  module.exports = {
    generateRateRanking,
  };
}

// vim: sw=2:ts=2:expandtab:fdm=syntax
