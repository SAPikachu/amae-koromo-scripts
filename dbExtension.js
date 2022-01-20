const vm = require("vm");

const { wrappedRun } = require("./entryPoint");
const { CouchStorage, MODE_GAME } = require("./couchStorage");
const { COUCHDB_USER, COUCHDB_PASSWORD, COUCHDB_PROTO } = require("./env");

async function createExecutor(storage, docName, codeGetter) {
  const doc = await storage._db.get(docName);
  const code = codeGetter(doc);
  const script = new vm.Script(`__output = (function() { "use strict"; return ${code}; })()(...__input);`, {
    fileName: `${docName}.vm`,
  });
  let context = {
    toJSON: JSON.stringify.bind(JSON),
    log: console.log.bind(console),
  };
  context.global = context;
  context = vm.createContext(context);
  return function (...args) {
    context.__input = args;
    context.__output = undefined;
    script.runInContext(context);
    return context.__output;
  };
}

async function createLiveExecutor(storage, docName, codeGetter) {
  let execute = await createExecutor(storage, docName, codeGetter);
  let handle = null;
  let seq = "now";
  function refreshListener() {
    handle = storage._db
      .changes({
        live: true,
        since: seq,
        timeout: 30000,
        heartbeat: false,
        doc_ids: [docName],
      })
      .on("change", function (info) {
        console.log(`Changed: ${docName}, recompiling...`);
        seq = info.seq;
        createExecutor(storage, docName, codeGetter)
          .then((newExec) => (execute = newExec))
          .catch((e) => {
            console.error(`Failed to create executor for changed doc ${docName}:`, e);
          });
      })
      .on("error", function (e) {
        console.error(`Failed to get changes for doc ${docName}:`, e);
        handle.cancel();
        setTimeout(refreshListener, 5000);
      });
  }
  refreshListener();
  const ret = function (...args) {
    return execute(...args);
  };
  ret.cancel = () => handle.cancel();
  return ret;
}

async function createFinalReducer(dbSuffix, docName, viewName) {
  const storage = new CouchStorage({ suffix: dbSuffix });
  const reduce = await createLiveExecutor(storage, docName, (doc) => doc.views[viewName].reduce);
  const ret = (values) => reduce(null, values, true);
  ret.cancel = () => reduce.cancel();
  return ret;
}

async function createMapper(dbSuffix, docName, viewName) {
  const storage = new CouchStorage({ suffix: dbSuffix });
  function mapWrapper(doc) {
    "use strict";
    const emitted = [];
    const emit = function (key, value) {
      emitted.push([key, value]);
    };
    __FUNCTION__(doc);
    return emitted;
  }
  const map = await createLiveExecutor(storage, docName, (doc) =>
    mapWrapper.toString().replace("__FUNCTION__", `(${doc.views[viewName].map})`)
  );
  return map;
}

async function createLiveDocGetter(dbSuffix, docName) {
  const storage = new CouchStorage({ suffix: dbSuffix });
  let doc = await storage.db.get(docName);
  let handle = null;
  let seq = "now";
  function refreshListener() {
    handle = storage._db
      .changes({
        live: true,
        since: seq,
        heartbeat: false,
        timeout: 30000,
        doc_ids: [docName],
      })
      .on("change", function (info) {
        console.log(`Changed: ${docName}, fetching...`);
        seq = info.seq;
        storage.db
          .get(docName)
          .then((newDoc) => (doc = newDoc))
          .catch((e) => {
            console.error(`Failed to fetch changed doc ${docName}:`, e);
          });
      })
      .on("error", function (e) {
        console.error(`Failed to get changes for doc ${docName}:`, e);
        handle.cancel();
        setTimeout(refreshListener, 5000);
      });
  }
  refreshListener();
  const ret = function () {
    return doc;
  };
  ret.cancel = () => handle.cancel();
  return ret;
}

async function createRenderer() {
  const storage = new CouchStorage({ suffix: "_meta_basic" });
  const render = await createLiveExecutor(storage, "_design/renderers", function (doc) {
    const wrapLib = function () {
      const exports = {};
      const module = { exports };

      __CODE__;

      return module.exports;
    };
    const wrapList = function (rows, req) {
      let ret = {
        code: 200,
        headers: {},
        body: "",
      };
      const start = function (params) {
        Object.assign(ret, {
          ...params,
          headers: {
            ...ret.headers,
            ...(params.headers || {}),
          },
        });
        if (ret.json) {
          ret.body = ret.json;
          delete ret.json;
        }
      };
      global.start = start;
      const getRow = function () {
        if (!rows.length) {
          return null;
        }
        return rows.shift();
      };
      global.getRow = getRow;
      const send = function (data) {
        ret.body += data;
      };
      global.send = send;
      const retBody = __CODE__(null, req);
      ret.body += retBody;
      return ret;
    };
    const require = function (name) {
      return __lib[name.replace(/^views\/lib\//, "")]();
    };
    const entryPoint = function () {
      const sum = function (arr) {
        return arr.reduce((acc, cur) => acc + cur, 0);
      };
      __CODE__;
      return function (type, name, data, query) {
        if (!["show", "list"].includes(type)) {
          throw new Error("Invalid type: " + type);
        }
        return (type === "show" ? __show : __list)[name](data, {
          query: { maxage: 300, ...(query || {}) },
          method: "GET",
          headers: {},
        });
      };
    };

    const lib = `const __lib = {${Object.entries(doc.views.lib)
      .map(([k, v]) => k + ": " + wrapLib.toString().replace("__CODE__", v))
      .join(", ")}};`;
    const show = `const __show = {${Object.entries(doc.shows)
      .map(([k, v]) => k + ": " + v)
      .join(", ")}};`;
    const list = `const __list = {${Object.entries(doc.lists)
      .map(([k, v]) => k + ": " + wrapList.toString().replace("__CODE__", v))
      .join(", ")}};`;
    const code = [`const require = ${require.toString()}`, lib, show, list].join(";\n");
    return `(${entryPoint.toString().replace("__CODE__;", code)})()`;
  });
  return render;
}

/*
async function main() {
  const basicReduce = await createFinalReducer("_meta_basic", "_design/player_stats_2", "player_stats");
  const extendedReduce = await createFinalReducer("_meta_extended", "_design/player_extended_stats", "player_stats");
  const basicDocs = [];
  const extendedDocs = [];
  for (const mode of [12, 16]) {
    const storage = new CouchStorage({
      uri: `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@127.0.0.1:5985/p${mode}_${"6063".padStart(10, "0")}`,
    });
    basicDocs.push((await storage._db.query("basic/basic", { reduce: true })).rows[0].value);
    extendedDocs.push((await storage._db.query("extended/extended", { reduce: true })).rows[0].value);
  }
  console.log(basicReduce(basicDocs));
  console.log(extendedReduce(extendedDocs));
}
*/
async function main() {
  const docGetter = await createLiveDocGetter("_meta_ext", "player_docs");
  console.log(docGetter());
}
if (require.main === module) {
  wrappedRun(main);
} else {
  module.exports = {
    createExecutor,
    createLiveExecutor,
    createFinalReducer,
    createMapper,
    createLiveDocGetter,
    createRenderer,
  };
}
// vim: sw=2:ts=2:expandtab:fdm=syntax
