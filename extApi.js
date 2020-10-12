const express = require("express");
const Sentry = require("@sentry/node");
const cors = require("cors");
const morgan = require("morgan");
const AsyncRouter = require("express-async-router").AsyncRouter;
const axios = require("axios").default;

const { createLiveExecutor, createFinalReducer } = require("./dbExtension");
const { CouchStorage } = require("./couchStorage");
const { COUCHDB_USER, COUCHDB_PASSWORD, COUCHDB_PROTO, COUCHDB_SERVER, PLAYER_SERVERS } = require("./env");

Sentry.init({ dsn: process.env.SENTRY_DSN });

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
      return function (type, name, data) {
        if (!["show", "list"].includes(type)) {
          throw new Error("Invalid type: " + type);
        }
        return (type === "show" ? __show : __list)[name](data, {
          query: { maxage: 300 },
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

async function withRetry(func, num = 5, retryInterval = 5000) {
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      return await func();
    } catch (e) {
      if (num <= 0 || (e.response && e.response.status >= 400 && e.response.status < 500)) {
        throw e;
      }
      console.log(e);
      console.log(`Retrying (${num})`);
      await new Promise((r) => setTimeout(r, Math.random() * retryInterval));
    }
    num--;
  }
}

const parseTs = function (val) {
  "use strict";
  let result;
  try {
    if (typeof val === "string") {
      if (val.charAt(0) === "[") {
        val = JSON.parse(val);
      } else if (/^\d+$/.test(val)) {
        val = parseInt(val);
      }
    }
    if (typeof val === "object" && val.length) {
      val[1]--; // Fix month
      val = Date.UTC.apply(Date, val);
    }
    if (typeof val === "number" && val < new Date(2000, 0, 1).getTime()) {
      val = val * 1000;
    }
    result = new Date(val);
  } catch (e) {
    log("Failed to parse timestamp");
    log(e);
    return null;
  }
  if (result.toString().toLowerCase() === "invalid date") {
    return null;
  }
  return result;
};
const dateToSecKey = function (date) {
  return date.getTime() / 1000;
};
async function main() {
  console.log("Fetching design docs...");
  const render = await createRenderer();
  const basicReduce = await createFinalReducer("_meta_basic", "_design/player_stats_2", "player_stats");
  const extendedReduce = await createFinalReducer("_meta_extended", "_design/player_extended_stats", "player_stats");

  const app = express();
  app.set("trust proxy", true);
  app.use(Sentry.Handlers.requestHandler());
  app.use(cors());
  app.use(morgan("dev"));
  const router = AsyncRouter();
  const TYPES = {
    jt: { modes: [12, 16], mainDb: "majsoul_basic" },
    gold: { modes: [9], mainDb: "majsoul_gold_basic" },
  };
  const VIEWS = {
    player_stats: {
      path: "_design/basic/_view/basic?reduce=true",
      reduce: basicReduce,
      renderer: "player_stats",
    },
    player_records: {
      path: "_design/basic/_view/basic?reduce=false",
      renderer: "result_from_doc",
    },
    player_extended_stats: {
      path: "_design/extended/_view/extended?reduce=true",
      reduce: extendedReduce,
      renderer: "player_extended_stats",
    },
  };
  router.get("/:type/:view/:id/:startDate?/:endDate?", async function (req, res) {
    const availableModes = TYPES[req.params.type].modes;
    if (!availableModes) {
      return res.status(404).json({
        error: "type_not_found",
      });
    }
    const mode = req.query.mode ? parseInt(req.query.mode, 10) : 0;
    if (mode !== 0 && !availableModes.includes(mode)) {
      return res.status(400).json({
        error: "invalid_mode",
      });
    }
    const view = VIEWS[req.params.view];
    if (!view) {
      return res.status(404).json({
        error: "view_not_found",
      });
    }
    const id = parseInt(req.params.id, 10);
    if (!id) {
      return res.status(400).json({
        error: "invalid_id",
      });
    }
    const startDate = parseTs(req.params.startDate || "1");
    if (!startDate) {
      return res.status(400).json({
        error: "invalid_start_date",
      });
    }
    const endDate = req.params.endDate ? parseTs(req.params.endDate) : undefined;
    if (endDate === null) {
      return res.status(400).json({
        error: "invalid_end_date",
      });
    }
    let limit = parseInt(req.query.limit || "100", 10);
    if (!limit || limit < 0) {
      return res.status(400).json({
        error: "invalid_limit",
      });
    }
    if (limit > 500) {
      limit = 500;
    }
    const skip = parseInt(req.query.skip || "0", 10);
    if (isNaN(skip) || skip < 0) {
      return res.status(400).json({
        error: "invalid_skip",
      });
    }
    const params = {
      startkey: dateToSecKey(startDate),
      endkey: endDate ? dateToSecKey(endDate) : undefined,
      stable: "false",
      update: "lazy",
    };
    const needLocalPaging = mode === 0 && availableModes.length > 1;
    if (req.params.view === "player_records") {
      params.descending = !!req.query.descending;
      if (needLocalPaging) {
        params.limit = skip + limit;
      } else {
        params.limit = limit;
        params.skip = skip;
      }
    }
    let rows = [];
    for (const m of mode === 0 ? availableModes : [mode]) {
      try {
        const resp = await withRetry(
          () =>
            axios.get(
              `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${
                PLAYER_SERVERS[m]
              }/p${m}_${id.toString().padStart(10, "0")}/${view.path}`,
              { params }
            ),
          5,
          500
        );
        rows.splice(rows.length, 0, ...resp.data.rows);
      } catch (e) {
        if (
          !e.response ||
          (e.response.status !== 404 &&
            ((e.response.data || {}).reason || "").indexOf("No rows can match your key range") === -1)
        ) {
          throw e;
        }
      }
    }
    let rendered = null;
    if (rows.length) {
      if (req.params.view === "player_records") {
        if (needLocalPaging) {
          rows.sort((a, b) => a.key - b.key);
          if (req.query.descending) {
            rows.reverse();
          }
          rows = rows.slice(skip, skip + limit);
        }
        if (rows.length) {
          const resp = await withRetry(
            () =>
              axios.post(
                `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${COUCHDB_SERVER}/${
                  TYPES[req.params.type].mainDb
                }/_all_docs`,
                { keys: rows.map((x) => x.id), include_docs: true }
              ),
            5,
            500
          );
          rendered = render("list", view.renderer, resp.data.rows);
        }
      } else {
        const reduced = view.reduce(rows.map((x) => x.value));
        rendered = render("list", view.renderer, [{ key: [id, id, id], value: reduced }]);
      }
    }
    if (!rendered) {
      rendered = render("list", view.renderer, []);
    }
    res
      .type("json")
      .status(rendered.code)
      .set(rendered.headers || {});
    return res.end(rendered.body);
  });
  app.use("/", router);
  app.use(Sentry.Handlers.errorHandler());
  app.use(function onError(err, req, res, next) {
    if (res.headersSent) {
      return next(err);
    }
    if (err.stack) {
      console.log(err.stack);
    }
    if (err.response) {
      console.log(err.response.data);
    }
    // The error id is attached to `res.sentry` to be returned
    // and optionally displayed to the user for support.
    const sendError = function () {
      if (res.errorRedirect) {
        res.errorRedirect += res.errorRedirect.indexOf("?") > -1 ? "&" : "?";
        let code = err.code;
        if (err.body) {
          try {
            const body = JSON.parse(err.body);
            code = body.status || body.code || code;
          } catch (e) {
            /* Nothing to do */
          }
        }
        res.errorRedirect += `sentry=${res.sentry}&code=${code}&message=${encodeURIComponent(err.message)}`;
        return res.redirect(res.errorRedirect);
      }
      res.statusCode = 500;
      res.json({
        success: false,
        message: err ? err.message || err.toString() : "",
        sentry: res.sentry,
      });
    };
    sendError();
  });

  const port = parseInt(process.env.PORT, 10) || 3000;
  const host = process.env.HOST || "0.0.0.0";
  app.listen(port, host, 128, () => {
    console.log(`Listening at http://${host}:${port}`);
  });
}
if (require.main === module) {
  main().catch((e) => {
    console.error(e);
    process.exit(1);
  });
}
// vim: sw=2:ts=2:expandtab:fdm=syntax
