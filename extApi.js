require("newrelic");

const express = require("express");
const rateLimit = require("express-rate-limit");
const assert = require("assert");
const Sentry = require("@sentry/node");
const cors = require("cors");
const morgan = require("morgan");
const _ = require("lodash");
const AsyncRouter = require("express-async-router").AsyncRouter;
const axios = require("axios").default;

const { createLiveExecutor, createFinalReducer } = require("./dbExtension");
const { CouchStorage, generateCompressedId } = require("./couchStorage");
const { COUCHDB_USER, COUCHDB_PASSWORD, COUCHDB_PROTO, COUCHDB_SERVER, PLAYER_SERVERS } = require("./env");

Sentry.init({ dsn: process.env.SENTRY_DSN });

if (!Array.prototype.flat) {
  Object.defineProperty(Array.prototype, "flat", {
    configurable: true,
    value: function flat() {
      var depth = isNaN(arguments[0]) ? 1 : Number(arguments[0]);

      return depth
        ? Array.prototype.reduce.call(
            this,
            function (acc, cur) {
              if (Array.isArray(cur)) {
                acc.push.apply(acc, flat.call(cur, depth - 1));
              } else {
                acc.push(cur);
              }

              return acc;
            },
            []
          )
        : Array.prototype.slice.call(this);
    },
    writable: true,
  });
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

async function withRetry(func, num = 5, retryInterval = 5000) {
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      return await func();
    } catch (e) {
      if (num <= 0 || (e.response && e.response.status >= 400 && e.response.status < 500)) {
        throw e;
      }
      console.log(e.response && e.response.data ? e.response.data : e);
      console.log(`Retrying (${num})`);
      await new Promise((r) => setTimeout(r, Math.random() * retryInterval));
    }
    num--;
  }
}

const MS_PER_DAY = 1000 * 60 * 60 * 24;
const SEC_PER_HOUR = 60 * 60;
const parseTs = function (val) {
  "use strict";
  if (val === undefined) {
    return undefined;
  }
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
function generateKey(date) {
  let uuid = "000000-########-####-####-####-############";
  const msTime = date.getTime();
  if (msTime / 1000 >= 0x0ffffffff) {
    return '"~"';
  }
  uuid = uuid.replace(/#/g, msTime % 1000 < 500 ? "0" : "f");
  return `"${generateCompressedId(uuid, Math.floor(msTime / 1000))}"`;
}
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
  const limiter = rateLimit({
    windowMs: 1 * 60 * 1000, // 1 minute
    message: "Too many requests, please contact SAPikachu if you want to use large amount of the data",
    max: function (req, res) {
      if (/.*(axios|python-requests.*)/.test(req.header("user-agent") || "axios") && req.path.includes("/games/")) {
        return 5;
      }
      return 0;
    },
  });
  app.use(limiter);

  const router = AsyncRouter();
  const TYPES = {
    jt: { modes: [12, 16], mainDb: "majsoul_basic" },
    gold: { modes: [9], mainDb: "majsoul_gold_basic" },
    sanma: { modes: [22, 24, 26], mainDb: "majsoul_sanma_basic" },
    e4: { modes: [8, 11, 15], mainDb: "majsoul_e4_basic" },
    e3: { modes: [21, 23, 25], mainDb: "majsoul_e3_basic" },
  };
  const MODE_DBS = {};
  Object.keys(TYPES).forEach((t) => TYPES[t].modes.forEach((mode) => (MODE_DBS[mode] = TYPES[t].mainDb)));
  const V2_TYPES = {
    pl4: { modes: [9, 12, 16, 8, 11, 15] },
    pl3: { modes: [22, 24, 26, 21, 23, 25] },
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
  const GLOBAL_VIEWS = {
    rank_rate_by_seat: {
      dbSuffix: "_basic",
      path: "_design/rank_rate_by_seat_2/_view/rank_rate_by_seat",
      params: {
        group: "true",
        stable: "false",
        update: "lazy",
      },
      renderType: "list",
      renderer: "rank_rate_by_seat",
    },
    fan_stats: {
      dbSuffix: "_extended",
      path: "_design/fan_stats_2/_view/fan_stats",
      params: {
        group_level: "2",
        stable: "false",
        update: "lazy",
      },
      renderType: "list",
      renderer: "fan_stats",
    },
    global_statistics: {
      dbSuffix: "_extended",
      path: "global_statistics",
      params: {
        group_level: "2",
        stable: "false",
        update: "lazy",
      },
      renderType: "show",
      renderer: "global_statistics",
    },
  };
  const GLOBAL_DOC_VIEWS = {
    global_statistics: {
      db: {
        pl4: "majsoul_aggregates",
        pl3: "majsoul_sanma_aggregates",
      },
      path: "global_statistics",
      renderer: "global_statistics",
    },
    global_statistics_2: {
      db: {
        pl4: "majsoul_aggregates",
        pl3: "majsoul_sanma_aggregates",
      },
      path: "global_statistics",
      renderer: "global_statistics",
    },
    player_delta_ranking: {
      db: {
        pl4: "majsoul_aggregates",
        pl3: "majsoul_sanma_aggregates",
      },
      path: (subView) => `player_delta_ranking_${subView}`,
      renderer: "generic_data",
    },
    career_ranking: {
      db: {
        pl4: "majsoul_aggregates",
        pl3: "majsoul_sanma_aggregates",
      },
      path: (subView) => `career_ranking_${encodeURIComponent(subView)}`,
      renderer: "career_ranking",
    },
  };
  router.get("/v2/:type/games/:startDate/:endDate", async function (req, res) {
    if (!MODE_DBS[req.query.mode]) {
      return res.status(404).json({
        error: "mode_not_found",
      });
    }
    if (req.query.skip) {
      return res.status(400).json({
        error: "skip_is_not_supported",
      });
    }
    if (req.query.descending) {
      Object.assign(req.params, {
        startDate: req.params.endDate,
        endDate: req.params.startDate,
      });
    }
    const startDate = parseTs(req.params.startDate || "1");
    if (!startDate) {
      return res.status(400).json({
        error: "invalid_start_date",
      });
    }
    const endDate = parseTs(req.params.endDate);
    if (!req.params.endDate || !endDate) {
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
    const params = {
      selector: {
        $and: [
          {
            _id: { $gte: JSON.parse(generateKey(startDate)) },
          },
          {
            _id: { $lt: JSON.parse(generateKey(endDate)) + "\ufff0" },
          },
          {
            "config.meta.mode_id": parseInt(req.query.mode, 10),
          },
        ],
      },
      sort: [{ _id: req.query.descending ? "desc" : "asc" }],
      limit,
      update: false,
      stable: false,
      execution_stats: true,
    };
    const resp = await withRetry(
      () =>
        axios.post(
          `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${COUCHDB_SERVER}/${MODE_DBS[req.query.mode]}/_find`,
          params
        ),
      5,
      500
    );
    const rendered = render(
      "list",
      "result_from_doc",
      (resp.data.docs || []).map((x) => ({ doc: x, key: x._id, id: x._id }))
    );
    if (params.descending && resp.data.docs && resp.data.docs.length > limit * 0.9) {
      // Optimize caching
      let hourMark = null;
      const cutoff = new Date().getTime() / 1000 - SEC_PER_HOUR * 3; // Only applies to older games
      let games = JSON.parse(rendered.body);
      for (let i = 0; i < games.length; i++) {
        const game = games[i];
        if (game.startTime > cutoff) {
          continue;
        }
        if (hourMark === null) {
          hourMark = Math.floor(game.startTime / SEC_PER_HOUR);
        } else if (Math.abs(Math.floor(game.startTime / SEC_PER_HOUR) - hourMark) > 0.001) {
          games = games.slice(0, i + 1);
          rendered.body = JSON.stringify(games);
          break;
        }
      }
    }
    rendered.headers["Cache-Control"] = "public, max-age=86400, stale-while-revalidate=600, stale-if-error=600";
    res
      .type("json")
      .status(rendered.code)
      .set(rendered.headers || {});
    return res.send(rendered.body).end();
  });
  router.get("/v2/:type/:view(rank_rate_by_seat|fan_stats)", async function (req, res) {
    if (!V2_TYPES[req.params.type]) {
      return res.status(404).json({
        error: "type_not_found",
      });
    }
    const view = GLOBAL_VIEWS[req.params.view];
    if (!view) {
      return res.status(404).json({
        error: "view_not_found",
      });
    }
    let rows = [];
    await Promise.all(
      _(V2_TYPES[req.params.type].modes)
        .map((x) => MODE_DBS[x])
        .uniq()
        .value()
        .map(async (db) => {
          const resp = await withRetry(
            () =>
              axios.get(
                `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${COUCHDB_SERVER}/${db.replace(
                  /_basic$/,
                  view.dbSuffix
                )}/${view.path}`,
                {
                  params: view.params,
                }
              ),
            5,
            500
          );
          rows = rows.concat(resp.data.rows || []);
        })
    );
    const rendered = render("list", view.renderer, rows);
    rendered.headers["Cache-Control"] = "public, max-age=3600, stale-while-revalidate=600, stale-if-error=600";
    res
      .type("json")
      .status(rendered.code)
      .set(rendered.headers || {});
    return res.send(rendered.body).end();
  });
  router.get(
    "/v2/:type/:view(global_statistics|global_statistics_2|player_delta_ranking|career_ranking)/:subView?",
    async function (req, res) {
      if (!V2_TYPES[req.params.type]) {
        return res.status(404).json({
          error: "type_not_found",
        });
      }
      const view = GLOBAL_DOC_VIEWS[req.params.view];
      if (!view) {
        return res.status(404).json({
          error: "view_not_found",
        });
      }
      let path = view.path;
      if (typeof path === "function") {
        path = path(req.params.subView);
      }
      const modes = (req.query.mode || "").split(".");
      if (new Set(modes).size !== modes.length) {
        return res.status(400).json({
          error: "duplicated_mode",
        });
      }
      const dbs = Array.from(
        new Set(
          modes.map((mode) => {
            if (!req.query.mode || req.query.mode.toString() === "0") {
              return view.db[req.params.type];
            }
            if (!V2_TYPES[req.params.type].modes.includes(parseInt(mode, 10))) {
              return null;
            }
            const mainDb = MODE_DBS[mode];
            if (!mainDb) {
              return null;
            }
            return mainDb;
          })
        )
      );
      if (dbs.some((x) => !x)) {
        return res.status(400).json({
          error: "invalid_mode_id",
        });
      }
      if (modes.length > 1 && req.params.view !== "global_statistics_2") {
        if (modes.some((x) => !x || x.toString() === "0")) {
          return res.status(400).json({
            error: "invalid_mode_all",
          });
        }
        if (dbs.length !== 1) {
          return res.status(400).json({
            error: "invalid_mode_combination",
          });
        }
        if (Object.values(MODE_DBS).filter((x) => x === dbs[0]).length !== modes.length) {
          return res.status(400).json({
            error: "invalid_mode_combination",
          });
        }
      }

      let rendered;
      if (req.params.view === "global_statistics_2") {
        if (!modes.length || modes.some((x) => !x || x.toString() === "0")) {
          return res.status(400).json({
            error: "invalid_mode",
          });
        }
        const resp = await Promise.all(
          dbs.map((db) =>
            withRetry(
              () =>
                axios.get(
                  `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${COUCHDB_SERVER}/${db.replace(
                    /_basic$/,
                    "_aggregates"
                  )}/${path}`
                ),
              5,
              500
            )
          )
        );
        const data = resp
          .map((x) => x.data.data)
          .reduce((prev, cur) => {
            cur = { ...cur };
            delete cur["0"];
            return { ...prev, ...cur };
          }, {});
        let result;
        if (resp.length === 1 && Object.keys(data).length === modes.length) {
          result = resp[0].data.data["0"];
        } else {
          const selected = modes.map((x) => data[x]);
          result =
            selected.length === 1
              ? selected[0]
              : selected.reduce((prev, cur) => {
                  Object.keys(cur).forEach((levelId) => {
                    delete cur[levelId].num_players;
                    if (!prev[levelId]) {
                      prev[levelId] = cur[levelId];
                      return;
                    }
                    const next = cur[levelId];
                    const result = prev[levelId];
                    next.accum.forEach((x, i) => (result.accum[i] += x));
                    Object.keys(next).forEach(function (key) {
                      if (typeof next[key] === "object") {
                        return;
                      }
                      if (!(key in result)) {
                        result[key] = 0;
                      }
                      if (key.indexOf("最大") === 0) {
                        result[key] = Math.max(result[key], next[key]);
                      } else {
                        result[key] += next[key];
                      }
                    });
                  });
                  return prev;
                }, {});
        }
        rendered = render(
          "show",
          "global_statistics",
          { ...resp[0].data, data: { [req.query.mode]: result } },
          { mode: "" }
        );
      } else {
        const resp = await withRetry(
          () =>
            axios.get(
              `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${COUCHDB_SERVER}/${dbs[0].replace(
                /_basic$/,
                "_aggregates"
              )}/${path}`
            ),
          5,
          500
        );
        rendered = render("show", view.renderer, resp.data, { mode: modes.length > 1 ? "" : modes[0] });
      }
      rendered.headers["Cache-Control"] = "public, max-age=3600, stale-while-revalidate=600, stale-if-error=600";
      res
        .type("json")
        .status(rendered.code || 200)
        .set(rendered.headers || {});
      return res.send(rendered.body).end();
    }
  );
  router.get(
    "/v2/:type/:view(global_statistics|player_delta_ranking|career_ranking)/:subView?",
    async function (req, res) {
      if (!V2_TYPES[req.params.type]) {
        return res.status(404).json({
          error: "type_not_found",
        });
      }
      const view = GLOBAL_DOC_VIEWS[req.params.view];
      if (!view) {
        return res.status(404).json({
          error: "view_not_found",
        });
      }
      let path = view.path;
      if (typeof path === "function") {
        path = path(req.params.subView);
      }
      const modes = (req.query.mode || "").split(".");
      const dbs = modes.map((mode) => {
        if (!req.query.mode || req.query.mode.toString() === "0") {
          return view.db[req.params.type];
        }
        const mainDb = MODE_DBS[mode];
        if (!mainDb) {
          return null;
        }
        return mainDb;
      });
      if (dbs.some((x) => !x)) {
        return res.status(400).json({
          error: "invalid_mode",
        });
      }
      if (modes.length > 1) {
        if (modes.some((x) => !x || x.toString() === "0")) {
          return res.status(400).json({
            error: "invalid_mode",
          });
        }
        if (new Set(modes).size !== modes.length) {
          return res.status(400).json({
            error: "duplicated_mode",
          });
        }
        if (new Set(dbs).size !== 1) {
          return res.status(400).json({
            error: "invalid_mode_combination",
          });
        }
        if (Object.values(MODE_DBS).filter((x) => x === dbs[0]).length !== modes.length) {
          return res.status(400).json({
            error: "invalid_mode_combination",
          });
        }
      }
      const resp = await withRetry(
        () =>
          axios.get(
            `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${COUCHDB_SERVER}/${dbs[0].replace(
              /_basic$/,
              "_aggregates"
            )}/${path}`
          ),
        5,
        500
      );
      const rendered = render("show", view.renderer, resp.data, { mode: modes.length > 1 ? "" : modes[0] });
      rendered.headers["Cache-Control"] = "public, max-age=3600, stale-while-revalidate=600, stale-if-error=600";
      res
        .type("json")
        .status(rendered.code || 200)
        .set(rendered.headers || {});
      return res.send(rendered.body).end();
    }
  );
  router.get("/v2/:type/recent_highlight_games", async function (req, res) {
    if (!MODE_DBS[req.query.mode]) {
      return res.status(404).json({
        error: "mode_not_found",
      });
    }
    if (req.query.skip) {
      return res.status(400).json({
        error: "skip_is_not_supported",
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
    const modeId = parseInt(req.query.mode, 10);
    const resp = await withRetry(
      () =>
        axios.get(
          `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${COUCHDB_SERVER}/${MODE_DBS[req.query.mode].replace(
            /_basic$/,
            "_extended"
          )}/_design/highlight_games_2/_view/highlight_games`,
          {
            params: {
              limit,
              startkey: JSON.stringify([modeId, {}]),
              include_docs: false,
              reduce: "false",
              descending: "true",
              stable: "false",
              update: "lazy",
            },
          }
        ),
      5,
      500
    );
    const rendered = render(
      "list",
      "highlight_games",
      (resp.data.rows || []).filter((x) => x.value.mode_id === modeId)
    );
    if (rendered.code === 200) {
      const body = JSON.parse(rendered.body);
      const resp = await withRetry(
        () =>
          axios.post(
            `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${COUCHDB_SERVER}/${
              MODE_DBS[req.query.mode]
            }/_all_docs`,
            {
              keys: body.map((x) => x._id),
              include_docs: true,
              reduce: "false",
              stable: "false",
              update: "lazy",
            }
          ),
        5,
        500
      );
      const transformed = render("list", "result_from_doc", resp.data.rows || []);
      const allDocs = JSON.parse(transformed.body);
      const allDocsMap = {};
      allDocs.forEach((x) => (allDocsMap[x._id] = x));
      rendered.body = JSON.stringify(body.map((x) => ({ ...allDocsMap[x._id], ...x })));
    }
    rendered.headers["Cache-Control"] = "public, max-age=600, stale-while-revalidate=600, stale-if-error=600";
    res
      .type("json")
      .status(rendered.code)
      .set(rendered.headers || {});
    return res.send(rendered.body).end();
  });
  router.get("/v2/:type/:view/:id/:startDate/:endDate", async function (req, res) {
    if (!V2_TYPES[req.params.type]) {
      return res.status(404).json({
        error: "type_not_found",
      });
    }
    if (!req.query.mode) {
      return res.status(400).json({
        error: "mode_is_required",
      });
    }
    if (req.query.skip) {
      return res.status(400).json({
        error: "skip_is_not_supported",
      });
    }
    const modes = req.query.mode.split(/[,.-]/).map((x) => parseInt(x, 10));
    if (!modes.length) {
      return res.status(400).json({
        error: "modes_must_be_specified",
      });
    }
    for (const m of modes) {
      if (!V2_TYPES[req.params.type].modes.includes(m)) {
        return res.status(400).json({
          error: "invalid_mode",
        });
      }
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
    const endDate = parseTs(req.params.endDate);
    if (!req.params.endDate || !endDate) {
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
    const params = {
      startkey: dateToSecKey(startDate),
      endkey: dateToSecKey(endDate),
    };
    if (req.params.view === "player_records") {
      params.descending = !!req.query.descending;
      params.limit = limit;
    }
    const playedModes = [];
    let rows = (
      await Promise.all(
        modes.map(async (m) => {
          try {
            const resp = await withRetry(
              () =>
                axios.get(
                  `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${PLAYER_SERVERS[m]}/p${m}_${id
                    .toString()
                    .padStart(10, "0")}/${view.path}`,
                  { params }
                ),
              5,
              500
            );
            if (resp.data.rows && resp.data.rows.length) {
              playedModes.push(m);
            }
            return (resp.data.rows || []).map((x) => ({ __mode: m, ...x }));
          } catch (e) {
            if (
              !e.response ||
              (e.response.status !== 404 &&
                ((e.response.data || {}).reason || "").indexOf("No rows can match your key range") === -1)
            ) {
              throw e;
            }
            return [];
          }
        })
      )
    ).flat();
    let rendered = null;
    if (rows.length) {
      if (req.params.view === "player_records") {
        rows.sort((a, b) => a.key - b.key);
        if (req.query.descending) {
          rows.reverse();
        }
        rows = rows.slice(0, limit);
        if (rows.length) {
          const grouped = _.groupBy(rows, (x) => MODE_DBS[x.__mode]);
          const results = await Promise.all(
            Array.from(Object.keys(grouped)).map(async (mainDb) => {
              assert(mainDb);
              assert(grouped[mainDb] && grouped[mainDb].length);
              return withRetry(
                () =>
                  axios.post(
                    `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${COUCHDB_SERVER}/${mainDb}/_all_docs`,
                    { keys: grouped[mainDb].map((x) => x.id), include_docs: true, stable: "false", update: "lazy" }
                  ),
                5,
                500
              );
            })
          );
          const resultRows = results.map((x) => x.data.rows).flat();
          resultRows.sort((a, b) => +(a.key > b.key) || +(a.key === b.key) - 1);
          if (req.query.descending) {
            resultRows.reverse();
          }
          rendered = render("list", view.renderer, resultRows);
        }
      } else {
        const reduced = view.reduce(rows.map((x) => x.value));
        rendered = render("list", view.renderer, [{ key: [id, id, id], value: reduced }]);

        rendered.body = JSON.stringify({
          ...JSON.parse(rendered.body),
          played_modes: playedModes,
        });
      }
    }
    if (!rendered) {
      rendered = render("list", view.renderer, []);
    }
    res
      .type("json")
      .status(rendered.code)
      .set(rendered.headers || {});
    return res.send(rendered.body).end();
  });
  router.get(["/v2/:type/search_player/:keyword", "/:type/search_player/:keyword"], async function (req, res) {
    const modeKeys = new Set();
    if (TYPES[req.params.type]) {
      const m = /^majsoul(_[^_]+)?_basic$/.exec(TYPES[req.params.type].mainDb);
      assert(m);
      modeKeys.add(m[1] || "_");
    } else if (V2_TYPES[req.params.type]) {
      for (const mode of V2_TYPES[req.params.type].modes) {
        const m = /^majsoul(_[^_]+)?_basic$/.exec(MODE_DBS[mode]);
        assert(m);
        modeKeys.add(m[1] || "_");
      }
    } else {
      return res.status(404).json({
        error: "type_not_found",
      });
    }
    assert(modeKeys.size);
    const keyword = req.params.keyword.toLowerCase().replace(/(^\s+|\s+$)/g, "");
    if (!keyword) {
      res.set("Cache-Control", "public, max-age=86400");
      return res.json([]);
    }
    const params = {
      selector: {
        $and: [
          {
            normalized_name: { $gte: keyword },
          },
          {
            normalized_name: { $lt: keyword + "\ufff0" },
          },
          {
            $or: Array.from(modeKeys).map((modeKey) => ({
              modes: { [modeKey]: { $exists: true } },
            })),
          },
        ],
      },
      limit: parseInt(req.query.limit, 10) || 20,
      use_index: "normalized_names",
      update: false,
      stable: false,
      execution_stats: true,
    };
    let resp = await withRetry(
      () =>
        axios.post(
          `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${COUCHDB_SERVER}/majsoul_nicknames/_find`,
          params,
          { timeout: 5000 }
        ),
      5,
      500
    );
    res.set("Cache-Control", "public, max-age=60, stale-while-revalidate=60, stale-if-error=60");
    return res.json(
      resp.data.docs.map((item) => {
        const mode = _(Array.from(modeKeys))
          .map((k) => item.modes[k])
          .compact()
          .maxBy((x) => x.timestamp);
        return {
          id: parseInt(/^0*([1-9]\d+)$/.exec(item._id), 10),
          nickname: item.nickname,
          level: {
            id: mode.level[0],
            score: mode.level[1],
            delta: mode.level[2],
          },
          latest_timestamp: mode.timestamp,
        };
      })
    );
  });

  router.get("/:type/count/:startDate/:endDate?", async function (req, res) {
    if (!TYPES[req.params.type]) {
      return res.status(404).json({
        error: "type_not_found",
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
    const endkey = endDate ? generateKey(endDate) : generateKey(new Date(startDate.getTime() + MS_PER_DAY - 1));
    const params = {
      startkey: generateKey(startDate),
      limit: 0,
    };
    if (endkey <= params.startkey) {
      res.set("Cache-Control", "public, max-age=86400");
      return res.json({ count: 0 });
    }
    let resp = await withRetry(
      () =>
        axios.get(
          `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${COUCHDB_SERVER}/${
            TYPES[req.params.type].mainDb
          }/_all_docs`,
          { params }
        ),
      5,
      500
    );
    const offsetStart = resp.data.offset;
    params.startkey = endkey;
    resp = await withRetry(
      () =>
        axios.get(
          `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${COUCHDB_SERVER}/${
            TYPES[req.params.type].mainDb
          }/_all_docs`,
          { params }
        ),
      5,
      500
    );
    res.set("Cache-Control", "public, max-age=60, stale-while-revalidate=600, stale-if-error=600");
    return res.json({ count: resp.data.offset - offsetStart });
  });
  router.get("/:type/games/:startDate/:endDate?", async function (req, res) {
    if (!TYPES[req.params.type]) {
      return res.status(404).json({
        error: "type_not_found",
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
      startkey: generateKey(startDate),
      endkey: endDate ? generateKey(endDate) : generateKey(new Date(startDate.getTime() + MS_PER_DAY - 1)),
      descending: !!req.query.descending,
      limit: limit,
      skip: skip,
      include_docs: "true",
    };
    const resp = await withRetry(
      () =>
        axios.get(
          `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${COUCHDB_SERVER}/${
            TYPES[req.params.type].mainDb
          }/_all_docs`,
          { params }
        ),
      5,
      500
    );
    const rendered = render("list", "result_from_doc", resp.data.rows || []);
    if (params.descending) {
      // Optimize caching
      let hourMark = null;
      const cutoff = new Date().getTime() / 1000 - SEC_PER_HOUR * 3; // Only applies to older games
      let games = JSON.parse(rendered.body);
      for (let i = 0; i < games.length; i++) {
        const game = games[i];
        if (game.startTime > cutoff) {
          continue;
        }
        if (hourMark === null) {
          hourMark = Math.floor(game.startTime / SEC_PER_HOUR);
        } else if (Math.abs(Math.floor(game.startTime / SEC_PER_HOUR) - hourMark) > 0.001) {
          games = games.slice(0, i + 1);
          rendered.body = JSON.stringify(games);
          break;
        }
      }
    }
    rendered.headers["Cache-Control"] = "public, max-age=86400, stale-while-revalidate=600, stale-if-error=600";
    res
      .type("json")
      .status(rendered.code)
      .set(rendered.headers || {});
    return res.send(rendered.body).end();
  });
  router.get("/:type/:view/:id/:startDate?/:endDate?", async function (req, res) {
    if (!TYPES[req.params.type]) {
      return res.status(404).json({
        error: "type_not_found",
      });
    }
    const availableModes = TYPES[req.params.type].modes;
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
              `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${PLAYER_SERVERS[m]}/p${m}_${id
                .toString()
                .padStart(10, "0")}/${view.path}`,
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
                { keys: rows.map((x) => x.id), include_docs: true, stable: "false", update: "lazy" }
              ),
            5,
            500
          );
          rendered = render("list", view.renderer, resp.data.rows || []);
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
    return res.send(rendered.body).end();
  });

  app.use("/api/", router);
  app.use("/api-test/", router);
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
