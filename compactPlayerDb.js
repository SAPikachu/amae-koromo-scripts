const { wrappedRun } = require("./entryPoint");

const axios = require("axios").default;

const { CouchStorage } = require("./couchStorage");
const { COUCHDB_USER, COUCHDB_PASSWORD, COUCHDB_PROTO } = require("./env");
const COUCHDB_SERVER = "127.0.0.1:5989";

async function withRetry(func, num = 5, retryInterval = 5000) {
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      return await func();
    } catch (e) {
      console.log(e);
      if (num <= 0 || e.status === 403) {
        throw e;
      }
      console.log(`Retrying (${num})`);
      await new Promise((r) => setTimeout(r, Math.random() * retryInterval));
    }
    num--;
  }
}

const URL_BASE = `${COUCHDB_PROTO}://${COUCHDB_USER}:${COUCHDB_PASSWORD}@${COUCHDB_SERVER}`;

Promise.allSettled =
  Promise.allSettled ||
  ((promises) =>
    Promise.all(
      promises.map((p) =>
        p
          .then((v) => ({
            status: "fulfilled",
            value: v,
          }))
          .catch((e) => ({
            status: "rejected",
            reason: e,
          }))
      )
    ));

async function main() {
  const dbStorage = new CouchStorage({
    uri: `${URL_BASE}/_dbs`,
    skipSetup: true,
  });
  const dbs = await dbStorage._db.allDocs({ startkey: "" });
  for (const row of dbs.rows) {
    if (!/^p\d+_/.test(row.id)) {
      continue;
    }
    console.log(row.id);
    const s = new CouchStorage({
      uri: `${URL_BASE}/${row.id}`,
      skipSetup: true,
    });
    await withRetry(() => axios.put(`${URL_BASE}/${row.id}/_revs_limit`, "1"));
    for (const result of await Promise.allSettled(
      [withRetry(() => s._db.compact({ interval: 50 }))].concat(
        ["basic", "extended"].map(async (x) => {
          await withRetry(() => axios.post(`${URL_BASE}/${row.id}/_compact/${x}`, {}));
          await new Promise((res) => setTimeout(res, 50));
          while (
            (await withRetry(() => axios.get(`${URL_BASE}/${row.id}/_design/${x}/_info`))).data.view_index
              .compact_running !== false
          ) {
            await new Promise((res) => setTimeout(res, 50));
          }
        })
      )
    )) {
      if (result.status !== "fulfilled") {
        throw result.reason || result;
      }
    }
    s._db.close().catch(() => {});
  }
}

if (require.main === module) {
  wrappedRun(main);
}
// vim: sw=2:ts=2:expandtab:fdm=syntax
