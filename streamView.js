const oboe = require("oboe");

const { COUCHDB_URL, COUCHDB_USER, COUCHDB_PASSWORD } = require("./couchStorage");

function stream (url, callback) {
  return new Promise((resolve, reject) => {
    let timeoutToken;
    let latestTs = new Date().getTime();
    let finished = false;
    const req = oboe({
      url,
      headers: {
        Authorization: `Basic ${Buffer.from(`${COUCHDB_USER}:${COUCHDB_PASSWORD}`).toString("base64")}`
      },
    }).done((result) => {
      finished = true;
      clearTimeout(timeoutToken);
      return resolve(result);
    }).fail((e) => {
      finished = true;
      clearTimeout(timeoutToken);
      return reject(e);
    }).node("rows.*", (row) => {
      latestTs = new Date().getTime();
      callback(row);
      return oboe.drop;
    });
    const timeoutTick = function () {
      if (finished) {
        return;
      }
      if (new Date().getTime() - latestTs > 60000) {
        req.abort();
        return reject(new Error("timeout"));
      }
      timeoutToken = setTimeout(timeoutTick, 5000);
    };
    timeoutTick();
  });
}
function streamView (docName, viewName, params, callback) {
  return stream(`${COUCHDB_URL}${params._suffix || ""}/_design/${docName}/_view/${viewName}?${Object.entries(params).map(([k, v]) => `${k}=${encodeURIComponent(typeof v === "object" ? JSON.stringify(v) : v)}`).join("&")}`, callback);
}
function streamAllDocs (params, callback) {
  return stream(`${COUCHDB_URL}${params._suffix || ""}/_all_docs?${Object.entries(params).map(([k, v]) => `${k}=${encodeURIComponent(typeof v === "object" ? JSON.stringify(v) : v)}`).join("&")}`, callback);
}

module.exports.streamView = streamView;
module.exports.streamAllDocs = streamAllDocs;
// vim: sw=2:ts=2:expandtab:fdm=syntax
