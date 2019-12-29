const { wrappedRun } = require("./entryPoint");

const moment = require("moment");
const { CouchStorage } = require("./couchStorage");
const deepEqual = require("deep-equal");

/* Backup deleted view
{
  "_id": "_design/agv_player_delta_ranking",
  "_rev": "2-19f473b2bdf98d5e2514f293dec7b1e6",
  "template_views": {
    "lib": {
      "map_impl": "exports.map = function (doc, args) {\n\t\"use strict\";\n\n  if (!doc.uuid) {\n    return;\n  }\n\tvar range_end = args.time_range_end;\n\tvar range_start = range_end - args.timeframe;\n\tif (!(doc.start_time >= range_start && doc.start_time < range_end)) {\n\t\treturn;\n\t}\n  var players = {};\n  doc.accounts.forEach(function (x) {\n\t\tplayers[x.seat] = { player: x };\n\t});\n  doc.result.players.forEach(function (x) {\n\t\tplayers[x.seat].result = x;\n\t});\n  var playerList = Object.keys(players).map(function(x) {\n\t\treturn players[x];\n\t});\n  playerList.forEach(function(x, index) {\n\t\tvar data = {\n\t\t\tnickname: x.player.nickname,\n\t\t\tlevel: [x.player.level.id, x.player.level.score, x.result.grading_score]\n\t\t};\n\t\temit([0,  x.player.account_id, doc.start_time], data);\n\t\temit([doc.config.meta.mode_id, x.player.account_id, doc.start_time], data);\n\t});\n}"
    },
    "view": {
      "reduce": "function (keys, values, rereduce) {\n\t\"use strict\";\n\tvar result = { };\n\tvar results = {};\n  if (rereduce) {\n\t\tresult.firstId = values[0].firstId;\n\t\tresult.lastId = values[values.length - 1].lastId;\n    values.forEach(function(value) {\n\t\t\tvalue.results.forEach(function(x) {\n\t\t\t\tvar id = x.id.toString();\n\t\t\t\tif (!results[id]) {\n\t\t\t\t\tresults[id] = x;\n\t\t\t\t} else {\n\t\t\t\t\tresults[id].delta += x.delta;\n\t\t\t\t\tvar newer = results[id].latest_timestamp > x.latest_timestamp ? results[id] : x;\n\t\t\t\t\tresults[id].latest_timestamp = newer.latest_timestamp;\n\t\t\t\t\tresults[id].nickname = newer.nickname;\n\t\t\t\t\tresults[id].level = newer.level;\n\t\t\t\t}\n\t\t\t});\n    });\n  } else {\n\t\tresult.firstId = keys[0][0][1];\n\t\tresult.lastId = keys[keys.length - 1][0][1];\n    values.forEach(function(x, index) {\n\t\t\tvar idNum = keys[index][0][1];\n\t\t\tvar id = idNum.toString();\n\t\t\tvar ts = keys[index][0][2];\n\t\t\tif (!results[id]) {\n\t\t\t\tresults[id] = {\n\t\t\t\t\tid: idNum,\n\t\t\t\t\tlatest_timestamp: 0,\n\t\t\t\t\tdelta: 0\n\t\t\t\t};\n\t\t\t}\n\t\t\tif (ts > results[id].latest_timestamp) {\n\t\t\t\tresults[id].latest_timestamp = ts;\n\t\t\t\tresults[id].nickname = x.nickname;\n\t\t\t\tresults[id].level = x.level;\n\t\t\t}\n\t\t\tresults[id].delta += x.level[2];\n    });\n  }\n\tvar itemList = Object.keys(results).map(function(key) { return results[key]; });\n\titemList.sort(function(a, b) { return a.delta - b.delta; });\n\tvar NUM_ITEMS_TO_KEEP = 10;\n\tvar idsToKeep = {};\n\tidsToKeep[result.firstId.toString()] = true;\n\tidsToKeep[result.lastId.toString()] = true;\n\tfor (var i = 0; i < Math.min(itemList.length, NUM_ITEMS_TO_KEEP); i++) {\n\t\tidsToKeep[itemList[i].id.toString()] = true;\n\t\tidsToKeep[itemList[itemList.length - i - 1].id.toString()] = true;\n\t}\n\tresult.results = itemList.filter(function(x) { return idsToKeep[x.id.toString()]; });\n  return result;\n}"
    }
  },
  "args_matrix": [
    {
      "_name": "1w",
      "timeframe": 604800
    },
    {
      "_name": "4w",
      "timeframe": 2419200
    }
  ],
  "group_level": 1,
  "_revs_info": [
    {
      "rev": "2-19f473b2bdf98d5e2514f293dec7b1e6",
      "status": "available"
    }
  ],
  "_local_seq": 74830
}
*/

async function updateAggregatedViews () {
  const couch = new CouchStorage({ timeout: 15000 });
  const viewResp = await couch.db.allDocs({
    startkey: "_design/agv_",
    endkey: "_design/agv_\uffff",
    include_docs: true,
  });
  const periods = [{
    name: "prev",
    date: moment.utc().subtract(1, "day"),
  }, {
    name: "current",
    date: moment.utc(),
  }, {
    name: "next",
    date: moment.utc().add(1, "day"),
  }];
  const pendingSnapshots = [];
  const periodViews = periods.map(x => ({
    docName: `_design/aggregated_views_${x.name}`,
    period: x,
    views: {
      lib: {},
    },
    needUpdate: false,
    oldRev: undefined,
  }));
  for (const x of periodViews) {
    let viewDoc;
    try {
      viewDoc = await couch.db.get(x.docName);
    } catch (e) {
      x.needUpdate = true;
      continue;
    }
    x.oldRev = viewDoc._rev;
    x.views = (viewDoc.views || {});
    x.views.lib = (viewDoc.views || {}).lib || {};
  }
  for (const doc of viewResp.rows.map(x => x.doc)) {
    if (!doc.template_views || !doc.args_matrix || !doc.template_views.lib || !doc.template_views.lib.map_impl) {
      continue;
    }
    for (const [i, args] of doc.args_matrix.entries()) {
      for (const periodView of periodViews) {
        const period = periodView.period;
        const viewKey = `${doc._id.replace("_design/agv_", "")}_${args._name || i}`;
        const endDate = moment.utc(period.date).add(1, "day").startOf("day");
        const fixedArgs = {
          ...args,
          time_range_end: endDate.unix(),
        };
        const implKey = `${viewKey}_map_impl`;
        const mapCode = `function (doc) { return require("views/lib/${implKey}").map(doc, ${JSON.stringify(fixedArgs)}); }`;
        if (
          !periodView.views[viewKey]
          || periodView.views[viewKey].map !== mapCode
          || periodView.views[viewKey].reduce !== doc.template_views.view.reduce
          || periodView.views.lib[implKey] !== doc.template_views.lib.map_impl
        ) {
          periodView.needUpdate = true;
        }
        periodView.views.lib[implKey] = doc.template_views.lib.map_impl;
        periodView.views[viewKey] = {
          ...doc.template_views.view,
          map: mapCode,
        };
        pendingSnapshots.push({
          view: `${periodView.docName.replace("_design/", "")}/${viewKey}`,
          docId: `${viewKey}_${period.date.format("YYYY-MM-DD")}`,
          date: period.date,
          groupLevel: doc.group_level || 1,
        });
        if (period.name === "current") {
          pendingSnapshots.push({
            view: `${periodView.docName.replace("_design/", "")}/${viewKey}`,
            docId: `${viewKey}_current`,
            date: period.date,
            groupLevel: doc.group_level || 1,
          });
        }
      }
    }
  }
  for (const x of periodViews) {
    if (!x.needUpdate) {
      continue;
    }
    console.log(`Updating ${x.docName}`);
    await couch.db.put({
      _id: x.docName,
      _rev: x.oldRev,
      views: x.views,
    });
  }
  for (const { view, docId, groupLevel, date } of pendingSnapshots) {
    let snapshotResp;
    let stale = false;
    try {
      snapshotResp = await couch.db.query(view, {
        group_level: groupLevel,
      });
    } catch (e) {
      console.log(`${view} is stale`);
      stale = true;
      snapshotResp = await couch.db.query(view, {
        group_level: groupLevel,
        stale: "update_after",
      });
    }
    if (snapshotResp.rows && snapshotResp.rows.length) {
      let rev = undefined;
      try {
        const existing = await couch.db.get(docId);
        rev = existing._rev;
        if (deepEqual(existing.data, snapshotResp.rows, {strict: true})) {
          console.log(`Skipping equivalent doc: ${docId}`);
          continue;
        }
      } catch (e) {
        // Ignore
      }
      await couch.db.put({
        _id: docId,
        _rev: rev,
        type: "agvSnapshot",
        stale,
        date: moment(date).startOf("day").valueOf(),
        data: snapshotResp.rows,
        updated: moment.utc().valueOf(),
      });
    }
  }
}
if (require.main === module) {
  wrappedRun(updateAggregatedViews);
} else {
  module.exports = {
    updateAggregatedViews,
  };
}


// vim: sw=2:ts=2:expandtab:fdm=syntax
