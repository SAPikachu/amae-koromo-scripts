const { wrappedRun } = require("./entryPoint");
const { streamAllDocs } = require("./streamView");

async function main() {
  let counter = 0;
  await streamAllDocs(
    {
      _suffix: "_gold_extended",
      include_docs: true,
    },
    function (row) {
      if (JSON.stringify(row.doc).indexOf("\uFFFD") > -1) {
        console.log(`-- ${row.doc._id} ${row.doc.game._id}`);
      }
      counter++;
      if (counter % 10000 === 0) {
        console.log(`# ${counter} ${row.doc._id} ${row.doc.game._id}`);
      }
    }
  );
}

if (require.main === module) {
  wrappedRun(main);
}
// vim: sw=2:ts=2:expandtab:fdm=syntax
