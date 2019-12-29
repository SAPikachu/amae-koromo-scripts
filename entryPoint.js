const Sentry = require("@sentry/node");
Sentry.init();

function wrappedMain (main) {
  return new Promise((resolve, reject) => {
    main().then(resolve).catch(e => {
      console.error("Error:", e);
      Sentry.captureException(e);
      Sentry.getCurrentHub().getClient().flush(2000).finally(() => reject(e));
    });
  });
}
exports["wrappedMain"] = (main) => () => wrappedMain(main);
exports["wrappedRun"] = (main) => wrappedMain(main).then((x) => console.log(require("util").inspect(x, {depth: 10}))).catch(() => {});
// vim: sw=2:ts=2:expandtab:fdm=syntax
