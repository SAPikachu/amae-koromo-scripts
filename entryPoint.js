const Sentry = require("@sentry/node");
Sentry.init();

require("tls").DEFAULT_ECDH_CURVE = "auto";

function rejectWithSendError(e, extra = {}) {
  return new Promise((resolve, reject) => {
    if (e.stack) {
      Sentry.captureException(e);
    } else {
      Sentry.captureException(
        new Error(
          e.message ||
            e.error ||
            e.status ||
            e.toString() +
              " -> " +
              Object.keys(e)
                .map((x) => `x: ${e[x]}`)
                .join(", ")
                .toString()
        ),
        {
          extra: {
            rawError: e,
            ...extra,
          },
        }
      );
    }
    Sentry.getCurrentHub()
      .getClient()
      .flush(2000)
      .finally(() => reject(e));
  });
}

process.on("unhandledRejection", (e) => {
  console.error("Unhandled rejection:", e);
  rejectWithSendError(e, { source: "unhandledRejection" }).finally(() => process.exit(1));
});

function wrappedMain(main) {
  return new Promise((resolve, reject) => {
    main()
      .then(function (ret) {
        resolve(ret);
        setTimeout(function () {
          let pendingCount = 0;
          const checkExit = function () {
            if (pendingCount <= 0) {
              setTimeout(() => process.exit(0), 10);
            }
          };
          pendingCount++;
          process.stdout.write("", () => {
            pendingCount--;
            checkExit();
          });
          pendingCount++;
          process.stderr.write("", () => {
            pendingCount--;
            checkExit();
          });
          checkExit();
          setTimeout(() => process.exit(0), 500);
        }, 100);
      })
      .catch((e) => {
        console.error("[wrappedMain] Error:", e);
        return rejectWithSendError(e).finally(() => process.exit(255));
      });
  });
}
exports["wrappedMain"] = (main) => () => wrappedMain(main);
exports["wrappedRun"] = (main) =>
  wrappedMain(main)
    .then((x) => console.log(require("util").inspect(x, { depth: 10 })))
    .catch(() => {});
// vim: sw=2:ts=2:expandtab:fdm=syntax
