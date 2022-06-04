const os = require("os");

const Sentry = require("@sentry/node");

require("tls").DEFAULT_ECDH_CURVE = "auto";
Sentry.init({
  dsn: process.env.SENTRY_DSN,
  integrations: function (integrations) {
    return integrations.filter(function (integration) {
      return integration.name !== "Http";
    });
  },
});

function flushStdout(timeout = 60000) {
  return new Promise((resolve) => {
    if (process.stdout.write("")) {
      process.nextTick(() => resolve());
      return;
    }
    let resolved = false;
    let timeoutToken;
    const cb = function () {
      if (resolved) {
        return;
      }
      resolved = true;
      if (timeoutToken) {
        clearTimeout(timeoutToken);
        timeoutToken = null;
      }
      process.nextTick(() => resolve());
    };
    process.stdout.once("drain", cb);
    timeoutToken = setTimeout(function () {
      Sentry.captureMessage("Stdout doesn't drain in time");
      Sentry.flush(2000).finally(cb);
    }, timeout);
  });
}

function rejectWithSendError(e, extra = {}) {
  return new Promise((resolve, reject) => {
    const report = process.report?.getReport?.(e?.stack ? e : undefined) || {};
    for (const key of Object.keys(report)) {
      if (!report.hasOwnProperty(key)) {
        continue;
      }
      let value = report[key];
      if (typeof value !== "object" || Array.isArray(value)) {
        value = { value };
      }
      Sentry.setContext("report_" + key, value);
    }
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
    Sentry.flush(2000).finally(() => reject(e));
  });
}

process.on("unhandledRejection", (e) => {
  console.error("Unhandled rejection:", e);
  rejectWithSendError(e, { source: "unhandledRejection" }).finally(() => process.exit(1));
});

function wrappedMain(main) {
  Sentry.setContext("info", {
    argv: process.argv,
    startTime: new Date(),
    memoryUsage: process.memoryUsage(),
  });
  Sentry.setContext("os", {
    uptime: os.uptime(),
    release: os.release(),
    version: os.version(),
    networkInterfaces: os.networkInterfaces(),
    hostname: os.hostname(),
    loadavg: os.loadavg(),
    freemem: os.freemem(),
    userInfo: os.userInfo(),
  });
  Sentry.setContext("env", process.env);
  return main().catch((e) => {
    console.error("[wrappedMain] Error:", e);
    return rejectWithSendError(e)
      .finally(flushStdout)
      .finally(() => process.exit(255));
  });
}
exports["wrappedMain"] = (main) => () => wrappedMain(main);
exports["wrappedRun"] = (main) =>
  wrappedMain(main)
    .then((x) => console.log(require("util").inspect(x, { depth: 10 })))
    .then(flushStdout)
    .then(() => process.exit(0))
    .catch(() => {});
// vim: sw=2:ts=2:expandtab:fdm=syntax
