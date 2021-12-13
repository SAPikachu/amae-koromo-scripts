#!./node_modules/.bin/ts-node
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const majsoul_1 = require("./majsoul");
const entryPoint_1 = require("./entryPoint");
const fs_1 = require("fs");
const cli_1 = require("protobufjs/cli");
const util_1 = require("util");
async function main() {
    const versionInfo = await (0, majsoul_1.getRes)("version.json", true);
    const resInfo = await (0, majsoul_1.getRes)(`resversion${versionInfo.version}.json`);
    const pbVersion = resInfo.res["res/proto/liqi.json"].prefix;
    const pbDef = await (0, majsoul_1.getRes)(`${pbVersion}/res/proto/liqi.json`);
    (0, fs_1.writeFileSync)("majsoulPb.proto.json", JSON.stringify(pbDef, undefined, "  "));
    await (0, util_1.promisify)(cli_1.pbjs.main)([
        "--target",
        "static-module",
        "--wrap",
        "commonjs",
        "-o",
        "majsoulPb.js",
        "majsoulPb.proto.json",
    ]);
    await (0, util_1.promisify)(cli_1.pbts.main)(["-o", "majsoulPb.d.ts", "majsoulPb.js"]);
}
(0, entryPoint_1.wrappedRun)(main);
//# sourceMappingURL=generateTsDefinition.js.map