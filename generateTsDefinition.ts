#!./node_modules/.bin/ts-node

import { getRes } from "./majsoul";
import { wrappedRun } from "./entryPoint";
import { writeFileSync } from "fs";
import { pbjs, pbts } from "protobufjs/cli";
import { promisify } from "util";

async function main() {
  const versionInfo = await getRes("version.json", true);
  const resInfo = await getRes(`resversion${versionInfo.version}.json`);
  const pbVersion = resInfo.res["res/proto/liqi.json"].prefix;
  const pbDef = await getRes(`${pbVersion}/res/proto/liqi.json`);
  writeFileSync("majsoulPb.proto.json", JSON.stringify(pbDef, undefined, "  "));
  await promisify(pbjs.main)([
    "--target",
    "static-module",
    "--wrap",
    "commonjs",
    "-o",
    "majsoulPb.js",
    "majsoulPb.proto.json",
  ]);
  await promisify(pbts.main)(["-o", "majsoulPb.d.ts", "majsoulPb.js"]);
}

wrappedRun(main);
