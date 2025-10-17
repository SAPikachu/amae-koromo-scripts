#!./node_modules/.bin/ts-node
"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MajsoulGameAnalyzer = void 0;
const assert_1 = __importDefault(require("assert"));
const protobufjs_1 = require("protobufjs");
const fs_1 = require("fs");
const majsoulPb_1 = require("./majsoulPb");
const shanten_1 = require("./shanten");
const entryPoint_1 = require("./entryPoint");
const TILE_RE = /^([0-9][mps]|[1-7]z)$/;
const KITA = "4z";
function isValidTile(tile) {
    return TILE_RE.test(tile);
}
function validateTile(tile) {
    if (!isValidTile(tile)) {
        throw new Error(`Invalid tile: ${tile}`);
    }
    return tile;
}
function isEquivantTile(a, b) {
    if (a === b) {
        return true;
    }
    if (a.charAt(1) !== b.charAt(1)) {
        return false;
    }
    return ["0", "5"].includes(a.charAt(0)) && ["0", "5"].includes(b.charAt(0));
}
function tilesToHaiArr(tiles) {
    const ret = [
        [0, 0, 0, 0, 0, 0, 0, 0, 0],
        [0, 0, 0, 0, 0, 0, 0, 0, 0],
        [0, 0, 0, 0, 0, 0, 0, 0, 0],
        [0, 0, 0, 0, 0, 0, 0],
    ];
    const INDEXER = "mpsz";
    for (const tile of tiles) {
        let n = parseInt(tile.charAt(0), 10);
        const type = tile.charAt(1);
        if (type === "z") {
            (0, assert_1.default)(n >= 1 && n <= 7);
        }
        else {
            if (n === 0) {
                n = 5;
            }
            (0, assert_1.default)(n >= 1 && n <= 9);
        }
        const typeIndex = INDEXER.indexOf(type);
        (0, assert_1.default)(typeIndex >= 0);
        ret[typeIndex][n - 1]++;
        (0, assert_1.default)(ret[typeIndex][n - 1] <= 4);
    }
    return ret;
}
class TileBin {
    constructor() {
        this._tiles = {};
    }
    put(tile) {
        if (/^0[mps]$/.test(tile)) {
            tile = ("5" + tile.charAt(1));
        }
        this._tiles[tile] = (this._tiles[tile] || 0) + 1;
        (0, assert_1.default)(this._tiles[tile] <= 4);
    }
    getNum(tile) {
        if (/^0[mps]$/.test(tile)) {
            tile = ("5" + tile.charAt(1));
        }
        return this._tiles[tile] || 0;
    }
}
class Player {
    constructor(hand) {
        (0, assert_1.default)(hand.length === 13 || hand.length === 14);
        this._hand = hand;
        this._opened = [];
        this._discarded = [];
    }
    deal(tile) {
        assert_1.default.equal(this._hand.length % 3, 1);
        this._hand.push(tile);
    }
    discard(tile) {
        assert_1.default.equal(this._hand.length % 3, 2);
        const index = this._hand.indexOf(tile);
        if (index === -1) {
            throw new Error(`Not in hand: ${tile}`);
        }
        this._discarded.push(tile);
        this._hand.splice(index, 1);
    }
    kita() {
        const tile = KITA;
        assert_1.default.equal(this._hand.length % 3, 2);
        const index = this._hand.indexOf(tile);
        if (index === -1) {
            throw new Error(`Not in hand: ${tile}`);
        }
        this._opened.push({ hand: [tile] });
        this._hand.splice(index, 1);
    }
    open(tile, handTiles) {
        assert_1.default.equal(this._hand.length % 3, 1);
        (0, assert_1.default)(handTiles.length === 2 || handTiles.length === 3);
        for (const handTile of handTiles) {
            const index = this._hand.indexOf(handTile);
            if (index === -1) {
                throw new Error(`Not in hand: ${handTile}`);
            }
            this._hand.splice(index, 1);
        }
        this._opened.push({ hand: handTiles, discard: tile });
    }
    kan(tile) {
        assert_1.default.equal(this._hand.length % 3, 2);
        let meld = this._opened.find((x) => x.hand.length === 2 &&
            x.discard &&
            isEquivantTile(x.discard, tile) &&
            x.hand.every((t) => isEquivantTile(t, tile)));
        if (meld) {
            const index = this._hand.indexOf(tile);
            if (index === -1) {
                throw new Error(`Not in hand: ${tile}`);
            }
            meld.hand.push(tile);
            this._hand.splice(index, 1);
        }
        else {
            const meld = [];
            for (let i = 0; i < 4; i++) {
                const index = this._hand.findIndex((x) => isEquivantTile(x, tile));
                if (index === -1) {
                    throw new Error(`Not in hand: ${tile}`);
                }
                meld.push(this._hand[index]);
                this._hand.splice(index, 1);
            }
            this._opened.push({ hand: meld });
        }
    }
    syanten() {
        return (0, shanten_1.calcShanten)(this._hand);
    }
    isKokushiTenpai() {
        if (this._hand.length !== 13) {
            return false;
        }
        const tiles = {};
        for (const tile of this._hand) {
            if (!/^([19][mps]|.z)$/.test(tile)) {
                return false;
            }
            tiles[tile] = (tiles[tile] || 0) + 1;
            if (tiles[tile] > 2) {
                return false;
            }
        }
        const entries = Object.entries(tiles);
        return entries.filter(([, count]) => count === 2).length <= 1;
    }
}
const ACCEPTED_RECORD_TYPES = {
    ".lq.RecordDealTile": new majsoulPb_1.lq.RecordDealTile(),
    ".lq.RecordChiPengGang": new majsoulPb_1.lq.RecordChiPengGang(),
    ".lq.RecordDiscardTile": new majsoulPb_1.lq.RecordDiscardTile(),
    ".lq.RecordNoTile": new majsoulPb_1.lq.RecordNoTile(),
    ".lq.RecordHule": new majsoulPb_1.lq.RecordHule(),
    ".lq.RecordBaBei": new majsoulPb_1.lq.RecordBaBei(),
    ".lq.RecordAnGangAddGang": new majsoulPb_1.lq.RecordAnGangAddGang(),
    ".lq.RecordLiuJu": new majsoulPb_1.lq.RecordLiuJu(),
};
class MajsoulGameAnalyzer {
    constructor(newRoundRecord) {
        this._latestDoras = [];
        (0, assert_1.default)([3, 4].includes(newRoundRecord.scores.length));
        const { tiles0, tiles1, tiles2, tiles3 } = newRoundRecord;
        const tiles = [tiles0, tiles1, tiles2, tiles3].slice(0, newRoundRecord.scores.length);
        this._players = tiles.map((t) => new Player(t.map(validateTile)));
        this._latestDoras = newRoundRecord.doras?.map(validateTile) || [];
    }
    getRemainingNumTiles(seat, tiles) {
        (0, assert_1.default)(this._latestDoras.length && this._latestDoras.length <= 5);
        const bin = new TileBin();
        for (const player of this._players) {
            player._discarded.forEach((x) => bin.put(x));
            player._opened.forEach((x) => x.hand.forEach((t) => bin.put(t)));
        }
        this._players[seat]._hand.forEach((x) => bin.put(x));
        this._latestDoras.forEach((x) => bin.put(x));
        let ret = 0;
        for (const tile of tiles) {
            ret += 4 - bin.getNum(validateTile(tile));
        }
        return ret;
    }
    processRecord(recordName, record) {
        if (!(recordName in ACCEPTED_RECORD_TYPES)) {
            throw new Error(`Unknown record: ${recordName}`);
        }
        (0, assert_1.default)(!record.dora, "Unexpected dora field, may be old data");
        const doras = record.doras;
        if (doras?.length) {
            this._latestDoras = doras.map(validateTile);
        }
        switch (recordName) {
            case ".lq.RecordDealTile": {
                const r = record;
                (0, assert_1.default)(typeof r.seat === "number");
                this._players[r.seat].deal(validateTile(r.tile));
                this._pendingTile = undefined;
                break;
            }
            case ".lq.RecordDiscardTile": {
                const r = record;
                (0, assert_1.default)(typeof r.seat === "number");
                const tile = validateTile(r.tile);
                this._players[r.seat].discard(tile);
                this._pendingTile = tile;
                if (r.tingpais?.length) {
                    (0, assert_1.default)(this._players[r.seat].syanten() === 0 || this._players[r.seat].isKokushiTenpai());
                }
                if (r.is_liqi) {
                    (0, assert_1.default)(r.zhenting.length === this._players.length);
                    (0, assert_1.default)(r.zhenting[r.seat] ===
                        this._players[r.seat]._discarded.some((x) => r.tingpais.some((t) => isEquivantTile(validateTile(t.tile), validateTile(x)))));
                    this.getRemainingNumTiles(r.seat, r.tingpais.map((x) => x.tile));
                }
                break;
            }
            case ".lq.RecordChiPengGang": {
                const r = record;
                (0, assert_1.default)(typeof r.seat === "number");
                const tiles = r.tiles.map(validateTile);
                if (!this._pendingTile) {
                    throw new Error("No pending tile");
                }
                if (tiles.length < 3) {
                    throw new Error("Unexpected number of tiles: " + tiles.length);
                }
                const index = tiles.indexOf(this._pendingTile);
                (0, assert_1.default)(index !== -1);
                tiles.splice(index, 1);
                this._players[r.seat].open(this._pendingTile, tiles);
                this._pendingTile = undefined;
                break;
            }
            case ".lq.RecordBaBei": {
                const r = record;
                (0, assert_1.default)(typeof r.seat === "number");
                this._players[r.seat].kita();
                this._pendingTile = KITA;
                break;
            }
            case ".lq.RecordAnGangAddGang": {
                const r = record;
                (0, assert_1.default)(typeof r.seat === "number");
                const tile = validateTile(r.tiles);
                this._players[r.seat].kan(tile);
                this._pendingTile = tile;
                break;
            }
        }
    }
}
exports.MajsoulGameAnalyzer = MajsoulGameAnalyzer;
if (require.main === module) {
    (0, entryPoint_1.wrappedRun)(async () => {
        console.log(process.argv[2]);
        const root = protobufjs_1.Root.fromJSON(JSON.parse((0, fs_1.readFileSync)("majsoulPb.proto.json", "utf8")));
        for (const file of process.argv.slice(2)) {
            const wrappedData = majsoulPb_1.lq.Wrapper.decode((0, fs_1.readFileSync)(file));
            const type = root.lookupType(wrappedData.name);
            const msg = type.decode(wrappedData.data);
            let gameAnalyzer;
            for (const actionData of msg?.actions || []) {
                if (!actionData.result?.length) {
                    continue;
                }
                const wrappedResult = majsoulPb_1.lq.Wrapper.decode(actionData.result);
                const type = root.lookupType(wrappedResult.name);
                const record = type.decode(wrappedResult.data);
                if (wrappedResult.name === ".lq.RecordNewRound") {
                    gameAnalyzer = new MajsoulGameAnalyzer(record);
                }
                else {
                    (0, assert_1.default)(gameAnalyzer);
                    gameAnalyzer.processRecord(wrappedResult.name, record);
                }
            }
        }
    });
}
//# sourceMappingURL=gameAnalyzer.js.map