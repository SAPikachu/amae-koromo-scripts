#!./node_modules/.bin/ts-node

import assert from "assert";
import { Root } from "protobufjs";
import { readFileSync } from "fs";
import { lq } from "./majsoulPb";
import { calcShanten } from "./shanten";
import { Hai, HaiArr, hairi, syantenAll } from "syanten";
import { wrappedRun } from "./entryPoint";

type SuitTileType = "m" | "p" | "s";
type HonorTileType = "z";

type HonorTileNumber = "1" | "2" | "3" | "4" | "5" | "6" | "7";
type SuitTileNumber = HonorTileNumber | "8" | "9" | "0";

type Tile = `${HonorTileNumber}${HonorTileType}` | `${SuitTileNumber}${SuitTileType}`;

type OpenedMeld = {
  hand: Tile[];
  discard?: Tile;
};

const TILE_RE = /^([0-9][mps]|[1-7]z)$/;

const KITA = "4z";

function isValidTile(tile: string): tile is Tile {
  return TILE_RE.test(tile);
}
function validateTile(tile: string): Tile {
  if (!isValidTile(tile)) {
    throw new Error(`Invalid tile: ${tile}`);
  }
  return tile;
}
function isEquivantTile(a: Tile, b: Tile): boolean {
  if (a === b) {
    return true;
  }
  if (a.charAt(1) !== b.charAt(1)) {
    return false;
  }
  return ["0", "5"].includes(a.charAt(0)) && ["0", "5"].includes(b.charAt(0));
}

function tilesToHaiArr(tiles: Tile[]): HaiArr {
  const ret: HaiArr = [
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
      assert(n >= 1 && n <= 7);
    } else {
      if (n === 0) {
        n = 5;
      }
      assert(n >= 1 && n <= 9);
    }
    const typeIndex = INDEXER.indexOf(type);
    assert(typeIndex >= 0);
    ret[typeIndex][n - 1]++;
    assert(ret[typeIndex][n - 1] <= 4);
  }
  return ret;
}

class TileBin {
  _tiles: { [tile: string]: number };
  constructor() {
    this._tiles = {};
  }
  put(tile: Tile): void {
    if (/^0[mps]$/.test(tile)) {
      tile = ("5" + tile.charAt(1)) as Tile;
    }
    this._tiles[tile] = (this._tiles[tile] || 0) + 1;
    assert(this._tiles[tile] <= 4);
  }
  getNum(tile: Tile): number {
    if (/^0[mps]$/.test(tile)) {
      tile = ("5" + tile.charAt(1)) as Tile;
    }
    return this._tiles[tile] || 0;
  }
}

class Player {
  _hand: Tile[];
  _opened: OpenedMeld[];
  _discarded: Tile[];

  constructor(hand: Tile[]) {
    assert(hand.length === 13 || hand.length === 14);
    this._hand = hand;
    this._opened = [];
    this._discarded = [];
  }
  deal(tile: Tile): void {
    assert.equal(this._hand.length % 3, 1);
    this._hand.push(tile);
  }
  discard(tile: Tile): void {
    assert.equal(this._hand.length % 3, 2);
    const index = this._hand.indexOf(tile);
    if (index === -1) {
      throw new Error(`Not in hand: ${tile}`);
    }
    this._discarded.push(tile);
    this._hand.splice(index, 1);
  }
  kita(): void {
    const tile = KITA;
    assert.equal(this._hand.length % 3, 2);
    const index = this._hand.indexOf(tile);
    if (index === -1) {
      throw new Error(`Not in hand: ${tile}`);
    }
    this._opened.push({ hand: [tile] });
    this._hand.splice(index, 1);
  }
  open(tile: Tile, handTiles: Tile[]): void {
    assert.equal(this._hand.length % 3, 1);
    assert(handTiles.length === 2 || handTiles.length === 3);
    for (const handTile of handTiles) {
      const index = this._hand.indexOf(handTile);
      if (index === -1) {
        throw new Error(`Not in hand: ${handTile}`);
      }
      this._hand.splice(index, 1);
    }
    this._opened.push({ hand: handTiles, discard: tile });
  }
  kan(tile: Tile) {
    assert.equal(this._hand.length % 3, 2);
    let meld = this._opened.find(
      (x) =>
        x.hand.length === 2 &&
        x.discard &&
        isEquivantTile(x.discard, tile) &&
        x.hand.every((t) => isEquivantTile(t, tile))
    );
    if (meld) {
      const index = this._hand.indexOf(tile);
      if (index === -1) {
        throw new Error(`Not in hand: ${tile}`);
      }
      meld.hand.push(tile);
      this._hand.splice(index, 1);
    } else {
      const meld: Tile[] = [];
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
  syanten(): number {
    return calcShanten(this._hand);
  }
  isKokushiTenpai(): boolean {
    if (this._hand.length !== 13) {
      return false;
    }
    const tiles = {} as { [key: string]: number };
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
  ".lq.RecordDealTile": new lq.RecordDealTile(),
  ".lq.RecordChiPengGang": new lq.RecordChiPengGang(),
  ".lq.RecordDiscardTile": new lq.RecordDiscardTile(),
  ".lq.RecordNoTile": new lq.RecordNoTile(),
  ".lq.RecordHule": new lq.RecordHule(),
  ".lq.RecordBaBei": new lq.RecordBaBei(),
  ".lq.RecordAnGangAddGang": new lq.RecordAnGangAddGang(),
  ".lq.RecordLiuJu": new lq.RecordLiuJu(),
};

export class MajsoulGameAnalyzer {
  _players: Player[];
  _pendingTile: Tile | undefined;
  constructor(newRoundRecord: lq.RecordNewRound) {
    assert([3, 4].includes(newRoundRecord.scores.length));
    const { tiles0, tiles1, tiles2, tiles3 } = newRoundRecord;
    const tiles = [tiles0, tiles1, tiles2, tiles3].slice(0, newRoundRecord.scores.length);
    this._players = tiles.map((t) => new Player(t.map(validateTile)));
  }
  getRemainingNumTiles(seat: number, tiles: (Tile | string)[]): number {
    const bin = new TileBin();
    for (const player of this._players) {
      player._discarded.forEach((x) => bin.put(x));
      player._opened.forEach((x) => x.hand.forEach((t) => bin.put(t)));
    }
    this._players[seat]._hand.forEach((x) => bin.put(x));
    let ret = 0;
    for (const tile of tiles) {
      ret += 4 - bin.getNum(validateTile(tile));
    }
    return ret;
  }
  processRecord<T extends keyof typeof ACCEPTED_RECORD_TYPES>(
    recordName: keyof typeof ACCEPTED_RECORD_TYPES,
    record: typeof ACCEPTED_RECORD_TYPES[T]
  ): void {
    if (!(recordName in ACCEPTED_RECORD_TYPES)) {
      throw new Error(`Unknown record: ${recordName}`);
    }
    switch (recordName) {
      case ".lq.RecordDealTile": {
        const r = record as lq.RecordDealTile;
        this._players[r.seat].deal(validateTile(r.tile));
        this._pendingTile = undefined;
        break;
      }
      case ".lq.RecordDiscardTile": {
        const r = record as lq.RecordDiscardTile;
        const tile = validateTile(r.tile);
        this._players[r.seat].discard(tile);
        this._pendingTile = tile;
        if (r.tingpais?.length) {
          assert(this._players[r.seat].syanten() === 0 || this._players[r.seat].isKokushiTenpai());
        }
        if (r.is_liqi) {
          assert(r.zhenting.length === this._players.length);
          assert(
            r.zhenting[r.seat] ===
              this._players[r.seat]._discarded.some((x) =>
                r.tingpais.some((t) => isEquivantTile(validateTile(t.tile!), validateTile(x)))
              )
          );
          this.getRemainingNumTiles(
            r.seat,
            r.tingpais.map((x) => x.tile!)
          );
        }
        break;
      }
      case ".lq.RecordChiPengGang": {
        const r = record as lq.RecordChiPengGang;
        const tiles = r.tiles.map(validateTile);
        if (!this._pendingTile) {
          throw new Error("No pending tile");
        }
        if (tiles.length < 3) {
          throw new Error("Unexpected number of tiles: " + tiles.length);
        }
        const index = tiles.indexOf(this._pendingTile);
        assert(index !== -1);
        tiles.splice(index, 1);
        this._players[r.seat].open(this._pendingTile, tiles);
        this._pendingTile = undefined;
        break;
      }
      case ".lq.RecordBaBei": {
        const r = record as lq.RecordBaBei;
        this._players[r.seat].kita();
        this._pendingTile = KITA;
        break;
      }
      case ".lq.RecordAnGangAddGang": {
        const r = record as lq.RecordAnGangAddGang;
        const tile = validateTile(r.tiles);
        this._players[r.seat].kan(tile);
        this._pendingTile = tile;
        break;
      }
    }
  }
}

if (require.main === module) {
  wrappedRun(async () => {
    console.log(process.argv[2]);
    const root = Root.fromJSON(JSON.parse(readFileSync("majsoulPb.proto.json", "utf8")));
    for (const file of process.argv.slice(2)) {
      const wrappedData = lq.Wrapper.decode(readFileSync(file));
      const type = root.lookupType(wrappedData.name);
      const msg = type.decode(wrappedData.data) as lq.IGameDetailRecords;

      let gameAnalyzer: MajsoulGameAnalyzer;
      for (const actionData of msg?.actions || []) {
        if (!actionData.result?.length) {
          continue;
        }
        const wrappedResult = lq.Wrapper.decode(actionData.result);
        const type = root.lookupType(wrappedResult.name);
        const record = type.decode(wrappedResult.data);
        if (wrappedResult.name === ".lq.RecordNewRound") {
          gameAnalyzer = new MajsoulGameAnalyzer(record as unknown as lq.RecordNewRound);
        } else {
          assert(gameAnalyzer!);
          gameAnalyzer!.processRecord(wrappedResult.name as any, record as any);
        }
      }
    }
  });
}
