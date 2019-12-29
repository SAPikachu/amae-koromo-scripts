const { CalculateShanten } = require("./shanten-core");

function handStrToArray (hand) {
  const ret = [];
  while (hand) {
    const m = /^\s*([0-9]+)([mspz])\s*/i.exec(hand);
    if (!m) {
      throw new Error("Failed to parse: " + hand);
    }
    const type = m[2].toLowerCase();
    for (const num of m[1]) {
      if (type === "z" && "1234567".indexOf(num) === -1) {
        throw new Error(`Invalid combination: ${num}${type}`);
      }
      ret.push(num + type);
    }
    hand = hand.slice(m[0].length);
  }
  return ret;
}

function handToTiles34 (hand) {
  if (typeof hand === "string") {
    hand = handStrToArray(hand);
  }
  const tiles34 = Array(34).fill(0);
  const BASES = {
    m: 0,
    p: 9,
    s: 18,
    z: 27,
  };
  for (const tile of hand) {
    let num = parseInt(tile[0], 10);
    const type = tile[1];
    if (!(type in BASES)) {
      throw new Error("Invalid type: " + type);
    }
    if (type === "z" && (num < 1 || num > 7)) {
      throw new Error(`Invalid combination: ${num}${type}`);
    }
    if (num === 0) {
      num = 5;
    }
    const index = BASES[type] + (num - 1);
    tiles34[index]++;
    if (tiles34[index] > 4) {
      throw new Error(`Too many tiles: ${num}${type}`);
    }
  }
  return tiles34;
}

function calcShanten (hand) {
  return CalculateShanten(handToTiles34(hand));
}

function main () {
  const assert = require("assert");
  assert.equal(1, CalculateShanten(handToTiles34("33m 5555p 66s 556666z")));
  assert.equal(4, CalculateShanten(handToTiles34("13579m 13579s 135p")));
  assert.equal(3, CalculateShanten(handToTiles34("13579m 12379s 135p")));
  assert.equal(1, CalculateShanten(handToTiles34("123456789m 147s 14m")));
  assert.equal(2, CalculateShanten(handToTiles34("123456789m 147s 1m")));
  assert.equal(6, CalculateShanten(handToTiles34("258m 258s 258p 12345z"))); // 和牌最远
  assert.equal(0, CalculateShanten(handToTiles34("123456789m 1134p")));
  assert.equal(-1, CalculateShanten(handToTiles34("123456789m 11345p")));

  assert.equal(-1, CalculateShanten(handToTiles34("11223344556677z")));
  assert.equal(0, CalculateShanten(handToTiles34("1223344556677z")));
  assert.equal(0, CalculateShanten(handToTiles34("1m 1223344556677z")));
  assert.equal(0, CalculateShanten(handToTiles34("1223344556677z")));
  assert.equal(1, CalculateShanten(handToTiles34("12m 123344556677z")));
  assert.equal(1, CalculateShanten(handToTiles34("1m 123344556677z")));
  assert.equal(1, CalculateShanten(handToTiles34("11222233445566z")));

  console.log(CalculateShanten(handToTiles34([
    "1m",
    "3m",
    "5m",
    "6m",
    "4p",
    "4p",
    "5p",
    "6p",
    "7p",
    "8p",
    "9p",
    "1s",
    "9s",
    "7z"
  ])));
}

if (require.main === module) {
  main();
} else {
  module.exports = { calcShanten };
}

// vim: sw=2:ts=2:expandtab:fdm=syntax
