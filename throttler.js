const assert = require("assert");

class Throttler {
  constructor(delay = 100) {
    this._queue = [];
    this._id = 0;
    this._delay = delay;
  }
  waitNext() {
    const id = this._id++;
    const delay = this._delay;
    return new Promise((res) => {
      this._queue.push(() => {
        this._queue[0] = id;
        setTimeout(() => res(id), delay);
      });
      if (this._queue.length === 1) {
        this._queue[0]();
      }
    });
  }
  complete(id) {
    assert(this._queue.length > 0);
    assert(this._queue[0] === id);
    this._queue.shift();
    if (this._queue.length) {
      this._queue[0]();
    }
  }
}
class DummyThrottler {
  waitNext() {}
  complete() {}
}

Object.assign(module.exports, {
  Throttler,
  DummyThrottler,
});
