const protobuf = require("protobufjs");
const assert = require("assert");
const WebSocket = require("ws");
const rp = require("request-promise");
const uuidv4 = require("uuid/v4");

const { URL_BASE, ACCESS_TOKEN, PREFERRED_SERVER, OAUTH_TYPE } = require("./env");

class MajsoulProtoCodec {
  constructor (pbDef, version) {
    this._pb = protobuf.Root.fromJSON(pbDef);
    this._index = 1;
    this._wrapper = this._pb.nested.lq.Wrapper;
    this._inflightRequests = {};
    this.version = version;
    this.rawDefinition = pbDef;
  }
  lookupMethod (path) {
    if (typeof path === "string") {
      path = path.split(".");
    }
    if (0 === path.length) {
      return null;
    }
    const service = this._pb.lookupService(path.slice(0, -1));
    if (!service) {
      return null;
    }
    const name = path[path.length - 1];
    return service.methods[name];
  }
  /**
   * @param {Buffer} buf
   */
  decodeMessage (buf) {
    const { REQUEST, RESPONSE } = MajsoulProtoCodec;
    const type = buf[0];
    assert([REQUEST, RESPONSE].includes(type));
    const reqIndex = buf[1] | (buf[2] << 8);
    const msg = this._wrapper.decode(buf.slice(3));
    let typeObj, methodName;
    if (type === REQUEST) {
      methodName = msg.name;
      const methodObj = this.lookupMethod(msg.name);
      const typeName = methodObj.requestType;
      typeObj = methodObj.parent.parent.lookupType(typeName);
    } else {
      ({ typeObj, methodName } = this._inflightRequests[reqIndex] || {});
      if (!typeObj) {
        throw new Error(`Unknown request ${reqIndex}`);
      }
      delete this._inflightRequests[reqIndex];
    }
    return {
      type,
      reqIndex,
      methodName,
      payload: typeObj.decode(msg.data),
    };
  }
  decodeDataMessage (buf, typeName) {
    const msg = this._wrapper.decode(buf);
    const typeObj = this._pb.lookupType(typeName || msg.name);
    return {
      dataType: msg.name,
      payload: typeObj.decode(msg.data),
    };
  }
  encodeRequest ({ methodName, payload }) {
    const currentIndex = this._index++;
    const methodObj = this.lookupMethod(methodName);
    const requestType = methodObj.parent.parent.lookupType(methodObj.requestType);
    const responseType = methodObj.parent.parent.lookupType(methodObj.responseType);
    const msg = this._wrapper.encode({
      name: methodName,
      data: requestType.encode(payload).finish(),
    }).finish();
    this._inflightRequests[currentIndex] = {
      methodName,
      typeObj: responseType,
    };
    return Buffer.concat([
      Buffer.from([MajsoulProtoCodec.REQUEST, currentIndex & 0xff, currentIndex >> 8]),
      msg,
    ]);
  }
}
Object.assign(MajsoulProtoCodec, {
  REQUEST: 2,
  RESPONSE: 3,
});

class MajsoulConnection {
  constructor (server, codec, onConnect, timeout = 10000) {
    this._server = server;
    this._timeout = timeout;
    this._pendingMessages = [];
    this._codec = codec;
    this._onConnect = onConnect;
    this.reconnect();
  }
  reconnect () {
    this._ready = false;
    if (this._socket) {
      this._socket.terminate();
    }
    this._createWaiter();
    console.log("Connecting to " + this._server);
    let agent = undefined;
    if (process.env.http_proxy) {
      console.log(`Using proxy ${process.env.http_proxy}`);
      const url = require("url");
      const HttpsProxyAgent = require("https-proxy-agent");
      agent = new HttpsProxyAgent(url.parse(process.env.http_proxy));
    }
    this._socket = new WebSocket(this._server, { agent });
    this._socket.on("message", (data) => {
      this._pendingMessages.push(data);
      this._waiterResolve();
    });
    this._socket.on("open", () => {
      this._waiterResolve();
      this._pendingMessages = [];
      this._onConnect(this).then(() => {
        this._ready = true;
        this._waiterResolve();
      }).catch((e) => {
        console.error(e);
        this._socket.terminate();
	this._socket = null;
        this._waiterResolve();
        setTimeout(() => this._waiterResolve(), 100);
      });
    });
  }
  async waitForReady () {
    while (!this._ready) {
      if (!this._socket || this._socket.readyState === WebSocket.CLOSED || this._socket.readyState === WebSocket.CLOSING) {
        throw new Error("WebSocket closed before successful connection");
      }
      await this._wait();
    }
  }
  _createWaiter () {
    this._waiter = new Promise((resolve) => {
      this._waiterResolve = resolve;
    });
  }
  async _wait () {
    await Promise.race([
      this._waiter,
      new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), this._timeout)),
    ]);
  }
  close () {
    this._socket.terminate();
    this._pendingMessages.push(undefined);
    this._waiterResolve();
  }
  async readMessage () {
    while (!this._pendingMessages.length) {
      if (!this._socket || this._socket.readyState === WebSocket.CLOSED) {
        return undefined;
      }
      await this._wait();
      this._createWaiter();
    }
    return this._pendingMessages.shift();
  }
  async rpcCall (methodName, payload) {
    if (!this._socket) {
      throw new Error("Connection is broken");
    }
    if (this._socket.readyState === WebSocket.CONNECTING) {
      await this._wait();
    }
    if (this._socket.readyState !== WebSocket.OPEN) {
      this._pendingMessages = [];
      throw new Error("Connection is not opened");
    }
    const req = this._codec.encodeRequest({ methodName, payload });
    this._socket.send(req);
    const resp = await this.readMessage();
    return this._codec.decodeMessage(resp).payload;
  }
}

function getRes (path) {
  return rp({ uri: `${URL_BASE}${path}`, json: true });
}

/**
 *  * Shuffles array in place.
 *   * @param {Array} a items An array containing the items.
 *    */
function shuffle(a) {
  var j, x, i;
  for (i = a.length - 1; i > 0; i--) {
    j = Math.floor(Math.random() * (i + 1));
    x = a[i];
    a[i] = a[j];
    a[j] = x;
  }
  return a;
}

async function createMajsoulConnection (accessToken = ACCESS_TOKEN, preferredServer = PREFERRED_SERVER) {
  let serverListUrl = process.env.SERVER_LIST_URL;
  const wsScheme = process.env.WS_SCHEME || "wss";
  const versionInfo = await getRes("version.json?randv=" + Math.random().toString().slice(2));
  const resInfo = await getRes(`resversion${versionInfo.version}.json`);
  const pbVersion = resInfo.res["res/proto/liqi.json"].prefix;
  const pbDef = await getRes(`${pbVersion}/res/proto/liqi.json`);
  const config = await getRes(`${resInfo.res["config.json"].prefix}/config.json`);
  const ipDef = config.ip.filter((x) => x.name === "player")[0];
  const triedListUrl = [];
  let serverList = null;
  let numTries = 0;
  let lastError = null;
  while (true) {
    try {
      if (!serverListUrl) {
        preferredServer = shuffle((preferredServer || "").split(","))[0];
        serverListUrl = ipDef.region_urls[preferredServer] || ipDef.region_urls.mainland;
        if (!serverListUrl) {
          serverListUrl = ipDef.region_urls.length ? shuffle(ipDef.region_urls)[0] : ipDef.region_urls[shuffle(Object.keys(ipDef.region_urls))[0]];
        }
        serverListUrl += "?service=ws-gateway&protocol=ws&ssl=true";
      }
      if (triedListUrl.includes(serverListUrl)) {
        numTries++;
        if (numTries > 10) {
          throw lastError;
        }
      }
      serverList = await rp({uri: serverListUrl, json: true});
      if (serverList.maintenance) {
        console.log("Maintenance in progress");
        return;
      }
      break;
    } catch (e) {
      if (process.env.SERVER_LIST_URL) {
        throw e;
      }
      lastError = e;
      triedListUrl.push(serverListUrl);
      serverListUrl = null;
      preferredServer = "";
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
  const proto = new MajsoulProtoCodec(pbDef, pbVersion);
  // console.log(proto.decodeMessage(Buffer.from("021e000a192e6c712e4c6f6262792e666574636847616d655265636f7264122d0a2b3139303832332d36346632326534372d376133342d343732302d393737662d323736376561653335373030", "hex")));
  const serverIndex = Math.floor(Math.random() * serverList.servers.length);
  const type = parseInt(OAUTH_TYPE) || 0;
  let server = serverList.servers[serverIndex];
  if (server.indexOf("maj-soul") > -1) {
    server += "/gateway";
  }
  const conn = new MajsoulConnection(`${wsScheme}://${server}`, proto, async (conn) => {
    console.log("Connection established, sending heartbeat");
    await conn.rpcCall(".lq.Lobby.heatbeat", { no_operation_counter: 0 });
    console.log("Authenticating");
    if (type === 7) {
      const [code, uid] = accessToken.split("-");
      const resp = await conn.rpcCall(".lq.Lobby.oauth2Auth", {
        type,
        code,
        uid,
      });
      accessToken = resp.access_token;
    }
    let resp = await conn.rpcCall(".lq.Lobby.oauth2Check", {type, access_token: accessToken});
    if (!resp.has_account) {
      await new Promise((res) => setTimeout(res, 2000));
      resp = await conn.rpcCall(".lq.Lobby.oauth2Check", {type, access_token: accessToken});
    }
    assert(resp.has_account);
    resp = await conn.rpcCall(".lq.Lobby.oauth2Login", {
      type,
      access_token: accessToken,
      reconnect: false,
      device: { device_type: "pc", browser: "safari" },
      random_key: uuidv4(),
      client_version: versionInfo.version,
    });
    assert(resp.account_id);
    console.log("Connection ready");
  });
  await conn.waitForReady();
  return conn;
}

exports.MajsoulProtoCodec = MajsoulProtoCodec;
exports.MajsoulConnection = MajsoulConnection;
exports.createMajsoulConnection = createMajsoulConnection;
exports.getRes = getRes;
