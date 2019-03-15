// const args = process.argv.slice(2);
// const WebSocket = require("ws");
// const uuidv1 = require("uuid/v1");
// const NodeCache = require("node-cache");

let toAckMsgs = {}; // 发送后待确认的消息
// const queueMsgCache = new NodeCache();
const queueMsgCache = lscache;

function WSClient({
  pingTimeout,
  pongTimeout,
  reconnectTimeout,
  wsURL,
  params
}) {
  this.opts = {
    wsURL,
    pingTimeout,
    pongTimeout,
    reconnectTimeout
  };

  this.ws = null;
  this.params = params;
  this.queueMsgSeq = 0;
}

WSClient.prototype.createWsCon = function() {
  if (this.ws) {
    return;
  }
  try {
    this.ws = new WebSocket(this.opts.wsURL);
    this.initEventHandle();
  } catch (e) {
    this.reconnect();
    throw e;
  }
};

WSClient.prototype.createMsg = function({ msgId, msgType, data }) {
  const msg = {
    msgData: {
      type: msgType,
      data
    },
    msgId
  };
  return msg;
};

WSClient.prototype.sendMsgCheckAck = function(msg) {
  let msgTimeoutId = null;
  let self = this;
  function sendAndCheckAck(msg) {
    self.send(JSON.stringify(msg), () => {
      // 定时检查是否收到同msgId对应的ack
      console.log("发送msg后待ack...");
      msgTimeoutId = setTimeout(() => {
        if (toAckMsgs[msg.msgId] !== undefined) {
          // 仍待确认
          sendAndCheckAck(msg);
        } else {
          console.log("完成ack....");
          clearTimeout(msgTimeoutId);
        }
      }, 2000);
    });
  }
  sendAndCheckAck(msg);
};

WSClient.prototype.AckMsg = function(msg) {
  this.send(msg, callback);
};

WSClient.prototype.checkWSOpen = function() {
  if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
    // 不是open态則重置心跳, 重連 todo
    this.reconnect();
    return false;
  }
  return true;
};

WSClient.prototype.send = function(msg, callback) {
  if (!this.checkWSOpen()) {
    // 非open
    return;
  }
  this.ws.send(msg, callback);
};

WSClient.prototype.initEventHandle = function() {
  // const { openId, env, collection } = this.params;
  console.log("connect starting....");

  this.ws.addEventListener("open", res => {
    // this.ws.on("open", res => {
    // 心跳重置
    this.heart();
    console.log("connect open....");
    // 建立连接后传 openid env collection
    const msgId = uuidv1();
    const msg = this.createMsg({ msgId, msgType: "ref", data: this.params });

    toAckMsgs[msgId] = msg;
    this.sendMsgCheckAck(msg);
  });

  this.ws.addEventListener("message", msg => {
    // this.ws.on("message", msg => {
    // 心跳重置
    this.heart();
    let msgObj = JSON.parse(msg.data);
    if (msgObj.msgData.type === "ack") {
      //收到ack消息后，toAckMsgs中删除该消息
      if (toAckMsgs[msgObj.msgId]) {
        console.log("收到ack");
        toAckMsgs[msgObj.msgId] = null;
        delete toAckMsgs[msgObj.msgId];
      }
    }

    if (msgObj.msgData.type === "queueMsg") {
      let { msgData } = msgObj;
      let { env, openId, collection } = msgData.data;
      // 收到queueMsg时，需保证队列消息有序并去重, 如果消息未按序达，丢弃
      const currentQueueSeqKey = `${openId}.${env}.${collection}.msgSeq`;

      let value = queueMsgCache.get(currentQueueSeqKey);
      if (value == undefined) {
        console.log("currentQueueSeqKey", currentQueueSeqKey, value);
        queueMsgCache.set(currentQueueSeqKey, {
          msgSeq: 0
        });
      }
      this.dealMultiQueueMsg(currentQueueSeqKey, msgObj);

      // 消息去重
      // dealMultiQueueMsg(msgObj);
    }

    if (msgObj.msgData.type === "pong") {
      console.log("client receive pong....");
      //心跳重置
      this.heart();
    }
  });

  this.ws.addEventListener("error", err => {
    // this.ws.on("error", err => {
    console.log("client receive error:", err);
    this.reconnect();
  });

  this.ws.addEventListener("close", (code, reason) => {
    // this.ws.on("close", (code, reason) => {
    console.log("close code: ", code);
    console.log("close reason: ", reason);
    this.reconnect();
  });

  // this.ws.on("pong", () => {
  //   console.log("client receive pong");
  //   //心跳重置
  //   this.heart();
  // });
};

WSClient.prototype.dealMultiQueueMsg = function(queueSeqKey, msgObj) {
  let { msgData, msgId } = msgObj;
  let { env, openId, collection, msgSeq } = msgData.data;

  const queueMsgId = `${openId}.${env}.${collection}.${msgId}`;

  let value = queueMsgCache.get(queueMsgId);
  if (value == undefined) {
    // 判断新消息序号，是否丢掉消息
    let seqValue = queueMsgCache.get(queueSeqKey);
    // 发送queue msg ack
    let msgAck = this.createMsg({
      msgId: uuidv1(),
      msgType: "queueAck",
      data: {
        env,
        openId,
        collection,
        msgSeq
      }
    });
    if (seqValue.msgSeq == parseInt(msgSeq)) {
      // 序号正确， 缓存消息及序号后发送queue msg ack
      queueMsgCache.set(queueSeqKey, {
        msgSeq: seqValue.msgSeq + 1
      });
      queueMsgCache.set(queueMsgId, {
        msgValue: msgSeq
      });

      this.send(JSON.stringify(msgAck));
      //
      // const keys = queueMsgCache.keys();
      // const allKeyValue = queueMsgCache.mget(keys);
      // console.log("本地缓存:", queueMsgCache);
      console.log(`本地消息序号${seqValue.msgSeq} 传入消息序号${msgSeq} 匹配`);
    } else {
      // 判断传入消息序号是否大于当前序号
      if (seqValue.msgSeq < parseInt(msgSeq)) {
        console.log(
          `本地消息序号${seqValue.msgSeq} 传入消息序号${msgSeq} 不匹配 丢弃`
        );
      } else {
        // 小于当前序号，表明该消息已收到，回ACK
        this.send(JSON.stringify(msgAck));
      }
    }
  } else {
    // 重复消息直接丢掉
    console.log(`${queueMsgId} 消息重复`);
  }
};

WSClient.prototype.reconnect = function() {
  this.ws = null;
  this.cleanHeart();
  setTimeout(() => {
    this.createWsCon();
  }, this.opts.reconnectTimeout);
};

WSClient.prototype.cleanHeart = function() {
  // 清除timeout
  clearTimeout(this.pingTimeoutId);
  clearTimeout(this.pongTimeoutId);
};

WSClient.prototype.heart = function() {
  this.cleanHeart();
  let { pingTimeout, pongTimeout } = this.opts;
  // 连接open或收到msg时, 间隔pingTimeout发送ping，并检测pongtimeout后能否收到pong(收到则重置，否则断开重连)
  this.pingTimeoutId = setTimeout(() => {
    this.ping();
    this.pongTimeoutId = setTimeout(() => {
      this.close();
    }, pongTimeout);
  }, pingTimeout);
};

WSClient.prototype.ping = function() {
  if (!this.checkWSOpen()) {
    return;
  }
  let pingMsg = this.createMsg({ msgId: null, msgType: "ping", data: null });
  this.ws.send(JSON.stringify(pingMsg));
  // this.ws.ping();
};

WSClient.prototype.close = function() {
  this.ws.close();
};

window.WSClient = WSClient;

let wsIns = new WSClient({
  pingTimeout: 5000,
  pongTimeout: 5000,
  reconnectTimeout: 3000,
  wsURL: "ws://localhost:3000",
  // params: { openId: args[1], env: args[2], collection: args[0] }
  params: { openId: "1", env: "test", collection: "c1" }
});

wsIns.createWsCon();
