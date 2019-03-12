const args = process.argv.slice(2);
const WebSocket = require("ws");
const uuidv1 = require("uuid/v1");

const wsURL = "ws://localhost:3000";

const pingTimeout = 5000;
const pongTimeout = 5000;

let wsClient = null,
  pingTimeoutId = null,
  pongTimeoutId = null,
  msgTimeoutId = null;

let toAckMsgs = {}; // 发送后待确认的消息
let localQueueMsgs = {}; //

// 重连
function reconnect({ openId, env }) {
  wsClient = null;
  // 延时重连
  setTimeout(() => {
    createWsCon({ openId, env });
  }, 3000);
}

// 心跳
function heart(wsClient) {
  // 清除timeout
  clearTimeout(pingTimeoutId);
  clearTimeout(pongTimeoutId);

  // 连接open或收到msg时, 间隔pingTimeout发送ping，并检测pongtimeout后能否收到pong(收到则重置，否则断开重连)
  pingTimeoutId = setTimeout(() => {
    wsClient.ping();
    pongTimeoutId = setTimeout(() => {
      wsClient.close();
    }, pongTimeout);
  }, pingTimeout);
}

// 创建连接
function createWsCon({ openId, env }) {
  if (wsClient) {
    return wsClient;
  }
  wsClient = new WebSocket(wsURL);
  console.log("connect starting....");
  wsClient.on("open", res => {
    // 心跳重置
    heart(wsClient);
    console.log("connect open....");
    // 建立连接后传 openid env collection
    const msgId = uuidv1();
    let msg = {
      msgData: {
        type: "ref",
        data: {
          openId,
          env,
          collection: args[0]
        }
      },
      msgId
    };
    toAckMsgs[msgId] = msg;

    function sendAndCheckAck(msg) {
      wsClient.send(JSON.stringify(msg), () => {
        // 定时检查是否收到同msgId对应的ack
        console.log("发送msg后待ack...");
        msgTimeoutId = setTimeout(() => {
          if (toAckMsgs[msg.msgId]) {
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
  });

  wsClient.on("message", msg => {
    // console.log("client receive message: ", msg);
    // 心跳重置
    heart(wsClient);

    let msgObj = JSON.parse(msg);
    if (msgObj.msgData.type === "ack") {
      //收到ack消息后，toAckMsgs中删除该消息
      if (toAckMsgs[msgObj.msgId]) {
        toAckMsgs[msgObj.msgId] = null;
        delete toAckMsgs[msgObj.msgId];
      }
    }

    if (msgObj.msgData.type === "queueMsg") {
      // console.log("client receive queueMsg:", msgObj.msgData.data);
      let { msgSeq, msgData } = msgObj;
      let { env, openId, collection } = msgData;
      // 收到queueMsg时，需保证队列消息有序并去重
      // 发送queue msg ack
      wsClient.send(
        JSON.stringify({
          msgData: {
            type: "queueAck",
            env,
            openId,
            collection
          },
          msgSeq: msgSeq
        })
      );

      // 消息去重
      dealMultiQueueMsg(msgObj);
    }
  });

  wsClient.on("error", err => {
    console.log("client receive error:", err);
    reconnect({ openId, env });
  });

  wsClient.on("close", (code, reason) => {
    console.log("close code: ", code);
    console.log("close reason: ", reason);
    reconnect({ openId, env });
  });

  wsClient.on("pong", () => {
    console.log("client receive pong");
    //心跳重置
    heart(wsClient);
  });

  return wsClient;
}

function dealMultiQueueMsg(msgObj) {
  // 区分当前消息对应的collection
  let { msgData, msgSeq } = msgObj;
  let { collection } = msgData;

  msgSeq = parseInt(msgSeq);
  if (!localQueueMsgs[collection]) {
    localQueueMsgs[collection] = [];
  }

  let flag = false,
    msgArr = localQueueMsgs[collection],
    i;
  length = msgArr.length;
  for (i = length - 1; i >= 0; i--) {
    if (msgArr[i] < msgSeq) {
      break;
    }
    if (msgArr[i] === msgSeq) {
      flag = true;
      break;
    }
  }

  if (!flag) {
    msgArr.splice(i + 1, 0, msgSeq);
  }

  console.log("客户端的消息数组:", JSON.stringify(localQueueMsgs));
}

createWsCon({ openId: args[1], env: args[2] }); // node c1 1 test / c2 2 test / c3 3 test
