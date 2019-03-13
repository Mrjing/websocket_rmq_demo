const websocketServer = require("ws").Server;
const amqp = require("amqplib");
const wss = new websocketServer({ port: 3000 });
const uuidv1 = require("uuid/v1");

let mqCon = null;
let channel = null;

const rabbitMQURL = "amqp://localhost";
const exchange = "test"; // 转发器名

const toAckQueueMsg = {};

// 连接rabbitMQ服务
amqp
  .connect(rabbitMQURL)
  .then(conn => {
    mqCon = conn;
    conn.on("close", () => {
      mqCon = null;
    });
  })
  .catch(err => {
    console.log("err:", err);
  });

function consumeMsg(wsClient, collection) {
  if (!mqCon) {
    console.log("mqCon closed");
    return;
  }

  if (channel) {
    consume(channel);
  } else {
    mqCon
      .createChannel()
      .then(ch => {
        channel = ch;
        consume(channel);
      })
      .catch(err => {
        console.log("err:", err);
      });
  }

  function consume(channel) {
    console.log("consume");
    // wsClient.MQChannel = channel;
    channel.assertExchange(exchange, "direct", { durable: true });
    channel
      .assertQueue("", { exclusive: true }) // 分配临时队列
      .then(q => {
        channel.bindQueue(q.queue, exchange, collection);
        console.log("bindQueue ok");
        channel.consume(
          q.queue,
          msg => {
            if (wsClient.readyState !== 1) {
              console.log(`当前连接未open`);
              return;
            }

            // 消息暂存toAckQueueMsg (消息序号用整数消息值)
            let { openId, env } = wsClient.userInfoObj;
            let msgValue = msg.content.toString();
            let msgContent = {
              msgData: {
                type: "queueMsg",
                data: {
                  msgValue,
                  env,
                  openId,
                  collection,
                  msgSeq: msgValue
                }
              },
              msgId: uuidv1()
            };
            toAckQueueMsg[`${openId}_${env}`] =
              toAckQueueMsg[`${openId}_${env}`] || {};
            let tempMsgObj = toAckQueueMsg[`${openId}_${env}`];
            tempMsgObj[collection] = {};
            tempMsgObj[collection][msgValue] = msgContent;

            let msgTimeoutId = null;
            function sendAndCheckAck(msgContent) {
              wsClient.send(JSON.stringify(msgContent), () => {
                // 定时检查是否收到当前临时队列下msgSeq对应的ack
                console.log("发送queue msg后等待ack");
                msgTimeoutId = setTimeout(() => {
                  if (toAckQueueMsg[`${openId}_${env}`][collection][msgValue]) {
                    // 仍待确认
                    sendAndCheckAck(msgContent);
                  } else {
                    console.log("完成queue msg ack");
                    clearTimeout(msgTimeoutId);
                  }
                }, 2000);
              });
            }
            sendAndCheckAck(msgContent);
          },
          { noAck: false }
        );
      })
      .catch(err => {
        console.log("err:", err);
      });
  }
}

wss.on("connection", ws => {
  ws.on("message", message => {
    // 解析收到的消息类型
    // 1. 'ref' 传订阅的数据库表名 openid env, 此时需建立临时队列
    let msgObj = JSON.parse(message);
    if (msgObj.msgData.type === "ref") {
      let { openId, env, collection } = msgObj.msgData.data;
      if (!ws.userInfoObj) {
        ws.userInfoObj = {
          openId,
          env
        };
      }
      consumeMsg(ws, collection);
      // 收到消息后回传ack
      ws.send(
        JSON.stringify({
          msgData: {
            type: "ack",
            data: {}
          },
          msgId: msgObj.msgId
        })
      );
    }

    if (msgObj.msgData.type === "queueAck") {
      // 收到当前连接下队列消息的确认
      let { msgData } = msgObj;
      let { env, openId, collection, msgSeq } = msgData.data;
      console.log("received: %s", msgSeq);
      let currentCol = toAckQueueMsg[`${openId}_${env}`][collection];
      if (currentCol && currentCol[msgSeq]) {
        currentCol[msgSeq] = null;
        delete currentCol[msgSeq];
      }
    }
  });

  ws.on("ping", () => {
    console.log("receive ping");
  });
});
