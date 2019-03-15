const websocketServer = require("ws").Server;
const amqp = require("amqplib");
const wss = new websocketServer({ port: 3000 });
const uuidv1 = require("uuid/v1");

let mqCon = null;
let channel = null;

const rabbitMQURL = "amqp://localhost";
const exchange = "test"; // 转发器名

const toAckQueueMsg = {};

let queueMap = {};

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

  function consumeChannel(channel, queue) {
    channel.consume(
      queue,
      msg => {
        let { openId, env } = wsClient.userInfoObj;
        let msgValue = msg.content.toString();
        let { consumerTag, routingKey } = msg.fields;
        console.log(`待消费msg ${msgValue}`);
        if (wsClient.readyState !== 1) {
          console.log(`当前连接未open ${openId}.${env}.${collection}`);
          channel.nack(msg);
          channel.cancel(consumerTag);
          return;
        }
        // 消息暂存toAckQueueMsg (消息序号用整数消息值)
        // let { openId, env } = wsClient.userInfoObj;
        // let msgValue = msg.content.toString();
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
        tempMsgObj[collection] = tempMsgObj[collection] || {};
        tempMsgObj[collection][msgValue] = msgContent;

        let msgTimeoutId = null;
        function sendAndCheckAck(msgContent) {
          console.log("wsClients:************", Array.from(wss.clients).length);

          wsClient.send(JSON.stringify(msgContent), () => {
            // 定时检查是否收到当前临时队列下msgSeq对应的ack
            console.log(
              "current wsclient:",
              wsClient.userInfoObj.timeStamp,
              wsClient.readyState
            );
            console.log(`发送queue msg ${msgValue}后等待ack`);

            msgTimeoutId = setTimeout(() => {
              if (wsClient.readyState !== 1) {
                clearTimeout(msgTimeoutId);
                channel.nack(msg);
                return;
              }
              if (
                toAckQueueMsg[`${openId}_${env}`][collection][msgValue] !==
                undefined
              ) {
                sendAndCheckAck(msgContent);
              } else {
                console.log(`完成queue msg ${msgValue} 消费 ack`);
                channel.ack(msg);
                clearTimeout(msgTimeoutId);
              }
            }, 2000);
          });
        }
        sendAndCheckAck(msgContent);
      },
      { noAck: false }
    );
  }

  function consume(channel) {
    console.log("consume");
    // wsClient.MQChannel = channel;
    let { openId, env } = wsClient.userInfoObj;
    // let currentQueueName = `${openId}.${env}.${collection}`;

    let currentQueueKey = `${openId}.${env}.${collection}`;
    if (!channel.checkExchange(exchange)) {
      channel.assertExchange(exchange, "direct", { durable: true });
    }
    console.log("currentQueueKey:", currentQueueKey);
    if (queueMap[currentQueueKey]) {
      consumeChannel(channel, queueMap[currentQueueKey]);
    } else {
      channel
        .assertQueue(currentQueueKey, { durable: true }) // 分配持久队列
        .then(q => {
          channel.bindQueue(q.queue, exchange, currentQueueKey);
          console.log("bindQueue ok");
          consumeChannel(channel, q.queue);
        })
        .catch(err => {
          console.log("err:", err);
        });
    }
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
          env,
          timeStamp: new Date().getTime()
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
      console.log("received queueAck: %s", msgSeq);
      let currentCol = toAckQueueMsg[`${openId}_${env}`][collection];
      if (currentCol && currentCol[msgSeq]) {
        currentCol[msgSeq] = null;
        delete currentCol[msgSeq];
      }
    }

    if (msgObj.msgData.type === "ping") {
      // 收到ping，回pong
      let pongMsg = {
        msgData: {
          type: "pong",
          data: null
        },
        msgId: null
      };
      console.log("receive ping....");
      ws.send(JSON.stringify(pongMsg));
    }
  });

  ws.on("close", () => {});

  // ws.on("ping", () => {
  //   console.log("receive ping");
  // });
});
