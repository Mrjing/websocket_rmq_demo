var amqp = require("amqplib");

const collectionNames = ["c1", "c2", "c3"];
const rabbitMQURL = "amqp://localhost";
const exchange = "test";
let mqCon = null;
let channel = null;
let number = 0;
let sendNumberTimeout;

const testNumberArr = [
  19,
  1,
  0,
  2,
  3,
  8,
  5,
  4,
  7,
  6,
  11,
  12,
  14,
  13,
  15,
  18,
  17,
  10,
  9,
  16
];

amqp
  .connect(rabbitMQURL)
  .then(conn => {
    mqCon = conn;
    return conn;
  })
  .then(conn => {
    conn.createChannel().then(ch => {
      ch.assertExchange(exchange, "direct", { durable: true });

      function sendNumber() {
        // pushToMQ(number++);

        if (number == 20) {
          clearTimeout(sendNumberTimeout);
          return;
        }
        // 1~20的序号 随机分发
        pushToMQ(testNumberArr[number]);
        number++;
        sendNumberTimeout = setTimeout(sendNumber, 1000);
      }

      sendNumber();
    });
  })
  .catch(err => {
    console.log("err:", err);
  });

function pushToMQ(message) {
  if (!mqCon) {
    console.log("MQConn closed");
    return;
  }

  // let seq = Math.floor(Math.random() * Math.floor(3));
  let seq = 0;
  const collection = `${seq + 1}.test.${collectionNames[seq]}`;
  // const collectionC1 = `${seq + 1}.test.${collectionNames[seq]}`;
  // const collectionC2 = `${seq + 2}.test.${collectionNames[seq]}`;

  message = JSON.stringify(message);
  if (channel) {
    channel.assertExchange(exchange, "direct", { durable: true });
    console.log("publish msg and collection:", message, collection);
    // console.log("publish msg and collection:", message, collectionC2);
    channel.publish(exchange, collection, Buffer.from(message), {
      persistent: true
    });
    // channel.publish(exchange, collectionC2, Buffer.from(message), {
    //   persistent: true
    // });
  } else {
    mqCon
      .createChannel()
      .then(ch => {
        channel = ch;
        channel.assertExchange(exchange, "direct", { durable: true });
        console.log("publish msg and collection:", message, collection);
        // console.log("publish msg and collection:", message, collectionC2);
        channel.publish(exchange, collection, Buffer.from(message), {
          persistent: true
        });
        // channel.publish(exchange, collectionC2, Buffer.from(message), {
        //   persistent: true
        // });
      })
      .catch(err => {
        console.log("err:", err);
      });
  }
}
