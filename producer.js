var amqp = require("amqplib");
var args = process.argv.slice(2);

const collectionNames = ["c1", "c2", "c3"];
const rabbitMQURL = "amqp://localhost";
const exchange = "test";
let mqCon = null;
let channel = null;
let number = 0;

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

        // 1~20的序号 随机分发
        pushToMQ(Math.floor(Math.random() * Math.floor(20)));
        setTimeout(sendNumber, 2000);
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

  const collection = collectionNames[Math.floor(Math.random() * Math.floor(3))];

  message = JSON.stringify(message);
  if (channel) {
    channel.assertExchange(exchange, "direct", { durable: true });
    console.log("publish msg and collection:", message, collection);
    channel.publish(exchange, collection, Buffer.from(message), {
      persistent: true
    });
  } else {
    mqCon
      .createChannel()
      .then(ch => {
        channel = ch;
        channel.assertExchange(exchange, "direct", { durable: true });
        console.log("publish msg and collection:", message);
        channel.publish(exchange, collection, Buffer.from(message), {
          persistent: true
        });
      })
      .catch(err => {
        console.log("err:", err);
      });
  }
}
