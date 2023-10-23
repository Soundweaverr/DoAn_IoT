const mqtt = require('mqtt');
const mysql = require('mysql2');

const config = require('./config');

const topic = config.serverMqtt.topic;
const clientId = `publish-${Math.floor(Math.random() * 1000)}`;

const options = {
  port: config.serverMqtt.port,
  clientId: clientId,
  username: config.serverMqtt.username,
  password: config.serverMqtt.password,
};

const client = mqtt.connect(`mqtt://${config.serverMqtt.broker}:${config.serverMqtt.port}`, options);

const runMqtt = () => {
  const mysqlConfig = {
    host: 'localhost', // Change this to your MySQL host
    user: 'root', // Change this to your MySQL username
    password: '123456', // Change this to your MySQL password
    database: 'agriculture_data', // Change this to your MySQL database name
  };

  const connection = mysql.createConnection(mysqlConfig);

  client.on('connect', () => {
    console.log('Đã kết nối tới máy chủ MQTT');
    client.subscribe(topic, (err) => {
      if (!err) {
        console.log(`Đã đăng ký nhận tin nhắn trên topic "${topic}"`);
      }
    });

    client.on('message', (topic, message) => {
      let time = new Date();
      console.log(`Nhận tin nhắn từ topic "${topic}" lúc ${time.toLocaleString()}:\n ${message.toString()}`);

      // Parse the message into an object
      const messageData = parseMessage(message.toString());

      // Insert the received data into MySQL
      const sql = 'INSERT INTO agricultureData (station_name, temp, feels_like, temp_min, temp_max, pressure, humidity, sea_level, grnd_level, speed, deg, gust, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';
      const values = [
        messageData.station_name,
        messageData.temp,
        messageData.feels_like,
        messageData.temp_min,
        messageData.temp_max,
        messageData.pressure,
        messageData.humidity,
        messageData.sea_level,
        messageData.grnd_level,
        messageData.speed,
        messageData.deg,
        messageData.gust,
        time,
      ];


      connection.query(sql, values, (err, result) => {
        if (err) {
          console.error(`Lỗi khi lưu dữ liệu vào MySQL: ${err}`);
        } else {
          console.log('Dữ liệu đã được lưu vào MySQL');
        }
      });
    });

    client.on('error', (err) => {
      console.error(`Lỗi kết nối: ${err}`);
    });
  });
};

// Function to parse the message into an object
function parseMessage(messageString) {
  const messageParts = messageString.split(', ');
  const messageData = {};

  messageParts.forEach((part) => {
    const keyValue = part.split(' = ');
    if (keyValue.length === 2) {
      const key = keyValue[0].trim();
      const value = keyValue[1].trim();
      messageData[key] = value;
    }
  });

  return messageData;
}

module.exports = {
  runMqtt,
};