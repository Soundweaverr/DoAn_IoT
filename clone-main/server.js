const express = require('express');
const path = require('path');
const WebSocket = require('ws');

const config = require('./config');
const { 
    runMqtt,
    data
} = require('./index');

const app = express();
const port = config.serverSystem.port || 3000;
const server = app.listen(port, () => console.log("listening on port http://localhost:" + port))
const wss = new WebSocket.Server({server});
var dataOld = [];

app.use('/assets', express.static(path.join(__dirname, 'assets')));

runMqtt();

function sendToClients() {
    const jsonData = JSON.stringify(data);
    let logger = '';
    
    if (jsonData && jsonData.length > 0 && (JSON.stringify(dataOld) !== jsonData)) {
        wss.clients.forEach(client => {
            if (client.readyState === WebSocket.OPEN) {
                client.send(jsonData, (err) => {
                    if (!err) {
                        let time = new Date()
                        console.log(`Gửi dữ liệu xuống client thành công vào lúc ${time.toLocaleString()}`);
                        console.log(jsonData);
                    } else {
                        console.log('Gửi dữ liệu xuống client thất bại');
                    }
                })         
            }
        })
        logger = "Dữ liệu được làm mới."
        dataOld = JSON.parse(jsonData)
    } else {
        logger = "Dữ liệu bị trùng lặp. Không cần thiết gửi đi tới client."
    }
    console.log(logger);
}

setInterval(() => {
    let time = new Date()
    console.log(`Kiểm tra server vào lúc ${time.toLocaleString()}`);
    sendToClients()
}, 60000)

app.get('/', (req,res) => {
    res.sendFile(path.join(__dirname, '/index.html'));
})
