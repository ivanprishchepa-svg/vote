const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const redis = require('redis');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// Получаем полную строку подключения к Redis из переменной окружения
const redisUrl = process.env.REDIS_URL || 'redis://redis:6379';

// Создаём клиент-подписчик (для получения сообщений из канала 'results')
const subscriber = redis.createClient({ url: redisUrl });
subscriber.connect().then(() => {
    console.log('Подключены к Redis (subscriber)');
    subscriber.subscribe('results', (message) => {
        // message – JSON-строка с результатами
        const counts = JSON.parse(message);
        io.emit('update', counts);
    });
});

// Отдача статической страницы
app.get('/', (req, res) => {
    res.sendFile(__dirname + '/index.html');
});

// При подключении клиента отправляем начальные данные
io.on('connection', async (socket) => {
    // Создаём отдельный клиент для чтения последнего сохранённого результата
    const pubClient = redis.createClient({ url: redisUrl });
    await pubClient.connect();
    const latest = await pubClient.get('latest_results');
    if (latest) {
        socket.emit('update', JSON.parse(latest));
    }
    await pubClient.quit();
});

server.listen(8081, () => {
    console.log('Result server запущен на порту 8081');
});
