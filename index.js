const WebSocket = require('ws');
const express = require('express');
const http = require('http');
const cors = require('cors');
const { consumeMessages } = require('./src/consumer');

const PORT = 8080;

const app = express();
app.use(cors());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

function broadcast(data) {
  const message = typeof data === 'string' ? data : JSON.stringify(data); // Serializar si no es string
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message); // Enviar mensaje como string
    }
  });
}

wss.on('connection', (ws) => {
  console.log('Cliente conectado');

  ws.on('message', (message) => {
    console.log('Mensaje recibido:', message);
  });

  // Notificar que un nuevo cliente se conectó
  broadcast({ event: 'new_connection', message: 'Un nuevo cliente se ha conectado.' });
});

// Iniciar el consumidor de RabbitMQ una sola vez
consumeMessages((msg) => {
  const payload = {
    event: 'rabbitmq_message',
    data: msg,
  };
  broadcast(payload); // Enviar mensaje como JSON
});

server.listen(PORT, () => {
  console.log(`Servidor HTTP y WebSocket escuchando en el puerto ${PORT}`);
});
