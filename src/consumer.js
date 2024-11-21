const amqp = require('amqplib');
const WebSocket = require('ws');

// Configuración de RabbitMQ
const RABBITMQ_URL = 'amqp://44.219.1.248'; // Cambia a la URL de RabbitMQ
const QUEUES = [
  { name: 'temperature_data', durable: false },
  { name: 'datos', durable: true },
];

// Configuración del servidor WebSocket
const WSS_URI = 'ws://localhost:8080'; // URL del servidor WebSocket existente

async function consumeMessages(callback) {
  try {
    // Conexión al servidor RabbitMQ
    const connection = await amqp.connect(RABBITMQ_URL);
    console.log('Conectado a RabbitMQ');

    const channel = await connection.createChannel();
    console.log('Canal creado');

    // Conexión persistente al servidor WebSocket
    const websocket = new WebSocket(WSS_URI);

    websocket.on('open', async () => {
      console.log(`Conectado al servidor WebSocket: ${WSS_URI}`);

      // Declarar y consumir mensajes de las colas
      for (const queue of QUEUES) {
        await channel.assertQueue(queue.name, { durable: queue.durable });
        console.log(`Suscrito a la cola '${queue.name}'`);

        channel.consume(queue.name, (msg) => {
          if (msg !== null) {
            const message = msg.content.toString(); // Convertir Buffer a texto
            console.log(`Mensaje recibido de la cola '${queue.name}': ${message}`);

            // Construir el payload para enviar al WebSocket
            const payload = JSON.stringify({
              queue: queue.name,
              message: message,
            });

            // Enviar el mensaje al servidor WebSocket
            if (websocket.readyState === WebSocket.OPEN) {
              websocket.send(payload); // Enviar el mensaje como JSON
              console.log(`Mensaje enviado al servidor WebSocket: ${payload}`);
            } else {
              console.error('Conexión al servidor WebSocket cerrada. No se pudo enviar el mensaje.');
            }

            // Confirmar el mensaje como procesado
            channel.ack(msg);

            // Ejecutar el callback con el mensaje
            callback(`Cola: ${queue.name}, Mensaje: ${message}`);
          }
        });
      }
    });

    websocket.on('error', (error) => {
      console.error(`Error en la conexión WebSocket: ${error.message}`);
    });

    websocket.on('close', () => {
      console.log('Conexión con el servidor WebSocket cerrada.');
    });
  } catch (error) {
    console.error('Error al consumir mensajes de RabbitMQ:', error);
  }
}

module.exports = { consumeMessages };
