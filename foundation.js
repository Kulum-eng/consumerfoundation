import amqp from 'amqplib';
import fetch from 'node-fetch';

const RABBITMQ_URL = 'amqp://ale:ale123@ec2-54-167-194-141.compute-1.amazonaws.com:5672';
const NOTIFICATION_QUEUE = 'notificaciones';
const CLOUD_FUNCTION_URL = 'https://us-central1-tests-abe52.cloudfunctions.net/api/send-notification';

async function sendToCloudFunction(notification) {
  try {
    console.log('📤 Enviando notificación a Cloud Function:', notification);

    const response = await fetch(CLOUD_FUNCTION_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(notification)
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const result = await response.json();
    console.log('✅ Respuesta exitosa de Cloud Function:', {
      status: response.status,
      statusText: response.statusText,
      data: result
    });

    return result;
  } catch (error) {
    console.error('❌ Error al enviar a Cloud Function:', {
      message: error.message,
      notification: notification
    });
    throw error;
  }
}

async function startConsumer() {
  try {
    const conn = await amqp.connect(RABBITMQ_URL);
    const channel = await conn.createChannel();

    await channel.assertQueue(NOTIFICATION_QUEUE, { durable: true });
    console.log(`🔍 Escuchando cola "${NOTIFICATION_QUEUE}"...`);

    channel.consume(NOTIFICATION_QUEUE, async (msg) => {
      if (msg) {
        try {
          const notification = JSON.parse(msg.content.toString());
          console.log('📥 Notificación recibida de RabbitMQ:', notification);

          
          if (!notification.token) {
            console.error('❌ Notificación sin token:', notification);
            throw new Error('Token no encontrado en la notificación');
          }

        
          const cloudFunctionResponse = await sendToCloudFunction(notification);
          console.log('🔁 Procesamiento completado para notificación:', {
            rabbitMQId: msg.properties.messageId,
            cloudFunctionResponse: cloudFunctionResponse
          });

        } catch (parseError) {
          console.error('❌ Error al procesar mensaje:', {
            error: parseError.message,
            content: msg.content.toString()
          });
        } finally {
          channel.ack(msg);
        }
      }
    });

  } catch (error) {
    console.error('❌ Error en conexión RabbitMQ:', {
      message: error.message,
      stack: error.stack
    });
    console.log(' Reintentando conexión en 5 segundos...');
    setTimeout(startConsumer, 5000);
  }
}

process.on('SIGINT', async () => {
  console.log('\n🔴 Deteniendo consumer...');
  process.exit(0);
});

console.log(' Iniciando servicio de notificaciones...');
startConsumer();
