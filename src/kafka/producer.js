const { buildKafka } = require('./admin'); 
const logger = require('../utils/logger');

let producer;

async function initializeProducer() {
  if (producer) return;

  const kafka = buildKafka();
  producer = kafka.producer();

  await producer.connect();
  logger.info('✅ KafkaJS Producer connected.');
}

async function sendKafkaMessage(topic, message, key = null) {
  if (!producer) throw new Error('KafkaJS Producer not initialized.');
  await producer.send({
    topic,
    messages: [{ key: key || undefined, value: JSON.stringify(message) }],
  });
  logger.info(`📤 KafkaJS -> topic=${topic}`);
}

async function disconnectProducer() {
  if (!producer) return;
  await producer.disconnect();
  logger.info('KafkaJS Producer disconnected.');
}

module.exports = {
  initializeProducer,
  sendKafkaMessage,
  disconnectProducer,
};
