// // src/kafka/consumer.js
// const { buildKafka } = require('./admin');
// const config = require('../config');
// const logger = require('../utils/logger');

// let consumer;

// async function initializeConsumer(handleOrderEventMessage) {
//   if (consumer) return;

//   const kafka = buildKafka();
//   consumer = kafka.consumer({ groupId: config.kafka.consumerGroupId });

//   await consumer.connect();
//   logger.info('✅ KafkaJS Consumer connected.');

//   // subscribe to the topic you produce to
//   await consumer.subscribe({ topic: config.kafka.topics.inventoryUpdates, fromBeginning: false });
//   // If you also want order-events:
//   // await consumer.subscribe({ topic: config.kafka.topics.orderEvents, fromBeginning: false });

//   await consumer.run({
//     eachMessage: async ({ topic, partition, message }) => {
//       try {
//         await handleOrderEventMessage(topic, message);
//       } catch (err) {
//         logger.error('[consumer] handler error:', err);
//       }
//     },
//   });

//   logger.info(`✅ KafkaJS Consumer subscribed to ${config.kafka.topics.inventoryUpdates}`);
// }

// async function disconnectConsumer() {
//   if (!consumer) return;
//   await consumer.disconnect();
//   logger.info('KafkaJS Consumer disconnected.');
// }

// module.exports = {
//   initializeConsumer,
//   disconnectConsumer,
// };

// src/kafka/consumer.js
const { buildKafka } = require('./admin');
const config = require('../config');
const logger = require('../utils/logger');

let consumer;

async function initializeConsumer(handleInventoryEventMessage) {
  if (consumer) return;

  const kafka = buildKafka();
  consumer = kafka.consumer({ groupId: config.kafka.consumerGroupId });

  await consumer.connect();
  logger.info('✅ KafkaJS Consumer connected.');

  // Subscribe to the SAME topic you produce to
  await consumer.subscribe({ topic: config.kafka.topics.inventoryUpdates, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        await handleInventoryEventMessage(topic, message); // <--- pass both
      } catch (err) {
        logger.error('[consumer] handler error:', err);
      }
    },
  });

  logger.info(`✅ KafkaJS Consumer subscribed to ${config.kafka.topics.inventoryUpdates}`);
}

async function disconnectConsumer() {
  if (!consumer) return;
  await consumer.disconnect();
  logger.info('KafkaJS Consumer disconnected.');
}

module.exports = {
  initializeConsumer,
  disconnectConsumer,
};
