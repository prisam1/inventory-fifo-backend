const { Kafka, logLevel } = require('kafkajs');
const fs = require('fs');
const config = require('../config');
const logger = require('../utils/logger');

function buildKafka() {
  const ssl = {};
  if (config.kafka.caCertPath) {
    ssl.ca = [fs.readFileSync(config.kafka.caCertPath, 'utf-8')];
  }
  ssl.rejectUnauthorized = config.kafka.sslRejectUnauthorized;

  if (config.kafka.useMtls) {
    ssl.cert = fs.readFileSync(config.kafka.clientCertPath, 'utf-8');
    ssl.key = fs.readFileSync(config.kafka.clientKeyPath, 'utf-8');
  }

  const sasl = config.kafka.useMtls
    ? undefined
    : (config.kafka.username && config.kafka.password
        ? { mechanism: config.kafka.saslMechanism || 'plain', username: config.kafka.username, password: config.kafka.password }
        : undefined);

  return new Kafka({
    clientId: 'inventory-admin',
    brokers: config.kafka.brokers,
    ssl,
    sasl,
    logLevel: logLevel.INFO,
  });
}

async function ensureTopics() {
  const kafka = buildKafka();
  const admin = kafka.admin();
  await admin.connect();

  const topicsToEnsure = [
    { topic: config.kafka.topics.inventoryUpdates, numPartitions: 1, replicationFactor: 3 },
    { topic: config.kafka.topics.orderEvents, numPartitions: 1, replicationFactor: 3 },
  ];

  const existing = await admin.listTopics();
  const toCreate = topicsToEnsure.filter(t => !existing.includes(t.topic));

  if (toCreate.length) {
    await admin.createTopics({ topics: toCreate });
    logger.info(`[admin] Created topics: ${toCreate.map(t => t.topic).join(', ')}`);
  } else {
    logger.info('[admin] All topics already exist.');
  }

  // force metadata refresh
  await admin.fetchTopicMetadata({ topics: topicsToEnsure.map(t => t.topic) });

  await admin.disconnect();
}

module.exports = { ensureTopics, buildKafka };
