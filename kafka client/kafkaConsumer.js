const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'consumer1',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'test-group' });

const run = async () => {
  // Connect the consumer
  await consumer.connect();
  console.log('Consumer connected');

  // Subscribe to the topic
  await consumer.subscribe({ topic: 'topic1', fromBeginning: true });

  // Consume messages
  await consumer.run({
    eachMessage: async ({ message }) => {
      const convertValue = message.value.toString();
      const parseMessage = JSON.parse(convertValue);
      console.log('I consumed: ', convertValue);
    },
  });
};

run().catch(console.error);
