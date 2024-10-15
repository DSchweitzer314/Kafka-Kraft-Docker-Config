const { Kafka } = require('kafkajs');

// Create a Kafka instance
const kafka = new Kafka({
  clientId: 'producer1',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

const run = async () => {
  // Connect the producer
  await producer.connect();
  console.log('Producer connected');

  try {
    setInterval(async () => {
      // Generate random data
      const message = {
        data: `I am producer #${Math.floor(Math.random() * 100 + 1)}`,
      };

      // Send the message to the topic
      await producer.send({
        topic: 'topic1',
        messages: [{ value: JSON.stringify(message) }],
      });

      console.log(`Sent: ${JSON.stringify(message)}`);
    }, 500);
  } catch (error) {
    console.error('Error sending message:', error);
  }
};

run().catch(console.error);
