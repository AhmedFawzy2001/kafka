const express = require('express');
const { Kafka } = require('kafkajs');

const app = express();
const PORT = 5000;

// Initialize Kafka
const kafka = new Kafka({
  clientId: 'my-consumer',
  brokers: ['localhost:9092']
});

// Create a Kafka consumer
const consumer = kafka.consumer({ groupId: 'my-group' });

// Middleware to parse JSON bodies
app.use(express.json());

// Function to consume messages and print them to console
const runConsumer = async (topic) => {
  try {
    // Connect the consumer to Kafka broker
    await consumer.connect();

    // Subscribe to the Kafka topic
    await consumer.subscribe({ topic });

    // Run the consumer to continuously fetch and process messages
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log({
          topic,
          partition,
          offset: message.offset,
          value: message.value.toString()
        });
      },
    });
  } catch (error) {
    console.error('Error consuming messages:', error);
  }
};

// Endpoint to start consuming messages from a specific topic
app.get('/consume-messages', (req, res) => {
  const { topic } = req.query;
  if (!topic) {
    return res.status(400).json({ error: 'Topic parameter is missing' });
  }

  // Start consuming messages from the specified topic
  runConsumer(topic);
  res.status(200).json({ message: `Started consuming messages from topic ${topic}` });
});

// Start the Express server
app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});
