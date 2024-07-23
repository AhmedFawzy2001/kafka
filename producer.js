


const express = require('express');
const { Kafka } = require('kafkajs');
const bodyParser = require('body-parser'); 

const app = express();
const PORT = 3000;

// Initialize Kafka
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
});

// Create an admin client
const admin = kafka.admin();

// Create a Kafka producer
const producer = kafka.producer();

// Middleware to parse JSON bodies
app.use(express.json());

// Endpoint to create a Kafka topic
app.post('/create-topic', async (req, res) => {
  const { topic, numPartitions, replicationFactor } = req.body;

  try {
    // Connect to the Kafka cluster
    await admin.connect();

    // Create the topic
    await admin.createTopics({
      topics: [
        {
          topic,
          numPartitions,
          replicationFactor
        }
      ]
    });

    res.status(200).json({ message: `Topic ${topic} created successfully` });
  } catch (error) {
    console.error('Error creating topic:', error);
    res.status(500).json({ error: 'Error creating topic' });
  } finally {
    // Disconnect the admin client
    await admin.disconnect();
  }
});

// Endpoint to send a message to a Kafka topic
app.post('/send-message', async (req, res) => {
  const { topic, message } = req.body;

  try {
    // Connect the producer to Kafka broker
    await producer.connect();

    // Send message to the Kafka topic
    await producer.send({
      topic,
      messages: [
        { value: message }
      ]
    });

    res.status(200).json({ message: 'Message sent to Kafka topic' });
  } catch (error) {
    console.error('Error sending message:', error);
    res.status(500).json({ error: 'Error sending message to Kafka' });
  }
});

// Start the Express server
app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});
