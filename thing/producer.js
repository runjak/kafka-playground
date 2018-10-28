const kafka = require('kafka-node');

const client = new kafka.KafkaClient({ kafkaHost: 'localhost:32770' });

const topics = ['foo', 'bar', 'baz'].map((topic) => ({ topic, partitions: 1, replicationFactor: 1 }));

function createTopics() {
  return new Promise((resolve, reject) => {
    client.createTopics(
      topics,
      (err, result) => {
        err ? reject(err) : resolve(result);
      },
    );
  });
}

async function registerProducer() {
  return new Promise((resolve, reject) => {
    const producer = new kafka.Producer(client);

    if (producer.ready) {
      resolve(producer);
    }

    producer.on('ready', () => (resolve(producer)));
    producer.on('error', reject);
  });
}

client.on('ready', async () => {
  console.log('client ready, creating topics…');

  await createTopics();

  console.log('topics ready, registering producer');

  const producer = await registerProducer();

  producer.send(
    topics.map(({ topic }) => ({
      topic,
      messages: [
        `Hi on topic ${topic}`,
        'Kragen!!!',
      ],
    })),
    (err, data) => {
      console.log('producer.send:', err, data);
    },
  );
});
