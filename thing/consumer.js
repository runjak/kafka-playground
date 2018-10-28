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

function registerConsumer(withMessage, withError) {
  const consumer = new kafka.Consumer(
    client,
    topics.map(({ topic }) => ({ topic, partition: 0 })),
  );

  consumer.on('message', withMessage);
  consumer.on('error', withError);

  return consumer;
}

client.on('ready', async () => {
  console.log('client ready, creating topics…');

  await createTopics();

  console.log('topics ready, registering consumer');

  const consumer = registerConsumer(
    (message) => {
      console.log('Consumer got a message:', message);
    },
    (error) => {
      console.log('Consumer got an error:', error);
    },
  );
});
