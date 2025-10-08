import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "kafka-playground-app",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "test-group" });

async function adminKafka() {
  const admin = kafka.admin();
  await admin.connect();

  const topics = await admin.listTopics();
  console.log("Existing topics:", topics);

  if (!topics.includes("test-topic")) {
    await admin.createTopics({
      topics: [
        {
          topic: "test-topic",
          numPartitions: 3,
          replicationFactor: 1,
        },
      ],
    });
  }

  await admin.disconnect();
}

async function runKafka() {
  await adminKafka();
  await producer.connect();
  await consumer.connect();

  //   setTimeout(async () => {
  await consumer.subscribe({ topic: "test-topic", fromBeginning: true });
  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(
        `Topic: ${topic} Received: ${message.value?.toString()} on partition ${partition}`
      );
    },
  });
  //   }, 5000);
  // consumer

  //   producer
  await producer.send({
    topic: "test-topic",
    messages: [{ value: "Hello Kafka KRaft!" }],
  });
}

export { runKafka };
