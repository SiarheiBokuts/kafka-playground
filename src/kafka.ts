import { Kafka, Producer, Consumer, Admin } from "kafkajs";

const APP_GROUP_ID = "kafka-playground-app";
// Session timeout in ms — the time after which the broker considers the consumer dead.
// Decreased from the default 30000 to 6000 for faster consumer startup during local development.
const CONSUME_SESSION_TIMEOUT = 6000; // in ms
export const USER_EVENTS_TOPIC = "user-events";

class KafkaService {
  private static instance: KafkaService;
  private kafka: Kafka;
  private producer: Producer;
  private consumer: Consumer;
  private admin: Admin;

  private constructor() {
    this.kafka = new Kafka({
      clientId: "kafka-playground-app",
      brokers: ["localhost:9092"],
    });

    this.producer = this.kafka.producer();
    this.consumer = this.kafka.consumer({
      groupId: APP_GROUP_ID,
      sessionTimeout: CONSUME_SESSION_TIMEOUT,
    });
    this.admin = this.kafka.admin();
  }

  public static getInstance(): KafkaService {
    if (!KafkaService.instance) {
      KafkaService.instance = new KafkaService();
    }
    return KafkaService.instance;
  }

  private async ensureTopic(topic: string) {
    await this.admin.connect();
    const topics = await this.admin.listTopics();

    if (!topics.includes(topic)) {
      await this.admin.createTopics({
        topics: [{ topic, numPartitions: 3, replicationFactor: 1 }],
      });
      console.log(`Created topic: ${topic}`);
    } else {
      console.log(`Topic "${topic}" already exists`);
    }

    await this.admin.disconnect();
  }

  public async init(): Promise<void> {
    await this.ensureTopic(USER_EVENTS_TOPIC);
    await this.producer.connect();
    await this.consumer.connect();
  }

  public async startDebug(topic = USER_EVENTS_TOPIC) {
    await this.consumer.subscribe({ topic, fromBeginning: true });

    this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(
          `Topic: ${topic} | Partition: ${partition} | Message: ${message.value?.toString()}`
        );
      },
    });

    setInterval(async () => {
      const msg = `Hello Kafka! ${new Date().toISOString()}`;
      await this.producer.send({ topic, messages: [{ value: msg }] });
      console.log(`Sent: ${msg}`);
    }, 2000);
  }

  public async startConsumer(
    topic: string = USER_EVENTS_TOPIC,
    onMessage?: (msg: {
      topic: string;
      partition: number;
      value: string;
    }) => Promise<void> | void
  ) {
    try {
      await this.consumer.subscribe({ topic, fromBeginning: true });
      console.log(`Subscribed to topic: ${topic}`);

      await this.consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          try {
            const value = message.value?.toString() ?? "";
            if (onMessage) {
              await onMessage({ topic, partition, value });
            } else {
              console.log(
                `Topic: ${topic} | Partition: ${partition} | Message: ${value}`
              );
            }
          } catch (err) {
            console.error("Error handling Kafka message:", err);
          }
        },
      });
    } catch (err) {
      console.error(`Failed to start consumer for topic "${topic}":`, err);
      throw err;
    }
  }

  public async sendEvent(
    topic: string,
    event: Record<string, unknown>
  ): Promise<void> {
    try {
      await this.producer.send({
        topic,
        messages: [{ value: JSON.stringify(event) }],
      });
    } catch (error) {
      console.error(`Failed to send event to topic "${topic}":`, error);
      throw error;
    }
  }
}

export const kafkaService = KafkaService.getInstance();
