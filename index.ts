import express, { Express, Request, Response } from "express";
import dotenv from "dotenv";
import { kafkaService, USER_EVENTS_TOPIC } from "./src/kafka";

dotenv.config();

const app: Express = express();
const port = process.env.PORT || 3000;

// JSON middleware для body
app.use(express.json());

// Роут для получения событий с фронта
app.post("/events", async (req: Request, res: Response) => {
  try {
    const event = req.body;

    if (!event) {
      return res.status(400).json({ message: "Event body is required" });
    }

    // Отправляем событие в Kafka
    await kafkaService.sendEvent(USER_EVENTS_TOPIC, event);

    return res.status(200).json({ message: "Event sent to Kafka" });
  } catch (err) {
    console.error("Failed to send event to Kafka:", err);
    return res.status(500).json({ message: "Internal Server Error" });
  }
});

app.listen(port, async () => {
  await kafkaService.init();
  await kafkaService.startConsumer(
    USER_EVENTS_TOPIC,
    async ({ topic, partition, value }) => {
      console.log(
        `consumer works on Topic: ${topic} | Partition: ${partition} | Message: ${value}`,
      );

      const event = JSON.parse(value);
      console.log("Received event:", event);
    },
  );

  console.log(`⚡️[server]: Server is running at https://localhost:${port}`);
});

// kafkaService.startDebug().catch(console.error);
