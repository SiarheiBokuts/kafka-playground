import express, { Express, Request, Response } from "express";
import dotenv from "dotenv";
import { kafkaService, USER_EVENTS_TOPIC } from "./src/kafka";
import { ApolloServer } from "@apollo/server";
import { expressMiddleware } from "@as-integrations/express5";
import { ApolloServerPluginDrainHttpServer } from "@apollo/server/plugin/drainHttpServer";
import http from "http";
import cors from "cors";

dotenv.config();

const app: Express = express();
const port = process.env.PORT || 3000;

// The GraphQL schema
const typeDefs = `#graphql
  type Query {
    hello: String
  }
`;

// A map of functions which return data for the schema.
const resolvers = {
  Query: {
    hello: () => "world",
  },
};

const httpServer = http.createServer(app);

// Set up Apollo Server
const server = new ApolloServer({
  typeDefs,
  resolvers,
  plugins: [ApolloServerPluginDrainHttpServer({ httpServer })],
});

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
        `consumer works on Topic: ${topic} | Partition: ${partition} | Message: ${value}`
      );

      const event = JSON.parse(value);
      console.log("Received event:", event);
    }
  );

  await server.start();

  app.use(cors(), express.json(), expressMiddleware(server));

  console.log(`⚡️[server]: Server is running at https://localhost:${port}`);
});

// kafkaService.startDebug().catch(console.error);
