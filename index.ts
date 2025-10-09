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

// === GraphQL Schema & Resolvers ===
const typeDefs = `#graphql
  type Query {
    hello: String
  }
`;

const resolvers = {
  Query: {
    hello: () => "world",
  },
};

// === HTTP Server for Apollo ===
const httpServer = http.createServer(app);

// === Apollo Server Setup ===
const server = new ApolloServer({
  typeDefs,
  resolvers,
  plugins: [ApolloServerPluginDrainHttpServer({ httpServer })],
});

// === Middleware ===
app.use(cors());
app.use(express.json());

// Debug middleware for incoming HTTP requests
app.use((req, _res, next) => {
  console.log(`[HTTP] ${req.method} ${req.url} Body:`, req.body);
  next();
});

// === REST Route to receive events from frontend ===
app.post("/events", async (req: Request, res: Response) => {
  const event = req.body;

  if (!event) {
    return res.status(400).json({ message: "Event body is required" });
  }

  try {
    await kafkaService.sendEvent(USER_EVENTS_TOPIC, event);
    console.log("[Kafka] Event sent:", event);
    return res.status(200).json({ message: "Event sent to Kafka" });
  } catch (err) {
    console.error("[Kafka] Failed to send event:", err);
    return res.status(500).json({ message: "Internal Server Error" });
  }
});

// === Server Initialization ===
async function startServer() {
  try {
    // Init Kafka
    await kafkaService.init();
    await kafkaService.startConsumer(
      USER_EVENTS_TOPIC,
      async ({ topic, partition, value }) => {
        console.log(
          `[Kafka] Topic: ${topic} | Partition: ${partition} | Message: ${value}`
        );
        try {
          const event = JSON.parse(value);
          console.log("[Kafka] Parsed event:", event);
        } catch (err) {
          console.error("[Kafka] Failed to parse event:", err);
        }
      }
    );

    // Start Apollo Server
    await server.start();
    app.use(expressMiddleware(server));

    // Start HTTP server
    httpServer.listen(port, () => {
      console.log(`⚡️ Server running at http://localhost:${port}`);
    });
  } catch (err) {
    console.error("Failed to start server:", err);
    process.exit(1);
  }
}

startServer();
