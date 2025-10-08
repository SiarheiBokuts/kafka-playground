import express, { Express } from "express";
import dotenv from "dotenv";
import { runKafka } from "./kafka";

dotenv.config();

const app: Express = express();
const port = process.env.PORT || 3000;

app.listen(port, () => {
  console.log(`⚡️[server]: Server is running at https://localhost:${port}`);
});

runKafka().catch(console.error);
