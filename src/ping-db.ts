import { MongoClient } from "mongodb";
import { config } from "dotenv";
config(); // διαβάζει .env

const client = new MongoClient(process.env.MONGO_URI!);

(async () => {
  await client.connect();
  const db = client.db(process.env.MONGO_DB || "aaql");
  console.log("✅ Connected to MongoDB database:", db.databaseName);
  await client.close();
})();
