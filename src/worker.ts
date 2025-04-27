import { Worker } from "bullmq";
import { QdrantVectorStore } from "@langchain/qdrant";
import { OpenAIEmbeddings } from "@langchain/openai";
import { PDFLoader } from "@langchain/community/document_loaders/fs/pdf";
import { Client, Storage } from "appwrite";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";
import { dirname } from "path";
import fetch from "node-fetch"; // ✅ import fetch for HTTP requests
import dotenv from "dotenv";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Setup Appwrite Client
const appwriteClient = new Client();

appwriteClient
  .setEndpoint(process.env.APPWRITE_ENDPOINT!)
  .setProject(process.env.APPWRITE_PROJECT_ID!);

appwriteClient.headers = {
  "X-Appwrite-Key": process.env.APPWRITE_API_KEY!,
};

const storage = new Storage(appwriteClient);

// Worker setup
const worker = new Worker(
  "file-upload-queue",
  async (job) => {
    try {
      console.log(`Processing job: ${JSON.stringify(job.data)}`);
      const data =
        typeof job.data === "string" ? JSON.parse(job.data) : job.data;

      const fileId = data.fileId;
      if (!fileId) {
        throw new Error("No fileId found in job data");
      }

      // 1. Get the file download URL from Appwrite
      const downloadUrl = await storage.getFileDownload(
        process.env.APPWRITE_BUCKET_ID!,
        fileId
      );

      if (typeof downloadUrl !== "string") {
        throw new Error("Invalid download URL from Appwrite");
      }

      console.log(`📥 Fetching file from URL: ${downloadUrl}`);

      // 2. Fetch the real file content
      const response = await fetch(downloadUrl);

      if (!response.ok) {
        throw new Error(`Failed to fetch file: ${response.statusText}`);
      }

      const arrayBuffer = await response.arrayBuffer();
      const buffer = Buffer.from(arrayBuffer);

      const tempFilePath = path.join(__dirname, `${fileId}.pdf`);
      fs.writeFileSync(tempFilePath, buffer);

      console.log(`✅ File downloaded and saved at ${tempFilePath}`);

      // 3. Load the PDF using PDFLoader
      const loader = new PDFLoader(tempFilePath);
      const docs = await loader.load();

      console.log(`📚 Loaded ${docs.length} documents from PDF.`);

      // 4. Embedding and Vector Store
      const embeddings = new OpenAIEmbeddings({
        model: "text-embedding-3-small",
        apiKey: process.env.OPENAI_API_KEY!,
      });

      const vectorStore = await QdrantVectorStore.fromExistingCollection(
        embeddings,
        {
          url: "http://localhost:6333",
          collectionName: "pdf-test",
        }
      );

      await vectorStore.addDocuments(docs);
      console.log(`✅ Successfully added documents to vector store.`);

      // 5. Cleanup: delete the temp file
      fs.unlinkSync(tempFilePath);
      console.log(`🗑️ Temp file deleted: ${tempFilePath}`);
    } catch (error) {
      console.error("❌ Error processing job:", error);
    }
  },
  {
    concurrency: 10,
    connection: { host: "localhost", port: 6379 },
  }
);
