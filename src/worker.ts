import { Worker } from "bullmq";
import { QdrantVectorStore } from "@langchain/qdrant";
import { OpenAIEmbeddings } from "@langchain/openai";
import { PDFLoader } from "@langchain/community/document_loaders/fs/pdf";
import dotenv from "dotenv";

dotenv.config();

// Worker listening to "file-upload-queue"
const worker = new Worker(
  "file-upload-queue",
  async (job) => {
    try {
      console.log(`Processing job: ${JSON.stringify(job.data)}`);
      const data =
        typeof job.data === "string" ? JSON.parse(job.data) : job.data;

      const loader = new PDFLoader(data.path);
      const docs = await loader.load();

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
      console.log(`✅ Successfully added documents to vector store`);
    } catch (error) {
      console.error("❌ Error processing job:", error);
    }
  },
  {
    concurrency: 10,
    connection: { host: "localhost", port: 6379 },
  }
);
