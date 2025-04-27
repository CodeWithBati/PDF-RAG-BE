import express from "express";
import { upload } from "../utils/multer";
import { fileUploadQueue } from "../queues/fileQueue";
import { getVectorStore } from "../utils/vectorStore";
import { openaiClient } from "../utils/openai";

const router = express.Router();

// Hello route
router.get("/", (req, res) => {
  res.json({ message: "Hello from API" });
});

// Upload PDF
router.post("/upload/pdf", upload.single("pdf"), async (req, res) => {
  try {
    const { originalname, destination, path } = req.file!;
    await fileUploadQueue.add("file-ready", {
      filename: originalname,
      destination,
      path,
    });
    res.json({ message: "PDF uploaded successfully" });
  } catch (error) {
    console.error("Error uploading PDF:", error);
    res.status(500).json({ error: "Failed to upload PDF" });
  }
});

// Chat API
router.get("/chat", async (req, res) => {
  const userQuery = req.query.message as string;

  try {
    const vectorStore = await getVectorStore();
    const retriever = vectorStore.asRetriever({ k: 2 });
    const documents = await retriever.invoke(userQuery);

    const systemPrompt = `
      You are a helpful AI Assistant who answers user queries based on the provided context from the PDF file.
      Context:
      ${JSON.stringify(documents)}
    `;

    const response = await openaiClient.chat.completions.create({
      model: "gpt-3.5-turbo",
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userQuery },
      ],
    });

    res.json({
      message: response.choices[0]?.message?.content ?? "No response",
      docs: documents,
    });
  } catch (error) {
    console.error("Error during chat processing:", error);
    res.status(500).json({ error: "Failed to process chat request" });
  }
});

export default router;
