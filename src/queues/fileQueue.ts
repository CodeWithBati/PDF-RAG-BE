import { Queue } from "bullmq";

export const fileUploadQueue = new Queue("file-upload-queue", {
  connection: { host: "localhost", port: 6379 },
});
