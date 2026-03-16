import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const streamPath = join(
  __dirname,
  "node_modules",
  "@platformatic",
  "kafka",
  "dist",
  "clients",
  "consumer",
  "messages-stream.js"
);

const original = readFileSync(streamPath, "utf8");

const needle = "const requests = new Map();";
const patch = `// [PATCH] Sync #partitionsEpochs with current leader epochs from metadata
            for (const [topicName, topicMeta] of metadata.topics) {
                for (const [idx, p] of topicMeta.partitions.entries()) {
                    this.#partitionsEpochs.set(\`\${topicName}:\${idx}\`, p.leaderEpoch);
                }
            }
            const requests = new Map();`;

function restore() {
  writeFileSync(streamPath, original);
  console.log("\nRestored original messages-stream.js.");
}

if (!original.includes(needle) || original.includes("[PATCH]")) {
  console.error("Needle not found or already patched. Run npm install to restore.");
  process.exit(1);
}

writeFileSync(streamPath, original.replace(needle, patch));
console.log("Patched messages-stream.js: syncing #partitionsEpochs from metadata on each fetch.\n");

// Restore on any exit
process.on("exit", restore);

// Now import Consumer (picks up the patched file)
const { Consumer } = await import("@platformatic/kafka");

const BROKERS = ["kafka1:9092", "kafka2:9092", "kafka3:9092"];
const TOPIC = "test-topic-compacted";
const GROUP_ID = `patched-consumer-${crypto.randomUUID()}`;

const consumer = new Consumer({
  clientId: "patched-consumer",
  bootstrapBrokers: BROKERS,
  groupId: GROUP_ID,
  autocommit: false,
});

let count = 0;

async function run() {
  console.log(`Consumer group: ${GROUP_ID}`);
  console.log(`Consuming from: ${TOPIC}`);
  console.log("Waiting for messages...\n");

  const stream = await consumer.consume({ topics: [TOPIC], mode: "earliest" });

  for await (const message of stream) {
    count++;
    if (count % 5000 === 0) {
      console.log(
        `${new Date().toISOString()} — ${count} messages consumed`
      );
    }
  }

  console.log(`\nDone. Total: ${count} messages consumed.`);
}

process.on("SIGINT", async () => {
  console.log(`\nStopping. Total messages: ${count}`);
  await consumer.close();
  process.exit(0);
});

run().catch((err) => {
  console.error(`\nCrashed after ${count} messages.\n`);
  console.error("Fatal:", err.message);

  for (const inner of err.errors ?? []) {
    console.error(`  └ ${inner.message}`);
    for (const proto of inner.errors ?? []) {
      console.error(
        `    └ ${proto.apiId ?? proto.message} (code ${proto.apiCode}, canRetry ${proto.canRetry})`
      );
    }
  }

  console.error("\nFull error:");
  console.dir(err, { depth: 5 });
  process.exit(1);
});
