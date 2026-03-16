import { Admin, Producer } from "@platformatic/kafka";

const BROKERS = ["kafka1:9092", "kafka2:9092", "kafka3:9092"];
const TOPIC = "test-topic-compacted";
const TOTAL_MESSAGES = 1000000; // You might want to increase this to trigger the error mid-consumption
const BATCH_SIZE = 200;

async function createTopic() {
  console.log("Creating topic with Admin client...");
  const admin = new Admin({
    clientId: "seed-admin",
    bootstrapBrokers: BROKERS,
  });

  try {
    const existing = await admin.listTopics();
    if (existing.includes(TOPIC)) {
      console.log(`Topic "${TOPIC}" already exists, deleting...`);
      await admin.deleteTopics({ topics: [TOPIC] });
      await new Promise((r) => setTimeout(r, 3000));
    }

    await admin.createTopics({
      topics: [TOPIC],
      partitions: 6,
      replicas: 3,
      configs: [
        { name: "cleanup.policy", value: "compact" },
        { name: "min.insync.replicas", value: "2" },
        { name: "unclean.leader.election.enable", value: "true" },
        { name: "min.compaction.lag.ms", value: "0" },
      ],
    });

    console.log(`Topic "${TOPIC}" created (6 partitions, RF=3, compacted)`);
  } finally {
    await admin.close();
  }
}

async function sendWithRetry(producer, messages, retries = 5) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      await producer.send({ messages });
      return;
    } catch (err) {
      if (attempt === retries) throw err;
      console.warn(`  Produce failed (attempt ${attempt}/${retries}): ${err.message}`);
      await new Promise((r) => setTimeout(r, attempt * 1000));
    }
  }
}

async function seedMessages() {
  console.log("Starting producer...");
  const producer = new Producer({
    clientId: "seed-producer",
    bootstrapBrokers: BROKERS,
    acks: 1,
  });

  let sent = 0;
  let batch = [];

  console.log(`Seeding ${TOTAL_MESSAGES} messages...`);

  for (let i = 0; i < TOTAL_MESSAGES; i++) {
    const key = String(i % 10000); // 10k unique keys for compaction

    batch.push({
      topic: TOPIC,
      key: Buffer.from(key),
      value: Buffer.from(JSON.stringify({ id: i, key, ts: Date.now() })),
    });

    if (batch.length >= BATCH_SIZE) {
      await sendWithRetry(producer, batch);
      sent += batch.length;
      batch = [];

      if (sent % 10000 === 0) {
        console.log(`  ${sent}/${TOTAL_MESSAGES} sent`);
      }
    }
  }

  if (batch.length > 0) {
    await sendWithRetry(producer, batch);
    sent += batch.length;
  }

  console.log(`Done. ${sent} messages sent.`);
  await producer.close();
}

async function main() {
  await createTopic();
  await seedMessages();
}

main().catch((err) => {
  console.error("Seed failed:", err);
  process.exit(1);
});
