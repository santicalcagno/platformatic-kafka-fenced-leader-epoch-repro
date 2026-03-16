import { Consumer } from "@platformatic/kafka";

const BROKERS = ["kafka1:9092", "kafka2:9092", "kafka3:9092"];
const TOPIC = "test-topic-compacted";
const GROUP_ID = `bare-consumer-test-${crypto.randomUUID()}`;

const consumer = new Consumer({
  clientId: "bare-consumer",
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
      console.log(`${new Date().toISOString()} — ${count} messages consumed`);
    }
  }
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
        `    └ ${proto.id ?? proto.message} (code ${proto.code}, canRetry ${proto.canRetry})`
      );
    }
  }

  console.error("\nFull error:");
  console.dir(err, { depth: 5 });

  process.exit(1);
});
