import { Kafka, logLevel } from "kafkajs";
import { retry } from "./utils/retry.js";

export class KafkaClient {
    constructor(logger, config) {
        if (!config){
            console.error("KafkaClient config missing");
            return;
        }

        this.logger = logger;
        this.brokers = config.brokers; // строка
        this.groupId = config.groupId ;

        this.kafka = new Kafka({
            clientId: config.clientId,
            brokers: [this.brokers],
            logLevel: logLevel.WARN,
            retry: {
                initialRetryTime: 300,
                retries: 12,
                maxRetryTime: 30000,
            },
        });

        this.producer = this.kafka.producer({
            allowAutoTopicCreation: true,
            idempotence: true,
        });

        this.consumer = this.kafka.consumer({
            groupId: this.groupId,
            sessionTimeout: 30000,
            heartbeatInterval: 3000,
            maxBytesPerPartition: 2 * 1024 * 1024,
            // дополнительные настройки латентности
            fetchMinBytes: 1,
            fetchMaxWaitMs: 50,
        });

        this.isShuttingDown = false;
        this.setupEvents();
    }

    setupEvents() {
        this.producer.on("producer.connect", () => this.logger.info("Kafka producer connected"));
        this.consumer.on("consumer.connect", () => this.logger.info("Kafka consumer connected"));
        this.consumer.on("consumer.rebalancing", () => this.logger.info("Kafka consumer rebalancing..."));
        this.consumer.on("consumer.crash", (e) => this.logger.error({ err: e }, "Kafka consumer crashed"));
    }

    async connect() {
        await retry(() => this.producer.connect(), 10, 500).catch((err) => {
            this.logger.error({ err }, "Producer connect failed");
            throw err;
        });

        await retry(() => this.consumer.connect(), 10, 500).catch((err) => {
            this.logger.error({ err }, "Consumer connect failed");
            throw err;
        });

        this.logger.info("Kafka fully connected (producer + consumer)");
    }

    async publish(topic, messages) {
        try {
            await this.producer.send({ topic, messages });
        } catch (err) {
            this.logger.error({ err, topic, messages }, "producer.send failed");
            throw err;
        }
    }

    async subscribeAndRun(topic, handler, { fromBeginning = false, autoCommit = false, commitBatchSize = 1, commitIntervalMs = 0 } = {}) {
        await this.consumer.subscribe({ topic, fromBeginning });

        let pendingOffsets = new Map();
        let lastCommitTime = Date.now();

        const commitIfNeeded = async () => {
            if (pendingOffsets.size === 0) return;
            if (commitBatchSize > 1 && pendingOffsets.size < commitBatchSize) return;
            if (commitIntervalMs > 0 && Date.now() - lastCommitTime < commitIntervalMs) return;

            const offsets = [];
            for (const [key, offset] of pendingOffsets.entries()) {
                const [t, p] = key.split(":");
                offsets.push({ topic: t, partition: Number(p), offset });
            }

            pendingOffsets.clear();
            lastCommitTime = Date.now();

            await this.consumer.commitOffsets(offsets).catch((err) => {
                this.logger.error({ err, offsets }, "Commit failed");
            });
        };

        await this.consumer.run({
            autoCommit,
            eachMessage: async ({ topic, partition, message }) => {
                if (this.isShuttingDown) return;
                if (!message.value) return;

                let payload;
                try {
                    payload = JSON.parse(message.value.toString());
                } catch {
                    await this.consumer.commitOffsets([{ topic, partition, offset: String(BigInt(message.offset) + 1n) }]);
                    return;
                }

                try {
                    await handler(payload, { topic, partition, message });

                    if (!autoCommit) {
                        const nextOffset = String(BigInt(message.offset) + 1n);
                        pendingOffsets.set(`${topic}:${partition}`, nextOffset);
                        await commitIfNeeded();
                    }
                } catch (err) {
                    this.logger.error({ err, payload }, `Handler failed: ${topic}`);
                }
            },
        });
    }

    async shutdown() {
        this.isShuttingDown = true;

        await Promise.allSettled([
            this.producer.disconnect(),
            this.consumer.stop(),
            this.consumer.disconnect(),
        ]);

        this.logger.info("Kafka shutdown complete");
    }
}

