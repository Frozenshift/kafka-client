import { Kafka } from "kafkajs";
import { retry } from "./utils/retry.js";

export class KafkaClient {
    constructor(logger, config) {
        this.logger = logger;

        const brokers = config.kafka?.brokers || config.brokers;
        const brokerList = Array.isArray(brokers) ? brokers : [brokers].filter(Boolean);
        this.groupId = config.kafka?.groupId || config.groupId;

        this.kafka = new Kafka({
            clientId: config.kafka?.clientId || config.clientId || "trashbuster-client",
            brokers: brokerList,
            logLevel: 1,
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
            minBytes: 64,
            maxBytes: 512 * 1024,
            maxBytesPerPartition: 100,
            maxInFlightRequests: 1,
            maxWaitTimeInMs: 1000,
            rebalanceTimeout: 60000,
        });

        this.isPolling = false;
        this.pollConsumer = null;
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

    async subscribeAndRun(topic, handler, {
        fromBeginning = false,
        autoCommit = false,
        commitBatchSize = 1,
        commitIntervalMs = 0
    } = {}) {
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
                    await this.consumer.commitOffsets([{
                        topic,
                        partition,
                        offset: String(BigInt(message.offset) + 1n)
                    }]);
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

    async subscribeAndPoll(topic, {
        fromBeginning = false,
        pollTimeout = 5000,
        maxMessages = 1,
        maxBytes = 1024 * 1024,
        autoCommit = false
    } = {}) {

        if (!this.pollConsumer) {
            this.pollConsumer = this.kafka.consumer({
                groupId: `${this.groupId}-poll-${Date.now()}`, // Уникальный ID
                sessionTimeout: 30000,
                heartbeatInterval: 3000,
                maxBytesPerPartition: maxBytes,
                maxWaitTimeInMs: pollTimeout,
            });
            await this.pollConsumer.connect();
            await this.pollConsumer.subscribe({ topic, fromBeginning });
        }

        let isRunning = false;
        let currentResolver = null;

        const runConsumer = async () => {
            if (isRunning) return;
            isRunning = true;

            await this.pollConsumer.run({
                autoCommit: false,
                eachBatchAutoResolve: false,
                eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning: batchIsRunning }) => {
                    const messages = [];

                    for (const message of batch.messages) {
                        if (!batchIsRunning() || this.isShuttingDown) {
                            break;
                        }

                        let payload;
                        try {
                            payload = JSON.parse(message.value.toString());
                        } catch (error) {
                            resolveOffset(message.offset);
                            continue;
                        }

                        messages.push({
                            topic: batch.topic,
                            partition: batch.partition,
                            offset: message.offset,
                            key: message.key?.toString(),
                            value: payload,
                            raw: message.value.toString(),
                            timestamp: message.timestamp,
                            headers: message.headers,

                            commit: async () => {
                                const nextOffset = String(BigInt(message.offset) + 1n);
                                await this.pollConsumer.commitOffsets([{
                                    topic: batch.topic,
                                    partition: batch.partition,
                                    offset: nextOffset
                                }]);
                            }
                        });

                        resolveOffset(message.offset);
                        await heartbeat();
                    }

                    if (currentResolver) {
                        currentResolver(messages.slice(0, maxMessages));
                        currentResolver = null;
                    }
                }
            });
        };

        runConsumer().catch(err => {
            this.logger.error({ err }, "Poll consumer failed");
            isRunning = false;
        });

        const pollOne = async (timeout = pollTimeout) => {
            return new Promise((resolve, reject) => {
                const timeoutId = setTimeout(() => {
                    currentResolver = null;
                    resolve([]); // Таймаут - возвращаем пустой массив
                }, timeout);

                currentResolver = (messages) => {
                    clearTimeout(timeoutId);
                    resolve(messages);
                };
            }).then(messages => messages[0] || null);
        };

        const pollBatch = async (count = maxMessages, timeout = pollTimeout) => {
            return new Promise((resolve, reject) => {
                const timeoutId = setTimeout(() => {
                    currentResolver = null;
                    resolve([]); // Таймаут
                }, timeout);

                currentResolver = (messages) => {
                    clearTimeout(timeoutId);
                    resolve(messages.slice(0, count));
                };
            });
        };

        return {
            pollOne,
            pollBatch,

            stop: async () => {
                if (this.pollConsumer) {
                    await this.pollConsumer.stop();
                    isRunning = false;
                }
            },

            pause: () => {
                if (this.pollConsumer) {
                    this.pollConsumer.pause(this.pollConsumer.assignments());
                }
            },

            resume: () => {
                if (this.pollConsumer) {
                    this.pollConsumer.resume(this.pollConsumer.assignments());
                }
            },

            async *pollGenerator(timeout = pollTimeout) {
                while (!this.isShuttingDown && isRunning) {
                    const message = await pollOne(timeout);
                    if (message) {
                        yield message;
                    } else {
                        yield null;
                        await new Promise(r => setTimeout(r, 100)); // Ждём немного
                    }
                }
            }
        };
    }

    async shutdown() {
        this.isShuttingDown = true;

        await Promise.allSettled([
            this.producer.disconnect(),
            this.consumer.stop(),
            this.consumer.disconnect(),
            this.pollConsumer?.stop(),
            this.pollConsumer?.disconnect(),
        ]);

        this.logger.info("Kafka shutdown complete");
    }
}