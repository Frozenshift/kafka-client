# @frozenshift/kafka-client

Боевой Kafka/Redpanda клиент для Node.js 
Враппер над https://www.npmjs.com/package/kafkajs
с ручным управлением оффсетами, низкой латентностью, graceful shutdown и 100% защитой от потери сообщений.

Идеально для Telegram-ботов, микросервисов и любых высоконагруженных приложений.

## Особенности

- Ручные коммиты с батчингом (по количеству или интервалу)
- Exactly-once семантика (`idempotence: true`)
- Оптимизирован под низкую латентность
- Graceful shutdown без потери сообщений
- Авто-ретри подключения
- Полная совместимость с Redpanda и Apache Kafka

## Установка

```bash
npm install @frozenshift/kafka-client
```

## Как этим пользоваться 

```js
import { KafkaClient } from "@frozenshift/kafka-client";

const logger = console; // или передать экземпляр Pino

const config = {
    kafka: {
        brokers: "127.0.0.1:9092",     
        groupId: "test",
        clientId: "test-client"
    }
};

const kafka = new KafkaClient(logger, config);
await kafka.connect();

// шлём сообщение
await kafka.publish("my-topic", [
    { key: "123", value: JSON.stringify({ hello: "world" }) }
]);

// ловим сообщения
await kafka.subscribeAndRun("my-topic", async (payload) => {
    console.log("Пришло:", payload);
});
```