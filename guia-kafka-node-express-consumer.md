# Guía completa: REST API con Express + Kafka + Consumer en Node.js + Kafka UI

Esta guía muestra cómo implementar, de principio a fin, un ejemplo sencillo de integración con Apache Kafka usando Node.js.

El objetivo es construir:

- Un clúster local de Kafka usando Docker Compose.
- Una interfaz web para Kafka usando Kafka UI.
- Un productor implementado como API REST con Express.
- Un consumidor independiente en Node.js.
- Un tópico de Kafka llamado `messages`.
- Una prueba completa desde una petición HTTP hasta el consumo del mensaje.
- Una demostración de consumer groups y particiones.

---

## 1. Arquitectura del ejemplo

El flujo será el siguiente:

```text
HTTP Client
    |
    | POST /messages
    v
+------------------+
| Express Producer |
+------------------+
         |
         | Kafka event
         v
   +-------------+
   | Kafka Topic |
   |   messages  |
   +-------------+
         |
         v
+------------------+
| Kafka Consumer   |
+------------------+
```

Kafka y Kafka UI se ejecutarán dentro de Docker.

La aplicación Node.js se ejecutará directamente en la computadora local.

```text
┌──────────────────────────────┐
│ Computadora local            │
│                              │
│ Express Producer             │
│ Kafka Consumer               │
│          │                   │
│          │ localhost:9092    │
└──────────┼───────────────────┘
           │
           ▼
┌────────────────────────────────────────┐
│ Docker                                 │
│                                        │
│          Kafka                         │
│     ┌─────────────┐                    │
│     │ :9092       │ ← clientes externos│
│     │ :29092      │ ← Docker interno   │
│     └──────┬──────┘                    │
│            │                           │
│            │ kafka:29092               │
│            ▼                           │
│       ┌──────────┐                     │
│       │ Kafka UI │                     │
│       └──────────┘                     │
└────────────────────────────────────────┘
```

---

## 2. Requisitos

Antes de comenzar se necesita:

- Docker Desktop o Docker Engine.
- Docker Compose.
- Node.js 20 o superior.
- NPM.
- Postman, Insomnia o `curl` para probar la API.
- Un editor como VS Code, WebStorm o IntelliJ IDEA.

Para verificar Node.js:

```bash
node --version
```

Para verificar NPM:

```bash
npm --version
```

Para verificar Docker:

```bash
docker --version
```

Para verificar Docker Compose:

```bash
docker compose version
```

---

# 3. Crear el proyecto

Crear una carpeta para el proyecto:

```bash
mkdir kafka-node-demo
cd kafka-node-demo
```

La estructura final será:

```text
kafka-node-demo/
├── docker-compose.yml
├── package.json
├── .env
├── producer/
│   ├── kafka.js
│   └── server.js
└── consumer/
    ├── kafka.js
    └── consumer.js
```

Crear las carpetas:

```bash
mkdir producer
mkdir consumer
```

Inicializar el proyecto Node.js:

```bash
npm init -y
```

Instalar las dependencias:

```bash
npm install express kafkajs dotenv
```

---

# 4. Configurar `package.json`

Modificar `package.json` para que tenga el siguiente contenido:

```json
{
  "name": "kafka-node-demo",
  "version": "1.0.0",
  "type": "module",
  "scripts": {
    "producer": "node producer/server.js",
    "consumer": "node consumer/consumer.js"
  },
  "dependencies": {
    "dotenv": "^17.0.0",
    "express": "^5.0.0",
    "kafkajs": "^2.2.4"
  }
}
```

La propiedad:

```json
"type": "module"
```

permite utilizar sintaxis ES Modules:

```javascript
import express from "express";
```

---

# 5. Crear el clúster Kafka con Docker Compose

Crear el archivo:

```text
docker-compose.yml
```

con el siguiente contenido:

```yaml
services:
  kafka:
    image: confluentinc/cp-kafka:8.0.0
    container_name: kafka
    hostname: kafka

    ports:
      - "9092:9092"

    environment:
      # KRaft configuration
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller

      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:29093"
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER

      # Listeners
      KAFKA_LISTENERS: >
        INTERNAL://0.0.0.0:29092,
        EXTERNAL://0.0.0.0:9092,
        CONTROLLER://0.0.0.0:29093

      KAFKA_ADVERTISED_LISTENERS: >
        INTERNAL://kafka:29092,
        EXTERNAL://localhost:9092

      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: >
        INTERNAL:PLAINTEXT,
        EXTERNAL:PLAINTEXT,
        CONTROLLER:PLAINTEXT

      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL

      # Configuración para un clúster de un solo nodo
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0

      # Conveniente para desarrollo
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"

    volumes:
      - kafka-data:/var/lib/kafka/data

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui

    depends_on:
      - kafka

    ports:
      - "8080:8080"

    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
      DYNAMIC_CONFIG_ENABLED: "true"

volumes:
  kafka-data:
```

---

# 6. Entender los listeners de Kafka

En este ejemplo Kafka expone dos listeners.

## Listener externo

```text
localhost:9092
```

Este listener se usa desde aplicaciones ejecutadas fuera de Docker.

Por ejemplo:

```text
Node.js → localhost:9092
```

## Listener interno

```text
kafka:29092
```

Este listener se usa entre contenedores Docker.

Por ejemplo:

```text
Kafka UI → kafka:29092
```

Esto es importante porque el hostname:

```text
kafka
```

solo puede ser resuelto dentro de la red de Docker Compose.

Por lo tanto, mientras Node.js se ejecute directamente en la computadora:

```env
KAFKA_BROKER=localhost:9092
```

No se debe utilizar:

```env
KAFKA_BROKER=kafka:29092
```

a menos que la aplicación Node.js también se ejecute como contenedor Docker.

---

# 7. Iniciar Kafka y Kafka UI

Ejecutar:

```bash
docker compose up -d
```

Verificar los contenedores:

```bash
docker compose ps
```

Se deberían observar al menos:

```text
kafka
kafka-ui
```

También se pueden revisar los logs:

```bash
docker compose logs -f kafka
```

Para Kafka UI:

```bash
docker compose logs -f kafka-ui
```

---

# 8. Abrir Kafka UI

Abrir en el navegador:

```text
http://localhost:8080
```

Kafka UI permite inspeccionar:

- Brokers.
- Topics.
- Partitions.
- Messages.
- Consumer groups.
- Offsets.
- Lag.

Durante la demostración es útil mantener Kafka UI abierta para observar lo que sucede dentro del clúster.

---

# 9. Crear el tópico `messages`

Kafka puede crear tópicos automáticamente porque se configuró:

```yaml
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```

Sin embargo, para una clase es mejor crear el tópico explícitamente.

Ejecutar:

```bash
docker exec kafka \
  kafka-topics \
  --bootstrap-server kafka:29092 \
  --create \
  --topic messages \
  --partitions 3 \
  --replication-factor 1
```

La opción:

```text
--partitions 3
```

crea tres particiones.

Esto será especialmente útil para demostrar consumer groups.

Verificar los tópicos:

```bash
docker exec kafka \
  kafka-topics \
  --bootstrap-server kafka:29092 \
  --list
```

Se debería observar:

```text
messages
```

También se puede inspeccionar desde Kafka UI.

---

# 10. Configuración de variables de entorno

Crear el archivo:

```text
.env
```

con el siguiente contenido:

```env
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=messages
PORT=3000
```

---

# 11. Crear el productor Kafka

Crear:

```text
producer/kafka.js
```

Contenido:

```javascript
import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "message-api",
  brokers: [process.env.KAFKA_BROKER ?? "localhost:9092"],
});

export const producer = kafka.producer();
```

Aquí se crea un cliente Kafka.

La propiedad:

```javascript
clientId: "message-api"
```

identifica la aplicación que se conecta al clúster.

El broker se obtiene desde:

```javascript
process.env.KAFKA_BROKER
```

---

# 12. Crear la API REST productora

Crear:

```text
producer/server.js
```

Contenido:

```javascript
import "dotenv/config";
import express from "express";
import { randomUUID } from "node:crypto";
import { producer } from "./kafka.js";

const app = express();

app.use(express.json());

const PORT = process.env.PORT ?? 3000;
const TOPIC = process.env.KAFKA_TOPIC ?? "messages";

async function start() {
  await producer.connect();

  console.log("Kafka producer connected");

  app.post("/messages", async (req, res) => {
    try {
      const event = {
        id: randomUUID(),
        timestamp: new Date().toISOString(),
        data: req.body,
      };

      await producer.send({
        topic: TOPIC,
        messages: [
          {
            key: event.id,
            value: JSON.stringify(event),
          },
        ],
      });

      console.log("Message published:", event);

      res.status(202).json({
        status: "accepted",
        event,
      });
    } catch (error) {
      console.error("Error publishing message:", error);

      res.status(500).json({
        error: "Could not publish message",
      });
    }
  });

  app.listen(PORT, () => {
    console.log(`Producer API listening on http://localhost:${PORT}`);
  });
}

start().catch((error) => {
  console.error("Could not start producer:", error);
  process.exit(1);
});
```

---

# 13. Cómo funciona el productor

Cuando se recibe:

```http
POST /messages
```

la API crea un evento con esta estructura:

```json
{
  "id": "UUID",
  "timestamp": "2026-09-17T13:30:00.000Z",
  "data": {
    "user": "Alice",
    "message": "Hello Kafka!"
  }
}
```

Posteriormente lo envía a Kafka:

```javascript
await producer.send({
  topic: TOPIC,
  messages: [
    {
      key: event.id,
      value: JSON.stringify(event),
    },
  ],
});
```

Kafka maneja principalmente datos binarios.

Por esta razón el objeto JavaScript debe serializarse:

```javascript
JSON.stringify(event)
```

Cuando el consumidor lo recibe, debe hacer el proceso inverso:

```javascript
JSON.parse(...)
```

---

# 14. Importancia de `async` y `await`

Kafka realiza operaciones de I/O.

Algunas de ellas son:

```javascript
producer.connect()
producer.send()
consumer.connect()
consumer.subscribe()
consumer.run()
```

Estas operaciones no son instantáneas.

Por ejemplo:

```javascript
await producer.send(...)
```

significa:

> Espera de manera no bloqueante hasta que Kafka responda.

Node.js puede seguir atendiendo otras operaciones mientras Kafka procesa la petición.

Esto es especialmente importante en sistemas backend que realizan:

- consultas a bases de datos,
- llamadas HTTP,
- acceso a archivos,
- comunicación con Kafka,
- comunicación con Redis,
- llamadas a servicios externos.

---

# 15. Crear el consumidor Kafka

Crear:

```text
consumer/kafka.js
```

Contenido:

```javascript
import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "message-consumer",
  brokers: [process.env.KAFKA_BROKER ?? "localhost:9092"],
});

export const consumer = kafka.consumer({
  groupId: "message-processing-group",
});
```

El elemento más importante aquí es:

```javascript
groupId: "message-processing-group"
```

El `groupId` identifica el consumer group.

---

# 16. Implementar el consumidor

Crear:

```text
consumer/consumer.js
```

Contenido:

```javascript
import "dotenv/config";
import { consumer } from "./kafka.js";

const TOPIC = process.env.KAFKA_TOPIC ?? "messages";

async function start() {
  await consumer.connect();

  console.log("Kafka consumer connected");

  await consumer.subscribe({
    topic: TOPIC,
    fromBeginning: true,
  });

  console.log(`Subscribed to topic: ${TOPIC}`);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const key = message.key?.toString();
      const value = message.value?.toString();

      console.log("------------------------------");
      console.log("Message received");
      console.log("Topic:", topic);
      console.log("Partition:", partition);
      console.log("Offset:", message.offset);
      console.log("Key:", key);

      try {
        const event = JSON.parse(value);

        console.log("Event ID:", event.id);
        console.log("Timestamp:", event.timestamp);
        console.log("Data:", event.data);
      } catch {
        console.log("Raw value:", value);
      }
    },
  });
}

start().catch((error) => {
  console.error("Consumer error:", error);
  process.exit(1);
});
```

---

# 17. Iniciar el consumidor

Abrir una terminal:

```bash
npm run consumer
```

La salida debería ser similar a:

```text
Kafka consumer connected
Subscribed to topic: messages
```

El proceso permanecerá ejecutándose esperando mensajes.

---

# 18. Iniciar el productor

Abrir otra terminal:

```bash
npm run producer
```

La salida debería ser:

```text
Kafka producer connected
Producer API listening on http://localhost:3000
```

---

# 19. Enviar un mensaje

Usando `curl`:

```bash
curl -X POST http://localhost:3000/messages \
  -H "Content-Type: application/json" \
  -d '{
    "user": "Alice",
    "message": "Hello Kafka!"
  }'
```

La API responderá con algo similar:

```json
{
  "status": "accepted",
  "event": {
    "id": "d7290535-eebc-4ef6-ae6e-67b64fa3bcc5",
    "timestamp": "2026-09-17T13:30:00.000Z",
    "data": {
      "user": "Alice",
      "message": "Hello Kafka!"
    }
  }
}
```

El código HTTP utilizado es:

```text
202 Accepted
```

Esto representa correctamente que la API recibió el mensaje y lo publicó a Kafka, pero el procesamiento final ocurre de forma asíncrona.

---

# 20. Observar el consumidor

En la terminal donde se ejecuta el consumidor se debería ver algo como:

```text
------------------------------
Message received
Topic: messages
Partition: 0
Offset: 12
Key: d7290535-eebc-4ef6-ae6e-67b64fa3bcc5
Event ID: d7290535-eebc-4ef6-ae6e-67b64fa3bcc5
Timestamp: 2026-09-17T13:30:00.000Z
Data: { user: 'Alice', message: 'Hello Kafka!' }
```

Esto permite observar conceptos importantes de Kafka:

```text
Topic
Partition
Offset
Key
Value
```

---

# 21. Inspeccionar el mensaje desde Kafka UI

Abrir:

```text
http://localhost:8080
```

Entrar a:

```text
local
  └── Topics
      └── messages
```

Desde aquí se pueden observar:

- Número de particiones.
- Mensajes.
- Keys.
- Values.
- Offsets.
- Consumer groups.

Kafka UI es especialmente útil para explicar visualmente lo que ocurre dentro del broker.

---

# 22. Consumer Groups

Kafka utiliza consumer groups para distribuir trabajo.

En este ejemplo el consumidor tiene:

```javascript
groupId: "message-processing-group"
```

Ejecutar un segundo consumidor:

```bash
npm run consumer
```

Ahora existirán dos procesos dentro del mismo consumer group.

```text
                       topic: messages
                              |
                  +-----------+-----------+
                  |           |           |
             Partition 0 Partition 1 Partition 2
                  |           |           |
                  +----- Consumer Group ---+
                              |
                       +------+------+
                       |             |
                  Consumer #1   Consumer #2
```

Kafka asignará particiones entre los consumidores.

Los dos consumidores no recibirán todos los mensajes.

En su lugar, compartirán el trabajo.

---

# 23. Relación entre particiones y consumidores

Si el tópico tiene:

```text
3 particiones
```

y el grupo tiene:

```text
1 consumidor
```

ese consumidor puede procesar:

```text
Partition 0
Partition 1
Partition 2
```

Si se agregan dos consumidores:

```text
Consumer A
Consumer B
```

Kafka distribuye las particiones entre ellos.

Por ejemplo:

```text
Consumer A
  ├── Partition 0
  └── Partition 2

Consumer B
  └── Partition 1
```

Si existen más consumidores que particiones, algunos consumidores quedarán sin trabajo.

Por ejemplo:

```text
3 particiones
4 consumidores
```

Uno de los consumidores no recibirá ninguna partición.

---

# 24. Diferentes consumer groups

También se puede mostrar que diferentes consumer groups reciben los mensajes de manera independiente.

Cambiar temporalmente:

```javascript
groupId: "message-processing-group"
```

por:

```javascript
groupId: "analytics-group"
```

Ahora existen:

```text
                         Kafka
                           |
                    topic: messages
                           |
               +-----------+-----------+
               |                       |
               v                       v
      Consumer Group A        Consumer Group B

   message-processing-group      analytics-group
```

Cada grupo recibe el stream independientemente.

Esto es muy útil en arquitecturas orientadas a eventos.

Por ejemplo:

```text
OrderCreated
      |
      v
Kafka Topic
      |
      +------------------+
      |                  |
      v                  v
Inventory Group      Analytics Group
      |
      v
Notification Group
```

El mismo evento puede ser procesado por múltiples sistemas independientes.

---

# 25. Message Keys

En el productor se está utilizando:

```javascript
key: event.id
```

Kafka utiliza la key para determinar la partición.

Los mensajes con la misma key normalmente son enviados a la misma partición.

Esto permite mantener orden por entidad.

Por ejemplo:

```text
key = order-123
```

Eventos:

```text
OrderCreated
OrderPaid
OrderShipped
```

pueden permanecer en la misma partición.

Esto permite que el orden sea preservado para esa key.

Kafka garantiza orden dentro de una partición, no necesariamente entre particiones diferentes.

---

# 26. Offsets

Cada mensaje dentro de una partición tiene un offset.

Ejemplo:

```text
Partition 0

offset 0 → message A
offset 1 → message B
offset 2 → message C
offset 3 → message D
```

El offset representa la posición del mensaje dentro de la partición.

Kafka usa los offsets para saber qué mensajes ya fueron procesados por un consumer group.

---

# 27. Probar múltiples mensajes

Ejecutar varias veces:

```bash
curl -X POST http://localhost:3000/messages \
  -H "Content-Type: application/json" \
  -d '{
    "user": "Alice",
    "message": "Message 1"
  }'
```

```bash
curl -X POST http://localhost:3000/messages \
  -H "Content-Type: application/json" \
  -d '{
    "user": "Bob",
    "message": "Message 2"
  }'
```

```bash
curl -X POST http://localhost:3000/messages \
  -H "Content-Type: application/json" \
  -d '{
    "user": "Charlie",
    "message": "Message 3"
  }'
```

Después revisar Kafka UI para observar:

- partición,
- offset,
- key,
- value.

---

# 28. Comandos útiles de Kafka

## Listar tópicos

```bash
docker exec kafka \
  kafka-topics \
  --bootstrap-server kafka:29092 \
  --list
```

## Describir un tópico

```bash
docker exec kafka \
  kafka-topics \
  --bootstrap-server kafka:29092 \
  --describe \
  --topic messages
```

## Consumir mensajes desde la terminal

```bash
docker exec -it kafka \
  kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic messages \
  --from-beginning
```

## Producir mensajes manualmente

```bash
docker exec -it kafka \
  kafka-console-producer \
  --bootstrap-server kafka:29092 \
  --topic messages
```

Después escribir:

```text
Hello Kafka
```

y presionar Enter.

---

# 29. Detener el ambiente

Para detener los contenedores:

```bash
docker compose down
```

Esto mantiene el volumen:

```text
kafka-data
```

y por lo tanto los datos pueden permanecer.

Para eliminar también el volumen:

```bash
docker compose down -v
```

Esto elimina los datos almacenados por Kafka.

---

# 30. Flujo completo del ejemplo

El flujo completo es:

```text
1. Cliente hace POST /messages

        |
        v

2. Express recibe JSON

        |
        v

3. Se crea un evento

        |
        v

4. JSON.stringify(event)

        |
        v

5. producer.send()

        |
        v

6. Kafka guarda el mensaje en una partición

        |
        v

7. El consumer group recibe el mensaje

        |
        v

8. Consumer obtiene topic, partition y offset

        |
        v

9. JSON.parse(message.value)

        |
        v

10. Aplicación procesa el evento
```

---

# 31. Conceptos que demuestra este ejemplo

Este ejercicio permite introducir los siguientes conceptos:

## Producer

Aplicación que publica mensajes.

En este caso:

```text
Express REST API
```

## Broker

Servidor Kafka que almacena y distribuye mensajes.

## Topic

Canal lógico donde se publican eventos.

En este ejemplo:

```text
messages
```

## Partition

División física de un tópico que permite paralelismo.

## Message Key

Valor utilizado para ayudar a determinar la partición.

## Message Value

Contenido del evento.

## Consumer

Aplicación que lee mensajes.

## Consumer Group

Grupo de consumidores que cooperan para procesar las particiones de un tópico.

## Offset

Posición de un mensaje dentro de una partición.

---

# 32. Resultado final

Al terminar el ejercicio se tendrá:

```text
HTTP Client
     |
     | POST /messages
     v
Express REST API
     |
     | producer.send()
     v
Kafka
     |
     | topic: messages
     |
     +--------+--------+--------+
     |        |        |
     v        v        v
Partition 0 Partition 1 Partition 2
     |
     v
Consumer Group
     |
     +-----------+
     |           |
     v           v
Consumer A   Consumer B
```

Además, Kafka UI permitirá observar visualmente el estado del clúster:

```text
http://localhost:8080
```

---

# 33. Posibles extensiones

A partir de este ejemplo se pueden agregar posteriormente:

- Schema Registry.
- Avro.
- JSON Schema.
- Protobuf.
- Retry topics.
- Dead Letter Topics.
- Idempotencia.
- Multiple consumer groups.
- Event versioning.
- Correlation IDs.
- Distributed tracing.
- Microservicios.
- Saga pattern.
- Coreografía con Kafka.
- Orquestación.
- Manejo de errores.
- Observabilidad.
- Métricas de consumer lag.

Este ejemplo sirve como una base sencilla para avanzar después hacia una arquitectura real de microservicios orientada a eventos.
