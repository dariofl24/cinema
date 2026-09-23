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
      KAFKA_LISTENERS: "INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka:29092,EXTERNAL://localhost:9092"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"

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
    image: ghcr.io/kafbat/kafka-ui:latest
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

> **Nota:** la imagen original de Kafka UI (`provectuslabs/kafka-ui`) ya no recibe mantenimiento. El proyecto continúa como **Kafbat UI** (`ghcr.io/kafbat/kafka-ui`), que usa las mismas variables de entorno. Para tener builds reproducibles se recomienda reemplazar `latest` por una versión específica.

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

Sin embargo, para la clase es mejor crear el tópico explícitamente.

> **Nota:** Kafka tarda unos segundos en estar listo después de `docker compose up -d`. Si el comando falla con un error de conexión, esperar entre 10 y 15 segundos y volver a intentarlo.

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
            key: req.body?.user ?? event.id,
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
      key: req.body?.user ?? event.id,
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

La key del mensaje es el campo `user` del cuerpo de la petición (o el `id` del evento si no se envía `user`). Esto se explica en la sección 25.

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
  groupId: process.env.KAFKA_GROUP_ID ?? "message-processing-group",
});
```

El elemento más importante aquí es:

```javascript
groupId: process.env.KAFKA_GROUP_ID ?? "message-processing-group"
```

Por defecto el grupo es `message-processing-group`, pero se puede cambiar con la variable `KAFKA_GROUP_ID` (se usa en la sección 24).

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
const GROUP_ID = process.env.KAFKA_GROUP_ID ?? "message-processing-group";

async function start() {
  await consumer.connect();

  console.log(`Kafka consumer connected (group: ${GROUP_ID}, pid: ${process.pid})`);

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
      console.log("Group:", GROUP_ID);
      console.log("Consumer PID:", process.pid);
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
Kafka consumer connected (group: message-processing-group, pid: 12345)
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

Es posible que también aparezcan estos avisos, que no afectan el funcionamiento:

- Un warning de kafkajs sobre el particionador por defecto. Se silencia con la variable de entorno `KAFKAJS_NO_PARTITIONER_WARNING=1`.
- Un `TimeoutNegativeWarning` en Node.js 24 o superior, causado por kafkajs 2.2.4.

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
Offset: 0
Key: Alice
Event ID: d7290535-eebc-4ef6-ae6e-67b64fa3bcc5
Timestamp: 2026-09-17T13:30:00.000Z
Data: { user: 'Alice', message: 'Hello Kafka!' }
```

El offset y la partición dependen del estado del tópico. En un tópico recién creado, el primer mensaje de cada partición tiene offset `0`.

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
        +---------------+---------------+
        |               |               |
   Partition 0     Partition 1     Partition 2
        |               |               |
        +-------+       +-------+-------+
                |               |
           Consumer #1     Consumer #2

        (ambos en message-processing-group)
```

Kafka asigna cada partición a un solo consumidor del grupo. Un consumidor puede recibir varias particiones, pero una partición nunca la procesan dos consumidores del mismo grupo a la vez.

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

El reparto exacto depende del asignador de particiones. kafkajs usa por defecto `RoundRobinAssigner`, así que el resultado puede variar según el orden en que se unan los consumidores. Por ejemplo:

```text
Consumer A
  ├── Partition 0
  └── Partition 1

Consumer B
  └── Partition 2
```

Lo que sí es constante es que cada partición tiene un único consumidor dentro del grupo. El reparto asignado se puede ver en el log del consumidor, en el campo `memberAssignment`, o en Kafka UI.

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

Para no editar el código, el `groupId` se puede leer de una variable de entorno. En `consumer/kafka.js`:

```javascript
export const consumer = kafka.consumer({
  groupId: process.env.KAFKA_GROUP_ID ?? "message-processing-group",
});
```

Después iniciar un consumidor en otro grupo:

```bash
KAFKA_GROUP_ID=analytics-group npm run consumer
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
key: req.body?.user ?? event.id
```

Es decir, la key es el usuario que envía el mensaje. Si la petición no incluye `user`, se usa el `id` del evento, que es un UUID distinto en cada mensaje.

Kafka utiliza la key para determinar la partición.

Los mensajes con la misma key son enviados a la misma partición.

Esto permite mantener orden por entidad.

> **Importante:** si la key fuera siempre `event.id`, cada mensaje tendría una key distinta y la key solo repartiría los mensajes entre particiones, sin garantizar ningún orden entre eventos relacionados. Por eso se usa un valor que identifica a la entidad, en este caso el usuario.

Para comprobarlo, enviar varios mensajes con `"user": "Alice"` y revisar en el consumidor o en Kafka UI que todos caen en la misma partición.

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

# 28. Ejercicio: varios consumidores y varios consumer groups

Este ejercicio comprueba en la práctica cómo Kafka reparte los mensajes. Tiene dos partes:

1. **Varios consumidores en el mismo consumer group:** se reparten el trabajo.
2. **Dos consumer groups, cada uno con varios consumidores:** cada grupo recibe todos los mensajes.

Para que el resultado se pueda leer, el consumidor imprime su grupo (`Group`) y su proceso (`Consumer PID`) en cada mensaje, como en la sección 16. Las variables `KAFKA_TOPIC` y `KAFKA_GROUP_ID` permiten cambiar el tópico y el grupo sin editar el código.

## Preparación

Crear un tópico nuevo para el ejercicio, con 3 particiones:

```bash
docker exec kafka \
  kafka-topics \
  --bootstrap-server kafka:29092 \
  --create \
  --topic exercise-messages \
  --partitions 3 \
  --replication-factor 1
```

Se usarán varias terminales. Todas se abren en la carpeta del proyecto.

## Parte 1: varios consumidores en el mismo grupo

**Terminal 1: productor** (publicando en el tópico del ejercicio):

```bash
KAFKA_TOPIC=exercise-messages npm run producer
```

**Terminales 2, 3 y 4: tres consumidores del mismo grupo.** Ejecutar el mismo comando en cada terminal, esperando unos segundos entre una y otra:

```bash
KAFKA_TOPIC=exercise-messages KAFKA_GROUP_ID=exercise-group npm run consumer
```

Cada vez que se une un consumidor, Kafka hace un *rebalanceo* y reasigna las particiones. Esperar unos 10 a 15 segundos después de iniciar el último. En los logs de kafkajs, el campo `memberAssignment` indica qué particiones recibió cada consumidor. Al final, cada uno debería tener una partición:

```text
Consumer 1  →  exercise-messages: [0]
Consumer 2  →  exercise-messages: [2]
Consumer 3  →  exercise-messages: [1]
```

El reparto exacto puede cambiar entre ejecuciones, pero con 3 particiones y 3 consumidores cada uno recibe una.

**Terminal 5: enviar los mensajes.**
Para enviar mensajes con distintas keys se usará este script, que publica 9 mensajes con los usuarios `user-1` a `user-9`:

```bash
for i in $(seq 1 9); do
  curl -s -o /dev/null -X POST http://localhost:3000/messages \
    -H "Content-Type: application/json" \
    -d "{\"user\":\"user-$i\",\"message\":\"Message $i\"}"
done
```

Como la key es el usuario (sección 25), cada usuario siempre cae en la misma partición.

**Resultado esperado.** Cada mensaje lo procesa un solo consumidor, el que tiene asignada la partición de su key. Por ejemplo:

```text
Consumer 1 (pid 40027)   partición 0:  user-8
Consumer 2 (pid 40048)   partición 2:  user-1, user-2, user-3, user-6, user-7, user-9
Consumer 3 (pid 40069)   partición 1:  user-4, user-5
```

Los 9 mensajes se procesaron una sola vez en total. La cantidad por consumidor no es pareja porque las keys se reparten según su hash, no de forma equitativa.

**Preguntas para analizar:**

- ¿Algún mensaje llegó a dos consumidores del grupo? (No.)
- Volver a ejecutar el script: ¿`user-1` cae en la misma partición y en el mismo consumidor?
- Detener con `Ctrl+C` el consumidor 2 y esperar unos segundos. ¿Qué pasa con su partición? Kafka la reasigna a alguno de los consumidores que siguen vivos. Enviar otra vez los mensajes para comprobarlo.
- Agregar un cuarto consumidor al grupo. Como solo hay 3 particiones, uno queda sin partición asignada y no recibe mensajes.

En **Kafka UI** se puede abrir `Consumers` → `exercise-group` para ver los miembros, las particiones asignadas y el lag.

Al terminar, detener con `Ctrl+C` todos los consumidores y el productor.

## Parte 2: dos consumer groups, cada uno con varios consumidores

Para que los resultados sean fáciles de contar, empezar con un tópico limpio. Con el productor y los consumidores detenidos:

```bash
docker exec kafka \
  kafka-topics \
  --bootstrap-server kafka:29092 \
  --delete \
  --topic exercise-messages
```

Esperar unos segundos y crearlo de nuevo con el comando de la preparación.

> Un consumer group nuevo empieza a leer desde el principio del tópico (`fromBeginning: true`). Sin el paso anterior, los grupos nuevos recibirían también los mensajes de la Parte 1 y las cuentas no coincidirían.

**Terminal 1: productor:**

```bash
KAFKA_TOPIC=exercise-messages npm run producer
```

**Terminales 2 y 3: dos consumidores del grupo `billing-group`:**

```bash
KAFKA_TOPIC=exercise-messages KAFKA_GROUP_ID=billing-group npm run consumer
```

**Terminales 4 y 5: dos consumidores del grupo `analytics-group`:**

```bash
KAFKA_TOPIC=exercise-messages KAFKA_GROUP_ID=analytics-group npm run consumer
```

Esperar 10 a 15 segundos a que ambos grupos terminen de repartirse las particiones.

**Terminal 6: enviar los mensajes.** Ejecutar el script de la preparación.

El esquema queda así:

```text
                 topic: exercise-messages
        +------------+------------+------------+
        | Partition 0| Partition 1| Partition 2|
        +-----+------+------+-----+------+-----+
              |             |            |
   billing-group:           |            |
      Consumer B1 <---------+            |      (B1: particiones 0 y 1)
      Consumer B2 <----------------------+      (B2: partición 2)
              |             |            |
   analytics-group:         |            |
      Consumer A1 <---------+            |      (A1: particiones 0 y 1)
      Consumer A2 <----------------------+      (A2: partición 2)
```

Las particiones se leen dos veces, una vez por grupo, y dentro de cada grupo cada partición tiene un solo consumidor.

**Resultado esperado.** Los mensajes se reparten dentro de cada grupo, pero cada grupo los recibe todos. Por ejemplo:

```text
billing-group
  Consumer B1 (pid 40220)   partición 1: user-4, user-5    partición 0: user-8
  Consumer B2 (pid 40243)   partición 2: user-1, user-2, user-3, user-6, user-7, user-9

analytics-group
  Consumer A1 (pid 40268)   partición 1: user-4, user-5    partición 0: user-8
  Consumer A2 (pid 40293)   partición 2: user-1, user-2, user-3, user-6, user-7, user-9
```

En total se publicaron 9 mensajes y se procesaron 18 veces: 9 por `billing-group` y 9 por `analytics-group`.

Esto es lo que permite que varios sistemas independientes reaccionen al mismo evento: facturación y analítica no se estorban entre sí, y cada uno puede escalar agregando consumidores sin afectar al otro.

**Preguntas para analizar:**

- ¿Cuántas veces se procesó el mensaje de `user-1` en total? (Dos: una por grupo.)
- Detener los dos consumidores de `analytics-group` y enviar más mensajes. ¿`billing-group` se ve afectado? Volver a iniciar `analytics-group` y observar que recibe los mensajes que se quedó sin leer, gracias a sus offsets.
- En Kafka UI, comparar el lag de `billing-group` y `analytics-group`.

Al terminar, detener todos los procesos con `Ctrl+C`.

---

# 29. Comandos útiles de Kafka

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

# 30. Detener el ambiente

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

# 31. Flujo completo del ejemplo

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

# 32. Conceptos que demuestra este ejemplo

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

# 33. Resultado final

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
     +-----------+-----------+
     |           |           |
     v           v           v
Partition 0 Partition 1 Partition 2
     |           |           |
     +-----+     +-----+-----+
           |           |
           v           v
      Consumer A   Consumer B
   (mismo consumer group)
```

Además, Kafka UI permitirá observar visualmente el estado del clúster:

```text
http://localhost:8080
```

---

# 34. Posibles extensiones

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
