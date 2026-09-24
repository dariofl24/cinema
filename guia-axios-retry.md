# Guía completa: Axios y axios-retry con un cliente y un servidor en Node.js

Esta guía muestra cómo construir, de principio a fin, un proyecto de demostración que explica para qué sirve **axios-retry** y cómo se comporta en distintos escenarios de falla.

El proyecto tiene dos componentes:

- Un **servidor** Express que falla de formas controladas: errores 503 y 500, rate limit con `Retry-After`, respuestas lentas, un POST que pierde su respuesta y una dependencia que se puede encender y apagar.
- Un **cliente** que usa Axios con y sin `axios-retry`, y ejecuta nueve escenarios para comparar el resultado.

Además incluye dos temas que complementan a los reintentos:

- **Timeouts:** cómo se combinan con los reintentos y cómo poner un plazo total.
- **Circuit breaker:** cómo dejar de llamar a un servicio que está caído, con la librería `opossum`.

---

## 1. Qué problema resuelve axios-retry

En una red real las peticiones fallan de forma intermitente: un servicio se reinicia, un balanceador responde 503, se llega al límite de peticiones o se pierde una conexión.

Muchas de esas fallas son **transitorias**: si se repite la petición un instante después, funciona.

**axios-retry** es un plugin que intercepta las peticiones fallidas de Axios y las repite automáticamente, sin que el código que llama tenga que escribir su propio ciclo de reintentos.

```text
Sin axios-retry                     Con axios-retry

cliente ──► servidor  503           cliente ──► servidor  503
cliente recibe el error                     ──► servidor  503   (reintento 1)
                                            ──► servidor  200   (reintento 2)
                                    cliente recibe la respuesta 200
```

Reintentar no es gratis ni siempre seguro. Por eso este ejemplo dedica la mitad de los escenarios a ver **cuándo no** conviene reintentar y qué precauciones tomar.

---

## 2. Arquitectura del ejemplo

```text
┌──────────────────────────┐          ┌──────────────────────────┐
│ Cliente (client/)        │          │ Servidor (server/)       │
│                          │          │                          │
│ axios + axios-retry      │  HTTP    │ Express                  │
│ opossum (circuit breaker)│ ───────► │ /flaky      503 y luego OK│
│ 9 escenarios + timeouts  │ ◄─────── │ /always-fails  500       │
│ + circuit breaker        │          │ /rate-limited  429       │
└──────────────────────────┘          │ /slow          lento     │
                                      │ /orders        POST      │
                                      │ /dependency    on / off  │
                                      └──────────────────────────┘
```

Ambos se ejecutan directamente en la computadora local. No se necesita Docker.

---

## 3. Requisitos

- Node.js 20 o superior.
- NPM.
- Un editor como VS Code, WebStorm o IntelliJ IDEA.

Para verificar Node.js y NPM:

```bash
node --version
npm --version
```

---

# 4. Crear el proyecto

Crear la carpeta e inicializar el proyecto:

```bash
mkdir axios-retry-demo
cd axios-retry-demo
npm init -y
```

La estructura final será:

```text
axios-retry-demo/
├── package.json
├── .env
├── server/
│   └── server.js
└── client/
    ├── http.js
    ├── client.js
    ├── timeouts.js
    └── breaker.js
```

Crear las carpetas:

```bash
mkdir server
mkdir client
```

Instalar las dependencias:

```bash
npm install axios axios-retry express dotenv opossum
```

---

# 5. Configurar `package.json`

Modificar `package.json` para que tenga este contenido (las versiones exactas pueden ser más nuevas):

```json
{
  "name": "axios-retry-demo",
  "version": "1.0.0",
  "description": "Demo de Axios y axios-retry con un servidor Express inestable",
  "type": "module",
  "scripts": {
    "server": "node server/server.js",
    "client": "node client/client.js",
    "timeouts": "node client/timeouts.js",
    "breaker": "node client/breaker.js"
  },
  "dependencies": {
    "axios": "^1.20.0",
    "axios-retry": "^4.5.0",
    "dotenv": "^18.0.3",
    "express": "^5.2.1",
    "opossum": "^10.0.0"
  }
}
```

La propiedad `"type": "module"` permite usar la sintaxis ES Modules (`import ... from`).

---

# 6. Variables de entorno

Crear el archivo `.env`:

```env
PORT=4000
SERVER_URL=http://localhost:4000
```

- `PORT` lo usa el servidor.
- `SERVER_URL` lo usa el cliente como URL base.

> **Nota:** `dotenv` imprime una línea `injected env (2) from .env` cada vez que carga el archivo. Es normal y se puede ignorar.

---

# 7. Crear el servidor

Crear `server/server.js`:

```javascript
import "dotenv/config";
import express from "express";
import { randomUUID } from "node:crypto";

const app = express();
app.use(express.json());

const PORT = process.env.PORT ?? 4000;

// Número de intentos recibidos por cada "key" (una key por escenario)
const attempts = new Map();
// Órdenes creadas por POST /orders
let orders = [];

function countAttempt(key = "default") {
  const n = (attempts.get(key) ?? 0) + 1;
  attempts.set(key, n);
  return n;
}

app.use((req, res, next) => {
  res.on("finish", () => {
    console.log(`${req.method} ${req.originalUrl} -> ${res.statusCode}`);
  });
  next();
});

// Falla con 503 las primeras `failures` veces y después responde 200
app.get("/flaky", (req, res) => {
  const attempt = countAttempt(req.query.key);
  const failures = Number(req.query.failures ?? 2);

  if (attempt <= failures) {
    return res.status(503).json({ error: "Service unavailable", attempt });
  }
  res.json({ status: "ok", attempt });
});

// Falla siempre con 500
app.get("/always-fails", (req, res) => {
  const attempt = countAttempt(req.query.key);
  res.status(500).json({ error: "Internal error", attempt });
});

// Responde 429 con Retry-After las primeras `limit` veces
app.get("/rate-limited", (req, res) => {
  const attempt = countAttempt(req.query.key);
  const limit = Number(req.query.limit ?? 2);

  if (attempt <= limit) {
    res.set("Retry-After", "1");
    return res.status(429).json({ error: "Too many requests", attempt });
  }
  res.json({ status: "ok", attempt });
});

// Responde lento (`delay` ms) las primeras `slowAttempts` veces
app.get("/slow", (req, res) => {
  const attempt = countAttempt(req.query.key);
  const slowAttempts = Number(req.query.slowAttempts ?? 2);
  const delay = Number(req.query.delay ?? 1500);
  const wait = attempt <= slowAttempts ? delay : 0;

  setTimeout(() => res.json({ status: "ok", attempt }), wait);
});

// Crea una orden. En el primer intento de cada key la orden SE CREA,
// pero la respuesta es 503 (simula que la respuesta se perdió).
// Si llega un Idempotency-Key ya conocido, devuelve la orden existente.
app.post("/orders", (req, res) => {
  const attempt = countAttempt(req.query.key);
  const idempotencyKey = req.get("Idempotency-Key");

  if (idempotencyKey) {
    const existing = orders.find((o) => o.idempotencyKey === idempotencyKey);
    if (existing) {
      return res.status(200).json({ order: existing, duplicate: true });
    }
  }

  const order = { id: randomUUID(), idempotencyKey, ...req.body };
  orders.push(order);

  if (attempt === 1) {
    return res.status(503).json({ error: "Response lost", attempt });
  }
  res.status(201).json({ order, attempt });
});

app.get("/orders", (req, res) => {
  const { ref } = req.query;
  res.json(ref ? orders.filter((o) => o.ref === ref) : orders);
});

// Dependencia externa simulada: se enciende y se apaga con POST /mode
let dependencyUp = true;
let dependencyRequests = 0;

app.post("/mode", (req, res) => {
  dependencyUp = req.body.mode === "up";
  console.log(`Dependency is now ${dependencyUp ? "UP" : "DOWN"}`);
  res.json({ mode: dependencyUp ? "up" : "down" });
});

app.get("/dependency", (req, res) => {
  dependencyRequests += 1;

  if (!dependencyUp) {
    return res.status(500).json({ error: "Dependency is down" });
  }
  res.json({ status: "ok" });
});

// Cuántas peticiones han llegado realmente a /dependency
app.get("/stats", (req, res) => {
  res.json({ dependencyRequests });
});

// Reinicia contadores y órdenes entre ejecuciones
app.post("/reset", (req, res) => {
  attempts.clear();
  orders = [];
  dependencyUp = true;
  dependencyRequests = 0;
  res.json({ status: "reset" });
});

app.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
});
```

---

# 8. Cómo funciona el servidor

## Contadores por `key`

El servidor necesita saber **cuántas veces** ha recibido cada petición para poder fallar las primeras veces y responder bien después. Cada escenario del cliente envía una `key` única como parámetro de consulta, y el servidor cuenta los intentos por `key`:

```javascript
const attempts = new Map();

function countAttempt(key = "default") {
  const n = (attempts.get(key) ?? 0) + 1;
  attempts.set(key, n);
  return n;
}
```

Así los escenarios no se interfieren entre sí, aunque se ejecuten uno tras otro.

## Endpoints

| Endpoint | Comportamiento | Qué demuestra |
|---|---|---|
| `GET /flaky?failures=N` | 503 las primeras N veces, luego 200 | Una falla transitoria que se recupera |
| `GET /always-fails` | Siempre 500 | Reintentos agotados |
| `GET /rate-limited?limit=N` | 429 con `Retry-After: 1` las primeras N veces | Respetar la espera que pide el servidor |
| `GET /slow?slowAttempts=N&delay=ms` | Tarda `delay` ms las primeras N veces | Timeouts |
| `POST /orders` | Crea la orden y responde 503 en el primer intento | Riesgo de duplicar en un POST |
| `GET /orders?ref=X` | Lista las órdenes | Contar duplicados |
| `POST /mode` | Enciende (`up`) o apaga (`down`) la dependencia | Circuit breaker |
| `GET /dependency` | Responde 200 si la dependencia está encendida, 500 si está apagada | Circuit breaker |
| `GET /stats` | Cuántas peticiones llegaron realmente a `/dependency` | Comprobar que el circuit breaker no llama al servidor |
| `POST /reset` | Reinicia contadores, órdenes y enciende la dependencia | Repetir la demo desde cero |

## El endpoint `POST /orders`

Este endpoint simula un caso real y peligroso: **el servidor procesa la petición, pero la respuesta se pierde**.

```javascript
const order = { id: randomUUID(), idempotencyKey, ...req.body };
orders.push(order);

if (attempt === 1) {
  return res.status(503).json({ error: "Response lost", attempt });
}
```

En el primer intento la orden **sí se crea**, pero el cliente recibe un 503. Desde el punto de vista del cliente la petición falló, aunque el servidor ya hizo el trabajo. Si el cliente reintenta a ciegas, se crea una segunda orden.

Para evitarlo, el servidor soporta el encabezado `Idempotency-Key`: si recibe una key que ya conoce, devuelve la orden existente en lugar de crear otra.

---

# 9. Iniciar el servidor

Abrir una terminal:

```bash
npm run server
```

La salida debería ser:

```text
Server listening on http://localhost:4000
```

Dejar esta terminal abierta. Cada petición recibida se imprime en el formato `MÉTODO /ruta -> estado`, lo que permite ver los reintentos desde el lado del servidor.

---

# 10. Crear los clientes HTTP

Crear `client/http.js`:

```javascript
import axios from "axios";
import axiosRetry from "axios-retry";

const baseURL = process.env.SERVER_URL ?? "http://localhost:4000";

// Cliente sin reintentos: un solo intento por petición
export const plainClient = axios.create({ baseURL, timeout: 3000 });

// Cliente con reintentos: hasta 3 reintentos con backoff exponencial
export const retryClient = axios.create({ baseURL, timeout: 3000 });

axiosRetry(retryClient, {
  retries: 3,
  retryDelay: axiosRetry.exponentialDelay,
  onRetry: (retryCount, error, requestConfig) => {
    const reason = error.response?.status ?? error.code;
    console.log(
      `  ↻ Reintento #${retryCount} de ${requestConfig.method.toUpperCase()} ${requestConfig.url} (motivo: ${reason})`,
    );
  },
  onMaxRetryTimesExceeded: (error, retryCount) => {
    console.log(`  ✖ Se agotaron los reintentos (${retryCount})`);
  },
});

export { axiosRetry };
```

---

# 11. Cómo funciona `client/http.js`

Se crean **dos instancias** de Axios con la misma configuración base:

- `plainClient`: sin reintentos. Un intento por petición.
- `retryClient`: con `axios-retry` aplicado.

Usar `axios.create()` en lugar de configurar el Axios global evita que el plugin afecte a todo el programa. Es la práctica recomendada.

## Opciones usadas

| Opción | Valor | Significado |
|---|---|---|
| `retries` | `3` | Hasta 3 reintentos después del primer intento (4 intentos en total) |
| `retryDelay` | `axiosRetry.exponentialDelay` | Espera cada vez más entre intentos: aproximadamente 200 ms, 400 ms, 800 ms... más un extra aleatorio de hasta 20 % |
| `onRetry` | función | Se llama justo antes de cada reintento. Se usa para registrar en consola |
| `onMaxRetryTimesExceeded` | función | Se llama cuando ya no quedan reintentos, antes de lanzar el error |

Por defecto `retryDelay` no espera nada entre reintentos. Sin espera, los reintentos golpean al servidor exactamente cuando está fallando. Por eso casi siempre se configura un *backoff* como `exponentialDelay`.

## Qué se reintenta por defecto

Sin configurar `retryCondition`, `axios-retry` reintenta **solo si se cumplen ambas cosas**:

1. La falla es reintentable: un error de red, un **5xx** o un **429**.
2. El método es **idempotente**: `GET`, `HEAD`, `OPTIONS`, `PUT` o `DELETE`.

Esto significa que, por defecto:

- Un `POST` **no** se reintenta.
- Un **timeout no** se reintenta (el error tiene el código `ECONNABORTED`).
- Un 4xx distinto de 429 (por ejemplo 400 o 404) **no** se reintenta, porque repetir la misma petición dará el mismo resultado.

Los escenarios 5 y 6 comprueban estos dos últimos casos.

---

# 12. Crear el cliente con los escenarios

Crear `client/client.js`:

```javascript
import "dotenv/config";
import { randomUUID } from "node:crypto";
import { plainClient, retryClient, axiosRetry } from "./http.js";

// Ejecuta un escenario e imprime el resultado sin detener el programa
async function scenario(title, fn) {
  console.log(`\n=== ${title} ===`);
  const start = Date.now();

  try {
    const response = await fn();
    console.log(`  ✔ ${response.status}`, response.data);
  } catch (error) {
    const reason = error.response?.status ?? error.code ?? error.message;
    console.log(`  ✖ Falló: ${reason}`);
  }

  console.log(`  Tiempo total: ${Date.now() - start} ms`);
}

// key única por escenario para que el servidor cuente los intentos por separado
const key = () => randomUUID();

async function countOrders(ref) {
  const { data } = await plainClient.get("/orders", { params: { ref } });
  console.log(`  Órdenes creadas para "${ref}": ${data.length}`);
}

await plainClient.post("/reset");

await scenario("1. Sin reintentos (el servidor falla 2 veces)", () =>
  plainClient.get("/flaky", { params: { key: key(), failures: 2 } }),
);

await scenario("2. Con reintentos (éxito en el tercer intento)", () =>
  retryClient.get("/flaky", { params: { key: key(), failures: 2 } }),
);

await scenario("3. Reintentos agotados (el servidor siempre falla)", () =>
  retryClient.get("/always-fails", { params: { key: key() } }),
);

await scenario("4. Rate limit: se respeta Retry-After (1 s)", () =>
  retryClient.get("/rate-limited", { params: { key: key(), limit: 2 } }),
);

await scenario("5a. Timeout: por defecto NO se reintenta", () =>
  retryClient.get("/slow", {
    params: { key: key(), slowAttempts: 2, delay: 1500 },
    timeout: 500,
  }),
);

await scenario("5b. Timeout: reintentado con retryCondition personalizado", () =>
  retryClient.get("/slow", {
    params: { key: key(), slowAttempts: 2, delay: 1500 },
    timeout: 500,
    "axios-retry": {
      shouldResetTimeout: true,
      retryCondition: (error) =>
        axiosRetry.isNetworkOrIdempotentRequestError(error) ||
        error.code === "ECONNABORTED",
    },
  }),
);

const order = (ref) => ({ ref, item: "Laptop", quantity: 1 });

await scenario("6a. POST: por defecto NO se reintenta", async () => {
  try {
    return await retryClient.post("/orders", order("6a"), {
      params: { key: key() },
    });
  } finally {
    await countOrders("6a");
  }
});

await scenario("6b. POST reintentado SIN Idempotency-Key (duplica la orden)", async () => {
  try {
    return await retryClient.post("/orders", order("6b"), {
      params: { key: key() },
      "axios-retry": { retryCondition: axiosRetry.isRetryableError },
    });
  } finally {
    await countOrders("6b");
  }
});

await scenario("6c. POST reintentado CON Idempotency-Key (no duplica)", async () => {
  try {
    return await retryClient.post("/orders", order("6c"), {
      params: { key: key() },
      headers: { "Idempotency-Key": randomUUID() },
      "axios-retry": { retryCondition: axiosRetry.isRetryableError },
    });
  } finally {
    await countOrders("6c");
  }
});
```

La función `scenario()` ejecuta cada caso, imprime el resultado y el tiempo total, y captura el error para que el programa continúe con el siguiente escenario.

---

# 13. Ejecutar el cliente

Con el servidor corriendo, abrir **otra terminal** y ejecutar:

```bash
npm run client
```

Observar también la terminal del servidor mientras el cliente trabaja.

Los demos de las secciones 15 y 16 se ejecutan con `npm run timeouts` y `npm run breaker`.

---

# 14. Resultados esperados

Los tiempos varían un poco entre ejecuciones porque `exponentialDelay` agrega un componente aleatorio. El resto debería ser igual.

## Escenario 1: sin reintentos

```text
=== 1. Sin reintentos (el servidor falla 2 veces) ===
  ✖ Falló: 503
  Tiempo total: 2 ms
```

`plainClient` hace un solo intento y recibe el 503. Este es el comportamiento base con el que se comparan los demás.

## Escenario 2: con reintentos

```text
=== 2. Con reintentos (éxito en el tercer intento) ===
  ↻ Reintento #1 de GET /flaky (motivo: 503)
  ↻ Reintento #2 de GET /flaky (motivo: 503)
  ✔ 200 { status: 'ok', attempt: 3 }
  Tiempo total: 679 ms
```

El mismo servidor con la misma falla, pero ahora `retryClient` la absorbe. El código que llamó a `retryClient.get(...)` solo ve la respuesta 200. El costo es el tiempo: 679 ms entre los dos retrasos de backoff.

## Escenario 3: reintentos agotados

```text
=== 3. Reintentos agotados (el servidor siempre falla) ===
  ↻ Reintento #1 de GET /always-fails (motivo: 500)
  ↻ Reintento #2 de GET /always-fails (motivo: 500)
  ↻ Reintento #3 de GET /always-fails (motivo: 500)
  ✖ Se agotaron los reintentos (3)
  ✖ Falló: 500
  Tiempo total: 1521 ms
```

Si el problema no es transitorio, los reintentos no ayudan. Después del último se lanza el error original y el código debe manejarlo. Nótese que el tiempo crece por el backoff: reintentar tiene un costo en latencia.

## Escenario 4: rate limit con `Retry-After`

```text
=== 4. Rate limit: se respeta Retry-After (1 s) ===
  ↻ Reintento #1 de GET /rate-limited (motivo: 429)
  ↻ Reintento #2 de GET /rate-limited (motivo: 429)
  ✔ 200 { status: 'ok', attempt: 3 }
  Tiempo total: 2368 ms
```

El servidor responde `429` con el encabezado `Retry-After: 1`. `exponentialDelay` toma el mayor entre su propio cálculo y el valor de `Retry-After`. Por eso cada espera fue de aproximadamente 1 segundo y no de 200 o 400 ms. Así el cliente respeta el ritmo que pide el servidor en lugar de insistir.

## Escenario 5a: timeout, por defecto no se reintenta

```text
=== 5a. Timeout: por defecto NO se reintenta ===
  ✖ Falló: ECONNABORTED
  Tiempo total: 505 ms
```

El servidor tarda 1500 ms y el cliente tiene un `timeout` de 500 ms. El error `ECONNABORTED` no es considerado reintentable por defecto.

## Escenario 5b: timeout con `retryCondition` personalizado

```text
=== 5b. Timeout: reintentado con retryCondition personalizado ===
  ↻ Reintento #1 de GET /slow (motivo: ECONNABORTED)
  ↻ Reintento #2 de GET /slow (motivo: ECONNABORTED)
  ✔ 200 { status: 'ok', attempt: 3 }
  Tiempo total: 1687 ms
```

Se configuran dos cosas **solo para esa petición**, usando la clave `"axios-retry"` en la configuración:

```javascript
"axios-retry": {
  shouldResetTimeout: true,
  retryCondition: (error) =>
    axiosRetry.isNetworkOrIdempotentRequestError(error) ||
    error.code === "ECONNABORTED",
},
```

- `retryCondition` amplía la regla por defecto para incluir los timeouts.
- `shouldResetTimeout: true` hace que **cada intento** tenga su propio límite de 500 ms. Sin esta opción, el `timeout` se interpreta como un límite **global** para la petición completa incluyendo los reintentos, y los reintentos casi no tendrían tiempo.

## Escenario 6a: POST, por defecto no se reintenta

```text
=== 6a. POST: por defecto NO se reintenta ===
  Órdenes creadas para "6a": 1
  ✖ Falló: 503
  Tiempo total: 14 ms
```

Este escenario muestra el problema: el cliente recibió un error, pero **el servidor sí creó la orden**. `axios-retry` no reintenta un POST por defecto precisamente porque no se puede saber si el servidor llegó a procesarlo.

## Escenario 6b: POST reintentado sin `Idempotency-Key`

```text
=== 6b. POST reintentado SIN Idempotency-Key (duplica la orden) ===
  ↻ Reintento #1 de POST /orders (motivo: 503)
  Órdenes creadas para "6b": 2
  ✔ 201 { ... attempt: 2 }
  Tiempo total: 235 ms
```

Se fuerza el reintento del POST con `retryCondition: axiosRetry.isRetryableError`. El cliente ve un éxito, pero **hay dos órdenes**: la del primer intento, cuya respuesta se perdió, y la del reintento. Este es el error clásico de reintentar peticiones no idempotentes.

## Escenario 6c: POST reintentado con `Idempotency-Key`

```text
=== 6c. POST reintentado CON Idempotency-Key (no duplica) ===
  ↻ Reintento #1 de POST /orders (motivo: 503)
  Órdenes creadas para "6c": 1
  ✔ 200 { order: { ... }, duplicate: true }
  Tiempo total: 247 ms
```

El cliente envía un encabezado `Idempotency-Key` con un UUID. Como los reintentos reutilizan la misma configuración, llevan **la misma key**. El servidor reconoce la key, no crea otra orden y devuelve la existente (`duplicate: true`). Hay una sola orden, aunque la petición se envió dos veces.

> **Importante:** en un sistema real, la key se genera **una vez por operación** del usuario, no por intento. En este ejemplo se genera antes de llamar a Axios, así que todos los intentos la comparten.

---

# 15. Timeouts

Un **timeout** es el tiempo máximo que el cliente espera una respuesta. Es la primera defensa contra un servicio lento: sin él, una petición puede quedarse esperando indefinidamente y acumular conexiones y memoria.

> Por defecto Axios **no tiene timeout** (`timeout: 0`). Siempre hay que configurarlo.

## El timeout de Axios

```javascript
const http = axios.create({ baseURL, timeout: 3000 });
```

Si el servidor no responde en 3000 ms, la petición falla con:

```text
error.code === "ECONNABORTED"
error.message === "timeout of 3000ms exceeded"
```

Como se vio en el escenario 5, `axios-retry` **no reintenta** este error por defecto.

## Timeouts y reintentos

Cuando se combinan, hay que decidir qué significa el timeout: ¿es el límite de **cada intento** o de **toda la operación**? Para verlo, crear `client/timeouts.js`:

```javascript
import "dotenv/config";
import { randomUUID } from "node:crypto";
import axios from "axios";
import axiosRetry from "axios-retry";

const baseURL = process.env.SERVER_URL ?? "http://localhost:4000";

// Reintenta también los timeouts (ECONNABORTED), pero nunca una petición
// cancelada (por ejemplo, cuando se agotó el plazo total)
const retryOnTimeout = (error) =>
  !axios.isCancel(error) &&
  (axiosRetry.isNetworkOrIdempotentRequestError(error) ||
    error.code === "ECONNABORTED");

function createClient(retryOptions = {}) {
  const client = axios.create({ baseURL });

  axiosRetry(client, {
    retries: 3,
    retryDelay: axiosRetry.exponentialDelay,
    retryCondition: retryOnTimeout,
    onRetry: (retryCount, error) =>
      console.log(`  ↻ Reintento #${retryCount} (motivo: ${error.code})`),
    ...retryOptions,
  });

  return client;
}

async function scenario(title, fn) {
  console.log(`\n=== ${title} ===`);
  const start = Date.now();

  try {
    const response = await fn();
    console.log(`  ✔ ${response.status}`, response.data);
  } catch (error) {
    console.log(`  ✖ Falló: ${error.code ?? error.message}`);
  }

  console.log(`  Tiempo total: ${Date.now() - start} ms`);
}

// El servidor tarda 1500 ms en las 2 primeras peticiones de cada key
const slowParams = () => ({ key: randomUUID(), slowAttempts: 2, delay: 1500 });

await scenario("T1. timeout de 500 ms SIN shouldResetTimeout", () =>
  createClient().get("/slow", { params: slowParams(), timeout: 500 }),
);

await scenario("T2. timeout de 500 ms CON shouldResetTimeout", () =>
  createClient({ shouldResetTimeout: true }).get("/slow", {
    params: slowParams(),
    timeout: 500,
  }),
);

await scenario("T3. timeout por intento + plazo total de 1000 ms", () =>
  createClient({ shouldResetTimeout: true }).get("/slow", {
    params: slowParams(),
    timeout: 500,
    signal: AbortSignal.timeout(1000),
  }),
);
```

Ejecutarlo con el servidor corriendo:

```bash
npm run timeouts
```

El servidor tarda 1500 ms en responder las dos primeras peticiones de cada `key`, y el cliente tiene un `timeout` de 500 ms.

### T1: sin `shouldResetTimeout`

```text
=== T1. timeout de 500 ms SIN shouldResetTimeout ===
  ✖ Falló: ECONNABORTED
  Tiempo total: 511 ms
```

Por defecto `axios-retry` trata el `timeout` como un límite **global** de toda la petición, incluyendo sus reintentos. Al agotarse el primer intento ya no queda tiempo, así que **no hay ningún reintento** aunque `retryCondition` lo permita.

### T2: con `shouldResetTimeout: true`

```text
=== T2. timeout de 500 ms CON shouldResetTimeout ===
  ↻ Reintento #1 (motivo: ECONNABORTED)
  ↻ Reintento #2 (motivo: ECONNABORTED)
  ✔ 200 { status: 'ok', attempt: 3 }
  Tiempo total: 1681 ms
```

Ahora **cada intento** tiene sus propios 500 ms. Los dos primeros se agotan, el tercero recibe la respuesta rápida.

### T3: timeout por intento y plazo total

```text
=== T3. timeout por intento + plazo total de 1000 ms ===
  ↻ Reintento #1 (motivo: ECONNABORTED)
  ✖ Falló: ERR_CANCELED
  Tiempo total: 1002 ms
```

Con `shouldResetTimeout` cada intento tiene su límite, pero la operación completa podría tardar mucho: con 3 reintentos y un timeout de 500 ms serían hasta 4 × 500 ms más las esperas del backoff.

Para poner un **plazo total** se usa una señal de cancelación:

```javascript
signal: AbortSignal.timeout(1000)
```

Al cumplirse 1000 ms, Axios cancela la petición en curso y falla con `ERR_CANCELED`, aunque queden reintentos disponibles.

> **Cuidado:** `axios-retry` considera las cancelaciones como errores de red y las reintenta. Como la señal ya está cancelada, cada reintento falla al instante y se desperdician los intentos. Por eso `retryOnTimeout` excluye explícitamente las cancelaciones:
>
> ```javascript
> const retryOnTimeout = (error) =>
>   !axios.isCancel(error) &&
>   (axiosRetry.isNetworkOrIdempotentRequestError(error) ||
>     error.code === "ECONNABORTED");
> ```
>
> Sin la línea `!axios.isCancel(error)`, la salida de T3 mostraba dos reintentos más (`ERR_CANCELED`) inmediatos e inútiles.

## Cómo elegir los valores

El peor caso de una operación con reintentos es aproximadamente:

```text
(reintentos + 1) × timeout por intento  +  suma de las esperas entre intentos
```

Con `retries: 3`, `timeout: 500` y `exponentialDelay` (unos 200, 400 y 800 ms) son unos 2000 ms + 1400 ms = **3,4 segundos** antes de rendirse. Ese es el tiempo que puede esperar quien llamó, así que conviene:

- Elegir primero el **tiempo máximo aceptable** para el usuario o para el servicio que llama.
- Repartirlo entre intentos, esperas y plazo total con `AbortSignal.timeout`.
- Poner el timeout de cada intento por encima de la latencia normal del servicio (por ejemplo, su p99), para no descartar respuestas que sí iban a llegar.

---

# 16. Circuit breaker

Los reintentos ayudan cuando la falla es breve. Pero si el servicio está **caído** durante minutos, reintentar empeora las cosas:

- Cada petición del cliente se convierte en hasta 4 peticiones al servidor que ya está en problemas.
- Cada una espera su timeout, así que los clientes se acumulan esperando.
- El servidor no tiene oportunidad de recuperarse.

Un **circuit breaker** (cortacircuitos) resuelve esto: cuando detecta que un servicio falla demasiado, **deja de llamarlo por un tiempo** y falla de inmediato, igual que un interruptor eléctrico que se dispara para proteger la instalación.

## Los tres estados

```text
                 muchas fallas
   ┌────────┐ ─────────────────► ┌────────┐
   │CERRADO │                    │ABIERTO │◄────────────┐
   │(normal)│ ◄───────────────── │(bloquea)│            │
   └────────┘   la prueba sale   └───┬────┘   la prueba │
        ▲            bien            │        falla     │
        │                            │ pasa resetTimeout│
        │                            ▼                  │
        │                       ┌───────────┐           │
        └───────────────────────┤SEMIABIERTO├───────────┘
                                │ (prueba)  │
                                └───────────┘
```

| Estado | Qué hace |
|---|---|
| **Cerrado** | Funcionamiento normal. Las peticiones pasan y se cuentan los éxitos y las fallas. |
| **Abierto** | Se superó el umbral de fallas. Las peticiones **fallan de inmediato sin llegar al servidor**. |
| **Semiabierto** | Pasó `resetTimeout`. Se deja pasar **una petición de prueba**. Si tiene éxito, el circuito se cierra. Si falla, se vuelve a abrir. |

## Reintentos vs circuit breaker

Se complementan, no se reemplazan:

| | axios-retry | Circuit breaker |
|---|---|---|
| Falla que ataca | Breve y transitoria | Prolongada |
| Qué hace | Repite la petición | Deja de intentar |
| Riesgo si se usa solo | Multiplica la carga sobre un servicio caído | Rechaza peticiones que hubieran funcionado si hay una falla puntual |

## opossum

Axios y `axios-retry` no incluyen circuit breaker. En Node.js se usa la librería **opossum** (ya instalada en el paso 4):

```bash
npm install opossum
```

## Crear el demo

Crear `client/breaker.js`:

```javascript
import "dotenv/config";
import axios from "axios";
import CircuitBreaker from "opossum";

const baseURL = process.env.SERVER_URL ?? "http://localhost:4000";
const http = axios.create({ baseURL, timeout: 1000 });

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// El breaker envuelve la función que hace la llamada a la dependencia
const breaker = new CircuitBreaker(() => http.get("/dependency"), {
  timeout: 2000, // si la llamada tarda más, cuenta como falla
  volumeThreshold: 4, // mínimo de llamadas antes de poder abrirse
  errorThresholdPercentage: 50, // % de fallas para abrirse
  resetTimeout: 3000, // ms abierto antes de probar de nuevo
});

// Respuesta alternativa cuando la llamada falla o el circuito está abierto
breaker.fallback(() => ({ status: "FALLBACK", data: { status: "cached" } }));

breaker.on("open", () => console.log("  🔴 Circuito ABIERTO"));
breaker.on("halfOpen", () => console.log("  🟡 Circuito SEMIABIERTO (prueba)"));
breaker.on("close", () => console.log("  🟢 Circuito CERRADO"));

let lastReason = "";
breaker.on("failure", (error) => {
  lastReason = `la dependencia falló (${error.response?.status ?? error.code})`;
});
breaker.on("reject", () => {
  lastReason = "circuito abierto: NO se llamó al servidor";
});

async function call(label) {
  lastReason = "";
  const response = await breaker.fire();

  if (response.status === "FALLBACK") {
    console.log(`  ${label}: fallback → ${lastReason}`);
  } else {
    console.log(`  ${label}: ${response.status} OK`);
  }
}

async function setMode(mode) {
  await http.post("/mode", { mode });
}

async function requestsReceived() {
  const { data } = await http.get("/stats");
  return data.dependencyRequests;
}

await http.post("/reset");

console.log("\n=== 1. Dependencia sana: el circuito está cerrado ===");
for (let i = 1; i <= 4; i++) await call(`Llamada ${i}`);

console.log("\n=== 2. La dependencia se cae ===");
await setMode("down");
for (let i = 5; i <= 12; i++) await call(`Llamada ${i}`);
console.log(`  Peticiones que llegaron al servidor: ${await requestsReceived()} de 12`);

console.log("\n=== 3. Se espera resetTimeout y la dependencia sigue caída ===");
await sleep(3500);
await call("Llamada 13 (prueba)");
await call("Llamada 14");

console.log("\n=== 4. La dependencia se recupera ===");
await setMode("up");
await sleep(3500);
await call("Llamada 15 (prueba)");
await call("Llamada 16");
await call("Llamada 17");

breaker.shutdown();
```

## Cómo funciona

El breaker recibe **una función** que hace la llamada, y se ejecuta con `breaker.fire()`:

```javascript
const breaker = new CircuitBreaker(() => http.get("/dependency"), { ... });
const response = await breaker.fire();
```

Opciones usadas:

| Opción | Valor | Significado |
|---|---|---|
| `timeout` | `2000` | Si la llamada tarda más, cuenta como falla (independiente del timeout de Axios) |
| `volumeThreshold` | `4` | Mínimo de llamadas antes de que el circuito pueda abrirse |
| `errorThresholdPercentage` | `50` | Se abre cuando el porcentaje de fallas **supera** este valor |
| `resetTimeout` | `3000` | Milisegundos que permanece abierto antes de pasar a semiabierto |

`breaker.fallback(...)` define una respuesta alternativa que se usa cuando la llamada falla o el circuito está abierto. En una aplicación real podría devolver datos en caché o un valor por defecto. Sin `fallback`, `fire()` lanza un error (con `code: "EOPENBREAKER"` si el circuito está abierto).

## Ejecutar

Con el servidor corriendo:

```bash
npm run breaker
```

## Resultado esperado

```text
=== 1. Dependencia sana: el circuito está cerrado ===
  Llamada 1: 200 OK
  Llamada 2: 200 OK
  Llamada 3: 200 OK
  Llamada 4: 200 OK

=== 2. La dependencia se cae ===
  Llamada 5: fallback → la dependencia falló (500)
  Llamada 6: fallback → la dependencia falló (500)
  Llamada 7: fallback → la dependencia falló (500)
  Llamada 8: fallback → la dependencia falló (500)
  🔴 Circuito ABIERTO
  Llamada 9: fallback → la dependencia falló (500)
  Llamada 10: fallback → circuito abierto: NO se llamó al servidor
  Llamada 11: fallback → circuito abierto: NO se llamó al servidor
  Llamada 12: fallback → circuito abierto: NO se llamó al servidor
  Peticiones que llegaron al servidor: 9 de 12

=== 3. Se espera resetTimeout y la dependencia sigue caída ===
  🟡 Circuito SEMIABIERTO (prueba)
  🔴 Circuito ABIERTO
  Llamada 13 (prueba): fallback → la dependencia falló (500)
  Llamada 14: fallback → circuito abierto: NO se llamó al servidor

=== 4. La dependencia se recupera ===
  🟡 Circuito SEMIABIERTO (prueba)
  🟢 Circuito CERRADO
  Llamada 15 (prueba): 200 OK
  Llamada 16: 200 OK
  Llamada 17: 200 OK
```

### Cómo leer el resultado

**Sección 1.** Con la dependencia sana, el circuito está cerrado y las 4 llamadas llegan al servidor.

**Sección 2.** Tras apagar la dependencia, cada llamada falla con 500 y devuelve el fallback en lugar de lanzar un error. El circuito se abre en la **llamada 9**, no en la 8. La razón es que el porcentaje de fallas se calcula sobre una ventana móvil (10 segundos por defecto) que también incluye las 4 llamadas exitosas:

```text
Después de la llamada 8:  4 fallas de 8 llamadas = 50 %   (no supera 50 %)
Después de la llamada 9:  5 fallas de 9 llamadas = 56 %   (supera 50 %, se abre)
```

A partir de ahí las llamadas 10 a 12 **no llegan al servidor**: fallan al instante. Por eso al final solo 9 de las 12 peticiones llegaron realmente (4 sanas + 5 fallidas), y el servidor caído recibió 3 peticiones menos.

**Sección 3.** Después de `resetTimeout` (3 s) el circuito pasa a semiabierto y deja pasar una petición de prueba. Como la dependencia sigue caída, la prueba falla y el circuito **vuelve a abrirse** de inmediato.

**Sección 4.** Se enciende la dependencia, y tras otro `resetTimeout` la petición de prueba tiene éxito: el circuito se **cierra** y todo vuelve a la normalidad.

## Cómo combinarlo con axios-retry

Un orden razonable es que el **circuit breaker envuelva** al cliente con reintentos:

```text
breaker.fire()  ──►  retryClient.get(...)  ──►  servidor
   (falla final)        (hasta 3 reintentos)
```

Así una operación con todos sus reintentos cuenta como **una** llamada para el breaker, y cuando el circuito está abierto no se hace ningún reintento. Al hacerlo:

- Reducir `retries` (uno o dos bastan) para que el breaker no espere demasiado.
- Asegurar que el `timeout` del breaker sea mayor que el peor caso de los reintentos (sección 15), o el breaker cortará la operación mientras todavía reintenta.

> Este esquema es una recomendación de diseño. El demo de esta guía usa el breaker sobre un cliente sin reintentos para que los estados sean fáciles de observar.

## Consideraciones

- **Un breaker por dependencia.** Si se comparte uno entre servicios distintos, la caída de uno bloquea a los demás.
- **El estado vive en memoria** de cada proceso. Si hay varias instancias del cliente, cada una tiene su propio circuito.
- **No todo error debe contar.** Un 404 o un 400 no significan que el servicio esté caído. `opossum` permite excluirlos con la opción `errorFilter`, una función que devuelve `true` para los errores que **no** deben contarse como falla.
- **Definir bien el fallback.** Un fallback que devuelve datos desactualizados puede ser mejor que un error, pero conviene que el usuario sepa que son datos en caché.
- **Monitorear los estados.** Los eventos `open`, `halfOpen` y `close` son buenos candidatos para registrarse o enviarse a métricas.

---

# 17. Resumen de comportamientos

| Situación | ¿Se reintenta por defecto? | Cómo cambiarlo |
|---|---|---|
| Error de red (sin respuesta) | Sí, en métodos idempotentes | `retryCondition` |
| `GET` con 5xx | Sí | `retries: 0` en la petición para desactivar |
| `GET` con 429 | Sí, respetando `Retry-After` | `retryDelay` |
| `GET` con 400 o 404 | No | `retryCondition` |
| Timeout (`ECONNABORTED`) | No | `retryCondition` + `shouldResetTimeout` |
| `POST` o `PATCH` con 5xx | No | `retryCondition` + `Idempotency-Key` |
| Espera entre intentos | Ninguna | `retryDelay: axiosRetry.exponentialDelay` |
| Cancelación (`ERR_CANCELED`) | Sí (es un error inútil de reintentar) | Excluirla con `!axios.isCancel(error)` en `retryCondition` |
| Servicio caído por mucho tiempo | Se reintenta cada petición | Circuit breaker (`opossum`) |

---

# 18. Configuración por petición

Además de la configuración global del cliente, cada petición puede sobrescribir opciones con la clave `"axios-retry"`:

```javascript
// Desactivar reintentos solo para esta petición
retryClient.get("/flaky", {
  "axios-retry": { retries: 0 },
});

// Más reintentos y espera lineal solo aquí
retryClient.get("/flaky", {
  "axios-retry": {
    retries: 5,
    retryDelay: axiosRetry.linearDelay(500),
  },
});
```

`axiosRetry.linearDelay(factor)` espera `factor × número de reintento` milisegundos (500 ms, 1000 ms, 1500 ms...).

---

# 19. Buenas prácticas

- **Siempre usar un backoff** (`exponentialDelay`). Reintentar sin espera puede empeorar una caída.
- **Limitar los reintentos.** Tres o cuatro suelen ser suficientes; más solo alarga la espera del usuario.
- **Reintentar solo fallas transitorias.** Un 400 o un 404 no se arreglan repitiéndolos.
- **Nunca reintentar un POST sin una `Idempotency-Key`** que el servidor respete.
- **Respetar `Retry-After`.** Es la forma en que el servidor indica cuánto espera necesita.
- **Registrar los reintentos** con `onRetry`. Ayuda a detectar servicios inestables que los reintentos estarían ocultando.
- **Recordar el costo en latencia.** Con backoff, una petición puede tardar varios segundos antes de fallar definitivamente.
- **Poner siempre un timeout** y un plazo total para la operación completa.
- **Excluir las cancelaciones** de `retryCondition`.
- **Usar un circuit breaker** para dependencias que pueden caerse por periodos largos.
- **Cuidar la amplificación.** Si muchos clientes reintentan a la vez, pueden saturar un servidor que ya está en problemas. El componente aleatorio de `exponentialDelay` ayuda a repartirlos en el tiempo.

---

# 20. Ejercicios adicionales

1. En el escenario 2, cambiar `failures` a `5`. ¿Qué pasa? (Solo hay 3 reintentos, así que falla.) Después subir `retries` a `5` en la petición.
2. Cambiar `retryDelay` por `axiosRetry.linearDelay(500)` y comparar el tiempo total del escenario 3.
3. Quitar `shouldResetTimeout: true` del escenario 5b. ¿Qué cambia?
4. Agregar en el servidor un endpoint `GET /not-found` que responda 404 y comprobar que `retryClient` **no** reintenta.
5. Agregar un endpoint `PUT /orders/:id` que falle una vez con 503 y comprobar que **sí** se reintenta por defecto, porque `PUT` es idempotente.
6. Detener el servidor con `Ctrl+C` a mitad del escenario 3 y observar el comportamiento ante un error de red (`ECONNREFUSED`).
7. Usar `onRetry` para agregar un encabezado `X-Retry-Count` en cada reintento y mostrarlo en el servidor.
8. En `client/timeouts.js`, quitar `!axios.isCancel(error)` de `retryOnTimeout` y comprobar que T3 vuelve a mostrar reintentos con `ERR_CANCELED`.
9. En `client/breaker.js`, cambiar `resetTimeout` a `1000` y observar cómo cambia el ritmo con el que el circuito prueba la dependencia.
10. Subir `errorThresholdPercentage` a `80` y calcular en qué llamada se abre ahora el circuito.
11. Quitar `breaker.fallback(...)` y capturar el error con `try/catch` para ver el `code` `EOPENBREAKER` cuando el circuito está abierto.
12. Envolver `retryClient` (con `retries: 2`) dentro del breaker y comparar cuántas peticiones llegan al servidor durante una caída.

---

# 21. Detener el ambiente

Detener el servidor con `Ctrl+C` en su terminal.

Como el servidor guarda todo en memoria, al reiniciarlo se pierden las órdenes y los contadores. El cliente también llama a `POST /reset` al inicio para empezar cada ejecución desde cero.

---

# 22. Resultado final

Al terminar el ejercicio se tendrá:

```text
axios-retry-demo/
├── server/server.js     Servidor Express con endpoints inestables
└── client/
    ├── http.js          plainClient y retryClient (Axios + axios-retry)
    ├── client.js        Nueve escenarios que comparan ambos clientes
    ├── timeouts.js      Timeouts, shouldResetTimeout y plazo total
    └── breaker.js       Circuit breaker con opossum
```

y una demostración práctica de:

- Cómo `axios-retry` absorbe fallas transitorias.
- Cómo configurar backoff exponencial y respetar `Retry-After`.
- Qué no se reintenta por defecto y por qué.
- Cómo reintentar timeouts con `retryCondition` y `shouldResetTimeout`.
- Por qué reintentar un POST puede duplicar datos y cómo evitarlo con `Idempotency-Key`.
- Cómo se combinan los timeouts con los reintentos y cómo fijar un plazo total.
- Cómo un circuit breaker protege a un servicio caído y a sus clientes.
