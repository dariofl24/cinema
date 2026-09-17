# Cine: seis microservicios para la clase

Aplicación en español para observar una compra, la competencia por asientos y la diferencia entre una llamada HTTP síncrona y eventos Kafka. Node.js se ejecuta localmente; los dos Compose existentes proporcionan MongoDB y Kafka. Pagos y notificaciones son simulaciones, sin autenticación ni proveedores externos.

El [flujo completo en diagramas de secuencia Mermaid](DIAGRAMAS_SECUENCIA.md) muestra compra, pagos, concurrencia, vencimientos y recuperación.

## Inicio

Requisitos: Node.js 22 o posterior, npm y Docker con Compose. Desde la raíz de esta carpeta:

```sh
docker compose -f docker-compose.mongodb.yml up -d
docker compose -f docker-compose.kafka.yml up -d
cd cinema
cp .env.example .env
npm ci
npm run setup
npm start
```

`npm install` también instala las dependencias; `npm ci` reproduce el archivo de bloqueo. Si Kafka todavía está iniciando, espera a que esté listo y repite `npm run setup`. La inicialización es idempotente: crea tópicos e índices y agrega dos funciones (`showtime-1` y `showtime-2`) con asientos D1–F10, incluidos F8/F9; no borra compras ni libera asientos vendidos.

Abre [la aplicación](http://localhost:3000), [Kafka UI](http://localhost:8080) o [Mongo Express](http://localhost:8081). La conexión local predeterminada es `mongodb://admin:admin123@localhost:27017/?authSource=admin`; Kafka anuncia `localhost:9092`. Las credenciales corresponden exclusivamente a la infraestructura didáctica del repositorio.

## Servicios y datos

| Servicio             | Entrada           | Responsabilidad                                           |
| -------------------- | ----------------- | --------------------------------------------------------- |
| ticket-purchase-api  | HTTP 3000 y Kafka | Página web, compras, coordinación y proyección de boletos |
| seat-service         | HTTP 3001 y Kafka | Funciones, exclusividad de asientos y bloqueos            |
| payment-service      | HTTP 3002         | Autorización simulada e idempotente                       |
| ticket-service       | Kafka             | Boletos y evento `TicketsIssued`                          |
| notification-service | Kafka             | Una confirmación simulada por compra                      |
| analytics-service    | Kafka             | Registro de compras, cantidad de boletos e ingresos       |

Cada servicio tiene su propia base lógica en MongoDB. Comparte configuración, conexiones, eventos y logs; consulta a otros servicios por HTTP o consume eventos, sin leer sus bases. Para inspección didáctica puedes consultar las bases desde Mongo Express.

```mermaid
flowchart LR
  Web["Navegador"] -->|HTTP| API["ticket-purchase-api :3000"]
  API -->|HTTP disponibilidad| Seats["seat-service :3001"]
  API -->|Axios: timeout 2 s| Pay["payment-service :3002"]
  API -->|SeatHoldRequested / SeatPaymentRequested| Requests["cinema.seat-hold-requests"]
  Requests --> Seats
  Seats -->|Respuesta / confirmación / liberación| Events["cinema.seat-hold-events"]
  Events --> API
  API -->|TicketPurchaseCompleted| Completed["cinema.purchase-events"]
  Completed --> Tickets["ticket-service"]
  Completed --> Notifications["notification-service"]
  Completed --> Analytics["analytics-service"]
  Tickets -->|TicketsIssued| Issued["cinema.ticket-events"]
  Issued --> API
```

Los tópicos tienen tres particiones y factor de replicación uno. Las solicitudes de asientos usan `showtimeId` como key. Cada consumidor tiene un grupo estable; los tres consumidores de compras completadas usan grupos distintos y reciben cada uno el evento. El sobre contiene `eventId`, `eventType`, `occurredAt` y `data`, con `purchaseId` para correlación. Los logs JSON incluyen servicio, compra, evento y, al consumir, tópico, partición y offset.

## Flujo y garantías

1. La API valida cliente, función, asientos y modo de pago; calcula el importe en el servidor y devuelve `202` con `purchaseId`.
2. `seat-service` guarda todos los asientos de una función en un documento y modifica el conjunto mediante una actualización condicional atómica. Acepta todos o ninguno y conserva el resultado de la compra para reconocer repeticiones.
3. Un bloqueo inicial dura cinco minutos. Antes de cobrar, la API solicita por Kafka pasar a pago en proceso. La transición compite atómicamente con el vencimiento: solo un bloqueo todavía vigente puede pasar a pago, y los asientos en pago dejan de vencer automáticamente.
4. Axios solicita autorización con timeout de dos segundos. Un pago aprobado conduce a confirmar los asientos; solo después se publica `TicketPurchaseCompleted`. Un HTTP 422 produce `PaymentRejected` y liberación. Un resultado incierto produce `PaymentPendingReview` y conserva los asientos.
5. El modo demorado persiste aprobación antes de responder después de tres segundos. **Consultar resultado del pago** lee el resultado guardado, sin realizar otra autorización. Si no existe un resultado conocido, la compra permanece pendiente para revisión.
6. Boletos, notificación y analítica persisten sus efectos independientemente. La página consulta el estado y espera la proyección de `TicketsIssued`. Si se pierde la respuesta HTTP al crear una compra, **Recuperar solicitud pendiente** reenvía los datos y la clave originales, incluso después de recargar la página.

Cada transición y sus eventos pendientes se guardan juntos en el mismo documento. Un despachador publica esa bandeja de salida (_outbox_) y marca su entrega después del envío. Si el proceso cae entre ambos pasos, puede publicar otra vez: la entrega es **al menos una vez**. El consumidor confirma offsets después de persistir sus efectos, de modo que un reinicio puede repetir una entrega sin repetir el efecto.

Idempotencia en cinco líneas:

1. La clave HTTP vincula una compra con sus datos; reutilizarla con datos diferentes devuelve conflicto.
2. El resultado de cada solicitud de asiento se conserva por compra dentro del documento de la función.
3. El pago tiene identidad única por `purchaseId` y conserva su resultado.
4. Cada consumidor persiste su efecto con una identidad determinista de compra o boleto.
5. La repetición de eventos y publicaciones pendientes no crea nuevos efectos persistidos.

## API y colección Postman

Importa [postman.json](postman.json). Sus variables incluyen `baseUrl`, `showtimeId`, `purchaseId` y claves de idempotencia. Ejecuta primero la lista de funciones y ajusta los asientos si ya están vendidos. Las solicitudes de competencia deben enviarse en paralelo desde dos pestañas; un Collection Runner secuencial no demuestra concurrencia.

| Método | Ruta                                                       | Uso                                        |
| ------ | ---------------------------------------------------------- | ------------------------------------------ |
| GET    | `/showtimes`                                               | Funciones a través de seat-service         |
| GET    | `/showtimes/:id/seats`                                     | Disponibilidad actual                      |
| POST   | `/ticket-purchases`                                        | Crear compra; encabezado `Idempotency-Key` |
| GET    | `/ticket-purchases/:id`                                    | Estado, historial y boletos                |
| POST   | `/ticket-purchases/:id/reconcile-payment`                  | Consultar resultado incierto               |
| POST   | `http://localhost:3002/payment-authorizations`             | Autorización interna simulada              |
| GET    | `http://localhost:3002/payment-authorizations/:purchaseId` | Resultado interno persistido               |

Ejemplo de compra:

```json
{
  "customer": { "name": "Ana", "email": "ana@example.com" },
  "showtimeId": "showtime-1",
  "seatIds": ["F8", "F9"],
  "paymentMode": "approved"
}
```

## Guion de demostración

1. **Éxito:** selecciona dos asientos disponibles con pago aprobado. Observa historial y boletos, el evento de compra en Kafka UI y los registros de notificación y analítica en Mongo Express.
2. **Competencia:** envía casi simultáneamente dos compras con un asiento compartido y otro distinto. Solo una obtiene todo su conjunto; la otra no retiene su asiento exclusivo. Consulta disponibilidad.
3. **Duplicados:** reenvía una solicitud con la misma clave y cuerpo. Obtén el mismo identificador. Cambia el cuerpo conservando la clave y verifica el conflicto. Las pruebas de integración también repiten eventos.
4. **Rechazo:** usa un conjunto libre y pago rechazado. Espera el estado rechazado y comprueba que vuelve a estar disponible.
5. **Timeout:** usa otros asientos y pago demorado. Observa el estado pendiente y asientos protegidos. Pulsa **Consultar resultado del pago** y espera boletos; la autorización original se reutiliza.
6. **Consumidor detenido:** inicia los servicios por separado, detén únicamente `analytics-service`, completa una compra y vuelve a iniciarlo. Su grupo recupera el evento pendiente. Los demás servicios continúan trabajando.
7. **Recuperación y vencimiento:** las pruebas de integración cubren la recuperación de publicaciones pendientes y la carrera entre vencimiento e inicio del pago; `HOLD_MS` permite acortar el bloqueo en un entorno de prueba.

Para ejecutar servicios por separado, sustituye `npm start` por estos comandos, cada uno en una terminal dentro de `cinema`:

```sh
npm run start:ticket-purchase-api
npm run start:seat-service
npm run start:payment-service
npm run start:ticket-service
npm run start:notification-service
npm run start:analytics-service
```

No ejecutes simultáneamente el iniciador conjunto y esas mismas instancias individuales en los mismos puertos.

## Validación

```sh
npm test
npm run test:integration
npm run test:browser
```

Las pruebas de integración y de navegador requieren MongoDB, Kafka y los seis servicios en ejecución (`npm start` en otra terminal). Crean funciones aisladas con identificadores aleatorios, sin reutilizar los asientos de la demo. Las pruebas de navegador requieren Chromium de Playwright (`npx playwright install chromium`).

## Límites de la demostración

Los documentos por función y sus registros de deduplicación crecen con las compras: este modelo está diseñado para un conjunto acotado de demostración y requiere una política de archivo antes de escalar. Tampoco ofrece autenticación, cobros reales, envío real de correo ni tolerancia a la pérdida de un broker.

El Compose de MongoDB conserva un volumen; **el Compose de Kafka no tiene volumen persistente**. Reiniciar un consumidor conserva el log y los offsets del broker; eliminar o recrear el contenedor del broker puede perder ambos. `npm run setup` recrea tópicos vacíos pero no restaura eventos ya entregados y retirados de las bandejas de salida. No confundir estos escenarios en la demo. Los Compose existentes se mantienen sin cambios.

Si ya existe `curso-mongodb` de otra clase, verifica sus credenciales y puerto y usa `docker start curso-mongodb`; no elimines el contenedor ni su volumen para resolver un conflicto de nombres.

Referencias: [atomicidad de documentos MongoDB](https://www.mongodb.com/docs/manual/core/write-operations-atomicity/) y [consumo y offsets en KafkaJS](https://kafka.js.org/docs/consuming). KafkaJS 2.2.4 puede mostrar `TimeoutNegativeWarning` al conectarse bajo Node.js 24; no impide estas pruebas.
