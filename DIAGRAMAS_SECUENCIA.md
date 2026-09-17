# Flujo completo de la aplicación de cine

Estos diagramas describen la implementación de [la aplicación](README.md). Se pueden visualizar en un lector Markdown con soporte para Mermaid. El flujo exitoso se divide en tres diagramas consecutivos; los siguientes muestran sus alternativas y la recuperación ante fallos.

## Cómo leerlos

Las flechas `->>` representan solicitudes HTTP u operaciones de persistencia; `-->>`, respuestas; y `-)`, envío o entrega de eventos asíncronos. Cada servicio accede únicamente a su propia base lógica de MongoDB, aunque todas residen en la misma instancia.

Las publicaciones Kafka dibujadas desde un servicio las realiza su despachador de eventos pendientes (**outbox**). Primero se persisten el estado y el evento en el mismo documento, y después se publica. El diagrama de recuperación detalla este mecanismo.

| Alias en los diagramas  | Tópico Kafka                | Key          |
| ----------------------- | --------------------------- | ------------ |
| Solicitudes de asientos | `cinema.seat-hold-requests` | `showtimeId` |
| Respuestas de asientos  | `cinema.seat-hold-events`   | `showtimeId` |
| Eventos de compra       | `cinema.purchase-events`    | `purchaseId` |
| Eventos de boletos      | `cinema.ticket-events`      | `purchaseId` |

Todos los eventos contienen `eventId`, `eventType`, `occurredAt` y `data`. El campo `data.purchaseId` permite seguir una compra entre servicios. Los tópicos tienen tres particiones y factor de replicación uno.

## 1. Selección y registro de la compra

Consultar disponibilidad no reserva asientos: la exclusividad se decide después, cuando `seat-service` procesa la solicitud de bloqueo.

```mermaid
sequenceDiagram
    autonumber
    actor U as Cliente
    participant W as Navegador
    participant A as ticket-purchase-api :3000
    participant S as seat-service :3001
    participant DS as MongoDB cinema_seats
    participant DA as MongoDB cinema_purchases
    participant K as Kafka - Solicitudes de asientos

    U->>W: Abrir la aplicación
    W->>A: GET /
    A-->>W: HTML, CSS y JavaScript
    W->>A: GET /showtimes
    A->>S: GET /showtimes (Axios)
    S->>DS: Consultar funciones
    DS-->>S: Funciones y precios
    S-->>A: Funciones
    A-->>W: Funciones
    U->>W: Elegir función
    W->>A: GET /showtimes/:id/seats
    A->>S: GET /showtimes/:id/seats
    S->>DS: Aplicar vencimientos y leer disponibilidad
    DS-->>S: Asientos y estados
    S-->>A: Disponibilidad y precio
    A-->>W: Disponibilidad y precio
    U->>W: Elegir F8/F9, cliente y modo de pago
    W->>A: POST /ticket-purchases + Idempotency-Key
    A->>A: Validar datos y normalizar asientos
    A->>DA: Buscar clave de idempotencia
    alt Clave existente con los mismos datos
        DA-->>A: Compra original
        A-->>W: 202 con el mismo purchaseId
    else Clave existente con datos distintos
        DA-->>A: Datos incompatibles
        A-->>W: 409 Conflict
    else Clave nueva
        A->>S: GET /showtimes/:id/seats
        S-->>A: Asientos existentes y precio del servidor
        A->>A: Calcular importe = precio por cantidad
        A->>DA: Insertar compra requested, historial y outbox SeatHoldRequested
        DA-->>A: Compra persistida con clave única
        A-->>W: 202 Accepted con purchaseId
        A-)K: SeatHoldRequested con key showtimeId
    end
    Note over W,K: 202 confirma el registro, todavía no la compra ni el bloqueo
    Note over A,K: La publicación y la respuesta HTTP son independientes tras persistir
```

Un cuerpo inválido o una clave ausente devuelve `400`. Una carrera entre dos solicitudes con la misma clave se resuelve mediante el índice único: se devuelve la compra existente o `409` si los datos difieren.

## 2. Bloqueo, pago aprobado y confirmación

Continúa la compra nueva del diagrama anterior. El bloqueo inicial dura cinco minutos. La API no solicita el pago hasta recibir `SeatPaymentAccepted`.

```mermaid
sequenceDiagram
    autonumber
    participant A as ticket-purchase-api
    participant DA as MongoDB cinema_purchases
    participant K as Kafka - Solicitudes y respuestas de asientos
    participant S as seat-service
    participant DS as MongoDB cinema_seats
    participant P as payment-service :3002
    participant DP as MongoDB cinema_payments
    participant C as Kafka - Eventos de compra

    K-)S: SeatHoldRequested con F8/F9
    S->>DS: Leer documento de función y revisión
    S->>DS: Reemplazo condicional por revisión<br/>F8/F9 held + resultado por compra + outbox
    DS-->>S: Bloqueo completo aceptado
    S-)K: SeatHoldAccepted
    K-)A: SeatHoldAccepted
    A->>DA: requested a held + historial + outbox SeatPaymentRequested
    A-)K: SeatPaymentRequested
    K-)S: SeatPaymentRequested
    S->>DS: Comprobar vigencia y cambiar held a paying<br/>Guardar SeatPaymentAccepted en outbox
    DS-->>S: Transición atómica aceptada
    Note over S,DS: Los asientos paying ya no vencen automáticamente
    S-)K: SeatPaymentAccepted
    K-)A: SeatPaymentAccepted
    A->>DA: held a paying + historial
    A->>P: POST /payment-authorizations<br/>purchaseId, importe, moneda y modo<br/>Axios timeout 2 segundos
    P->>DP: Crear autorización idempotente por purchaseId
    DP-->>P: Resultado approved persistido
    P-->>A: HTTP 200 approved
    A->>DA: paying a confirming + outbox SeatConfirmRequested
    A-)K: SeatConfirmRequested
    K-)S: SeatConfirmRequested
    S->>DS: paying a sold para todo el conjunto<br/>Guardar SeatConfirmed en outbox
    S-)K: SeatConfirmed
    K-)A: SeatConfirmed
    A->>DA: confirming a completed + historial<br/>Guardar TicketPurchaseCompleted en outbox
    A-)C: TicketPurchaseCompleted
    Note over A,C: La compra se completa después de confirmar los asientos
```

## 3. Boletos, notificación, analítica y seguimiento

Los tres consumidores de `TicketPurchaseCompleted` utilizan grupos distintos. No existe un orden global entre sus efectos: la notificación y la analítica no esperan a que se emitan los boletos.

```mermaid
sequenceDiagram
    autonumber
    participant C as Kafka - Eventos de compra
    participant T as ticket-service
    participant DT as MongoDB cinema_tickets
    participant N as notification-service
    participant DN as MongoDB cinema_notifications
    participant G as analytics-service
    participant DG as MongoDB cinema_analytics
    participant B as Kafka - Eventos de boletos
    participant A as ticket-purchase-api
    participant DA as MongoDB cinema_purchases
    participant W as Navegador

    par Grupo ticket-service
        C-)T: TicketPurchaseCompleted
        T->>DT: Insertar lote único por purchaseId<br/>Boletos y outbox TicketsIssued juntos
        DT-->>T: Efecto persistido o ya existente
        T->>C: Confirmar offset después de persistir
        T-)B: TicketsIssued
        B-)A: TicketsIssued
        A->>DA: Guardar proyección de boletos en la compra
        A->>B: Confirmar offset
    and Grupo notification-service
        C-)N: TicketPurchaseCompleted
        N->>DN: Insertar confirmación simulada única por purchaseId
        DN-->>N: Efecto persistido o ya existente
        N->>C: Confirmar offset
    and Grupo analytics-service
        C-)G: TicketPurchaseCompleted
        G->>DG: Insertar compra única con importe y cantidad de boletos
        G->>DG: Agregar compras, boletos e ingresos
        DG-->>G: Totales para el log
        G->>C: Confirmar offset
    end
    loop Cada 1.5 segundos mientras la página está abierta
        W->>A: GET /ticket-purchases/:id
        A->>DA: Leer estado, historial y proyección de boletos
        DA-->>A: Compra
        A-->>W: Compra con boletos emitidos cuando estén disponibles
    end
    Note over W,DA: completed puede aparecer antes que los boletos<br/>La página continúa consultando
```

La página también actualiza la disponibilidad cada 2.5 segundos mediante la API y `seat-service`.

## 4. Competencia por asientos: todos o ninguno

Dos compras comparten F9. Aquí A gana; si B se procesa primero, el resultado se invierte. Kafka agrupa las solicitudes de una función por key, pero la garantía de exclusividad reside en la actualización condicional de MongoDB.

```mermaid
sequenceDiagram
    autonumber
    actor U1 as Cliente A
    actor U2 as Cliente B
    participant A as ticket-purchase-api
    participant K as Kafka - Solicitudes de asientos
    participant S as seat-service
    participant D as MongoDB cinema_seats
    participant R as Kafka - Respuestas de asientos

    par Compra A
        U1->>A: Comprar F8/F9 con clave A
        A-->>U1: 202 con purchaseId A
        A-)K: SeatHoldRequested A, misma showtimeId
    and Compra B
        U2->>A: Comprar F9/F10 con clave B
        A-->>U2: 202 con purchaseId B
        A-)K: SeatHoldRequested B, misma showtimeId
    end
    K-)S: Solicitud A con F8/F9
    S->>D: Cambiar F8/F9 a held en una operación condicional
    D-->>S: Aceptada, resultado A y outbox persistidos
    S-)R: SeatHoldAccepted A
    K-)S: Solicitud B con F9/F10
    S->>D: Leer función y evaluar conjunto completo
    D-->>S: F9 ya no está disponible
    S->>D: Persistir rechazo B y outbox, sin bloquear F10
    S-)R: SeatHoldRejected B
    R-)A: SeatHoldAccepted A
    A->>A: Continuar al inicio del pago de A
    R-)A: SeatHoldRejected B
    A->>A: Persistir compra B como rejected
    U2->>A: GET /ticket-purchases/B
    A-->>U2: rejected, asientos no disponibles
    Note over S,D: Si cambia la revisión entre lectura y escritura,<br/>se relee y se vuelve a evaluar todo el conjunto
    Note over A,D: B no se cobra y F10 sigue libre
```

## 5. Pago rechazado y liberación

Este escenario comienza después de que los asientos pasan a `paying`. La API no muestra el rechazo definitivo hasta recibir la confirmación de liberación.

```mermaid
sequenceDiagram
    autonumber
    participant A as ticket-purchase-api
    participant DA as MongoDB cinema_purchases
    participant P as payment-service
    participant DP as MongoDB cinema_payments
    participant C as Kafka - Eventos de compra
    participant K as Kafka - Solicitudes y respuestas de asientos
    participant S as seat-service
    participant DS as MongoDB cinema_seats
    participant W as Navegador

    A->>P: POST /payment-authorizations con modo rejected
    P->>DP: Persistir autorización rejected por purchaseId
    P-->>A: HTTP 422
    A->>DA: paying a releasing + historial<br/>Outbox PaymentRejected y SeatReleaseRequested
    A-)C: PaymentRejected
    Note over C: Los consumidores de efectos finales ignoran este tipo<br/>Solo reaccionan a TicketPurchaseCompleted
    A-)K: SeatReleaseRequested
    K-)S: SeatReleaseRequested
    S->>DS: Liberar todo el conjunto de esta compra<br/>Guardar SeatReleased en outbox
    S-)K: SeatReleased
    K-)A: SeatReleased
    A->>DA: releasing a rejected + historial
    W->>A: Consultar compra y disponibilidad
    A-->>W: Compra rechazada y asientos disponibles
    Note over A,W: No se emiten boletos, confirmación ni ingresos
```

## 6. Timeout y consulta manual del resultado

Un timeout significa que la API desconoce el resultado, no que el pago haya sido rechazado. En el modo `delayed`, el stub guarda una aprobación inmediatamente y demora tres segundos la respuesta.

```mermaid
sequenceDiagram
    autonumber
    participant W as Navegador
    participant A as ticket-purchase-api
    participant DA as MongoDB cinema_purchases
    participant P as payment-service
    participant DP as MongoDB cinema_payments
    participant C as Kafka - Eventos de compra
    participant K as Kafka - Solicitudes de asientos

    Note over A,P: Los asientos ya están paying y protegidos
    A->>P: POST /payment-authorizations con modo delayed
    P->>DP: Persistir approved por purchaseId
    P->>P: Demorar respuesta 3 segundos
    A->>A: Axios vence a los 2 segundos
    A->>DA: paying a pending_review + historial<br/>Guardar PaymentPendingReview en outbox
    A-)C: PaymentPendingReview
    Note over A,P: La respuesta HTTP tardía no resuelve el timeout de Axios
    W->>A: GET /ticket-purchases/:id
    A-->>W: pending_review
    W->>A: POST /ticket-purchases/:id/reconcile-payment
    A->>P: GET /payment-authorizations/:purchaseId
    P->>DP: Leer autorización original, sin volver a cobrar
    DP-->>P: Resultado persistido o ausencia
    alt Resultado approved, como en el modo delayed
        P-->>A: HTTP 200 con approved
        A->>DA: pending_review a confirming<br/>Outbox SeatConfirmRequested
        A-->>W: 202 Accepted
        A-)K: SeatConfirmRequested
        Note over A,K: Continúa la confirmación del diagrama 2<br/>y los efectos del diagrama 3
    else Resultado rejected
        P-->>A: HTTP 200 con rejected
        A->>DA: pending_review a releasing<br/>Outbox PaymentRejected y SeatReleaseRequested
        A-->>W: 202 Accepted
        A-)C: PaymentRejected
        A-)K: SeatReleaseRequested
        Note over A,K: Continúa la liberación del diagrama 5
    else No existe autorización persistida
        P-->>A: HTTP 404
        A-->>W: 409, mantener revisión y asientos protegidos
    else El servicio no responde
        A->>A: Error o timeout de la consulta
        A-->>W: Error de servicio, mantener pending_review
    end
```

La consulta nunca hace otro `POST /payment-authorizations`. Si la API reinicia y encuentra compras en `paying`, las pasa a `pending_review`: un proceso interrumpido tampoco permite asumir el resultado del cobro. Una conciliación sobre una compra que ya no está pendiente devuelve `409`.

## 7. Vencimiento contra inicio del pago

El vencimiento se aplica periódicamente y también al consultar asientos o procesar comandos. Solo libera bloqueos `held`; la revisión del documento resuelve la carrera con el inicio del pago.

```mermaid
sequenceDiagram
    autonumber
    participant A as ticket-purchase-api
    participant K as Kafka - Solicitudes y respuestas de asientos
    participant S as seat-service
    participant D as MongoDB cinema_seats

    Note over S,D: F8/F9 están held, expiresAt = creación + 5 minutos
    A-)K: SeatPaymentRequested
    K-)S: Solicitar inicio del pago
    alt El vencimiento se persiste primero o el plazo ya terminó
        S->>D: held a expired, liberar F8/F9<br/>Outbox SeatHoldExpired
        S-)K: SeatHoldExpired
        S->>D: Releer bloqueo vencido y guardar rechazo de inicio
        S-)K: SeatPaymentRejected
        K-)A: SeatHoldExpired y SeatPaymentRejected
        A->>A: Persistir expired una sola vez, sin solicitar cobro
    else Inicio del pago válido se persiste primero
        S->>D: held a paying + outbox SeatPaymentAccepted
        S-)K: SeatPaymentAccepted
        K-)A: SeatPaymentAccepted
        A->>A: Persistir paying y solicitar pago HTTP
        S->>D: Ejecutar siguiente revisión de vencimientos
        D-->>S: Bloqueo paying, no liberar aunque pase el plazo
    end
    Note over S,D: Un reemplazo con revisión obsoleta falla<br/>El servicio relee antes de decidir
```

## 8. Publicaciones pendientes, duplicados y consumidores detenidos

No hay una transacción distribuida entre MongoDB y Kafka. La outbox permite reintentar publicaciones, y la identidad por compra evita repetir efectos persistidos.

```mermaid
sequenceDiagram
    autonumber
    participant P as Servicio productor
    participant DP as Su MongoDB
    participant O as Su despachador outbox
    participant K as Kafka
    participant C as Servicio consumidor
    participant DC as Su MongoDB

    P->>DP: Guardar cambio de estado y evento en un mismo documento
    Note over P,O: Si el productor cae antes de publicar,<br/>el evento permanece pendiente para el reinicio
    O->>DP: Leer eventos pendientes
    DP-->>O: Evento con eventId y purchaseId
    O-)K: Publicar evento
    K-->>O: Confirmación del broker
    alt El despachador continúa
        O->>DP: Retirar evento de outbox e incrementar revisión
    else Cae después de publicar y antes de retirarlo
        Note over O,DP: Al reiniciar, la outbox todavía contiene el evento
        O->>DP: Leer evento pendiente
        O-)K: Publicar otra vez el mismo eventId
        K-->>O: Confirmación del broker
        O->>DP: Retirar evento de outbox
    end
    alt Consumidor activo
        K-)C: Entregar evento
    else Consumidor detenido, por ejemplo analytics-service
        Note over K,C: Kafka conserva el evento mientras su retención lo permita
        C->>K: Reiniciar y unirse al mismo grupo estable
        K-)C: Entregar desde el offset confirmado
    end
    C->>DC: Persistir efecto idempotente por purchaseId
    DC-->>C: Efecto creado o previamente existente
    alt Continúa después de persistir
        C->>K: Confirmar offset siguiente
    else Cae antes de confirmar el offset
        C->>K: Reiniciar con el mismo grupo
        K-)C: Reentregar evento sin offset confirmado
        C->>DC: Reconocer efecto existente, sin duplicarlo
        C->>K: Confirmar offset siguiente
    end
```

El resultado es entrega **al menos una vez** con efectos persistidos idempotentes, no una promesa de entrega exactamente una vez. El mismo principio permite recuperar `TicketsIssued` cuando el servicio de boletos reinicia con una publicación pendiente.

El Compose de Kafka no tiene volumen persistente. Estos diagramas de recuperación suponen que el broker conserva sus mensajes y offsets: recrearlo puede perderlos y no equivale a reiniciar un consumidor.
