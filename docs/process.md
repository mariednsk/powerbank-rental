# Главный процесс: запуск аренды пауэрбанка

**Проект:** Powerbank Rental Platform  
**Статус:** архитектурный проект, часть 1 (без реализации)  
**Ответственный:** участник 3 — главный процесс и отказоустойчивость

## 1. Цель и границы процесса

Пользователь выбирает станцию и нажимает «Арендовать». Система должна зарезервировать **конкретный** пауэрбанк, авторизовать депозит (удержать сумму, а не списать её окончательно), отправить команду станции на выдачу и подтвердить физическую выдачу. **Аренда считается начатой только после подтверждённой физической выдачи устройства**, а не после отправки команды станции.

В процессе участвуют:

| Компонент | Ответственность в этом процессе |
|---|---|
| **Client / API Gateway** | Принимает запрос пользователя и возвращает статус процесса. |
| **Rental Service** | Владеет арендой, её статусом и шагами Saga; оркестратор. |
| **Inventory Service** | Атомарно резервирует конкретное устройство и снимает резерв. |
| **Payment Service** | Авторизует депозит и отменяет авторизацию при неуспешной аренде. |
| **Station Gateway** | Доставляет команду физической станции и получает подтверждение фактической выдачи. |
| **Physical Station** | Выдаёт устройство и сообщает результат по `commandId`. |
| **Event Broker (Kafka)** | Доставляет события состояния другим сервисам; не является источником истины. |

Вне границ диаграмм: регистрация пользователя, окончательный расчёт аренды после возврата, уведомления. Эти процессы могут быть описаны отдельно. Общую модель имен сервисов и команд надо синхронизировать с авторами `context-map.md`, `c4.md`, `openapi.yaml`, `asyncapi.yaml`.

## 2. Состояния и инварианты

### Состояния Rental

```mermaid
stateDiagram-v2
    [*] --> PENDING
    PENDING --> RESERVED: Inventory.reserve OK
    RESERVED --> AUTHORIZED: Payment.authorize OK
    AUTHORIZED --> DISPENSING: Выдача запрошена
    DISPENSING --> ACTIVE: Физическая выдача подтверждена
    PENDING --> CANCELLING: Нет доступного устройства
    RESERVED --> CANCELLING: Платёж отклонён
    AUTHORIZED --> CANCELLING: Выдача точно не произошла
    DISPENSING --> RECONCILING: Результат выдачи неизвестен
    RECONCILING --> ACTIVE: Выдача подтверждена
    RECONCILING --> CANCELLING: Подтверждено, что выдачи не было
    CANCELLING --> CANCELLED: Компенсации завершены
    CANCELLING --> COMPENSATION_PENDING: Временный сбой компенсации
    COMPENSATION_PENDING --> CANCELLED: Повтор завершился успешно
    ACTIVE --> [*]
    CANCELLED --> [*]
```

`COMPENSATION_PENDING` — технический статус, означающий, что пользовательская операция не состоялась, но очистка резерва и/или отмена удержания ещё выполняется. **Нельзя показывать `CANCELLED` до подтверждения требуемых компенсаций.**

### Инварианты

1. **Один пауэрбанк — максимум одна активная бронь или аренда.** Это гарантирует Inventory Service атомарной операцией с блокировкой строки или условным обновлением в собственной БД; проверка только на стороне Rental недостаточна.
2. **Одна аренда — не более одной авторизации депозита.** Payment Service обрабатывает повторные команды с тем же ключом идемпотентности и хранит `providerPaymentId`.
3. **Одна команда на выдачу — один логический результат.** Используется устойчивый `commandId`; при повторе нельзя физически выдать второе устройство.
4. **Аренда `ACTIVE` только после положительного подтверждения фактической выдачи от станции** (`Dispensed`), а не после HTTP `200` от шлюза.
5. **При неизвестном результате выдачи не освобождаем бронь и не отменяем авторизацию вслепую.** Сначала сверяем состояние станции по `commandId` и физический статус ячейки.
6. **Публикация события не может потеряться после записи состояния.** Для изменения состояния и исходящего события применяется Transactional Outbox в той же локальной транзакции.

### Идентификаторы и таймауты

| Идентификатор | Для чего |
|---|---|
| `rentalId` | Основной сквозной идентификатор процесса. |
| `idempotencyKey` | Повтор пользовательского запроса создания аренды возвращает прежнюю операцию. |
| `reservationId` | Резерв конкретного устройства. |
| `paymentOperationId` | Идемпотентная авторизация депозита / её отмена. |
| `commandId` | Идемпотентная физическая команда выдачи. |
| `eventId` | Дедупликация событий в обработчиках. |

Конкретные длительности таймаутов и TTL брони — конфигурация, а не зафиксированные в архитектуре числа. TTL **не должен автоматически возвращать устройство в `AVAILABLE`, пока выдача имеет неизвестный исход**.

## 3. Общий алгоритм: Saga orchestration

Rental Service сохраняет текущий шаг и статус в своей БД. Для каждого следующего действия отправляется команда соответствующему сервису. Если шаг не выполнен, оркестратор запускает обратные действия; каждый шаг и компенсация идемпотентны.

| Шаг | Действие | Компенсация |
|---|---|---|
| 1 | Inventory: зарезервировать устройство | Inventory: снять резерв |
| 2 | Payment: авторизовать депозит | Payment: отменить авторизацию (`void`) |
| 3 | Station: выдать устройство | Если выдачи **точно не было** — отменить авторизацию и бронь; если уже была — нельзя считать аренду несостоявшейся, перейти к `ACTIVE` и разбирать фактическую аренду отдельно |

**Важно:** `void` отменяет неисполненную авторизацию; это не `refund` уже списанного платежа. При иной модели провайдера компенсацию нужно согласовать с его API.

## 4. Sequence-диаграмма № 1 — успешный запуск аренды


```mermaid
sequenceDiagram
    autonumber
    actor U as Пользователь
    participant C as Client/API Gateway
    participant R as Rental Service
    participant I as Inventory Service
    participant P as Payment Service
    participant G as Station Gateway
    participant S as Физическая станция
    participant K as Kafka

    U->>C: POST /rentals (Idempotency-Key)
    C->>R: createRental(userId, stationId, key)
    R->>R: Создать PENDING и шаг Saga

    R->>I: reserve(stationId, rentalId)
    I->>I: Атомарно закрепить powerbankId
    I-->>R: Reserved(reservationId, powerbankId)
    R->>R: Сохранить RESERVED

    R->>P: authorize(rentalId, amount, paymentOperationId)
    P-->>R: Authorized(providerPaymentId)
    R->>R: Сохранить AUTHORIZED

    R->>G: dispense(commandId, powerbankId)
    G->>S: Выдать устройство по commandId
    S-->>G: Dispensed(commandId, powerbankId)

    Note over G,R: Ответ потерян, результат выдачи неизвестен
    G-->>R: Timeout
    R->>R: Сохранить DISPENSE_UNKNOWN

    Note over R,G: Проверка фактического состояния
    R->>G: getDispenseStatus(commandId)
    G->>S: Проверить состояние выдачи
    S-->>G: NOT_DISPENSED
    G-->>R: NOT_DISPENSED

    Note over R,P: Запуск компенсации
    R->>P: voidAuthorization(paymentOperationId)
    P-->>R: AuthorizationVoided

    R->>I: releaseReservation(reservationId)
    I-->>R: ReservationReleased

    R->>R: Сохранить CANCELLED
    R-->>K: RentalCancelled (через Outbox)
    R-->>C: rentalId, status=CANCELLED
    C-->>U: Аренда отменена, блокировка средств снята
```

Депозит авторизован, но не списан окончательно. После возврата устройства отдельный процесс рассчитает стоимость и произведёт списание/освобождение остатка — не часть этой Saga.

## 5. Sequence-диаграмма № 2 — сбой: устройство уже занято

**Что сломалось:** свободное устройство было занято другим запросом до резервирования либо все устройства станции недоступны. **До оплаты процесс не дошёл.**

```mermaid
sequenceDiagram
    autonumber
    actor U as Пользователь
    participant R as Rental Service
    participant I as Inventory Service
    participant K as Kafka

    U->>R: Создать аренду
    R->>R: PENDING
    R->>I: reserve(stationId, rentalId)
    I->>I: Условное обновление / блокировка
    I-->>R: ReservationRejected(NO_AVAILABLE_DEVICE)
    R->>R: CANCELLING → CANCELLED
    R-->>U: Нет доступных пауэрбанков
    R-->>K: RentalCancelled (через Outbox)
```

**Компенсация:** не требуется: ни резерв, ни платёж не созданы. Повторный запрос с тем же `idempotencyKey` не запускает новую аренду; новая попытка — с новым ключом. **Гарантия от двойной брони находится в Inventory**, а не в диаграмме.

## 6. Sequence-диаграмма № 3 — сбой: авторизация депозита отклонена

**Что сломалось:** резерв получен, но платёжный провайдер однозначно отклонил авторизацию. Требуется снять резерв.

```mermaid
sequenceDiagram
    autonumber
    actor U as Пользователь
    participant R as Rental Service
    participant I as Inventory Service
    participant P as Payment Service
    participant K as Kafka

    U->>R: Создать аренду
    R->>I: reserve(rentalId)
    I-->>R: Reserved(reservationId)
    R->>R: RESERVED
    R->>P: authorize(rentalId, paymentOperationId)
    P-->>R: Declined (окончательный отказ)
    R->>R: CANCELLING
    R->>I: release(reservationId, rentalId)
    I-->>R: Released
    R->>R: CANCELLED
    R-->>U: Не удалось авторизовать депозит
    R-->>K: RentalCancelled (через Outbox)
```

**Компенсация:** `Inventory.release`. Если Inventory временно не отвечает, Rental сохраняет `COMPENSATION_PENDING`, повторяет **ту же** команду освобождения, а не создаёт новую. Для `Payment.authorize` timeout нельзя считать окончательным отказом: сначала проверить статус `paymentOperationId` у Payment Service/провайдера, иначе можно освободить устройство при существующем удержании.

## 7. Sequence-диаграмма № 4 — сбой: результат физической выдачи неизвестен

**Что сломалось:** деньги авторизованы, команда выдачи отправлена, но Station Gateway потерял связь со станцией / ответ не дошёл. Это **не то же самое**, что «станция точно не выдала устройство». Вариант ниже показывает сверку, по результатам которой достоверно установлено: устройство **не выдано**.

```mermaid
sequenceDiagram
    autonumber
    actor U as Пользователь
    participant C as Client/API Gateway
    participant R as Rental Service
    participant I as Inventory Service
    participant P as Payment Service
    participant G as Station Gateway
    participant S as Физическая станция
    participant K as Kafka

    U->>C: POST /rentals
    C->>R: createRental(userId, stationId, key)
    R->>R: Создать PENDING

    R->>I: reserve(stationId, rentalId)
    I-->>R: Reserved(reservationId, powerbankId)
    R->>R: Сохранить RESERVED

    R->>P: authorize(rentalId, amount, paymentOperationId)
    P-->>R: Authorized(providerPaymentId)
    R->>R: Сохранить AUTHORIZED

    R->>G: dispense(commandId, powerbankId)
    G->>S: Команда выдачи устройства
    Note over G,S: Соединение потеряно, результат неизвестен
    G-->>R: Timeout
    R->>R: Сохранить DISPENSE_UNKNOWN

    Note over R,G: Проверка фактического состояния
    R->>G: getDispenseStatus(commandId)
    G->>S: Проверить результат выдачи
    S-->>G: NOT_DISPENSED
    G-->>R: NOT_DISPENSED

    Note over R,P: Устройство не выдано, запускаем компенсацию
    R->>P: voidAuthorization(paymentOperationId)
    P-->>R: AuthorizationVoided

    R->>I: releaseReservation(reservationId)
    I-->>R: ReservationReleased

    R->>R: Сохранить CANCELLED
    R-->>K: RentalCancelled через Outbox
    R-->>C: status=CANCELLED
    C-->>U: Аренда отменена, блокировка средств снята
```

**Компенсация:** сначала `Payment.voidAuthorization`, затем `Inventory.release`. Если один шаг недоступен, сохраняем `COMPENSATION_PENDING` и повторяем до подтверждения. Если сверка покажет `DISPENSED`, Rental переключает аренду в `ACTIVE` и **не отменяет депозит**. Если станция остаётся недоступной, результат остаётся `RECONCILING`: нельзя обещать отмену или освобождать резерв по одному таймауту. Случаи физической потери/спорного состояния устройства требуют ручного разбора.

## 8. Общие отказы: что отвечать на защите

| Отказ | Реакция | Что гарантируем |
|---|---|---|
| Rental Service упал между шагами | После восстановления читает сохранённые шаги Saga и продолжает; команды повторяются с прежними ключами. | Не создаём вторую аренду / платёж при восстановлении. |
| Inventory упал до ответа на `reserve` | Rental сверяет результат по `rentalId`/ключу операции, а не предполагает отказ. | Не резервируем дважды; после восстановления определяем исход. |
| Payment упал после авторизации, до ответа | Запрос статуса по `paymentOperationId`, повтор с тем же ключом. | Не авторизуем депозит второй раз. |
| Station Gateway или станция недоступны | `RECONCILING`, опрос результата `commandId`; компенсация только при подтверждённой невыдаче. | Не выдаём потенциально занятый пауэрбанк другому пользователю. |
| Kafka недоступна | Бизнес-статус и событие записываются в локальную БД/Outbox; публикация будет повторена. | Событие не теряется из-за временного простоя брокера. |
| Kafka доставила событие повторно | Consumer проверяет `eventId` в Inbox в одной транзакции с бизнес-эффектом. | Не обрабатываем одно событие дважды в рамках локального эффекта. |
| Компенсация не прошла | `COMPENSATION_PENDING`, повтор идемпотентной команды + мониторинг. | Не объявляем успешную отмену раньше подтверждения. |

### Ограничения гарантий

«Exactly once» между независимыми сервисами и физическим оборудованием **не предполагается**. Архитектура использует доставку *at least once*, идемпотентные операции и сверку внешних результатов. Физическую недвойную выдачу должно поддерживать само оборудование или шлюз (журнал обработанных `commandId` и защита от повторного открытия); без этого обещать строгую гарантию нельзя.

