# Метрики Kafka-команд

Метрики работают с MeterRegistry хост-приложения. Actuator и внешний сборщик необязательны.
При отсутствии единственного/Primary MeterRegistry библиотека использует приватный SimpleMeterRegistry.
Данные такого fallback доступны в памяти процесса, но автоматически наружу не экспортируются.
Существующие registry и их настройки принадлежат хосту.

## Выполнение команд

| Имя Micrometer | Тип | Метки | Значение |
|---|---|---|---|
| `lwm2m.commands.started` | Counter | — | Локальные команды, переданные в use case |
| `lwm2m.commands.inflight` | Gauge | — | Занятые слоты лимита Kafka-команд |
| `lwm2m.commands.completed` | Counter | operation, outcome | Завершения use case |
| `lwm2m.commands.duration` | Timer | operation, outcome | Длительность выполнения use case |
| `lwm2m.commands.errors` | Counter | operation, code | Ошибки выполнения, включая отказы устройства |
| `lwm2m.commands.timeouts` | Counter | — | Общее число результатов с кодом TIMEOUT |
| `lwm2m.commands.skipped` | Counter | — | Команды без локальной регистрации |
| `lwm2m.commands.invalid` | Counter | — | Некорректные envelope |

operation — имя операции, например READ, EXECUTE, READ_COMPOSITE. outcome:

- `success` — успешное завершение;
- `error` — структурированная ошибка, например TIMEOUT, DEVICE_SLEEPING, COMMAND_EXPIRED;
- `device_error` — получен ответ устройства с response.success=false.

code — имя ErrorCode из модели команд. Для отказа самого устройства используется ограниченная
метка DEVICE_RESPONSE_ERROR. Исключение, выброшенное пользовательским use case либо его future,
сохраняет существующую нормализацию в INTERNAL_ERROR.

Timer начинается непосредственно перед вызовом ExecuteDeviceCommand и заканчивается по завершению
его future. Время ожидания сообщения в Kafka и публикации результата не входит в эту длительность.
Если ответ пришёл во время медленного commit, timer остановится до завершения commit. Если ошибка
получена на проверке срока или сна, она также входит в completed/errors и обычно имеет малую длительность.

inflight показывает занятые слоты, которые освобождаются после попытки commit и передачи результата
publisher. Поэтому при задержке commit число занятых слотов может временно отличаться от числа
use cases, ещё ожидающих ответ. Метрики не изменяют порядок send → commit и не дожидаются Kafka ack
публикации результата.

Invalid и skipped не считаются выполненными командами, тайм-аутами либо ошибками выполнения.
Ошибка публикации результата не превращает успешную операцию устройства в ошибочную — для Kafka
publisher существуют отдельные метрики ниже. Лимит и эти execution-метрики относятся к Kafka;
HTTP-путь не инструментируется ими.

## Паузы и состояние consumer

| Имя Micrometer | Тип | Значение |
|---|---|---|
| `lwm2m.commands.consumer.paused` | Gauge | 1 при паузе на лимите и назначенных partitions, иначе 0 |
| `lwm2m.commands.consumer.pauses` | Counter | Число начавшихся интервалов паузы |
| `lwm2m.commands.consumer.pause.duration` | Timer | Длительности завершённых интервалов паузы |
| `lwm2m.commands.consumer.pause.active` | LongTaskTimer | Число активных пауз и длительность текущей |
| `lwm2m.commands.consumer.failures` | Counter | Ошибки, остановившие consumer |

Во время pause consumer продолжает poll, а LongTaskTimer показывает длительность ещё не завершённой
паузы. Это позволяет видеть зависшую паузу до первого resume. Обычный Timer получает запись при resume,
остановке consumer либо отзыве всех partitions. Повторные poll и добавление partitions в уже идущую
паузу не увеличивают число интервалов. После полного отзыва и нового назначения начинается новый интервал.
Остановка consumer закрывает активную паузу без ожидания ответа оставшихся запросов.

## Публикация

- `lwm2m.commands.publication.succeeded` — Kafka подтвердила результат/invalid-сообщение;
- `lwm2m.commands.publication.failed` — публикация завершилась ошибкой;
- `lwm2m.commands.publication.dropped` — результат отброшен из-за заполнения/остановки буфера;
- `lwm2m.events.published`, `.failed`, `.dropped` — те же показатели для Observe/Send.

Ни commandId, ни endpoint, ни текст исключения или payload не используются в метках. Набор
operation/outcome/code ограничен моделью, поэтому число рядов не растёт вместе с числом устройств.
Счётчики completed/errors и execution timers с метками появляются после первого подходящего события.

## Actuator

В standalone уже открыт `/actuator/metrics`. Для хоста со starter-actuator можно явно включить:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,metrics
```

Примеры:

```text
GET /actuator/metrics/lwm2m.commands.inflight
GET /actuator/metrics/lwm2m.commands.timeouts
GET /actuator/metrics/lwm2m.commands.errors?tag=operation:READ&tag=code:TIMEOUT
GET /actuator/metrics/lwm2m.commands.duration?tag=operation:READ&tag=outcome:success
GET /actuator/metrics/lwm2m.commands.consumer.paused
GET /actuator/metrics/lwm2m.commands.consumer.pause.active
```

Timer предоставляет count, total time и max. Для p95/агрегируемых histogram buckets настройки
задаются хостом через MeterRegistry или стандартные Spring Boot distribution properties:

```yaml
management:
  metrics:
    distribution:
      percentiles-histogram:
        lwm2m.commands.duration: true
        lwm2m.commands.consumer.pause.duration: true
```

Для Prometheus отдельно подключается micrometer-registry-prometheus и открывается actuator/prometheus.
Библиотека сохраняет выбор экспортера и политику экспозиции endpoints за хостом.
