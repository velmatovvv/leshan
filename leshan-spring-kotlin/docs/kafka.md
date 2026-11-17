# Асинхронные команды Kafka

## Включение и fan-out

```bash
export KAFKA_COMMANDS_ENABLED=true
export LWM2M_SERVER_ID=node-A
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_COMMAND_MAX_IN_FLIGHT=100
java -jar server/target/server-0.1.0-SNAPSHOT.jar
```

На другом сервере задайте другой стабильный LWM2M_SERVER_ID, например node-B.
По умолчанию group ID — `lwm2m-server-<serverId>`. Одинаковый group ID разделит сообщения между
серверами вместо fan-out. Допускается явно задать KAFKA_COMMAND_GROUP_ID.

Регистрационные stores независимы. Команду выполняет каждый сервер, в собственном store которого
существует endpoint. Остальные коммитят offset без результата. Если устройство отсутствует везде,
отправитель завершает ожидание по своему тайм-ауту. Для единственного выполнения endpoint должен
быть зарегистрирован только на одном сервере.

Новые группы по умолчанию начинают с `latest`, существующие — с сохранённого offset.
Поведение новой группы меняется через `spring.kafka.consumer.auto-offset-reset`.
Топики создаются инфраструктурой:

| Топик | Назначение | Key |
|---|---|---|
| `lwm2m.commands` | Входные команды | endpoint, обязателен |
| `lwm2m.command-results` | Конечные результаты | endpoint |
| `lwm2m.commands.invalid` | Некорректные сообщения | исходный key, может отсутствовать |
| `lwm2m.observations` | Значения и события Observe | endpoint |
| `lwm2m.send` | Данные Send | endpoint |

Названия командных топиков: KAFKA_COMMAND_TOPIC, KAFKA_COMMAND_RESULT_TOPIC, KAFKA_COMMAND_INVALID_TOPIC.
Три command-топика должны различаться. SASL/TLS задаются стандартными `spring.kafka.*` свойствами.

## Входная команда v1

Kafka key: `device-001`. Value: UTF-8 JSON. Подставьте актуальный expiresAt в будущем.

```json
{
  "schemaVersion": 1,
  "commandId": "read-device-001-001",
  "endpoint": "device-001",
  "operation": "READ_COMPOSITE",
  "paths": ["/3/0/0", "/3/0/1"],
  "timeoutMs": 30000,
  "expiresAt": "2026-11-18T12:00:00Z"
}
```

Обязательны schemaVersion, commandId, endpoint, operation, expiresAt. Key должен точно совпадать
с endpoint. commandId — непустая строка для корреляции, а не ключ дедупликации. Неизвестные поля,
версии, операции и повторяющиеся JSON-ключи отклоняются. Tombstone и невалидный JSON идут в invalid-топик.

`timeoutMs` — целое число от 1 до 300000, default 30000. HTTP сохраняет свой default 5 секунд.
`expiresAt` — дата ISO-8601 с часовым поясом, до которой разрешено начать выполнение. Проверяется перед
обращением к устройству и не отменяет начатую операцию. Истёкший срок даёт результат COMMAND_EXPIRED.

| operation | Данные операции |
|---|---|
| `READ` | path, опционально format |
| `DISCOVER` | path |
| `WRITE` | path, node, опционально replace (default true), format |
| `WRITE_ATTRIBUTES` | path, attributes (null или пустая строка снимает атрибут) |
| `EXECUTE` | path ресурса, опционально arguments — строка аргументов LwM2M |
| `CREATE` | path объекта/экземпляра, node с kind=instance; id=-1 для назначения ID устройством |
| `DELETE` | path экземпляра |
| `OBSERVE` | path, опционально format |
| `CANCEL_OBSERVATION` | path, опционально active (default false — локальная отмена) |
| `READ_COMPOSITE` | paths, опционально pathFormat, nodeFormat |
| `WRITE_COMPOSITE` | nodes — объект path → Node, опционально nodeFormat |
| `OBSERVE_COMPOSITE` | paths, опционально pathFormat, nodeFormat |
| `CANCEL_COMPOSITE_OBSERVATION` | paths, опционально active |

Node соответствует HTTP API: [README](../README.md#http-api). В Kafka paths — массив строк,
пути абсолютные, например `/3/0/0`. Имена форматов: TLV, SENML_CBOR, SENML_JSON и другие допустимые
для операции форматы Leshan. Для Composite default — SENML_CBOR.

## Результат v1

```json
{
  "schemaVersion": 1,
  "serverId": "node-A",
  "commandId": "read-device-001-001",
  "endpoint": "device-001",
  "operation": "READ_COMPOSITE",
  "completedAt": "2026-11-17T17:00:00Z",
  "response": {
    "status": "CONTENT(205)",
    "success": true,
    "content": {
      "/3/0/0": {"id":0,"kind":"singleResource","type":"STRING","value":"Acme"},
      "/3/0/1": {"id":1,"kind":"singleResource","type":"STRING","value":"Sensor-100"}
    },
    "objectLinks": []
  }
}
```

Отказ устройства представлен response.success=false с исходным кодом LwM2M и errorMessage.
Техническая ошибка — поле error вместо response:

```json
{
  "schemaVersion": 1,
  "serverId": "node-A",
  "commandId": "read-device-001-001",
  "endpoint": "device-001",
  "operation": "READ_COMPOSITE",
  "completedAt": "2026-11-17T17:00:00Z",
  "error": {"code":"DEVICE_SLEEPING","message":"Device 'device-001' is sleeping"}
}
```

Observe возвращает subscriptionId. Первое значение и последующие уведомления публикуются также
в observation-топик с тем же commandId и subscriptionId. Send остаётся в отдельном топике.
Порядок между разными топиками не гарантирован.

## Offset и параллелизм

Consumer вызывает общий асинхронный use case. Он инициирует send и возвращает future без ожидания
ответа. Затем вызывается `commitSync(offset + 1)`. После попытки коммита подключается callback
публикации результата. Ответ может физически прийти ещё до завершения коммита.

KAFKA_COMMAND_MAX_IN_FLIGHT ограничивает незавершённые Kafka-операции на сервере. Команды одному
endpoint могут выполняться одновременно; порядок выполнения и результатов не гарантирован.
Для зависимых операций отправитель ожидает предыдущий результат по commandId.

На лимите consumer приостанавливает все назначенные partitions через pause. Poll продолжается
каждые 100 мс, обслуживая группу; при освобождении слота выполняется resume. Для этого consumer
принудительно заданы `max.poll.records=1` и `enable.auto.commit=false`. Прикладной очереди нет,
ещё не переданные в use case команды не подтверждаются. Kafka client имеет штатные fetch-буферы.
Остальные настройки клиента берутся из `spring.kafka.consumer.*` и общих Kafka properties.

Спящее устройство получает DEVICE_SLEEPING сразу, команда не сохраняется до пробуждения.
Нет retry-топиков, outbox, статусов команд или дедупликации. Сбой после send до commit допускает
повторное выполнение; сбой после commit может потерять результат. Ошибка commit останавливает
consumer, health kafkaCommands становится DOWN. Внешний перезапуск может повторить неподтверждённую команду.

Publisher имеет отдельный поток и ограниченный буфер KAFKA_COMMAND_RESULT_BUFFER_CAPACITY (default 1000).
Действуют повторы Kafka producer до delivery timeout (default 30 секунд). Ошибка/переполнение может
потерять результат, но не вызывает устройство повторно и не блокирует его callback на ожидании Kafka.

## Некорректные сообщения и наблюдаемость

Invalid-событие: schemaVersion, serverId, sourceTopic, sourcePartition, sourceOffset, исходный key,
valueBase64, reason, receivedAt. Base64 сохраняет байты даже при невалидной кодировке. Для tombstone
valueBase64 отсутствует. Такие записи коммитятся без send. Ошибки параметров корректного envelope,
например неправильный LwM2M path, возвращаются как INVALID_REQUEST в result-топике для локального устройства.
При fan-out каждый сервер может опубликовать свою копию invalid-события, различающуюся по serverId.

Метрики через Actuator:

- lwm2m.commands.started, .skipped, .invalid, .inflight;
- lwm2m.commands.consumer.pauses, .failures;
- lwm2m.commands.publication.succeeded, .failed, .dropped.

Pause на лимите не является ошибкой health. UP показывает активность consumer, а не гарантирует
постоянную доступность broker. Состояние назначенных partitions и pause включено в детали health.
