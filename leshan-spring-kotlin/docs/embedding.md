# Подключение к существующему Spring Boot проекту

Модули библиотеки — обычные JAR без BOOT-INF и точки входа. Они используют существующий
bean `org.eclipse.leshan.server.LeshanServer`. Жизненным циклом Leshan, моделями, transport endpoints,
security store и собственным registration store каждого сервера управляет ваш проект.

Сборка рассчитана на Java 21, Kotlin 2.2.21, Spring Boot 3.5.7 и Leshan 2.0.0-M18.
Leshan 1.x имеет другой API.

## Только HTTP API

Из каталога `leshan-spring-kotlin` установите выбранный модуль вместе с parent POM и зависимостями:

```bash
./mvnw -pl http-api -am install
```

В хост-проекте добавьте одну зависимость:

```xml
<dependency>
  <groupId>io.lwm2m</groupId>
  <artifactId>http-api</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Она подключает `leshan-integration`, `core` и `spring-boot-starter-web`. Kafka, Kafka-адаптер,
Actuator и Micrometer core не входят в её зависимости.

```yaml
lwm2m:
  rest:
    enabled: true
spring:
  mvc:
    async:
      request-timeout: 310s
```

`server-id` для HTTP не требуется. Существующий bean LeshanServer автоматически связывается с API.
Дополнительный ComponentScan или Import пакетов библиотеки не нужен.

```bash
curl http://localhost:8080/api/clients
curl 'http://localhost:8080/api/clients/device-001/3/0/0?timeout=30'
```

HTTP использует порт и context path хост-приложения. Настройка async HTTP timeout позволяет дождаться
запросов с максимальным LwM2M timeout 300 секунд; тайм-аут операции задаётся параметром `timeout`.
У Observe первоначальный ответ возвращается по HTTP. Последующие уведомления и входящий Send можно
обработать своим bean `PublishDeviceEvent` либо подключить публикацию событий из `kafka-integration`.
Потока последующих уведомлений по HTTP нет.

Для CI или других машин опубликуйте выбранные JAR и их POM, включая parent POM, во внутренний
Maven-репозиторий. Артефакты проекта не опубликованы в Maven Central.

## Выбор остальных модулей

| Зависимость `io.lwm2m`, версия `0.1.0-SNAPSHOT` | Что подключается |
|---|---|
| `core` | Команды, результаты и порты без Spring, Leshan, Jackson, Redis и Kafka |
| `leshan-integration` | `core`, адаптер Leshan и общая Spring-автоконфигурация, без HTTP/Kafka |
| `http-api` | `leshan-integration` и HTTP API со Spring MVC |
| `kafka-integration` | `leshan-integration`, Spring Kafka, команды, результаты, Observe/Send и Kafka-метрики; без MVC |
| `spring-integration` | Объединение `http-api` и `kafka-integration` для совместимости |

Например, для Kafka без HTTP установите `./mvnw -pl kafka-integration -am install` и добавьте:

```xml
<dependency>
  <groupId>io.lwm2m</groupId>
  <artifactId>kafka-integration</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Для HTTP и Kafka добавьте обе зависимости или оставьте прежнюю `spring-integration`.
В объединяющем артефакте нет собственных классов и автоконфигураций; имена классов и настройки
сохранены. MVC и Kafka теперь подключаются через его зависимости транзитивно. Старые явные
зависимости хоста на `spring-boot-starter-web` и `spring-kafka` можно оставить.

Actuator необязателен. Для HTTP Actuator и для health/экспорта Kafka-метрик при необходимости добавьте
`org.springframework.boot:spring-boot-starter-actuator`. Сам учёт Kafka-метрик работает без Actuator.
Redis-клиент и транспорт Californium библиотечные модули не добавляют: их выбирает хост.
Автоконфигурация обнаруживается обычным `@SpringBootApplication`/`@EnableAutoConfiguration`.

## Существующий сервер

Если у вас уже есть `@Bean LeshanServer`, оставьте его создание и lifecycle в хосте. Например:

```kotlin
@Bean(initMethod = "start", destroyMethod = "destroy")
fun leshanServer(): LeshanServer = buildConfiguredLeshanServer()
```

`buildConfiguredLeshanServer()` здесь обозначает существующий код вашего проекта.
Интеграция не вызывает start, stop или destroy сервера; подключает собственные listeners и снимает их
при закрытии контекста. Повторное attach не дублирует listeners. Если нужно получать события с самого
начала работы сервера, хост может запускать его через SmartLifecycle после инициализации singletons,
как это делает standalone-приложение.

При нескольких beans LeshanServer отметьте один `@Primary`. Без единственного кандидата
автоконфигурация не активируется. Без bean LeshanServer библиотека ничего не создаёт.
Если сервер создаётся другой автоконфигурацией, она должна обрабатываться раньше
`LeshanIntegrationAutoConfiguration`; для этого используется стандартный порядок AutoConfiguration.

## Независимое включение адаптеров

HTTP API, приём Kafka-команд и публикация Kafka-событий по умолчанию выключены в библиотечном режиме.
Следующий пример включает оба модуля, если они присутствуют в зависимостях:

```yaml
lwm2m:
  enabled: true
  server-id: node-A
  rest:
    enabled: true
  commands:
    enabled: true
    max-in-flight: 100
  events:
    enabled: true
spring:
  kafka:
    bootstrap-servers: localhost:9092
  mvc:
    async:
      request-timeout: 310s
```

Настройки адаптера действуют только при наличии соответствующего модуля в classpath.
Например, `lwm2m.commands.enabled=true` при подключённом только `http-api` не создаёт Kafka-клиенты
и не требует server-id. `lwm2m.enabled=false` отключает стандартную интеграцию целиком.
При подключённом `kafka-integration` server-id обязателен для включённых Kafka-команд и должен
отличаться между серверами fan-out.

Настройка async HTTP timeout приведена для запросов с максимальным LwM2M timeout 300 секунд;
библиотека сохраняет управление общими HTTP-настройками за хостом.

Библиотечные модули не содержат application.yml и не устанавливают HTTP/CoAP порты, Redis URL или
глобальные Jackson/Kafka настройки. Переменные вроде KAFKA_COMMANDS_ENABLED в документации standalone
являются aliases его application.yml. В хосте используйте YAML выше либо стандартное relaxed binding,
например LWM2M_COMMANDS_ENABLED=true.

## Изоляция инфраструктуры

JSON для входных Kafka envelope и выходных событий/результатов использует приватный `Lwm2mJson`.
Он сохраняет camelCase, ISO timestamps и формат контракта независимо от настроек хостового ObjectMapper.
Существующий ObjectMapper не изменяется. REST-ответы сериализуются HTTP converters хоста.

`Lwm2mKafkaClient` инкапсулирует собственные KafkaTemplate и ConsumerFactory: они не публикуются
отдельными beans и не создают неоднозначности в Autowired хостового KafkaTemplate/ConsumerFactory.
SASL/TLS и общие client properties берутся из `spring.kafka.*`. Для wire JSON принудительно используются
String serializer и ByteArray consumer value deserializer. Для consumer также заданы auto-commit=false
и max.poll.records=1; остальные ограничения и семантика описаны в [kafka.md](kafka.md).

В `kafka-integration` существующий MeterRegistry используется, если есть единственный кандидат или
Primary. Иначе создаётся приватный SimpleMeterRegistry. Модуль закрывает только свой fallback registry.
HTTP exception advice ограничен DeviceController и не перехватывает исключения контроллеров хоста.
Имена стандартных beans интеграции имеют префикс lwm2m, чтобы не конфликтовать с доменными beans хоста.
Длительности Kafka-команд и пауз, ошибки по кодам и отдельный счётчик TIMEOUT описаны в [metrics.md](metrics.md).

При необходимости можно предоставить свои beans DeviceGateway, HasLocalRegistration, PublishDeviceEvent,
CommandPublisher или пару use cases ExecuteDeviceCommand/QueryDevices — стандартные beans отступают.
Вместо пары use cases удобно предоставить собственный DeviceService, реализующий оба интерфейса.

## Standalone

`application` явно зависит от `http-api` и `kafka-integration` и добавляет создание Leshan, Redis stores,
endpoints, application.yml, HTTP server и actuator. Его настройки по умолчанию сохраняют REST и Kafka-события
включёнными, Kafka-команды — выключенными.

```bash
./mvnw verify
java -jar application/target/application-0.1.0-SNAPSHOT.jar
```
