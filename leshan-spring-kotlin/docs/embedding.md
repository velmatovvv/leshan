# Подключение к существующему Spring Boot проекту

Модуль `spring-integration` — обычный JAR без BOOT-INF и точки входа. Он использует существующий
bean `org.eclipse.leshan.server.LeshanServer`. Жизненным циклом Leshan, моделями, transport endpoints,
security store и собственным registration store каждого сервера управляет ваш проект.

Сборка рассчитана на Java 21, Spring Boot 3.5.7 и Leshan 2.0.0-M18. Leshan 1.x имеет другой API.

## Зависимость

Для локального использования установите библиотеку вместе с parent POM и core:

```bash
./mvnw -pl spring-integration -am install
```

В хост-проекте добавьте:

```xml
<dependency>
  <groupId>io.lwm2m</groupId>
  <artifactId>spring-integration</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Для общей работы команд и запросов достаточно существующего LeshanServer. Дополнительные зависимости
подключаются по используемому адаптеру:

```xml
<!-- Для REST на Spring MVC, если ещё не подключён -->
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-web</artifactId>
</dependency>

<!-- Для Kafka-команд и/или публикации Observe/Send -->
<dependency>
  <groupId>org.springframework.kafka</groupId>
  <artifactId>spring-kafka</artifactId>
</dependency>

<!-- По желанию: health Kafka consumer и метрики -->
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
```

Kafka, MVC, Servlet API и Actuator — optional зависимости библиотеки. Redis-клиент и транспорт
Californium библиотека не добавляет. Их выбирает хост вместе со своей реализацией Leshan.

Автоконфигурация обнаруживается обычным `@SpringBootApplication`/`@EnableAutoConfiguration`.
Дополнительный ComponentScan или Import пакетов библиотеки не нужен.

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

Все три внешних адаптера по умолчанию выключены в библиотечном режиме:

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

REST работает и без Kafka. Kafka-команды и публикация событий работают без MVC и web server.
Actuator необязателен. `lwm2m.enabled=false` отключает стандартную интеграцию целиком.
server-id обязателен при включении Kafka-команд и должен отличаться между серверами fan-out.

Настройка async HTTP timeout приведена для запросов с максимальным LwM2M timeout 300 секунд;
библиотека сохраняет управление общими HTTP-настройками за хостом.

В библиотечном JAR нет application.yml: он не устанавливает HTTP/CoAP порты, Redis URL или
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

Существующий MeterRegistry используется, если есть единственный кандидат или Primary. Иначе создаётся
приватный SimpleMeterRegistry. Библиотека закрывает только свой fallback registry.
HTTP exception advice ограничен DeviceController и не перехватывает исключения контроллеров хоста.
Имена стандартных beans интеграции имеют префикс lwm2m, чтобы не конфликтовать с доменными beans хоста.

При необходимости можно предоставить свои beans DeviceGateway, HasLocalRegistration, PublishDeviceEvent,
CommandPublisher или пару use cases ExecuteDeviceCommand/QueryDevices — стандартные beans отступают.
Вместо пары use cases удобно предоставить собственный DeviceService, реализующий оба интерфейса.

## Standalone

`application` зависит от этой же библиотеки и добавляет создание Leshan, Redis stores, endpoints,
application.yml, HTTP server и actuator. Его настройки по умолчанию сохраняют REST и Kafka-события
включёнными, Kafka-команды — выключенными.

```bash
./mvnw verify
java -jar application/target/application-0.1.0-SNAPSHOT.jar
```
