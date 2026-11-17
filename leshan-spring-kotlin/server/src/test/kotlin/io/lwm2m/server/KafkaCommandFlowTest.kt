package io.lwm2m.server

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.lwm2m.core.*
import io.lwm2m.server.adapter.kafka.*
import io.lwm2m.server.adapter.leshan.LeshanGateway
import io.lwm2m.server.config.Lwm2mProperties
import io.lwm2m.server.config.ServerIdentity
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.awaitility.Awaitility.await
import org.eclipse.leshan.client.LeshanClient
import org.eclipse.leshan.client.LeshanClientBuilder
import org.eclipse.leshan.client.engine.DefaultRegistrationEngineFactory
import org.eclipse.leshan.client.`object`.Security
import org.eclipse.leshan.client.`object`.Server
import org.eclipse.leshan.client.resource.ObjectsInitializer
import org.eclipse.leshan.core.model.LwM2mModelRepository
import org.eclipse.leshan.core.model.ObjectLoader
import org.eclipse.leshan.server.LeshanServer
import org.eclipse.leshan.server.LeshanServerBuilder
import org.eclipse.leshan.server.queue.PresenceServiceImpl
import org.eclipse.leshan.transport.californium.client.endpoint.CaliforniumClientEndpointsProvider
import org.eclipse.leshan.transport.californium.client.endpoint.coap.CoapClientProtocolProvider
import org.eclipse.leshan.transport.californium.server.endpoint.CaliforniumServerEndpointsProvider
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.annotation.DirtiesContext
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = [
    "lwm2m.redis.enabled=false", "lwm2m.coap-uri=coap://127.0.0.1:0", "lwm2m.coaps-uri=",
    "lwm2m.server-id=node-A", "lwm2m.commands.enabled=true", "logging.level.root=WARN",
    "logging.level.kafka=ERROR", "logging.level.org.apache.kafka=ERROR", "logging.level.org.apache.zookeeper=ERROR"
])
@EmbeddedKafka(partitions = 1, topics = ["lwm2m.commands", "lwm2m.command-results", "lwm2m.commands.invalid",
    "lwm2m.observations", "lwm2m.send", "capacity.commands"], bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@DirtiesContext
class KafkaCommandFlowTest {
    @Autowired lateinit var server: LeshanServer
    @Autowired lateinit var kafka: EmbeddedKafkaBroker
    @Autowired lateinit var mapper: ObjectMapper
    @Autowired lateinit var producer: KafkaTemplate<String, String>
    @Autowired lateinit var publisher: CommandPublisher
    @Autowired lateinit var mainConsumer: KafkaCommandConsumer
    @Autowired @Qualifier("commandConsumerFactory") lateinit var factory: ConsumerFactory<String, ByteArray>

    private fun admin() = AdminClient.create(mapOf("bootstrap.servers" to kafka.brokersAsString))
    private fun committed(admin: AdminClient, group: String, topic: String): Long =
        admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS)[TopicPartition(topic, 0)]?.offset() ?: 0L
    private fun envelope(id: String, endpoint: String, operation: Operation = Operation.READ, path: String? = "/3/0/0",
                         paths: List<String> = emptyList(), expiresAt: Instant = Instant.now().plusSeconds(60), timeoutMs: Long = 3000) =
        KafkaDeviceCommand(1, id, endpoint, operation, expiresAt, timeoutMs, path = path, paths = paths)
    private fun publish(command: KafkaDeviceCommand, topic: String = "lwm2m.commands"): Long =
        producer.send(topic, command.endpoint, mapper.writeValueAsString(command)).get(5, TimeUnit.SECONDS).recordMetadata.offset()

    private fun client(endpoint: String, queue: Boolean = false): LeshanClient {
        val objects = ObjectsInitializer(LwM2mModelRepository(ObjectLoader.loadAllDefault()).lwM2mModel)
        objects.setInstancesForObject(0, Security.noSec(server.endpoints.first().uri.toString(), 1))
        objects.setInstancesForObject(1, Server(1, 300))
        objects.setInstancesForObject(3, org.eclipse.leshan.client.`object`.Device("Acme", "Sensor-100", endpoint))
        return LeshanClientBuilder(endpoint).setObjects(objects.create(0, 1, 3))
            .setEndpointsProviders(CaliforniumClientEndpointsProvider.Builder(CoapClientProtocolProvider()).build())
            .setRegistrationEngineFactory(DefaultRegistrationEngineFactory().setQueueMode(queue)).build().also { it.start() }
    }

    @Test fun `fanout executes only on locally registered server and publishes terminal results`() {
        val other = LeshanServerBuilder().setEndpointsProviders(CaliforniumServerEndpointsProvider.Builder()
            .addEndpoint("coap://127.0.0.1:0").build()).build()
        val otherGateway = LeshanGateway(other, PublishDeviceEvent {})
        otherGateway.start()
        val otherService = DeviceService(otherGateway)
        val foreignDispatches = AtomicInteger()
        val registry = SimpleMeterRegistry()
        val identity = ServerIdentity("node-B")
        val otherHandler = KafkaCommandHandler(CommandCodec(mapper), object : ExecuteDeviceCommand {
            override fun execute(command: DeviceCommand) = otherService.execute(command).also { foreignDispatches.incrementAndGet() }
        }, HasLocalRegistration { other.registrationService.getByEndpoint(it) != null }, publisher, identity, registry)
        val otherConsumer = KafkaCommandConsumer(factory, otherHandler, Lwm2mProperties(serverId = identity.serverId), identity, registry)
        otherConsumer.start()
        val device = client("kafka-device")
        val queueDevice = client("kafka-queue", true)
        var destroyed = false
        val reader = KafkaConsumer<String, String>(KafkaTestUtils.consumerProps("command-result-test", "true", kafka), StringDeserializer(), StringDeserializer())
        try {
            kafka.consumeFromEmbeddedTopics(reader, "lwm2m.command-results", "lwm2m.commands.invalid")
            await().atMost(Duration.ofSeconds(15)).until {
                mainConsumer.assignedPartitions.isNotEmpty() && otherConsumer.assignedPartitions.isNotEmpty() &&
                    device.registeredServers.isNotEmpty() && queueDevice.registeredServers.isNotEmpty()
            }
            val received = mutableListOf<Pair<String, JsonNode>>()
            fun collect() { reader.poll(Duration.ofMillis(100)).forEach { received.add(it.topic() to mapper.readTree(it.value())) } }
            fun result(id: String): JsonNode {
                await().atMost(Duration.ofSeconds(10)).until { collect(); received.any { it.second.path("commandId").asText() == id } }
                return received.first { it.second.path("commandId").asText() == id }.second
            }
            admin().use { admin ->
                val offset = publish(envelope("composite", "kafka-device", Operation.READ_COMPOSITE, path = null, paths = listOf("/3/0/0", "/3/0/1")))
                val read = result("composite")
                assertEquals("node-A", read.path("serverId").asText())
                assertTrue(read.path("response").path("success").asBoolean(), read.toString())
                assertEquals("Acme", read.path("response").path("content").path("/3/0/0").path("value").asText())
                await().atMost(Duration.ofSeconds(10)).until { committed(admin, mainConsumer.groupId, "lwm2m.commands") > offset && committed(admin, otherConsumer.groupId, "lwm2m.commands") > offset }
                assertEquals(0, foreignDispatches.get())

                publish(envelope("observe", "kafka-device", Operation.OBSERVE))
                assertFalse(result("observe").path("response").path("subscriptionId").asText().isBlank())
                publish(envelope("cancel", "kafka-device", Operation.CANCEL_OBSERVATION))
                assertTrue(result("cancel").path("response").path("success").asBoolean())
                publish(envelope("expired", "kafka-device", expiresAt = Instant.EPOCH))
                assertEquals("COMMAND_EXPIRED", result("expired").path("error").path("code").asText())

                (server.presenceService as PresenceServiceImpl).setSleeping(server.registrationService.getByEndpoint("kafka-queue"))
                publish(envelope("sleeping", "kafka-queue"))
                assertEquals("DEVICE_SLEEPING", result("sleeping").path("error").path("code").asText())

                val missingOffset = publish(envelope("missing", "not-registered"))
                await().atMost(Duration.ofSeconds(10)).until {
                    collect()
                    committed(admin, mainConsumer.groupId, "lwm2m.commands") > missingOffset && committed(admin, otherConsumer.groupId, "lwm2m.commands") > missingOffset
                }
                assertTrue(received.none { it.second.path("commandId").asText() == "missing" })

                val invalidOffset = producer.send("lwm2m.commands", "kafka-device", "{broken").get(5, TimeUnit.SECONDS).recordMetadata.offset()
                await().atMost(Duration.ofSeconds(10)).until {
                    collect()
                    received.count { it.first == "lwm2m.commands.invalid" && it.second.path("sourceOffset").asLong() == invalidOffset } == 2
                }
                assertEquals(setOf("node-A", "node-B"), received.filter { it.first == "lwm2m.commands.invalid" && it.second.path("sourceOffset").asLong() == invalidOffset }
                    .map { it.second.path("serverId").asText() }.toSet())

                device.destroy(false)
                destroyed = true
                publish(envelope("timeout", "kafka-device", timeoutMs = 1000))
                assertEquals("TIMEOUT", result("timeout").path("error").path("code").asText())
                assertEquals(0, foreignDispatches.get())
                assertTrue(received.filter { it.first == "lwm2m.command-results" }.all { it.second.path("serverId").asText() == "node-A" })
            }
        } finally {
            reader.close()
            if (!destroyed) device.destroy(true)
            queueDevice.destroy(true)
            otherConsumer.stop()
            other.destroy()
        }
    }

    @Test fun `global limit pauses input until a slot is released and offsets commit before completion`() {
        val pending = ConcurrentHashMap<String, CompletableFuture<CommandResult>>()
        val results = ConcurrentLinkedQueue<KafkaCommandResult>()
        val registry = SimpleMeterRegistry()
        val identity = ServerIdentity("capacity-node")
        val properties = Lwm2mProperties(serverId = identity.serverId, commands = Lwm2mProperties.Commands(
            topic = "capacity.commands", resultTopic = "capacity.results", invalidTopic = "capacity.invalid", maxInFlight = 2))
        val handler = KafkaCommandHandler(CommandCodec(mapper), object : ExecuteDeviceCommand {
            override fun execute(command: DeviceCommand) = CompletableFuture<CommandResult>().also { pending[command.commandId] = it }
        }, HasLocalRegistration { true }, object : CommandPublisher {
            override fun result(result: KafkaCommandResult) { results.add(result) }
            override fun invalid(message: InvalidKafkaCommand) { fail<Unit>(message.toString()) }
        }, identity, registry)
        val consumer = KafkaCommandConsumer(factory, handler, properties, identity, registry)
        consumer.start()
        try {
            await().atMost(Duration.ofSeconds(15)).until { consumer.assignedPartitions.isNotEmpty() }
            listOf("one", "two", "three").forEach { publish(envelope(it, "same-device"), "capacity.commands") }
            admin().use { admin ->
                await().atMost(Duration.ofSeconds(10)).until { pending.size == 2 && consumer.paused && committed(admin, consumer.groupId, "capacity.commands") == 2L }
                assertEquals(2, handler.inFlight)
                assertFalse(pending.getValue("one").isDone)
                assertFalse(pending.getValue("two").isDone)
                await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3)).untilAsserted {
                    assertEquals(2, pending.size)
                    assertEquals(2L, committed(admin, consumer.groupId, "capacity.commands"))
                    assertTrue(consumer.isRunning)
                }
                pending.getValue("two").complete(CommandResult("two", "same-device", Operation.READ))
                await().atMost(Duration.ofSeconds(10)).until { pending.containsKey("three") && committed(admin, consumer.groupId, "capacity.commands") == 3L }
                assertFalse(pending.getValue("one").isDone)
                assertFalse(pending.getValue("three").isDone)
                assertEquals(2, handler.inFlight)
                pending.getValue("one").complete(CommandResult("one", "same-device", Operation.READ))
                pending.getValue("three").complete(CommandResult("three", "same-device", Operation.READ))
                await().atMost(Duration.ofSeconds(5)).until { handler.inFlight == 0 && !consumer.paused }
                assertEquals(listOf("two", "one", "three"), results.map { it.commandId })
                assertTrue(registry.counter("lwm2m.commands.consumer.pauses").count() >= 1)
            }
        } finally {
            pending.forEach { (id, future) -> future.complete(CommandResult(id, "same-device", Operation.READ)) }
            consumer.stop()
        }
    }
}
