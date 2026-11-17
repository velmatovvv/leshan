package io.lwm2m.server

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.awaitility.Awaitility.await
import org.eclipse.leshan.client.LeshanClient
import org.eclipse.leshan.client.LeshanClientBuilder
import org.eclipse.leshan.client.engine.DefaultRegistrationEngineFactory
import org.eclipse.leshan.client.`object`.Security
import org.eclipse.leshan.client.`object`.Server
import org.eclipse.leshan.client.resource.ObjectsInitializer
import org.eclipse.leshan.client.resource.SimpleInstanceEnabler
import org.eclipse.leshan.client.servers.LwM2mServer
import org.eclipse.leshan.core.model.LwM2mModelRepository
import org.eclipse.leshan.core.model.ObjectLoader
import org.eclipse.leshan.core.node.LwM2mResource
import org.eclipse.leshan.core.request.ContentFormat
import org.eclipse.leshan.core.request.argument.Arguments
import org.eclipse.leshan.core.response.ExecuteResponse
import org.eclipse.leshan.server.LeshanServer
import org.eclipse.leshan.server.queue.PresenceServiceImpl
import org.eclipse.leshan.transport.californium.client.endpoint.CaliforniumClientEndpointsProvider
import org.eclipse.leshan.transport.californium.client.endpoint.coap.CoapClientProtocolProvider
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.annotation.DirtiesContext
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import java.io.File

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = [
    "lwm2m.redis.enabled=false", "lwm2m.coap-uri=coap://127.0.0.1:0", "lwm2m.coaps-uri=",
    "lwm2m.models-directory=src/test/resources/models", "logging.level.root=WARN", "logging.level.kafka=ERROR",
    "logging.level.org.apache.kafka=ERROR", "logging.level.org.apache.zookeeper=ERROR"
])
@EmbeddedKafka(partitions = 1, topics = ["lwm2m.observations", "lwm2m.send"], bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@DirtiesContext
class DeviceFlowTest {
    @Autowired lateinit var server: LeshanServer
    @Autowired lateinit var kafka: EmbeddedKafkaBroker
    @Autowired lateinit var mapper: ObjectMapper
    @LocalServerPort var port: Int = 0
    private val http = HttpClient.newHttpClient()

    class Instance : SimpleInstanceEnabler() {
        init { initialValues = mapOf<Int, Any>(0 to 10L, 1 to "initial") }
        val executions = AtomicInteger()
        override fun execute(server: LwM2mServer, resourceid: Int, arguments: Arguments): ExecuteResponse {
            executions.incrementAndGet()
            return ExecuteResponse.success()
        }
    }

    private fun client(endpoint: String, queue: Boolean, instance: Instance): LeshanClient {
        val models = ObjectLoader.loadAllDefault() + ObjectLoader.loadObjectsFromDir(File("src/test/resources/models"))
        val objects = ObjectsInitializer(LwM2mModelRepository(models).lwM2mModel)
        objects.setInstancesForObject(0, Security.noSec(server.endpoints.first().uri.toString(), 1))
        objects.setInstancesForObject(1, Server(1, 300))
        objects.setInstancesForObject(3, org.eclipse.leshan.client.`object`.Device("test", "test", endpoint))
        objects.setInstancesForObject(12345, instance)
        objects.setClassForObject(12345, Instance::class.java)
        return LeshanClientBuilder(endpoint).setObjects(objects.create(0, 1, 3, 12345))
            .setEndpointsProviders(CaliforniumClientEndpointsProvider.Builder(CoapClientProtocolProvider()).build())
            .setRegistrationEngineFactory(DefaultRegistrationEngineFactory().setQueueMode(queue)).build().also { it.start() }
    }

    private fun request(method: String, path: String, body: String? = null): Pair<Int, JsonNode> {
        val req = HttpRequest.newBuilder(URI("http://localhost:$port/api/clients$path"))
            .header("Content-Type", "application/json").header("X-Command-Id", "test-correlation")
            .method(method, body?.let(HttpRequest.BodyPublishers::ofString) ?: HttpRequest.BodyPublishers.noBody()).build()
        val response = http.send(req, HttpResponse.BodyHandlers.ofString())
        return response.statusCode() to mapper.readTree(response.body())
    }
    private fun successful(method: String, path: String, body: String? = null): JsonNode {
        val (status, result) = request(method, path, body)
        assertEquals(200, status, result.toString())
        assertTrue(result.path("response").path("success").asBoolean(), result.toString())
        assertEquals("test-correlation", result.path("commandId").asText())
        return result.path("response")
    }

    @Test fun `real device supports all fifteen endpoints and publishes observations and Send to Kafka`() {
        val instance = Instance()
        val device = client("integration-device", false, instance)
        val consumer = KafkaConsumer<String, String>(KafkaTestUtils.consumerProps("flow-test", "true", kafka), StringDeserializer(), StringDeserializer())
        try {
            kafka.consumeFromAllEmbeddedTopics(consumer)
            await().atMost(Duration.ofSeconds(10)).until { server.registrationService.getByEndpoint("integration-device") != null && device.registeredServers.isNotEmpty() }
            val base = "/integration-device"
            assertEquals(200, request("GET", "").first)
            assertEquals("integration-device", request("GET", base).second.path("endpoint").asText())
            assertEquals(10L, successful("GET", "$base/12345/0/0").path("content").path("/12345/0/0").path("value").asLong())
            assertTrue(successful("GET", "$base/12345/0/discover").path("objectLinks").size() > 0)
            successful("PUT", "$base/12345/0/0", """{"id":0,"type":"INTEGER","value":42}""")
            successful("PUT", "$base/12345/0/attributes?pmin=0&pmax=60")
            successful("POST", "$base/12345/0/2", "")
            assertEquals(1, instance.executions.get())
            val created = successful("POST", "$base/12345", """{"id":-1,"kind":"instance","resources":[{"id":0,"type":"INTEGER","value":7}]}""")
            successful("DELETE", "$base/${created.path("location").asText().trimStart('/')}")
            val observe = successful("POST", "$base/12345/0/0/observe")
            assertFalse(observe.path("subscriptionId").asText().isBlank())
            successful("PUT", "$base/12345/0/0", """{"id":0,"type":"INTEGER","value":43}""")
            val received = mutableListOf<Pair<String, JsonNode>>()
            await().atMost(Duration.ofSeconds(10)).until {
                consumer.poll(Duration.ofMillis(200)).forEach { received.add(it.topic() to mapper.readTree(it.value())) }
                received.any { (_, e) -> e.path("type").asText() == "OBSERVATION_VALUE" && e.path("data").path("/12345/0/0").path("value").asLong() == 43L }
            }
            successful("DELETE", "$base/12345/0/0/observe?active=true")
            successful("GET", "$base/composite?paths=/12345/0/0,/12345/0/1")
            successful("PUT", "$base/composite", """{"/12345/0/0":{"id":0,"type":"INTEGER","value":44},"/12345/0/1":{"id":1,"type":"STRING","value":"changed"}}""")
            val composite = successful("POST", "$base/composite/observe?paths=/12345/0/0,/12345/0/1")
            successful("DELETE", "$base/composite/observe?paths=/12345/0/1,/12345/0/0&active=true")
            assertTrue(server.observationService.getObservations(server.registrationService.getByEndpoint("integration-device")).isEmpty())
            val sent = device.sendService.sendData(device.registeredServers.values.first(), ContentFormat.SENML_CBOR, listOf("/12345/0/0"), 5000)
            assertTrue(sent.isSuccess)
            await().atMost(Duration.ofSeconds(10)).until {
                consumer.poll(Duration.ofMillis(200)).forEach { received.add(it.topic() to mapper.readTree(it.value())) }
                received.any { (topic, e) -> topic == "lwm2m.send" && e.path("type").asText() == "SEND" } &&
                    received.any { (topic, e) -> topic == "lwm2m.observations" && e.path("subscriptionId").asText() == composite.path("subscriptionId").asText() }
            }
            val values = received.filter { it.second.path("type").asText() == "OBSERVATION_VALUE" }
            assertTrue(values.all { it.first == "lwm2m.observations" && it.second.path("commandId").asText() == "test-correlation" })
            assertTrue(values.any { it.second.path("data").path("/12345/0/0").path("value").asLong() == 42L })
            assertEquals(404, request("GET", "/missing/12345/0/0").first)
            assertEquals(400, request("GET", "$base/12345/0/0?timeout=abc").first)
        } finally { consumer.close(); device.destroy(true) }
    }

    @Test fun `queue device operates awake and fails while sleeping without deferred execution`() {
        val instance = Instance()
        val device = client("queue-device", true, instance)
        try {
            await().atMost(Duration.ofSeconds(10)).until { server.registrationService.getByEndpoint("queue-device") != null }
            val registration = server.registrationService.getByEndpoint("queue-device")
            assertTrue(registration.usesQueueMode())
            successful("GET", "/queue-device/12345/0/0")
            val presence = server.presenceService as PresenceServiceImpl
            presence.setSleeping(registration)
            val (status, result) = request("POST", "/queue-device/12345/0/2")
            assertEquals(409, status)
            assertEquals("DEVICE_SLEEPING", result.path("error").path("code").asText())
            successful("DELETE", "/queue-device/12345/0/0/observe")
            presence.setAwake(registration)
            successful("GET", "/queue-device/12345/0/0")
            assertEquals(0, instance.executions.get())
        } finally { device.destroy(true) }
    }
}
