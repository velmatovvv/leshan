package io.lwm2m.integration

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.lwm2m.core.*
import io.lwm2m.integration.adapter.kafka.*
import io.lwm2m.integration.config.ServerIdentity
import io.micrometer.core.instrument.MockClock
import io.micrometer.core.instrument.simple.SimpleConfig
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.TimeUnit

class CommandMetricsTest {
    private val mapper = jacksonObjectMapper().findAndRegisterModules()
    private class Results : CommandPublisher {
        val values = mutableListOf<KafkaCommandResult>()
        override fun result(result: KafkaCommandResult) { values.add(result) }
        override fun invalid(message: InvalidKafkaCommand) = Unit
    }
    private fun record(id: String = "test") = ConsumerRecord("commands", 0, 0L, "device",
        mapper.writeValueAsBytes(KafkaDeviceCommand(1, id, "device", Operation.READ, Instant.now().plusSeconds(60), path = "/3/0/0")))
    private fun handler(registry: SimpleMeterRegistry, publisher: CommandPublisher, local: Boolean = true,
                        action: (DeviceCommand) -> CompletionStage<CommandResult>) = KafkaCommandHandler(
        CommandCodec(mapper), object : ExecuteDeviceCommand {
            override fun execute(command: DeviceCommand) = action(command)
        }, HasLocalRegistration { local }, publisher, ServerIdentity("metrics"), registry)
    private fun success(id: String = "test") = CommandResult(id, "device", Operation.READ, response = DeviceResponse("CONTENT(205)", true))
    private fun count(registry: SimpleMeterRegistry, name: String, vararg tags: String) = registry.find(name).tags(*tags).counter()?.count() ?: 0.0

    @Test fun `execution timer stops on response during slow commit and excludes publication latency`() {
        val clock = MockClock()
        val registry = SimpleMeterRegistry(SimpleConfig.DEFAULT, clock)
        val pending = CompletableFuture<CommandResult>()
        var published = false
        val publisher = object : CommandPublisher {
            override fun result(result: KafkaCommandResult) { published = true; clock.add(Duration.ofSeconds(11)) }
            override fun invalid(message: InvalidKafkaCommand) = Unit
        }
        val handler = handler(registry, publisher) { pending }
        handler.handle(record()) {
            clock.add(Duration.ofSeconds(2))
            pending.complete(success())
            assertFalse(published)
            assertEquals(1.0, count(registry, "lwm2m.commands.completed", "operation", "READ", "outcome", "success"))
            clock.add(Duration.ofSeconds(7))
        }
        val timer = registry.get("lwm2m.commands.duration").tags("operation", "READ", "outcome", "success").timer()
        assertEquals(1L, timer.count())
        assertEquals(2.0, timer.totalTime(TimeUnit.SECONDS))
        assertTrue(published)
        assertEquals(0, handler.inFlight)
    }

    @Test fun `timeouts validation errors and device refusals are distinguished`() {
        val clock = MockClock()
        val registry = SimpleMeterRegistry(SimpleConfig.DEFAULT, clock)
        val pending = mutableListOf<CompletableFuture<CommandResult>>()
        val results = Results()
        val handler = handler(registry, results) { CompletableFuture<CommandResult>().also { pending.add(it) } }
        for ((id, code) in listOf("timeout" to ErrorCode.TIMEOUT, "sleeping" to ErrorCode.DEVICE_SLEEPING)) {
            handler.handle(record(id)) {}
            clock.add(Duration.ofSeconds(3))
            pending.last().complete(CommandResult(id, "device", Operation.READ, error = CommandError(code, "test")))
        }
        handler.handle(record("device-error")) {}
        clock.add(Duration.ofSeconds(1))
        pending.last().complete(CommandResult("device-error", "device", Operation.READ, response = DeviceResponse("NOT_FOUND(404)", false)))
        assertEquals(1.0, count(registry, "lwm2m.commands.timeouts"))
        for (code in listOf("TIMEOUT", "DEVICE_SLEEPING", "DEVICE_RESPONSE_ERROR"))
            assertEquals(1.0, count(registry, "lwm2m.commands.errors", "operation", "READ", "code", code))
        assertEquals(2.0, count(registry, "lwm2m.commands.completed", "operation", "READ", "outcome", "error"))
        assertEquals(1.0, count(registry, "lwm2m.commands.completed", "operation", "READ", "outcome", "device_error"))
        assertEquals(6.0, registry.get("lwm2m.commands.duration").tags("operation", "READ", "outcome", "error").timer().totalTime(TimeUnit.SECONDS))
        assertEquals(3, results.values.size)
        assertEquals(0, handler.inFlight)
    }

    @Test fun `response after commit is measured before a slow result publisher runs`() {
        val clock = MockClock()
        val registry = SimpleMeterRegistry(SimpleConfig.DEFAULT, clock)
        val pending = CompletableFuture<CommandResult>()
        val publisher = object : CommandPublisher {
            override fun result(result: KafkaCommandResult) { clock.add(Duration.ofSeconds(11)) }
            override fun invalid(message: InvalidKafkaCommand) = Unit
        }
        val handler = handler(registry, publisher) { pending }
        handler.handle(record()) {}
        clock.add(Duration.ofSeconds(2))
        pending.complete(success())
        val timer = registry.get("lwm2m.commands.duration").tags("operation", "READ", "outcome", "success").timer()
        assertEquals(2.0, timer.totalTime(TimeUnit.SECONDS))
        assertEquals(0, handler.inFlight)
    }

    @Test fun `synchronous dispatch failure records an execution error and releases slot`() {
        val clock = MockClock()
        val registry = SimpleMeterRegistry(SimpleConfig.DEFAULT, clock)
        val results = Results()
        val handler = handler(registry, results) { clock.add(Duration.ofSeconds(1)); error("dispatch failed") }
        var committed = false
        handler.handle(record()) { committed = true }
        assertTrue(committed)
        assertEquals(ErrorCode.INTERNAL_ERROR, results.values.single().error!!.code)
        assertEquals(1.0, count(registry, "lwm2m.commands.errors", "operation", "READ", "code", "INTERNAL_ERROR"))
        assertEquals(1.0, registry.get("lwm2m.commands.duration").tags("operation", "READ", "outcome", "error").timer().totalTime(TimeUnit.SECONDS))
        assertEquals(0, handler.inFlight)
    }

    @Test fun `exceptional future completion counts once and preserves result normalization`() {
        val clock = MockClock()
        val registry = SimpleMeterRegistry(SimpleConfig.DEFAULT, clock)
        val pending = CompletableFuture<CommandResult>()
        val results = Results()
        val handler = handler(registry, results) { pending }
        handler.handle(record()) {}
        clock.add(Duration.ofSeconds(4))
        pending.completeExceptionally(IllegalStateException("completion failed"))
        pending.complete(success())
        assertEquals(1.0, count(registry, "lwm2m.commands.completed", "operation", "READ", "outcome", "error"))
        assertEquals(1.0, count(registry, "lwm2m.commands.errors", "operation", "READ", "code", "INTERNAL_ERROR"))
        assertEquals(0.0, count(registry, "lwm2m.commands.timeouts"))
        assertEquals(ErrorCode.INTERNAL_ERROR, results.values.single().error!!.code)
        assertEquals(0, handler.inFlight)
    }

    @Test fun `invalid and nonlocal messages do not inflate execution failures or latency`() {
        val registry = SimpleMeterRegistry()
        val handler = handler(registry, Results(), local = false) { fail("No dispatch expected") }
        handler.handle(record()) {}
        handler.handle(ConsumerRecord("commands", 0, 1L, "device", "{broken".toByteArray())) {}
        assertEquals(1.0, count(registry, "lwm2m.commands.skipped"))
        assertEquals(1.0, count(registry, "lwm2m.commands.invalid"))
        assertEquals(0.0, count(registry, "lwm2m.commands.started"))
        assertNull(registry.find("lwm2m.commands.duration").timer())
        assertNull(registry.find("lwm2m.commands.errors").counter())
    }

    @Test fun `result publication failure does not turn successful device operation into execution failure`() {
        val registry = SimpleMeterRegistry()
        val publisher = object : CommandPublisher {
            override fun result(result: KafkaCommandResult) { error("publisher failed") }
            override fun invalid(message: InvalidKafkaCommand) = Unit
        }
        val handler = handler(registry, publisher) { CompletableFuture.completedFuture(success()) }
        handler.handle(record()) {}
        assertEquals(1.0, count(registry, "lwm2m.commands.completed", "operation", "READ", "outcome", "success"))
        assertNull(registry.find("lwm2m.commands.errors").counter())
        assertEquals(0, handler.inFlight)
    }
}
