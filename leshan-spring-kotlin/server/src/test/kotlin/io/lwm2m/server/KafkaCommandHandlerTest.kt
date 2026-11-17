package io.lwm2m.server

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.lwm2m.core.*
import io.lwm2m.server.adapter.kafka.*
import io.lwm2m.server.config.ServerIdentity
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.CompletableFuture

class KafkaCommandHandlerTest {
    private val mapper = jacksonObjectMapper().findAndRegisterModules()
    private class Results : CommandPublisher {
        val results = mutableListOf<KafkaCommandResult>()
        val invalid = mutableListOf<InvalidKafkaCommand>()
        override fun result(result: KafkaCommandResult) { results.add(result) }
        override fun invalid(message: InvalidKafkaCommand) { invalid.add(message) }
    }
    private class Commands(val action: (DeviceCommand) -> CompletableFuture<CommandResult>) : ExecuteDeviceCommand {
        override fun execute(command: DeviceCommand) = action(command)
    }
    private fun record(id: String = "cmd", operation: Operation = Operation.READ) = ConsumerRecord("lwm2m.commands", 0, 42L, "device",
        mapper.writeValueAsBytes(KafkaDeviceCommand(1, id, "device", operation, Instant.now().plusSeconds(60), path = "/3/0/0")))
    private fun handler(commands: ExecuteDeviceCommand, results: Results, local: Boolean = true) = KafkaCommandHandler(
        CommandCodec(mapper), commands, HasLocalRegistration { local }, results, ServerIdentity("node-A"), SimpleMeterRegistry())

    @Test fun `dispatch happens before commit without waiting for device response`() {
        val events = mutableListOf<String>()
        val pending = CompletableFuture<CommandResult>()
        val results = Results()
        val handler = handler(Commands { events.add("send"); pending }, results)
        handler.handle(record()) { events.add("commit") }
        assertEquals(listOf("send", "commit"), events)
        assertFalse(pending.isDone)
        assertTrue(results.results.isEmpty())
        assertEquals(1, handler.inFlight)
        pending.complete(CommandResult("cmd", "device", Operation.READ, response = DeviceResponse("CONTENT(205)", true)))
        assertEquals("cmd", results.results.single().commandId)
        assertEquals("node-A", results.results.single().serverId)
        assertEquals(0, handler.inFlight)
    }

    @Test fun `commands to the same device run concurrently and may finish out of order`() {
        val pending = mutableMapOf<String, CompletableFuture<CommandResult>>()
        val results = Results()
        val handler = handler(Commands { CompletableFuture<CommandResult>().also { f -> pending[it.commandId] = f } }, results)
        val committed = mutableListOf<String>()
        handler.handle(record("first")) { committed.add("first") }
        handler.handle(record("second")) { committed.add("second") }
        assertEquals(listOf("first", "second"), committed)
        assertEquals(2, handler.inFlight)
        pending.getValue("second").complete(CommandResult("second", "device", Operation.READ))
        pending.getValue("first").complete(CommandResult("first", "device", Operation.READ))
        assertEquals(listOf("second", "first"), results.results.map { it.commandId })
        assertEquals(0, handler.inFlight)
    }

    @Test fun `nonlocal registration is committed without execution or result`() {
        var committed = false
        val results = Results()
        val handler = handler(Commands { fail("Nonlocal command must not execute") }, results, local = false)
        handler.handle(record()) { committed = true }
        assertTrue(committed)
        assertTrue(results.results.isEmpty())
        assertEquals(0, handler.inFlight)
    }

    @Test fun `commit failure never triggers another device operation`() {
        var sends = 0
        val pending = CompletableFuture<CommandResult>()
        val results = Results()
        val handler = handler(Commands { sends++; pending }, results)
        assertThrows(IllegalStateException::class.java) { handler.handle(record()) { error("commit failed") } }
        pending.complete(CommandResult("cmd", "device", Operation.READ))
        assertEquals(1, sends)
        assertEquals(1, results.results.size)
        assertEquals(0, handler.inFlight)
    }

    @Test fun `immediate result is published after the commit attempt`() {
        var committed = false
        val results = Results()
        val handler = handler(Commands {
            assertFalse(committed)
            CompletableFuture.completedFuture(CommandResult(it.commandId, it.endpoint, it.operation,
                error = CommandError(ErrorCode.DEVICE_SLEEPING, "sleeping")))
        }, results)
        handler.handle(record()) { committed = true; assertTrue(results.results.isEmpty()) }
        assertEquals(ErrorCode.DEVICE_SLEEPING, results.results.single().error!!.code)
        assertEquals(0, handler.inFlight)
    }

    @Test fun `invalid envelopes are committed and published with source coordinates`() {
        val results = Results()
        val handler = handler(Commands { fail("Invalid command must not execute") }, results)
        var commits = 0
        val cases = listOf<ByteArray?>(null, byteArrayOf(0, 1), "{}".toByteArray(),
            """{"schemaVersion":1,"schemaVersion":1}""".toByteArray())
        cases.forEach { handler.handle(ConsumerRecord("input", 2, 9L, "device", it)) { commits++ } }
        assertEquals(cases.size, commits)
        assertEquals(cases.size, results.invalid.size)
        assertTrue(results.invalid.all { it.sourceTopic == "input" && it.sourcePartition == 2 && it.sourceOffset == 9L })
        assertTrue(results.results.isEmpty())
    }

    @Test fun `codec requires key expiry and supported schema and applies Kafka timeout default`() {
        val codec = CommandCodec(mapper)
        val valid = """{"schemaVersion":1,"commandId":"x","endpoint":"device","operation":"READ","path":"/3/0/0","expiresAt":"2099-01-01T00:00:00Z"}"""
        assertEquals(30000L, codec.decode("device", valid.toByteArray()).timeoutMs)
        assertThrows(IllegalArgumentException::class.java) { codec.decode("wrong-key", valid.toByteArray()) }
        assertThrows(IllegalArgumentException::class.java) { codec.decode("device", valid.replace("\"schemaVersion\":1", "\"schemaVersion\":2").toByteArray()) }
        assertThrows(Exception::class.java) { codec.decode("device", valid.replace(",\"expiresAt\":\"2099-01-01T00:00:00Z\"", "").toByteArray()) }
        assertThrows(Exception::class.java) { codec.decode("device", valid.replace("\"operation\":\"READ\"", "\"operation\":0").toByteArray()) }
    }
}
