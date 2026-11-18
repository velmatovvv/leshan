package io.lwm2m.integration

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.lwm2m.core.*
import io.lwm2m.integration.adapter.kafka.*
import io.lwm2m.integration.config.Lwm2mProperties
import io.lwm2m.integration.config.ServerIdentity
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.Mockito.*
import org.springframework.kafka.core.ConsumerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

class KafkaCommitFailureTest {
    @Suppress("UNCHECKED_CAST")
    @Test fun `failed commit stops consumer after one dispatch and still allows its result`() {
        val topic = "lwm2m.commands"
        val partition = TopicPartition(topic, 0)
        val mockConsumer = spy(MockConsumer<String, ByteArray>(OffsetResetStrategy.EARLIEST))
        mockConsumer.updateBeginningOffsets(mapOf(partition to 0L))
        val mapper = jacksonObjectMapper().findAndRegisterModules()
        val value = mapper.writeValueAsBytes(KafkaDeviceCommand(1, "test", "device", Operation.READ,
            Instant.now().plusSeconds(60), path = "/3/0/0"))
        mockConsumer.schedulePollTask {
            mockConsumer.rebalance(listOf(partition))
            mockConsumer.addRecord(ConsumerRecord(topic, 0, 0L, "device", value))
        }
        doThrow(CommitFailedException("injected commit failure")).`when`(mockConsumer).commitSync(anyMap(), any(Duration::class.java))
        val factory = mock(ConsumerFactory::class.java) as ConsumerFactory<String, ByteArray>
        `when`(factory.createConsumer(anyString(), anyString(), isNull())).thenReturn(mockConsumer)
        val sends = AtomicInteger()
        val pending = CompletableFuture<CommandResult>()
        val results = ConcurrentLinkedQueue<KafkaCommandResult>()
        val registry = SimpleMeterRegistry()
        val identity = ServerIdentity("commit-failure")
        val handler = KafkaCommandHandler(CommandCodec(mapper), object : ExecuteDeviceCommand {
            override fun execute(command: DeviceCommand) = pending.also { sends.incrementAndGet() }
        }, HasLocalRegistration { true }, object : CommandPublisher {
            override fun result(result: KafkaCommandResult) { results.add(result) }
            override fun invalid(message: InvalidKafkaCommand) = fail<Unit>(message.toString())
        }, identity, registry)
        val consumer = KafkaCommandConsumer(factory, handler, Lwm2mProperties(), identity, registry)
        consumer.start()
        try {
            await().atMost(Duration.ofSeconds(5)).until { sends.get() == 1 && !consumer.isRunning }
            assertEquals(1.0, registry.counter("lwm2m.commands.consumer.failures").count())
            assertFalse(pending.isDone)
            pending.complete(CommandResult("test", "device", Operation.READ, response = DeviceResponse("CONTENT(205)", true)))
            assertEquals("test", results.single().commandId)
            assertEquals(0, handler.inFlight)
            assertEquals(1, sends.get())
            verify(mockConsumer, times(1)).commitSync(anyMap(), any(Duration::class.java))
        } finally { consumer.stop() }
    }
}
