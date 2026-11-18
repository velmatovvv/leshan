package io.lwm2m.integration

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.lwm2m.core.*
import io.lwm2m.integration.adapter.kafka.*
import io.lwm2m.integration.config.Lwm2mProperties
import io.lwm2m.integration.config.ServerIdentity
import io.micrometer.core.instrument.MockClock
import io.micrometer.core.instrument.simple.SimpleConfig
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
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class ConsumerPauseMetricsTest {
    private class Fixture : AutoCloseable {
        val clock = MockClock()
        val registry = SimpleMeterRegistry(SimpleConfig.DEFAULT, clock)
        val partition = TopicPartition("commands", 0)
        val broker = MockConsumer<String, ByteArray>(OffsetResetStrategy.EARLIEST)
        val pending = CompletableFuture<CommandResult>()
        private val failure = AtomicReference<Throwable?>()
        private val mapper = jacksonObjectMapper().findAndRegisterModules()
        private val identity = ServerIdentity("pause-metrics")
        val handler = KafkaCommandHandler(CommandCodec(mapper), object : ExecuteDeviceCommand {
            override fun execute(command: DeviceCommand) = pending
        }, HasLocalRegistration { true }, object : CommandPublisher {
            override fun result(result: KafkaCommandResult) = Unit
            override fun invalid(message: InvalidKafkaCommand) = fail<Unit>(message.toString())
        }, identity, registry)
        @Suppress("UNCHECKED_CAST")
        private val factory = (mock(ConsumerFactory::class.java) as ConsumerFactory<String, ByteArray>).also {
            `when`(it.createConsumer(anyString(), anyString(), isNull())).thenReturn(broker)
        }
        val consumer = KafkaCommandConsumer(factory, handler, Lwm2mProperties(commands = Lwm2mProperties.Commands(
            topic = "commands", maxInFlight = 1)), identity, registry)

        init {
            broker.updateBeginningOffsets(mapOf(partition to 0L))
            onPoll {
                broker.rebalance(listOf(partition))
                broker.addRecord(ConsumerRecord("commands", 0, 0L, "device", mapper.writeValueAsBytes(
                    KafkaDeviceCommand(1, "test", "device", Operation.READ, Instant.now().plusSeconds(60), path = "/3/0/0"))))
            }
        }
        fun onPoll(action: () -> Unit) {
            broker.schedulePollTask {
                try { action() } catch (e: Throwable) { failure.set(e); consumer.stop() }
            }
        }
        fun run() {
            consumer.start()
            await().atMost(Duration.ofSeconds(5)).until { !consumer.isRunning }
            consumer.stop() // Join final cleanup before reading completed pause metrics.
            failure.get()?.let { throw AssertionError("Poll task assertion failed", it) }
            assertEquals(0.0, registry.counter("lwm2m.commands.consumer.failures").count())
        }
        fun complete() { pending.complete(CommandResult("test", "device", Operation.READ, response = DeviceResponse("CONTENT(205)", true))) }
        fun pausedGauge() = registry.get("lwm2m.commands.consumer.paused").gauge().value()
        fun activePause() = registry.get("lwm2m.commands.consumer.pause.active").longTaskTimer()
        fun completedPauses() = registry.get("lwm2m.commands.consumer.pause.duration").timer()
        override fun close() { complete(); consumer.stop(); registry.close() }
    }

    @Test fun `pause remains observable while waiting and records duration on resume`() {
        Fixture().use { f ->
            f.onPoll {
                assertTrue(f.broker.paused().contains(f.partition))
                assertEquals(1.0, f.pausedGauge())
                assertEquals(1, f.activePause().activeTasks())
                f.clock.add(Duration.ofSeconds(5))
                assertEquals(5.0, f.activePause().duration(TimeUnit.SECONDS))
                assertEquals(0L, f.completedPauses().count())
                f.complete()
            }
            f.onPoll {
                assertTrue(f.broker.paused().isEmpty())
                assertEquals(0.0, f.pausedGauge())
                f.consumer.stop()
            }
            f.run()
            assertEquals(1L, f.completedPauses().count())
            assertEquals(5.0, f.completedPauses().totalTime(TimeUnit.SECONDS))
            assertEquals(0, f.activePause().activeTasks())
            assertEquals(1.0, f.registry.counter("lwm2m.commands.consumer.pauses").count())
            assertEquals(0, f.handler.inFlight)
        }
    }

    @Test fun `shutdown closes active pause without waiting for the outstanding device request`() {
        Fixture().use { f ->
            f.onPoll {
                assertEquals(1.0, f.pausedGauge())
                f.clock.add(Duration.ofSeconds(4))
                f.consumer.stop()
            }
            f.run()
            assertFalse(f.pending.isDone)
            assertEquals(1L, f.completedPauses().count())
            assertEquals(4.0, f.completedPauses().totalTime(TimeUnit.SECONDS))
            assertEquals(0.0, f.pausedGauge())
            assertEquals(0, f.activePause().activeTasks())
            f.complete()
            assertEquals(0, f.handler.inFlight)
        }
    }

    @Test fun `revoking all partitions ends pause and reassignment starts a fresh interval`() {
        Fixture().use { f ->
            f.onPoll {
                f.clock.add(Duration.ofSeconds(2))
                f.broker.rebalance(emptyList())
                assertEquals(0.0, f.pausedGauge())
                assertEquals(0, f.activePause().activeTasks())
            }
            f.onPoll {
                f.broker.rebalance(listOf(f.partition))
                assertEquals(1.0, f.pausedGauge())
            }
            f.onPoll { f.clock.add(Duration.ofSeconds(3)); f.complete() }
            f.onPoll { f.consumer.stop() }
            f.run()
            assertEquals(2L, f.completedPauses().count())
            assertEquals(5.0, f.completedPauses().totalTime(TimeUnit.SECONDS))
            assertEquals(2.0, f.registry.counter("lwm2m.commands.consumer.pauses").count())
            assertEquals(0, f.activePause().activeTasks())
        }
    }
}
