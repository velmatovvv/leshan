package io.lwm2m.integration

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.lwm2m.core.*
import io.lwm2m.integration.adapter.kafka.KafkaEventPublisher
import io.lwm2m.integration.config.Lwm2mProperties
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.Mockito.*
import org.springframework.kafka.core.KafkaTemplate
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class KafkaEventPublisherTest {
    @Suppress("UNCHECKED_CAST")
    @Test fun `unavailable Kafka does not block protocol callbacks and buffer overflow is counted`() {
        val kafka = mock(KafkaTemplate::class.java) as KafkaTemplate<String, String>
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        `when`(kafka.send(anyString(), anyString(), anyString())).thenAnswer {
            entered.countDown()
            assertTrue(release.await(3, TimeUnit.SECONDS))
            CompletableFuture.failedFuture<org.springframework.kafka.support.SendResult<String, String>>(IllegalStateException("Kafka unavailable"))
        }
        val registry = SimpleMeterRegistry()
        val publisher = KafkaEventPublisher(kafka, jacksonObjectMapper().findAndRegisterModules(),
            Lwm2mProperties(events = Lwm2mProperties.Events(bufferCapacity = 1)), registry)
        val event = DeviceEvent(EventType.SEND, "test", "reg", emptyList())
        try {
            publisher.publish(event)
            assertTrue(entered.await(3, TimeUnit.SECONDS))
            publisher.publish(event)
            publisher.publish(event)
            assertEquals(1.0, registry.counter("lwm2m.events.dropped").count())
        } finally { release.countDown(); publisher.close() }
        assertEquals(2.0, registry.counter("lwm2m.events.failed").count())
        assertEquals(0.0, registry.counter("lwm2m.events.published").count())
    }
}
