package io.lwm2m.server.adapter.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import io.lwm2m.core.*
import io.lwm2m.server.config.Lwm2mProperties
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

/** Best effort, bounded in-memory buffering. Never blocks Leshan's protocol threads on Kafka metadata. */
@Component
class KafkaEventPublisher(private val kafka: KafkaTemplate<String, String>, private val mapper: ObjectMapper,
                          private val properties: Lwm2mProperties, registry: MeterRegistry) : PublishDeviceEvent {
    private val log = LoggerFactory.getLogger(javaClass)
    private val dropped = registry.counter("lwm2m.events.dropped")
    private val published = registry.counter("lwm2m.events.published")
    private val failed = registry.counter("lwm2m.events.failed")
    private val executor = ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
        ArrayBlockingQueue<Runnable>(properties.events.bufferCapacity.also { require(it > 0) }),
        { task -> Thread(task, "lwm2m-kafka-publisher").apply { isDaemon = true } }, ThreadPoolExecutor.AbortPolicy())

    override fun publish(event: DeviceEvent) {
        try {
            executor.execute {
                try {
                    val topic = if (event.type == EventType.SEND) properties.events.sendTopic else properties.events.observationTopic
                    kafka.send(topic, event.endpoint, mapper.writeValueAsString(event)).whenComplete { _, error ->
                        if (error == null) published.increment()
                        else { failed.increment(); log.warn("Kafka publication failed: eventId={}, endpoint={}", event.eventId, event.endpoint, error) }
                    }
                } catch (e: Exception) {
                    failed.increment()
                    log.warn("Kafka publication failed: eventId={}, endpoint={}", event.eventId, event.endpoint, e)
                }
            }
        } catch (_: RejectedExecutionException) {
            dropped.increment()
            log.warn("Kafka event buffer is full or stopped; dropping eventId={}, endpoint={}", event.eventId, event.endpoint)
        }
    }

    @PreDestroy
    fun close() {
        executor.shutdown()
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            val remaining = executor.shutdownNow().size
            dropped.increment(remaining.toDouble())
        }
    }
}
