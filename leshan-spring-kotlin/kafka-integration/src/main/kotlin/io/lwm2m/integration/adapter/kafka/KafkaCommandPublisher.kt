package io.lwm2m.integration.adapter.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import io.lwm2m.integration.config.Lwm2mProperties
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

/** Result publication never calls the device again, including after a Kafka failure. */
class KafkaCommandPublisher(private val kafka: KafkaTemplate<String, String>, private val mapper: ObjectMapper,
                            private val properties: Lwm2mProperties, registry: MeterRegistry) : CommandPublisher, AutoCloseable {
    private val log = LoggerFactory.getLogger(javaClass)
    private val failed = registry.counter("lwm2m.commands.publication.failed")
    private val published = registry.counter("lwm2m.commands.publication.succeeded")
    private val dropped = registry.counter("lwm2m.commands.publication.dropped")
    private val executor = ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
        ArrayBlockingQueue<Runnable>(properties.commands.resultBufferCapacity.also { require(it > 0) }),
        { task -> Thread(task, "lwm2m-command-results").apply { isDaemon = true } }, ThreadPoolExecutor.AbortPolicy())

    override fun result(result: KafkaCommandResult) = publish(properties.commands.resultTopic, result.endpoint, result)
    override fun invalid(message: InvalidKafkaCommand) = publish(properties.commands.invalidTopic, message.key, message)

    private fun publish(topic: String, key: String?, message: Any) {
        try {
            executor.execute {
                try {
                    kafka.send(topic, key, mapper.writeValueAsString(message)).whenComplete { _, error ->
                        if (error == null) published.increment()
                        else { failed.increment(); log.warn("Command publication failed: topic={}, key={}", topic, key, error) }
                    }
                } catch (e: Exception) {
                    failed.increment()
                    log.warn("Command publication failed: topic={}, key={}", topic, key, e)
                }
            }
        } catch (_: RejectedExecutionException) {
            dropped.increment()
            log.warn("Command result buffer full or stopped: topic={}, key={}", topic, key)
        }
    }

    override fun close() {
        executor.shutdown()
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) dropped.increment(executor.shutdownNow().size.toDouble())
    }
}
