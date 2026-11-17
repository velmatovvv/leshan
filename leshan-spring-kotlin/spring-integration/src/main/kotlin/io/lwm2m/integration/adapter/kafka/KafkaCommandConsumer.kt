package io.lwm2m.integration.adapter.kafka

import io.lwm2m.integration.config.Lwm2mProperties
import io.lwm2m.integration.config.ServerIdentity
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.kafka.core.ConsumerFactory
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/** One poll owner. max.poll.records=1 lets us pause exactly at the in-flight limit, with no
 * application command queue. Paused consumers still poll to maintain group membership. */
class KafkaCommandConsumer(
    private val factory: ConsumerFactory<String, ByteArray>,
    private val handler: KafkaCommandHandler,
    properties: Lwm2mProperties,
    identity: ServerIdentity,
    registry: MeterRegistry
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(javaClass)
    private val running = AtomicBoolean()
    private val consumerRef = AtomicReference<Consumer<String, ByteArray>?>()
    private val failureCount = registry.counter("lwm2m.commands.consumer.failures")
    private val pauseCount = registry.counter("lwm2m.commands.consumer.pauses")
    private val topic = properties.commands.topic
    val groupId: String = properties.commands.groupId.ifBlank { "lwm2m-server-${identity.serverId}" }
    private val maxInFlight = properties.commands.maxInFlight.also { require(it > 0) { "max-in-flight must be positive" } }
    @Volatile private var thread: Thread? = null
    @Volatile var assignedPartitions: Set<TopicPartition> = emptySet()
        private set
    @Volatile var paused: Boolean = false
        private set

    init {
        require(setOf(topic, properties.commands.resultTopic, properties.commands.invalidTopic).size == 3) { "Command, result and invalid topics must be distinct" }
        require(listOf(topic, properties.commands.resultTopic, properties.commands.invalidTopic, groupId).all { it.isNotBlank() }) { "Kafka command topics and group-id must not be blank" }
    }

    override fun isRunning() = running.get()
    override fun getPhase() = Int.MAX_VALUE - 100
    override fun start() {
        if (!running.compareAndSet(false, true)) return
        thread = Thread({ consume() }, "lwm2m-commands-$groupId").apply { isDaemon = true; start() }
    }

    private fun flowControl(consumer: Consumer<String, ByteArray>) {
        val partitions = consumer.assignment()
        assignedPartitions = partitions.toSet()
        val shouldPause = handler.inFlight >= maxInFlight
        if (shouldPause) {
            val newlyPaused = partitions - consumer.paused()
            if (newlyPaused.isNotEmpty()) { consumer.pause(newlyPaused); pauseCount.increment() }
        } else {
            val toResume = consumer.paused().intersect(partitions)
            if (toResume.isNotEmpty()) consumer.resume(toResume)
        }
        paused = shouldPause && partitions.isNotEmpty()
    }

    private fun consume() {
        var consumer: Consumer<String, ByteArray>? = null
        try {
            consumer = factory.createConsumer(groupId, "lwm2m-commands-$groupId", null)
            consumerRef.set(consumer)
            val owner = consumer
            consumer.subscribe(listOf(topic), object : ConsumerRebalanceListener {
                override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) { assignedPartitions = assignedPartitions - partitions.toSet() }
                override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) { flowControl(owner) }
            })
            while (running.get()) {
                flowControl(consumer)
                val records = consumer.poll(Duration.ofMillis(100))
                for (record in records) {
                    if (!running.get()) break
                    check(handler.inFlight < maxInFlight) { "Consumer delivered a record above the in-flight limit; max.poll.records must be 1" }
                    handler.handle(record) {
                        consumer.commitSync(mapOf(TopicPartition(record.topic(), record.partition()) to OffsetAndMetadata(record.offset() + 1)), Duration.ofSeconds(10))
                    }
                }
            }
        } catch (e: WakeupException) {
            if (running.get()) { failureCount.increment(); log.error("Kafka command consumer interrupted unexpectedly", e) }
        } catch (e: Exception) {
            // A dispatch may already have happened. Do not retry it in an error-handler loop.
            // A later consumer restart may replay uncommitted input; duplicates are permitted.
            failureCount.increment()
            log.error("Kafka command consumer stopped; groupId={}. No automatic command retry.", groupId, e)
        } finally {
            running.set(false)
            consumerRef.set(null)
            try { consumer?.close(Duration.ofSeconds(5)) } catch (e: Exception) { log.warn("Kafka consumer close failed", e) }
            assignedPartitions = emptySet()
            paused = false
        }
    }

    override fun stop() {
        running.set(false)
        consumerRef.get()?.wakeup()
        thread?.takeIf { it !== Thread.currentThread() }?.join(15_000)
    }
    override fun stop(callback: Runnable) { try { stop() } finally { callback.run() } }
}
