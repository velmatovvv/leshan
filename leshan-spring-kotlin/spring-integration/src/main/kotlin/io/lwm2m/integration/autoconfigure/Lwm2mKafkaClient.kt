package io.lwm2m.integration.autoconfigure

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate

/** Encapsulation keeps module clients out of the host's KafkaTemplate/ConsumerFactory bean selection. */
class Lwm2mKafkaClient(properties: KafkaProperties) : AutoCloseable {
    private val producerFactory: DefaultKafkaProducerFactory<String, String>
    val template: KafkaTemplate<String, String>
    val consumerFactory: ConsumerFactory<String, ByteArray>

    init {
        val producer = properties.buildProducerProperties().toMutableMap()
        producer["key.serializer"] = StringSerializer::class.java
        producer["value.serializer"] = StringSerializer::class.java
        mapOf<String, Any>("acks" to "all", "enable.idempotence" to true, "delivery.timeout.ms" to 30000,
            "request.timeout.ms" to 10000, "max.block.ms" to 1000, "buffer.memory" to 33554432).forEach { (key, value) -> producer.putIfAbsent(key, value) }
        producerFactory = DefaultKafkaProducerFactory(producer)
        template = KafkaTemplate(producerFactory)
        val consumer = properties.buildConsumerProperties().toMutableMap()
        consumer[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        consumer[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 1
        consumer.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
        consumer[ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG] = false
        consumer[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        consumer[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java
        consumerFactory = DefaultKafkaConsumerFactory(consumer)
    }

    override fun close() { template.destroy(); producerFactory.destroy() }
}
