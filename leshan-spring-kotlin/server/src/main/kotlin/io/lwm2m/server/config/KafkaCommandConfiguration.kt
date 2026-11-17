package io.lwm2m.server.config

import com.fasterxml.jackson.databind.ObjectMapper
import io.lwm2m.core.ExecuteDeviceCommand
import io.lwm2m.core.HasLocalRegistration
import io.lwm2m.server.adapter.kafka.*
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory

@Configuration
@ConditionalOnProperty("lwm2m.commands.enabled", havingValue = "true")
class KafkaCommandConfiguration {
    @Bean
    fun commandHandler(mapper: ObjectMapper, commands: ExecuteDeviceCommand, registrations: HasLocalRegistration,
                       publisher: CommandPublisher, identity: ServerIdentity, registry: MeterRegistry) =
        KafkaCommandHandler(CommandCodec(mapper), commands, registrations, publisher, identity, registry)

    @Bean
    fun commandConsumerFactory(properties: KafkaProperties): ConsumerFactory<String, ByteArray> {
        val config = properties.buildConsumerProperties().toMutableMap()
        config[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        config[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 1
        config.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
        config[ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG] = false
        config[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        config[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java
        return DefaultKafkaConsumerFactory(config)
    }

    @Bean
    fun commandConsumer(@Qualifier("commandConsumerFactory") factory: ConsumerFactory<String, ByteArray>,
                        handler: KafkaCommandHandler, properties: Lwm2mProperties, identity: ServerIdentity,
                        registry: MeterRegistry) = KafkaCommandConsumer(factory, handler, properties, identity, registry)

    @Bean
    fun kafkaCommandsHealth(consumer: KafkaCommandConsumer) = HealthIndicator {
        (if (consumer.isRunning) Health.up() else Health.down())
            .withDetail("groupId", consumer.groupId).withDetail("assignedPartitions", consumer.assignedPartitions.size)
            .withDetail("paused", consumer.paused).build()
    }
}
