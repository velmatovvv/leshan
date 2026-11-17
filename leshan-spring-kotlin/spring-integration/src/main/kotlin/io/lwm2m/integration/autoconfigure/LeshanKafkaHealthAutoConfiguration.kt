package io.lwm2m.integration.autoconfigure

import io.lwm2m.integration.adapter.kafka.KafkaCommandConsumer
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.*
import org.springframework.context.annotation.Bean

@AutoConfiguration(after = [LeshanKafkaCommandsAutoConfiguration::class])
@ConditionalOnClass(HealthIndicator::class)
@ConditionalOnBean(KafkaCommandConsumer::class)
class LeshanKafkaHealthAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean(name = ["kafkaCommandsHealth"])
    fun kafkaCommandsHealth(consumer: KafkaCommandConsumer) = HealthIndicator {
        (if (consumer.isRunning) Health.up() else Health.down())
            .withDetail("groupId", consumer.groupId).withDetail("assignedPartitions", consumer.assignedPartitions.size)
            .withDetail("paused", consumer.paused).build()
    }
}
