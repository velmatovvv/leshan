package io.lwm2m.integration.autoconfigure

import io.lwm2m.core.PublishDeviceEvent
import io.lwm2m.integration.adapter.kafka.KafkaEventPublisher
import io.lwm2m.integration.config.Lwm2mProperties
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.*
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.KafkaTemplate

@AutoConfiguration(after = [LeshanKafkaInfrastructureAutoConfiguration::class])
@ConditionalOnClass(KafkaTemplate::class)
@ConditionalOnBean(Lwm2mKafkaClient::class)
@ConditionalOnProperty("lwm2m.events.enabled", havingValue = "true")
class LeshanKafkaEventsAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean(PublishDeviceEvent::class)
    fun lwm2mKafkaEventPublisher(client: Lwm2mKafkaClient, json: Lwm2mJson, properties: Lwm2mProperties, metrics: Lwm2mMetrics) =
        KafkaEventPublisher(client.template, json.mapper, properties, metrics.registry)
}
