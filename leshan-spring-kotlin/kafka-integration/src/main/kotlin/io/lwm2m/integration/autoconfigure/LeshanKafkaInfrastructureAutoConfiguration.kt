package io.lwm2m.integration.autoconfigure

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.*
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Conditional
import org.springframework.context.annotation.ConfigurationCondition.ConfigurationPhase
import org.springframework.kafka.core.KafkaTemplate

class KafkaFeatureEnabled : AnyNestedCondition(ConfigurationPhase.PARSE_CONFIGURATION) {
    @ConditionalOnProperty("lwm2m.events.enabled", havingValue = "true") class Events
    @ConditionalOnProperty("lwm2m.commands.enabled", havingValue = "true") class Commands
}

@AutoConfiguration(after = [LeshanIntegrationAutoConfiguration::class])
@ConditionalOnClass(KafkaTemplate::class)
@ConditionalOnBean(Lwm2mJson::class)
@Conditional(KafkaFeatureEnabled::class)
@EnableConfigurationProperties(KafkaProperties::class)
class LeshanKafkaInfrastructureAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    fun lwm2mKafkaClient(properties: KafkaProperties) = Lwm2mKafkaClient(properties)

    @Bean
    @ConditionalOnMissingBean
    fun lwm2mMetrics(registry: ObjectProvider<MeterRegistry>): Lwm2mMetrics {
        val existing = registry.ifUnique
        return Lwm2mMetrics(existing ?: SimpleMeterRegistry(), existing == null)
    }
}
