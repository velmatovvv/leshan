package io.lwm2m.integration.autoconfigure

import io.lwm2m.core.ExecuteDeviceCommand
import io.lwm2m.core.HasLocalRegistration
import io.lwm2m.integration.adapter.kafka.*
import io.lwm2m.integration.config.Lwm2mProperties
import io.lwm2m.integration.config.ServerIdentity
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.*
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.KafkaTemplate

@AutoConfiguration(after = [LeshanKafkaInfrastructureAutoConfiguration::class])
@ConditionalOnClass(KafkaTemplate::class)
@ConditionalOnBean(value = [Lwm2mKafkaClient::class, ExecuteDeviceCommand::class, HasLocalRegistration::class])
@ConditionalOnProperty("lwm2m.commands.enabled", havingValue = "true")
class LeshanKafkaCommandsAutoConfiguration(properties: Lwm2mProperties) {
    init {
        require(properties.serverId.isNotBlank()) { "lwm2m.server-id is required when Kafka commands are enabled" }
    }

    @Bean
    @ConditionalOnMissingBean(CommandPublisher::class)
    fun lwm2mKafkaCommandPublisher(client: Lwm2mKafkaClient, json: Lwm2mJson, properties: Lwm2mProperties, metrics: Lwm2mMetrics) =
        KafkaCommandPublisher(client.template, json.mapper, properties, metrics.registry)

    @Bean
    @ConditionalOnMissingBean
    fun lwm2mCommandHandler(json: Lwm2mJson, commands: ExecuteDeviceCommand, registrations: HasLocalRegistration,
                       publisher: CommandPublisher, identity: ServerIdentity, metrics: Lwm2mMetrics) =
        KafkaCommandHandler(CommandCodec(json.mapper), commands, registrations, publisher, identity, metrics.registry)

    @Bean
    @ConditionalOnMissingBean
    fun lwm2mCommandConsumer(client: Lwm2mKafkaClient, handler: KafkaCommandHandler, properties: Lwm2mProperties,
                        identity: ServerIdentity, metrics: Lwm2mMetrics) =
        KafkaCommandConsumer(client.consumerFactory, handler, properties, identity, metrics.registry)
}
