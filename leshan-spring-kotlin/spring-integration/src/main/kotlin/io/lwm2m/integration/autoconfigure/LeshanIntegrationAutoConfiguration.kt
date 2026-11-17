package io.lwm2m.integration.autoconfigure

import io.lwm2m.core.*
import io.lwm2m.integration.adapter.leshan.LeshanGateway
import io.lwm2m.integration.config.Lwm2mProperties
import io.lwm2m.integration.config.ServerIdentity
import org.eclipse.leshan.server.LeshanServer
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.*
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean

@AutoConfiguration
@ConditionalOnClass(LeshanServer::class)
@ConditionalOnSingleCandidate(LeshanServer::class)
@ConditionalOnProperty("lwm2m.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(Lwm2mProperties::class)
class LeshanIntegrationAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    fun lwm2mJson() = Lwm2mJson()

    @Bean
    @ConditionalOnMissingBean
    fun lwm2mServerIdentity(properties: Lwm2mProperties): ServerIdentity {
        require(!properties.commands.enabled || properties.serverId.isNotBlank()) { "lwm2m.server-id is required when Kafka commands are enabled" }
        return ServerIdentity(properties.serverId.ifBlank { "standalone" })
    }

    @Bean(initMethod = "attach", destroyMethod = "detach")
    @ConditionalOnMissingBean(DeviceGateway::class)
    fun lwm2mDeviceGateway(server: LeshanServer, publishers: ObjectProvider<PublishDeviceEvent>): LeshanGateway {
        val sinks = publishers.orderedStream().toList()
        return LeshanGateway(server, PublishDeviceEvent { event -> sinks.forEach { it.publish(event) } })
    }

    @Bean
    @ConditionalOnMissingBean(value = [ExecuteDeviceCommand::class, QueryDevices::class])
    fun lwm2mDeviceService(gateway: DeviceGateway) = DeviceService(gateway)

    @Bean
    @ConditionalOnMissingBean
    fun lwm2mLocalRegistrations(server: LeshanServer): HasLocalRegistration = HasLocalRegistration {
        server.registrationService.getByEndpoint(it) != null
    }
}
