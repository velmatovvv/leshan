package io.lwm2m.server.config

import io.lwm2m.core.*
import io.lwm2m.server.adapter.leshan.LeshanGateway
import org.eclipse.leshan.core.model.ObjectLoader
import org.eclipse.leshan.server.LeshanServer
import org.eclipse.leshan.server.LeshanServerBuilder
import org.eclipse.leshan.server.model.VersionedModelProvider
import org.eclipse.leshan.server.redis.RedisRegistrationStore
import org.eclipse.leshan.server.redis.RedisSecurityStore
import org.eclipse.leshan.transport.californium.server.endpoint.CaliforniumServerEndpointsProvider
import org.eclipse.leshan.transport.californium.server.endpoint.coap.CoapServerProtocolProvider
import org.eclipse.leshan.transport.californium.server.endpoint.coaps.CoapsServerProtocolProvider
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import redis.clients.jedis.JedisPool
import java.net.URI
import java.io.File

@ConfigurationProperties("lwm2m")
data class Lwm2mProperties(
    var coapUri: String = "coap://0.0.0.0:5683",
    var coapsUri: String = "coaps://0.0.0.0:5684",
    var modelsDirectory: String? = null,
    var redis: Redis = Redis(),
    var events: Events = Events()
) {
    data class Redis(var enabled: Boolean = true, var uri: String = "redis://localhost:6379", var prefix: String = "")
    data class Events(var observationTopic: String = "lwm2m.observations", var sendTopic: String = "lwm2m.send", var bufferCapacity: Int = 1000)
}

@Configuration
@EnableConfigurationProperties(Lwm2mProperties::class)
class ServerConfiguration {
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty("lwm2m.redis.enabled", havingValue = "true", matchIfMissing = true)
    fun jedisPool(properties: Lwm2mProperties) = JedisPool(URI(properties.redis.uri))

    // Start only after the gateway has attached observation and Send listeners.
    @Bean(destroyMethod = "destroy")
    fun leshanServer(properties: Lwm2mProperties, pool: org.springframework.beans.factory.ObjectProvider<JedisPool>): LeshanServer {
        val endpoints = CaliforniumServerEndpointsProvider.Builder(CoapServerProtocolProvider(), CoapsServerProtocolProvider())
            .addEndpoint(properties.coapUri).apply {
                if (properties.coapsUri.isNotBlank()) addEndpoint(properties.coapsUri)
            }.build()
        val models = ObjectLoader.loadAllDefault().toMutableList()
        properties.modelsDirectory?.takeIf { it.isNotBlank() }?.let { directory ->
            require(File(directory).isDirectory) { "models-directory must be an existing directory" }
            models.addAll(ObjectLoader.loadObjectsFromDir(File(directory)))
        }
        return LeshanServerBuilder().setEndpointsProviders(endpoints)
            .setObjectModelProvider(VersionedModelProvider(models)).apply {
                if (properties.redis.enabled) {
                    val jedis = pool.getObject()
                    setSecurityStore(RedisSecurityStore.Builder(jedis).setPrefix(properties.redis.prefix + "SECSTORE#").build())
                    setRegistrationStore(RedisRegistrationStore.Builder(jedis).setPrefix(properties.redis.prefix + "REGSTORE#").build())
                }
            }.build()
    }

    @Bean(initMethod = "start")
    fun deviceGateway(server: LeshanServer, publisher: PublishDeviceEvent) = LeshanGateway(server, publisher)

    @Bean
    fun deviceService(gateway: DeviceGateway) = DeviceService(gateway)
}
