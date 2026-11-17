package io.lwm2m.application.config

import io.lwm2m.core.DeviceGateway
import org.eclipse.leshan.core.model.ObjectLoader
import org.eclipse.leshan.server.LeshanServer
import org.eclipse.leshan.server.LeshanServerBuilder
import org.eclipse.leshan.server.model.VersionedModelProvider
import org.eclipse.leshan.server.redis.RedisRegistrationStore
import org.eclipse.leshan.server.redis.RedisSecurityStore
import org.eclipse.leshan.transport.californium.server.endpoint.CaliforniumServerEndpointsProvider
import org.eclipse.leshan.transport.californium.server.endpoint.coap.CoapServerProtocolProvider
import org.eclipse.leshan.transport.californium.server.endpoint.coaps.CoapsServerProtocolProvider
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import redis.clients.jedis.JedisPool
import java.io.File
import java.net.URI

@ConfigurationProperties("lwm2m")
data class StandaloneProperties(
    var coapUri: String = "coap://0.0.0.0:5683",
    var coapsUri: String = "coaps://0.0.0.0:5684",
    var modelsDirectory: String? = null,
    var serverId: String = "",
    var redis: Redis = Redis(),
    var registrations: Registrations = Registrations()
) {
    data class Redis(var enabled: Boolean = true, var uri: String = "redis://localhost:6379", var prefix: String = "")
    data class Registrations(var redisEnabled: Boolean = false, var redisPrefix: String = "")
}

@Configuration
@EnableConfigurationProperties(StandaloneProperties::class)
class ServerConfiguration {
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty("lwm2m.redis.enabled", havingValue = "true", matchIfMissing = true)
    fun jedisPool(properties: StandaloneProperties) = JedisPool(URI(properties.redis.uri))

    @Bean(destroyMethod = "destroy")
    @ConditionalOnMissingBean(LeshanServer::class)
    fun leshanServer(properties: StandaloneProperties, pool: ObjectProvider<JedisPool>): LeshanServer {
        require(!properties.registrations.redisEnabled || properties.redis.enabled) { "Redis registrations require lwm2m.redis.enabled=true" }
        val endpoints = CaliforniumServerEndpointsProvider.Builder(CoapServerProtocolProvider(), CoapsServerProtocolProvider())
            .addEndpoint(properties.coapUri).apply { if (properties.coapsUri.isNotBlank()) addEndpoint(properties.coapsUri) }.build()
        val models = ObjectLoader.loadAllDefault().toMutableList()
        properties.modelsDirectory?.takeIf { it.isNotBlank() }?.let {
            require(File(it).isDirectory) { "models-directory must be an existing directory" }
            models.addAll(ObjectLoader.loadObjectsFromDir(File(it)))
        }
        return LeshanServerBuilder().setEndpointsProviders(endpoints).setObjectModelProvider(VersionedModelProvider(models)).apply {
            if (properties.redis.enabled) {
                val jedis = pool.getObject()
                setSecurityStore(RedisSecurityStore.Builder(jedis).setPrefix(properties.redis.prefix + "SECSTORE#").build())
                if (properties.registrations.redisEnabled) {
                    val serverId = properties.serverId.also { require(it.isNotBlank()) { "server-id is required for isolated Redis registrations" } }
                    val prefix = properties.registrations.redisPrefix.ifBlank { properties.redis.prefix + "REGSTORE#$serverId#" }
                    setRegistrationStore(RedisRegistrationStore.Builder(jedis).setPrefix(prefix).build())
                }
            }
        }.build()
    }

    @Bean
    fun standaloneServerLifecycle(server: LeshanServer, gateway: ObjectProvider<DeviceGateway>): SmartLifecycle = object : SmartLifecycle {
        @Volatile private var running = false
        override fun start() {
            gateway.ifAvailable // Ensure integration listeners are attached before standalone startup.
            server.start()
            running = true
        }
        override fun stop() { server.stop(); running = false }
        override fun isRunning() = running
        override fun getPhase() = 0
    }
}
