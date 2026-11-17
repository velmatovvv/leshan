package io.lwm2m.server

import io.lwm2m.server.config.Lwm2mProperties
import io.lwm2m.server.config.ServerConfiguration
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Test
import org.mockito.Mockito.*
import org.springframework.beans.factory.ObjectProvider
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool

class RedisStoreConfigurationTest {
    @Suppress("UNCHECKED_CAST")
    @Test fun `security uses existing Redis keys and registrations are local by default`() {
        val pool = mock(JedisPool::class.java)
        val jedis = mock(Jedis::class.java)
        `when`(pool.resource).thenReturn(jedis)
        val provider = mock(ObjectProvider::class.java) as ObjectProvider<JedisPool>
        `when`(provider.getObject()).thenReturn(pool)
        val server = ServerConfiguration().leshanServer(Lwm2mProperties(coapUri = "coap://127.0.0.1:0", coapsUri = ""), provider)
        try {
            assertNull(server.securityStore.getByEndpoint("existing-device"))
            assertNull(server.registrationStore.getRegistrationByEndpoint("existing-device"))
            verify(jedis).get("SECSTORE#SEC#EP#existing-device".toByteArray())
            assertInstanceOf(org.eclipse.leshan.server.registration.InMemoryRegistrationStore::class.java, server.registrationStore)
        } finally { server.destroy() }
    }

    @Suppress("UNCHECKED_CAST")
    @Test fun `optional Redis registration stores are isolated by server identity`() {
        val pool = mock(JedisPool::class.java)
        val jedis = mock(Jedis::class.java)
        `when`(pool.resource).thenReturn(jedis)
        val provider = mock(ObjectProvider::class.java) as ObjectProvider<JedisPool>
        `when`(provider.getObject()).thenReturn(pool)
        for (id in listOf("node-A", "node-B")) {
            val properties = Lwm2mProperties(coapUri = "coap://127.0.0.1:0", coapsUri = "", serverId = id,
                registrations = Lwm2mProperties.Registrations(redisEnabled = true))
            val server = ServerConfiguration().leshanServer(properties, provider)
            try {
                assertNull(server.registrationStore.getRegistrationByEndpoint("device"))
                verify(jedis).get("REGSTORE#$id#REG#EP#device".toByteArray())
            } finally { server.destroy() }
        }
    }
}
