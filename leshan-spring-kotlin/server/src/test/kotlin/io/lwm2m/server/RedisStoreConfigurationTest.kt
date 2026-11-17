package io.lwm2m.server

import io.lwm2m.server.config.Lwm2mProperties
import io.lwm2m.server.config.ServerConfiguration
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.mockito.Mockito.*
import org.springframework.beans.factory.ObjectProvider
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool

class RedisStoreConfigurationTest {
    @Suppress("UNCHECKED_CAST")
    @Test fun `existing Redis keys use standard Leshan store namespaces`() {
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
            verify(jedis).get("REGSTORE#REG#EP#existing-device".toByteArray())
        } finally { server.destroy() }
    }
}
