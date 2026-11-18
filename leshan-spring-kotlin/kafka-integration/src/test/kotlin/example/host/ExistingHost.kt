package example.host

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.eclipse.leshan.server.LeshanServer
import org.eclipse.leshan.server.registration.Registration
import org.mockito.Mockito.*
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/** Deliberately outside the integration package: the library is discovered through AutoConfiguration.imports. */
@Configuration(proxyBeanMethods = false)
@EnableAutoConfiguration
class ExistingHost {
    @Bean(initMethod = "start", destroyMethod = "destroy")
    fun existingLeshanServer(): LeshanServer = mock(LeshanServer::class.java, RETURNS_DEEP_STUBS).also {
        `when`(it.registrationService.allRegistrations).thenReturn(emptyList<Registration>().iterator())
        `when`(it.registrationService.getByEndpoint(anyString())).thenReturn(null)
    }
}

@Configuration(proxyBeanMethods = false)
@EnableAutoConfiguration
class NoServerHost

@Configuration(proxyBeanMethods = false)
class HostKafkaConfiguration {
    @Bean fun hostMapper(): ObjectMapper = jacksonObjectMapper().findAndRegisterModules().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
    @Suppress("UNCHECKED_CAST")
    @Bean fun hostKafkaTemplate(): KafkaTemplate<String, String> = mock(KafkaTemplate::class.java) as KafkaTemplate<String, String>
    @Suppress("UNCHECKED_CAST")
    @Bean fun hostConsumerFactory(): ConsumerFactory<String, ByteArray> = mock(ConsumerFactory::class.java) as ConsumerFactory<String, ByteArray>
    @Bean fun hostMetrics(): MeterRegistry = spy(SimpleMeterRegistry())
}
