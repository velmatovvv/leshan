package example.host

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.lwm2m.core.DeviceGateway
import io.lwm2m.core.DeviceService
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.eclipse.leshan.server.LeshanServer
import org.eclipse.leshan.server.registration.Registration
import org.mockito.Mockito.*
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

/** Outside the integration package: only AutoConfiguration.imports can discover the module. */
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
class CustomGatewayHost {
    @Bean fun hostGateway(): DeviceGateway = mock(DeviceGateway::class.java)
}

@Configuration(proxyBeanMethods = false)
class CustomServiceHost {
    @Bean fun hostDeviceService(): DeviceService = mock(DeviceService::class.java)
}

@Configuration(proxyBeanMethods = false)
class HostCommonNames {
    @Bean("deviceService") fun domainService() = Any()
    @Bean("deviceGateway") fun domainGateway() = Any()
    @Bean("serverIdentity") fun domainIdentity() = Any()
}

@Configuration(proxyBeanMethods = false)
class HostKafkaConfiguration {
    @Bean fun hostMapper(): ObjectMapper = jacksonObjectMapper().findAndRegisterModules().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
    @Suppress("UNCHECKED_CAST")
    @Bean fun hostKafkaTemplate(): KafkaTemplate<String, String> = mock(KafkaTemplate::class.java) as KafkaTemplate<String, String>
    @Suppress("UNCHECKED_CAST")
    @Bean fun hostConsumerFactory(): ConsumerFactory<String, ByteArray> = mock(ConsumerFactory::class.java) as ConsumerFactory<String, ByteArray>
    @Bean fun hostMetrics(): MeterRegistry = spy(SimpleMeterRegistry())
}

@Configuration(proxyBeanMethods = false)
class HostWebConfiguration {
    @Bean fun hostController() = HostController()
}

@RestController
class HostController {
    @GetMapping("/host/ping") fun ping() = "host"
    @GetMapping("/host/fail") fun fail(): String = throw IllegalArgumentException("Host exception")
}
