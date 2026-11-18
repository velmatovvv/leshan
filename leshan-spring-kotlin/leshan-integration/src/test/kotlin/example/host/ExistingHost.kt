package example.host

import io.lwm2m.core.DeviceGateway
import io.lwm2m.core.DeviceService
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
