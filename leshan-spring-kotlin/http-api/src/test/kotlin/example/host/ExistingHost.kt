package example.host

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
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
class HostWebConfiguration {
    @Bean fun hostController() = HostController()
}

@RestController
class HostController {
    @GetMapping("/host/ping") fun ping() = "host"
    @GetMapping("/host/fail") fun fail(): String = throw IllegalArgumentException("Host exception")
}
