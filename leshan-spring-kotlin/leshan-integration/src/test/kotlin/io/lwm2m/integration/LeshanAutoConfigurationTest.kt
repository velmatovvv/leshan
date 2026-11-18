package io.lwm2m.integration

import example.host.*
import io.lwm2m.core.*
import io.lwm2m.integration.adapter.leshan.LeshanGateway
import org.assertj.core.api.Assertions.assertThat
import org.eclipse.leshan.server.LeshanServer
import org.eclipse.leshan.server.observation.ObservationListener
import org.eclipse.leshan.server.send.SendListener
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.*
import org.springframework.boot.test.context.FilteredClassLoader
import org.springframework.boot.test.context.runner.ApplicationContextRunner

class LeshanAutoConfigurationTest {
    private val host = ApplicationContextRunner().withUserConfiguration(ExistingHost::class.java)

    @Test fun `auto discovery reuses host server and only attaches and detaches its own listeners`() {
        lateinit var server: LeshanServer
        val observation = ArgumentCaptor.forClass(ObservationListener::class.java)
        val send = ArgumentCaptor.forClass(SendListener::class.java)
        host.run { context ->
            assertThat(context).hasNotFailed().hasSingleBean(LeshanServer::class.java).hasSingleBean(DeviceGateway::class.java)
            server = context.getBean(LeshanServer::class.java)
            assertThat(context.getBean(DeviceGateway::class.java)).isInstanceOf(LeshanGateway::class.java)
            context.getBean(LeshanGateway::class.java).attach() // Must remain idempotent.
            verify(server, times(1)).start() // The host's @Bean init method, never the library.
            verify(server, never()).stop()
            verify(server, never()).destroy()
            verify(server.observationService, times(1)).addListener(observation.capture())
            verify(server.sendService, times(1)).addListener(send.capture())
            assertThat(context).doesNotHaveBean("lwm2mDeviceController").doesNotHaveBean("lwm2mKafkaClient")
            assertThat(context).doesNotHaveBean("jedisPool").doesNotHaveBean("standaloneServerLifecycle")
        }
        verify(server.observationService).removeListener(observation.value)
        verify(server.sendService).removeListener(send.value)
        verify(server, times(1)).start()
        verify(server, never()).stop()
        verify(server, times(1)).destroy() // The host's @Bean destroy method, exactly once.
    }

    @Test fun `module does not create a server when host has none`() {
        ApplicationContextRunner().withUserConfiguration(NoServerHost::class.java)
            .withPropertyValues("lwm2m.commands.enabled=true", "lwm2m.events.enabled=true", "lwm2m.rest.enabled=true")
            .run { context ->
                assertThat(context).hasNotFailed().doesNotHaveBean(LeshanServer::class.java)
                    .doesNotHaveBean(DeviceGateway::class.java).doesNotHaveBean("lwm2mKafkaClient")
            }
    }

    @Test fun `explicit disable leaves the host server alone`() {
        host.withPropertyValues("lwm2m.enabled=false", "lwm2m.events.enabled=true").run { context ->
            assertThat(context).hasNotFailed().hasSingleBean(LeshanServer::class.java)
                .doesNotHaveBean(DeviceGateway::class.java).doesNotHaveBean("lwm2mKafkaClient")
            verify(context.getBean(LeshanServer::class.java).observationService, never()).addListener(any())
        }
    }

    @Test fun `gateway works without Kafka MVC Actuator Redis or Californium on the classpath`() {
        host.withClassLoader(FilteredClassLoader("org.springframework.kafka", "org.springframework.web", "jakarta.servlet",
            "org.springframework.boot.actuate", "redis.clients", "org.eclipse.leshan.transport"))
            .run { context ->
                assertThat(context).hasNotFailed().hasSingleBean(DeviceGateway::class.java)
                    .hasSingleBean(ExecuteDeviceCommand::class.java).hasSingleBean(QueryDevices::class.java)
                    .doesNotHaveBean("lwm2mKafkaClient").doesNotHaveBean("lwm2mDeviceController")
            }
    }

    @Test fun `custom gateway replaces default adapter`() {
        host.withUserConfiguration(CustomGatewayHost::class.java).run { context ->
            assertThat(context).hasNotFailed().hasSingleBean(DeviceGateway::class.java).doesNotHaveBean(LeshanGateway::class.java)
            assertThat(context.getBean(DeviceGateway::class.java)).isSameAs(context.getBean("hostGateway"))
            verify(context.getBean(LeshanServer::class.java).observationService, never()).addListener(any())
        }
    }

    @Test fun `host use cases replace default service`() {
        host.withUserConfiguration(CustomServiceHost::class.java).run { context ->
            assertThat(context).hasNotFailed().hasSingleBean(ExecuteDeviceCommand::class.java).hasSingleBean(QueryDevices::class.java)
            assertThat(context.getBean(ExecuteDeviceCommand::class.java)).isSameAs(context.getBean("hostDeviceService"))
            assertThat(context).doesNotHaveBean("lwm2mDeviceService")
        }
    }

    @Test fun `module bean names do not collide with unrelated host domain services`() {
        host.withUserConfiguration(HostCommonNames::class.java).run { context ->
            assertThat(context).hasNotFailed().hasBean("deviceService").hasBean("deviceGateway").hasBean("serverIdentity")
                .hasBean("lwm2mDeviceService").hasBean("lwm2mDeviceGateway").hasBean("lwm2mServerIdentity")
            assertThat(context.getBean("deviceService")).isNotSameAs(context.getBean("lwm2mDeviceService"))
        }
    }

    @Test fun `base module has no HTTP Kafka or metrics implementation dependencies`() {
        listOf("org.springframework.web.servlet.DispatcherServlet", "org.springframework.kafka.core.KafkaTemplate",
            "io.lwm2m.integration.adapter.http.DeviceController", "io.lwm2m.integration.adapter.kafka.KafkaCommandConsumer",
            "io.micrometer.core.instrument.MeterRegistry").forEach {
            assertThat(org.springframework.util.ClassUtils.isPresent(it, javaClass.classLoader)).describedAs(it).isFalse()
        }
    }
}
