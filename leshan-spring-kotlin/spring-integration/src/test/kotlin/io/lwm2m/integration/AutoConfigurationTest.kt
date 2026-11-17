package io.lwm2m.integration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import example.host.*
import io.lwm2m.core.*
import io.lwm2m.integration.adapter.http.DeviceController
import io.lwm2m.integration.adapter.http.HttpErrors
import io.lwm2m.integration.adapter.kafka.KafkaDeviceCommand
import io.lwm2m.integration.adapter.leshan.LeshanGateway
import io.lwm2m.integration.autoconfigure.*
import io.micrometer.core.instrument.MeterRegistry
import jakarta.servlet.ServletException
import org.assertj.core.api.Assertions.assertThat
import org.eclipse.leshan.server.LeshanServer
import org.eclipse.leshan.server.observation.ObservationListener
import org.eclipse.leshan.server.send.SendListener
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.*
import org.springframework.boot.test.context.FilteredClassLoader
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.test.context.runner.WebApplicationContextRunner
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.*
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import java.time.Instant

class AutoConfigurationTest {
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
            assertThat(context).doesNotHaveBean(DeviceController::class.java).doesNotHaveBean(Lwm2mKafkaClient::class.java)
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
                    .doesNotHaveBean(DeviceGateway::class.java).doesNotHaveBean(Lwm2mKafkaClient::class.java)
            }
    }

    @Test fun `explicit disable leaves the host server alone`() {
        host.withPropertyValues("lwm2m.enabled=false", "lwm2m.events.enabled=true").run { context ->
            assertThat(context).hasNotFailed().hasSingleBean(LeshanServer::class.java)
                .doesNotHaveBean(DeviceGateway::class.java).doesNotHaveBean(Lwm2mKafkaClient::class.java)
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

    @Test fun `Kafka clients and wire mapper do not replace or mutate host infrastructure`() {
        lateinit var hostMetrics: MeterRegistry
        host.withUserConfiguration(HostKafkaConfiguration::class.java)
            .withPropertyValues("lwm2m.events.enabled=true", "spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer")
            .run { context ->
                assertThat(context).hasNotFailed().hasSingleBean(KafkaTemplate::class.java).hasSingleBean(ConsumerFactory::class.java)
                    .hasSingleBean(ObjectMapper::class.java).hasSingleBean(PublishDeviceEvent::class.java)
                val client = context.getBean(Lwm2mKafkaClient::class.java)
                assertThat(client.template).isNotSameAs(context.getBean("hostKafkaTemplate"))
                assertThat(client.consumerFactory).isNotSameAs(context.getBean("hostConsumerFactory"))
                val hostMapper = context.getBean(ObjectMapper::class.java)
                assertThat(hostMapper.propertyNamingStrategy).isEqualTo(PropertyNamingStrategies.SNAKE_CASE)
                val wire = context.getBean(Lwm2mJson::class.java).mapper
                assertThat(wire).isNotSameAs(hostMapper)
                assertThat(wire.writeValueAsString(KafkaDeviceCommand(1, "id", "endpoint", Operation.READ, Instant.EPOCH)))
                    .contains("\"schemaVersion\":1", "\"expiresAt\":\"1970-01-01T00:00:00Z\"").doesNotContain("schema_version")
                hostMetrics = context.getBean(MeterRegistry::class.java)
                assertThat(context.getBean(Lwm2mMetrics::class.java).registry).isSameAs(hostMetrics)
            }
        verify(hostMetrics, times(1)).close() // Host owns this registry; library must not close it again.
    }

    @Test fun `REST is opt in and serves API without scanning integration packages`() {
        WebApplicationContextRunner().withUserConfiguration(ExistingHost::class.java, HostWebConfiguration::class.java)
            .withPropertyValues("lwm2m.rest.enabled=true").run { context ->
                assertThat(context).hasNotFailed().hasSingleBean(DeviceController::class.java).hasSingleBean(HttpErrors::class.java)
                    .doesNotHaveBean(Lwm2mKafkaClient::class.java)
                val mvc = MockMvcBuilders.webAppContextSetup(context).build()
                mvc.perform(get("/api/clients")).andExpect(status().isOk).andExpect(content().json("[]"))
                mvc.perform(get("/api/clients/missing")).andExpect(status().isNotFound)
                mvc.perform(get("/host/ping")).andExpect(status().isOk).andExpect(content().string("host"))
                val failure = assertThrows(ServletException::class.java) { mvc.perform(get("/host/fail")) }
                assertThat(failure.cause).isInstanceOf(IllegalArgumentException::class.java)
            }
    }

    @Test fun `disabled REST adds no endpoints or global exception advice`() {
        WebApplicationContextRunner().withUserConfiguration(ExistingHost::class.java).run { context ->
            assertThat(context).hasNotFailed().doesNotHaveBean(DeviceController::class.java).doesNotHaveBean(HttpErrors::class.java)
            MockMvcBuilders.webAppContextSetup(context).build().perform(get("/api/clients")).andExpect(status().isNotFound)
        }
    }
}
