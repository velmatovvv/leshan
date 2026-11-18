package io.lwm2m.integration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import example.host.*
import io.lwm2m.core.*
import io.lwm2m.integration.adapter.kafka.KafkaDeviceCommand
import io.lwm2m.integration.autoconfigure.*
import io.micrometer.core.instrument.MeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.Mockito.*
import org.springframework.boot.test.context.FilteredClassLoader
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.util.ClassUtils
import java.time.Instant

class KafkaAutoConfigurationTest {
    private val host = ApplicationContextRunner().withUserConfiguration(ExistingHost::class.java)

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

    @Test fun `Kafka module discovers event publisher without MVC or HTTP adapter`() {
        listOf("org.springframework.web.servlet.DispatcherServlet", "jakarta.servlet.Servlet",
            "io.lwm2m.integration.adapter.http.DeviceController").forEach {
            assertThat(ClassUtils.isPresent(it, javaClass.classLoader)).describedAs(it).isFalse()
        }
        host.withPropertyValues("lwm2m.events.enabled=true", "lwm2m.rest.enabled=true").run { context ->
            assertThat(context).hasNotFailed().hasSingleBean(Lwm2mKafkaClient::class.java)
                .hasSingleBean(PublishDeviceEvent::class.java).doesNotHaveBean("lwm2mDeviceController")
        }
    }

    @Test fun `Kafka features remain opt in`() {
        host.run { context ->
            assertThat(context).hasNotFailed().doesNotHaveBean(Lwm2mKafkaClient::class.java)
                .doesNotHaveBean(PublishDeviceEvent::class.java).doesNotHaveBean("lwm2mCommandConsumer")
        }
    }

    @Test fun `Kafka commands require an explicit stable server id`() {
        host.withPropertyValues("lwm2m.commands.enabled=true").run { context ->
            assertThat(context).hasFailed()
            assertThat(context.startupFailure).hasStackTraceContaining("lwm2m.server-id is required")
        }
    }

    @Test fun `Kafka module creates no clients without a host server`() {
        ApplicationContextRunner().withUserConfiguration(NoServerHost::class.java)
            .withPropertyValues("lwm2m.commands.enabled=true", "lwm2m.events.enabled=true").run { context ->
                assertThat(context).hasNotFailed().doesNotHaveBean(Lwm2mKafkaClient::class.java)
            }
    }

    @Test fun `Actuator remains optional for Kafka events`() {
        host.withClassLoader(FilteredClassLoader("org.springframework.boot.actuate"))
            .withPropertyValues("lwm2m.events.enabled=true").run { context ->
                assertThat(context).hasNotFailed().hasSingleBean(PublishDeviceEvent::class.java)
                    .doesNotHaveBean("kafkaCommandsHealth")
            }
    }
}
