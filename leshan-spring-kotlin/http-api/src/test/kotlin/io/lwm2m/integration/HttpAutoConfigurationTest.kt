package io.lwm2m.integration

import example.host.*
import io.lwm2m.integration.adapter.http.DeviceController
import io.lwm2m.integration.adapter.http.HttpErrors
import jakarta.servlet.ServletException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.runner.WebApplicationContextRunner
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.*
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.util.ClassUtils

class HttpAutoConfigurationTest {

    @Test fun `REST is opt in and serves API without scanning integration packages`() {
        WebApplicationContextRunner().withUserConfiguration(ExistingHost::class.java, HostWebConfiguration::class.java)
            .withPropertyValues("lwm2m.rest.enabled=true").run { context ->
                assertThat(context).hasNotFailed().hasSingleBean(DeviceController::class.java).hasSingleBean(HttpErrors::class.java)
                    .doesNotHaveBean("lwm2mKafkaClient")
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

    @Test fun `HTTP module runs without Kafka classes even when Kafka flags are set`() {
        listOf("org.springframework.kafka.core.KafkaTemplate", "org.apache.kafka.clients.consumer.KafkaConsumer",
            "io.lwm2m.integration.adapter.kafka.KafkaCommandConsumer", "io.micrometer.core.instrument.MeterRegistry").forEach {
            assertThat(ClassUtils.isPresent(it, javaClass.classLoader)).describedAs(it).isFalse()
        }
        WebApplicationContextRunner().withUserConfiguration(ExistingHost::class.java)
            .withPropertyValues("lwm2m.rest.enabled=true", "lwm2m.commands.enabled=true", "lwm2m.events.enabled=true")
            .run { context ->
                assertThat(context).hasNotFailed().hasSingleBean(DeviceController::class.java)
                    .doesNotHaveBean("lwm2mKafkaClient")
                val mvc = MockMvcBuilders.webAppContextSetup(context).build()
                val request = mvc.perform(get("/api/clients/missing/3/0/0").header("X-Command-Id", "http-only"))
                    .andExpect(request().asyncStarted()).andReturn()
                mvc.perform(asyncDispatch(request)).andExpect(status().isNotFound)
                    .andExpect(jsonPath("$.commandId").value("http-only"))
                    .andExpect(jsonPath("$.error.code").value("DEVICE_NOT_FOUND"))
            }
    }

    @Test fun `HTTP module creates no API without a host server`() {
        WebApplicationContextRunner().withUserConfiguration(NoServerHost::class.java)
            .withPropertyValues("lwm2m.rest.enabled=true").run { context ->
                assertThat(context).hasNotFailed().doesNotHaveBean(DeviceController::class.java)
                    .doesNotHaveBean(HttpErrors::class.java)
            }
    }
}
