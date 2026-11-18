package io.lwm2m.integration

import example.host.ExistingHost
import io.lwm2m.core.DeviceGateway
import io.lwm2m.core.ExecuteDeviceCommand
import io.lwm2m.core.PublishDeviceEvent
import io.lwm2m.integration.adapter.http.DeviceController
import io.lwm2m.integration.autoconfigure.LeshanIntegrationAutoConfiguration
import io.lwm2m.integration.autoconfigure.LeshanKafkaEventsAutoConfiguration
import io.lwm2m.integration.autoconfigure.LeshanRestAutoConfiguration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.runner.WebApplicationContextRunner
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.*
import org.springframework.test.web.servlet.setup.MockMvcBuilders

class CompatibilityBundleTest {

    @Test fun `legacy dependency discovers both adapters once and preserves existing settings`() {
        WebApplicationContextRunner().withUserConfiguration(ExistingHost::class.java)
            .withPropertyValues("lwm2m.rest.enabled=true", "lwm2m.events.enabled=true").run { context ->
                assertThat(context).hasNotFailed().hasSingleBean(DeviceGateway::class.java)
                    .hasSingleBean(ExecuteDeviceCommand::class.java).hasSingleBean(DeviceController::class.java)
                    .hasSingleBean(PublishDeviceEvent::class.java)
                    .hasSingleBean(LeshanIntegrationAutoConfiguration::class.java)
                    .hasSingleBean(LeshanRestAutoConfiguration::class.java)
                    .hasSingleBean(LeshanKafkaEventsAutoConfiguration::class.java)
                MockMvcBuilders.webAppContextSetup(context).build().perform(get("/api/clients"))
                    .andExpect(status().isOk).andExpect(content().json("[]"))
            }
    }
}
