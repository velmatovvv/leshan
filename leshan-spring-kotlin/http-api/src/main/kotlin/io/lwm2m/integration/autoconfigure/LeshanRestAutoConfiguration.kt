package io.lwm2m.integration.autoconfigure

import io.lwm2m.core.ExecuteDeviceCommand
import io.lwm2m.core.QueryDevices
import io.lwm2m.integration.adapter.http.DeviceController
import io.lwm2m.integration.adapter.http.HttpErrors
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.*
import org.springframework.context.annotation.Bean
import org.springframework.web.servlet.DispatcherServlet

@AutoConfiguration(after = [LeshanIntegrationAutoConfiguration::class])
@ConditionalOnClass(DispatcherServlet::class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnBean(value = [ExecuteDeviceCommand::class, QueryDevices::class, Lwm2mJson::class])
@ConditionalOnProperty("lwm2m.rest.enabled", havingValue = "true")
class LeshanRestAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    fun lwm2mDeviceController(commands: ExecuteDeviceCommand, queries: QueryDevices, json: Lwm2mJson) = DeviceController(commands, queries, json.mapper)

    @Bean
    @ConditionalOnMissingBean
    fun lwm2mHttpErrors() = HttpErrors()
}
