package io.lwm2m.core

import java.util.concurrent.CompletionStage

/** Inbound port shared by REST and a future Kafka command consumer. */
interface ExecuteDeviceCommand { fun execute(command: DeviceCommand): CompletionStage<CommandResult> }
interface QueryDevices {
    fun list(): List<Device>
    fun get(endpoint: String): Device
}

/** Outbound ports. Only adapters depend on Leshan, Redis, HTTP or Kafka. */
interface DeviceGateway {
    fun list(): List<Device>
    fun get(endpoint: String): Device?
    fun execute(command: DeviceCommand, registrationId: String): CompletionStage<DeviceResponse>
}
fun interface PublishDeviceEvent { fun publish(event: DeviceEvent) }
