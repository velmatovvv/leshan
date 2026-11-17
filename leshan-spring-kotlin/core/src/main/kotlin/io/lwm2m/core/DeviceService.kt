package io.lwm2m.core

import java.time.Clock
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.CompletionStage

class DeviceService(private val gateway: DeviceGateway, private val clock: Clock = Clock.systemUTC()) : ExecuteDeviceCommand, QueryDevices {
    override fun list() = gateway.list()
    override fun get(endpoint: String) = gateway.get(endpoint)
        ?: throw DeviceException(ErrorCode.DEVICE_NOT_FOUND, "Device '$endpoint' is not registered")

    override fun execute(command: DeviceCommand): CompletionStage<CommandResult> {
        val response = try {
            require(command.commandId.isNotBlank()) { "commandId must not be blank" }
            require(command.endpoint.isNotBlank()) { "endpoint must not be blank" }
            require(command.timeoutMs in 1..300_000) { "timeout must be between 1 ms and 300 seconds" }
            if (command.expiresAt?.let { !it.isAfter(Instant.now(clock)) } == true)
                throw DeviceException(ErrorCode.COMMAND_EXPIRED, "Command has expired")
            val device = get(command.endpoint)
            if (command.requiresDevice && device.queueMode && !device.awake)
                throw DeviceException(ErrorCode.DEVICE_SLEEPING, "Device '${command.endpoint}' is sleeping")
            gateway.execute(command, device.registrationId)
        } catch (e: Exception) {
            CompletableFuture.failedFuture(e)
        }
        return response.handle { value, failure ->
            var cause = failure
            while (cause is CompletionException && cause.cause != null) cause = cause.cause
            val error = when (cause) {
                null -> null
                is DeviceException -> CommandError(cause.code, cause.message ?: cause.code.name)
                is IllegalArgumentException -> CommandError(ErrorCode.INVALID_REQUEST, cause.message ?: "Invalid request")
                else -> CommandError(ErrorCode.INTERNAL_ERROR, "Operation failed")
            }
            CommandResult(command.commandId, command.endpoint, command.operation, Instant.now(clock), value, error)
        }
    }
}
