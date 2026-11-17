package io.lwm2m.core

import java.time.Instant
import java.util.UUID

enum class Operation {
    READ, DISCOVER, WRITE, WRITE_ATTRIBUTES, EXECUTE, CREATE, DELETE, OBSERVE, CANCEL_OBSERVATION,
    READ_COMPOSITE, WRITE_COMPOSITE, OBSERVE_COMPOSITE, CANCEL_COMPOSITE_OBSERVATION
}

/** Transport-neutral node. Scalars use their JSON types; TIME is epoch seconds, OPAQUE is base64,
 * OBJLNK is "objectId:instanceId", UNSIGNED_INTEGER is a decimal string. */
data class Node(
    val id: Int,
    val kind: String = "singleResource",
    val type: String? = null,
    val value: Any? = null,
    val values: Map<Int, Any>? = null,
    val resources: List<Node>? = null,
    val instances: List<Node>? = null
)

data class Device(
    val endpoint: String,
    val registrationId: String,
    val lwm2mVersion: String,
    val lifetime: Long,
    val binding: String,
    val queueMode: Boolean,
    val awake: Boolean,
    val objectLinks: List<String>
)

data class DeviceCommand(
    val endpoint: String,
    val operation: Operation,
    val path: String? = null,
    val paths: List<String> = emptyList(),
    val node: Node? = null,
    val nodes: Map<String, Node> = emptyMap(),
    val attributes: Map<String, String?> = emptyMap(),
    val arguments: String? = null,
    val replace: Boolean = true,
    val active: Boolean = false,
    val format: String? = null,
    val pathFormat: String? = null,
    val nodeFormat: String? = null,
    val timeoutMs: Long = 5_000,
    val commandId: String = UUID.randomUUID().toString(),
    val expiresAt: Instant? = null
) {
    val requiresDevice: Boolean get() = active || operation !in setOf(
        Operation.CANCEL_OBSERVATION, Operation.CANCEL_COMPOSITE_OBSERVATION
    )
}

data class DeviceResponse(
    val status: String,
    val success: Boolean,
    val content: Map<String, Node> = emptyMap(),
    val objectLinks: List<String> = emptyList(),
    val location: String? = null,
    val subscriptionId: String? = null,
    val errorMessage: String? = null
)

enum class ErrorCode { INVALID_REQUEST, DEVICE_NOT_FOUND, DEVICE_SLEEPING, SUBSCRIPTION_NOT_FOUND,
    COMMAND_EXPIRED, TIMEOUT, REQUEST_REJECTED, REQUEST_CANCELLED, INVALID_RESPONSE, INTERNAL_ERROR }

data class CommandError(val code: ErrorCode, val message: String)
data class CommandResult(
    val commandId: String,
    val endpoint: String,
    val operation: Operation,
    val completedAt: Instant = Instant.now(),
    val response: DeviceResponse? = null,
    val error: CommandError? = null
)

class DeviceException(val code: ErrorCode, message: String, cause: Throwable? = null) : RuntimeException(message, cause)

enum class EventType { OBSERVATION_VALUE, OBSERVATION_ERROR, OBSERVATION_CANCELLED, SEND }
data class DeviceEvent(
    val type: EventType,
    val endpoint: String,
    val registrationId: String,
    val paths: List<String>,
    val data: Map<String, Node> = emptyMap(),
    val subscriptionId: String? = null,
    val commandId: String? = null,
    val lwm2mCode: String? = null,
    val error: String? = null,
    val deviceTimestamp: Double? = null,
    val receivedAt: Instant = Instant.now(),
    val eventId: String = UUID.randomUUID().toString(),
    val schemaVersion: Int = 1
)
