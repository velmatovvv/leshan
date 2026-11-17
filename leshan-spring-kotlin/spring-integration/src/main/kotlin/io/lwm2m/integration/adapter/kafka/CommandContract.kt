package io.lwm2m.integration.adapter.kafka

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import io.lwm2m.core.*
import java.time.Instant

/** External v1 envelope. Correlation and expiry must be supplied by the producer. */
data class KafkaDeviceCommand(
    val schemaVersion: Int,
    val commandId: String,
    val endpoint: String,
    val operation: Operation,
    val expiresAt: Instant,
    val timeoutMs: Long = 30_000,
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
    val nodeFormat: String? = null
) {
    fun command() = DeviceCommand(endpoint, operation, path, paths, node, nodes, attributes, arguments,
        replace, active, format, pathFormat, nodeFormat, timeoutMs, commandId, expiresAt)
}

class CommandCodec(mapper: ObjectMapper) {
    private val mapper = mapper.copy().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
        .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
        .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
        .enable(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS)

    fun decode(key: String?, value: ByteArray?): DeviceCommand {
        require(value != null && value.isNotEmpty()) { "Command value must be a JSON object" }
        val envelope = mapper.readValue(value, KafkaDeviceCommand::class.java)
        require(envelope.schemaVersion == 1) { "Unsupported schemaVersion" }
        require(envelope.commandId.isNotBlank()) { "commandId must not be blank" }
        require(envelope.endpoint.isNotBlank()) { "endpoint must not be blank" }
        require(key == envelope.endpoint) { "Kafka key must equal endpoint" }
        require(envelope.timeoutMs in 1..300_000) { "timeoutMs must be between 1 and 300000" }
        return envelope.command()
    }
}

data class KafkaCommandResult(
    val serverId: String,
    val commandId: String,
    val endpoint: String,
    val operation: Operation,
    val completedAt: Instant,
    val response: DeviceResponse? = null,
    val error: CommandError? = null,
    val schemaVersion: Int = 1
) {
    companion object {
        fun from(serverId: String, result: CommandResult) = KafkaCommandResult(serverId, result.commandId,
            result.endpoint, result.operation, result.completedAt, result.response, result.error)
    }
}

data class InvalidKafkaCommand(
    val serverId: String,
    val sourceTopic: String,
    val sourcePartition: Int,
    val sourceOffset: Long,
    val key: String?,
    val valueBase64: String?,
    val reason: String,
    val receivedAt: Instant = Instant.now(),
    val schemaVersion: Int = 1
)

interface CommandPublisher {
    fun result(result: KafkaCommandResult)
    fun invalid(message: InvalidKafkaCommand)
}
