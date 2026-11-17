package io.lwm2m.server.adapter.http

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import io.lwm2m.core.*
import jakarta.servlet.http.HttpServletRequest
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.util.UUID
import java.util.concurrent.CompletionStage

@RestController
@RequestMapping("/api/clients")
class DeviceController(private val commands: ExecuteDeviceCommand, private val devices: QueryDevices, private val mapper: ObjectMapper) {
    @GetMapping("", "/") fun list() = devices.list()
    @GetMapping("/{endpoint}") fun get(@PathVariable endpoint: String) = devices.get(endpoint)

    @RequestMapping("/{endpoint}/{*target}", method = [RequestMethod.GET, RequestMethod.PUT, RequestMethod.POST, RequestMethod.DELETE])
    fun operate(@PathVariable endpoint: String, @PathVariable target: String, request: HttpServletRequest,
                @RequestBody(required = false) body: String?): CompletionStage<ResponseEntity<CommandResult>> {
        val parts = target.trim('/').split('/')
        val composite = parts == listOf("composite") || parts == listOf("composite", "observe")
        val observe = parts.last() == "observe"
        val attributes = parts.last() == "attributes"
        val discover = parts.last() == "discover"
        val nodeParts = if (observe || attributes || discover) parts.dropLast(1) else parts
        if (!composite) require(nodeParts.size in 1..4 && nodeParts.all { it.toIntOrNull() in 0..65535 }) { "Invalid LwM2M path" }
        val operation = when {
            composite -> when (request.method to observe) {
                "GET" to false -> Operation.READ_COMPOSITE
                "PUT" to false -> Operation.WRITE_COMPOSITE
                "POST" to true -> Operation.OBSERVE_COMPOSITE
                "DELETE" to true -> Operation.CANCEL_COMPOSITE_OBSERVATION
                else -> throw IllegalArgumentException("Unsupported composite operation")
            }
            observe -> when (request.method) {
                "POST" -> Operation.OBSERVE
                "DELETE" -> Operation.CANCEL_OBSERVATION
                else -> throw IllegalArgumentException("Observe supports POST and DELETE")
            }
            attributes -> { require(request.method == "PUT"); Operation.WRITE_ATTRIBUTES }
            discover -> { require(request.method == "GET"); Operation.DISCOVER }
            else -> when (request.method) {
                "GET" -> Operation.READ
                "PUT" -> Operation.WRITE
                "DELETE" -> { require(nodeParts.size == 2) { "Delete requires an instance path" }; Operation.DELETE }
                "POST" -> when (nodeParts.size) {
                    1, 2 -> Operation.CREATE
                    3 -> Operation.EXECUTE
                    else -> throw IllegalArgumentException("Invalid Create or Execute path")
                }
                else -> throw IllegalArgumentException("Unsupported method")
            }
        }
        val path = if (composite) null else "/" + nodeParts.joinToString("/")
        val node = if (operation in setOf(Operation.WRITE, Operation.CREATE)) {
            require(!body.isNullOrBlank()) { "Request body is required" }
            if (request.contentType?.substringBefore(';') == "text/plain") {
                require(operation == Operation.WRITE && nodeParts.size == 3) { "text/plain requires a resource Write" }
                Node(nodeParts.last().toInt(), type = "STRING", value = body)
            } else mapper.readValue(body, Node::class.java)
        } else null
        val nodes = if (operation == Operation.WRITE_COMPOSITE) {
            require(!body.isNullOrBlank()) { "Request body is required" }
            mapper.readValue(body, object : TypeReference<Map<String, Node>>() {})
        } else emptyMap()
        val timeoutSeconds = request.getParameter("timeout")?.let {
            it.toLongOrNull() ?: throw IllegalArgumentException("timeout must be an integer number of seconds")
        } ?: 5L
        require(timeoutSeconds in 1..300) { "timeout must be between 1 and 300 seconds" }
        fun boolean(name: String, default: Boolean) = request.getParameter(name)?.let {
            it.toBooleanStrictOrNull() ?: throw IllegalArgumentException("$name must be true or false")
        } ?: default
        val paths = request.getParameter("paths")?.split(',')?.map { it.trim() }.orEmpty()
        val reserved = setOf("timeout", "format", "replace", "active", "paths", "pathformat", "nodeformat")
        val attrs = if (attributes) request.parameterMap.filterKeys { it !in reserved }.mapValues { it.value.firstOrNull() } else emptyMap()
        val command = DeviceCommand(endpoint, operation, path, paths, node, nodes, attrs,
            arguments = if (operation == Operation.EXECUTE) body else null,
            replace = boolean("replace", true), active = boolean("active", false),
            format = request.getParameter("format"), pathFormat = request.getParameter("pathformat"), nodeFormat = request.getParameter("nodeformat"),
            timeoutMs = timeoutSeconds * 1000,
            commandId = request.getHeader("X-Command-Id") ?: UUID.randomUUID().toString())
        return commands.execute(command).thenApply { result ->
            ResponseEntity.status(result.error?.code?.let(::httpStatus) ?: 200)
                .header("X-Command-Id", result.commandId).body(result)
        }
    }
}

internal fun httpStatus(code: ErrorCode) = when (code) {
    ErrorCode.INVALID_REQUEST -> 400
    ErrorCode.DEVICE_NOT_FOUND, ErrorCode.SUBSCRIPTION_NOT_FOUND -> 404
    ErrorCode.DEVICE_SLEEPING, ErrorCode.COMMAND_EXPIRED -> 409
    ErrorCode.TIMEOUT -> 504
    ErrorCode.REQUEST_REJECTED, ErrorCode.REQUEST_CANCELLED, ErrorCode.INVALID_RESPONSE -> 502
    ErrorCode.INTERNAL_ERROR -> 500
}
