package io.lwm2m.integration.adapter.leshan

import io.lwm2m.core.*
import org.eclipse.leshan.core.link.lwm2m.attributes.DefaultLwM2mAttributeParser
import org.eclipse.leshan.core.link.lwm2m.attributes.LwM2mAttributeSet
import org.eclipse.leshan.core.node.*
import org.eclipse.leshan.core.observation.*
import org.eclipse.leshan.core.request.*
import org.eclipse.leshan.core.request.exception.*
import org.eclipse.leshan.core.response.*
import org.eclipse.leshan.server.LeshanServer
import org.eclipse.leshan.server.observation.ObservationListener
import org.eclipse.leshan.server.registration.Registration
import org.eclipse.leshan.server.send.SendListener
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicBoolean

class LeshanGateway(private val server: LeshanServer, private val publisher: PublishDeviceEvent) : DeviceGateway {
    private val log = LoggerFactory.getLogger(javaClass)
    private val listener = Events()
    private val attached = AtomicBoolean()
    fun attach() {
        if (attached.compareAndSet(false, true)) {
            server.observationService.addListener(listener)
            server.sendService.addListener(listener)
        }
    }
    fun detach() {
        if (attached.compareAndSet(true, false)) {
            server.observationService.removeListener(listener)
            server.sendService.removeListener(listener)
        }
    }

    override fun list() = server.registrationService.allRegistrations.asSequence().map(::device).toList()
    override fun get(endpoint: String) = server.registrationService.getByEndpoint(endpoint)?.let(::device)
    private fun device(r: Registration) = Device(r.endpoint, r.id, r.lwM2mVersion.toString(), r.lifeTimeInSec,
        r.bindingMode.joinToString(""), r.usesQueueMode(), !r.usesQueueMode() || server.presenceService.isClientAwake(r),
        r.objectLinks.map { it.toCoreLinkFormat() })

    override fun execute(command: DeviceCommand, registrationId: String): CompletionStage<DeviceResponse> {
        val result = CompletableFuture<DeviceResponse>()
        try {
            val registration = server.registrationService.getById(registrationId)
                ?: throw DeviceException(ErrorCode.DEVICE_NOT_FOUND, "Device registration has expired")
            if (!command.requiresDevice) {
                cancelLocal(command, registration)
                return CompletableFuture.completedFuture(DeviceResponse("CANCELLED", true))
            }
            val request = request(command, registration)
            send(registration, request, command.timeoutMs).whenComplete { response, error ->
                if (error != null) result.completeExceptionally(translate(error))
                else try {
                    val mapped = response(command, response)
                    if (response.isSuccess) {
                        when (response) {
                            is ObserveResponse -> response.observation?.let { listener.value(it, registration, response) }
                            is ObserveCompositeResponse -> response.observation?.let { listener.value(it, registration, response) }
                        }
                        if (command.operation in setOf(Operation.CANCEL_OBSERVATION, Operation.CANCEL_COMPOSITE_OBSERVATION))
                            cancelLocal(command, registration)
                    }
                    result.complete(mapped)
                } catch (e: Exception) { result.completeExceptionally(translate(e)) }
            }
        } catch (e: Exception) { result.completeExceptionally(translate(e)) }
        return result
    }

    private fun <T : LwM2mResponse> send(registration: Registration, request: DownlinkDeviceManagementRequest<T>, timeout: Long): CompletableFuture<T> {
        val result = CompletableFuture<T>()
        // Leshan's async sender checks sleep again, closing the race after the application check.
        // No QueueHandler and no deferred command storage.
        server.send(registration, request, timeout,
            { response -> result.complete(response) }, { error -> result.completeExceptionally(error) })
        return result
    }

    private fun path(c: DeviceCommand) = requireNotNull(c.path) { "path is required" }.also { LwM2mPath(it) }
    private fun paths(c: DeviceCommand) = c.paths.also { require(it.isNotEmpty()) { "paths are required" } }.map(::LwM2mPath)
    private fun format(name: String?): ContentFormat? = name?.let {
        ContentFormat.fromName(it.uppercase()) ?: throw IllegalArgumentException("Unknown content format '$it'")
    }
    private fun request(c: DeviceCommand, r: Registration): DownlinkDeviceManagementRequest<*> = when (c.operation) {
        Operation.READ -> ReadRequest(format(c.format), path(c))
        Operation.DISCOVER -> DiscoverRequest(path(c))
        Operation.WRITE -> WriteRequest(if (c.replace) WriteRequest.Mode.REPLACE else WriteRequest.Mode.UPDATE,
            format(c.format), path(c), NodeMapper.toLeshan(requireNotNull(c.node) { "node is required" }))
        Operation.WRITE_ATTRIBUTES -> {
            require(c.attributes.isNotEmpty()) { "attributes are required" }
            val query = c.attributes.map { (key, value) -> if (value.isNullOrEmpty()) key else "$key=$value" }.joinToString("&")
            WriteAttributesRequest(path(c), LwM2mAttributeSet(DefaultLwM2mAttributeParser().parseUriQuery(query)))
        }
        Operation.EXECUTE -> ExecuteRequest(path(c), c.arguments)
        Operation.CREATE -> {
            val node = NodeMapper.toLeshan(requireNotNull(c.node) { "node is required" }) as? LwM2mObjectInstance
                ?: throw IllegalArgumentException("Create requires an instance node")
            if (node.id == LwM2mObjectInstance.UNDEFINED) CreateRequest(format(c.format), path(c), node.resources.values)
            else CreateRequest(format(c.format), path(c), node)
        }
        Operation.DELETE -> DeleteRequest(path(c))
        Operation.OBSERVE -> ObserveRequest(format(c.format), path(c), mapOf("endpoint" to c.endpoint, "commandId" to c.commandId))
        Operation.READ_COMPOSITE -> ReadCompositeRequest(format(c.pathFormat) ?: ContentFormat.SENML_CBOR,
            format(c.nodeFormat) ?: ContentFormat.SENML_CBOR, c.paths.also { paths(c) })
        Operation.WRITE_COMPOSITE -> {
            require(c.nodes.isNotEmpty()) { "nodes are required" }
            WriteCompositeRequest(format(c.nodeFormat) ?: ContentFormat.SENML_CBOR,
                c.nodes.entries.associate { LwM2mPath(it.key) to NodeMapper.toLeshan(it.value) }, null)
        }
        Operation.OBSERVE_COMPOSITE -> object : ObserveCompositeRequest(format(c.pathFormat) ?: ContentFormat.SENML_CBOR,
            format(c.nodeFormat) ?: ContentFormat.SENML_CBOR, paths(c)) {
            override fun getContext() = mapOf("endpoint" to c.endpoint, "commandId" to c.commandId)
        }
        Operation.CANCEL_OBSERVATION -> CancelObservationRequest(findObservation(c, r) as SingleObservation)
        Operation.CANCEL_COMPOSITE_OBSERVATION -> CancelCompositeObservationRequest(findObservation(c, r) as CompositeObservation)
    }

    private fun findObservation(c: DeviceCommand, r: Registration): Observation = server.observationService.getObservations(r).firstOrNull {
        when (it) {
            is SingleObservation -> c.operation == Operation.CANCEL_OBSERVATION && it.path == LwM2mPath(path(c))
            is CompositeObservation -> c.operation == Operation.CANCEL_COMPOSITE_OBSERVATION && it.paths.toSet() == paths(c).toSet()
            else -> false
        }
    } ?: throw DeviceException(ErrorCode.SUBSCRIPTION_NOT_FOUND, "Subscription does not exist")

    private fun cancelLocal(c: DeviceCommand, r: Registration) {
        if (c.operation == Operation.CANCEL_OBSERVATION) server.observationService.cancelObservations(r, path(c))
        else {
            val targets = paths(c).toSet()
            server.observationService.getObservations(r).filterIsInstance<CompositeObservation>()
                .filter { it.paths.toSet() == targets }.forEach { server.observationService.cancelObservation(it) }
        }
    }

    private fun response(c: DeviceCommand, r: LwM2mResponse): DeviceResponse {
        val content = when (r) {
            is ReadResponse -> r.content?.let { mapOf(path(c) to NodeMapper.fromLeshan(it)) }.orEmpty()
            is ReadCompositeResponse -> NodeMapper.nodes(r.content)
            else -> emptyMap()
        }
        val observation = when (r) { is ObserveResponse -> r.observation; is ObserveCompositeResponse -> r.observation; else -> null }
        return DeviceResponse(r.code.toString(), r.isSuccess, content,
            (r as? DiscoverResponse)?.objectLinks?.map { it.toCoreLinkFormat() }.orEmpty(),
            (r as? CreateResponse)?.location, observation?.let(::subscriptionId), r.errorMessage)
    }

    private fun subscriptionId(o: Observation) = "${o.registrationId}:${o.id}"
    private fun observationPaths(o: Observation): List<String> = when (o) {
        is SingleObservation -> listOf(o.path.toString())
        is CompositeObservation -> o.paths.map { it.toString() }
        else -> emptyList()
    }

    private fun translate(e: Throwable): DeviceException {
        if (e is DeviceException) return e
        val code = when (e) {
            is ClientSleepingException -> ErrorCode.DEVICE_SLEEPING
            is TimeoutException -> ErrorCode.TIMEOUT
            is InvalidRequestException, is IllegalArgumentException, is LwM2mNodeException, is org.eclipse.leshan.core.node.codec.CodecException,
            is org.eclipse.leshan.core.link.attributes.InvalidAttributeException, is ArithmeticException, is ClassCastException -> ErrorCode.INVALID_REQUEST
            is RequestRejectedException -> ErrorCode.REQUEST_REJECTED
            is RequestCanceledException -> ErrorCode.REQUEST_CANCELLED
            is InvalidResponseException -> ErrorCode.INVALID_RESPONSE
            else -> ErrorCode.INTERNAL_ERROR
        }
        if (code == ErrorCode.INTERNAL_ERROR) log.error("LwM2M operation failed", e)
        return DeviceException(code, if (code == ErrorCode.INTERNAL_ERROR) "Operation failed" else e.message ?: code.name, e)
    }

    private inner class Events : ObservationListener, SendListener {
        override fun newObservation(observation: Observation, registration: Registration) = Unit
        override fun cancelled(observation: Observation) {
            val endpoint = observation.context["endpoint"] ?: server.registrationService.getById(observation.registrationId)?.endpoint ?: return
            publisher.publish(DeviceEvent(EventType.OBSERVATION_CANCELLED, endpoint, observation.registrationId,
                observationPaths(observation), subscriptionId = subscriptionId(observation), commandId = observation.context["commandId"]))
        }
        override fun onResponse(observation: SingleObservation, registration: Registration, response: ObserveResponse) = value(observation, registration, response)
        override fun onResponse(observation: CompositeObservation, registration: Registration, response: ObserveCompositeResponse) = value(observation, registration, response)
        fun value(o: Observation, r: Registration, response: LwM2mResponse) {
            val nodes = when (response) {
                is ObserveResponse -> response.content?.let { mapOf((o as SingleObservation).path.toString() to NodeMapper.fromLeshan(it)) }.orEmpty()
                is ObserveCompositeResponse -> NodeMapper.nodes(response.content)
                else -> emptyMap()
            }
            publisher.publish(DeviceEvent(if (response.isSuccess) EventType.OBSERVATION_VALUE else EventType.OBSERVATION_ERROR,
                r.endpoint, r.id, observationPaths(o), nodes, subscriptionId(o), o.context["commandId"], response.code.toString(), response.errorMessage))
        }
        override fun onError(observation: Observation, registration: Registration, error: Exception) {
            publisher.publish(DeviceEvent(EventType.OBSERVATION_ERROR, registration.endpoint, registration.id,
                observationPaths(observation), subscriptionId = subscriptionId(observation), commandId = observation.context["commandId"], error = error.message))
        }
        override fun dataReceived(registration: Registration, data: TimestampedLwM2mNodes, request: SendRequest) {
            // Preserve every timestamp in a Send payload, not only its latest sample.
            data.timestamps.forEach { timestamp ->
                val nodes = NodeMapper.nodes(data.getNodesAt(timestamp))
                publisher.publish(DeviceEvent(EventType.SEND, registration.endpoint, registration.id, nodes.keys.toList(), nodes,
                    deviceTimestamp = timestamp?.let { it.epochSecond + it.nano / 1_000_000_000.0 }))
            }
        }
        override fun onError(registration: Registration?, errorMessage: String, error: Exception?) {
            log.warn("Unable to handle Send from {}: {}", registration?.endpoint, errorMessage, error)
        }
    }
}
