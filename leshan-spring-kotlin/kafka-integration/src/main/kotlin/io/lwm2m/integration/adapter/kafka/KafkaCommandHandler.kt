package io.lwm2m.integration.adapter.kafka

import io.lwm2m.core.*
import io.lwm2m.integration.config.ServerIdentity
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import java.util.Base64
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

/** No command persistence, deduplication or per-device sequencing. Consumer calls handle serially;
 * completions may run on Leshan threads and never touch the Kafka consumer. */
class KafkaCommandHandler(
    private val codec: CommandCodec,
    private val commands: ExecuteDeviceCommand,
    private val registrations: HasLocalRegistration,
    private val publisher: CommandPublisher,
    private val identity: ServerIdentity,
    private val registry: MeterRegistry
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val active = AtomicInteger()
    private val started = registry.counter("lwm2m.commands.started")
    private val skipped = registry.counter("lwm2m.commands.skipped")
    private val invalid = registry.counter("lwm2m.commands.invalid")
    private val timeouts = registry.counter("lwm2m.commands.timeouts")
    val inFlight: Int get() = active.get()

    init { registry.gauge("lwm2m.commands.inflight", active) }

    fun handle(record: ConsumerRecord<String, ByteArray>, commit: () -> Unit) {
        val command = try { codec.decode(record.key(), record.value()) } catch (e: Exception) {
            commit()
            invalid.increment()
            publisher.invalid(InvalidKafkaCommand(identity.serverId, record.topic(), record.partition(), record.offset(),
                record.key(), record.value()?.let { Base64.getEncoder().encodeToString(it) },
                (e.message ?: "Invalid command envelope").take(1024)))
            return
        }
        if (!registrations.contains(command.endpoint)) {
            commit()
            skipped.increment()
            return
        }
        active.incrementAndGet()
        started.increment()
        val duration = Timer.start(registry)
        val future = try { commands.execute(command) } catch (e: Exception) {
            log.error("Command dispatch failed: commandId={}", command.commandId, e)
            CompletableFuture.completedFuture(CommandResult(command.commandId, command.endpoint, command.operation,
                error = CommandError(ErrorCode.INTERNAL_ERROR, "Command dispatch failed")))
        }
        // Observe the original future before committing. A response can arrive during commit;
        // neither commit latency nor result publication should inflate execution duration.
        val observed = future.whenComplete { result, failure ->
            try { recordCompletion(command.operation, result, failure, duration) }
            catch (e: Exception) { log.warn("Unable to record command metrics", e) }
        }
        try {
            // execute() has called Leshan's async send (or returned an immediate validation error).
            // Commit does not depend on the device response or result publication.
            commit()
        } finally {
            // Attach even when commit failed: the device operation cannot be undone.
            observed.whenComplete { result, failure ->
                try {
                    val outcome = if (failure == null) result else {
                        log.error("Command completion failed: commandId={}", command.commandId, failure)
                        CommandResult(command.commandId, command.endpoint, command.operation,
                            error = CommandError(ErrorCode.INTERNAL_ERROR, "Command completion failed"))
                    }
                    publisher.result(KafkaCommandResult.from(identity.serverId, outcome))
                } catch (e: Exception) {
                    log.error("Unable to publish command result: commandId={}", command.commandId, e)
                } finally { active.decrementAndGet() }
            }
        }
    }

    private fun recordCompletion(operation: Operation, result: CommandResult?, failure: Throwable?, duration: Timer.Sample) {
        val errorCode = if (failure != null) ErrorCode.INTERNAL_ERROR.name else result?.error?.code?.name
        val deviceError = errorCode == null && result?.response?.success == false
        val outcome = when {
            errorCode != null -> "error"
            deviceError -> "device_error"
            else -> "success"
        }
        duration.stop(registry.timer("lwm2m.commands.duration", "operation", operation.name, "outcome", outcome))
        registry.counter("lwm2m.commands.completed", "operation", operation.name, "outcome", outcome).increment()
        val code = errorCode ?: if (deviceError) "DEVICE_RESPONSE_ERROR" else null
        if (code != null) registry.counter("lwm2m.commands.errors", "operation", operation.name, "code", code).increment()
        if (errorCode == ErrorCode.TIMEOUT.name) timeouts.increment()
    }
}
