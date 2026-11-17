package io.lwm2m.core

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.CompletableFuture

class DeviceServiceTest {
    private class Gateway : DeviceGateway {
        var device: Device? = Device("test", "reg", "1.1", 300, "U", true, false, emptyList())
        val sent = mutableListOf<DeviceCommand>()
        var pending = CompletableFuture.completedFuture(DeviceResponse("2.05", true))
        override fun list() = listOfNotNull(device)
        override fun get(endpoint: String) = device
        override fun execute(command: DeviceCommand, registrationId: String): CompletableFuture<DeviceResponse> {
            sent.add(command)
            return pending
        }
    }

    @Test fun `sleeping commands fail and are never replayed after waking`() {
        val gateway = Gateway()
        val service = DeviceService(gateway)
        val result = service.execute(DeviceCommand("test", Operation.EXECUTE, "/3/0/4")).toCompletableFuture().join()
        assertEquals(ErrorCode.DEVICE_SLEEPING, result.error?.code)
        assertTrue(gateway.sent.isEmpty())
        gateway.device = gateway.device!!.copy(awake = true)
        service.execute(DeviceCommand("test", Operation.READ, "/3/0/0")).toCompletableFuture().join()
        assertEquals(listOf(Operation.READ), gateway.sent.map { it.operation })
    }

    @Test fun `local cancellation is allowed while sleeping but active cancellation fails`() {
        val gateway = Gateway()
        val service = DeviceService(gateway)
        assertNull(service.execute(DeviceCommand("test", Operation.CANCEL_OBSERVATION, "/3/0/0")).toCompletableFuture().join().error)
        assertEquals(ErrorCode.DEVICE_SLEEPING, service.execute(DeviceCommand("test", Operation.CANCEL_OBSERVATION,
            "/3/0/0", active = true)).toCompletableFuture().join().error?.code)
        assertEquals(1, gateway.sent.size)
    }

    @Test fun `execution stays asynchronous and preserves correlation`() {
        val gateway = Gateway().apply { device = device!!.copy(awake = true); pending = CompletableFuture() }
        val result = DeviceService(gateway).execute(DeviceCommand("test", Operation.READ, "/3/0/0", commandId = "caller-id")).toCompletableFuture()
        assertFalse(result.isDone)
        gateway.pending.complete(DeviceResponse("2.05", true))
        assertEquals("caller-id", result.join().commandId)
        assertTrue(result.join().response!!.success)
    }

    @Test fun `unknown and expired commands never reach device`() {
        val gateway = Gateway().apply { device = null }
        val service = DeviceService(gateway)
        assertEquals(ErrorCode.DEVICE_NOT_FOUND, service.execute(DeviceCommand("test", Operation.READ)).toCompletableFuture().join().error?.code)
        assertEquals(ErrorCode.COMMAND_EXPIRED, service.execute(DeviceCommand("test", Operation.EXECUTE,
            expiresAt = Instant.EPOCH)).toCompletableFuture().join().error?.code)
        assertTrue(gateway.sent.isEmpty())
    }

    @Test fun `sleep race reported by protocol adapter becomes command failure`() {
        val gateway = Gateway().apply {
            device = device!!.copy(awake = true)
            pending = CompletableFuture.failedFuture(DeviceException(ErrorCode.DEVICE_SLEEPING, "sleep race"))
        }
        assertEquals(ErrorCode.DEVICE_SLEEPING, DeviceService(gateway).execute(DeviceCommand("test", Operation.READ)).toCompletableFuture().join().error?.code)
    }
}
