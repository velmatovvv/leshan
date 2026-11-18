package io.lwm2m.integration.autoconfigure

import io.micrometer.core.instrument.MeterRegistry

/** A fallback registry stays private and cannot suppress the host's metrics auto-configuration. */
class Lwm2mMetrics(val registry: MeterRegistry, private val owned: Boolean) : AutoCloseable {
    override fun close() { if (owned) registry.close() }
}
