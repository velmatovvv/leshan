package io.lwm2m.integration.config

/** Stable, distinct identity for each fan-out subscriber. */
data class ServerIdentity(val serverId: String) {
    init { require(serverId.isNotBlank()) { "serverId must not be blank" } }
}
