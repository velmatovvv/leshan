package io.lwm2m.integration.config

import org.springframework.boot.context.properties.ConfigurationProperties

/** Integration only: the host owns Leshan transports, models and all stores. */
@ConfigurationProperties("lwm2m")
data class Lwm2mProperties(
    var enabled: Boolean = true,
    var serverId: String = "",
    var rest: Rest = Rest(),
    var events: Events = Events(),
    var commands: Commands = Commands()
) {
    data class Rest(var enabled: Boolean = false)
    data class Events(var enabled: Boolean = false, var observationTopic: String = "lwm2m.observations", var sendTopic: String = "lwm2m.send", var bufferCapacity: Int = 1000)
    data class Commands(
        var enabled: Boolean = false,
        var topic: String = "lwm2m.commands",
        var resultTopic: String = "lwm2m.command-results",
        var invalidTopic: String = "lwm2m.commands.invalid",
        var groupId: String = "",
        var maxInFlight: Int = 100,
        var resultBufferCapacity: Int = 1000
    )
}
