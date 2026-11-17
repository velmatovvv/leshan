package io.lwm2m.integration.adapter.leshan

import io.lwm2m.core.Node
import org.eclipse.leshan.core.link.DefaultLinkParser
import org.eclipse.leshan.core.link.Link
import org.eclipse.leshan.core.model.ResourceModel.Type
import org.eclipse.leshan.core.node.*
import org.eclipse.leshan.core.util.datatype.ULong
import java.util.Base64
import java.util.Date

object NodeMapper {
    fun toLeshan(node: Node): LwM2mNode {
        require(node.id in -1..65535) { "Invalid node ID" }
        return when (node.kind) {
            "object" -> LwM2mObject(node.id, node.instances.orEmpty().map { toLeshan(it) as? LwM2mObjectInstance
                ?: throw IllegalArgumentException("Object children must be instances") })
            "instance" -> {
                val resources = node.resources.orEmpty().map { toLeshan(it) as? LwM2mResource
                    ?: throw IllegalArgumentException("Instance children must be resources") }
                require(resources.map { it.id }.distinct().size == resources.size) { "Duplicate resource IDs" }
                if (node.id == -1) LwM2mObjectInstance(resources) else LwM2mObjectInstance(node.id, resources)
            }
            "singleResource", "resourceInstance", "multipleResource" -> {
                val type = Type.valueOf(requireNotNull(node.type) { "Resource type is required" })
                when (node.kind) {
                    "singleResource" -> LwM2mSingleResource.newResource(node.id, toValue(type, node.value), type)
                    "resourceInstance" -> LwM2mResourceInstance.newInstance(node.id, toValue(type, node.value), type)
                    else -> LwM2mMultipleResource.newResource(node.id,
                        requireNotNull(node.values) { "values are required" }.mapValues { toValue(type, it.value) }, type)
                }
            }
            else -> throw IllegalArgumentException("Unknown node kind '${node.kind}'")
        }
    }

    private fun toValue(type: Type, value: Any?): Any {
        requireNotNull(value) { "Resource value is required" }
        return when (type) {
            Type.STRING -> value as? String ?: throw IllegalArgumentException("STRING requires a string")
            Type.INTEGER -> value.toString().toBigDecimal().longValueExact()
            Type.UNSIGNED_INTEGER -> ULong.valueOf(value.toString())
            Type.FLOAT -> value.toString().toDouble().also { require(it.isFinite()) { "FLOAT must be finite" } }
            Type.BOOLEAN -> value as? Boolean ?: throw IllegalArgumentException("BOOLEAN requires a boolean")
            Type.TIME -> Date(value.toString().toBigDecimal().multiply(1000.toBigDecimal()).longValueExact())
            Type.OPAQUE -> Base64.getDecoder().decode(value as String)
            Type.OBJLNK -> ObjectLink.decodeFromString(value as String)
            Type.CORELINK -> DefaultLinkParser().parseCoreLinkFormat((value as String).toByteArray(Charsets.UTF_8))
            Type.NONE -> throw IllegalArgumentException("NONE has no value")
        }
    }

    fun fromLeshan(node: LwM2mNode): Node = when (node) {
        is LwM2mObject -> Node(node.id, "object", instances = node.instances.values.map(::fromLeshan))
        is LwM2mObjectInstance -> Node(node.id, "instance", resources = node.resources.values.map(::fromLeshan))
        is LwM2mMultipleResource -> Node(node.id, "multipleResource", node.type.name,
            values = node.instances.mapValues { fromValue(it.value.value) })
        is LwM2mSingleResource -> Node(node.id, "singleResource", node.type.name, fromValue(node.value))
        is LwM2mResourceInstance -> Node(node.id, "resourceInstance", node.type.name, fromValue(node.value))
        else -> throw IllegalArgumentException("Unsupported node ${node.javaClass.simpleName}")
    }

    private fun fromValue(value: Any): Any = when (value) {
        is ByteArray -> Base64.getEncoder().encodeToString(value)
        is Date -> value.time.toBigDecimal().divide(1000.toBigDecimal())
        is ObjectLink -> value.encodeToString()
        is ULong -> value.toString()
        is Array<*> -> value.joinToString(",") { (it as Link).toCoreLinkFormat() }
        else -> value
    }

    fun nodes(nodes: Map<LwM2mPath, LwM2mNode>?) = nodes.orEmpty().entries.associate { it.key.toString() to fromLeshan(it.value) }
}
