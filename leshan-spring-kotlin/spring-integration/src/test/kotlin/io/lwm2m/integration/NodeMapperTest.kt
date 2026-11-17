package io.lwm2m.integration

import io.lwm2m.core.Node
import io.lwm2m.integration.adapter.leshan.NodeMapper
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class NodeMapperTest {
    @Test fun `wire scalar types survive conversion`() {
        val nodes = listOf(
            Node(1, type = "INTEGER", value = Long.MAX_VALUE),
            Node(2, type = "UNSIGNED_INTEGER", value = "18446744073709551615"),
            Node(3, type = "BOOLEAN", value = true),
            Node(4, type = "OPAQUE", value = "AAECAw=="),
            Node(5, type = "OBJLNK", value = "3:0"),
            Node(6, type = "STRING", value = "device"),
            Node(7, type = "FLOAT", value = 12.5)
        )
        nodes.forEach { assertEquals(it, NodeMapper.fromLeshan(NodeMapper.toLeshan(it))) }
    }
    @Test fun `fractional and overflowing integers are rejected`() {
        assertThrows(ArithmeticException::class.java) { NodeMapper.toLeshan(Node(1, type = "INTEGER", value = 1.1)) }
        assertThrows(ArithmeticException::class.java) { NodeMapper.toLeshan(Node(1, type = "INTEGER", value = "9223372036854775808")) }
    }
    @Test fun `multiple resources preserve instance IDs`() {
        val node = Node(9, "multipleResource", "INTEGER", values = mapOf(0 to 10L, 3 to 20L))
        assertEquals(node, NodeMapper.fromLeshan(NodeMapper.toLeshan(node)))
    }
}
