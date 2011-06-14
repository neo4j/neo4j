package org.neo4j.cypher.pipes

import org.junit.Assert
import org.junit.Test
import org.neo4j.cypher.commands.{NodeIdentifier, EntityOutput}

class ColumnFilterPipeTest {
  @Test def shouldReturnColumnsFromReturnItems() {
    val returnItems = List(EntityOutput("foo"))
    val source = new FakePipe(List(Map("x" -> "x", "foo" -> "bar")))
    val columnPipe = new ColumnFilterPipe(returnItems,source)

    Assert.assertEquals(Map("foo" -> NodeIdentifier("foo")), columnPipe.symbols.identifiers)
    Assert.assertEquals(List(Map("foo" -> "bar")), columnPipe.toList)
  }
}