/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.virtual.VirtualValues

import scala.jdk.CollectionConverters.MapHasAsJava

class ContainerIndexTest extends CypherFunSuite {

  val qtx = mock[QueryContext]
  implicit val state: QueryState = QueryStateHelper.empty.withQueryContext(qtx)
  val ctx = CypherRow.empty
  val expectedNull: AnyValue = Values.NO_VALUE

  test("handles collection lookup") {
    implicit val collection = literal(Seq(1, 2, 3, 4))

    idx(0) should equal(longValue(1))
    idx(1) should equal(longValue(2))
    idx(2) should equal(longValue(3))
    idx(3) should equal(longValue(4))
    idx(-1) should equal(longValue(4))
    idx(100) should equal(expectedNull)
  }

  test("handles empty collections") {
    implicit val collection = ListLiteral()

    idx(0) should equal(expectedNull)
    idx(-1) should equal(expectedNull)
    idx(100) should equal(expectedNull)
  }

  test("handles nulls") {
    implicit val collection = Literal(Values.NO_VALUE)

    idx(0) should equal(expectedNull)
  }

  test("handles scala map lookup") {
    implicit val expression = literal(Map("a" -> 1, "b" -> "foo"))

    idx("a") should equal(longValue(1))
    idx("b") should equal(Values.stringValue("foo"))
    idx("c") should equal(expectedNull)
  }

  test("handles java map lookup") {
    implicit val expression = literal(Map[String, Any]("a" -> 1, "b" -> "foo").asJava)

    idx("a") should equal(longValue(1))
    idx("b") should equal(Values.stringValue("foo"))
    idx("c") should equal(expectedNull)
  }

  test("handles node lookup") {
    val node = mock[Node]
    when(node.getId).thenReturn(0)
    when(node.getElementId).thenReturn("dummy")
    implicit val expression = literal(node)
    val nodeCursor = state.cursors.nodeCursor
    val propertyCursor = state.cursors.propertyCursor

    when(qtx.propertyKey("v")).thenReturn(42)
    when(qtx.propertyKey("c")).thenReturn(43)

    when(qtx.nodeProperty(0, 42, nodeCursor, propertyCursor, throwOnDeleted = true)).thenReturn(longValue(1))
    when(qtx.nodeProperty(0, 43, nodeCursor, propertyCursor, throwOnDeleted = true)).thenReturn(Values.NO_VALUE)
    idx("v") should equal(longValue(1))
    idx("c") should equal(expectedNull)
  }

  test("handles relationship lookup") {
    val rel = mock[Relationship]
    when(rel.getId).thenReturn(0)
    implicit val expression = literal(rel)
    val relationshipScanCursor = state.cursors.relationshipScanCursor
    val propertyCursor = state.cursors.propertyCursor

    when(qtx.propertyKey("v")).thenReturn(42)
    when(qtx.propertyKey("c")).thenReturn(43)
    when(qtx.relationshipProperty(
      VirtualValues.relationship(0),
      42,
      relationshipScanCursor,
      propertyCursor,
      throwOnDeleted = true
    )).thenReturn(
      longValue(1)
    )
    when(qtx.relationshipProperty(
      VirtualValues.relationship(0),
      43,
      relationshipScanCursor,
      propertyCursor,
      throwOnDeleted = true
    )).thenReturn(
      Values.NO_VALUE
    )
    idx("v") should equal(longValue(1))
    idx("c") should equal(expectedNull)
  }

  test("should fail when not integer values are passed") {
    implicit val collection = literal(Seq(1, 2, 3, 4))

    a[CypherTypeException] should be thrownBy idx(1.0f)
    a[CypherTypeException] should be thrownBy idx(1.0d)
    a[CypherTypeException] should be thrownBy idx("bad value")
  }

  private def idx(value: Any)(implicit collection: Expression) =
    ContainerIndex(collection, literal(value))(ctx, state)
}
