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

import org.eclipse.collections.api.set.primitive.IntSet
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.NodeReadOperations
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.RelationshipReadOperations
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.virtual.VirtualValues.map

import java.util

class PropertiesFunctionTest extends CypherFunSuite {

  private val query = mock[QueryContext]
  private val nodeReadOps = mock[NodeReadOperations]
  private val relReadOps = mock[RelationshipReadOperations]
  private val state = QueryStateHelper.empty.withQueryContext(query)

  when(query.nodeReadOps).thenReturn(nodeReadOps)
  when(query.relationshipReadOps).thenReturn(relReadOps)

  test("should return null if argument is null") {
    properties(null.asInstanceOf[AnyRef]) should be(NO_VALUE)
  }

  test("should map Java maps to maps") {
    val m = new util.HashMap[String, String]()
    m.put("a", "x")
    m.put("b", "y")

    properties(m) should equal(map(Array("a", "b"), Array(stringValue("x"), stringValue("y"))))
  }

  test("should map Scala maps to maps") {
    val m = Map("a" -> "x", "b" -> "y")
    properties(m) should equal(map(Array("a", "b"), Array(stringValue("x"), stringValue("y"))))

  }

  test("should map nodes to maps") {
    val node = mock[Node]
    when(node.getId).thenReturn(0)
    when(node.getElementId).thenReturn("dummy")
    val value = map(Array("a", "b"), Array(stringValue("x"), stringValue("y")))
    when(
      query.nodeAsMap(0, state.cursors.nodeCursor, state.cursors.propertyCursor, any[MapValueBuilder], any[IntSet])
    ).thenReturn(value)

    properties(node) should equal(value)
  }

  test("should map relationships to maps") {
    val rel = mock[Relationship]
    when(rel.getId).thenReturn(0)
    val value = map(Array("a", "b"), Array(stringValue("x"), stringValue("y")))
    when(query.relationshipAsMap(
      VirtualValues.relationship(rel.getId),
      state.cursors.relationshipScanCursor,
      state.cursors.propertyCursor,
      any[MapValueBuilder],
      any[IntSet]
    )).thenReturn(
      value
    )

    properties(rel) should equal(value)
  }

  test("should fail trying to map an int") {
    a[CypherTypeException] should be thrownBy {
      properties(12)
    }
  }

  test("should fail trying to map a string") {
    a[CypherTypeException] should be thrownBy {
      properties("Hullo")
    }
  }

  test("should fail trying to map a list") {
    a[CypherTypeException] should be thrownBy {
      properties(List.empty)
    }
  }

  private def properties(orig: Any) = {
    PropertiesFunction(literal(orig))(CypherRow.empty, state)
  }
}
