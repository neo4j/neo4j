/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.{ImplicitDummyPos, QueryStateHelper}
import org.neo4j.cypher.internal.runtime.{IndexedNodeWithProperties, QueryContext}
import org.neo4j.cypher.internal.v3_5.logical.plans.{GetValue, IndexedProperty}
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue
import org.opencypher.v9_0.expressions.{LabelName, LabelToken, PropertyKeyName, PropertyKeyToken}
import org.opencypher.v9_0.util.test_helpers.{CypherFunSuite, WindowsStringSafe}
import org.opencypher.v9_0.util.{LabelId, PropertyKeyId}

class NodeIndexStringSeekPipeTest extends CypherFunSuite with ImplicitDummyPos {

  implicit val windowsSafe = WindowsStringSafe

  private val label = LabelToken(LabelName("LabelName") _, LabelId(11))
  private val propertyKey = PropertyKeyToken(PropertyKeyName("PropertyName") _, PropertyKeyId(10))
  private val node = nodeValue(1)
  private val node2 = nodeValue(2)

  private def nodeValue(id: Long) = {
    val node = mock[NodeValue]
    when(node.id()).thenReturn(id)
    node
  }

  test("should use index provided values when available for ends with") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = indexFor(
        "hello" -> Seq(IndexedNodeWithProperties(node, Array(Values.stringValue("hello")))),
        "bye" -> Seq(IndexedNodeWithProperties(node2, Array(Values.stringValue("bye"))))
      )
    )

    // when
    val pipe = NodeIndexEndsWithScanPipe("n", label, IndexedProperty(propertyKey, GetValue), Literal("hello"))()
    val result = pipe.createResults(queryState)

    // then
    result.toList should equal(List(
      Map("n" -> node, "n." + propertyKey.name -> Values.stringValue("hello"))
    ))
  }

  test("should use index provided values when available for contains with") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = indexFor(
        "hello" -> Seq(IndexedNodeWithProperties(node, Array(Values.stringValue("hello")))),
        "bye" -> Seq(IndexedNodeWithProperties(node2, Array(Values.stringValue("bye"))))
      )
    )

    // when
    val pipe = NodeIndexContainsScanPipe("n", label, IndexedProperty(propertyKey, GetValue), Literal("bye"))()
    val result = pipe.createResults(queryState)

    // then
    result.toList should equal(List(
      Map("n" -> node2, "n." + propertyKey.name -> Values.stringValue("bye"))
    ))
  }

  private def indexFor(values: (String, Iterable[IndexedNodeWithProperties])*): QueryContext = {
    val query = mock[QueryContext]
    when(query.indexSeek(any(), any(), any())).thenReturn(Iterator.empty)
    when(query.lockingUniqueIndexSeek(any(), any(), any())).thenReturn(None)

    values.foreach {
      case (searchTerm, resultIterable) =>
        when(query.indexSeekByContains(any(), any(), ArgumentMatchers.eq(searchTerm))).thenReturn(resultIterable.toIterator)
        when(query.indexSeekByEndsWith(any(), any(), ArgumentMatchers.eq(searchTerm))).thenReturn(resultIterable.toIterator)
    }

    query
  }
}
