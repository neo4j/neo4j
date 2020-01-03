/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.mockito.Mockito._
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.{ImplicitDummyPos, QueryStateHelper}
import org.neo4j.cypher.internal.v3_5.logical.plans.{GetValue, IndexOrderNone, IndexedProperty}
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue
import org.neo4j.cypher.internal.v3_5.expressions.{LabelName, LabelToken, PropertyKeyName, PropertyKeyToken}
import org.neo4j.cypher.internal.v3_5.util.test_helpers.{CypherFunSuite, WindowsStringSafe}
import org.neo4j.cypher.internal.v3_5.util.{LabelId, PropertyKeyId}

class NodeIndexStringSeekPipeTest extends CypherFunSuite with ImplicitDummyPos with IndexMockingHelp {

  implicit val windowsSafe = WindowsStringSafe

  private val label = LabelToken(LabelName("LabelName") _, LabelId(11))
  private val propertyKey = PropertyKeyToken(PropertyKeyName("PropertyName") _, PropertyKeyId(10))
  override val propertyKeys = Seq(propertyKey)
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
      query = stringIndexFor(
        "hello" -> Seq(nodeValueHit(node, "hello")),
        "bye" -> Seq(nodeValueHit(node2, "bye"))
      )
    )

    // when
    val pipe = NodeIndexEndsWithScanPipe("n", label, IndexedProperty(propertyKey, GetValue), Literal("hello"), IndexOrderNone)()
    val result = pipe.createResults(queryState).toList

    // then
    result.map(_("n")) should be(List(node))
    result.map(_.getCachedProperty(cachedNodeProperty("n", propertyKey))) should be(
      List(Values.stringValue("hello"))
    )
  }

  test("should use index provided values when available for contains with") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = stringIndexFor(
        "hello" -> Seq(nodeValueHit(node, "hello")),
        "bye" -> Seq(nodeValueHit(node2, "bye"))
      )
    )

    // when
    val pipe = NodeIndexContainsScanPipe("n", label, IndexedProperty(propertyKey, GetValue), Literal("bye"), IndexOrderNone)()
    val result = pipe.createResults(queryState).toList

    // then
    result.map(_("n")) should be(List(node2))
    result.map(_.getCachedProperty(cachedNodeProperty("n", propertyKey))) should be(
      List(Values.stringValue("bye"))
    )
  }
}
