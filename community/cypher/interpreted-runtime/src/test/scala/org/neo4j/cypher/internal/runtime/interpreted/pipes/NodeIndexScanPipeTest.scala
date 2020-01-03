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
import org.neo4j.cypher.internal.runtime.interpreted.{ImplicitDummyPos, QueryStateHelper}
import org.neo4j.cypher.internal.v3_5.logical.plans.{DoNotGetValue, GetValue, IndexOrderNone, IndexedProperty}
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue
import org.neo4j.cypher.internal.v3_5.expressions.{LabelName, LabelToken, PropertyKeyName, PropertyKeyToken}
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.util.{LabelId, PropertyKeyId}

class NodeIndexScanPipeTest extends CypherFunSuite with ImplicitDummyPos with IndexMockingHelp {

  private val label = LabelToken(LabelName("LabelName")_, LabelId(11))
  private val propertyKey = PropertyKeyToken(PropertyKeyName("prop")_, PropertyKeyId(10))
  override val propertyKeys = Seq(propertyKey)
  private val node = nodeValue(11)

  private def nodeValue(id: Long) = {
    val node = mock[NodeValue]
    when(node.id()).thenReturn(id)
    node
  }

  test("should return nodes found by index scan when both labelId and property key id are solved at compile time") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = scanFor(List(nodeValueHit(node)))
    )

    // when
    val pipe = NodeIndexScanPipe("n", label, IndexedProperty(propertyKey, DoNotGetValue), IndexOrderNone)()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node))
  }

  test("should use cache node properties when asked for") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = scanFor(List(nodeValueHit(node, "hello")))
    )

    // when
    val pipe = NodeIndexScanPipe("n", label, IndexedProperty(propertyKey, GetValue), IndexOrderNone)()
    val result = pipe.createResults(queryState).toList

    // then
    result.map(_("n")) should be(List(node))
    result.head.getCachedProperty(cachedNodeProperty("n", propertyKey)) should be(Values.stringValue("hello"))
  }
}
