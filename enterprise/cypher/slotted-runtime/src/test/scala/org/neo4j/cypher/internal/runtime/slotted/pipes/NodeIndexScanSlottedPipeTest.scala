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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.helpers.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, ImplicitDummyPos, QueryStateHelper}
import org.neo4j.cypher.internal.runtime.{IndexedPrimitiveNodeWithProperties, QueryContext}
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue
import org.opencypher.v9_0.expressions.{LabelName, LabelToken, PropertyKeyName, PropertyKeyToken}
import org.opencypher.v9_0.util.symbols._
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.util.{LabelId, PropertyKeyId}

class NodeIndexScanSlottedPipeTest extends CypherFunSuite with ImplicitDummyPos with SlottedPipeTestHelper {

  private val label = LabelToken(LabelName("LabelName")_, LabelId(11))
  private val propertyKey = PropertyKeyToken(PropertyKeyName("PropertyName")_, PropertyKeyId(10))
  private val node = nodeValue(11)

  private def nodeValue(id: Long) = {
    val node = mock[NodeValue]
    when(node.id()).thenReturn(id)
    node
  }

  test("should return nodes found by index scan when both labelId and property key id are solved at compile time") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = scanFor(Seq(IndexedPrimitiveNodeWithProperties(node.id, Array.empty)))
    )

    // when
    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
    val pipe = NodeIndexScanSlottedPipe("n", label, propertyKey, None, slots, slots.size())()
    val result = pipe.createResults(queryState)

    // then
    val list: Iterator[ExecutionContext] = result
    testableResult(list, slots).map(_("n")) should equal(List(node.id))
  }

  test("should use index provided values when available") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = scanFor(Seq(IndexedPrimitiveNodeWithProperties(node.id, Array(Values.stringValue("hello")))))
    )

    // when
    val nDotProp = "n." + propertyKey.name
    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
      .newReference(nDotProp, nullable = false, CTAny)
    val pipe = NodeIndexScanSlottedPipe("n", label, propertyKey, Some(slots.getReferenceOffsetFor(nDotProp)), slots, slots.size())()
    val result = pipe.createResults(queryState)

    // then
    val list: Iterator[ExecutionContext] = result
    testableResult(list, slots) should equal(List(
      Map("n" -> node.id, "n." + propertyKey.name -> Values.stringValue("hello"))
    ))
  }

  private def scanFor(results: Iterable[IndexedPrimitiveNodeWithProperties]): QueryContext = {
    val query = mock[QueryContext]
    when(query.indexScanPrimitive(any())).thenReturn(PrimitiveLongHelper.mapToPrimitive[IndexedPrimitiveNodeWithProperties](results.iterator, _.node))
    when(query.indexScanPrimitiveWithValues(any(), any())).thenReturn(results.iterator)
    query
  }

}
