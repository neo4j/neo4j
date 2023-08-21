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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.helpers.StubNodeValueIndexCursor

class NodeIndexScanPipeTest extends CypherFunSuite {

  test("exhaust should close cursor") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)

    val cursor = new StubNodeValueIndexCursor().withNode(0)
    when(state.query.nodeIndexScan(any[IndexReadSession], any[Boolean], any[IndexOrder])).thenAnswer(
      (_: InvocationOnMock) => {
        // NOTE: this is what is done in TransactionBoundQueryContext
        resourceManager.trace(cursor)
        cursor
      }
    )

    val pipe = NodeIndexScanPipe(
      "n",
      LabelToken("Awesome", LabelId(0)),
      Seq(IndexedProperty(PropertyKeyToken("prop", PropertyKeyId(0)), DoNotGetValue, NODE_TYPE)),
      0,
      IndexOrderNone
    )()
    // exhaust
    pipe.createResults(state).toList
    monitor.closedResources.collect { case `cursor` => cursor } should have size (1)
  }

  test("close should close cursor") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)

    val cursor = new StubNodeValueIndexCursor().withNode(0)
    when(state.query.nodeIndexScan(any[IndexReadSession], any[Boolean], any[IndexOrder])).thenAnswer(
      (_: InvocationOnMock) => {
        // NOTE: this is what is done in TransactionBoundQueryContext
        resourceManager.trace(cursor)
        cursor
      }
    )

    val pipe = NodeIndexScanPipe(
      "n",
      LabelToken("Awesome", LabelId(0)),
      Seq(IndexedProperty(PropertyKeyToken("prop", PropertyKeyId(0)), DoNotGetValue, NODE_TYPE)),
      0,
      IndexOrderNone
    )()
    val result = pipe.createResults(state)
    result.close()
    monitor.closedResources.collect { case `cursor` => cursor } should have size (1)
  }
}
