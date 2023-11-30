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
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.ImplicitDummyPos
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.helpers.StubNodeValueIndexCursor
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue

class NodeIndexStringScanPipeTest extends CypherFunSuite with ImplicitDummyPos with IndexMockingHelp {
  implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

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
    val pipe = NodeIndexEndsWithScanPipe(
      "n",
      label,
      IndexedProperty(propertyKey, GetValue, NODE_TYPE),
      0,
      literal("hello"),
      IndexOrderNone
    )()
    val result = pipe.createResults(queryState).toList

    // then
    result.map(_.getByName("n").asInstanceOf[NodeValue].id()) should be(List(node.id()))
    result.map(_.getCachedProperty(cachedProperty("n", propertyKey).runtimeKey)) should be(
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
    val pipe = NodeIndexContainsScanPipe(
      "n",
      label,
      IndexedProperty(propertyKey, GetValue, NODE_TYPE),
      0,
      literal("bye"),
      IndexOrderNone
    )()
    val result = pipe.createResults(queryState).toList

    // then
    result.map(_.getByName("n").asInstanceOf[NodeValue].id()) should be(List(node2.id()))
    result.map(_.getCachedProperty(cachedProperty("n", propertyKey).runtimeKey)) should be(
      List(Values.stringValue("bye"))
    )
  }

  test("Contains: exhaust should close cursor") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)

    val cursor = new StubNodeValueIndexCursor().withNode(0)
    when(
      state.query.nodeIndexSeekByContains(any[IndexReadSession], any[Boolean], any[IndexOrder], any[TextValue])
    ).thenAnswer((_: InvocationOnMock) => {
      // NOTE: this is what is done in TransactionBoundQueryContext
      resourceManager.trace(cursor)
      cursor
    })

    val pipe = NodeIndexContainsScanPipe(
      "n",
      LabelToken("Awesome", LabelId(0)),
      IndexedProperty(PropertyKeyToken("prop", PropertyKeyId(0)), DoNotGetValue, NODE_TYPE),
      0,
      LiteralHelper.literal("text"),
      IndexOrderNone
    )()
    // exhaust
    pipe.createResults(state).toList
    monitor.closedResources.collect { case `cursor` => cursor } should have size (1)
  }

  test("Contains: close should close cursor") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)

    val cursor = new StubNodeValueIndexCursor().withNode(0)
    when(
      state.query.nodeIndexSeekByContains(any[IndexReadSession], any[Boolean], any[IndexOrder], any[TextValue])
    ).thenAnswer((_: InvocationOnMock) => {
      // NOTE: this is what is done in TransactionBoundQueryContext
      resourceManager.trace(cursor)
      cursor
    })
    val pipe = NodeIndexContainsScanPipe(
      "n",
      LabelToken("Awesome", LabelId(0)),
      IndexedProperty(PropertyKeyToken("prop", PropertyKeyId(0)), DoNotGetValue, NODE_TYPE),
      0,
      LiteralHelper.literal("text"),
      IndexOrderNone
    )()
    val result = pipe.createResults(state)
    result.close()
    monitor.closedResources.collect { case `cursor` => cursor } should have size (1)
  }

  test("Ends with: exhaust should close cursor") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)

    val cursor = new StubNodeValueIndexCursor().withNode(0)
    when(
      state.query.nodeIndexSeekByEndsWith(any[IndexReadSession], any[Boolean], any[IndexOrder], any[TextValue])
    ).thenAnswer((_: InvocationOnMock) => {
      // NOTE: this is what is done in TransactionBoundQueryContext
      resourceManager.trace(cursor)
      cursor
    })
    val pipe = NodeIndexEndsWithScanPipe(
      "n",
      LabelToken("Awesome", LabelId(0)),
      IndexedProperty(PropertyKeyToken("prop", PropertyKeyId(0)), DoNotGetValue, NODE_TYPE),
      0,
      LiteralHelper.literal("text"),
      IndexOrderNone
    )()
    // exhaust
    pipe.createResults(state).toList
    monitor.closedResources.collect { case `cursor` => cursor } should have size (1)
  }

  test("Ends with: close should close cursor") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)

    val cursor = new StubNodeValueIndexCursor().withNode(0)
    when(
      state.query.nodeIndexSeekByEndsWith(any[IndexReadSession], any[Boolean], any[IndexOrder], any[TextValue])
    ).thenAnswer((_: InvocationOnMock) => {
      // NOTE: this is what is done in TransactionBoundQueryContext
      resourceManager.trace(cursor)
      cursor
    })
    val pipe = NodeIndexEndsWithScanPipe(
      "n",
      LabelToken("Awesome", LabelId(0)),
      IndexedProperty(PropertyKeyToken("prop", PropertyKeyId(0)), DoNotGetValue, NODE_TYPE),
      0,
      LiteralHelper.literal("text"),
      IndexOrderNone
    )()
    val result = pipe.createResults(state)
    result.close()
    monitor.closedResources.collect { case `cursor` => cursor } should have size (1)
  }
}
