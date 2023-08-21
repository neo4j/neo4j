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

import org.mockito.Mockito
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.helpers.CachingExpandInto
import org.neo4j.internal.kernel.api.helpers.StubNodeCursor
import org.neo4j.internal.kernel.api.helpers.StubRelationshipCursor
import org.neo4j.internal.kernel.api.helpers.TestRelationshipChain

class ExpandIntoPipeTest extends CypherFunSuite {

  test("exhaust should close cursor and cache") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)
    val nodeCursor = new StubNodeCursor(false)
      .withNode(10).withNode(10).withDegree(25)
    val relCursor = new StubRelationshipCursor(new TestRelationshipChain(10).outgoing(1, 20, 0))
    Mockito.when(state.query.traversalCursor()).thenReturn(relCursor)
    Mockito.when(state.query.nodeCursor()).thenReturn(nodeCursor)

    val input = new FakePipe(Seq(Map("a" -> newMockedNode(10), "b" -> newMockedNode(20))))
    val pipe = ExpandIntoPipe(input, "a", "r", "b", SemanticDirection.OUTGOING, new EagerTypes(Array(0)))()
    // exhaust
    pipe.createResults(state).toList
    input.wasClosed shouldBe true
    // Our RelationshipTraversalCursor is wrapped in an ExpandIntoSelectionCursor. Thus not asserting on same instance.
    monitor.closedResources.collect { case r: RelationshipTraversalCursor => r } should have size (1)
    monitor.closedResources.collect { case r: CachingExpandInto => r } should have size (1)
  }

  test("close should close cursor and cache") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)
    val nodeCursor = new StubNodeCursor(false)
      .withNode(10).withNode(10).withDegree(25)
    val relCursor = new StubRelationshipCursor(new TestRelationshipChain(10).outgoing(1, 20, 0))
    Mockito.when(state.query.traversalCursor()).thenReturn(relCursor)
    Mockito.when(state.query.nodeCursor()).thenReturn(nodeCursor)

    val input = new FakePipe(Seq(Map("a" -> newMockedNode(10), "b" -> newMockedNode(20))))
    val pipe = ExpandIntoPipe(input, "a", "r", "b", SemanticDirection.OUTGOING, new EagerTypes(Array(0)))()
    val result = pipe.createResults(state)
    result.hasNext shouldBe true // Need to initialize to get cursor registered
    result.close()
    input.wasClosed shouldBe true
    // Our RelationshipTraversalCursor is wrapped in an ExpandIntoSelectionCursor. Thus not asserting on same instance.
    monitor.closedResources.collect { case r: RelationshipTraversalCursor => r } should have size (1)
    monitor.closedResources.collect { case r: CachingExpandInto => r } should have size (1)
  }

  private def newMockedNode(id: Int): Node = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    when(node.getElementId).thenReturn(id.toString)
    when(node.toString).thenReturn("node - " + id.toString)
    node
  }
}
