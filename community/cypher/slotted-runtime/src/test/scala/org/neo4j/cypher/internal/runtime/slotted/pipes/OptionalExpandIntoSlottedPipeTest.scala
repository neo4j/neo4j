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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.mockito.Mockito
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.EagerTypes
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.helpers.CachingExpandInto
import org.neo4j.internal.kernel.api.helpers.StubNodeCursor
import org.neo4j.internal.kernel.api.helpers.StubRelationshipCursor
import org.neo4j.internal.kernel.api.helpers.TestRelationshipChain

class OptionalExpandIntoSlottedPipeTest extends CypherFunSuite {

  test("exhaust should close cursor and cache") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)
    val nodeCursor = new StubNodeCursor(false)
      .withNode(10).withNode(10).withDegree(25)
    val relCursor = new StubRelationshipCursor(new TestRelationshipChain(10).outgoing(1, 20, 0))
    Mockito.when(state.query.traversalCursor()).thenReturn(relCursor)
    Mockito.when(state.query.nodeCursor()).thenReturn(nodeCursor)

    val slots = SlotConfiguration.empty
      .newLong("a", nullable = false, CTNode)
      .newLong("r", nullable = false, CTRelationship)
      .newLong("b", nullable = false, CTNode)

    val input = FakeSlottedPipe(Seq(Map("a" -> 10, "b" -> 20)), slots)
    val pipe = OptionalExpandIntoSlottedPipe(
      input,
      slots("a"),
      1,
      slots("b"),
      SemanticDirection.OUTGOING,
      new EagerTypes(Array(0)),
      slots,
      None
    )()
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

    val slots = SlotConfiguration.empty
      .newLong("a", nullable = false, CTNode)
      .newLong("r", nullable = false, CTRelationship)
      .newLong("b", nullable = false, CTNode)

    val input = FakeSlottedPipe(Seq(Map("a" -> 10, "b" -> 20)), slots)
    val pipe = OptionalExpandIntoSlottedPipe(
      input,
      slots("a"),
      1,
      slots("b"),
      SemanticDirection.OUTGOING,
      new EagerTypes(Array(0)),
      slots,
      None
    )()
    val result = pipe.createResults(state)
    result.hasNext shouldBe true // Need to initialize to get cursor registered
    result.close()
    input.wasClosed shouldBe true
    // Our RelationshipTraversalCursor is wrapped in an ExpandIntoSelectionCursor. Thus not asserting on same instance.
    monitor.closedResources.collect { case r: RelationshipTraversalCursor => r } should have size (1)
    monitor.closedResources.collect { case r: CachingExpandInto => r } should have size (1)
  }
}
