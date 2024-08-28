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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.EagerTypes
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TraversalPredicates
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.storageengine.api.RelationshipVisitor
import org.neo4j.values.virtual.RelationshipValue

class VarLengthExpandSlottedPipeTest extends CypherFunSuite {

  private trait WasClosed {
    def wasClosed: Boolean
  }

  private def relationshipIterator: ClosingLongIterator with RelationshipIterator with WasClosed =
    new ClosingLongIterator with RelationshipIterator with WasClosed {
      private val inner = Iterator(1L, 2L, 3L)
      private var _wasClosed = false

      override def close(): Unit = _wasClosed = true

      override def wasClosed: Boolean = _wasClosed

      override protected[this] def innerHasNext: Boolean = inner.hasNext

      override def relationshipVisit[EXCEPTION <: Exception](
        relationshipId: Long,
        visitor: RelationshipVisitor[EXCEPTION]
      ): Boolean = {
        visitor.visit(relationshipId, -1, -1, -1)
        true
      }

      override def next(): Long = inner.next()

      override def startNodeId(): Long = 0

      override def endNodeId(): Long = 1

      override def typeId(): Int = 2
    }

  private def relationshipValue(id: Long): RelationshipValue = {
    val r = mock[RelationshipValue]
    Mockito.when(r.id).thenReturn(id)
    r
  }

  test("exhaust should close cursor") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)
    val nodeCursor = mock[NodeCursor]
    Mockito.when(nodeCursor.next()).thenReturn(true, false)
    Mockito.when(state.query.nodeCursor()).thenReturn(nodeCursor)
    val rels = relationshipIterator
    Mockito.when(state.query.getRelationshipsForIds(any[Long], any[SemanticDirection], any[Array[Int]])).thenReturn(
      rels
    )

    Mockito.when(state.query.relationshipById(any[Long], any[Long], any[Long], any[Int])).thenAnswer(
      (invocation: InvocationOnMock) => relationshipValue(invocation.getArgument[Long](0))
    )

    val slots = SlotConfiguration.empty
      .newLong("a", nullable = false, CTNode)
      .newReference("r", nullable = false, CTList(CTRelationship))
      .newLong("b", nullable = false, CTNode)

    val input = FakeSlottedPipe(Seq(Map("a" -> 10)), slots)
    val pipe = VarLengthExpandSlottedPipe(
      input,
      slots("a"),
      slots("r").offset,
      slots("b"),
      SemanticDirection.OUTGOING,
      SemanticDirection.OUTGOING,
      new EagerTypes(Array(0)),
      1,
      None,
      shouldExpandAll = true,
      slots,
      TraversalPredicates.NONE,
      SlotConfiguration.Size(0, 0)
    )()
    // exhaust
    pipe.createResults(state).toList
    input.wasClosed shouldBe true
    rels.wasClosed shouldBe true
  }

  test("close should close cursor") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)
    val nodeCursor = mock[NodeCursor]
    Mockito.when(nodeCursor.next()).thenReturn(true, false)
    Mockito.when(state.query.nodeCursor()).thenReturn(nodeCursor)
    val rels = relationshipIterator
    Mockito.when(state.query.getRelationshipsForIds(any[Long], any[SemanticDirection], any[Array[Int]])).thenReturn(
      rels
    )

    Mockito.when(state.query.relationshipById(any[Long], any[Long], any[Long], any[Int])).thenAnswer(
      (invocation: InvocationOnMock) => relationshipValue(invocation.getArgument[Long](0))
    )

    val slots = SlotConfiguration.empty
      .newLong("a", nullable = false, CTNode)
      .newReference("r", nullable = false, CTList(CTRelationship))
      .newLong("b", nullable = false, CTNode)

    val input = FakeSlottedPipe(Seq(Map("a" -> 10)), slots)
    val pipe = VarLengthExpandSlottedPipe(
      input,
      slots("a"),
      slots("r").offset,
      slots("b"),
      SemanticDirection.OUTGOING,
      SemanticDirection.OUTGOING,
      new EagerTypes(Array(0)),
      1,
      None,
      shouldExpandAll = true,
      slots,
      TraversalPredicates.NONE,
      SlotConfiguration.Size(0, 0)
    )()
    val result = pipe.createResults(state)
    result.hasNext shouldBe true // Need to initialize to get cursor registered
    result.close()
    input.wasClosed shouldBe true
    rels.wasClosed shouldBe true
  }
}
