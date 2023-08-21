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
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.storageengine.api.RelationshipVisitor
import org.neo4j.values.virtual.RelationshipValue

class VarLengthExpandPipeTest extends CypherFunSuite {

  private trait WasClosed {
    def wasClosed: Boolean
  }

  private def relationshipIterator: ClosingLongIterator with RelationshipIterator with WasClosed =
    new ClosingLongIterator with RelationshipIterator with WasClosed {
      private val inner = PrimitiveLongHelper.relationshipIteratorFrom((1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3))
      private var _wasClosed = false

      override def close(): Unit = _wasClosed = true

      override def wasClosed: Boolean = _wasClosed

      override protected[this] def innerHasNext: Boolean = inner.hasNext

      override def next(): Long = inner.next()

      override def relationshipVisit[EXCEPTION <: Exception](
        relationshipId: Long,
        visitor: RelationshipVisitor[EXCEPTION]
      ): Boolean = inner.relationshipVisit(relationshipId, visitor)

      override def startNodeId(): Long = inner.startNodeId()

      override def endNodeId(): Long = inner.endNodeId()

      override def typeId(): Int = inner.typeId()
    }

  private def relationshipValue(id: Long): RelationshipValue = {
    val r = mock[RelationshipValue]
    when(r.id).thenReturn(id)
    r
  }

  test("exhaust should close cursor") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)
    val nodeCursor = mock[NodeCursor]
    when(nodeCursor.next()).thenReturn(true, false)
    when(state.query.nodeCursor()).thenReturn(nodeCursor)
    val rels = relationshipIterator
    when(state.query.getRelationshipsForIds(any[Long], any[SemanticDirection], any[Array[Int]])).thenReturn(
      rels
    )

    when(state.query.relationshipById(any[Long], any[Long], any[Long], any[Int])).thenAnswer(
      (invocation: InvocationOnMock) => relationshipValue(invocation.getArgument[Long](0))
    )

    val input = FakePipe(Seq(Map("a" -> newMockedNode(10))))
    val pipe = VarLengthExpandPipe(
      input,
      "a",
      "r",
      "b",
      SemanticDirection.OUTGOING,
      SemanticDirection.OUTGOING,
      new EagerTypes(Array(0)),
      1,
      Some(2),
      nodeInScope = false
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
    when(nodeCursor.next()).thenReturn(true, false)
    when(state.query.nodeCursor()).thenReturn(nodeCursor)
    val rels = relationshipIterator
    when(state.query.getRelationshipsForIds(any[Long], any[SemanticDirection], any[Array[Int]])).thenReturn(
      rels
    )

    when(state.query.relationshipById(any[Long], any[Long], any[Long], any[Int])).thenAnswer(
      (invocation: InvocationOnMock) => relationshipValue(invocation.getArgument[Long](0))
    )

    val input = FakePipe(Seq(Map("a" -> newMockedNode(10))))
    val pipe = VarLengthExpandPipe(
      input,
      "a",
      "r",
      "b",
      SemanticDirection.OUTGOING,
      SemanticDirection.OUTGOING,
      new EagerTypes(Array(0)),
      1,
      Some(2),
      nodeInScope = false
    )()
    val result = pipe.createResults(state)
    result.hasNext shouldBe true // Need to initialize to get cursor registered
    result.close()
    input.wasClosed shouldBe true
    rels.wasClosed shouldBe true
  }

  private def newMockedNode(id: Int): Node = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    when(node.getElementId).thenReturn(id.toString)
    when(node.toString).thenReturn("node - " + id.toString)
    node
  }
}
