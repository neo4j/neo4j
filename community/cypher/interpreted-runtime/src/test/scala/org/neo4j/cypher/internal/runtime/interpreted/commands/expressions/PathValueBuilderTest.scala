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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.virtual.VirtualValues.pathReference

class PathValueBuilderTest extends CypherFunSuite {

  private val A = VirtualValues.node(1)
  private val B = VirtualValues.node(2)
  private val C = VirtualValues.node(3)
  private val D = VirtualValues.node(4)
  private val E = VirtualValues.node(5)

  private val rel1 = VirtualValues.relationship(1)
  private val rel2 = VirtualValues.relationship(2)
  private val rel3 = VirtualValues.relationship(3)
  private val rel4 = VirtualValues.relationship(4)
  private val state = mockState

  private val graph = Map(rel1 -> (A -> B), rel2 -> (B -> C), rel3 -> (C -> D), rel4 -> (D -> E))

  test("p = (a)") {
    val builder = new PathValueBuilder(state)

    builder.addNode(A)

    builder.result() should equal(pathReference(Array(A.id()), Array.empty[Long]))
  }

  test("p = (a)-[r:X]->(b)") {
    val builder = new PathValueBuilder(state)

    builder.addNode(A)
      .addOutgoingRelationship(rel1)

    builder.result() should equal(pathReference(Array(A.id(), B.id()), Array(rel1.id())))
  }

  test("p = (b)<-[r:X]-(a)") {
    val builder = new PathValueBuilder(state)

    builder.addNode(B)
      .addIncomingRelationship(rel1)

    builder.result() should equal(pathReference(Array(B.id(), A.id()), Array(rel1.id())))
  }

  test("p = (a)-[r:X]-(b)") {
    val builder = new PathValueBuilder(state)

    builder.addNode(A)
      .addUndirectedRelationship(rel1)

    builder.result() should equal(pathReference(Array(A.id(), B.id()), Array(rel1.id())))
  }

  test("p = (b)-[r:X]-(a)") {
    val builder = new PathValueBuilder(state)

    builder.addNode(B)
      .addUndirectedRelationship(rel1)

    builder.result() should equal(pathReference(Array(B.id(), A.id()), Array(rel1.id())))
  }

  test("p = <empty> should throw") {
    val builder = new PathValueBuilder(state)

    an[IllegalArgumentException] shouldBe thrownBy {
      builder.result()
    }
  }

  test("p = (a)-[r:X*]->(b)") {
    val builder = new PathValueBuilder(state)

    builder.addNode(A)
      .addOutgoingRelationships(VirtualValues.list(rel1, rel2))

    builder.result() should equal(pathReference(Array(A.id(), B.id(), C.id()), Array(rel1.id(), rel2.id())))
  }

  test("p = (a)-[r:X*]->(b) when rels is null") {
    val builder = new PathValueBuilder(state)

    builder.addNode(A)
      .addOutgoingRelationships(NO_VALUE)

    builder.result() should equal(NO_VALUE)
  }

  test("p = (a)-[r:X]->(b)--(c)") {
    val builder = new PathValueBuilder(state)

    builder.addNode(A)
      .addOutgoingRelationship(rel1)
      .addUndirectedRelationship(rel2)

    builder.result() should equal(pathReference(Array(A.id(), B.id(), C.id()), Array(rel1.id(), rel2.id())))
  }

  test("p = (b)<-[r:X*]-(a)") {
    val builder = new PathValueBuilder(state)

    builder.addNode(C)
      .addIncomingRelationships(VirtualValues.list(rel2, rel1))

    builder.result() should equal(pathReference(Array(C.id(), B.id(), A.id()), Array(rel2.id(), rel1.id())))
  }

  test("p = (b)<-[r:X*]-(a) when rels is null") {
    val builder = new PathValueBuilder(state)

    builder.addNode(A)
      .addIncomingRelationships(NO_VALUE)

    builder.result() should equal(NO_VALUE)
  }

  test("p = (b)-[r:X*]-(a)") {
    val builder = new PathValueBuilder(state)

    builder.addNode(C)
      .addUndirectedRelationships(VirtualValues.list(rel2, rel1))

    builder.result() should equal(pathReference(Array(C.id(), B.id(), A.id()), Array(rel2.id(), rel1.id())))
  }

  test("p = (a)-[r1*]-()-[r2*]-()") {
    val builder = new PathValueBuilder(state)

    builder.addNode(A)
      .addUndirectedRelationships(VirtualValues.list(rel1, rel2))
      .addUndirectedRelationships(VirtualValues.list(rel3, rel4))

    builder.result() should equal(pathReference(
      Array(A.id(), B.id(), C.id(), D.id(), E.id()),
      Array(rel1.id(), rel2.id(), rel3.id(), rel4.id())
    ))
  }

  test("p = (b)-[r:X*0]-(a)") {
    val builder = new PathValueBuilder(state)

    builder.addNode(C)
      .addUndirectedRelationships(VirtualValues.EMPTY_LIST)

    builder.result() should equal(pathReference(Array(C.id()), Array.empty[Long]))
  }

  test("p = (b)-[r:X*]-(a) when rels is null") {
    val builder = new PathValueBuilder(state)

    builder.addNode(A)
      .addUndirectedRelationships(NO_VALUE)

    builder.result() should equal(NO_VALUE)
  }

  test("p = (a) when single node is null") {
    val builder = new PathValueBuilder(state)

    val result = builder
      .addNode(NO_VALUE)
      .result()

    result should equal(NO_VALUE)
  }

  test("p = (a) when single node is null also for mutable builder") {
    val builder = new PathValueBuilder(state)

    builder.addNode(NO_VALUE)

    builder.result() should equal(NO_VALUE)
  }

  test("p = (a)-[r]->(b) when relationship is null") {
    val builder = new PathValueBuilder(state)

    val result = builder
      .addNode(A)
      .addIncomingRelationship(NO_VALUE)
      .result()

    result should equal(NO_VALUE)
  }

  private def mockState = {
    val cursor = mock[RelationshipScanCursor]
    val cursors = mock[ExpressionCursors]
    val query = mock[QueryContext]
    val state = mock[QueryState]
    when(state.query).thenReturn(query)
    when(state.cursors).thenReturn(cursors)
    when(cursors.relationshipScanCursor).thenReturn(cursor)
    Mockito.doAnswer(new Answer[Void] {
      override def answer(invocationOnMock: InvocationOnMock): Void = {
        val rel = VirtualValues.relationship(invocationOnMock.getArgument(0))
        val cursor = invocationOnMock.getArgument(1).asInstanceOf[RelationshipScanCursor]
        graph.get(rel).foreach {
          case (start, end) =>
            when(cursor.next).thenReturn(true)
            when(cursor.sourceNodeReference()).thenReturn(start.id())
            when(cursor.targetNodeReference()).thenReturn(end.id())
        }
        null
      }
    }).when(query).singleRelationship(anyLong(), any[RelationshipScanCursor])
    state
  }
}
