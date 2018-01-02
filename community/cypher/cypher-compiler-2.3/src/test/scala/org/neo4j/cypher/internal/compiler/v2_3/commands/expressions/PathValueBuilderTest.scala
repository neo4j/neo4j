/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.mockito.Mockito
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{DynamicRelationshipType, Node, Relationship}

class PathValueBuilderTest extends CypherFunSuite {

  val A = mockedNode(1)
  val B = mockedNode(2)
  val C = mockedNode(3)
  val D = mockedNode(4)
  val E = mockedNode(5)

  val rel1 = mockedRelationship(1, A, B)
  val rel2 = mockedRelationship(2, B, C)
  val rel3 = mockedRelationship(3, C, D)
  val rel4 = mockedRelationship(3, D, E)

  test("p = (a)") {
    val builder = new PathValueBuilder

    builder.addNode(A)

    builder.result() should equal(new PathImpl(A))
  }

  test("p = (a)-[r:X]->(b)") {
    val builder = new PathValueBuilder

    builder.addNode(A)
      .addOutgoingRelationship(rel1)

    builder.result() should equal(new PathImpl(A, rel1, B))
  }

  test("p = (b)<-[r:X]-(a)") {
    val builder = new PathValueBuilder

    builder.addNode(B)
      .addIncomingRelationship(rel1)

    builder.result() should equal(new PathImpl(B, rel1, A))
  }

  test("p = (a)-[r:X]-(b)") {
    val builder = new PathValueBuilder

    builder.addNode(A)
      .addUndirectedRelationship(rel1)

    builder.result() should equal(new PathImpl(A, rel1, B))
  }

  test("p = (b)-[r:X]-(a)") {
    val builder = new PathValueBuilder

    builder.addNode(B)
      .addUndirectedRelationship(rel1)

    builder.result() should equal(new PathImpl(B, rel1, A))
  }

  test("p = <empty> should throw") {
    val builder = new PathValueBuilder

    evaluating {
      builder.result()
    } should produce[IllegalArgumentException]
  }

  test("p = (a)-[r:X*]->(b)") {
    val builder = new PathValueBuilder

    builder.addNode(A)
      .addOutgoingRelationships(Iterable(rel1, rel2))

    builder.result() should equal(new PathImpl(A, rel1, B, rel2, C))
  }

  test("p = (a)-[r:X*]->(b) when rels is null") {
    val builder = new PathValueBuilder

    builder.addNode(A)
      .addOutgoingRelationships(null)

    builder.result() should equal(null)
  }

  test("p = (a)-[r:X]->(b)--(c)") {
    val builder = new PathValueBuilder

    builder.addNode(A)
      .addOutgoingRelationship(rel1)
      .addUndirectedRelationship(rel2)

    builder.result() should equal(new PathImpl(A, rel1, B, rel2, C))
  }

  test("p = (b)<-[r:X*]-(a)") {
    val builder = new PathValueBuilder

    builder.addNode(C)
      .addIncomingRelationships(Iterable(rel2, rel1))

    builder.result() should equal(new PathImpl(C, rel2, B, rel1, A))
  }

  test("p = (b)<-[r:X*]-(a) when rels is null") {
    val builder = new PathValueBuilder

    builder.addNode(A)
      .addIncomingRelationships(null)

    builder.result() should equal(null)
  }

  test("p = (b)-[r:X*]-(a)") {
    val builder = new PathValueBuilder

    builder.addNode(C)
      .addUndirectedRelationships(Iterable(rel2, rel1))

    builder.result() should equal(new PathImpl(C, rel2, B, rel1, A))
  }

  test("p = (b)-[r:X*]-(a) reversed") {
    val builder = new PathValueBuilder

    builder.addNode(C)
      .addUndirectedRelationships(Iterable(rel1, rel2))

    builder.result() should equal(new PathImpl(C, rel2, B, rel1, A))
  }

  test("p = (a)-[r1*]-()-[r2*]-()") {
    val builder = new PathValueBuilder

    builder.addNode(A)
      .addUndirectedRelationships(Iterable(rel1, rel2))
      .addUndirectedRelationships(Iterable(rel3, rel4))

    builder.result() should equal(new PathImpl(A, rel1, B, rel2, C, rel3, D, rel4, E))
  }

  test("p = (a)-[r1*]-()-[r2*]-() reversed r1") {
    val builder = new PathValueBuilder

    builder.addNode(A)
      .addUndirectedRelationships(Iterable(rel2, rel1))
      .addUndirectedRelationships(Iterable(rel3, rel4))

    builder.result() should equal(new PathImpl(A, rel1, B, rel2, C, rel3, D, rel4, E))
  }

  test("p = (a)-[r1*]-()-[r2*]-() reversed r2") {
    val builder = new PathValueBuilder

    builder.addNode(A)
      .addUndirectedRelationships(Iterable(rel1, rel2))
      .addUndirectedRelationships(Iterable(rel4, rel3))

    builder.result() should equal(new PathImpl(A, rel1, B, rel2, C, rel3, D, rel4, E))
  }

  test("p = (a)-[r1*]-()-[r2*]-() reversed r1 && r2") {
    val builder = new PathValueBuilder

    builder.addNode(A)
      .addUndirectedRelationships(Iterable(rel2, rel1))
      .addUndirectedRelationships(Iterable(rel4, rel3))

    builder.result() should equal(new PathImpl(A, rel1, B, rel2, C, rel3, D, rel4, E))
  }

  test("p = (b)-[r:X*0]-(a)") {
    val builder = new PathValueBuilder

    builder.addNode(C)
      .addUndirectedRelationships(Iterable())

    builder.result() should equal(new PathImpl(C))
  }

  test("p = (b)-[r:X*]-(a) when rels is null") {
    val builder = new PathValueBuilder

    builder.addNode(A)
      .addUndirectedRelationships(null)

    builder.result() should equal(null)
  }

  test("p = (a) when single node is null") {
    val builder = new PathValueBuilder

    val result = builder
      .addNode(null)
      .result()

    result should equal(null)
  }

  test("p = (a) when single node is null also for mutable builder") {
    val builder = new PathValueBuilder

    builder.addNode(null)

    builder.result() should equal(null)
  }

  test("p = (a)-[r]->(b) when relationship is null") {
    val builder = new PathValueBuilder

    val result = builder
      .addNode(A)
      .addIncomingRelationship(null)
      .result()

    result should equal(null)
  }

  private def mockedRelationship(id: Long, start: Node, end: Node) = {
    val rel = mock[Relationship]

    Mockito.when(rel.getId).thenReturn(id)
    Mockito.when(rel.getStartNode).thenReturn(start)
    Mockito.when(rel.getEndNode).thenReturn(end)
    Mockito.when(rel.getType).thenReturn(DynamicRelationshipType.withName("X"))
    Mockito.when(rel.getOtherNode(start)).thenReturn(end)
    Mockito.when(rel.getOtherNode(end)).thenReturn(start)

    rel
  }

  private def mockedNode(id: Long): Node = {
    val node = mock[Node]
    Mockito.when(node.getId).thenReturn(id)
    node
  }
}
