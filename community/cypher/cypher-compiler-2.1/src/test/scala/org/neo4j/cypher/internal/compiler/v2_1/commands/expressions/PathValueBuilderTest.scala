/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.commands.expressions

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.graphdb.{DynamicRelationshipType, Relationship, Node}
import org.neo4j.cypher.internal.PathImpl
import org.mockito.Mockito

class PathValueBuilderTest extends CypherFunSuite {

  val node1 = mockedNode(1)
  val node2 = mockedNode(2)
  val node3 = mockedNode(3)

  val rel1 = mockedRelationship(1, node1, node2)
  val rel2 = mockedRelationship(2, node2, node3)

  test("p = (a)") {
    val builder = new PathValueBuilder

    builder.addNode(node1)

    builder.result() should equal(new PathImpl(node1))
  }

  test("p = (a)-[r:X]->(b)") {
    val builder = new PathValueBuilder

    builder.addNode(node1)
      .addOutgoingRelationship(rel1)

    builder.result() should equal(new PathImpl(node1, rel1, node2))
  }

  test("p = (b)<-[r:X]-(a)") {
    val builder = new PathValueBuilder

    builder.addNode(node2)
      .addIncomingRelationship(rel1)

    builder.result() should equal(new PathImpl(node2, rel1, node1))
  }

  test("p = <empty> should throw") {
    val builder = new PathValueBuilder

    evaluating {
      builder.result()
    } should produce[IllegalArgumentException]
  }

  test("p = (a)-[r:X*]->(b)") {
    val builder = new PathValueBuilder

    builder.addNode(node1)
      .addOutgoingRelationships(Iterator(rel1, rel2))

    builder.result() should equal(new PathImpl(node1, rel1, node2, rel2, node3))
  }

  test("p = (b)<-[r:X*]-(a)") {
    val builder = new PathValueBuilder

    builder.addNode(node3)
      .addIncomingRelationships(Iterator(rel2, rel1))

    builder.result() should equal(new PathImpl(node3, rel2, node2, rel1, node1))
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
      .addNode(node1)
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
