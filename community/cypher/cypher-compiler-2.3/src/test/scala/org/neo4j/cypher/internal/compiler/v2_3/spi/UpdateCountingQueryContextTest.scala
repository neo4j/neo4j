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
package org.neo4j.cypher.internal.compiler.v2_3.spi

import org.mockito.Matchers._
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v2_3.InternalQueryStatistics
import org.neo4j.cypher.internal.compiler.v2_3.spi.SchemaTypes.{IndexDescriptor, NodePropertyExistenceConstraint, RelationshipPropertyExistenceConstraint, UniquenessConstraint}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{Node, Relationship}

class UpdateCountingQueryContextTest extends CypherFunSuite {

  val inner = mock[QueryContext]
  val nodeA = mock[Node]
  val nodeB = mock[Node]
  val nodeAId = 666
  val rel = mock[Relationship]
  val relId = 42

  val nodeOps = mock[Operations[Node]]
  val relOps = mock[Operations[Relationship]]

  when(inner.nodeOps).thenReturn(nodeOps)
  when(inner.relationshipOps).thenReturn(relOps)

  // We need to have the inner mock return the right counts for added/removed labels.
  when( inner.setLabelsOnNode(anyLong(), any()) ).thenAnswer( new Answer[Int]() {
    def answer(invocation: InvocationOnMock):Int = {
      invocation.getArguments()(1).asInstanceOf[Iterator[String]].size
    }
  } )

  when( inner.removeLabelsFromNode(anyLong(), any()) ).thenAnswer( new Answer[Int]() {
    def answer(invocation: InvocationOnMock):Int = {
      invocation.getArguments()(1).asInstanceOf[Iterator[String]].size
    }
  } )

  when( inner.createUniqueConstraint(anyInt(), anyInt()) )
    .thenReturn(IdempotentResult(mock[UniquenessConstraint]))

  when( inner.createNodePropertyExistenceConstraint(anyInt(), anyInt()) )
    .thenReturn(IdempotentResult(mock[NodePropertyExistenceConstraint]))

  when( inner.createRelationshipPropertyExistenceConstraint(anyInt(), anyInt()) )
    .thenReturn(IdempotentResult(mock[RelationshipPropertyExistenceConstraint]))

  when( inner.addIndexRule(anyInt(), anyInt()) )
    .thenReturn(IdempotentResult(mock[IndexDescriptor]))

  var context: UpdateCountingQueryContext = null

  override def beforeEach() {
    super.beforeEach()
    context = new UpdateCountingQueryContext(inner)
  }

  test("create_node") {
    context.createNode()

    context.getStatistics should equal(InternalQueryStatistics(nodesCreated = 1))
  }

  test("delete_node") {
    context.nodeOps.delete(nodeA)

    context.getStatistics should equal(InternalQueryStatistics(nodesDeleted = 1))
  }

  test("create_relationship") {
    context.createRelationship(nodeA, nodeB, "FOO")

    context.getStatistics should equal(InternalQueryStatistics(relationshipsCreated = 1))
  }

  test("delete_relationship") {
    context.relationshipOps.delete(rel)

    context.getStatistics should equal(InternalQueryStatistics(relationshipsDeleted = 1))
  }

  test("set_property") {
    context.nodeOps.setProperty(nodeAId, 1, "value")

    context.getStatistics should equal(InternalQueryStatistics(propertiesSet = 1))
  }

  test("remove_property") {
    context.nodeOps.removeProperty(nodeAId, context.getPropertyKeyId("key"))

    context.getStatistics should equal(InternalQueryStatistics(propertiesSet = 1))
  }

  test("set_property_relationship") {
    context.relationshipOps.setProperty(relId, 1, "value")

    context.getStatistics should equal(InternalQueryStatistics(propertiesSet = 1))
  }

  test("remove_property_relationship") {
    context.relationshipOps.removeProperty(relId, context.getPropertyKeyId("key"))

    context.getStatistics should equal(InternalQueryStatistics(propertiesSet = 1))
  }
//
//  test("add_label") {
//    context.setLabelsOnNode(0l, Seq(1, 2, 3).iterator)
//
//    context.getStatistics should equal(QueryStatistics(labelsAdded = 3))
//  }

  test("remove_label") {
    context.removeLabelsFromNode(0l, Seq(1, 2, 3).iterator)

    context.getStatistics should equal(InternalQueryStatistics(labelsRemoved = 3))
  }

  test("add_index") {
    context.addIndexRule(0, 1)

    context.getStatistics should equal(InternalQueryStatistics(indexesAdded = 1))
  }

  test("remove_index") {
    context.dropIndexRule(0, 1)

    context.getStatistics should equal(InternalQueryStatistics(indexesRemoved = 1))
  }

  test("create_unique_constraint") {
    context.createUniqueConstraint(0, 1)

    context.getStatistics should equal(InternalQueryStatistics(uniqueConstraintsAdded = 1))
  }

  test("constraint_dropped") {
    context.dropUniqueConstraint(0, 42)

    context.getStatistics should equal(InternalQueryStatistics(uniqueConstraintsRemoved = 1))
  }

  test("create node property existence constraint") {
    context.createNodePropertyExistenceConstraint(0, 1)

    context.getStatistics should equal(InternalQueryStatistics(existenceConstraintsAdded = 1))
  }

  test("drop node property existence constraint") {
    context.dropNodePropertyExistenceConstraint(0, 42)

    context.getStatistics should equal(InternalQueryStatistics(existenceConstraintsRemoved = 1))
  }

  test("create rel property existence constraint") {
    context.createRelationshipPropertyExistenceConstraint(0, 42)

    context.getStatistics should equal(InternalQueryStatistics(existenceConstraintsAdded = 1))
  }

  test("drop rel property existence constraint") {
    context.dropRelationshipPropertyExistenceConstraint(0, 1)

    context.getStatistics should equal(InternalQueryStatistics(existenceConstraintsRemoved = 1))
  }
}
