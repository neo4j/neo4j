/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.planner.v3_4.spi.{IdempotentResult, IndexDescriptor}
import org.neo4j.cypher.internal.runtime.{Operations, QueryContext, QueryStatistics}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.internal.kernel.api.IndexReference
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}

class UpdateCountingQueryContextTest extends CypherFunSuite {

  val inner = mock[QueryContext]
  val nodeA = mock[Node]
  val nodeB = mock[Node]
  val nodeAId = 666
  val rel = mock[Relationship]
  val relId = 42

  val nodeOps = mock[Operations[NodeValue]]
  val relOps = mock[Operations[RelationshipValue]]

  when(inner.nodeOps).thenReturn(nodeOps)
  when(inner.relationshipOps).thenReturn(relOps)

  // We need to have the inner mock return the right counts for added/removed labels.
  when( inner.setLabelsOnNode(anyLong(), any()) ).thenAnswer( new Answer[Int]() {
    def answer(invocation: InvocationOnMock):Int = {
      invocation.getArgument(1).asInstanceOf[Iterator[String]].size
    }
  } )

  when( inner.removeLabelsFromNode(anyLong(), any()) ).thenAnswer( new Answer[Int]() {
    def answer(invocation: InvocationOnMock):Int = {
      invocation.getArgument(1).asInstanceOf[Iterator[String]].size
    }
  } )

  when(inner.createUniqueConstraint(any())).thenReturn(true)

  when(inner.createNodeKeyConstraint(any())).thenReturn(true)

  when( inner.createNodePropertyExistenceConstraint(anyInt(), anyInt()) ).thenReturn(true)

  when( inner.createRelationshipPropertyExistenceConstraint(anyInt(), anyInt()) ).thenReturn(true)

  when(inner.addIndexRule(any()))
    .thenReturn(IdempotentResult(mock[IndexReference]))

  var context: UpdateCountingQueryContext = null

  override def beforeEach() {
    super.beforeEach()
    context = new UpdateCountingQueryContext(inner)
  }

  test("create_node") {
    context.createNode()

    context.getStatistics should equal(QueryStatistics(nodesCreated = 1))
  }

  test("delete_node") {
    context.nodeOps.delete(nodeA.getId)

    context.getStatistics should equal(QueryStatistics(nodesDeleted = 1))
  }

  test("create_relationship") {
    context.createRelationship(nodeA.getId, nodeB.getId, 13)

    context.getStatistics should equal(QueryStatistics(relationshipsCreated = 1))
  }

  test("delete_relationship") {
    context.relationshipOps.delete(rel.getId)

    context.getStatistics should equal(QueryStatistics(relationshipsDeleted = 1))
  }

  test("set_property") {
    context.nodeOps.setProperty(nodeAId, 1, Values.stringValue("value"))

    context.getStatistics should equal(QueryStatistics(propertiesSet = 1))
  }

  test("remove_property") {
    context.nodeOps.removeProperty(nodeAId, context.getPropertyKeyId("key"))

    context.getStatistics should equal(QueryStatistics(propertiesSet = 1))
  }

  test("set_property_relationship") {
    context.relationshipOps.setProperty(relId, 1, Values.stringValue("value"))

    context.getStatistics should equal(QueryStatistics(propertiesSet = 1))
  }

  test("remove_property_relationship") {
    context.relationshipOps.removeProperty(relId, context.getPropertyKeyId("key"))

    context.getStatistics should equal(QueryStatistics(propertiesSet = 1))
  }
//
//  test("add_label") {
//    context.setLabelsOnNode(0l, Seq(1, 2, 3).iterator)
//
//    context.getStatistics should equal(QueryStatistics(labelsAdded = 3))
//  }

  test("remove_label") {
    context.removeLabelsFromNode(0l, Seq(1, 2, 3).iterator)

    context.getStatistics should equal(QueryStatistics(labelsRemoved = 3))
  }

  test("add_index") {
    context.addIndexRule(IndexDescriptor(0, 1))

    context.getStatistics should equal(QueryStatistics(indexesAdded = 1))
  }

  test("remove_index") {
    context.dropIndexRule(IndexDescriptor(0, 1))

    context.getStatistics should equal(QueryStatistics(indexesRemoved = 1))
  }

  test("create_unique_constraint") {
    context.createUniqueConstraint(IndexDescriptor(0, 1))

    context.getStatistics should equal(QueryStatistics(uniqueConstraintsAdded = 1))
  }

  test("constraint_dropped") {
    context.dropUniqueConstraint(IndexDescriptor(0, 42))

    context.getStatistics should equal(QueryStatistics(uniqueConstraintsRemoved = 1))
  }

  test("create node property existence constraint") {
    context.createNodePropertyExistenceConstraint(0, 1)

    context.getStatistics should equal(QueryStatistics(existenceConstraintsAdded = 1))
  }

  test("drop node property existence constraint") {
    context.dropNodePropertyExistenceConstraint(0, 42)

    context.getStatistics should equal(QueryStatistics(existenceConstraintsRemoved = 1))
  }

  test("create rel property existence constraint") {
    context.createRelationshipPropertyExistenceConstraint(0, 42)

    context.getStatistics should equal(QueryStatistics(existenceConstraintsAdded = 1))
  }

  test("drop rel property existence constraint") {
    context.dropRelationshipPropertyExistenceConstraint(0, 1)

    context.getStatistics should equal(QueryStatistics(existenceConstraintsRemoved = 1))
  }

  test("create node key constraint") {
    context.createNodeKeyConstraint(IndexDescriptor(0, 1))

    context.getStatistics should equal(QueryStatistics(nodekeyConstraintsAdded = 1))
  }

  test("drop node key constraint") {
    context.dropNodeKeyConstraint(IndexDescriptor(0, 42))

    context.getStatistics should equal(QueryStatistics(nodekeyConstraintsRemoved = 1))
  }
}
