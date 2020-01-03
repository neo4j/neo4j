/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.cypher.internal.runtime.{QueryContext, QueryStatistics, _}
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.values.storable.Values
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.internal.schema.{IndexPrototype, SchemaDescriptor}

class UpdateCountingQueryContextTest extends CypherFunSuite {

  val inner = mock[QueryContext]
  val nodeA = mock[Node]
  val nodeB = mock[Node]
  val nodeAId = 666
  val rel = mock[Relationship]
  val relId = 42

  val nodeOps = mock[NodeOperations]
  val relOps = mock[RelationshipOperations]

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

  when(inner.addIndexRule(anyInt(), any(), any()))
    .thenReturn(IndexPrototype.forSchema(SchemaDescriptor.forLabel(1, 2)).withName("index_1").materialise(1))

  var context: UpdateCountingQueryContext = _

  override def beforeEach() {
    super.beforeEach()
    context = new UpdateCountingQueryContext(inner)
  }

  test("create_node") {
    context.createNode(Array(1, 2, 3))

    context.getStatistics should equal(QueryStatistics(nodesCreated = 1, labelsAdded = 3))
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

  test("add_label") {
    context.setLabelsOnNode(0L, Seq(1, 2, 3).iterator)

    context.getStatistics should equal(QueryStatistics(labelsAdded = 3))
  }

  test("remove_label") {
    context.removeLabelsFromNode(0L, Seq(1, 2, 3).iterator)

    context.getStatistics should equal(QueryStatistics(labelsRemoved = 3))
  }

  test("add_index") {
    context.addIndexRule(0, Array(1), None)

    context.getStatistics should equal(QueryStatistics(indexesAdded = 1))
  }

  test("add_index with name") {
    context.addIndexRule(0, Array(1), Some("name"))

    context.getStatistics should equal(QueryStatistics(indexesAdded = 1))
  }

  test("remove_index") {
    context.dropIndexRule(0, Array(1))

    context.getStatistics should equal(QueryStatistics(indexesRemoved = 1))
  }

  test("remove_index with name") {
    context.dropIndexRule("name")

    context.getStatistics should equal(QueryStatistics(indexesRemoved = 1))
  }

  test("create_unique_constraint") {
    context.createUniqueConstraint(0, Array(1), None)

    context.getStatistics should equal(QueryStatistics(uniqueConstraintsAdded = 1))
  }

  test("create_unique_constraint with name") {
    context.createUniqueConstraint(0, Array(1), Some("name"))

    context.getStatistics should equal(QueryStatistics(uniqueConstraintsAdded = 1))
  }

  test("constraint_dropped") {
    context.dropUniqueConstraint(0, Array(1))

    context.getStatistics should equal(QueryStatistics(uniqueConstraintsRemoved = 1))
  }

  test("create node property existence constraint") {
    context.createNodePropertyExistenceConstraint(0, 1, None)

    context.getStatistics should equal(QueryStatistics(existenceConstraintsAdded = 1))
  }

  test("create node property existence constraint with name") {
    context.createNodePropertyExistenceConstraint(0, 1, Some("name"))

    context.getStatistics should equal(QueryStatistics(existenceConstraintsAdded = 1))
  }

  test("drop node property existence constraint") {
    context.dropNodePropertyExistenceConstraint(0, 42)

    context.getStatistics should equal(QueryStatistics(existenceConstraintsRemoved = 1))
  }

  test("create rel property existence constraint") {
    context.createRelationshipPropertyExistenceConstraint(0, 42, None)

    context.getStatistics should equal(QueryStatistics(existenceConstraintsAdded = 1))
  }

  test("create rel property existence constraint with name") {
    context.createRelationshipPropertyExistenceConstraint(0, 42, Some("name"))

    context.getStatistics should equal(QueryStatistics(existenceConstraintsAdded = 1))
  }

  test("drop rel property existence constraint") {
    context.dropRelationshipPropertyExistenceConstraint(0, 1)

    context.getStatistics should equal(QueryStatistics(existenceConstraintsRemoved = 1))
  }

  test("create node key constraint") {
    context.createNodeKeyConstraint(0, Array(1), None)

    context.getStatistics should equal(QueryStatistics(nodekeyConstraintsAdded = 1))
  }

  test("create node key constraint with name") {
    context.createNodeKeyConstraint(0, Array(1), Some("name"))

    context.getStatistics should equal(QueryStatistics(nodekeyConstraintsAdded = 1))
  }

  test("drop node key constraint") {
    context.dropNodeKeyConstraint(0, Array(1))

    context.getStatistics should equal(QueryStatistics(nodekeyConstraintsRemoved = 1))
  }

  test("drop named constraint") {
    context.dropNamedConstraint("name")

    context.getStatistics should equal(QueryStatistics(namedConstraintsRemoved = 1))
  }
}
