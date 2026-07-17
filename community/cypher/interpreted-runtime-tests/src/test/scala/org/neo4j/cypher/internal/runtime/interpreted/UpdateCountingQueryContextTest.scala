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
package org.neo4j.cypher.internal.runtime.interpreted

import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.runtime.NodeOperations
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.runtime.RelationshipOperations
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.internal.schema.EndpointType
import org.neo4j.internal.schema.IndexPrototype
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create
import org.neo4j.internal.schema.SchemaDescriptors
import org.neo4j.internal.schema.constraints.PropertyTypeSet
import org.neo4j.internal.schema.constraints.SchemaValueType
import org.neo4j.values.storable.Values

import java.util

class UpdateCountingQueryContextTest extends InterpretedRuntimeTestSuite {

  private val inner = mock[QueryContext]
  private val nodeA = mock[Node]
  private val nodeB = mock[Node]
  private val nodeAId = 666
  private val rel = mock[Relationship]
  private val relId = 42

  private val nodeWriteOps = mock[NodeOperations]
  private val relWriteOps = mock[RelationshipOperations]

  when(inner.nodeWriteOps).thenReturn(nodeWriteOps)
  when(inner.relationshipWriteOps).thenReturn(relWriteOps)

  // We need to have the inner mock return the right counts for added/removed labels.
  when(inner.setLabelsOnNode(anyLong(), any())).thenAnswer(new Answer[Int]() {

    def answer(invocation: InvocationOnMock): Int = {
      invocation.getArgument(1).asInstanceOf[Iterator[String]].size
    }
  })

  when(inner.removeLabelsFromNode(anyLong(), any())).thenAnswer(new Answer[Int]() {

    def answer(invocation: InvocationOnMock): Int = {
      invocation.getArgument(1).asInstanceOf[Iterator[String]].size
    }
  })

  when(inner.addRangeIndexRule(anyInt(), ArgumentMatchers.eq(EntityType.NODE), any(), any(), any()))
    .thenReturn(IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 2)).withName("index_1").materialise(1))

  when(inner.addRangeIndexRule(anyInt(), ArgumentMatchers.eq(EntityType.RELATIONSHIP), any(), any(), any()))
    .thenReturn(IndexPrototype.forSchema(SchemaDescriptors.forRelType(1, 2)).withName("index_1").materialise(1))

  var context: UpdateCountingQueryContext = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    context = new UpdateCountingQueryContext(inner)
  }

  test("create_node") {
    context.createNodeId(Array(1, 2, 3))

    context.getStatistics should equal(QueryStatistics(nodesCreated = 1, labelsAdded = 3))
  }

  test("delete_node") {
    when(nodeWriteOps.delete(any())).thenReturn(true)
    context.nodeWriteOps.delete(nodeA.getId)

    context.getStatistics should equal(QueryStatistics(nodesDeleted = 1))
  }

  test("delete_node without delete") {
    when(nodeWriteOps.delete(any())).thenReturn(false)
    context.nodeWriteOps.delete(nodeA.getId)

    context.getStatistics should equal(QueryStatistics())
  }

  test("create_relationship") {
    context.createRelationshipId(nodeA.getId, nodeB.getId, 13)

    context.getStatistics should equal(QueryStatistics(relationshipsCreated = 1))
  }

  test("delete_relationship") {
    when(relWriteOps.delete(any())).thenReturn(true)
    context.relationshipWriteOps.delete(rel.getId)

    context.getStatistics should equal(QueryStatistics(relationshipsDeleted = 1))
  }

  test("delete_relationship without delete") {
    when(relWriteOps.delete(any())).thenReturn(false)
    context.relationshipWriteOps.delete(rel.getId)

    context.getStatistics should equal(QueryStatistics())
  }

  test("set_property") {
    context.nodeWriteOps.setProperty(nodeAId, 1, Values.stringValue("value"))

    context.getStatistics should equal(QueryStatistics(propertiesSet = 1))
  }

  test("remove_property") {
    // given
    val propertyKey = context.getPropertyKeyId("key")
    when(nodeWriteOps.removeProperty(nodeAId, propertyKey)).thenReturn(true)

    // when
    context.nodeWriteOps.removeProperty(nodeAId, propertyKey)

    // then
    context.getStatistics should equal(QueryStatistics(propertiesSet = 1))
  }

  test("remove_property does nothing") {
    // given
    val propertyKey = context.getPropertyKeyId("key")
    when(nodeWriteOps.removeProperty(nodeAId, propertyKey)).thenReturn(false)

    // when
    context.nodeWriteOps.removeProperty(nodeAId, propertyKey)

    // then
    context.getStatistics should equal(QueryStatistics())
  }

  test("set_property_relationship") {
    context.relationshipWriteOps.setProperty(relId, 1, Values.stringValue("value"))

    context.getStatistics should equal(QueryStatistics(propertiesSet = 1))
  }

  test("remove_property_relationship") {
    // given
    val propertyKey = context.getPropertyKeyId("key")
    when(relWriteOps.removeProperty(relId, propertyKey)).thenReturn(true)

    // when
    context.relationshipWriteOps.removeProperty(relId, propertyKey)

    // then
    context.getStatistics should equal(QueryStatistics(propertiesSet = 1))
  }

  test("remove_property_relationship does nothing") {
    // given
    val propertyKey = context.getPropertyKeyId("key")
    when(relWriteOps.removeProperty(relId, propertyKey)).thenReturn(false)

    // when
    context.relationshipWriteOps.removeProperty(relId, propertyKey)

    // then
    context.getStatistics should equal(QueryStatistics())
  }

  test("add_label") {
    context.setLabelsOnNode(0L, Seq(1, 2, 3).iterator)

    context.getStatistics should equal(QueryStatistics(labelsAdded = 3))
  }

  test("remove_label") {
    context.removeLabelsFromNode(0L, Seq(1, 2, 3).iterator)

    context.getStatistics should equal(QueryStatistics(labelsRemoved = 3))
  }

  test("add_index for node") {
    context.addRangeIndexRule(0, EntityType.NODE, Array(1), None, None)

    context.getStatistics should equal(QueryStatistics(indexesAdded = 1))
  }

  test("add_index for node with name") {
    context.addRangeIndexRule(0, EntityType.NODE, Array(1), Some("name"), None)

    context.getStatistics should equal(QueryStatistics(indexesAdded = 1))
  }

  test("add_index for relationship") {
    context.addRangeIndexRule(0, EntityType.RELATIONSHIP, Array(1), None, None)

    context.getStatistics should equal(QueryStatistics(indexesAdded = 1))
  }

  test("add_index for relationship with name") {
    context.addRangeIndexRule(0, EntityType.RELATIONSHIP, Array(1), Some("name"), None)

    context.getStatistics should equal(QueryStatistics(indexesAdded = 1))
  }

  test("remove_index with name") {
    context.dropIndexRule("name")

    context.getStatistics should equal(QueryStatistics(indexesRemoved = 1))
  }

  test("create_unique_constraint") {
    context.createConstraint(new Create.NodeUniqueness(null, "label", util.List.of("prop"), false))

    context.getStatistics should equal(QueryStatistics(nodePropUniquenessConstraintsAdded = 1))
  }

  test("create_unique_constraint with name") {
    context.createConstraint(new Create.NodeUniqueness("name", "label", util.List.of("prop"), false))

    context.getStatistics should equal(QueryStatistics(nodePropUniquenessConstraintsAdded = 1))
  }

  test("create node property existence constraint") {
    context.createConstraint(new Create.NodeExistence(null, "label", "prop", false, false))

    context.getStatistics should equal(QueryStatistics(nodePropExistenceConstraintsAdded = 1))
  }

  test("create node property existence constraint with name") {
    context.createConstraint(new Create.NodeExistence("name", "label", "prop", false, false))

    context.getStatistics should equal(QueryStatistics(nodePropExistenceConstraintsAdded = 1))
  }

  test("create node key constraint") {
    context.createConstraint(new Create.NodeKey(null, "label", util.List.of("prop"), false))

    context.getStatistics should equal(QueryStatistics(nodekeyConstraintsAdded = 1))
  }

  test("create node key constraint with name") {
    context.createConstraint(new Create.NodeKey("name", "label", util.List.of("prop"), false))

    context.getStatistics should equal(QueryStatistics(nodekeyConstraintsAdded = 1))
  }

  test("create node property type constraint") {
    context.createConstraint(new Create.NodePropertyType(
      null,
      "label",
      "prop",
      PropertyTypeSet.of(SchemaValueType.INTEGER),
      false,
      false
    ))

    context.getStatistics should equal(QueryStatistics(nodePropTypeConstraintsAdded = 1))
  }

  test("create node property type constraint with name") {
    context.createConstraint(new Create.NodePropertyType(
      "name",
      "label",
      "prop",
      PropertyTypeSet.of(SchemaValueType.INTEGER),
      false,
      false
    ))

    context.getStatistics should equal(QueryStatistics(nodePropTypeConstraintsAdded = 1))
  }

  test("create rel property existence constraint") {
    context.createConstraint(new Create.RelationshipExistence(null, "rel", "prop", false, false))

    context.getStatistics should equal(QueryStatistics(relPropExistenceConstraintsAdded = 1))
  }

  test("create rel property existence constraint with name") {
    context.createConstraint(new Create.RelationshipExistence("name", "rel", "prop", false, false))

    context.getStatistics should equal(QueryStatistics(relPropExistenceConstraintsAdded = 1))
  }

  test("create rel property uniqueness constraint") {
    context.createConstraint(new Create.RelationshipUniqueness(null, "rel", util.List.of("prop"), false))

    context.getStatistics should equal(QueryStatistics(relPropUniquenessConstraintsAdded = 1))
  }

  test("create rel property uniqueness constraint with name") {
    context.createConstraint(new Create.RelationshipUniqueness("name", "rel", util.List.of("prop"), false))

    context.getStatistics should equal(QueryStatistics(relPropUniquenessConstraintsAdded = 1))
  }

  test("create rel key constraint") {
    context.createConstraint(new Create.RelationshipKey(null, "rel", util.List.of("prop"), false))

    context.getStatistics should equal(QueryStatistics(relkeyConstraintsAdded = 1))
  }

  test("create rel key constraint with name") {
    context.createConstraint(new Create.RelationshipKey("name", "rel", util.List.of("prop"), false))

    context.getStatistics should equal(QueryStatistics(relkeyConstraintsAdded = 1))
  }

  test("create rel property type constraint") {
    context.createConstraint(new Create.RelationshipPropertyType(
      null,
      "rel",
      "prop",
      PropertyTypeSet.of(SchemaValueType.INTEGER),
      false,
      false
    ))

    context.getStatistics should equal(QueryStatistics(relPropTypeConstraintsAdded = 1))
  }

  test("create rel property type constraint with name") {
    context.createConstraint(new Create.RelationshipPropertyType(
      "name",
      "rel",
      "prop",
      PropertyTypeSet.of(SchemaValueType.INTEGER),
      false,
      false
    ))

    context.getStatistics should equal(QueryStatistics(relPropTypeConstraintsAdded = 1))
  }

  test("create node label existence constraint") {
    context.createConstraint(new Create.NodeLabelExistence(null, "label", "label2", false))

    context.getStatistics should equal(QueryStatistics(nodeLabelExistenceConstraintsAdded = 1))
  }

  test("create rel source label constraint") {
    context.createConstraint(new Create.RelationshipEndpointLabel(null, "label", "label2", EndpointType.START, false))

    context.getStatistics should equal(QueryStatistics(relSourceLabelConstraintsAdded = 1))
  }

  test("create rel target label constraint") {
    context.createConstraint(new Create.RelationshipEndpointLabel(null, "label", "label2", EndpointType.END, false))

    context.getStatistics should equal(QueryStatistics(relTargetLabelConstraintsAdded = 1))
  }

  test("drop named constraint") {
    context.dropNamedConstraint("name", allowDependent = false)

    context.getStatistics should equal(QueryStatistics(constraintsRemoved = 1))
  }

  test("drop named dependent constraint") {
    context.dropNamedConstraint("name", allowDependent = true)

    context.getStatistics should equal(QueryStatistics(constraintsRemoved = 1))
  }
}
