/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.spi

import org.neo4j.cypher.QueryStatistics
import org.neo4j.graphdb.{Relationship, Node}
import org.neo4j.kernel.api.constraints.UniquenessConstraint
import org.junit.{Before, Test}
import org.scalatest.mock.MockitoSugar
import org.scalatest.Assertions
import org.mockito.Mockito.when
import org.mockito.Matchers
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import org.neo4j.kernel.api.index.IndexDescriptor

class UpdateCountingQueryContextTest extends MockitoSugar with Assertions {
  @Test def create_node() {
    context.createNode()

    assert(context.getStatistics === QueryStatistics(nodesCreated = 1))
  }

  @Test def delete_node() {
    context.nodeOps.delete(nodeA)

    assert(context.getStatistics === QueryStatistics(nodesDeleted = 1))
  }

  @Test def create_relationship() {
    context.createRelationship(nodeA, nodeB, "FOO")

    assert(context.getStatistics === QueryStatistics(relationshipsCreated = 1))
  }

  @Test def delete_relationship() {
    context.relationshipOps.delete(rel)

    assert(context.getStatistics === QueryStatistics(relationshipsDeleted = 1))
  }

  @Test def set_property() {
    context.nodeOps.setProperty(nodeAId, 1, "value")

    assert(context.getStatistics === QueryStatistics(propertiesSet = 1))
  }

  @Test def remove_property() {
    context.nodeOps.removeProperty(nodeAId, context.getPropertyKeyId("key"))

    assert(context.getStatistics === QueryStatistics(propertiesSet = 1))
  }

  @Test def set_property_relationship() {
    context.relationshipOps.setProperty(relId, 1, "value")

    assert(context.getStatistics === QueryStatistics(propertiesSet = 1))
  }

  @Test def remove_property_relationship() {
    context.relationshipOps.removeProperty(relId, context.getPropertyKeyId("key"))

    assert(context.getStatistics === QueryStatistics(propertiesSet = 1))
  }

  @Test def add_label() {
    context.setLabelsOnNode(0l, Seq(1, 2, 3).iterator)

    assert(context.getStatistics === QueryStatistics(labelsAdded = 3))
  }

  @Test def remove_label() {
    context.removeLabelsFromNode(0l, Seq(1, 2, 3).iterator)

    assert(context.getStatistics === QueryStatistics(labelsRemoved = 3))
  }

  @Test def add_index() {
    context.addIndexRule(0, 1)

    assert(context.getStatistics === QueryStatistics(indexesAdded = 1))
  }

  @Test def remove_index() {
    context.dropIndexRule(0, 1)

    assert(context.getStatistics === QueryStatistics(indexesRemoved = 1))
  }

  @Test def create_unique_constraint() {
    context.createUniqueConstraint(0, 1)

    assert(context.getStatistics === QueryStatistics(constraintsAdded = 1))
  }

  @Test def constraint_dropped() {
    context.dropUniqueConstraint(0, 42)

    assert(context.getStatistics === QueryStatistics(constraintsRemoved = 1))
  }


  val inner = mock[QueryContext]
  val nodeA = mock[Node]
  val nodeB = mock[Node]
  val nodeAId = 666
  val rel = mock[Relationship]
  val relId = 42
  var context: UpdateCountingQueryContext = null
  val nodeOps = mock[Operations[Node]]
  val relOps = mock[Operations[Relationship]]

  when(inner.nodeOps).thenReturn(nodeOps)
  when(inner.relationshipOps).thenReturn(relOps)

  @Before
  def init() {
    // We need to have the inner mock return the right counts for added/removed labels.
    when( inner.setLabelsOnNode(Matchers.anyLong(), Matchers.any()) ).thenAnswer( new Answer[Int]() {
      def answer(invocation:InvocationOnMock):Int = {
        invocation.getArguments()(1).asInstanceOf[Iterator[String]].size
      }
    } )
    when( inner.removeLabelsFromNode(Matchers.anyLong(), Matchers.any()) ).thenAnswer( new Answer[Int]() {
      def answer(invocation:InvocationOnMock):Int = {
        invocation.getArguments()(1).asInstanceOf[Iterator[String]].size
      }
    } )
    when( inner.createUniqueConstraint(Matchers.anyInt(), Matchers.anyInt()) )
      .thenReturn(IdempotentResult(mock[UniquenessConstraint]))
    when( inner.addIndexRule(Matchers.anyInt(), Matchers.anyInt()) )
      .thenReturn(IdempotentResult(mock[IndexDescriptor]))
    context = new UpdateCountingQueryContext(inner)
  }
}
