/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.spi

import org.junit.{Before, Test}
import org.scalatest.mock.MockitoSugar
import org.neo4j.cypher.QueryStatistics
import org.scalatest.Assertions
import org.neo4j.graphdb.{Relationship, Node}
import org.mockito.Mockito.when
import org.mockito.Matchers
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

class UpdateCountingQueryContextTest extends MockitoSugar with Assertions {
  @Test def create_node() {
    context.createNode()

    assertStats(context.getStatistics,nodesCreated=1)
  }

  @Test def delete_node() {
    context.nodeOps.delete(nodeA)

    assertStats(context.getStatistics,deletedNodes = 1)
  }

  @Test def create_relationship() {
    context.createRelationship(nodeA, nodeB, "FOO")

    assertStats(context.getStatistics,relationshipsCreated = 1)
  }

  @Test def delete_relationship() {
    context.relationshipOps.delete(rel)

    assertStats(context.getStatistics,deletedRelationships = 1)
  }

  @Test def set_property() {
    context.nodeOps.setProperty(nodeA, "key", "value")

    assertStats(context.getStatistics,propertiesSet = 1)
  }

  @Test def remove_property() {
    context.nodeOps.removeProperty(nodeA, "key")

    assertStats(context.getStatistics,propertiesSet = 1)
  }

  @Test def set_property_relationship() {
    context.relationshipOps.setProperty(rel, "key", "value")

    assertStats(context.getStatistics,propertiesSet = 1)
  }

  @Test def remove_property_relationship() {
    context.relationshipOps.removeProperty(rel, "key")

    assertStats(context.getStatistics,propertiesSet = 1)
  }

  @Test def add_label() {
//    when( inner.addLabelsToNode(Matchers.anyLong(), Matchers.any()) ).thenAnswer(answer)
    context.setLabelsOnNode(0, Seq(1,2,3))
    
    assertStats(context.getStatistics,addedLabels = 3)
  }

  @Test def remove_label() {
    context.removeLabelsFromNode(0, Seq(1,2,3))

    assertStats(context.getStatistics,removedLabels = 3)
  }

  val inner = mock[QueryContext]
  val nodeA = mock[Node]
  val nodeB = mock[Node]
  val rel = mock[Relationship]
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
        invocation.getArguments()(1).asInstanceOf[Iterable[String]].size
      }
    } )
    when( inner.removeLabelsFromNode(Matchers.anyLong(), Matchers.any()) ).thenAnswer( new Answer[Int]() {
      def answer(invocation:InvocationOnMock):Int = {
        invocation.getArguments()(1).asInstanceOf[Iterable[String]].size
      }
    } )
    context = new UpdateCountingQueryContext(inner)
  }

  def assertStats(statistics: QueryStatistics,
                  nodesCreated: Int = 0,
                  relationshipsCreated: Int = 0,
                  propertiesSet: Int = 0,
                  deletedNodes: Int = 0,
                  deletedRelationships: Int = 0,
                  addedLabels: Int = 0,
                  removedLabels: Int = 0) {
    assert(statistics === QueryStatistics(nodesCreated, relationshipsCreated, propertiesSet, deletedNodes,
                                deletedRelationships, addedLabels, removedLabels,statistics.timeTaken)
    )
  }
}