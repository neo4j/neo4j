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
package org.neo4j.cypher

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.graphdb.Node


class MergeAcceptanceTest extends ExecutionEngineHelper with Assertions with StatisticsChecker {
  @Test
  def merge_node_when_no_nodes_exist() {
    // Given no reference node
    graph.inTx(graph.getReferenceNode.delete())

    // When
    val result = parseAndExecute("merge (a) return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    assert(createdNodes.size === 1)
    assertStats(result, nodesCreated = 1)
  }

  @Test
  def merge_node_and_find_reference_node() {
    // Given common database with reference node

    // When
    val result = parseAndExecute("merge (a) return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    assert(createdNodes === List(graph.getReferenceNode))
    assertStats(result, nodesCreated = 0)
  }


  @Test
  def merge_node_with_label() {
    // When
    val result = parseAndExecute("merge (a:Label) return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    assert(createdNodes.size === 1)
    assertStats(result, nodesCreated = 1, addedLabels = 1)

    createdNodes.foreach {
      n => assert(n.labels === List("Label"))
    }
  }

  @Test
  def merge_node_with_label_add_label_on_create() {
    // When
    val result = parseAndExecute("merge (a:Label) on create a set a:Foo return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    assert(createdNodes.size === 1)
    assertStats(result, nodesCreated = 1, addedLabels = 2)
    createdNodes.foreach {
      n => assert(n.labels.toSet === Set("Label", "Foo"))
    }

  }

  @Test
  def merge_node_with_label_add_property_on_update() {
    // When
    val result = parseAndExecute("merge (a:Label) on create a set a.prop = 42 return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    assert(createdNodes.size === 1)
    assertStats(result, nodesCreated = 1, addedLabels = 1, propertiesSet = 1)
    createdNodes.foreach {
      n => assert(n.getProperty("prop") === 42)
    }
  }

  @Test
  def merge_node_with_label_when_it_exists() {
    val existingNode = createLabeledNode("Label")

    // When
    val result = parseAndExecute("merge (a:Label) return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    assert(createdNodes === List(existingNode))
    assertStats(result, nodesCreated = 0)
  }

  @Test
  def merge_node_with_label_add_label_on_create_when_it_exists() {
    val existingNode = createLabeledNode("Label")

    // When
    val result = parseAndExecute("merge (a:Label) on create a set a:Foo return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    assert(createdNodes === List(existingNode))
    assertStats(result, nodesCreated = 0)
  }

  @Test
  def merge_node_with_label_add_property_on_update_when_it_exists() {
    val existingNode = createLabeledNode("Label")

    // When
    val result = parseAndExecute("merge (a:Label) on create a set a.prop = 42 return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    assert(createdNodes === List(existingNode))
  }

  @Test
  def can_not_rebind_an_identifier() {
    // Given common database with reference node

    // When
    intercept[CypherTypeException](parseAndExecute("match (a) merge (a) return a"))
  }

  override def parseAndExecute(q: String, params: (String, Any)*): ExecutionResult =
    super.parseAndExecute("CYPHER experimental " + q, params: _*)
}