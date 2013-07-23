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

    assertInTx(createdNodes === List(graph.getReferenceNode))
    assertStats(result, nodesCreated = 0)
  }


  @Test
  def merge_node_with_label() {
    // When
    val result = parseAndExecute("merge (a:Label) return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    assert(createdNodes.size === 1)
    assertStats(result, nodesCreated = 1, labelsAdded = 1)

    graph.inTx {
      createdNodes.foreach {
        n => assert(n.labels === List("Label"))
      }
    }
  }

  @Test
  def merge_node_with_label_add_label_on_create() {
    // When
    val result = parseAndExecute("merge (a:Label) on create a set a:Foo return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    assert(createdNodes.size === 1)
    assertStats(result, nodesCreated = 1, labelsAdded = 2)

    graph.inTx {
      createdNodes.foreach {
        n => assert(n.labels.toSet === Set("Label", "Foo"))
      }
    }
  }

  @Test
  def merge_node_with_label_add_property_on_update() {
    // When
    val result = parseAndExecute("merge (a:Label) on create a set a.prop = 42 return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    assert(createdNodes.size === 1)
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesSet = 1)

    graph.inTx {
      createdNodes.foreach {
        n => assert(n.getProperty("prop") === 42)
      }
    }
  }

  @Test
  def merge_node_with_label_when_it_exists() {
    // Given
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
    // Given
    val existingNode = createLabeledNode("Label")

    // When
    val result = parseAndExecute("merge (a:Label) on match a set a:Foo return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    assert(createdNodes === List(existingNode))
    assertStats(result, nodesCreated = 0, labelsAdded = 1)
  }

  @Test
  def merge_node_with_label_add_property_on_update_when_it_exists() {
    // Given
    val existingNode = createLabeledNode("Label")

    // When
    val result = parseAndExecute("merge (a:Label) on create a set a.prop = 42 return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    assert(createdNodes === List(existingNode))
  }

  @Test
  def merge_node_and_set_property_on_match() {
    // Given
    val existingNode = createLabeledNode("Label")

    // When
    parseAndExecute("merge (a:Label) on match a set a.prop = 42 return a")

    // Then
    assertInTx(existingNode.getProperty("prop") === 42)
  }

  @Test
  def merge_node_should_match_properties_given_ad_map() {
    // Given
    val existingNode = createLabeledNode(Map("prop" -> 42), "Label")

    // When
    val result = parseAndExecute("merge (a:Label {prop:42}) return a")

    // Then
    assertStats(result, nodesCreated = 0)
    assert(result.columnAs[Node]("a").toList === List(existingNode))
  }

  @Test
  def merge_node_should_create_a_node_with_given_properties_when_no_match_is_found() {
    // Given - a node that does not match
    val other = createLabeledNode(Map("prop" -> 666), "Label")

    // When
    val result = parseAndExecute("merge (a:Label {prop:42}) return a")

    // Then
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesSet = 1)
    val nodes = result.columnAs[Node]("a").toList
    assert(nodes.size === 1)
    val a = nodes.head
    assertInTx(a.getProperty("prop") === 42)
    assertInTx(other.getProperty("prop") === 666)
  }

  @Test
  def can_not_rebind_an_identifier() {
    // Given common database with reference node

    // When
    intercept[CypherTypeException](parseAndExecute("match (a) merge (a) return a"))
  }
}