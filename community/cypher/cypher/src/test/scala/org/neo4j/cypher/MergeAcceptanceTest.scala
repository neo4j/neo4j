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

class MergeAcceptanceTest
  extends ExecutionEngineHelper with Assertions with StatisticsChecker {

  @Test
  def merge_node_when_no_nodes_exist() {
    // When
    val result = execute("merge (a) return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    assert(createdNodes.size === 1)
    assertStats(result, nodesCreated = 1)
  }

  @Test
  def merge_node_with_label() {
    // When
    val result = execute("merge (a:Label) return a")

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
    val result = execute("merge (a:Label) on create a set a:Foo return a")

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
    val result = execute("merge (a:Label) on create a set a.prop = 42 return a")

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
    val result = execute("merge (a:Label) return a")

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
    val result = execute("merge (a:Label) on match a set a:Foo return a")

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
    val result = execute("merge (a:Label) on create a set a.prop = 42 return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    assert(createdNodes === List(existingNode))
  }

  @Test
  def merge_node_and_set_property_on_match() {
    // Given
    val existingNode = createLabeledNode("Label")

    // When
    execute("merge (a:Label) on match a set a.prop = 42 return a")

    // Then
    assertInTx(existingNode.getProperty("prop") === 42)
  }

  @Test
  def merge_node_should_match_properties_given_ad_map() {
    // Given
    val existingNode = createLabeledNode(Map("prop" -> 42), "Label")

    // When
    val result = execute("merge (a:Label {prop:42}) return a")

    // Then
    assertStats(result, nodesCreated = 0)
    assert(result.columnAs[Node]("a").toList === List(existingNode))
  }

  @Test
  def merge_node_should_create_a_node_with_given_properties_when_no_match_is_found() {
    // Given - a node that does not match
    val other = createLabeledNode(Map("prop" -> 666), "Label")

    // When
    val result = execute("merge (a:Label {prop:42}) return a")

    // Then
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesSet = 1)
    val nodes = result.columnAs[Node]("a").toList
    assert(nodes.size === 1)
    val a = nodes.head
    assertInTx(a.getProperty("prop") === 42)
    assertInTx(other.getProperty("prop") === 666)
  }

  @Test
  def merge_using_unique_constraint_should_update_existing_node() {
    // given
    graph.createConstraint("Person", "id")
    val node = createLabeledNode("Person")
    graph.inTx {
      node.setProperty("id", 23)
      node.setProperty("country", "Sweden")
    }

    // when
    val result =
      executeScalar[Node]("merge (a:Person {id: 23, country: 'Sweden'}) on match a set a.name='Emil' return a")

    // then
    assert(1 === countNodes())
    assertInTx(node.getId === result.getId)
    assertInTx("Sweden" === result.getProperty("country"))
    assertInTx("Emil" === result.getProperty("name"))
  }

  @Test
  def merge_using_unique_constraint_should_create_missing_node() {
    // given
    graph.createConstraint("Person", "id")

    // when
    val result =
      executeScalar[Node]("merge (a:Person {id: 23, country: 'Sweden'}) on create a set a.name='Emil' return a")

    // then
    assert(1 === countNodes())
    assertInTx(23 === result.getProperty("id"))
    assertInTx("Sweden" === result.getProperty("country"))
    assertInTx("Emil" === result.getProperty("name"))
  }

  @Test
  def should_match_on_merge_using_multiple_unique_indexes_if_only_found_single_node_for_both_indexes() {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("Person", "mail")

    createLabeledNode(Map("id" -> 23, "mail" -> "emil@neo.com"), "Person")

    // when
    val result =
      executeScalar[Node]("merge (a:Person {id: 23, mail: 'emil@neo.com'}) on match a set a.country='Sweden' return a")

    // then
    assert(1 === countNodes())
    assertInTx(23 === result.getProperty("id"))
    assertInTx("emil@neo.com" === result.getProperty("mail"))
    assertInTx("Sweden" === result.getProperty("country"))
  }

  @Test
  def should_match_on_merge_using_multiple_unique_indexes_and_labels_if_only_found_single_node_for_both_indexes() {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("User", "mail")

    createLabeledNode(Map("id" -> 23, "mail" -> "emil@neo.com"), "Person", "User")

    // when
    val result =
      executeScalar[Node]("merge (a:Person:User {id: 23, mail: 'emil@neo.com'}) on match a set a.country='Sweden' return a")

    // then
    assert(1 === countNodes())
    assertInTx(23 === result.getProperty("id"))
    assertInTx("emil@neo.com" === result.getProperty("mail"))
    assertInTx("Sweden" === result.getProperty("country"))
  }

  @Test
  def should_match_on_merge_using_multiple_unique_indexes_using_same_key_if_only_found_single_node_for_both_indexes() {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("User", "id")

    createLabeledNode(Map("id" -> 23), "Person", "User")

    // when
    val result =
      executeScalar[Node]("merge (a:Person:User {id: 23}) on match a set a.country='Sweden' return a")

    // then
    assert(1 === countNodes())
    assertInTx(23 === result.getProperty("id"))
    assertInTx("Sweden" === result.getProperty("country"))
  }

  @Test
  def should_fail_on_merge_using_multiple_unique_indexes_using_same_key_if_found_different_nodes() {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("User", "id")

    createLabeledNode(Map("id" -> 23), "Person")
    createLabeledNode(Map("id" -> 23), "User")

    // when + then
    intercept[MergeConstraintConflictException](executeScalar[Node]("merge (a:Person:User {id: 23}) return a"))
    assert(2 === countNodes())
  }

  @Test
  def should_create_on_merge_using_multiple_unique_indexes_if_found_no_nodes() {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("Person", "mail")

    // when
    val result =
      executeScalar[Node]("merge (a:Person {id: 23, mail: 'emil@neo.com'}) on create a set a.country='Sweden' return a")

    // then
    assert(1 === countNodes())
    assertInTx(23 === result.getProperty("id"))
    assertInTx("emil@neo.com" === result.getProperty("mail"))
    assertInTx("Sweden" === result.getProperty("country"))
    assert(Set("Person") === labels(result))
  }

  @Test
  def should_create_on_merge_using_multiple_unique_indexes_and_labels_if_found_no_nodes() {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("User", "mail")

    // when
    val result =
      executeScalar[Node]("merge (a:Person:User {id: 23, mail: 'emil@neo.com'}) on create a set a.country='Sweden' return a")

    // then
    assert(1 === countNodes())
    assertInTx(23 === result.getProperty("id"))
    assertInTx("emil@neo.com" === result.getProperty("mail"))
    assertInTx("Sweden" === result.getProperty("country"))
    assert(Set("Person", "User") === labels(result))
  }

  @Test
  def should_fail_on_merge_using_multiple_unique_indexes_if_found_different_nodes() {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("Person", "mail")

    createLabeledNode(Map("id" -> 23), "Person")
    createLabeledNode(Map("mail" -> "emil@neo.com"), "Person")

    runAndFail[MergeConstraintConflictException](
      "merge (a:Person {id: 23, mail: 'emil@neo.com'}) return a") messageContains
      "Merge did not find a matching node and can not create a new node due to conflicts with existing unique nodes. The conflicting constraints are on: :Person.id and :Person.mail"

    assert(2 === countNodes())
  }

  @Test
  def should_fail_on_merge_using_multiple_unique_indexes_if_it_found_a_node_matching_single_property_only() {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("Person", "mail")

    createLabeledNode(Map("id" -> 23), "Person")

    // when + then
    runAndFail[MergeConstraintConflictException](
      "merge (a:Person {id: 23, mail: 'emil@neo.com'}) return a") messageContains
      "Merge did not find a matching node and can not create a new node due to conflicts with both existing and missing unique nodes. The conflicting constraints are on: :Person.id and :Person.mail"
    assert(1 === countNodes())
  }

  @Test
  def should_fail_on_merge_using_multiple_unique_indexes_and_labels_if_found_different_nodes() {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("User", "mail")

    createLabeledNode(Map("id" -> 23), "Person")
    createLabeledNode(Map("mail" -> "emil@neo.com"), "User")

    // when
    runAndFail[MergeConstraintConflictException](
      "merge (a:Person:User {id: 23, mail: 'emil@neo.com'}) return a") messageContains
      "Merge did not find a matching node and can not create a new node due to conflicts with existing unique nodes. The conflicting constraints are on: :Person.id and :User.mail"

    // then
    assert(2 === countNodes())
  }

  @Test
  def should_handle_running_merge_inside_a_foreach_loop() {
    // given an empty database

    // when
    val result = execute("foreach(x in [1,2,3] | merge ({property: x}))")

    // then
    assertStats(result, nodesCreated = 3, propertiesSet = 3)
  }

  @Test
  def unrelated_nodes_with_same_property_should_not_clash() {
    // given
    graph.createConstraint("Person", "id")
    execute("MERGE (a:Item {id:1}) MERGE (b:Person {id:1})")

    // when
    val result = execute("MERGE (a:Item {id:2}) MERGE (b:Person {id:1})")

    // then does not throw
  }
}
