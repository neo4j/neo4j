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
package org.neo4j.cypher

import org.neo4j.graphdb.Node
import org.neo4j.kernel.api.exceptions.schema.{UniquePropertyConstraintViolationKernelException}

class MergeNodeAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport {

  test("merge_node_when_no_nodes_exist") {
    // When
    val result = execute("merge (a) return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    createdNodes should have size 1
    assertStats(result, nodesCreated = 1)
  }

  test("merge_node_with_label") {
    // When
    val result = execute("merge (a:Label) return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    createdNodes should have size 1
    assertStats(result, nodesCreated = 1, labelsAdded = 1)

    graph.inTx {
      createdNodes.foreach {
        n => n.labels should equal(List("Label"))
      }
    }
  }

  test("merge_node_with_label_add_label_on_create") {
    // When
    val result = execute("merge (a:Label) on create set a:Foo return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    createdNodes should have size 1
    assertStats(result, nodesCreated = 1, labelsAdded = 2)

    graph.inTx {
      createdNodes.foreach {
        n => n.labels.toSet should equal(Set("Label", "Foo"))
      }
    }
  }

  test("merge_node_with_label_add_property_on_update") {
    // When
    val result = execute("merge (a:Label) on create set a.prop = 42 return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    createdNodes should have size 1
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesSet = 1)

    graph.inTx {
      createdNodes.foreach {
        n => n.getProperty("prop") should equal(42)
      }
    }
  }

  test("merge_node_with_label_when_it_exists") {
    // Given
    val existingNode = createLabeledNode("Label")

    // When
    val result = execute("merge (a:Label) return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    createdNodes should equal(List(existingNode))
    assertStats(result, nodesCreated = 0)
  }

  test("merge_node_with_label_add_label_on_create_when_it_exists") {
    // Given
    val existingNode = createLabeledNode("Label")

    // When
    val result = execute("merge (a:Label) on match set a:Foo return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    createdNodes should equal(List(existingNode))
    assertStats(result, nodesCreated = 0, labelsAdded = 1)
  }

  test("merge_node_with_label_add_property_on_update_when_it_exists") {
    // Given
    val existingNode = createLabeledNode("Label")

    // When
    val result = execute("merge (a:Label) on create set a.prop = 42 return a")

    // Then
    val createdNodes = result.columnAs[Node]("a").toList

    createdNodes should equal(List(existingNode))
  }

  test("merge_node_and_set_property_on_match") {
    // Given
    val existingNode = createLabeledNode("Label")

    // When
    execute("merge (a:Label) on match set a.prop = 42 return a")

    // Then
    graph.inTx{
      existingNode.getProperty("prop") should equal(42)
    }
  }

  test("merge_node_should_match_properties_given_ad_map") {
    // Given
    val existingNode = createLabeledNode(Map("prop" -> 42), "Label")

    // When
    val result = execute("merge (a:Label {prop:42}) return a")

    // Then
    assertStats(result, nodesCreated = 0)
    result.columnAs[Node]("a").toList should equal(List(existingNode))
  }

  test("merge_node_should_create_a_node_with_given_properties_when_no_match_is_found") {
    // Given - a node that does not match
    val other = createLabeledNode(Map("prop" -> 666), "Label")

    // When
    val result = execute("merge (a:Label {prop:42}) return a")

    // Then
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesSet = 1)
    val nodes = result.columnAs[Node]("a").toList
    nodes should have size 1
    val a = nodes.head
    graph.inTx {
      a.getProperty("prop") should equal(42)
      other.getProperty("prop") should equal(666)
    }
  }

  test("merge_using_unique_constraint_should_update_existing_node") {
    // given
    graph.createConstraint("Person", "id")
    val node = createLabeledNode("Person")
    graph.inTx {
      node.setProperty("id", 23)
      node.setProperty("country", "Sweden")
    }

    // when
    val result =
      executeScalar[Node]("merge (a:Person {id: 23, country: 'Sweden'}) on match set a.name='Emil' return a")

    // then
    countNodes() should equal(1)
    graph.inTx {
      result.getId should equal(node.getId)
      result.getProperty("country") should equal("Sweden")
      result.getProperty("name") should equal("Emil")
    }
  }

  test("merge_using_unique_constraint_should_create_missing_node") {
    // given
    graph.createConstraint("Person", "id")

    // when
    val result =
      executeScalar[Node]("merge (a:Person {id: 23, country: 'Sweden'}) on create set a.name='Emil' return a")

    // then
    countNodes() should equal(1)
    graph.inTx {
      result.getProperty("id") should equal(23)
      result.getProperty("country") should equal("Sweden")
      result.getProperty("name") should equal("Emil")
    }
  }

  test("should_match_on_merge_using_multiple_unique_indexes_if_only_found_single_node_for_both_indexes") {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("Person", "mail")

    createLabeledNode(Map("id" -> 23, "mail" -> "emil@neo.com"), "Person")

    // when
    val result =
      executeScalar[Node]("merge (a:Person {id: 23, mail: 'emil@neo.com'}) on match set a.country='Sweden' return a")

    // then
    countNodes() should equal(1)
    graph.inTx {
      result.getProperty("id") should equal(23)
      result.getProperty("mail") should equal("emil@neo.com")
      result.getProperty("country") should equal("Sweden")
    }
  }

  test("should_match_on_merge_using_multiple_unique_indexes_and_labels_if_only_found_single_node_for_both_indexes") {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("User", "mail")

    createLabeledNode(Map("id" -> 23, "mail" -> "emil@neo.com"), "Person", "User")

    // when
    val result =
      executeScalar[Node]("merge (a:Person:User {id: 23, mail: 'emil@neo.com'}) on match set a.country='Sweden' return a")

    // then
    countNodes() should equal(1)
    graph.inTx {
      result.getProperty("id") should equal(23)
      result.getProperty("mail") should equal("emil@neo.com")
      result.getProperty("country") should equal("Sweden")
    }
  }

  test("should_match_on_merge_using_multiple_unique_indexes_using_same_key_if_only_found_single_node_for_both_indexes") {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("User", "id")

    createLabeledNode(Map("id" -> 23), "Person", "User")

    // when
    val result =
      executeScalar[Node]("merge (a:Person:User {id: 23}) on match set a.country='Sweden' return a")

    // then
    countNodes() should equal(1)
    graph.inTx {
      result.getProperty("id") should equal(23)
      result.getProperty("country") should equal("Sweden")
    }
  }

  test("should_fail_on_merge_using_multiple_unique_indexes_using_same_key_if_found_different_nodes") {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("User", "id")

    createLabeledNode(Map("id" -> 23), "Person")
    createLabeledNode(Map("id" -> 23), "User")

    // when + then
    intercept[MergeConstraintConflictException](executeScalar[Node]("merge (a:Person:User {id: 23}) return a"))
    countNodes() should equal(2)
  }

  test("should_create_on_merge_using_multiple_unique_indexes_if_found_no_nodes") {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("Person", "mail")

    // when
    val result =
      executeScalar[Node]("merge (a:Person {id: 23, mail: 'emil@neo.com'}) on create set a.country='Sweden' return a")

    // then
    countNodes() should equal(1)
    labels(result) should equal(Set("Person"))
    graph.inTx {
      result.getProperty("id") should equal(23)
      result.getProperty("country") should equal("Sweden")
      result.getProperty("mail") should equal("emil@neo.com")
    }
  }

  test("should_create_on_merge_using_multiple_unique_indexes_and_labels_if_found_no_nodes") {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("User", "mail")

    // when
    val result =
      executeScalar[Node]("merge (a:Person:User {id: 23, mail: 'emil@neo.com'}) on create set a.country='Sweden' return a")

    // then
    countNodes() should equal(1)
    labels(result) should equal(Set("Person", "User"))
    graph.inTx {
      result.getProperty("id") should equal(23)
      result.getProperty("country") should equal("Sweden")
      result.getProperty("mail") should equal("emil@neo.com")
    }
  }

  test("should_fail_on_merge_using_multiple_unique_indexes_if_found_different_nodes") {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("Person", "mail")

    createLabeledNode(Map("id" -> 23), "Person")
    createLabeledNode(Map("mail" -> "emil@neo.com"), "Person")

    runAndFail[MergeConstraintConflictException](
      "merge (a:Person {id: 23, mail: 'emil@neo.com'}) return a") messageContains
      "Merge did not find a matching node and can not create a new node due to conflicts with existing unique nodes. The conflicting constraints are on: :Person.id and :Person.mail"

    countNodes() should equal(2)
  }

  test("should_fail_on_merge_using_multiple_unique_indexes_if_it_found_a_node_matching_single_property_only") {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("Person", "mail")

    createLabeledNode(Map("id" -> 23), "Person")

    // when + then
    runAndFail[MergeConstraintConflictException](
      "merge (a:Person {id: 23, mail: 'emil@neo.com'}) return a") messageContains
      "Merge did not find a matching node and can not create a new node due to conflicts with both existing and missing unique nodes. The conflicting constraints are on: :Person.id and :Person.mail"

    countNodes() should equal(1)
  }

  test("should_fail_on_merge_using_multiple_unique_indexes_and_labels_if_found_different_nodes") {
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
    countNodes() should equal(2)
  }

  test("should_handle_running_merge_inside_a_foreach_loop") {
    // given an empty database

    // when
    val result = execute("foreach(x in [1,2,3] | merge ({property: x}))")

    // then
    assertStats(result, nodesCreated = 3, propertiesSet = 3)
  }

  test("unrelated_nodes_with_same_property_should_not_clash") {
    // given
    graph.createConstraint("Person", "id")
    execute("MERGE (a:Item {id:1}) MERGE (b:Person {id:1})")

    // when
    val result = execute("MERGE (a:Item {id:2}) MERGE (b:Person {id:1})")

    // then does not throw
  }

  test("works_fine_with_index") {
    // given
    execute("create index on :Person(name)")

    // when
    val result = execute("MERGE (person:Person {name:'Lasse'}) RETURN person")

    // then does not throw
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesSet = 1)
  }

  test("works_with_index_and_constraint") {
    // given
    execute("create index on :Person(name)")
    graph.createConstraint("Person", "id")

    // when
    val result = execute("MERGE (person:Person {name:'Lasse', id:42}) RETURN person")

    // then does not throw
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesSet = 2)
  }

  test("works_with_indexed_and_unindexed_property") {
    // given
    execute("create index on :Person(name)")

    // when
    val result = execute("MERGE (person:Person {name:'Lasse', id:42}) RETURN person")

    // then does not throw
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesSet = 2)
  }

  test("works_with_two_indexed_properties") {
    // given
    execute("create index on :Person(name)")
    execute("create index on :Person(id)")

    // when
    val result = execute("MERGE (person:Person {name:'Lasse', id:42}) RETURN person")

    // then does not throw
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesSet = 2)
  }

  test("works_with_property_repeated_in_literal_map_in_set") {
    // given
    graph.createConstraint("Person","ssn")

    // when
    val result = execute("MERGE (person:Person {ssn:42}) ON CREATE SET person = {ssn:42,name:'Robert Paulsen'} RETURN person")

    // then - does not throw
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesSet = 3/*should really be 2!*/)
  }

  test("works_with_property_in_map_that_gets_set") {
    // given
    graph.createConstraint("Person","ssn")

    // when
    val result = execute("MERGE (person:Person {ssn:{p}.ssn}) ON CREATE SET person = {p} RETURN person",
      "p"->Map("ssn" -> 42, "name"->"Robert Paulsen"))

    // then - does not throw
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesSet = 3/*should really be 2!*/)
  }

  test("should_work_when_finding_multiple_elements") {
    assertStats(execute( "CREATE (:X) CREATE (:X) MERGE (:X)"), nodesCreated = 2, labelsAdded = 2)
  }

  test("should_support_updates_while_merging") {
    (0 to 2) foreach(x =>
      (0 to 2) foreach( y=>
        createNode("x"->x, "y"->y)
        ))

    // when
    execute(
      "MATCH (foo) WITH foo.x AS x, foo.y AS y " +
        "MERGE (c:N {x: x, y: y+1}) " +
        "MERGE (a:N {x: x, y: y}) " +
        "MERGE (b:N {x: x+1, y: y})  " +
        "RETURN x, y;").toList
  }

  test("merge_should_see_identifiers_introduced_by_other_update_actions") {
    // when
    val result = execute("CREATE a MERGE a-[:X]->() RETURN a")

    // then
    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  test("merge_inside_foreach_should_see_identifiers_introduced_by_update_actions_outside_foreach") {
    // when
    val result = execute("CREATE a FOREACH(x in [1,2,3] | MERGE (a)-[:X]->({id: x})) RETURN a")

    // then
    assertStats(result, nodesCreated = 4, relationshipsCreated = 3, propertiesSet = 3)
  }

  test("merge_should_see_identifiers_introduced_by_update_actions") {
    // when
    val result = execute("CREATE a MERGE (a)-[:X]->() RETURN a")

    // then
    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  test("should not use eager if on create modifies relationships which don't affect the match clauses") {
    val result = execute(
      """MATCH (src:LeftLabel), (dst:RightLabel)
        |MERGE (src)-[r:IS_RELATED_TO ]->(dst)
        |ON CREATE SET r.p3 = 42;""".stripMargin)

    result.executionPlanDescription().toString should not include "Eager"
  }

  test("merge must properly handle multiple labels") {
    createLabeledNode(Map("prop" -> 42), "L", "A")

    val result = execute("merge (test:L:B {prop : 42}) return labels(test) as labels")

    assertStats(result, nodesCreated = 1, propertiesSet = 1, labelsAdded = 2)
    result.toList should equal(List(Map("labels" -> List("L", "B"))))
  }

  test("merge with an index must properly handle multiple labels") {
    graph.createIndex("L", "prop")
    createLabeledNode(Map("prop" -> 42), "L", "A")

    val result = execute("merge (test:L:B {prop : 42}) return labels(test) as labels")

    assertStats(result, nodesCreated = 1, propertiesSet = 1, labelsAdded = 2)
    result.toList should equal(List(Map("labels" -> List("L", "B"))))
  }

  test("merge with uniqueness constraints must properly handle multiple labels") {
    graph.createConstraint("L", "prop")
    val node = createLabeledNode(Map("prop" -> 42), "L", "A")

    val result = intercept[CypherExecutionException](execute("merge (test:L:B {prop : 42}) return labels(test) as labels"))

    result.getCause shouldBe a [UniquePropertyConstraintViolationKernelException]
    result.getMessage should equal(s"""Node ${node.getId} already exists with label L and property "prop"=[42]""")
  }
}
