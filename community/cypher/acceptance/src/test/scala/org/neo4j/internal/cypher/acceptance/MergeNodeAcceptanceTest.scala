/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{CypherExecutionException, ExecutionEngineFunSuite, MergeConstraintConflictException, NewPlannerTestSupport, QueryStatisticsTestSupport}
import org.neo4j.graphdb.Node
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyConstraintViolationKernelException

class MergeNodeAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("merge node when no nodes exist") {
    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a) return count(*) as n")

    // Then
    val createdNodes = result.columnAs[Int]("n").toList

    createdNodes should equal(List(1))
    assertStats(result, nodesCreated = 1)
  }

  test("merge node with label") {
    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label) return labels(a)")

    result.toList should equal(List(Map("labels(a)" -> List("Label"))))
    assertStats(result, nodesCreated = 1, labelsAdded = 1)
  }

  test("merge node with label add label on create") {
    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label) on create set a:Foo return labels(a)")

    // Then

    result.toList should equal(List(Map("labels(a)" -> List("Label", "Foo"))))
    assertStats(result, nodesCreated = 1, labelsAdded = 2)
  }

  test("merge node with label add property on update") {
    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label) on create set a.prop = 42 return a.prop")

    result.toList should equal(List(Map("a.prop" -> 42)))
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesWritten = 1)
  }

  test("merge node with label when it exists") {
    // Given
    val existingNode = createLabeledNode("Label")

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label) return id(a)")

    // Then
    val createdNodes = result.columnAs[Long]("id(a)").toList

    createdNodes should equal(List(existingNode.getId))
    assertStats(result, nodesCreated = 0)
  }

  test("merge node with property when it exists") {
    // Given
    createNode("prop" -> 42)

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a {prop: 42}) return a.prop")

    // Then
    result.toList should equal(List(Map("a.prop" -> 42)))
    assertStats(result, nodesCreated = 0)
  }

  test("merge node should create when it doesn't match") {
    // Given
    createNode("prop" -> 42)

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a {prop: 43}) return a.prop")

    // Then
    result.toList should equal(List(Map("a.prop" -> 43)))
    assertStats(result, nodesCreated = 1, propertiesWritten = 1)
  }

  test("merge node with prop and label") {
    // Given
    createLabeledNode(Map("prop" -> 42), "Label")

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label {prop: 42}) return a.prop")

    // Then
    result.toList should equal(List(Map("a.prop" -> 42)))
    assertStats(result, nodesCreated = 0)
  }

  test("merge node with prop and label and unique index") {
    // Given
    graph.createConstraint("Label", "prop")
    createLabeledNode(Map("prop" -> 42), "Label")

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label {prop: 42}) return a.prop")
    // Then
    result.toList should equal(List(Map("a.prop" -> 42)))
    assertStats(result, nodesCreated = 0)
  }

  test("merge node with prop and label and unique index when no match") {
    // Given
    graph.createConstraint("Label", "prop")
    createLabeledNode(Map("prop" -> 42), "Label")

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label {prop: 11}) return a.prop")

    // Then
    result.toList should equal(List(Map("a.prop" -> 11)))
    assertStats(result, nodesCreated = 1, propertiesWritten = 1, labelsAdded = 1)
  }

  test("multiple merges after each other") {
    1 to 100 foreach { prop =>
      val result = updateWithBothPlannersAndCompatibilityMode(s"merge (a:Label {prop: $prop}) return a.prop")
      assertStats(result, nodesCreated = 1, propertiesWritten = 1, labelsAdded = 1)
    }
  }

  test("merge node with label add label on create when it exists") {
    // Given
    createLabeledNode("Label")

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label) on match set a:Foo return labels(a)")

    // Then
    result.toList should equal(List(Map("labels(a)" -> List("Label", "Foo"))))
    assertStats(result, nodesCreated = 0, labelsAdded = 1)
  }

  test("merge node with label add property on update when it exists") {
    // Given
    createLabeledNode("Label")

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label) on create set a.prop = 42 return a.prop")

    // Then
    result.toList should equal(List(Map("a.prop" -> null)))
    assertStats(result, nodesCreated = 0)
  }

  test("merge node and set property on match") {
    // Given
    createLabeledNode("Label")

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label) on match set a.prop = 42 return a.prop")

    // Then
    result.toList should equal(List(Map("a.prop" -> 42)))
    assertStats(result, propertiesWritten = 1)
  }

  test("merge node should match properties given ad map") {
    // Given
    createLabeledNode(Map("prop" -> 42), "Label")

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label {prop:42}) return a.prop")

    // Then
    assertStats(result, nodesCreated = 0)
    result.columnAs[Int]("a.prop").toList should equal(List(42))
  }

  test("merge node should create a node with given properties when no match is found") {
    // Given - a node that does not match
    val other = createLabeledNode(Map("prop" -> 666), "Label")

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label {prop:42}) return a.prop")

    // Then
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesWritten = 1)
    val props = result.columnAs[Node]("a.prop").toList
    props should equal(List(42))
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

  test("should fail on merge using multiple unique indexes using same key if found different nodes") {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("User", "id")

    createLabeledNode(Map("id" -> 23), "Person")
    createLabeledNode(Map("id" -> 23), "User")

    // when + then
    intercept[MergeConstraintConflictException](executeWithCostPlannerOnly("merge (a:Person:User {id: 23}) return a.id"))
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

  test("should fail on merge using multiple unique indexes if found different nodes") {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("Person", "mail")

    createLabeledNode(Map("id" -> 23), "Person")
    createLabeledNode(Map("mail" -> "emil@neo.com"), "Person")

    val error = intercept[MergeConstraintConflictException](executeWithCostPlannerOnly("merge (a:Person {id: 23, mail: 'emil@neo.com'}) return a"))

    error.getMessage contains "Merge did not find a matching node and can not create a new node due to conflicts with existing unique nodes. The conflicting constraints are on: :Person.id and :Person.mail"

    countNodes() should equal(2)
  }

  test("should fail on merge using multiple unique indexes if it found a node matching single property only") {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("Person", "mail")

    createLabeledNode(Map("id" -> 23), "Person")

    // when + then
    val error = intercept[MergeConstraintConflictException](executeWithCostPlannerOnly("merge (a:Person {id: 23, mail: 'emil@neo.com'}) return a"))

    error.getMessage contains "Merge did not find a matching node and can not create a new node due to conflicts with both existing and missing unique nodes. The conflicting constraints are on: :Person.id and :Person.mail"

    countNodes() should equal(1)
  }

  //TODO rule planner fails on this
  test("should fail on merge using multiple unique indexes if it found a node matching single property only flipped order") {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("Person", "mail")

    createLabeledNode(Map("mail" -> "emil@neo.com"), "Person")

    // when + then
    val error = intercept[MergeConstraintConflictException](executeWithCostPlannerOnly("merge (a:Person {id: 23, mail: 'emil@neo.com'}) return a"))

    error.getMessage contains "Merge did not find a matching node and can not create a new node due to conflicts with both existing and missing unique nodes. The conflicting constraints are on: :Person.id and :Person.mail"

    countNodes() should equal(1)
  }

  test("should fail on merge using multiple unique indexes and labels if found different nodes") {
    // given
    graph.createConstraint("Person", "id")
    graph.createConstraint("User", "mail")

    createLabeledNode(Map("id" -> 23), "Person")
    createLabeledNode(Map("mail" -> "emil@neo.com"), "User")

    // when
    val error = intercept[MergeConstraintConflictException](executeWithCostPlannerOnly("merge (a:Person:User {id: 23, mail: 'emil@neo.com'}) return a"))

    error.getMessage contains "Merge did not find a matching node and can not create a new node due to conflicts with existing unique nodes. The conflicting constraints are on: :Person.id and :User.mail"

    // then
    countNodes() should equal(2)
  }

  test("should_handle_running_merge_inside_a_foreach_loop") {
    // given an empty database

    // when
    val result = updateWithBothPlanners("foreach(x in [1,2,3] | merge ({property: x}))")

    // then
    assertStats(result, nodesCreated = 3, propertiesWritten = 3)
  }

  test("unrelated nodes with same property should not clash") {
    // given
    graph.createConstraint("Person", "id")
    graph.execute("MERGE (a:Item {id:1}) MERGE (b:Person {id:1})")

    // when
    updateWithBothPlannersAndCompatibilityMode("MERGE (a:Item {id:2}) MERGE (b:Person {id:1})")

    // then does not throw
  }

  test("works fine with index") {
    // given
    updateWithBothPlannersAndCompatibilityMode("create index on :Person(name)")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (person:Person {name:'Lasse'}) RETURN person.name")

    // then does not throw
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesWritten = 1)
  }

  test("works with index and constraint") {
    // given
    updateWithBothPlannersAndCompatibilityMode("create index on :Person(name)")
    graph.createConstraint("Person", "id")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (person:Person {name:'Lasse', id:42}) RETURN person.name")

    // then does not throw
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesWritten = 2)
  }

  test("works with indexed and unindexed property") {
    // given
    updateWithBothPlannersAndCompatibilityMode("create index on :Person(name)")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (person:Person {name:'Lasse', id:42}) RETURN person.name")

    // then does not throw
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesWritten = 2)
  }

  test("works with two indexed properties") {
    // given
    updateWithBothPlannersAndCompatibilityMode("create index on :Person(name)")
    updateWithBothPlannersAndCompatibilityMode("create index on :Person(id)")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (person:Person {name:'Lasse', id:42}) RETURN person.name")

    // then does not throw
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesWritten = 2)
  }

  test("works with property repeated in literal map in set") {
    // given
    graph.createConstraint("Person","ssn")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (person:Person {ssn:42}) ON CREATE SET person = {ssn:42,name:'Robert Paulsen'} RETURN person.ssn")

    // then - does not throw
    result.toList should equal(List(Map("person.ssn" -> 42)))
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesWritten = 3)
  }

  test("works with property in map that gets set") {
    // given
    graph.createConstraint("Person","ssn")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (person:Person {ssn:{p}.ssn}) ON CREATE SET person = {p} RETURN person.ssn",
      "p" -> Map("ssn" -> 42, "name"->"Robert Paulsen"))

    // then - does not throw
    result.toList should equal(List(Map("person.ssn" -> 42)))
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesWritten = 3)
  }

  test("should work when finding multiple elements") {
    assertStats(updateWithBothPlannersAndCompatibilityMode( "CREATE (:X) CREATE (:X) MERGE (:X)"), nodesCreated = 2, labelsAdded = 2)
  }

  test("merge should handle argument properly") {
    createNode("x" -> 42)
    createNode("x" -> "not42")

    val query = "WITH 42 AS x MERGE (c:N {x: x})"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesWritten = 1)
  }

  ignore("merge should handle arguments properly with only write clauses") {
    val query = "CREATE (a {p: 1}) MERGE (b {v: a.p})"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 2, propertiesWritten = 2)
  }

  test("should be able to merge using property from match") {
    createLabeledNode(Map("name" -> "A", "bornIn" -> "New York"), "Person")
    createLabeledNode(Map("name" -> "B", "bornIn" -> "Ohio"), "Person")
    createLabeledNode(Map("name" -> "C", "bornIn" -> "New Jersey"), "Person")
    createLabeledNode(Map("name" -> "D", "bornIn" -> "New York"), "Person")
    createLabeledNode(Map("name" -> "E", "bornIn" -> "Ohio"), "Person")
    createLabeledNode(Map("name" -> "F", "bornIn" -> "New Jersey"), "Person")

    val query = "MATCH (person:Person) MERGE (city: City {name: person.bornIn}) RETURN person.name"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 3, propertiesWritten = 3, labelsAdded = 3)
  }

  test("should be able to merge using property from match with index") {
    graph.createIndex("City", "name")

    createLabeledNode(Map("name" -> "A", "bornIn" -> "New York"), "Person")
    createLabeledNode(Map("name" -> "B", "bornIn" -> "Ohio"), "Person")
    createLabeledNode(Map("name" -> "C", "bornIn" -> "New Jersey"), "Person")
    createLabeledNode(Map("name" -> "D", "bornIn" -> "New York"), "Person")
    createLabeledNode(Map("name" -> "E", "bornIn" -> "Ohio"), "Person")
    createLabeledNode(Map("name" -> "F", "bornIn" -> "New Jersey"), "Person")

    val query = "MATCH (person:Person) MERGE (city: City {name: person.bornIn}) RETURN person.name"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 3, propertiesWritten = 3, labelsAdded = 3)
  }

  test("should be able to use properties from match in ON CREATE") {
    createLabeledNode(Map("bornIn" -> "New York"), "Person")
    createLabeledNode(Map("bornIn" -> "Ohio"), "Person")

    val query = "MATCH (person:Person) MERGE (city: City) ON CREATE SET city.name = person.bornIn RETURN person.bornIn"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 1, propertiesWritten = 1, labelsAdded = 1)
  }

  test("should be able to use properties from match in ON MATCH") {
    createLabeledNode(Map("bornIn" -> "New York"), "Person")
    createLabeledNode(Map("bornIn" -> "Ohio"), "Person")

    val query = "MATCH (person:Person) MERGE (city: City) ON MATCH SET city.name = person.bornIn RETURN person.bornIn"

    val result = updateWithBothPlannersAndCompatibilityMode(query)
    assertStats(result, nodesCreated = 1, propertiesWritten = 1, labelsAdded = 1)
  }

  test("should be able to use properties from match in ON MATCH and ON CREATE") {
    createLabeledNode(Map("bornIn" -> "New York"), "Person")
    createLabeledNode(Map("bornIn" -> "Ohio"), "Person")

    val query = "MATCH (person:Person) MERGE (city: City) ON MATCH SET city.name = person.bornIn ON CREATE SET city.name = person.bornIn RETURN person.bornIn"

    val result = updateWithBothPlannersAndCompatibilityMode(query)
    assertStats(result, nodesCreated = 1, propertiesWritten = 2, labelsAdded = 1)
    executeWithAllPlannersAndCompatibilityMode("MATCH (n:City) WHERE NOT exists(n.name) RETURN n").toList shouldBe empty
  }

  test("should be able to set labels on match") {
    createNode()

    val query = "MERGE (a) ON MATCH SET a:L"

    val result = updateWithBothPlannersAndCompatibilityMode(query)
    assertStats(result, labelsAdded = 1)
  }

  test("should be able to set labels on match and on create") {
    createNode()
    createNode()

    val query = "MATCH () MERGE (a:L) ON MATCH SET a:M1 ON CREATE SET a:M2"

    val result = updateWithBothPlannersAndCompatibilityMode(query)
    assertStats(result, nodesCreated=1, labelsAdded = 3)
    executeScalarWithAllPlannersAndCompatibilityMode[Int]("MATCH (a:L) RETURN count(a)") should equal(1)
    executeScalarWithAllPlannersAndCompatibilityMode[Int]("MATCH (a:M1) RETURN count(a)") should equal(1)
    executeScalarWithAllPlannersAndCompatibilityMode[Int]("MATCH (a:M2) RETURN count(a)") should equal(1)
  }


  test("should support updates while merging") {
    (0 to 2) foreach(x =>
      (0 to 2) foreach( y=>
        createNode("x"->x, "y"->y)
        ))

    // when
    updateWithBothPlannersAndCompatibilityMode(
      "MATCH (foo) WITH foo.x AS x, foo.y AS y " +
        "MERGE (c:N {x: x, y: y+1}) " +
        "MERGE (a:N {x: x, y: y}) " +
        "MERGE (b:N {x: x+1, y: y})  " +
        "RETURN x, y;").toList
  }



  test("merge_inside_foreach_should_see_variables_introduced_by_update_actions_outside_foreach") {
    // when
    val result = updateWithBothPlanners("CREATE (a {name: 'Start'}) FOREACH(x in [1,2,3] | MERGE (a)-[:X]->({id: x})) RETURN a.name")

    // then
    assertStats(result, nodesCreated = 4, relationshipsCreated = 3, propertiesWritten = 4)
  }

  test("merge must properly handle multiple labels") {
    createLabeledNode(Map("prop" -> 42), "L", "A")

    val result = updateWithBothPlannersAndCompatibilityMode("merge (test:L:B {prop : 42}) return labels(test) as labels")

    assertStats(result, nodesCreated = 1, propertiesWritten = 1, labelsAdded = 2)
    result.toList should equal(List(Map("labels" -> List("L", "B"))))
  }

  test("merge with an index must properly handle multiple labels") {
    graph.createIndex("L", "prop")
    createLabeledNode(Map("prop" -> 42), "L", "A")

    val result = updateWithBothPlannersAndCompatibilityMode("merge (test:L:B {prop : 42}) return labels(test) as labels")

    assertStats(result, nodesCreated = 1, propertiesWritten = 1, labelsAdded = 2)
    result.toList should equal(List(Map("labels" -> List("L", "B"))))
  }

  test("merge with uniqueness constraints must properly handle multiple labels") {
    graph.createConstraint("L", "prop")
    val node = createLabeledNode(Map("prop" -> 42), "L", "A")

    val result = intercept[CypherExecutionException](updateWithBothPlannersAndCompatibilityMode("merge (test:L:B {prop : 42}) return labels(test) as labels"))

    result.getCause shouldBe a [UniquePropertyConstraintViolationKernelException]
    result.getMessage should equal(s"""Node ${node.getId} already exists with label L and property "prop"=[42]""")
  }

  test("merge followed by multiple creates") {
    val query =
      """MERGE (t:T {id:42})
        |CREATE (f:R)
        |CREATE (t)-[:REL]->(f)
      """.stripMargin

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 2, labelsAdded = 2, relationshipsCreated = 1, propertiesWritten = 1)
  }

  test("unwind combined with merge") {
    val query = "UNWIND [1,2,3,4] AS int MERGE (n {id: int}) RETURN count(*)"
    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 4, propertiesWritten = 4)
    result.toList should equal(List(Map("count(*)" -> 4)))
  }

  test("merges should not be able to match on deleted nodes") {
    // GIVEN
    val node1 = createLabeledNode(Map("value" -> 1), "A")
    val node2 = createLabeledNode(Map("value" -> 2), "A")

    val query = """
                  |MATCH (a:A)
                  |DELETE a
                  |MERGE (a2:A)
                  |RETURN a2.value
                """.stripMargin

    // WHEN
    val result = updateWithBothPlannersAndCompatibilityMode(query)

    // THEN
    result.toList should not contain Map("a2" -> node1)
    result.toList should not contain Map("a2" -> node2)
  }
}
