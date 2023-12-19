/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.internal.cypher.acceptance

import java.time.{LocalDate, LocalDateTime}

import org.neo4j.cypher._
import org.neo4j.cypher.internal.frontend.v3_4.helpers.StringHelper._
import org.neo4j.graphdb.ConstraintViolationException
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

import scala.collection.Map

class CompositeNodeKeyConstraintAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport with QueryStatisticsTestSupport {

  private val duplicateConstraintConfiguration = Configs.AbsolutelyAll - Configs.Compiled - Configs.Cost2_3

  test("Node key constraint creation should be reported") {
    // When
    val result = innerExecuteDeprecated("CREATE CONSTRAINT ON (n:Person) ASSERT (n.email) IS NODE KEY", Map.empty)

    // Then
    assertStats(result, nodekeyConstraintsAdded = 1)
  }

  test("Uniqueness constraint creation should be reported") {
    // When
    val result = innerExecuteDeprecated("CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE", Map.empty)

    // Then
    assertStats(result, uniqueConstraintsAdded = 1)
  }

  test("should be able to create and remove single property NODE KEY") {
    // When
    graph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT (n.email) IS NODE KEY")

    // Then
    graph should haveConstraints("NODE_KEY:Person(email)")

    // When
    graph.execute("DROP CONSTRAINT ON (n:Person) ASSERT (n.email) IS NODE KEY")

    // Then
    graph should not(haveConstraints("NODE_KEY:Person(email)"))
  }

  test("should be able to create and remove multiple property NODE KEY") {
    // When
    graph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT (n.email) IS NODE KEY")
    graph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY")

    // Then
    graph should haveConstraints("NODE_KEY:Person(email)", "NODE_KEY:Person(firstname,lastname)")

    // When
    graph.execute("DROP CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY")

    // Then
    graph should haveConstraints("NODE_KEY:Person(email)")
    graph should not(haveConstraints("NODE_KEY:Person(firstname,lastname)"))
  }

  test("composite NODE KEY constraint should not block adding nodes with different properties") {
    // When
    graph.execute("CREATE CONSTRAINT ON (n:User) ASSERT (n.firstname,n.lastname) IS NODE KEY")

    // Then
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")
    createLabeledNode(Map("firstname" -> "Jake", "lastname" -> "Soap"), "User")
  }

  test("composite NODE KEY constraint should block adding nodes with same properties") {
    // When
    graph.execute("CREATE CONSTRAINT ON (n:User) ASSERT (n.firstname,n.lastname) IS NODE KEY")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")

    // Then
    a[ConstraintViolationException] should be thrownBy {
      createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    }
  }

  test("single property NODE KEY constraint should block adding nodes with missing property") {
    // When
    graph.execute("CREATE CONSTRAINT ON (n:User) ASSERT (n.email) IS NODE KEY")
    createLabeledNode(Map("email" -> "joe@soap.tv"), "User")
    createLabeledNode(Map("email" -> "jake@soap.tv"), "User")

    // Then
    a[ConstraintViolationException] should be thrownBy {
      createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    }
  }

  test("composite NODE KEY constraint should block adding nodes with missing properties") {
    // When
    graph.execute("CREATE CONSTRAINT ON (n:User) ASSERT (n.firstname,n.lastname) IS NODE KEY")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")

    // Then
    a[ConstraintViolationException] should be thrownBy {
      createLabeledNode(Map("firstname" -> "Joe", "lastnamex" -> "Soap"), "User")
    }
  }

  test("composite NODE KEY constraint should not fail when we have nodes with different properties") {
    // When
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")
    createLabeledNode(Map("firstname" -> "Jake", "lastname" -> "Soap"), "User")

    // Then
    graph.execute("CREATE CONSTRAINT ON (n:User) ASSERT (n.firstname,n.lastname) IS NODE KEY")
  }

  test("composite NODE KEY constraint should fail when we have nodes with same properties") {
    // When
    val a = createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User").getId
    val b = createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User").getId
    val c = createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User").getId

    // Then
    val query = "CREATE CONSTRAINT ON (n:User) ASSERT (n.firstname,n.lastname) IS NODE KEY"
    val errorMessage = "Both Node(%d) and Node(%d) have the label `User` and properties `firstname` = 'Joe', `lastname` = 'Soap'".format(a, c)
    failWithError(Configs.AbsolutelyAll - Configs.OldAndRule, query, List(errorMessage))
  }

  test("trying to add duplicate node when node key constraint exists") {
    createLabeledNode(Map("name" -> "A"), "Person")
    graph.execute("CREATE CONSTRAINT ON (person:Person) ASSERT (person.name) IS NODE KEY".fixNewLines)

    failWithError(
      duplicateConstraintConfiguration,
      "CREATE (n:Person) SET n.name = 'A'",
      List("Node(0) already exists with label `Person` and property `name` = 'A'")
    )
  }

  test("trying to add duplicate node when composite NODE KEY constraint exists") {
    createLabeledNode(Map("name" -> "A", "surname" -> "B"), "Person")
    graph.execute("CREATE CONSTRAINT ON (person:Person) ASSERT (person.name, person.surname) IS NODE KEY".fixNewLines)

    failWithError(
      duplicateConstraintConfiguration,
      "CREATE (n:Person) SET n.name = 'A', n.surname = 'B'",
      List(String.format("Node(0) already exists with label `Person` and properties `name` = 'A', `surname` = 'B'"))
    )
  }

  test("trying to add a composite node key constraint when duplicates exist") {
    val a = createLabeledNode(Map("name" -> "A", "surname" -> "B"), "Person").getId
    val b = createLabeledNode(Map("name" -> "A", "surname" -> "B"), "Person").getId

    failWithError(
      Configs.AbsolutelyAll - Configs.OldAndRule,
      "CREATE CONSTRAINT ON (person:Person) ASSERT (person.name, person.surname) IS NODE KEY",
      List(("Unable to create CONSTRAINT ON ( person:Person ) ASSERT (person.name, person.surname) IS NODE KEY:%s" +
        "Both Node(%d) and Node(%d) have the label `Person` and properties `name` = 'A', `surname` = 'B'").format(String.format("%n"), a, b))
    )
  }

  test("trying to add a node key constraint when duplicates exist") {
    val a = createLabeledNode(Map("name" -> "A"), "Person").getId
    val b = createLabeledNode(Map("name" -> "A"), "Person").getId

    failWithError(
      Configs.AbsolutelyAll - Configs.OldAndRule,
      "CREATE CONSTRAINT ON (person:Person) ASSERT (person.name) IS NODE KEY",
      List(("Unable to create CONSTRAINT ON ( person:Person ) ASSERT person.name IS NODE KEY:%s" +
        "Both Node(%d) and Node(%d) have the label `Person` and property `name` = 'A'").format(String.format("%n"), a, b))
    )
  }

  test("drop a non existent node key constraint") {
    failWithError(
      Configs.AbsolutelyAll - Configs.OldAndRule,
      "DROP CONSTRAINT ON (person:Person) ASSERT (person.name) IS NODE KEY",
      List("No such constraint")
    )
  }

  test("trying to add duplicate node when composite node key constraint exists") {
    createLabeledNode(Map("name" -> "A", "surname" -> "B"), "Person")
    graph.execute("CREATE CONSTRAINT ON (person:Person) ASSERT (person.name, person.surname) IS NODE KEY".fixNewLines)

    failWithError(
      duplicateConstraintConfiguration,
      "CREATE (n:Person) SET n.name = 'A', n.surname = 'B'",
      List(String.format("Node(0) already exists with label `Person` and properties `name` = 'A', `surname` = 'B'"))
    )
  }

  test("should give appropriate error message when there is already an index") {
    // Given
    graph.execute("CREATE INDEX ON :Person(firstname, lastname)".fixNewLines)

    // then
    failWithError(Configs.AbsolutelyAll - Configs.OldAndRule,
      "CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY",
      List("There already exists an index for label 'Person' on properties 'firstname' and 'lastname'. " +
                  "A constraint cannot be created until the index has been dropped."))
  }

  test("should give appropriate error message when there is already a NODE KEY constraint") {
    // Given
    graph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY".fixNewLines)

    // then
    failWithError(
      Configs.AbsolutelyAll - Configs.OldAndRule,
      "CREATE INDEX ON :Person(firstname, lastname)",
      List("Label 'Person' and properties 'firstname' and 'lastname' have a unique constraint defined on them, " +
                  "so an index is already created that matches this."))
  }

  test("Should give a nice error message when trying to remove property with node key constraint") {
    // Given
    graph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname, n.surname) IS NODE KEY".fixNewLines)
    val id = createLabeledNode(Map("firstname" -> "John", "surname" -> "Wood"), "Person").getId

    // Expect
    failWithError(duplicateConstraintConfiguration,
      "MATCH (p:Person {firstname: 'John', surname: 'Wood'}) REMOVE p.surname",
      List(s"Node($id) with label `Person` must have the properties `firstname, surname`"))

  }

  test("Should be able to remove non constrained property") {
    // Given
    graph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname, n.surname) IS NODE KEY".fixNewLines)
    val node = createLabeledNode(Map("firstname" -> "John", "surname" -> "Wood", "foo" -> "bar"), "Person")

    executeWith(Configs.UpdateConf, "MATCH (p:Person {firstname: 'John', surname: 'Wood'}) REMOVE p.foo".fixNewLines)

    // Then
    graph.inTx {
      node.hasProperty("foo") shouldBe false
    }
  }

  test("Should be able to delete node constrained with node key constraint") {
    // Given
    graph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname, n.surname) IS NODE KEY".fixNewLines)
    createLabeledNode(Map("firstname" -> "John", "surname" -> "Wood", "foo" -> "bar"), "Person")

    executeWith(Configs.UpdateConf, "MATCH (p:Person {firstname: 'John', surname: 'Wood'}) DELETE p".fixNewLines)

    // Then
    graph.execute("MATCH (p:Person {firstname: 'John', surname: 'Wood'}) RETURN p".fixNewLines).hasNext shouldBe false
  }

  test("Should be able to remove label when node key constraint") {
    // Given
    graph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname, n.surname) IS NODE KEY".fixNewLines)
    createLabeledNode(Map("firstname" -> "John", "surname" -> "Wood", "foo" -> "bar"), "Person")

    executeWith(Configs.UpdateConf, "MATCH (p:Person {firstname: 'John', surname: 'Wood'}) REMOVE p:Person".fixNewLines)

    // Then
    graph.execute("MATCH (p:Person {firstname: 'John', surname: 'Wood'}) RETURN p".fixNewLines).hasNext shouldBe false
  }

  test("Should handle temporal with node key constraint") {
      // When
      graph.execute("CREATE CONSTRAINT ON (n:User) ASSERT (n.birthday) IS NODE KEY")

      // Then
      createLabeledNode(Map("birthday" -> LocalDate.of(1991, 10, 18)), "User")
      createLabeledNode(Map("birthday" -> LocalDateTime.of(1991, 10, 18, 0, 0, 0, 0)), "User")
      createLabeledNode(Map("birthday" -> "1991-10-18"), "User")
      a[ConstraintViolationException] should be thrownBy {
        createLabeledNode(Map("birthday" -> LocalDate.of(1991, 10, 18)), "User")
      }
  }

  test("Should handle temporal with composite node key constraint") {
    // When
    graph.execute("CREATE CONSTRAINT ON (n:User) ASSERT (n.name, n.birthday) IS NODE KEY")

    // Then
    createLabeledNode(Map("name" -> "Neo", "birthday" -> LocalDate.of(1991, 10, 18)), "User")
    createLabeledNode(Map("name" -> "Neo", "birthday" -> LocalDateTime.of(1991, 10, 18, 0, 0, 0, 0)), "User")
    createLabeledNode(Map("name" -> "Neo", "birthday" -> "1991-10-18"), "User")
    createLabeledNode(Map("name" -> "Neolina", "birthday" -> "1991-10-18"), "User")
    a[ConstraintViolationException] should be thrownBy {
      createLabeledNode(Map("name" -> "Neo", "birthday" -> LocalDate.of(1991, 10, 18)), "User")
    }
  }
}
