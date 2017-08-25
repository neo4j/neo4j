/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.cypher.internal.frontend.v3_3.helpers.StringHelper._
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.graphdb.ConstraintViolationException
import org.neo4j.graphdb.config.Setting
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory

import scala.collection.JavaConverters._
import scala.collection.Map

class CompositeNodeKeyConstraintAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport{

  override protected def createGraphDatabase(config: Map[Setting[_], String] = databaseConfig()): GraphDatabaseCypherService = {
    new GraphDatabaseCypherService(new TestEnterpriseGraphDatabaseFactory().newImpermanentDatabase(config.asJava))
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
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")

    // Then
    a[CypherExecutionException] should be thrownBy {
      succeedWith(Configs.AbsolutelyAll,
        "CREATE CONSTRAINT ON (n:User) ASSERT (n.firstname,n.lastname) IS NODE KEY")
    }
  }

  test("trying to add duplicate node when node key constraint exists") {
    createLabeledNode(Map("name" -> "A"), "Person")
    graph.execute("CREATE CONSTRAINT ON (person:Person) ASSERT (person.name) IS NODE KEY".fixNewLines)

    failWithError(
      Configs.AllExceptSlotted + Configs.Procs - Configs.Compiled - Configs.Cost2_3,
      "CREATE (n:Person) SET n.name = 'A'",
      "Node(0) already exists with label `Person` and property `name` = 'A'"
    )
  }

  test("trying to add duplicate node when composite NODE KEY constraint exists") {
    createLabeledNode(Map("name" -> "A", "surname" -> "B"), "Person")
    graph.execute("CREATE CONSTRAINT ON (person:Person) ASSERT (person.name, person.surname) IS NODE KEY".fixNewLines)

    failWithError(
      Configs.AllExceptSlotted + Configs.Procs - Configs.Compiled - Configs.Cost2_3,
      "CREATE (n:Person) SET n.name = 'A', n.surname = 'B'",
      String.format("Node(0) already exists with label `Person` and properties `name` = 'A', `surname` = 'B'")
    )
  }

  test("trying to add a composite node key constraint when duplicates exist") {
    createLabeledNode(Map("name" -> "A", "surname" -> "B"), "Person")
    createLabeledNode(Map("name" -> "A", "surname" -> "B"), "Person")

    failWithError(
      Configs.AbsolutelyAll - Configs.BackwardsCompatibility + Configs.Version3_2 - Configs.AllRulePlanners,
      "CREATE CONSTRAINT ON (person:Person) ASSERT (person.name, person.surname) IS NODE KEY",
      String.format("Unable to create CONSTRAINT ON ( person:Person ) ASSERT (person.name, person.surname) IS NODE KEY:%n" +
        "Both Node(0) and Node(1) have the label `Person` and properties `name` = 'A', `surname` = 'B'")
    )
  }

  test("trying to add a node key constraint when duplicates exist") {
    createLabeledNode(Map("name" -> "A"), "Person")
    createLabeledNode(Map("name" -> "A"), "Person")

    failWithError(
      Configs.AbsolutelyAll - Configs.BackwardsCompatibility + Configs.Version3_2 - Configs.AllRulePlanners,
      "CREATE CONSTRAINT ON (person:Person) ASSERT (person.name) IS NODE KEY",
      String.format("Unable to create CONSTRAINT ON ( person:Person ) ASSERT person.name IS NODE KEY:%n" +
        "Both Node(0) and Node(1) have the label `Person` and property `name` = 'A'")
    )
  }

  test("drop a non existent node key constraint") {
    failWithError(
      Configs.AbsolutelyAll - Configs.BackwardsCompatibility + Configs.Version3_2 - Configs.AllRulePlanners,
      "DROP CONSTRAINT ON (person:Person) ASSERT (person.name) IS NODE KEY",
      "No such constraint"
    )
  }

  test("trying to add duplicate node when composite node key constraint exists") {
    createLabeledNode(Map("name" -> "A", "surname" -> "B"), "Person")
    graph.execute("CREATE CONSTRAINT ON (person:Person) ASSERT (person.name, person.surname) IS NODE KEY".fixNewLines)

    failWithError(
      Configs.AllExceptSlotted + Configs.Procs - Configs.Compiled - Configs.Cost2_3,
      "CREATE (n:Person) SET n.name = 'A', n.surname = 'B'",
      String.format("Node(0) already exists with label `Person` and properties `name` = 'A', `surname` = 'B'")
    )
  }

  test("trying to add node withoutwhen composite node key constraint exists") {
    createLabeledNode(Map("name" -> "A", "surname" -> "B"), "Person")
    graph.execute("CREATE CONSTRAINT ON (person:Person) ASSERT (person.name, person.surname) IS NODE KEY".fixNewLines)

    failWithError(
      Configs.AllExceptSlotted + Configs.Procs - Configs.Compiled - Configs.Cost2_3,
      "CREATE (n:Person) SET n.name = 'A', n.surname = 'B'",
      String.format("Node(0) already exists with label `Person` and properties `name` = 'A', `surname` = 'B'")
      )
  }

  test("should give appropriate error message when there is already an index") {
    // Given
    graph.execute("CREATE INDEX ON :Person(firstname, lastname)".fixNewLines)

    // then
    failWithError(Configs.Interpreted + Configs.Compiled + Configs.Procs - Configs.BackwardsCompatibility + Configs.Version3_2 - Configs.AllRulePlanners,
      "CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY",
      "There already exists an index for label 'Person' on properties 'firstname' and 'lastname'. " +
                  "A constraint cannot be created until the index has been dropped.")
  }

  test("should give appropriate error message when there is already a NODE KEY constraint") {
    // Given
    graph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY".fixNewLines)

    // then
    failWithError(
      Configs.Interpreted + Configs.Compiled + Configs.Procs - Configs.BackwardsCompatibility + Configs.Version3_2 - Configs.AllRulePlanners,
      "CREATE INDEX ON :Person(firstname, lastname)",
      "Label 'Person' and properties 'firstname' and 'lastname' have a unique constraint defined on them, " +
                  "so an index is already created that matches this.")
  }

  test("Should give a nice error message when trying to remove property with node key constraint") {
    // Given
    graph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname, n.surname) IS NODE KEY".fixNewLines)
    val id = createLabeledNode(Map("firstname" -> "John", "surname" -> "Wood"), "Person").getId

    // Expect
    failWithError(Configs.CommunityInterpreted + Configs.Procs - Configs.Cost2_3,
      "MATCH (p:Person {firstname: 'John', surname: 'Wood'}) REMOVE p.surname",
      s"Node($id) with label `Person` must have the properties `firstname, surname`")

  }

  test("Should be able to remove non constrained property") {
    // Given
    graph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname, n.surname) IS NODE KEY".fixNewLines)
    val node = createLabeledNode(Map("firstname" -> "John", "surname" -> "Wood", "foo" -> "bar"), "Person")

    // When
    succeedWith(Configs.CommunityInterpreted - Configs.Cost2_3,
      "MATCH (p:Person {firstname: 'John', surname: 'Wood'}) REMOVE p.foo".fixNewLines)

    // Then
    graph.inTx {
      node.hasProperty("foo") shouldBe false
    }
  }

  test("Should be able to delete node constrained with node key constraint") {
    // Given
    graph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname, n.surname) IS NODE KEY".fixNewLines)
    createLabeledNode(Map("firstname" -> "John", "surname" -> "Wood", "foo" -> "bar"), "Person")

    // When
    succeedWith(Configs.CommunityInterpreted - Configs.Cost2_3,
      "MATCH (p:Person {firstname: 'John', surname: 'Wood'}) DELETE p".fixNewLines)

    // Then
    succeedWith(Configs.Interpreted,
      "MATCH (p:Person {firstname: 'John', surname: 'Wood'}) RETURN p".fixNewLines) shouldBe empty
  }

  test("Should be able to remove label when node key constraint") {
    // Given
    graph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname, n.surname) IS NODE KEY".fixNewLines)
    createLabeledNode(Map("firstname" -> "John", "surname" -> "Wood", "foo" -> "bar"), "Person")

    // When
    succeedWith(Configs.CommunityInterpreted - Configs.Cost2_3,
      "MATCH (p:Person {firstname: 'John', surname: 'Wood'}) REMOVE p:Person".fixNewLines)

    // Then
    succeedWith(Configs.Interpreted,
      "MATCH (p:Person {firstname: 'John', surname: 'Wood'}) RETURN p".fixNewLines) shouldBe empty
  }
}
