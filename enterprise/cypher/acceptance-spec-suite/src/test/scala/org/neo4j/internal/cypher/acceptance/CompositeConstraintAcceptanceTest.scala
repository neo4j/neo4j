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

import org.hamcrest.CoreMatchers.containsString
import org.junit.Assert.assertThat
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.cypher._
import org.neo4j.graphdb.ConstraintViolationException
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory
import org.scalatest.matchers.{MatchResult, Matcher}
import org.neo4j.cypher.internal.frontend.v3_2.helpers.StringHelper._

import scala.collection.JavaConverters._

class CompositeConstraintAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  override protected def createGraphDatabase(): GraphDatabaseCypherService = {
    new GraphDatabaseCypherService(new TestEnterpriseGraphDatabaseFactory().newImpermanentDatabase(databaseConfig().asJava))
  }

  test("should be able to create and remove composite uniqueness constraints") {
    // When
    executeWithCostPlannerAndInterpretedRuntimeOnly("CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE")
    executeWithCostPlannerAndInterpretedRuntimeOnly("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS UNIQUE")

    // Then
    graph should haveConstraints("UNIQUENESS:Person(email)", "UNIQUENESS:Person(firstname,lastname)")

    // When
    executeWithCostPlannerAndInterpretedRuntimeOnly("DROP CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS UNIQUE")

    // Then
    graph should haveConstraints("UNIQUENESS:Person(email)")
    graph should not(haveConstraints("UNIQUENESS:Person(firstname,lastname)"))
  }

  test("composite uniqueness constraint should not block adding nodes with different properties") {
    // When
    executeWithCostPlannerAndInterpretedRuntimeOnly("CREATE CONSTRAINT ON (n:User) ASSERT (n.firstname,n.lastname) IS UNIQUE")

    // Then
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")
    createLabeledNode(Map("firstname" -> "Jake", "lastname" -> "Soap"), "User")
  }

  test("composite uniqueness constraint should block adding nodes with same properties") {
    // When
    executeWithCostPlannerAndInterpretedRuntimeOnly("CREATE CONSTRAINT ON (n:User) ASSERT (n.firstname,n.lastname) IS UNIQUE")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")

    // Then
    a[ConstraintViolationException] should be thrownBy {
      createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    }
  }

  test("composite uniqueness constraint should not fail when we have nodes with different properties") {
    // When
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")
    createLabeledNode(Map("firstname" -> "Jake", "lastname" -> "Soap"), "User")

    // Then
    executeWithCostPlannerAndInterpretedRuntimeOnly("CREATE CONSTRAINT ON (n:User) ASSERT (n.firstname,n.lastname) IS UNIQUE")
  }

  test("composite uniqueness constraint should fail when we have nodes with same properties") {
    // When
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")

    // Then
    a[CypherExecutionException] should be thrownBy {
      executeWithCostPlannerAndInterpretedRuntimeOnly("CREATE CONSTRAINT ON (n:User) ASSERT (n.firstname,n.lastname) IS UNIQUE")
    }
  }

  test("should be able to create and remove single property NODE KEY") {
    // When
    executeWithCostPlannerAndInterpretedRuntimeOnly("CREATE CONSTRAINT ON (n:Person) ASSERT (n.email) IS NODE KEY")

    // Then
    graph should haveConstraints("NODE_KEY:Person(email)")

    // When
    executeWithCostPlannerAndInterpretedRuntimeOnly("DROP CONSTRAINT ON (n:Person) ASSERT (n.email) IS NODE KEY")

    // Then
    graph should not(haveConstraints("NODE_KEY:Person(email)"))
  }

  test("should be able to create and remove multiple property NODE KEY") {
    // When
    executeWithCostPlannerAndInterpretedRuntimeOnly("CREATE CONSTRAINT ON (n:Person) ASSERT (n.email) IS NODE KEY")
    executeWithCostPlannerAndInterpretedRuntimeOnly("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY")

    // Then
    graph should haveConstraints("NODE_KEY:Person(email)", "NODE_KEY:Person(firstname,lastname)")

    // When
    executeWithCostPlannerAndInterpretedRuntimeOnly("DROP CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY")

    // Then
    graph should haveConstraints("NODE_KEY:Person(email)")
    graph should not(haveConstraints("NODE_KEY:Person(firstname,lastname)"))
  }

  test("trying to add duplicate node when unique constraint exists") {
    createLabeledNode(Map("name" -> "A"), "Person")
    executeQuery("CREATE CONSTRAINT ON (person:Person) ASSERT person.name IS UNIQUE")

    expectError(
      "CREATE (n:Person) SET n.name = 'A'",
      String.format("Node(0) already exists with label `Person` and property `name` = 'A'")
    )
  }

  test("trying to add a node key constraint when duplicates exist") {
    createLabeledNode(Map("name" -> "A"), "Person")
    createLabeledNode(Map("name" -> "A"), "Person")

    expectError(
      "CREATE CONSTRAINT ON (person:Person) ASSERT (person.name) IS NODE KEY",
      String.format("Unable to create CONSTRAINT ON ( person:Person ) ASSERT person.name IS NODE KEY:%n" +
        "Both Node(0) and Node(1) have the label `Person` and property `name` = 'A'")
    )
  }

  test("trying to add duplicate node when node key constraint exists") {
    createLabeledNode(Map("name" -> "A"), "Person")
    executeQuery("CREATE CONSTRAINT ON (person:Person) ASSERT (person.name) IS NODE KEY")

    expectError(
      "CREATE (n:Person) SET n.name = 'A'",
      String.format("Node(0) already exists with label `Person` and property `name` = 'A'")
    )
  }

  test("drop a non existent node key constraint") {
    expectError(
      "DROP CONSTRAINT ON (person:Person) ASSERT (person.name) IS NODE KEY",
      "No such constraint"
    )
  }

  test("trying to add duplicate node when composite unique constraint exists") {
    createLabeledNode(Map("name" -> "A", "surname" -> "B"), "Person")
    executeQuery("CREATE CONSTRAINT ON (person:Person) ASSERT (person.name, person.surname) IS UNIQUE")

    expectError(
      "CREATE (n:Person) SET n.name = 'A', n.surname = 'B'",
      String.format("Node(0) already exists with label `Person` and properties `name` = 'A', `surname` = 'B'")
    )
  }

  test("trying to add a composite node key constraint when duplicates exist") {
    createLabeledNode(Map("name" -> "A", "surname" -> "B"), "Person")
    createLabeledNode(Map("name" -> "A", "surname" -> "B"), "Person")

    expectError(
      "CREATE CONSTRAINT ON (person:Person) ASSERT (person.name, person.surname) IS NODE KEY",
      String.format("Unable to create CONSTRAINT ON ( person:Person ) ASSERT (person.name, person.surname) IS NODE KEY:%n" +
        "Both Node(0) and Node(1) have the label `Person` and properties `name` = 'A', `surname` = 'B'")
    )
  }

  test("trying to add duplicate node when composite node key constraint exists") {
    createLabeledNode(Map("name" -> "A", "surname" -> "B"), "Person")
    executeQuery("CREATE CONSTRAINT ON (person:Person) ASSERT (person.name, person.surname) IS NODE KEY")

    expectError(
      "CREATE (n:Person) SET n.name = 'A', n.surname = 'B'",
      String.format("Node(0) already exists with label `Person` and properties `name` = 'A', `surname` = 'B'")
    )
  }

  private def expectError(query: String, expectedError: String) {
    val error = intercept[CypherException](executeQuery(query))
    assertThat(error.getMessage, containsString(expectedError))
  }

  private def executeQuery(query: String) {
    executeWithCostPlannerAndInterpretedRuntimeOnly(query.fixNewLines).toList
  }

  case class haveConstraints(expectedConstraints: String*) extends Matcher[GraphDatabaseQueryService] {
    def apply(graph: GraphDatabaseQueryService): MatchResult = {
      graph.inTx {
        val constraintNames = graph.schema().getConstraints.asScala.toList.map(i => s"${i.getConstraintType}:${i.getLabel}(${i.getPropertyKeys.asScala.toList.mkString(",")})")
        val result = expectedConstraints.forall(i => constraintNames.contains(i.toString))
        MatchResult(
          result,
          s"Expected graph to have constraints ${expectedConstraints.mkString(", ")}, but it was ${constraintNames.mkString(", ")}",
          s"Expected graph to not have constraints ${expectedConstraints.mkString(", ")}, but it did."
        )
      }
    }
  }
}
