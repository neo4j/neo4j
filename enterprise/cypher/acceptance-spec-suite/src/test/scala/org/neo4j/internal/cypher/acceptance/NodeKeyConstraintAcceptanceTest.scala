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

import org.neo4j.cypher.internal.compiler.v3_2.executionplan.InternalExecutionResult
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.cypher.{ConstraintValidationException, CypherExecutionException, ExecutionEngineFunSuite, NewPlannerTestSupport}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.JavaConverters._

class NodeKeyConstraintAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  override protected def createGraphDatabase(): GraphDatabaseCypherService = {
    new GraphDatabaseCypherService(new TestEnterpriseGraphDatabaseFactory().newImpermanentDatabase(databaseConfig().asJava))
  }

  test("should be able to create and remove single property NODE KEY") {
    // When
    exec("CREATE CONSTRAINT ON (n:Person) ASSERT (n.email) IS NODE KEY")

    // Then
    graph should haveConstraints("NODE_KEY:Person(email)")

    intercept[ConstraintValidationException](exec("CREATE (:Person)"))
    exec("CREATE (:Person {email: 42})")
    intercept[CypherExecutionException](exec("CREATE (:Person {email: 42})"))

    // When
    exec("DROP CONSTRAINT ON (n:Person) ASSERT (n.email) IS NODE KEY")

    // Then
    graph should haveNoConstraints()
  }

  test("should fail to create NODE KEY on node without property") {
    // When
    exec("CREATE (:Person)")

    // Then
    intercept[CypherExecutionException](exec(
      "CREATE CONSTRAINT ON (n:Person) ASSERT (n.email) IS NODE KEY"))
    graph should haveNoConstraints()
  }

  test("should fail to create NODE KEY on duplicate values") {
    // When
    exec("CREATE (:Person {email: 42})")
    exec("CREATE (:Person {email: 42})")

    // Then
    intercept[CypherExecutionException](exec(
      "CREATE CONSTRAINT ON (n:Person) ASSERT (n.email) IS NODE KEY"))
    graph should haveNoConstraints()
  }

  test("should fail to create NODE KEY on node without secondary property") {
    // When
    exec("CREATE (:Person {email: 42})")

    // Then
    intercept[CypherExecutionException](exec(
      "CREATE CONSTRAINT ON (n:Person) ASSERT (n.email,n.name) IS NODE KEY"))
    graph should haveNoConstraints()
  }

  test("should idempotently enrich preexisting uniqueness constraint") {
    // When
    exec("CREATE CONSTRAINT ON (n:Person) ASSERT (n.email) IS UNIQUE")
    exec("CREATE CONSTRAINT ON (n:Person) ASSERT (n.email) IS NODE KEY")

    // Then
    graph should haveConstraints("NODE_KEY:Person(email)")
  }

  test("should silently ignore duplicate NODE KEY constraint") {
    // When
    exec("CREATE CONSTRAINT ON (n:Person) ASSERT (n.email) IS NODE KEY")
    exec("CREATE CONSTRAINT ON (n:Person) ASSERT (n.email) IS NODE KEY")

    // Then
    graph should haveConstraints("NODE_KEY:Person(email)")

    // When
    exec("CREATE CONSTRAINT ON (n:Person) ASSERT (n.email,n.name) IS NODE KEY")
    exec("CREATE CONSTRAINT ON (n:Person) ASSERT (n.email,n.name) IS NODE KEY")

    // Then
    graph should haveConstraints("NODE_KEY:Person(email,name)")
  }

  test("should be able to create and remove multiple property NODE KEY") {
    // When
    exec("CREATE CONSTRAINT ON (n:Person) ASSERT (n.email) IS NODE KEY")
    exec("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY")

    // Then
    graph should haveConstraints("NODE_KEY:Person(email)", "NODE_KEY:Person(firstname,lastname)")

    // When
    exec("DROP CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY")

    // Then
    graph should haveConstraints("NODE_KEY:Person(email)")
    graph should not(haveConstraints("NODE_KEY:Person(firstname,lastname)"))
  }

  test("should be able to create and remove overlapping NODE KEY constraints") {
    // When
    exec("CREATE CONSTRAINT ON (n:Person) ASSERT (n.a,n.b) IS NODE KEY")
    exec("CREATE CONSTRAINT ON (n:Person) ASSERT (n.b,n.c) IS NODE KEY")

    // Then
    graph should haveConstraints("NODE_KEY:Person(a,b)", "NODE_KEY:Person(b,c)")

    intercept[ConstraintValidationException](exec("CREATE (n:Person{ a: 42, b: 23})"))
    intercept[ConstraintValidationException](exec("CREATE (n:Person{ b: 42, c: 23})"))
    exec("CREATE (:Person {a: 1, b: 2, c: 3})")
    intercept[CypherExecutionException](exec("CREATE (:Person {a: 1, b: 2, c: 666})"))
    intercept[CypherExecutionException](exec("CREATE (:Person {a: 666, b: 2, c: 3})"))
    exec("CREATE (:Person {a: 666, b: 2, c: 666})")

    // When
    exec("DROP CONSTRAINT ON (n:Person) ASSERT (n.a,n.b) IS NODE KEY")

    // Then
    graph should haveConstraints("NODE_KEY:Person(b,c)")
    graph should not(haveConstraints("NODE_KEY:Person(a,b)"))
  }

  case class haveConstraints(expectedConstraints: String*) extends Matcher[GraphDatabaseQueryService] {
    def apply(graph: GraphDatabaseQueryService): MatchResult = {
      graph.inTx {
        val constraintDefinitions = graph.schema().getConstraints.asScala.toList
        val constraintNames = constraintDefinitions.map(i => s"${i.getConstraintType}:${i.getLabel}(${i.getPropertyKeys.asScala.toList.mkString(",")})")
        val result = expectedConstraints.forall(expected => constraintNames.contains(expected))
        MatchResult(
          result,
          s"Expected graph to have constraints ${expectedConstraints.mkString(", ")}, but it was ${constraintNames.mkString(", ")}",
          s"Expected graph to not have constraints ${expectedConstraints.mkString(", ")}, but it did."
        )
      }
    }
  }

  case class haveNoConstraints() extends Matcher[GraphDatabaseQueryService] {
    def apply(graph: GraphDatabaseQueryService): MatchResult = {
      graph.inTx {
        val constraintDefinitions = graph.schema().getConstraints.asScala.toList
        val constraintNames = constraintDefinitions.map(i => s"${i.getConstraintType}:${i.getLabel}(${i.getPropertyKeys.asScala.toList.mkString(",")})")
        MatchResult(
          constraintNames.isEmpty,
          s"Expected graph to not have constraints, but it had ${constraintNames.mkString(", ")}",
          s"Expected graph to have constraints, but it didn't."
        )
      }
    }
  }

  def exec(q: String): InternalExecutionResult = {
    executeWithCostPlannerAndInterpretedRuntimeOnly(q)
  }
}
