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

import org.neo4j.cypher.{CypherExecutionException, ExecutionEngineFunSuite, NewPlannerTestSupport}
import org.neo4j.graphdb.{ConstraintViolationException, Node}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.JavaConverters._

class CompositeConstraintAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("should be able to create and remove composite uniquness constraints") {
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

  case class haveConstraints(expectedConstraints: String*) extends Matcher[GraphDatabaseQueryService] {
    def apply(graph: GraphDatabaseQueryService): MatchResult = {
      graph.inTx {
        val constraintName = graph.schema().getConstraints.asScala.toList.map(i => s"${i.getConstraintType}:${i.getLabel}(${i.getPropertyKeys.asScala.toList.mkString(",")})")
        val result = expectedConstraints.forall(i => constraintName.contains(i.toString))
        MatchResult(
          result,
          s"Expected graph to have constraints ${expectedConstraints.mkString(", ")}, but it was ${constraintName.mkString(", ")}",
          s"Expected graph to not have constraints ${expectedConstraints.mkString(", ")}, but it did."
        )
      }
    }
  }
}
