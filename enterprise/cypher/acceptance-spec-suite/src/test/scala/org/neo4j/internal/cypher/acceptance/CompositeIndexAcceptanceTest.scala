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

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}
import org.neo4j.graphdb.Node
import org.neo4j.kernel.GraphDatabaseQueryService
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.JavaConverters._

/**
  * These tests are testing the actual index implementation, thus they should all check the actual result.
  * If you only want to verify that plans using indexes are actually planned, please use
  * [[org.neo4j.cypher.internal.compiler.v3_2.planner.logical.LeafPlanningIntegrationTest]]
  */
class CompositeIndexAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("should succeed in creating composite index") {
    // When
    executeWithCostPlannerAndInterpretedRuntimeOnly("CREATE INDEX ON :Person(firstname)")
    executeWithCostPlannerAndInterpretedRuntimeOnly("CREATE INDEX ON :Person(firstname,lastname)")

    // Then
    graph should haveIndexes(":Person(firstname)")
    graph should haveIndexes(":Person(firstname)", ":Person(firstname,lastname)")

    // When
    executeWithCostPlannerAndInterpretedRuntimeOnly("DROP INDEX ON :Person(firstname , lastname)")

    // Then
    graph should haveIndexes(":Person(firstname)")
    graph should not(haveIndexes(":Person(firstname,lastname)"))
  }

  test("should use composite index when all predicates are present") {
    System.setProperty("pickBestPlan.VERBOSE", "true")
    // Given
    executeWithCostPlannerAndInterpretedRuntimeOnly("CREATE INDEX ON :User(firstname,lastname)")
    val n1 = createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    val n2 = createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")
    val n3 = createLabeledNode(Map("firstname" -> "Jake", "lastname" -> "Soap"), "User")
    for (i <- 1 to 100) {
      createLabeledNode(Map("firstname" -> "Joe"), "User")
      createLabeledNode(Map("lastname" -> "Soap"), "User")
    }

    // When
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly("CYPHER runtime=interpreted MATCH (n:User) WHERE n.lastname = 'Soap' AND n.firstname = 'Joe' RETURN n")

    // Then
    println(result.executionPlanDescription())
    result should use("NodeIndexSeek")
    result should evaluateTo(List(Map("n" -> n1)))
  }

  ignore("should be able to update composite index when only one property has changed") {
    executeWithCostPlannerAndInterpretedRuntimeOnly("CREATE INDEX ON :Person(firstname, lastname)")
    val n = executeWithCostPlannerAndInterpretedRuntimeOnly("CREATE (n:Person {firstname:'Joe', lastname:'Soap'}) RETURN n").columnAs("n").toList(0).asInstanceOf[Node]
    executeWithCostPlannerAndInterpretedRuntimeOnly("MATCH (n:Person) SET n.lastname = 'Bloggs'")
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly("CYPHER runtime=interpreted MATCH (n:Person) where n.firstname = 'Joe' and n.lastname = 'Bloggs' RETURN n")
    result should use("NodeIndexSeek")
    result.columnAs("n").toList should be(List(n))
  }

  ignore("should plan a composite index seek for a multiple property predicate expression") {
    executeWithCostPlannerAndInterpretedRuntimeOnly("WITH RANGE(0,10) AS num CREATE (:Person {id:num})") // ensure label cardinality favors index
    executeWithCostPlannerAndInterpretedRuntimeOnly("CREATE (n:Person {firstname:'Joe', lastname:'Soap'})")
    executeWithCostPlannerAndInterpretedRuntimeOnly("CREATE INDEX ON :Person(firstname)")
    executeWithCostPlannerAndInterpretedRuntimeOnly("CALL db.awaitIndex(':Person(firstname)')")
    //    executeWithCostPlannerOnly("CREATE INDEX ON :Person(firstname, lastname)")
    //    executeWithCostPlannerOnly("CALL db.awaitIndex(':Person(firstname,lastname)')")
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly("MATCH (n:Person) WHERE n.firstname = 'Joe' AND n.lastname = 'Soap' RETURN n")
    result should use("NodeIndexSeek")
  }

  case class haveIndexes(expectedIndexes: String*) extends Matcher[GraphDatabaseQueryService] {
    def apply(graph: GraphDatabaseQueryService): MatchResult = {

      graph.inTx {
        val indexNames = graph.schema().getIndexes.asScala.toList.map(i => s":${i.getLabel}(${i.getPropertyKeys.asScala.toList.mkString(",")})")
        println("Found indexes" + indexNames.mkString(", "))

        val result = expectedIndexes.forall(i => indexNames.contains(i.toString))

        MatchResult(
          result,
          s"Expected graph to have indexes ${expectedIndexes.mkString(", ")}, but it was ${indexNames.mkString(", ")}",
          s"Expected graph to not have indexes ${expectedIndexes.mkString(", ")}, but it did."
        )
      }
    }
  }

}
