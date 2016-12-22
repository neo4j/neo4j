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
import org.neo4j.kernel.GraphDatabaseQueryService
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.JavaConverters._

class CompositeIndexAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {
/*
TODO: add once composite indexes are actually implemented
  test("should succeed in creating composite index") {
    // When
    executeWithCostPlannerOnly("CREATE INDEX ON :Person(firstname)")
    executeWithCostPlannerOnly("CREATE INDEX ON :Person(firstname,lastname)")
    executeWithCostPlannerOnly("CREATE INDEX ON :Person(firstname, lastname)")
    executeWithCostPlannerOnly("CREATE INDEX ON :Person(firstname , lastname)")

    // Then
    graph should haveIndexes(":Person(firstname)")
    graph should haveIndexes(":Person(firstname)", ":Person(firstname,lastname)")

    // When
    executeWithCostPlannerOnly("DROP INDEX ON :Person(firstname , lastname)")

    // Then
    graph should haveIndexes(":Person(firstname)")
    graph should not(haveIndexes(":Person(firstname,lastname)"))
  }

  test("should be able to update composite index when only one property has changed") {
    executeWithCostPlannerOnly("CREATE INDEX ON :Person(firstname, lastname)")
    val n = executeWithCostPlannerOnly("CREATE (n:Person {firstname:'Joe', lastname:'Soap'}) RETURN n").columnAs("n").toList(0)
    executeWithCostPlannerOnly("MATCH (n:Person) SET n.lastname = 'Bloggs'")
    val result = executeWithCostPlannerOnly("MATCH (n:Person) where n.firstname = 'Joe' and n.lastname = 'Bloggs' RETURN n")
    result should use("NodeIndexSeek")
    result.columnAs("n").toList should be(List(n))
  }
*/

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
