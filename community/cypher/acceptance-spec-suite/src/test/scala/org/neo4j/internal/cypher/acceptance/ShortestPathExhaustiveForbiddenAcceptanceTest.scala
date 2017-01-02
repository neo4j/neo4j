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

import org.neo4j.cypher.internal.frontend.v3_2.notification.ExhaustiveShortestPathForbiddenNotification
import org.neo4j.cypher.internal.frontend.v3_2.{ExhaustiveShortestPathForbiddenException => InternalExhaustiveShortestPathForbiddenException, InputPosition}
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, ExhaustiveShortestPathForbiddenException}
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings

import scala.collection.mutable

class ShortestPathExhaustiveForbiddenAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  override def databaseConfig(): Map[Setting[_], String] =
    Map(GraphDatabaseSettings.forbid_exhaustive_shortestpath -> "true")

  test("should fail at run time when using the shortest path fallback") {
    // when
    val exception = the [ExhaustiveShortestPathForbiddenException] thrownBy executeWithCostPlannerOnly(
      s"""MATCH p = shortestPath((src:$topLeft)-[*0..]-(dst:$topLeft))
          |WHERE ANY(n in nodes(p) WHERE n:$topRight)
          |RETURN nodes(p) AS nodes""".stripMargin)

    // then
    exception should have message InternalExhaustiveShortestPathForbiddenException.ERROR_MSG
  }

  test("should warn if shortest path fallback is planned") {
    // when
    val result = executeWithCostPlannerOnly(
      s"""EXPLAIN MATCH p = shortestPath((src:$topLeft)-[*0..]-(dst:$topLeft))
          |WHERE ANY(n in nodes(p) WHERE n:$topRight)
          |RETURN nodes(p) AS nodes""".stripMargin)

    // then
    result.notifications.toSeq should equal(
      Seq(new ExhaustiveShortestPathForbiddenNotification(new InputPosition(10,1,11)))
    )
  }

  val dim = 4
  val dMax = dim - 1
  val topLeft = "CELL00"
  val topRight = s"CELL0${dMax}"
  val bottomLeft = s"CELL${dMax}0"
  val bottomRight = s"CELL${dMax}${dMax}"
  val middle = s"CELL${dMax/2}${dMax/2}"
  val nodesByName: mutable.Map[String, Node] = mutable.Map[String, Node]()

  override protected def initTest(): Unit = {
    super.initTest()
    0 to dMax foreach { row =>
      0 to dMax foreach { col =>
        val name = s"$row$col"
        val node = createLabeledNode(Map("name" -> name, "row" -> row, "col" -> col), s"CELL$row$col", s"ROW$row", s"COL$col")
        nodesByName(name) = node
        if (row > 0) {
          relate(nodesByName(s"${row - 1}$col"), nodesByName(name), "DOWN", s"r${row - 1}-${row}c$col")
        }
        if (col > 0) {
          relate(nodesByName(s"$row${col - 1}"), nodesByName(name), "RIGHT", s"r${row}c${col - 1}${col}")
        }
      }
    }
  }
}
