/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.opencypher.v9_0.util.{ExhaustiveShortestPathForbiddenException => InternalExhaustiveShortestPathForbiddenException}
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.graphdb.impl.notification.NotificationCode.EXHAUSTIVE_SHORTEST_PATH
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

import scala.collection.mutable

class ShortestPathExhaustiveForbiddenAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  override def databaseConfig(): Map[Setting[_], String] =
    Map(GraphDatabaseSettings.forbid_exhaustive_shortestpath -> "true")

  test("should fail at run time when using the shortest path fallback") {
    // when
    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Cost2_3,
      s"""MATCH p = shortestPath((src:$topLeft)-[*0..]-(dst:$topLeft))
         |WHERE ANY(n in nodes(p) WHERE n:$topRight)
         |RETURN nodes(p) AS nodes""".stripMargin,
      List(InternalExhaustiveShortestPathForbiddenException.ERROR_MSG)
    )
  }

  test("should warn if shortest path fallback is planned") {
    // when
    val result = executeWith(Configs.Interpreted,
      s"""EXPLAIN MATCH p = shortestPath((src:$topLeft)-[*0..]-(dst:$topLeft))
         |WHERE ANY(n in nodes(p) WHERE n:$topRight)
         |RETURN nodes(p) AS nodes""".stripMargin)

    // then
    result.notifications.toSeq should equal(
      Seq(EXHAUSTIVE_SHORTEST_PATH.notification(new org.neo4j.graphdb.InputPosition(47, 1, 48))
      )
    )
  }

  val dim = 4
  val dMax = dim - 1
  val topLeft = "CELL00"
  val topRight = s"CELL0${dMax}"
  val bottomLeft = s"CELL${dMax}0"
  val bottomRight = s"CELL${dMax}${dMax}"
  val middle = s"CELL${dMax / 2}${dMax / 2}"
  val nodesByName: mutable.Map[String, Node] = mutable.Map[String, Node]()

  override protected def initTest(): Unit = {
    super.initTest()
    0 to dMax foreach { row =>
      0 to dMax foreach { col =>
        val name = s"$row$col"
        val node = createLabeledNode(Map("name" -> name, "row" -> row, "col" -> col), s"CELL$row$col", s"ROW$row",
                                     s"COL$col")
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
