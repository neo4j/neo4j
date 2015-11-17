/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.cypher.{NewPlannerTestSupport, ExecutionEngineFunSuite}
import org.neo4j.graphdb.Node
import scala.collection.immutable.IndexedSeq
import scala.collection.mutable

class ShortestPathLongerAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  val nodesByName: mutable.Map[String, Node] = mutable.Map[String, Node]()

  override protected def initTest(): Unit = {
    super.initTest()
    0 to 9 foreach { row =>
      0 to 9 foreach { col =>
        val name = s"$row$col"
        val node = createLabeledNode(Map("name" -> name, "row" -> row, "col" -> col), s"CELL$row$col", s"ROW$row", s"COL$col")
        nodesByName(name) = node
        if (row > 0) {
          relate(nodesByName(s"${row - 1}$col"), nodesByName(name), "RIGHT", s"r${row - 1}-${row}c$col")
        }
        if (col > 0) {
          relate(nodesByName(s"$row${col - 1}"), nodesByName(name), "DOWN", s"r${row}c${col - 1}${col}")
        }
      }
    }
    nodes
  }

  private def row(row: Int): Set[Node] = {
    val nodes = 0 to 9 map { col: Int =>
      nodesByName(s"$row$col")
    }
    nodes.toSet
  }

  private def col(col: Int): Set[Node] = {
    val nodes = 0 to 9 map { row: Int =>
      nodesByName(s"$row$col")
    }
    nodes.toSet
  }

  test("Shortest path from first to last node via top right") {
    val result = executeWithAllPlanners(
      """MATCH p = shortestPath((src:CELL00)-[*]->(dst:CELL99))
        | WHERE ANY(n in nodes(p) WHERE n:CELL09)
        |RETURN nodes(p) AS nodes""".stripMargin)
      .columnAs[List[Node]]("nodes").toList.map(res => res.toSet)

    result.length should equal(1)
    result(0) should equal(row(0) ++ col(9))
  }

  test("Shortest path from first to last node via bottom left") {
    val result = executeWithAllPlanners(
      """MATCH p = shortestPath((src:CELL00)-[*]->(dst:CELL99))
        | WHERE ANY(n in nodes(p) WHERE n:CELL90)
        |RETURN nodes(p) AS nodes""".stripMargin)
      .columnAs[List[Node]]("nodes").toList.map(res => res.toSet)

    result.length should equal(1)
    result(0) should equal(col(0) ++ row(9))
  }

  // TODO: Fix limitation in shortestPath predicate pull-in
  ignore("Shortest path from first to last node via top right and bottom left") {
    val result = executeWithAllPlanners(
      """MATCH p = shortestPath((src:CELL00)-[*]->(dst:CELL99))
        | WHERE ANY(n in nodes(p) WHERE n:CELL09) AND ANY(n in nodes(p) WHERE n:CELL90)
        |RETURN nodes(p) AS nodes""".stripMargin)
      .columnAs[List[Node]]("nodes").toList.map(res => res.toSet)

    result.length should equal(1)

    println("Got results: " + result(0).toList.sortWith( (a:Node, b:Node) => a.getId < b.getId))
    println("Expect results: " + (row(0) ++ row(9)).toList.sortWith( (a:Node, b:Node) => a.getId < b.getId))

    result(0) should contain(row(0) ++ row(9))
  }
}
