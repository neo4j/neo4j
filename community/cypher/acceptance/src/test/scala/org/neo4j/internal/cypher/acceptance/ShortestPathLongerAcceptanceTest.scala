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

import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.compatibility.ExecutionResultWrapperFor3_0
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.InternalExecutionResult
import org.neo4j.cypher.{NewPlannerTestSupport, ExecutionEngineFunSuite}
import org.neo4j.graphalgo.impl.path.ShortestPath
import org.neo4j.graphalgo.impl.path.ShortestPath.DataMonitor
import org.neo4j.graphdb.{Path, Node}
import org.neo4j.kernel.monitoring.Monitors
import scala.collection.mutable
import scala.collection.JavaConverters._
import java.util

class ShortestPathLongerAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("Shortest path from first to last node via top right") {
    val start = System.currentTimeMillis
    val result = executeWithAllPlanners(
      s"""MATCH p = shortestPath((src:$topLeft)-[*]-(dst:$bottomRight))
        | WHERE ANY(n in nodes(p) WHERE n:$topRight)
        |RETURN nodes(p) AS nodes""".stripMargin)
      .columnAs[List[Node]]("nodes").toList

    result.length should equal(1)
    println(s"Query took ${(System.currentTimeMillis - start)/1000.0}s")
    debugResults(result(0))
    result(0).toSet should equal(row(0) ++ col(dMax))
  }

  test("Shortest path from first to last node via bottom left") {
    val start = System.currentTimeMillis
    val result = executeWithAllPlanners(
      s"""MATCH p = shortestPath((src:$topLeft)-[*]-(dst:$bottomRight))
        | WHERE ANY(n in nodes(p) WHERE n:$bottomLeft)
        |RETURN nodes(p) AS nodes""".stripMargin)
      .columnAs[List[Node]]("nodes").toList

    result.length should equal(1)
    println(s"Query took ${(System.currentTimeMillis - start)/1000.0}s")
    debugResults(result(0))
    result(0).toSet should equal(col(0) ++ row(dMax))
  }

  ignore("Exhaustive shortest path from first to last node via top right") {
    val query =
      s"""PROFILE MATCH p=(src:$topLeft)-[*]-(dst:$bottomRight)
         | WHERE ANY(n in nodes(p) WHERE n:$topRight)
         |RETURN nodes(p) AS nodes ORDER BY length(p) ASC LIMIT 1""".stripMargin
    val startOne = System.currentTimeMillis()
    evaluateShortestPathResults(executeUsingCostPlannerOnly(query), startOne, row(0) ++ col(dMax))
    val startTwo = System.currentTimeMillis()
    evaluateShortestPathResults(executeUsingRulePlannerOnly(query), startTwo, row(0) ++ col(dMax))
  }

  ignore("Exhaustive shortest path from first to last node via bottom left") {
    val query =
      s"""PROFILE MATCH p=(src:$topLeft)-[*]-(dst:$bottomRight)
         | WHERE ANY(n in nodes(p) WHERE n:$bottomLeft)
         | RETURN nodes(p) AS nodes ORDER BY length(p) ASC LIMIT 1""".stripMargin
    val startOne = System.currentTimeMillis()
    evaluateShortestPathResults(executeUsingCostPlannerOnly(query), startOne, col(0) ++ row(dMax))
    val startTwo = System.currentTimeMillis()
    evaluateShortestPathResults(executeUsingRulePlannerOnly(query), startTwo, col(0) ++ row(dMax))
  }

  ignore("Exhaustive shortest path from first to last node via top right and bottom left") {
    val query =
      s"""PROFILE MATCH p=(src:$topLeft)-[*]-(dst:$bottomRight)
         | WHERE ANY(n in nodes(p) WHERE n:$topRight) AND ANY(n in nodes(p) WHERE n:$bottomLeft)
         | RETURN nodes(p) AS nodes ORDER BY length(p) ASC LIMIT 1""".stripMargin
    val startOne = System.currentTimeMillis()
    evaluateShortestPathResults(executeUsingCostPlannerOnly(query), startOne, row(0) ++ row(dMax))
//    val startTwo = System.currentTimeMillis()
//    evaluateShortestPathResults(executeUsingRulePlannerOnly(query), startTwo, row(0) ++ row(dMax))
  }

  test("All shortest path from first to last node") {
    val start = System.currentTimeMillis
    val result = executeUsingCostPlannerOnly(
      s"""MATCH p = allShortestPaths((src:$topLeft)-[*]-(dst:$bottomRight))
         | RETURN p""".stripMargin)

    evaluateAllShortestPathResults(result, "p", start, 70, Set(col(0) ++ row(dMax), row(0) ++ col(dMax)))
  }

  test("All shortest path from first to last node via bottom left") {
    val start = System.currentTimeMillis
    val result = executeUsingCostPlannerOnly(
      s"""MATCH p = allShortestPaths((src:$topLeft)-[*]-(dst:$bottomRight))
         | WHERE ANY(n in nodes(p) WHERE n:$bottomLeft)
         | RETURN p""".stripMargin)

    // TODO: There is a bug in allShortestPaths where it returns the same path twice, change the following test once that bug is fixed.
    evaluateAllShortestPathResults(result, "p", start, 2, Set(col(0) ++ row(dMax)))
  }

  test("All shortest path from first to last node via top right") {
    val start = System.currentTimeMillis
    val result = executeUsingCostPlannerOnly(
      s"""MATCH p = allShortestPaths((src:$topLeft)-[*]-(dst:$bottomRight))
         | WHERE ANY(n in nodes(p) WHERE n:$topRight)
         | RETURN p""".stripMargin)

    evaluateAllShortestPathResults(result, "p", start, 1, Set(row(0) ++ col(dMax)))
  }

  test("All shortest path from first to last node via middle") {
    val start = System.currentTimeMillis
    val result = executeUsingCostPlannerOnly(
      s"""MATCH p = allShortestPaths((src:$topLeft)-[*]-(dst:$bottomRight))
         | WHERE ANY(n in nodes(p) WHERE n:$middle)
         | RETURN p""".stripMargin)

    // TODO: Fix the result count once the duplicate results bug in allShortestPath is fixed
    evaluateAllShortestPathResults(result, "p", start, 54, Set(
      row(0, min = 0, max = dMax / 2) ++ col(dMax / 2) ++ row(dMax, min = dMax / 2, max = dMax),
      col(0, min = 0, max = dMax / 2) ++ row(dMax / 2) ++ col(dMax, min = dMax / 2, max = dMax)
    ))
  }

  ignore("Exhaustive All shortest paths from first to last node via bottom left") {
    val query =
      s"""PROFILE MATCH (src:$topLeft), (dst:$bottomRight)
         | MATCH p=(src)-[*]-(dst)
         | WHERE ANY(n in nodes(p) WHERE n:$bottomLeft)
         | WITH collect(p) as paths
         | WITH reduce(a={l:10000}, p IN paths | case when length(p) > a.l then a when length(p) < a.l then {l:length(p),c:[p]} else {l:a.l, c:a.c +[p]} end).c as all_shortest_paths
         | UNWIND all_shortest_paths as shortest_paths
         | RETURN shortest_paths""".stripMargin
    val startMs = System.currentTimeMillis()
    evaluateAllShortestPathResults(executeUsingCostPlannerOnly(query), "shortest_paths", startMs, 1, Set(col(0) ++ row(dMax)))
  }

  ignore("Exhaustive All shortest paths from first to last node via top right and bottom left") {
    val query =
      s"""PROFILE MATCH (src:$topLeft), (dst:$bottomRight)
         | MATCH p=(src)-[*]-(dst)
         | WHERE ANY(n in nodes(p) WHERE n:$topRight) AND ANY(n in nodes(p) WHERE n:$bottomLeft)
         | WITH collect(p) as paths
         | WITH reduce(a={l:10000}, p IN paths | case when length(p) > a.l then a when length(p) < a.l then {l:length(p),c:[p]} else {l:a.l, c:a.c +[p]} end).c as all_shortest_paths
         | UNWIND all_shortest_paths as shortest_paths
         | RETURN shortest_paths""".stripMargin
    val startMs = System.currentTimeMillis()
    evaluateAllShortestPathResults(executeUsingCostPlannerOnly(query), "shortest_paths", startMs, 30, Set(row(0) ++ row(1) ++ row(dMax) ++ col(0)))
  }

  // TODO: Fix limitation in shortestPath predicate pull-in
  ignore("Shortest path from first to last node via top right and bottom left") {
    val result = executeWithAllPlanners(
      s"""MATCH p = shortestPath((src:$topLeft)-[*]-(dst:$bottomRight))
        | WHERE ANY(n in nodes(p) WHERE n:$topRight) AND ANY(n in nodes(p) WHERE n:$bottomLeft)
        |RETURN nodes(p) AS nodes""".stripMargin)
      .columnAs[List[Node]]("nodes").toList

    result.length should equal(1)

    println("Got results: " + result(0).sortWith( (a:Node, b:Node) => a.getId < b.getId))
    println("Expect results: " + (row(0) ++ row(dMax)).toList.sortWith( (a:Node, b:Node) => a.getId < b.getId))

    result(0).toSet should contain(row(0) ++ row(dMax))
  }

  /*
   * This test class builds a complex but very regular model that can be easily debugged and visualized.
   * It is an n*n square latice with nodes in every cell position with both labels and properties that
   * identify the cell, row and column. If the latice is 10x10 then the nodeid is also a direction function
   * of the position in the latice, with node[54] meaning node in row 5 and column 4 (zero indexed).
   *
   * A 5x5 matrix will allow exhaustive cypher searches to complete in 30s, and so that is the current
   * default.
   *
   *   (00)-[:RIGHT]->(01)-[:RIGHT]->(02)-[:RIGHT]->(03)-[:RIGHT]->(04)
   *    |              |              |              |              |
   * [:DOWN]        [:DOWN]        [:DOWN]        [:DOWN]        [:DOWN]
   *    |              |              |              |              |
   *    V              V              V              V              V
   *   (05)-[:RIGHT]->(06)-[:RIGHT]->(07)-[:RIGHT]->(08)-[:RIGHT]->(09)
   *    |              |              |              |              |
   * [:DOWN]        [:DOWN]        [:DOWN]        [:DOWN]        [:DOWN]
   *    |              |              |              |              |
   *    V              V              V              V              V
   *   (10)-[:RIGHT]->(11)-[:RIGHT]->(12)-[:RIGHT]->(13)-[:RIGHT]->(14)
   *    |              |              |              |              |
   * [:DOWN]        [:DOWN]        [:DOWN]        [:DOWN]        [:DOWN]
   *    |              |              |              |              |
   *    V              V              V              V              V
   *   (15)-[:RIGHT]->(16)-[:RIGHT]->(17)-[:RIGHT]->(18)-[:RIGHT]->(19)
   *    |              |              |              |              |
   * [:DOWN]        [:DOWN]        [:DOWN]        [:DOWN]        [:DOWN]
   *    |              |              |              |              |
   *    V              V              V              V              V
   *   (20)-[:RIGHT]->(21)-[:RIGHT]->(22)-[:RIGHT]->(23)-[:RIGHT]->(24)
   *
   * All tests are initialized with all horizontal and vertical relationships built. But some tests can
   * also call the addDiagonal method to add a single diagonal for testing specific cases.
   *
   *   (00)--->(01)--->...
   *    | \     | \
   *    |[:DIAG]|  .
   *    |   \   |
   *    V    V  V
   *   (05)--->(06)--->...
   *    |       | \
   *    |       |[:DIAG]
   *    |       |   \
   *    V       V    V
   *    ..      ..    ..
   *
   */
  val dim = 5
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

    val monitors = graph.getDependencyResolver.resolveDependency(classOf[Monitors])
    monitors.addMonitorListener(new DebugDataMonitor)
  }

  private def addDiagonal(): Unit = {
    1 to dMax foreach { cell =>
      val name = s"${cell}${cell}"
      val prev = s"${cell-1}${cell-1}"
      relate(nodesByName(prev), nodesByName(name), "DIAG", s"c${prev}-c${name}")
    }
  }

  private def row(row: Int, min: Int = 0, max: Int = dMax): Set[Node] = {
    val nodes = min to max map { col: Int =>
      nodesByName(s"$row$col")
    }
    nodes.toSet
  }

  private def col(col: Int, min: Int = 0, max: Int = dMax): Set[Node] = {
    val nodes = min to max map { row: Int =>
      nodesByName(s"$row$col")
    }
    nodes.toSet
  }

  private def debugResults(nodes: List[Node]): Unit = {
    println
    val nodeMap: Map[String, Map[Node, Int]] = nodes.foldLeft(Map[String,Map[Node,Int]]()) { (acc, node) =>
      val row = node.getId / dim
      val col = node.getId - dim * row
      val name = s"$row$col"
      acc + (name -> Map(node -> acc.size))
    }
    0 to dMax foreach { row =>
      0 to dMax foreach { col =>
        val name = s"$row$col"
        val text = if (nodeMap.isDefinedAt(name)) nodeMap(name).values.head.toString else "-"
        val toPrint = "  " + text
        print(toPrint.substring(toPrint.length - 3))
      }
      println
    }
    println
  }

  private def evaluateShortestPathResults(results: InternalExecutionResult, startMs: Long, expectedNodes: Set[Node]): Unit = {
    val duration = System.currentTimeMillis() - startMs
    println(results.executionPlanDescription())

    val result = results.columnAs[scala.collection.GenTraversable[Node]]("nodes").toList

    println(s"Query took ${duration/1000.0}s")

    debugResults(result(0).toList)

    println("Got results: " + result(0).toList.sortWith( (a:Node, b:Node) => a.getId < b.getId))
    println("Expect results: " + expectedNodes.toList.sortWith( (a:Node, b:Node) => a.getId < b.getId))

    val expected = expectedNodes
    assert(expected.forall { cell =>
      result(0).toSet.contains(cell)
    })
  }

  private def evaluateAllShortestPathResults(results: InternalExecutionResult, identifier: String, startMs: Long, expectedPathCount: Int, expectedNodes: Set[Set[Node]]): Unit = {
    val resultList = results.toList
    val duration = System.currentTimeMillis() - startMs
    println(results.executionPlanDescription())
    println(s"Query took ${duration / 1000.0}s")
    resultList.length should be(expectedPathCount)
    val matches = resultList.foldLeft(Map[Set[Node],Int]()) { (acc, row) =>
      if (row.isDefinedAt(identifier)) {
        val path: Path = row(identifier).asInstanceOf[Path]
        val nodes: List[Node] = graph.inTx {
          println(path)
          path.nodes().asScala.toList
        }
        nodes.length should be(expectedNodes.head.size)
        debugResults(nodes)
        val nodeSet = nodes.toSet
        if (acc.isDefinedAt(nodeSet))
          acc.updated(nodeSet, acc(nodeSet) + 1)
        else
          acc.updated(nodeSet, 1)
      } else {
        println("No result")
        acc
      }
    }

    val matchCount = matches.keys.foldLeft[Int](0) { (acc, nodeSet) =>
      val count = matches(nodeSet)
      if(count != 1) {
        println(s"Unexpectedly found $count matches for: "+nodeSet)
      }
      // TODO: change 1 to count when bug with allShortestPath repeating results is fixed
      val num = if (expectedNodes.contains(nodeSet)) 1 else 0
      acc + num
    }
    println(s"There were $matchCount results matching: " + expectedNodes)
    matchCount should be (expectedNodes.size)
  }

  def executeUsingRulePlannerOnly(query: String) =
    eengine.execute(s"CYPHER planner=RULE $query") match {
      case e:ExecutionResultWrapperFor3_0 => RewindableExecutionResult(e)
    }

  def executeUsingCostPlannerOnly(query: String) =
    eengine.execute(s"CYPHER planner=COST $query") match {
      case e:ExecutionResultWrapperFor3_0 => RewindableExecutionResult(e)
    }

  private class DebugDataMonitor extends DataMonitor {

    def monitorData(theseVisitedNodes: util.Map[Node, ShortestPath.LevelData], theseNextNodes: util.Collection[Node],
                    thoseVisitedNodes: util.Map[Node, ShortestPath.LevelData], thoseNextNodes: util.Collection[Node]) {
      println("------------------------------------------------------------")
      debug(dim, theseVisitedNodes, theseNextNodes)
      debug(dim, thoseVisitedNodes, thoseNextNodes)
      println
    }

    private def debugNode(dim: Int, matrix: mutable.Map[String, String], cellSize: Int, node: Node,
                          text: String): Int = {
      val row: Long = node.getId / dim
      val col: Long = node.getId - dim * row
      val key: String = row.toString + col.toString
      val value = if (matrix.isDefinedAt(key)) matrix(key) + text else text
      matrix += (key -> value)
      return Math.max(cellSize, value.length)
    }

    def debug(dim: Int, visitedNodes: util.Map[Node, ShortestPath.LevelData],
              nextNodes: util.Collection[Node]) {
      import scala.collection.JavaConversions._
      val matrix: mutable.Map[String, String] = mutable.Map[String, String]()
      var cellSize: Int = 0
      for (entry <- visitedNodes.entrySet) {
        cellSize = debugNode(dim, matrix, cellSize, entry.getKey, "[" + entry.getValue.depth + "]")
      }
      for (node <- nextNodes) {
        cellSize = debugNode(dim, matrix, cellSize, node, "(*)")
      }
      0 until dim foreach { row =>
        print(s"$row:")
        0 until dim foreach { col =>
          val key = s"$row$col"
          val text = matrix.getOrElse(key, "- ")
          printf(s"%${4 + cellSize}s", text)
        }
        println
      }
      println
    }
  }
}
