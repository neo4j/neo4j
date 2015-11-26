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
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription.Arguments.Rows
import org.neo4j.cypher.{NewPlannerTestSupport, ExecutionEngineFunSuite}
import org.neo4j.graphalgo.impl.path.ShortestPath
import org.neo4j.graphalgo.impl.path.ShortestPath.DataMonitor
import org.neo4j.graphdb.{Path, Node}
import org.neo4j.kernel.monitoring.Monitors
import org.scalatest.matchers.{MatchResult, Matcher}
import scala.collection.mutable
import scala.collection.JavaConverters._
import java.util

class ShortestPathLongerAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("Shortest path from first to first node via top right (reverts to exhaustive)") {
    val start = System.currentTimeMillis
    val results = executeUsingCostPlannerOnly(
      s"""MATCH p = shortestPath((src:$topLeft)-[*]-(dst:$topLeft))
        | WHERE ANY(n in nodes(p) WHERE n:$topRight)
        |RETURN nodes(p) AS nodes""".stripMargin)

    println(s"Query took ${(System.currentTimeMillis - start)/1000.0}s")
    println(results.executionPlanDescription())

    val result = results.columnAs[List[Node]]("nodes").toList
    debugResults(result(0).asJava.asScala.toList)

    // Then
    result.length should equal(1)
    result(0).toSet should equal(row(0) ++ row(1))
    results should use("ShortestPathVarLengthExpand")
  }

  test("Shortest path from first to last node via top right") {
    val start = System.currentTimeMillis
    val results = executeWithAllPlanners(
      s"""MATCH p = shortestPath((src:$topLeft)-[*]-(dst:$bottomRight))
        | WHERE ANY(n in nodes(p) WHERE n:$topRight)
        |RETURN nodes(p) AS nodes""".stripMargin)

    println(s"Query took ${(System.currentTimeMillis - start)/1000.0}s")

    val result = results.columnAs[List[Node]]("nodes").toList
    debugResults(result(0))

    // Then
    result.length should equal(1)
    result(0).toSet should equal(row(0) ++ col(dMax))
    results should use("ShortestPathVarLengthExpand")
  }

  test("Shortest path from first to last node via bottom left") {
    val start = System.currentTimeMillis
    val result = executeWithAllPlanners(
      s"""MATCH p = shortestPath((src:$topLeft)-[*]-(dst:$bottomRight))
        | WHERE ANY(n in nodes(p) WHERE n:$bottomLeft)
        |RETURN nodes(p) AS nodes""".stripMargin)
      .columnAs[List[Node]]("nodes").toList

    println(s"Query took ${(System.currentTimeMillis - start)/1000.0}s")
    debugResults(result(0))

    // Then
    result.length should equal(1)
    result(0).toSet should equal(col(0) ++ row(dMax))
  }

  test("Shortest path from first to last node via top right and bottom left (reverts to exhaustive)") {
    val start = System.currentTimeMillis
    val query =
      s"""MATCH p = shortestPath((src:$topLeft)-[*]-(dst:$bottomRight))
         | WHERE ANY(n in nodes(p) WHERE n:$topRight) AND ANY(n in nodes(p) WHERE n:$bottomLeft)
         |RETURN nodes(p) AS nodes""".stripMargin

    evaluateShortestPathResults(executeUsingCostPlannerOnly(query), start, dim * 4 - 3, row(0) ++ row(dMax))
  }

  test("Shortest path from first to last node via middle (reverts to exhaustive)") {
    val start = System.currentTimeMillis
    val query =
      s"""MATCH p = shortestPath((src:$topLeft)-[*]-(dst:$bottomRight))
         | WHERE ANY(n in nodes(p) WHERE n:$middle)
         |RETURN nodes(p) AS nodes""".stripMargin

    evaluateShortestPathResults(executeUsingCostPlannerOnly(query), start, dim * 2 - 1, Set(nodesByName(s"${dMax / 2}${dMax / 2}")))
  }

  test("Exhaustive shortest path from first to last node via top right") {
    val query =
      s"""PROFILE MATCH p=(src:$topLeft)-[*]-(dst:$bottomRight)
         | WHERE ANY(n in nodes(p) WHERE n:$topRight)
         |RETURN nodes(p) AS nodes ORDER BY length(p) ASC LIMIT 1""".stripMargin
    val startOne = System.currentTimeMillis()
    evaluateShortestPathResults(executeUsingCostPlannerOnly(query), startOne, dim * 2 - 1, row(0) ++ col(dMax))
  }

  test("Exhaustive shortest path from first to last node via bottom left") {
    val query =
      s"""PROFILE MATCH p=(src:$topLeft)-[*]-(dst:$bottomRight)
         | WHERE ANY(n in nodes(p) WHERE n:$bottomLeft)
         | RETURN nodes(p) AS nodes ORDER BY length(p) ASC LIMIT 1""".stripMargin
    val startOne = System.currentTimeMillis()
    evaluateShortestPathResults(executeUsingCostPlannerOnly(query), startOne, dim * 2 - 1, col(0) ++ row(dMax))
  }

  test("Exhaustive shortest path from first to last node via top right and bottom left") {
    val query =
      s"""PROFILE MATCH p=(src:$topLeft)-[*]-(dst:$bottomRight)
         | WHERE ANY(n in nodes(p) WHERE n:$topRight) AND ANY(n in nodes(p) WHERE n:$bottomLeft)
         | RETURN nodes(p) AS nodes ORDER BY length(p) ASC LIMIT 1""".stripMargin
    val startOne = System.currentTimeMillis()
    evaluateShortestPathResults(executeUsingCostPlannerOnly(query), startOne, dim * 4 - 3, row(0) ++ row(dMax))
  }

  test("Exhaustive shortest path from first to last node via middle") {
    val query =
      s"""PROFILE MATCH p=(src:$topLeft)-[*]-(dst:$bottomRight)
         | WHERE ANY(n in nodes(p) WHERE n:$middle)
         | RETURN nodes(p) AS nodes ORDER BY length(p) ASC LIMIT 1""".stripMargin
    val startOne = System.currentTimeMillis()
    evaluateShortestPathResults(executeUsingCostPlannerOnly(query), startOne, dim * 2 - 1, Set(nodesByName(s"${dMax / 2}${dMax / 2}")))
  }

  test("All shortest paths from first to last node") {
    val start = System.currentTimeMillis
    val result = executeUsingCostPlannerOnly(
      s"""MATCH p = allShortestPaths((src:$topLeft)-[*]-(dst:$bottomRight))
         | RETURN p""".stripMargin)

    val expectedLength = Map(3 -> 6, 4 -> 20, 5 -> 70)
    evaluateAllShortestPathResults(result, "p", start, expectedLength(dim), Set(col(0) ++ row(dMax), row(0) ++ col(dMax)))
  }

  test("All shortest paths from first to last node via bottom left") {
    val start = System.currentTimeMillis
    val result = executeUsingCostPlannerOnly(
      s"""MATCH p = allShortestPaths((src:$topLeft)-[*]-(dst:$bottomRight))
         | WHERE ANY(n in nodes(p) WHERE n:$bottomLeft)
         | RETURN p""".stripMargin)

    evaluateAllShortestPathResults(result, "p", start, 1, Set(col(0) ++ row(dMax)))
  }

  test("All shortest paths from first to last node via top right") {
    val start = System.currentTimeMillis
    val result = executeUsingCostPlannerOnly(
      s"""MATCH p = allShortestPaths((src:$topLeft)-[*]-(dst:$bottomRight))
         | WHERE ANY(n in nodes(p) WHERE n:$topRight)
         | RETURN p""".stripMargin)

    evaluateAllShortestPathResults(result, "p", start, 1, Set(row(0) ++ col(dMax)))
  }

  test("All shortest paths from first to last node via middle") {
    val start = System.currentTimeMillis
    val result = executeUsingCostPlannerOnly(
      s"""MATCH p = allShortestPaths((src:$topLeft)-[*]-(dst:$bottomRight))
         | WHERE ANY(n in nodes(p) WHERE n:$middle)
         | RETURN p""".stripMargin)

    val expectedLength = Map(3 -> 4, 4 -> 12, 5 -> 36)
    evaluateAllShortestPathResults(result, "p", start, expectedLength(dim), Set(
      row(0, min = 0, max = dMax / 2) ++ col(dMax / 2) ++ row(dMax, min = dMax / 2, max = dMax),
      col(0, min = 0, max = dMax / 2) ++ row(dMax / 2) ++ col(dMax, min = dMax / 2, max = dMax)
    ))
  }

  // TODO: Support TOP by group in fallback
  ignore("All shortest paths from first to last node via top right and bottom left (needs to be with fallback)") {
    val start = System.currentTimeMillis
    val result = executeUsingCostPlannerOnly(
      s"""MATCH p = allShortestPaths((src:$topLeft)-[*]-(dst:$bottomRight))
         | WHERE ANY(n in nodes(p) WHERE n:$topRight) AND ANY(n in nodes(p) WHERE n:$bottomLeft)
         | RETURN p""".stripMargin)

    val expectedLength = Map(3 -> 2, 4 -> 8, 5 -> 30)
    evaluateAllShortestPathResults(result, "p", start, expectedLength(dim), Set(
      row(0, min = 0, max = dMax / 2) ++ col(dMax / 2) ++ row(dMax, min = dMax / 2, max = dMax),
      col(0, min = 0, max = dMax / 2) ++ row(dMax / 2) ++ col(dMax, min = dMax / 2, max = dMax)
    ))
    result should useShortestPathVarLengthWithAtLeast(rows = 1)
  }

  test("Exhaustive All shortest paths from first to last node") {
    val query =
      s"""PROFILE MATCH (src:$topLeft), (dst:$bottomRight)
         | MATCH p=(src)-[*]-(dst)
         | WITH collect(p) as paths
         | WITH reduce(a={l:10000}, p IN paths | case when length(p) > a.l then a when length(p) < a.l then {l:length(p),c:[p]} else {l:a.l, c:a.c +[p]} end).c as all_shortest_paths
         | UNWIND all_shortest_paths as shortest_paths
         | RETURN shortest_paths""".stripMargin
    val startMs = System.currentTimeMillis()
    val expectedLength = Map(3 -> 6, 4 -> 20, 5 -> 70)
    evaluateAllShortestPathResults(executeUsingCostPlannerOnly(query), "shortest_paths", startMs, expectedLength(dim), Set(col(0) ++ row(dMax)))
  }

  test("Exhaustive All shortest paths from first to last node via bottom left") {
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

  test("Exhaustive All shortest paths from first to last node via top right") {
    val query =
      s"""PROFILE MATCH (src:$topLeft), (dst:$bottomRight)
         | MATCH p=(src)-[*]-(dst)
         | WHERE ANY(n in nodes(p) WHERE n:$topRight)
         | WITH collect(p) as paths
         | WITH reduce(a={l:10000}, p IN paths | case when length(p) > a.l then a when length(p) < a.l then {l:length(p),c:[p]} else {l:a.l, c:a.c +[p]} end).c as all_shortest_paths
         | UNWIND all_shortest_paths as shortest_paths
         | RETURN shortest_paths""".stripMargin
    val startMs = System.currentTimeMillis()
    evaluateAllShortestPathResults(executeUsingCostPlannerOnly(query), "shortest_paths", startMs, 1, Set(row(0) ++ col(dMax)))
  }

  test("Exhaustive All shortest paths from first to last node via middle") {
    val query =
      s"""PROFILE MATCH (src:$topLeft), (dst:$bottomRight)
         | MATCH p=(src)-[*]-(dst)
         | WHERE ANY(n in nodes(p) WHERE n:$middle)
         | WITH collect(p) as paths
         | WITH reduce(a={l:10000}, p IN paths | case when length(p) > a.l then a when length(p) < a.l then {l:length(p),c:[p]} else {l:a.l, c:a.c +[p]} end).c as all_shortest_paths
         | UNWIND all_shortest_paths as shortest_paths
         | RETURN shortest_paths""".stripMargin
    val startMs = System.currentTimeMillis()
    val expectedLength = Map(3 -> 4, 4 -> 12, 5 -> 36)
    evaluateAllShortestPathResults(executeUsingCostPlannerOnly(query), "shortest_paths", startMs, expectedLength(dim), Set(
      row(0, min = 0, max = dMax / 2) ++ col(dMax / 2) ++ row(dMax, min = dMax / 2, max = dMax),
      col(0, min = 0, max = dMax / 2) ++ row(dMax / 2) ++ col(dMax, min = dMax / 2, max = dMax)
    ))
  }

  test("Exhaustive All shortest paths from first to last node via top right and bottom left") {
    val query =
      s"""PROFILE MATCH (src:$topLeft), (dst:$bottomRight)
         | MATCH p=(src)-[*]-(dst)
         | WHERE ANY(n in nodes(p) WHERE n:$topRight) AND ANY(n in nodes(p) WHERE n:$bottomLeft)
         | WITH collect(p) as paths
         | WITH reduce(a={l:10000}, p IN paths | case when length(p) > a.l then a when length(p) < a.l then {l:length(p),c:[p]} else {l:a.l, c:a.c +[p]} end).c as all_shortest_paths
         | UNWIND all_shortest_paths as shortest_paths
         | RETURN shortest_paths""".stripMargin
    val startMs = System.currentTimeMillis()
    val expectedLength = Map(3 -> 2, 4 -> 8, 5 -> 30)
    evaluateAllShortestPathResults(executeUsingCostPlannerOnly(query), "shortest_paths", startMs, expectedLength(dim), Set(
      row(0) ++ row(1) ++ row(dMax) ++ col(0),
      col(0) ++ col(1) ++ col(dMax) ++ row(0)
    ))
  }

  //---------------------------------------------------------------------------
  // Negative tests
  test("Shortest path from first to last node without predicate") {
    val start = System.currentTimeMillis
    val results = executeUsingCostPlannerOnly(
      s"""MATCH p = shortestPath((src:$topLeft)-[*]-(dst:$bottomRight))
          |RETURN nodes(p) AS nodes""".stripMargin)

    val result = results.columnAs[List[Node]]("nodes").toList
    result.length should equal(1)
    result(0).length should equal (2 * dim - 1)
    results shouldNot use("ShortestPathVarLengthExpand")
  }

  test("Shortest path from first to last node with ALL predicate") {
    addDiagonal()
    val start = System.currentTimeMillis
    val results = executeUsingCostPlannerOnly(
      s"""MATCH p = shortestPath((src:$topLeft)-[*]-(dst:$bottomRight))
          | WHERE ALL(r in rels(p) WHERE type(r) = 'DIAG')
          | RETURN nodes(p) AS nodes""".stripMargin)

    val result = results.columnAs[List[Node]]("nodes").toList
    result.length should equal(1)
    result(0).toSet should equal (diag())
    results shouldNot use("ShortestPathVarLengthExpand")
  }

  test("Shortest path from first to last node with NONE predicate") {
    val start = System.currentTimeMillis
    val results = executeUsingCostPlannerOnly(
      s"""PROFILE MATCH p = shortestPath((src:$topLeft)-[*]-(dst:$bottomRight))
          | WHERE NONE(r in rels(p) WHERE exists(r.blocked))
          | RETURN nodes(p) AS nodes""".stripMargin)

    val result = results.columnAs[List[Node]]("nodes").toList
    result.length should equal(1)
    result(0).length should equal (2 * dim - 1)
    results shouldNot use("ShortestPathVarLengthExpand")
  }

  test("Shortest path from first to last node with NONE predicate with a composite predicate") {
    val start = System.currentTimeMillis
    val results = executeUsingCostPlannerOnly(
      s"""PROFILE MATCH p = shortestPath((src:$topLeft)-[*]-(dst:$bottomRight))
          | WHERE NONE(r in rels(p) WHERE exists(r.blocked) AND src:$bottomLeft) AND src:$topLeft
          | RETURN nodes(p) AS nodes""".stripMargin)

    val result = results.columnAs[List[Node]]("nodes").toList
    result.length should equal(1)
    result(0).length should equal (2 * dim - 1)
    results shouldNot use("ShortestPathVarLengthExpand")
  }

  test("Shortest path from first to last node with path length predicate") {
    addDiagonal()
    val start = System.currentTimeMillis
    val results = executeUsingCostPlannerOnly(
      // This predicate dictates that we cannot use the entire diagonal, we need to make one side-step
      s"""PROFILE MATCH p = shortestPath((src:$topLeft)-[*]-(dst:$bottomRight))
          | WHERE length(p) >= $dim
          | RETURN nodes(p) AS nodes""".stripMargin)

    val result = results.columnAs[List[Node]]("nodes").toList
    result.length should equal(1)
    debugResults(result(0))
    println(results.executionPlanDescription())
    result(0).length should equal (dim + 1)
    results should use("ShortestPathVarLengthExpand")
  }

  test("Shortest path from first to last node with ALL node predicate") {
    addDiagonal()
    val start = System.currentTimeMillis
    val results = executeUsingCostPlannerOnly(
      s"""PROFILE MATCH p = shortestPath((src:$topLeft)-[*]-(dst:$bottomRight))
          | WHERE ALL(n in nodes(p) WHERE n.row = 0 OR n.col = $dMax)
          | RETURN nodes(p) AS nodes""".stripMargin)

    val result = results.columnAs[List[Node]]("nodes").toList
    result.length should equal(1)
    result(0).toSet should equal (row(0) ++ col(dMax))
    // TODO: Stop using fallback once node predicates are supported in expander
    results should use("ShortestPathVarLengthExpand")
  }

  test("Shortest path from first to last node with NONE node predicate") {
    addDiagonal()
    val start = System.currentTimeMillis
    val results = executeUsingCostPlannerOnly(
      s"""PROFILE MATCH p = shortestPath((src:$topLeft)-[*]-(dst:$bottomRight))
          | WHERE NONE(n in nodes(p) WHERE n.row > 0 AND n.col < $dMax)
          | RETURN nodes(p) AS nodes""".stripMargin)

    val result = results.columnAs[List[Node]]("nodes").toList
    result.length should equal(1)
    result(0).toSet should equal (row(0) ++ col(dMax))
    // TODO: Stop using fallback once node predicates are supported in expander
    results should use("ShortestPathVarLengthExpand")
  }

  def useShortestPathVarLengthWithAtLeast(rows: Int): Matcher[InternalExecutionResult] = new Matcher[InternalExecutionResult] {
    override def apply(result: InternalExecutionResult): MatchResult = {
      val plan: InternalPlanDescription = result.executionPlanDescription()
      val operators = plan.find("ShortestPathVarLengthExpand")
      if (operators.isEmpty) {
        MatchResult(
          matches = false,
          rawFailureMessage = "Plan should use ShortestPathVarLengthExpand",
          rawNegatedFailureMessage = "Plan should use ShortestPathVarLengthExpand")
      } else {
        val rowCount: Seq[Long] = operators.flatMap(o => o.arguments.collect {
          case Rows(r) => r
        })
        MatchResult(
          matches = rowCount.exists(r => r >= rows),
          rawFailureMessage = s"Plan should use ShortestPathVarLengthExpand with at least ${rows} row(s):\n$plan",
          rawNegatedFailureMessage = s"Plan should use ShortestPathVarLengthExpand with less than ${rows} row(s):\n$plan")
      }
    }
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

  private def diag(min: Int = 0, max: Int = dMax): Set[Node] = {
    val nodes = min to max map { row: Int =>
      nodesByName(s"$row$row")
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

  private def evaluateShortestPathResults(results: InternalExecutionResult, startMs: Long, pathLength: Int, expectedNodes: Set[Node]): Unit = {
    val duration = System.currentTimeMillis() - startMs
    println(results.executionPlanDescription())

    val result = results.columnAs[scala.collection.GenTraversable[Node]]("nodes").toList

    println(s"Query took ${duration/1000.0}s")

    debugResults(result(0).toList)

    result(0).toList.length should equal(pathLength)

    println("Got results: " + result(0).toList.sortWith( (a:Node, b:Node) => a.getId < b.getId))
    println("Expect results: " + expectedNodes.toList.sortWith( (a:Node, b:Node) => a.getId < b.getId))

    assert(expectedNodes.forall { cell =>
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
                    thoseVisitedNodes: util.Map[Node, ShortestPath.LevelData], thoseNextNodes: util.Collection[Node],
                    connectingNode: Node) {
      println("------------------------------------------------------------")
      debug(dim, theseVisitedNodes, theseNextNodes, connectingNode)
      debug(dim, thoseVisitedNodes, thoseNextNodes, connectingNode)
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
              nextNodes: util.Collection[Node], connectingNode: Node) {
      import scala.collection.JavaConversions._
      val matrix: mutable.Map[String, String] = mutable.Map[String, String]()
      var cellSize: Int = 0
      for (entry <- visitedNodes.entrySet) {
        cellSize = debugNode(dim, matrix, cellSize, entry.getKey, entry.getKey.getId.toString + "[" + entry.getValue.depth + "]")
      }
      for (node <- nextNodes) {
        cellSize = debugNode(dim, matrix, cellSize, node, if (node == connectingNode) "(@)" else "(*)")
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
