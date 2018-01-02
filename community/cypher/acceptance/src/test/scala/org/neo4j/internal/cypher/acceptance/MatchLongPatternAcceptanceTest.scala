/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util

import org.neo4j.cypher._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp.IDPSolverMonitor
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.{monitoring, GraphDatabaseAPI}
import org.neo4j.test.ImpermanentGraphDatabase
import scala.collection.mutable
import scala.collection.JavaConverters._

class MatchLongPatternAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  val VERBOSE = false

  override def databaseConfig() = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.cypher_min_replan_interval.name -> "0",
    GraphDatabaseSettings.cypher_compiler_tracing.name -> "true",
    GraphDatabaseSettings.pagecache_memory.name -> "8M"
  )

  test("changing idp max table size should affect IDP inner loop count") {
    // GIVEN
    val numberOfPatternRelationships = 13
    val maxTableSizes = Seq(128, 64, 32, 16)

    // WHEN
    val idpInnerIterations = determineIDPLoopSizes(numberOfPatternRelationships,
      GraphDatabaseSettings.cypher_idp_solver_table_threshold.name, maxTableSizes)

    // THEN
    maxTableSizes.slice(0, maxTableSizes.size - 1).foreach { (maxTableSize) =>
      idpInnerIterations(maxTableSize) should be < idpInnerIterations(maxTableSizes.last)
    }
  }

  test("changing idp iteration duration threshold should affect IDP inner loop count") {
    // GIVEN
    val numberOfPatternRelationships = 13
    val iterationDurationThresholds = Seq(1000, 500, 100, 10)

    // WHEN
    val idpInnerIterations = determineIDPLoopSizes(numberOfPatternRelationships,
      GraphDatabaseSettings.cypher_idp_solver_duration_threshold.name, iterationDurationThresholds)

    // THEN
    iterationDurationThresholds.slice(0, iterationDurationThresholds.size - 1).foreach { (duration) =>
      idpInnerIterations(duration) should be < idpInnerIterations(iterationDurationThresholds.last)
    }
  }

  test("should plan a very long relationship pattern without combinatorial explosion") {
    // GIVEN
    makeLargeMatrixDataset(100)

    // WHEN
    val numberOfPatternRelationships = 20
    val query = makeLongPatternQuery(numberOfPatternRelationships)
    if (VERBOSE) {
      println(s"Running IDP on pattern expression of length $numberOfPatternRelationships")
      println(s"\t$query")
    }
    val start = System.currentTimeMillis()
    val result = eengine.execute(s"EXPLAIN CYPHER planner=IDP $query")
    val duration = System.currentTimeMillis() - start
    if (VERBOSE) {
      println(result.executionPlanDescription())
      println(s"IDP took ${duration}ms to solve length $numberOfPatternRelationships")
    }

    // THEN
    val plan = result.executionPlanDescription()
    assertMinExpandsAndJoins(plan, Map("expands" -> numberOfPatternRelationships, "joins" -> 1))
    // For length 12 we improved compiler times from tens of minutes down to ~3s, we think this test of 30s is stable on a wide range of computing hardware
    duration should be <= 30000L
  }

  test("very long pattern expressions should be solvable with multiple planners giving identical results using index lookups, expands and joins") {

    graph.createIndex("Person", "name")

    val planners = Seq("RULE", "GREEDY", "IDP")
    val minPathLength = 8
    val maxPathLength = 15
    makeLargeMatrixDataset(maxPathLength + 100)

    val indexStep = 5
    val results = planners.foldLeft(mutable.Map.empty[String,Seq[Seq[Long]]]) { (data: mutable.Map[String, Seq[Seq[Long]]], planner: String) =>
      val times = (minPathLength to maxPathLength).foldLeft(Seq.empty[Seq[Long]]) { (acc,pathlen) =>
        val query = (1 to pathlen).foldLeft("MATCH p = (s:Person {name:'n(0,0)'})") { (text, index) =>
          text + (if(index % indexStep == 0) s"-->(c$index:Person {name:'n(0,$index)'})" else s"-->(c$index)")
        } + " RETURN p"
        if(VERBOSE) println("QUERY: " + query)

        // measure planning time
        var startPlaning = System.currentTimeMillis()
        val resultPlanning = eengine.execute(s"EXPLAIN CYPHER planner=$planner $query")
        val durationPlanning = System.currentTimeMillis()-startPlaning
        val plan = resultPlanning.executionPlanDescription()

        // measure query time
        var start = System.currentTimeMillis()
        val result = eengine.execute(s"CYPHER planner=$planner $query")
        val resultCount = result.toList.length
        val duration = System.currentTimeMillis()-start
        val expectedResultCount = Math.pow(2, pathlen % indexStep).toInt
        resultCount should equal(expectedResultCount)

        if(VERBOSE) println(s"$planner took ${durationPlanning}ms to solve length $pathlen and ${duration}ms to run query (got $resultCount results)")
        val minCounts = Map(
          "expands" -> (if (planner == "RULE") 0 else pathlen),
          "joins" -> (if(planner == "IDP") pathlen / 15 else 0)
        )
        val counts = assertMinExpandsAndJoins(plan, minCounts)
        acc :+ Seq(durationPlanning,duration,counts("joins").toLong,resultCount.toLong)
      }
      data + (planner -> times)
    }
    if (VERBOSE) {
      Seq("Compile Time", "Query Time", "Number of Joins in Plan", "Number of Results").zipWithIndex.foreach { (pair) =>
        val name = pair._1
        val index = pair._2
        println(s"\n$name\n")
        println(planners.mkString("\t"))
        for (elem <- minPathLength to maxPathLength) {
          val times = planners.map(planner => results(planner)(elem - minPathLength))
          print(s"$elem\t")
          println(times.map(_(index)).mkString("\t"))
        }
      }
    }
  }

  private def assertMinExpandsAndJoins(plan: PlanDescription, minCounts: Map[String, Int]) = {
    val counts = countExpandsAndJoins(plan)
    Seq("expands", "joins").foreach { op =>
      if(VERBOSE) println(s"\t$op\t${counts(op)}")
      counts(op) should be >= minCounts(op)
    }
    counts
  }

  private def countExpandsAndJoins(plan: PlanDescription) = {
    def addCounts(map1: Map[String, Int], map2: Map[String, Int]) = map1 ++ map2.map { case (k, v) => k -> (v + map1.getOrElse(k, 0)) }
    def incrCount(map: Map[String, Int], key: String) = addCounts(map, Map(key -> 1))
    def expandsAndJoinsCount(plan: PlanDescription, counts: Map[String, Int]): Map[String, Int] = {
      val c = plan.name match {
        case "NodeHashJoin" => incrCount(counts, "joins")
        case "Expand(All)" => incrCount(counts, "expands")
        case _ => counts
      }
      plan.children.foldLeft(c) { (acc, child) =>
        expandsAndJoinsCount(child, acc)
      }
    }

    expandsAndJoinsCount(plan, Map("expands" -> 0, "joins" -> 0))
  }

  private def determineIDPLoopSizes(numberOfPatternRelationships: Int, configKey: String, keys: Seq[Int]): Map[Any, Int] = {
    val query = makeLongPatternQuery(numberOfPatternRelationships)
    val idpInnerIterations: mutable.Map[Int, Int] = keys.foldLeft(mutable.Map.empty[Int, Int]) { (acc, configValue) =>
      val config = databaseConfig() + (configKey -> configValue.toString)
      runWithConfig(config.toSeq: _*) {
        (engine, db) =>
          graph = db.asInstanceOf[GraphDatabaseAPI]
          makeLargeMatrixDataset(100)
          val monitor = TestIDPSolverMonitor()
          val monitors: monitoring.Monitors = graph.getDependencyResolver.resolveDependency(classOf[org.neo4j.kernel.monitoring.Monitors])
          monitors.addMonitorListener(monitor)
          val result = engine.execute(s"EXPLAIN CYPHER planner=IDP $query")
          val counts = countExpandsAndJoins(result.executionPlanDescription())
          counts("joins") should be > 1
          counts("joins") should be < numberOfPatternRelationships / 2
          acc(configValue) = monitor.maxStartIteration
      }
      acc
    }
    if (VERBOSE) keys.foreach { (configValue) =>
      println(s"$configValue\t${idpInnerIterations(configValue)}")
    }
    idpInnerIterations.toMap[Any, Int]
  }

  private def makeLongPatternQuery(numberOfPatternRelationships: Int) =
    (1 to numberOfPatternRelationships).foldLeft("MATCH p = (n0)") { (text, index) =>
      text + s"-[r$index]->(n$index)"
    } + " RETURN p"

  private def makeLargeMatrixDataset(size: Int): Unit = {
    val nodes = (for (
      a <- 0 to size;
      b <- 0 to size
    ) yield {
      val name = s"n($a,$b)"
      name -> createLabeledNode(Map("name" -> name), "Person")
    }).toMap
    for (
      a <- 0 to size;
      b <- 0 to size
    ) yield {
      if (a > 0) relate(nodes(s"n(${a - 1},${b})"), nodes(s"n(${a},${b})"), "KNOWS", s"n(${a - 1},${b}-n(${a},${b})")
      if (b > 0) relate(nodes(s"n(${a},${b - 1})"), nodes(s"n(${a},${b})"), "KNOWS", s"n(${a},${b - 1}-n(${a},${b})")
    }
  }

  private def runWithConfig(m: (String, String)*)(run: (ExecutionEngine, GraphDatabaseService) => Unit) = {
    val config: util.Map[String, String] = m.toMap.asJava

    val graph = new ImpermanentGraphDatabase(config)
    try {
      val engine = new ExecutionEngine(graph)
      run(engine, graph)
    } finally {
      graph.shutdown()
    }
  }

  case class TestIDPSolverMonitor() extends IDPSolverMonitor {
    var maxStartIteration = 0
    var foundPlanIteration = 0

    override def startIteration(iteration: Int): Unit = maxStartIteration = iteration

    override def foundPlanAfter(iterations: Int): Unit = foundPlanIteration = iterations

    override def endIteration(iteration: Int, depth: Int, tableSize: Int): Unit = {}
  }
}
