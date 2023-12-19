/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.internal.cypher.acceptance

import java.io.File
import java.util

import org.neo4j.cypher._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.idp.IDPSolverMonitor
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.{CommunityCompatibilityFactory, ExecutionEngine}
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.graphdb.factory.GraphDatabaseSettings.{cypher_idp_solver_duration_threshold, cypher_idp_solver_table_threshold}
import org.neo4j.kernel.monitoring
import org.neo4j.kernel.monitoring.Monitors
import org.neo4j.logging.NullLogProvider
import org.neo4j.test.ImpermanentGraphDatabase

import scala.collection.JavaConverters._
import scala.collection.mutable

class MatchLongPatternAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  val VERBOSE = false

  override def databaseConfig(): collection.Map[Setting[_], String] = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.cypher_min_replan_interval -> "0",
    GraphDatabaseSettings.cypher_compiler_tracing -> "true",
    GraphDatabaseSettings.pagecache_memory -> "8M"
  )

  test("changing idp max table size should affect IDP inner loop count") {
    graph.shutdown()
    // GIVEN
    val numberOfPatternRelationships = 13
    val maxTableSizes = Seq(128, 64, 32, 16)

    // WHEN
    val idpInnerIterations = determineIDPLoopSizes(numberOfPatternRelationships,
      cypher_idp_solver_table_threshold, maxTableSizes, Map(cypher_idp_solver_duration_threshold -> "10000"))
    // Added an increased duration to make up for the test running in parallel, should preferably be solved in a different way

    // THEN
    maxTableSizes.slice(0, maxTableSizes.size - 1).foreach { maxTableSize =>
      withClue("Less restricted IDP should use fewer iterations") {
        idpInnerIterations(maxTableSize) should be < idpInnerIterations(maxTableSizes.last)
      }
    }
  }

  test("changing idp iteration duration threshold should affect IDP inner loop count") {
    // GIVEN
    graph.shutdown()
    val numberOfPatternRelationships = 13
    val iterationDurationThresholds = Seq(1000, 500, 10)

    // WHEN
    val idpInnerIterations = determineIDPLoopSizes(numberOfPatternRelationships,
      cypher_idp_solver_duration_threshold, iterationDurationThresholds, Map.empty)

    // THEN
    iterationDurationThresholds.slice(0, iterationDurationThresholds.size - 1).foreach { duration =>
      withClue(s"For duration threshold at $duration: ") {
        idpInnerIterations(duration) should be < idpInnerIterations(iterationDurationThresholds.last)
      }
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
    val result = innerExecuteDeprecated(s"EXPLAIN CYPHER planner=IDP $query", Map.empty)
    val duration = System.currentTimeMillis() - start
    if (VERBOSE) {
      println(result.executionPlanDescription())
      println(s"IDP took ${duration}ms to solve length $numberOfPatternRelationships")
    }

    // THEN
    val plan = result.executionPlanDescription()
    assertMinExpandsAndJoins(plan, Map("expands" -> numberOfPatternRelationships, "joins" -> 1))
    // For length 12 we improved compiler times from tens of minutes down to ~3s, we think this test of 120s is stable on a wide range of computing hardware
    duration should be <= 120000L
  }

  test("should plan a large star relationship pattern") {
    for (numberOfPatternRelationships <- Range(9, 50, 9))
    {
      // GIVEN
      makeStarDataset(numberOfPatternRelationships)

      // WHEN
      val query = makeStarPatternQuery(numberOfPatternRelationships)
      if (VERBOSE) {
        println(s"Running IDP on pattern expression of length $numberOfPatternRelationships")
        println(s"\t$query")
      }
      val start = System.currentTimeMillis()
      val result = innerExecuteDeprecated(s"EXPLAIN CYPHER planner=IDP $query", Map.empty)
      val duration = System.currentTimeMillis() - start
      if (VERBOSE) {
        println(result.executionPlanDescription())
        println(s"IDP took ${duration}ms to solve length $numberOfPatternRelationships")
      }

      // THEN
      val plan = result.executionPlanDescription()
      assertMinExpandsAndJoins(plan, Map("expands" -> numberOfPatternRelationships, "joins" -> 0))
    }
  }

  test("very long pattern expressions should be solvable with multiple planners giving identical results using index lookups, expands and joins") {

    graph.createIndex("Person", "name")

    val planners = Seq("IDP")
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
        val startPlaning = System.currentTimeMillis()
        val resultPlanning = innerExecuteDeprecated(s"EXPLAIN CYPHER planner=$planner $query", Map.empty)
        val durationPlanning = System.currentTimeMillis()-startPlaning
        val plan = resultPlanning.executionPlanDescription()

        // measure query time
        val start = System.currentTimeMillis()
        val result = innerExecuteDeprecated(s"CYPHER planner=$planner $query", Map.empty)
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
        acc :+ Seq(durationPlanning, duration, counts("joins").toLong, resultCount.toLong)
      }
      data + (planner -> times)
    }
    if (VERBOSE) {
      Seq("Compile Time", "Query Time", "Number of Joins in Plan", "Number of Results").zipWithIndex.foreach { pair =>
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

  private def assertMinExpandsAndJoins(plan: InternalPlanDescription, minCounts: Map[String, Int]): Map[String, Int] = {
    val counts = countExpandsAndJoins(plan)
    Seq("expands", "joins").foreach { op =>
      if(VERBOSE) println(s"\t$op\t${counts(op)}")
      counts(op) should be >= minCounts(op)
    }
    counts
  }

  private def countExpandsAndJoins(plan: InternalPlanDescription): Map[String, Int] = {
    def addCounts(map1: Map[String, Int], map2: Map[String, Int]) = map1 ++ map2.map { case (k, v) => k -> (v + map1.getOrElse(k, 0)) }
    def incrCount(map: Map[String, Int], key: String) = addCounts(map, Map(key -> 1))
    def expandsAndJoinsCount(plan: InternalPlanDescription, counts: Map[String, Int]): Map[String, Int] = {
      val c = plan.name match {
        case "NodeHashJoin" => incrCount(counts, "joins")
        case "Expand(All)" => incrCount(counts, "expands")
        case _ => counts
      }
      plan.children.toIndexedSeq.foldLeft(c) { (acc, child) =>
        expandsAndJoinsCount(child, acc)
      }
    }

    expandsAndJoinsCount(plan, Map("expands" -> 0, "joins" -> 0))
  }

  private def determineIDPLoopSizes(numberOfPatternRelationships: Int, configKey: Setting[_], configValues: Seq[Int], additionalConfig: Map[Setting[_], String]): Map[Any, Int] = {
    val query = makeLongPatternQuery(numberOfPatternRelationships)
    if (VERBOSE) println(configKey)
    val idpInnerIterations: mutable.Map[Int, Int] = configValues.foldLeft(mutable.Map.empty[Int, Int]) { (acc, configValue) =>
      val config = databaseConfig() + (configKey -> configValue.toString) ++ additionalConfig
      runWithConfig(config.toSeq: _*) {
        (engine, db) =>
          graph = db
          graphOps = db.getGraphDatabaseService
          eengine = engine
          makeLargeMatrixDataset(100)
          val monitor = TestIDPSolverMonitor()
          val monitors: monitoring.Monitors = graph.getDependencyResolver.resolveDependency(classOf[monitoring.Monitors])
          monitors.addMonitorListener(monitor)
          innerExecuteDeprecated(s"EXPLAIN CYPHER planner=IDP $query", Map.empty)
          acc(configValue) = monitor.maxStartIteration
      }
      acc
    }
    if (VERBOSE) configValues.foreach { configValue =>
      println(s"$configValue\t${idpInnerIterations(configValue)}")
    }
    idpInnerIterations.toMap[Any, Int]
  }

  private def makeLongPatternQuery(numberOfPatternRelationships: Int) =
    (1 to numberOfPatternRelationships).foldLeft("MATCH p = (n0)") { (text, index) =>
      text + s"-[r$index]->(n$index)"
    } + " RETURN p"

  private def makeLargeMatrixDataset(size: Int): Unit = graph.inTx {
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
      if (a > 0) relate(nodes(s"n(${a - 1},$b)"), nodes(s"n($a,$b)"), "KNOWS", s"n(${a - 1},$b-n($a,$b)")
      if (b > 0) relate(nodes(s"n($a,${b - 1})"), nodes(s"n($a,$b)"), "KNOWS", s"n($a,${b - 1}-n($a,$b)")
    }
  }

  private def makeStarDataset(size: Int): Unit = graph.inTx {
    val center = createLabeledNode("Center")

    for (i <- 1 to size) {
      val node = createLabeledNode(s"Label$i")
      relate(center, node, s"REL$i")
    }
  }

  private def makeStarPatternQuery(size: Int): String = {
    val (matchStatement, returnStatement) = (1 to size).foldLeft(("MATCH (c:Center) WITH c LIMIT 1 ", "RETURN c")) {
      (strings, i) => (strings._1 + s"MATCH (c)-[:REL$i]->(n$i:Label$i) ", strings._2 + s", n$i")
    }
    matchStatement + returnStatement
  }

  private def runWithConfig(m: (Setting[_], String)*)(run: (ExecutionEngine, GraphDatabaseCypherService) => Unit): Unit = {
    val config: util.Map[String, String] = m.map {
      case (setting, settingValue) => setting.name() -> settingValue
    }.toMap.asJava

    val graph = new GraphDatabaseCypherService(new ImpermanentGraphDatabase(new File("target/test-data/pattern-acceptance"), config))
    try {
      val monitors = graph.getDependencyResolver.resolveDependency(classOf[Monitors])
      val logProvider = NullLogProvider.getInstance()
      // FIXME: probably both?
      val factory = new CommunityCompatibilityFactory(graph, monitors, logProvider)
      val engine = new ExecutionEngine(graph, logProvider, factory)
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
