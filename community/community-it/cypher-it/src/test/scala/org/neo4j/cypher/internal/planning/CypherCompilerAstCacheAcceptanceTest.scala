/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.planning

import java.time.{Clock, Duration, Instant, ZoneOffset}

import org.neo4j.configuration.{Config, GraphDatabaseSettings}
import org.neo4j.cypher
import org.neo4j.cypher._
import org.neo4j.cypher.internal.QueryCache.ParameterTypeMap
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compiler.{CypherPlannerConfiguration, StatsDivergenceCalculator}
import org.neo4j.cypher.internal.planner.spi.MinimumGraphStatistics.{MIN_NODES_ALL, MIN_NODES_WITH_LABEL}
import org.neo4j.cypher.internal.runtime.interpreted.CSVResources
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.v4_0.util.DummyPosition
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.helpers.collection.Pair
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.logging.AssertableLogProvider.inLog
import org.neo4j.logging.{AssertableLogProvider, Log, NullLog, NullLogProvider}

import scala.collection.Map

class CypherCompilerAstCacheAcceptanceTest extends CypherFunSuite with GraphDatabaseTestSupport {

  private def plannerConfig(queryCacheSize: Int = 128, statsDivergenceThreshold: Double = 0.5,
                            queryPlanTTL: Long = 1000): CypherPlannerConfiguration = {
    CypherPlannerConfiguration(
      queryCacheSize,
      StatsDivergenceCalculator.divergenceNoDecayCalculator(statsDivergenceThreshold, queryPlanTTL),
      useErrorsOverWarnings = false,
      idpMaxTableSize = 128,
      idpIterationDuration = 1000,
      errorIfShortestPathFallbackUsedAtRuntime = false,
      errorIfShortestPathHasCommonNodesAtRuntime = true,
      legacyCsvQuoteEscaping = false,
      csvBufferSize = CSVResources.DEFAULT_BUFFER_SIZE,
      nonIndexedLabelWarningThreshold = 10000L,
      planSystemCommands = false
    )
  }

  private def createCompiler(config: CypherPlannerConfiguration, clock: Clock = Clock.systemUTC(),
                             log: Log = NullLog.getInstance): CypherCurrentCompiler[RuntimeContext] = {
    val planner = CypherPlanner(config,
      clock,
      kernelMonitors,
      log,
      cypher.CypherPlannerOption.default,
      CypherUpdateStrategy.default,
      () => 1,
      compatibilityMode = false)
    createCompiler(planner, log, config)
  }

  private def createCompiler(planner: CypherPlanner, log: Log, config: CypherPlannerConfiguration):
  CypherCurrentCompiler[RuntimeContext] = {
    CypherCurrentCompiler(
      planner,
      CommunityRuntimeFactory.getRuntime(CypherRuntimeOption.default, disallowFallback = true),
      CommunityRuntimeContextManager(log, CypherConfiguration.fromConfig(Config.defaults()).toCypherRuntimeConfiguration),
      kernelMonitors)

  }

  case class CacheCounts(hits: Int = 0, misses: Int = 0, flushes: Int = 0, evicted: Int = 0, recompiled: Int = 0) {
    override def toString = s"hits = $hits, misses = $misses, flushes = $flushes, evicted = $evicted"
  }

  class CacheCounter(var counts: CacheCounts = CacheCounts()) extends CacheTracer[Pair[AnyRef, ParameterTypeMap]] {
    override def queryCacheHit(key: Pair[AnyRef, ParameterTypeMap], metaData: String) {
      counts = counts.copy(hits = counts.hits + 1)
    }

    override def queryCacheMiss(key: Pair[AnyRef, ParameterTypeMap], metaData: String) {
      counts = counts.copy(misses = counts.misses + 1)
    }

    override def queryCacheFlush(sizeBeforeFlush: Long) {
      counts = counts.copy(flushes = counts.flushes + 1)
    }

    override def queryCacheStale(key: Pair[AnyRef, ParameterTypeMap], secondsSincePlan: Int, metaData: String): Unit = {
      counts = counts.copy(evicted = counts.evicted + 1)
    }

    override def queryCacheRecompile(queryKey: Pair[AnyRef, ParameterTypeMap],
                                     metaData: String): Unit = {
      counts = counts.copy(recompiled = counts.recompiled + 1)
    }
  }

  override def databaseConfig(): Map[Setting[_], Object] = Map(GraphDatabaseSettings.cypher_min_replan_interval -> Duration.ZERO)

  var counter: CacheCounter = _
  var compiler: CypherCurrentCompiler[RuntimeContext] = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    counter = new CacheCounter()
    compiler = createCompiler(plannerConfig())
    kernelMonitors.addMonitorListener(counter)
  }

  private def runQuery(query: String,
                       params: scala.Predef.Map[String, AnyRef] = Map.empty,
                       cypherCompiler: Compiler = compiler): Unit = {
    import collection.JavaConverters._

    val preParser = new PreParser(CypherVersion.default,
      CypherPlannerOption.default,
      CypherRuntimeOption.default,
      CypherExpressionEngineOption.default,
      CypherOperatorEngineOption.default,
      CypherInterpretedPipesFallbackOption.default,
      1)

    val preParsedQuery = preParser.preParseQuery(query)

    graph.withTx { tx =>
      val noTracing = CompilationPhaseTracer.NO_TRACING
      val context = graph.transactionalContext(tx, query = query -> params)
      cypherCompiler.compile(preParsedQuery, noTracing, Set.empty, context, ValueUtils.asParameterMapValue(params.asJava))
      context.close()
    }
  }

  test("should monitor cache misses") {
    runQuery("return 42")

    counter.counts should equal(CacheCounts(misses = 1, flushes = 1))
  }

  test("should monitor cache hits") {
    runQuery("return 42")
    runQuery("return 42")

    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1))
  }

  test("Constant values in query should use same plan") {
    runQuery("return 42 AS a")
    runQuery("return 53 AS a")
    runQuery("return 76 AS a")

    counter.counts should equal(CacheCounts(hits = 2, misses = 1, flushes = 1))
  }

  test("should fold to constants and use the same plan") {
    runQuery("return 42 AS a")
    runQuery("return 5 + 3 AS a")
    runQuery("return 5 - 3 AS a")
    runQuery("return 7 / 6 AS a")
    runQuery("return 7 * 6 AS a")

    counter.counts should equal(CacheCounts(hits = 4, misses = 1, flushes = 1))
  }

  test("should keep different cache entries for different literal types") {
    runQuery("WITH 1 as x RETURN x")      // miss
    runQuery("WITH 2 as x RETURN x")      // hit
    runQuery("WITH 1.0 as x RETURN x")    // miss
    runQuery("WITH 2.0 as x RETURN x")    // hit
    runQuery("WITH 'foo' as x RETURN x")  // miss
    runQuery("WITH 'bar' as x RETURN x")  // hit
    runQuery("WITH $p as x RETURN x")    // not enough parameters -> not even miss
    runQuery("WITH $k as x RETURN x")    // not enough parameters -> not even miss
    runQuery("WITH [1,2] as x RETURN x")  // miss
    runQuery("WITH [3] as x RETURN x")    // hit

    counter.counts should equal(CacheCounts(hits = 4, misses = 4, flushes = 1))
  }

  test("should not care about white spaces") {
    runQuery("return 42")
    runQuery("\treturn          42")

    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1))
  }

  test("should cache easily parametrized queries") {
    runQuery("return 42 as result")
    runQuery("return 43 as result")

    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1))
  }

  test("should monitor cache flushes") {
    runQuery("return 42")
    graph.createUniqueConstraint("Person", "id")
    runQuery("return 42")

    counter.counts should equal(CacheCounts(misses = 2, flushes = 2))
  }

  test("should monitor cache remove") {
    // given
    val clock: Clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC)
    counter = new CacheCounter()
    compiler = createCompiler(plannerConfig(queryPlanTTL = 0), clock = clock)
    compiler.kernelMonitors.addMonitorListener(counter)
    val query: String = "match (n:Person:Dog) return n"

    createLabeledNode("Dog")
    (0 until 50).foreach { _ => createLabeledNode("Person") }
    runQuery(query)

    // when
    (0 until 1000).foreach { _ => createLabeledNode("Dog") }
    runQuery(query)

    // then
    counter.counts should equal(CacheCounts(hits = 0, misses = 2, flushes = 1, evicted = 1))
  }

  // This test is only added to communicate that we're aware of this behaviour and consider it
  // acceptable, because divergence in NodesAllCardinality will be very rare in a production system
  // except for the initial population. It would be preferable to not evict here, but that would
  // required changes that are too risky for a patch release.
  test("it's ok to evict query because of total nodes change") {
    // given
    val clock: Clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC)
    counter = new CacheCounter()
    compiler = createCompiler(plannerConfig(queryPlanTTL = 0), clock = clock)
    compiler.kernelMonitors.addMonitorListener(counter)
    val query: String = "MATCH (n:Person) RETURN n"
    (0 until MIN_NODES_ALL).foreach { _ => createNode() }
    createLabeledNode("Person")

    // when
    runQuery(query)

    // then
    counter.counts should equal(CacheCounts(hits = 0, misses = 1, flushes = 1, evicted = 0))

    // when
    // we create enough nodes for NodesAllCardinality to trigger a replan
    (0 until 10 * MIN_NODES_ALL).foreach { _ => createNode() }
    runQuery(query)

    // then
    counter.counts should equal(CacheCounts(hits = 0, misses = 2, flushes = 1, evicted = 1))
  }

  test("should not evict query because of unrelated statistics change") {
    // given
    val clock: Clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC)
    counter = new CacheCounter()
    compiler = createCompiler(plannerConfig(queryPlanTTL = 0), clock = clock)
    compiler.kernelMonitors.addMonitorListener(counter)
    val query: String = "MATCH (n:Person) RETURN n"
    (0 until MIN_NODES_WITH_LABEL * 3).foreach { _ => createLabeledNode("Person") }

    // when
    runQuery(query)

    // then
    counter.counts should equal(CacheCounts(hits = 0, misses = 1, flushes = 1, evicted = 0))

    // when
    // we create enough nodes for NodesLabelCardinality("Dog") to trigger a replan
    // but not NodesAllCardinality or NodesLabelCardinality("Person")
    (0 until MIN_NODES_WITH_LABEL * 3).foreach { _ => createLabeledNode("Dog") }
    runQuery(query)

    // then
    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, evicted = 0))
  }

  test("should log on cache remove") {
    // given
    val logProvider = new AssertableLogProvider()
    val logName = "testlog"
    val clock: Clock = Clock.fixed(Instant.ofEpochMilli(10000L), ZoneOffset.UTC)
    compiler = createCompiler(plannerConfig(queryPlanTTL = 0), clock = clock, log = logProvider.getLog(logName))
    val query: String = "match (n:Person:Dog) return n"

    createLabeledNode("Dog")
    (0 until 50).foreach { _ => createLabeledNode("Person") }
    runQuery(query)

    // when
    (0 until 1000).foreach { _ => createLabeledNode("Dog") }
    runQuery(query)

    // then
    logProvider.assertExactly(
      inLog(logName).debug(s"Discarded stale plan from the plan cache after 0 seconds: $query")
    )
  }

  test("when running queries with debug options - never cache") {
    runQuery("CYPHER debug=foo RETURN 42")
    runQuery("CYPHER debug=foo RETURN 42")

    counter.counts.hits should equal(0)
  }

  test("should not find query in cache with different parameter types") {
    val map1: scala.Predef.Map[String, AnyRef] = scala.Predef.Map("number" -> new Integer(42))
    val map2: scala.Predef.Map[String, AnyRef] = scala.Predef.Map("number" -> "nope")
    runQuery("return $number", params = map1)
    runQuery("return $number", params = map2)

    counter.counts should equal(CacheCounts(hits = 0, misses = 2, flushes = 1))
  }

  test("should find query in cache with same parameter types") {
    val map1: scala.Predef.Map[String, AnyRef] = scala.Predef.Map("number" -> new Integer(42))
    val map2: scala.Predef.Map[String, AnyRef] = scala.Predef.Map("number" -> new Integer(43))
    runQuery("return $number", params = map1)
    runQuery("return $number", params = map2)

    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1))
  }

  test("should find query in cache with same parameter types, ignoring unused parameters") {
    val map1: scala.Predef.Map[String, AnyRef] = scala.Predef.Map("number" -> new Integer(42), "foo" -> "bar")
    val map2: scala.Predef.Map[String, AnyRef] = scala.Predef.Map("number" -> new Integer(43), "bar" -> new Integer(10))
    runQuery("return $number", params = map1)
    runQuery("return $number", params = map2)

    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1))
  }

  test("should clear all compiler library caches") {
    val compilerLibrary = createCompilerLibrary()
    val compilers = CypherVersion.all.map { version =>
      compilerLibrary.selectCompiler(version, CypherPlannerOption.default, CypherRuntimeOption.default, CypherUpdateStrategy.default)
    }

    compilers.foreach { compiler =>
      runQuery("return 42", cypherCompiler = compiler) // Misses
      runQuery("return 42", cypherCompiler = compiler) // Hits
    }

    compilerLibrary.clearCaches()

    compilers.foreach { compiler =>
      runQuery("return 42", cypherCompiler = compiler) // Misses
    }

    counter.counts should equal(CacheCounts(hits = compilers.size, misses = 2 * compilers.size, flushes = 2 * compilers.size))
  }

  private def createCompilerLibrary(): CompilerLibrary = {
    val resolver = graph.getDependencyResolver
    val monitors = kernelMonitors
    val nullLogProvider = NullLogProvider.getInstance
    val config = resolver.resolveDependency(classOf[Config])
    val cypherConfig = CypherConfiguration.fromConfig(config)
    val compilerFactory =
      new CommunityCompilerFactory(graph, monitors, nullLogProvider,
        cypherConfig.toCypherPlannerConfiguration(config, planSystemCommands = false), cypherConfig.toCypherRuntimeConfiguration)
    new CompilerLibrary(compilerFactory, () => null)
  }
}
