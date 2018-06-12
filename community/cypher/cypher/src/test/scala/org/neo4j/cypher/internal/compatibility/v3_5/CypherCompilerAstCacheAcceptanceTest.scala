/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_5

import java.time.{Clock, Instant, ZoneOffset}

import org.neo4j.cypher
import org.neo4j.cypher._
import org.neo4j.cypher.internal.compatibility.{CommunityRuntimeContextCreator, CypherCurrentCompiler, RuntimeContext}
import org.neo4j.cypher.internal.compiler.v3_5._
import org.neo4j.cypher.internal.runtime.interpreted.CSVResources
import org.neo4j.cypher.internal.{CacheTracer, CommunityRuntimeFactory, PreParsedQuery}
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.logging.AssertableLogProvider.inLog
import org.neo4j.logging.{AssertableLogProvider, Log, NullLog}
import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.frontend.phases.CompilationPhaseTracer
import org.opencypher.v9_0.util.DummyPosition
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

import scala.collection.Map

class CypherCompilerAstCacheAcceptanceTest extends CypherFunSuite with GraphDatabaseTestSupport {

  def createCompiler(queryCacheSize: Int = 128, statsDivergenceThreshold: Double = 0.5, queryPlanTTL: Long = 1000,
                     clock: Clock = Clock.systemUTC(), log: Log = NullLog.getInstance):
  CypherCurrentCompiler[RuntimeContext] = {

    val config = CypherPlannerConfiguration(
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
      planWithMinimumCardinalityEstimates = true
    )
    CypherCurrentCompiler(
      Cypher35Planner(config,
                      clock,
                      kernelMonitors,
                      log,
                      cypher.CypherPlannerOption.default,
                      CypherUpdateStrategy.default,
                      () => 1),
      CommunityRuntimeFactory.getRuntime(CypherRuntimeOption.default, disallowFallback = true),
      CommunityRuntimeContextCreator,
      kernelMonitors)

  }

  case class CacheCounts(hits: Int = 0, misses: Int = 0, flushes: Int = 0, evicted: Int = 0) {
    override def toString = s"hits = $hits, misses = $misses, flushes = $flushes, evicted = $evicted"
  }

  class CacheCounter(var counts: CacheCounts = CacheCounts()) extends CacheTracer[Statement] {
    override def queryCacheHit(key: Statement, metaData: String) {
      counts = counts.copy(hits = counts.hits + 1)
    }

    override def queryCacheMiss(key: Statement, metaData: String) {
      counts = counts.copy(misses = counts.misses + 1)
    }

    override def queryCacheFlush(sizeBeforeFlush: Long) {
      counts = counts.copy(flushes = counts.flushes + 1)
    }

    override def queryCacheStale(key: Statement, secondsSincePlan: Int, metaData: String): Unit = {
      counts = counts.copy(evicted = counts.evicted + 1)
    }
  }

  override def databaseConfig(): Map[Setting[_], String] = Map(GraphDatabaseSettings.cypher_min_replan_interval -> "0")

  var counter: CacheCounter = _
  var compiler: CypherCurrentCompiler[RuntimeContext] = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    counter = new CacheCounter()
    compiler = createCompiler()
    compiler.kernelMonitors.addMonitorListener(counter)
  }

  private def runQuery(query: String, debugOptions: Set[String] = Set.empty): Unit = {
    graph.withTx { tx =>
      val noTracing = CompilationPhaseTracer.NO_TRACING
      val context = graph.transactionalContext(query = query -> Map.empty)
      compiler.compile(PreParsedQuery(query, DummyPosition(0), query,
                                      isPeriodicCommit = false,
                                      CypherVersion.default,
                                      CypherExecutionMode.default,
                                      CypherPlannerOption.default,
                                      CypherRuntimeOption.default,
                                      CypherUpdateStrategy.default,
                                      debugOptions),
                                  noTracing, Set.empty, context)
      context.close(true)
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
    runQuery("WITH {p} as x RETURN x")    // miss
    runQuery("WITH {k} as x RETURN x")    // miss, a little surprising but not harmful
    runQuery("WITH [1,2] as x RETURN x")  // miss
    runQuery("WITH [3] as x RETURN x")    // hit

    counter.counts should equal(CacheCounts(hits = 4, misses = 6, flushes = 1))
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
    graph.createConstraint("Person", "id")
    runQuery("return 42")

    counter.counts should equal(CacheCounts(misses = 2, flushes = 2))
  }

  test("should monitor cache remove") {
    // given
    val clock: Clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC)
    counter = new CacheCounter()
    compiler = createCompiler(queryPlanTTL = 0, clock = clock)
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

  test("should not evict query because of unrelated statistics change") {
    // given
    val clock: Clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC)
    counter = new CacheCounter()
    compiler = createCompiler(queryPlanTTL = 0, clock = clock)
    compiler.kernelMonitors.addMonitorListener(counter)
    val query: String = "match (n:Person) return n"

    // when
    runQuery(query)

    // then
    counter.counts should equal(CacheCounts(hits = 0, misses = 1, flushes = 1, evicted = 0))

    // when
    (0 until 5).foreach { _ => createLabeledNode("Dog") }
    runQuery(query)

    // then
    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, evicted = 0))
  }

  test("should log on cache remove") {
    // given
    val logProvider = new AssertableLogProvider()
    val logName = "testlog"
    val clock: Clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC)
    compiler = createCompiler(queryPlanTTL = 0, clock = clock, log = logProvider.getLog(logName))
    val query: String = "match (n:Person:Dog) return n"

    createLabeledNode("Dog")
    (0 until 50).foreach { _ => createLabeledNode("Person") }
    runQuery(query)

    // when
    (0 until 1000).foreach { _ => createLabeledNode("Dog") }
    runQuery(query)

    // then
    logProvider.assertExactly(
      inLog(logName).info( s"Discarded stale query from the query cache after 0 seconds: $query" )
    )
  }

  test("when running queries with debug options - never cache") {
    runQuery("return 42", Set("debug"))
    runQuery("return 42", Set("debug"))

    counter.counts.hits should equal(0)
  }
}
