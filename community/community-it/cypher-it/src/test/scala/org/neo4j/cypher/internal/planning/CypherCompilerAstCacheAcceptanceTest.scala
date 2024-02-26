/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.CacheCounts
import org.neo4j.cypher.ExecutionEngineHelper.asJavaMapDeep
import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.cypher.internal.CacheTracer
import org.neo4j.cypher.internal.CommunityCompilerFactory
import org.neo4j.cypher.internal.CommunityRuntimeContextManager
import org.neo4j.cypher.internal.CommunityRuntimeFactory
import org.neo4j.cypher.internal.Compiler
import org.neo4j.cypher.internal.CompilerLibrary
import org.neo4j.cypher.internal.CypherCurrentCompiler
import org.neo4j.cypher.internal.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.PreParser
import org.neo4j.cypher.internal.QueryCache.CacheKey
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.cache.TestExecutorCaffeineCacheFactory
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.phases.Compatibility4_4
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.options.CypherVersion
import org.neo4j.cypher.internal.planner.spi.MinimumGraphStatistics.MIN_NODES_ALL
import org.neo4j.cypher.internal.planner.spi.MinimumGraphStatistics.MIN_NODES_WITH_LABEL
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.logging.AssertableLogProvider
import org.neo4j.logging.AssertableLogProvider.Level
import org.neo4j.logging.Log
import org.neo4j.logging.LogAssertions.assertThat
import org.neo4j.logging.NullLog
import org.neo4j.logging.NullLogProvider

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset

class CypherCompilerAstCacheAcceptanceTest extends CypherFunSuite with GraphDatabaseTestSupport {

  private val cacheFactory = TestExecutorCaffeineCacheFactory

  private def plannerConfig(queryCacheSize: Int = 128,
                            statsDivergenceThreshold: Double = 0.5,
                            queryPlanTTL: Long = 1000): CypherPlannerConfiguration = {
    val builder = Config.newBuilder()
    builder.set(GraphDatabaseSettings.query_cache_size, Int.box(queryCacheSize))
    builder.set(GraphDatabaseSettings.query_statistics_divergence_threshold, Double.box(statsDivergenceThreshold))
    builder.set(GraphDatabaseSettings.cypher_min_replan_interval, Duration.ofMillis(queryPlanTTL))
    val config = builder.build()
    CypherPlannerConfiguration.fromCypherConfiguration(CypherConfiguration.fromConfig(config), config, planSystemCommands = false)

  }

  private def createCompiler(config: CypherPlannerConfiguration,
                             clock: Clock = Clock.systemUTC(),
                             log: Log = NullLog.getInstance): CypherCurrentCompiler[RuntimeContext] = {
    val planner = CypherPlanner(config,
      clock,
      kernelMonitors,
      log,
      cacheFactory,
      CypherPlannerOption.default,
      () => 1,
      compatibilityMode = Compatibility4_4)
    createCompiler(planner, log)
  }

  private def createCompiler(planner: CypherPlanner, log: Log):
  CypherCurrentCompiler[RuntimeContext] = {
    CypherCurrentCompiler(
      planner,
      CommunityRuntimeFactory.getRuntime(CypherRuntimeOption.default, disallowFallback = true),
      CommunityRuntimeContextManager(log, CypherRuntimeConfiguration.fromCypherConfiguration(CypherConfiguration.fromConfig(Config.defaults()))),
      kernelMonitors)

  }

  private class ASTCacheCounter() extends CacheTracer[CacheKey[AnyRef]] {
    var counts: CacheCounts = CacheCounts()
    override def queryCacheHit(key: CacheKey[AnyRef], metaData: String): Unit = counts = counts.copy(hits = counts.hits + 1)
    override def queryCacheMiss(key: CacheKey[AnyRef], metaData: String): Unit = counts = counts.copy(misses = counts.misses + 1)
    override def queryCacheFlush(sizeBeforeFlush: Long): Unit = counts = counts.copy(flushes = counts.flushes + 1)
    override def queryCacheStale(key: CacheKey[AnyRef], secondsSincePlan: Int, metaData: String, maybeReason: Option[String]): Unit =
      counts = counts.copy(evicted = counts.evicted + 1)
    override def queryCompile(queryKey: CacheKey[AnyRef], metaData: String): Unit = counts = counts.copy(compilations = counts.compilations + 1)
    override def queryCompileWithExpressionCodeGen(queryKey: CacheKey[AnyRef],
                                                   metaData: String): Unit = {counts = counts.copy(compilationsWithExpressionCodeGen = counts.compilationsWithExpressionCodeGen + 1)
    }
  }

  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(GraphDatabaseSettings.cypher_min_replan_interval -> Duration.ZERO)

  private var counter: ASTCacheCounter = _
  private var compiler: CypherCurrentCompiler[RuntimeContext] = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    counter = new ASTCacheCounter()
    compiler = createCompiler(plannerConfig())
    kernelMonitors.addMonitorListener(counter)
  }

  private def runQuery(query: String,
                       params: scala.Predef.Map[String, AnyRef] = Map.empty,
                       cypherCompiler: Compiler = compiler): String = {

    val preParser = new PreParser(
      CypherConfiguration.fromConfig(Config.defaults()),
      1,
      cacheFactory)

    val preParsedQuery = preParser.preParseQuery(query)

    graph.withTx { tx =>
      val noTracing = CompilationPhaseTracer.NO_TRACING
      val context = graph.transactionalContext(tx, query = query -> params)
      cypherCompiler.compile(preParsedQuery, noTracing, Set.empty, context, ValueUtils.asParameterMapValue(asJavaMapDeep(params)))
      val id = context.executingQuery().id()
      context.close()
      id
    }
  }

  test("should monitor cache misses") {
    runQuery("return 42")

    counter.counts should equal(CacheCounts(misses = 1, flushes = 1, compilations = 1))
  }

  test("should monitor cache hits") {
    runQuery("return 42")
    runQuery("return 42")

    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("Constant values in query should use same plan") {
    runQuery("return 42 AS a")
    runQuery("return 53 AS a")
    runQuery("return 76 AS a")

    counter.counts should equal(CacheCounts(hits = 2, misses = 1, flushes = 1, compilations = 1))
  }

  test("Query with generated names (pattern expression) should use same plan") {
    runQuery("MATCH (a) WHERE (a)-[:REL]->(:L) RETURN a")
    // Different whitespace so the String->AST cache does not hit
    runQuery("MATCH (a)  WHERE (a)-[:REL]->(:L) RETURN a")

    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("Query with generated names (pattern comprehension) should use same plan") {
    runQuery("MATCH (a) RETURN [(a)-[:REL]->(:L) WHERE a.prop = 0 | a.foo]")
    // Different whitespace so the String->AST cache does not hit
    runQuery("MATCH (a)  RETURN [(a)-[:REL]->(:L) WHERE a.prop = 0 | a.foo]")

    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("Query with generated names (unnamed elements in pattern) should use same plan") {
    runQuery("MATCH (a)-[:REL]->(:L) RETURN a")
    // Different whitespace so the String->AST cache does not hit
    runQuery("MATCH  (a)-[:REL]->(:L) RETURN a")

    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("should fold to constants and use the same plan") {
    runQuery("return 42 AS a")
    runQuery("return 5 + 3 AS a")
    runQuery("return 5 - 3 AS a")
    runQuery("return 7 / 6 AS a")
    runQuery("return 7 * 6 AS a")

    counter.counts should equal(CacheCounts(hits = 4, misses = 1, flushes = 1, compilations = 1))
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
    runQuery("WITH [1,2,3] as x RETURN x")    // hit (list of size 2 and list of size 3 both fall into the same bucket of size 10)

    counter.counts should equal(CacheCounts(hits = 4, misses = 4, flushes = 1, compilations = 4))
  }

  test("should not care about white spaces") {
    runQuery("return 42")
    runQuery("\treturn          42")

    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("should cache easily parametrized queries") {
    runQuery("return 42 as result")
    runQuery("return 43 as result")

    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("should cache auto-parameterized lists") {
    runQuery("return [1, 2, 3] as result")
    runQuery("return [2, 3, 4] as result")

    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("should keep different cache entries for lists where inner type is not string and where inner type is string") {
    runQuery("MATCH (n:Label) WHERE n.prop IN ['1', '2', '3'] RETURN *")
    runQuery("MATCH (n:Label) WHERE n.prop IN ['1', 2, 3] RETURN *")

    counter.counts should equal(CacheCounts(misses = 2, flushes = 1, compilations = 2))
  }

  test("should keep one entry for all lists where inner type is not string") {
    runQuery("MATCH (n:Label) WHERE n.prop IN ['1', 2, 3] RETURN *")
    runQuery("MATCH (n:Label) WHERE n.prop IN [1, 2, 3] RETURN *")

    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("should keep one entry for all lists where inner type is string") {
    runQuery("MATCH (n:Label) WHERE n.prop IN ['1', '2', '3'] RETURN *")
    runQuery("MATCH (n:Label) WHERE n.prop IN ['2', '3', '4'] RETURN *")

    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("should keep different cache entries for explicitly parametrized lists where inner type is not string and where inner type is string") {
    runQuery("MATCH (n:Label) WHERE n.prop IN $list RETURN *", params = Map("list" -> Seq("1", "2", "3")))
    runQuery("MATCH (n:Label) WHERE n.prop IN $list RETURN *", params = Map("list" -> Seq("1", 2, "3")))

    counter.counts should equal(CacheCounts(misses = 2, flushes = 1, compilations = 2))
  }

  test("should keep one entry for all explicitly parametrized lists where inner type is string") {
    runQuery("MATCH (n:Label) WHERE n.prop IN $list RETURN *", params = Map("list" -> Seq("1", "2", "3")))
    runQuery("MATCH (n:Label) WHERE n.prop IN $list RETURN *", params = Map("list" -> Seq("2", "3", "4")))

    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("should recompile for auto-parameterized lists with different bucket size") {
    val listBucketSize1 = Seq(1).mkString("[", ",", "]")
    val listBucketSize10 = (1 to 5).mkString("[", ",", "]")
    val listBucketSize100 = (1 to 100).mkString("[", ",", "]")
    val listBucketSize1000 = (1 to 1000).mkString("[", ",", "]")

    runQuery(s"return $listBucketSize1 as result")
    runQuery(s"return $listBucketSize10 as result")
    runQuery(s"return $listBucketSize100 as result")
    runQuery(s"return $listBucketSize1000 as result")
    runQuery(s"return $listBucketSize1000 as result")
    runQuery(s"return $listBucketSize100 as result")
    runQuery(s"return $listBucketSize10 as result")
    runQuery(s"return $listBucketSize1 as result")

    counter.counts should equal(CacheCounts(hits = 4, misses = 4, flushes = 1, compilations = 4))
  }

  test("should monitor cache flushes") {
    runQuery("return 42")
    graph.createUniqueConstraint("Person", "id")
    runQuery("return 42")

    counter.counts should equal(CacheCounts(misses = 2, flushes = 2, compilations = 2))
  }

  test("should monitor cache remove") {
    // given
    val clock: Clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC)
    counter = new ASTCacheCounter()
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
    counter.counts should equal(CacheCounts(misses = 2, flushes = 1, evicted = 1, compilations = 2))
  }

  // This test is only added to communicate that we're aware of this behaviour and consider it
  // acceptable, because divergence in NodesAllCardinality will be very rare in a production system
  // except for the initial population. It would be preferable to not evict here, but that would
  // required changes that are too risky for a patch release.
  test("it's ok to evict query because of total nodes change") {
    // given
    val clock: Clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC)
    counter = new ASTCacheCounter()
    compiler = createCompiler(plannerConfig(queryPlanTTL = 0), clock = clock)
    compiler.kernelMonitors.addMonitorListener(counter)
    val query: String = "MATCH (n:Person) RETURN n"
    (0 until MIN_NODES_ALL).foreach { _ => createNode() }
    createLabeledNode("Person")

    // when
    runQuery(query)

    // then
    counter.counts should equal(CacheCounts(misses = 1, flushes = 1, compilations = 1))

    // when
    // we create enough nodes for NodesAllCardinality to trigger a replan
    (0 until 10 * MIN_NODES_ALL).foreach { _ => createNode() }
    runQuery(query)

    // then
    counter.counts should equal(CacheCounts(misses = 2, flushes = 1, evicted = 1, compilations = 2))
  }

  test("should not evict query because of unrelated statistics change") {
    // given
    val clock: Clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC)
    counter = new ASTCacheCounter()
    compiler = createCompiler(plannerConfig(queryPlanTTL = 0), clock = clock)
    compiler.kernelMonitors.addMonitorListener(counter)
    val query: String = "MATCH (n:Person) RETURN n"
    (0 until MIN_NODES_WITH_LABEL * 3).foreach { _ => createLabeledNode("Person") }

    // when
    runQuery(query)

    // then
    counter.counts should equal(CacheCounts(misses = 1, flushes = 1, compilations = 1))

    // when
    // we create enough nodes for NodesLabelCardinality("Dog") to trigger a replan
    // but not NodesAllCardinality or NodesLabelCardinality("Person")
    (0 until MIN_NODES_WITH_LABEL * 3).foreach { _ => createLabeledNode("Dog") }
    runQuery(query)

    // then
    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("should log on cache remove") {
    // given
    val logProvider = new AssertableLogProvider()
    val clock: Clock = Clock.fixed(Instant.ofEpochMilli(10000L), ZoneOffset.UTC)
    compiler = createCompiler(plannerConfig(queryPlanTTL = 0), clock = clock, log = logProvider.getLog(getClass))
    val query: String = "match (n:Person:Dog) return n"

    createLabeledNode("Dog")
    (0 until 50).foreach { _ => createLabeledNode("Person") }
    runQuery(query)

    // when
    (0 until 1000).foreach { _ => createLabeledNode("Dog") }
    val queryId = runQuery(query)

    // then
    val dogId = graph.withTx(tx => tokenReader(tx, _.nodeLabel("Dog")))

    assertThat(logProvider).forClass(getClass).forLevel(Level.DEBUG)
      .containsMessages(s"Discarded stale plan from the plan cache after 0 seconds. " +
                             s"Reason: NodesWithLabelCardinality(Some(LabelId($dogId))) changed from 10.0 to 1001.0, " +
                             s"which is a divergence of 0.99000999000999 which is greater than threshold 0.5. Metadata: $queryId")
      .doesNotContainMessage(query)
  }

  test("when running queries with debug options - never cache") {
    runQuery("CYPHER debug=logicalplan RETURN 42")
    runQuery("CYPHER debug=logicalplan RETURN 42")

    counter.counts.hits should equal(0)
  }

  test("should not find query in cache with different parameter types") {
    val map1: scala.Predef.Map[String, AnyRef] = scala.Predef.Map("number" -> Integer.valueOf(42))
    val map2: scala.Predef.Map[String, AnyRef] = scala.Predef.Map("number" -> "nope")
    runQuery("return $number", params = map1)
    runQuery("return $number", params = map2)

    counter.counts should equal(CacheCounts(misses = 2, flushes = 1, compilations = 2))
  }

  test("should find query in cache with same parameter types") {
    val map1: scala.Predef.Map[String, AnyRef] = scala.Predef.Map("number" -> Integer.valueOf(42))
    val map2: scala.Predef.Map[String, AnyRef] = scala.Predef.Map("number" -> Integer.valueOf(43))
    runQuery("return $number", params = map1)
    runQuery("return $number", params = map2)

    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("should find query in cache with same parameter types, ignoring unused parameters") {
    val map1: scala.Predef.Map[String, AnyRef] = scala.Predef.Map("number" -> Integer.valueOf(42), "foo" -> "bar")
    val map2: scala.Predef.Map[String, AnyRef] = scala.Predef.Map("number" -> Integer.valueOf(43), "bar" -> Integer.valueOf(10))
    runQuery("return $number", params = map1)
    runQuery("return $number", params = map2)

    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("should clear all compiler library caches") {
    val compilerLibrary = createCompilerLibrary()
    val compilers = CypherVersion.values.map { version =>
      compilerLibrary.selectCompiler(version, CypherPlannerOption.default, CypherRuntimeOption.default)
    }

    compilers.foreach { compiler =>
      runQuery("return 42", cypherCompiler = compiler) // Misses
      runQuery("return 42", cypherCompiler = compiler) // Hits
    }

    compilerLibrary.clearCaches()

    compilers.foreach { compiler =>
      runQuery("return 42", cypherCompiler = compiler) // Misses
    }

    counter.counts should equal(CacheCounts(hits = compilers.size, misses = 2 * compilers.size, flushes = 2 * compilers.size, compilations = 2 * compilers.size))
  }

  private def createCompilerLibrary(): CompilerLibrary = {
    val resolver = graph.getDependencyResolver
    val monitors = kernelMonitors
    val nullLogProvider = NullLogProvider.getInstance
    val config = resolver.resolveDependency(classOf[Config])
    val cypherConfig = CypherConfiguration.fromConfig(config)
    val compilerFactory =
      new CommunityCompilerFactory(graph, monitors, cacheFactory, nullLogProvider,
        CypherPlannerConfiguration.fromCypherConfiguration(cypherConfig, config, planSystemCommands = false),
        CypherRuntimeConfiguration.fromCypherConfiguration(cypherConfig),
      )
    new CompilerLibrary(compilerFactory, () => null)
  }
}
