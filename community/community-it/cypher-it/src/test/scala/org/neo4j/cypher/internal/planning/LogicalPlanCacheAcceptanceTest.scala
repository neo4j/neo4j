/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.planning

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.ExecutionEngineHelper.asJavaMapDeep
import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.cypher.internal.CachingPreParser
import org.neo4j.cypher.internal.CommunityCompilerFactory
import org.neo4j.cypher.internal.CommunityRuntimeContextManager
import org.neo4j.cypher.internal.CommunityRuntimeFactory
import org.neo4j.cypher.internal.Compiler
import org.neo4j.cypher.internal.CompilerLibrary
import org.neo4j.cypher.internal.CypherCurrentCompiler
import org.neo4j.cypher.internal.LastCommittedTxIdProvider
import org.neo4j.cypher.internal.MasterCompiler
import org.neo4j.cypher.internal.PreParsedQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.cache.CypherQueryCaches
import org.neo4j.cypher.internal.cache.LFUCache
import org.neo4j.cypher.internal.cache.TestExecutorCaffeineCacheFactory
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.options.CypherUpdateStrategy
import org.neo4j.cypher.internal.planner.spi.MinimumGraphStatistics.MIN_NODES_ALL
import org.neo4j.cypher.internal.planner.spi.MinimumGraphStatistics.MIN_NODES_WITH_LABEL
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.util.devNullLogger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.util.CacheCountsTestSupport
import org.neo4j.cypher.util.CacheCountsTestSupport.CacheCounts
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.logging.AssertableLogProvider
import org.neo4j.logging.AssertableLogProvider.Level
import org.neo4j.logging.InternalLogProvider
import org.neo4j.logging.LogAssertions.assertThat
import org.neo4j.logging.NullLogProvider

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset

class LogicalPlanCacheAcceptanceTest extends CypherFunSuite with GraphDatabaseTestSupport with CacheCountsTestSupport {

  private val cacheFactory = TestExecutorCaffeineCacheFactory

  private def cypherConfig(
    queryCacheSize: Int = 128,
    statsDivergenceThreshold: Double = 0.5,
    queryPlanTTL: Long = 1000
  ): CypherConfiguration = {
    val builder = Config.newBuilder()
    builder.set(GraphDatabaseSettings.query_cache_size, Int.box(queryCacheSize))
    builder.set(GraphDatabaseSettings.query_statistics_divergence_threshold, Double.box(statsDivergenceThreshold))
    builder.set(GraphDatabaseSettings.cypher_min_replan_interval, Duration.ofMillis(queryPlanTTL))
    val config = builder.build()
    CypherConfiguration.fromConfig(config)
  }

  private def createCompiler(
    config: CypherConfiguration,
    clock: Clock = Clock.systemUTC(),
    logProvider: InternalLogProvider = NullLogProvider.getInstance
  ): CypherCurrentCompiler[RuntimeContext] = {
    val caches = new CypherQueryCaches(
      CypherQueryCaches.Config.fromCypherConfiguration(config),
      () => 1,
      cacheFactory,
      clock,
      kernelMonitors,
      logProvider
    )

    val log = logProvider.getLog(getClass)

    val planner = CypherPlanner(
      CypherPlannerConfiguration.fromCypherConfiguration(config, Config.defaults(), planSystemCommands = false, false),
      clock,
      kernelMonitors,
      log,
      caches,
      CypherPlannerOption.default,
      CypherUpdateStrategy.default,
      null,
      null
    )

    CypherCurrentCompiler(
      planner,
      CommunityRuntimeFactory.getRuntime(CypherRuntimeOption.default, disallowFallback = true),
      CommunityRuntimeContextManager(
        log,
        CypherRuntimeConfiguration.fromCypherConfiguration(CypherConfiguration.fromConfig(Config.defaults()))
      ),
      kernelMonitors,
      caches
    )
  }

  override def databaseConfig(): Map[Setting[_], Object] =
    super.databaseConfig() ++ Map(GraphDatabaseSettings.cypher_min_replan_interval -> Duration.ZERO)

  private var compiler: CypherCurrentCompiler[RuntimeContext] = _

  private def logicalPlanCacheCounts: CacheCounts = {
    cacheCountsFor(CypherQueryCaches.LogicalPlanCache, compiler.queryCaches.statistics())
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    compiler = createCompiler(cypherConfig())
  }

  private def runQuery(
    query: String,
    params: scala.Predef.Map[String, AnyRef] = Map.empty,
    cypherCompiler: Compiler = compiler
  ): String = {

    val preParser = new CachingPreParser(
      CypherConfiguration.fromConfig(Config.defaults()),
      new LFUCache[String, PreParsedQuery](TestExecutorCaffeineCacheFactory, 1)
    )

    val preParsedQuery = preParser.preParseQuery(query, devNullLogger)

    graph.withTx { tx =>
      val noTracing = CompilationPhaseTracer.NO_TRACING
      val context = graph.transactionalContext(tx, query = query -> params)
      cypherCompiler.compile(
        preParsedQuery,
        noTracing,
        context,
        ValueUtils.asParameterMapValue(asJavaMapDeep(params)),
        devNullLogger
      )
      val id = context.executingQuery().id()
      context.close()
      id
    }
  }

  test("should monitor cache misses") {
    runQuery("return 42")

    logicalPlanCacheCounts should equal(CacheCounts(misses = 1, flushes = 1, compilations = 1))
  }

  test("should monitor cache hits") {
    runQuery("return 42")
    runQuery("return 42")

    logicalPlanCacheCounts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("Constant values in query should use same plan") {
    runQuery("return 42 AS a")
    runQuery("return 53 AS a")
    runQuery("return 76 AS a")

    logicalPlanCacheCounts should equal(CacheCounts(hits = 2, misses = 1, flushes = 1, compilations = 1))
  }

  test("Query with generated names (pattern expression) should use same plan") {
    runQuery("MATCH (a) WHERE (a)-[:REL]->(:L) RETURN a")
    // Different whitespace so the String->AST cache does not hit
    runQuery("MATCH (a)  WHERE (a)-[:REL]->(:L) RETURN a")

    logicalPlanCacheCounts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("Query with generated names (pattern comprehension) should use same plan") {
    runQuery("MATCH (a) RETURN [(a)-[:REL]->(:L) WHERE a.prop = 0 | a.foo]")
    // Different whitespace so the String->AST cache does not hit
    runQuery("MATCH (a)  RETURN [(a)-[:REL]->(:L) WHERE a.prop = 0 | a.foo]")

    logicalPlanCacheCounts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("Query with generated names (unnamed elements in pattern) should use same plan") {
    runQuery("MATCH (a)-[:REL]->(:L) RETURN a")
    // Different whitespace so the String->AST cache does not hit
    runQuery("MATCH  (a)-[:REL]->(:L) RETURN a")

    logicalPlanCacheCounts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("should fold to constants and use the same plan") {
    runQuery("return 42 AS a")
    runQuery("return 5 + 3 AS a")
    runQuery("return 5 - 3 AS a")
    runQuery("return 7 / 6 AS a")
    runQuery("return 7 * 6 AS a")

    logicalPlanCacheCounts should equal(CacheCounts(hits = 4, misses = 1, flushes = 1, compilations = 1))
  }

  test("should keep different cache entries for different literal types") {
    runQuery("WITH 1 as x RETURN x") // miss
    runQuery("WITH 2 as x RETURN x") // hit
    runQuery("WITH 1.0 as x RETURN x") // miss
    runQuery("WITH 2.0 as x RETURN x") // hit
    runQuery("WITH 'foo' as x RETURN x") // miss
    runQuery("WITH 'bar' as x RETURN x") // hit
    runQuery("WITH $p as x RETURN x") // not enough parameters -> not even miss
    runQuery("WITH $k as x RETURN x") // not enough parameters -> not even miss
    runQuery("WITH [1,2] as x RETURN x") // miss
    runQuery(
      "WITH [1,2,3] as x RETURN x"
    ) // hit (list of size 2 and list of size 3 both fall into the same bucket of size 10)

    logicalPlanCacheCounts should equal(CacheCounts(hits = 4, misses = 4, flushes = 1, compilations = 4))
  }

  test("should not care about white spaces") {
    runQuery("return 42")
    runQuery("\treturn          42")

    logicalPlanCacheCounts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("should cache easily parametrized queries") {
    runQuery("return 42 as result")
    runQuery("return 43 as result")

    logicalPlanCacheCounts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("should cache auto-parameterized lists") {
    runQuery("return [1, 2, 3] as result")
    runQuery("return [2, 3, 4] as result")

    logicalPlanCacheCounts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("should cache auto-parameterized strings") {
    runQuery("return 'straw' as result")
    runQuery("return 'warts' as result")

    logicalPlanCacheCounts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("should keep different cache entries for lists where inner type is not string and where inner type is string") {
    runQuery("MATCH (n:Label) WHERE n.prop IN ['1', '2', '3'] RETURN *")
    runQuery("MATCH (n:Label) WHERE n.prop IN ['1', 2, 3] RETURN *")

    logicalPlanCacheCounts should equal(CacheCounts(misses = 2, flushes = 1, compilations = 2))
  }

  test("should keep one entry for all lists where inner type is not string") {
    runQuery("MATCH (n:Label) WHERE n.prop IN ['1', 2, 3] RETURN *")
    runQuery("MATCH (n:Label) WHERE n.prop IN [1, 2, 3] RETURN *")

    logicalPlanCacheCounts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("should keep one entry for all lists where inner type is string") {
    runQuery("MATCH (n:Label) WHERE n.prop IN ['1', '2', '3'] RETURN *")
    runQuery("MATCH (n:Label) WHERE n.prop IN ['2', '3', '4'] RETURN *")

    logicalPlanCacheCounts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test(
    "should keep different cache entries for explicitly parametrized lists where inner type is not string and where inner type is string"
  ) {
    runQuery("MATCH (n:Label) WHERE n.prop IN $list RETURN *", params = Map("list" -> Seq("1", "2", "3")))
    runQuery("MATCH (n:Label) WHERE n.prop IN $list RETURN *", params = Map("list" -> Seq("1", 2, "3")))

    logicalPlanCacheCounts should equal(CacheCounts(misses = 2, flushes = 1, compilations = 2))
  }

  test("should keep one entry for all explicitly parametrized lists where inner type is string") {
    runQuery("MATCH (n:Label) WHERE n.prop IN $list RETURN *", params = Map("list" -> Seq("1", "2", "3")))
    runQuery("MATCH (n:Label) WHERE n.prop IN $list RETURN *", params = Map("list" -> Seq("2", "3", "4")))

    logicalPlanCacheCounts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
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

    logicalPlanCacheCounts should equal(CacheCounts(hits = 4, misses = 4, flushes = 1, compilations = 4))
  }

  test("should recompile for auto-parameterized strings with different bucket size") {
    val stringBucketSize1 = "-"
    val stringBucketSize10 = "-".repeat(10)
    val stringBucketSize100 = "-".repeat(100)
    val stringBucketSize1000 = "-".repeat(1000)

    runQuery(s"return '$stringBucketSize1' as result")
    runQuery(s"return '$stringBucketSize10' as result")
    runQuery(s"return '$stringBucketSize100' as result")
    runQuery(s"return '$stringBucketSize1000' as result")
    runQuery(s"return '$stringBucketSize1000' as result")
    runQuery(s"return '$stringBucketSize100' as result")
    runQuery(s"return '$stringBucketSize10' as result")
    runQuery(s"return '$stringBucketSize1' as result")

    logicalPlanCacheCounts should equal(CacheCounts(hits = 4, misses = 4, flushes = 1, compilations = 4))
  }

  test("should monitor cache flushes") {
    runQuery("return 42")
    graph.createNodeUniquenessConstraint("Person", "id")
    runQuery("return 42")

    logicalPlanCacheCounts should equal(CacheCounts(misses = 2, flushes = 2, compilations = 2, discards = 1))
  }

  test("should monitor cache remove") {
    // given
    val clock: Clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC)
    compiler = createCompiler(cypherConfig(queryPlanTTL = 0), clock = clock)
    val query: String = "match (n:Person:Dog) return n"

    createLabeledNode("Dog")
    (0 until 50).foreach { _ => createLabeledNode("Person") }
    runQuery(query)

    // when
    (0 until 1000).foreach { _ => createLabeledNode("Dog") }
    runQuery(query)

    // then
    logicalPlanCacheCounts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, evicted = 1, compilations = 2))
  }

  // This test is only added to communicate that we're aware of this behaviour and consider it
  // acceptable, because divergence in NodesAllCardinality will be very rare in a production system
  // except for the initial population. It would be preferable to not evict here, but that would
  // required changes that are too risky for a patch release.
  test("it's ok to evict query because of total nodes change") {
    // given
    val clock: Clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC)
    compiler = createCompiler(cypherConfig(queryPlanTTL = 0), clock = clock)
    val query: String = "MATCH (n:Person) RETURN n"
    (0 until MIN_NODES_ALL).foreach { _ => createNode() }
    createLabeledNode("Person")

    // when
    runQuery(query)

    // then
    logicalPlanCacheCounts should equal(CacheCounts(misses = 1, flushes = 1, compilations = 1))

    // when
    // we create enough nodes for NodesAllCardinality to trigger a replan
    (0 until 10 * MIN_NODES_ALL).foreach { _ => createNode() }
    runQuery(query)

    // then
    logicalPlanCacheCounts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, evicted = 1, compilations = 2))
  }

  test("should not evict query because of unrelated statistics change") {
    // given
    val clock: Clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC)
    compiler = createCompiler(cypherConfig(queryPlanTTL = 0), clock = clock)
    val query: String = "MATCH (n:Person) RETURN n"
    (0 until MIN_NODES_WITH_LABEL * 3).foreach { _ => createLabeledNode("Person") }

    // when
    runQuery(query)

    // then
    logicalPlanCacheCounts should equal(CacheCounts(misses = 1, flushes = 1, compilations = 1))

    // when
    // we create enough nodes for NodesLabelCardinality("Dog") to trigger a replan
    // but not NodesAllCardinality or NodesLabelCardinality("Person")
    (0 until MIN_NODES_WITH_LABEL * 3).foreach { _ => createLabeledNode("Dog") }
    runQuery(query)

    // then
    logicalPlanCacheCounts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("should log on cache remove") {
    // given
    val logProvider = new AssertableLogProvider()
    val clock: Clock = Clock.fixed(Instant.ofEpochMilli(10000L), ZoneOffset.UTC)
    compiler = createCompiler(cypherConfig(queryPlanTTL = 0), clock = clock, logProvider = logProvider)
    val query: String = "match (n:Person:Dog) return n"

    createLabeledNode("Dog")
    (0 until 50).foreach { _ => createLabeledNode("Person") }
    runQuery(query)

    // when
    (0 until 1000).foreach { _ => createLabeledNode("Dog") }
    val queryId = runQuery(query)

    // then
    val dogId = graph.withTx(tx => tokenReader(tx, _.nodeLabel("Dog")))

    assertThat(logProvider).forClass(classOf[CypherQueryCaches]).forLevel(Level.DEBUG)
      .containsMessages(s"Discarded stale plan from the plan cache after 0 seconds. " +
        s"Reason: NodesWithLabelCardinality(Some(LabelId($dogId))) changed from 10.0 to 1001.0, " +
        s"which is a divergence of 0.99000999000999 which is greater than threshold 0.5. Query id: $queryId.")
      .doesNotContainMessage(query)
  }

  test("when running queries with debug options - never cache") {
    runQuery("CYPHER debug=logicalplan RETURN 42")
    runQuery("CYPHER debug=logicalplan RETURN 42")

    logicalPlanCacheCounts.hits should equal(0)
  }

  test("should not find query in cache with different parameter types") {
    val map1: scala.Predef.Map[String, AnyRef] = scala.Predef.Map("number" -> Integer.valueOf(42))
    val map2: scala.Predef.Map[String, AnyRef] = scala.Predef.Map("number" -> "nope")
    runQuery("return $number", params = map1)
    runQuery("return $number", params = map2)

    logicalPlanCacheCounts should equal(CacheCounts(misses = 2, flushes = 1, compilations = 2))
  }

  test("should find query in cache with same parameter types") {
    val map1: scala.Predef.Map[String, AnyRef] = scala.Predef.Map("number" -> Integer.valueOf(42))
    val map2: scala.Predef.Map[String, AnyRef] = scala.Predef.Map("number" -> Integer.valueOf(43))
    runQuery("return $number", params = map1)
    runQuery("return $number", params = map2)

    logicalPlanCacheCounts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("should find query in cache with same parameter types, ignoring unused parameters") {
    val map1: scala.Predef.Map[String, AnyRef] = scala.Predef.Map("number" -> Integer.valueOf(42), "foo" -> "bar")
    val map2: scala.Predef.Map[String, AnyRef] =
      scala.Predef.Map("number" -> Integer.valueOf(43), "bar" -> Integer.valueOf(10))
    runQuery("return $number", params = map1)
    runQuery("return $number", params = map2)

    logicalPlanCacheCounts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("should clear all compiler library caches") {
    val (compilerLibrary, queryCaches) = createCompilerLibrary()
    val compilers = CypherRuntimeOption.values.map { runtime =>
      compilerLibrary.selectCompiler(
        CypherPlannerOption.default,
        CommunityRuntimeFactory.getRuntime(runtime, disallowFallback = false).correspondingRuntimeOption.get,
        CypherUpdateStrategy.default
      )
    }

    compilers.foreach { compiler =>
      runQuery("return 42", cypherCompiler = compiler) // Misses
      runQuery("return 42", cypherCompiler = compiler) // Hits
    }

    compilerLibrary.clearCaches()

    compilers.foreach { compiler =>
      runQuery("return 42", cypherCompiler = compiler) // Misses
    }

    val counts = cacheCountsFor(CypherQueryCaches.LogicalPlanCache, queryCaches.statistics())
    counts should equal(CacheCounts(
      hits = compilers.size,
      misses = 2 * compilers.size,
      flushes = 2 * compilers.size,
      compilations = 2 * compilers.size,
      discards = compilers.size
    ))
  }

  private def createCompilerLibrary(): (CompilerLibrary, CypherQueryCaches) = {
    val resolver = graph.getDependencyResolver
    val monitors = kernelMonitors
    val nullLogProvider = NullLogProvider.getInstance
    val config = resolver.resolveDependency(classOf[Config])
    val cypherConfig = CypherConfiguration.fromConfig(config)
    val queryCaches = new CypherQueryCaches(
      CypherQueryCaches.Config.fromCypherConfiguration(cypherConfig),
      LastCommittedTxIdProvider(graph),
      cacheFactory,
      MasterCompiler.CLOCK,
      monitors,
      logProvider
    )
    val compilerFactory =
      new CommunityCompilerFactory(
        graph,
        monitors,
        nullLogProvider,
        CypherPlannerConfiguration.fromCypherConfiguration(cypherConfig, config, planSystemCommands = false, false),
        CypherRuntimeConfiguration.fromCypherConfiguration(cypherConfig),
        queryCaches
      )
    (new CompilerLibrary(compilerFactory, () => null), queryCaches)
  }
}
