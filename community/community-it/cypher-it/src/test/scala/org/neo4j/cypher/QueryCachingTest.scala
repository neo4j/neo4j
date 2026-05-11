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
package org.neo4j.cypher

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.CypherITTestSuite
import org.neo4j.cypher.internal.cache.CacheTracer
import org.neo4j.cypher.internal.cache.CypherQueryCaches
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_00001
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_01N60
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.config.Setting
import org.scalatest.prop.TableDrivenPropertyChecks

import java.time.Duration

import scala.collection.mutable
import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava

abstract class QueryCachingTest(executionPlanCacheSize: Int =
  GraphDatabaseInternalSettings.query_execution_plan_cache_size.defaultValue()) extends CypherITTestSuite
    with GraphDatabaseTestSupport with TableDrivenPropertyChecks {

  override def databaseConfig(): Map[Setting[?], Object] = super.databaseConfig() ++ Map(
    // String cache JIT compiles on the first hit
    GraphDatabaseInternalSettings.cypher_expression_recompilation_limit -> Integer.valueOf(2),
    GraphDatabaseInternalSettings.cypher_enable_runtime_monitors -> java.lang.Boolean.TRUE,
    GraphDatabaseInternalSettings.cypher_enable_query_cache_monitors -> java.lang.Boolean.TRUE,
    GraphDatabaseInternalSettings.query_execution_plan_cache_size -> Integer.valueOf(executionPlanCacheSize),
    GraphDatabaseSettings.cypher_min_replan_interval -> Duration.ofSeconds(0),
    GraphDatabaseSettings.default_language -> GraphDatabaseSettings.CypherVersion.Cypher5
    // Might need to be enabled when the next experimental version appear: GraphDatabaseInternalSettings.enable_experimental_cypher_versions -> java.lang.Boolean.TRUE
  )

  private val empty_parameters = "Map()"

  test("AstLogicalPlanCache re-uses cached plan across different execution modes") {
    // ensure label exists
    graph.withTx(tx => tx.createNode(Label.label("Person")))

    val cacheListener = new LoggingTracer(traceExecutionEngineQueryCache = false, traceExecutionPlanCache = false)

    val query = "MATCH (n:Person) RETURN n"
    val profileQuery = s"PROFILE $query"
    val explainQuery = s"EXPLAIN $query"

    val modeCombinations = Table(
      ("firstQuery", "secondQuery"),
      (query, query),
      (query, profileQuery),
      (query, explainQuery),
      (profileQuery, query),
      (profileQuery, profileQuery),
      (profileQuery, explainQuery),
      (explainQuery, query),
      (explainQuery, profileQuery),
      (explainQuery, explainQuery)
    )

    forAll(modeCombinations) {
      case (firstQuery, secondQuery) =>
        // Flush cache
        cacheListener.clear()
        graph.withTx(tx => {
          tx.kernelTransaction().schemaRead().schemaStateFlush()
        })

        graph.withTx(tx => tx.execute(firstQuery).resultAsString())
        // run first query twice in order to reach recompilation limit - otherwise we will get the plan from the query string cache
        graph.withTx(tx => tx.execute(firstQuery).resultAsString())
        graph.withTx(tx => tx.execute(secondQuery).resultAsString())

        cacheListener.expectTrace(List(
          "AST:    cacheFlushDetected",
          // firstQuery
          "AST:    cacheMiss",
          "AST:    cacheCompile",
          // secondQuery
          "AST:    cacheHit"
        ))
    }
  }

  test("ExecutionEngineQueryCache re-uses cached plan with and without explain") {
    // ensure label exists
    graph.withTx(tx => tx.createNode(Label.label("Person")))

    val cacheListener = new LoggingTracer(traceAstLogicalPlanCache = false, traceExecutionPlanCache = false)

    val query = "MATCH (n:Person) RETURN n"
    val explainQuery = s"EXPLAIN $query"

    val modeCombinations = Table(
      ("firstQuery", "secondQuery", "third query"),
      (query, query, query),
      (query, explainQuery, query),
      (explainQuery, query, query),
      (explainQuery, explainQuery, explainQuery)
    )

    forAll(modeCombinations) {
      case (firstQuery, secondQuery, thirdQuery) =>
        // Flush cache
        cacheListener.clear()
        graph.withTx(tx => {
          tx.kernelTransaction().schemaRead().schemaStateFlush()
        })

        graph.withTx(tx => tx.execute(firstQuery).resultAsString())
        graph.withTx(tx => tx.execute(secondQuery).resultAsString())
        graph.withTx(tx => tx.execute(thirdQuery).resultAsString())

        cacheListener.expectTrace(List(
          s"String: cacheFlushDetected",
          // firstQuery
          s"String: cacheMiss: CacheKey($query,$empty_parameters,false,5)",
          s"String: cacheCompile: CacheKey($query,$empty_parameters,false,5)",
          // secondQuery
          s"String: cacheHit: CacheKey($query,$empty_parameters,false,5)",
          // thirdQuery
          s"String: cacheCompileWithExpressionCodeGen: CacheKey($query,$empty_parameters,false,5)", // String cache JIT compiles on the second hit
          s"String: cacheHit: CacheKey($query,$empty_parameters,false,5)"
        ))
    }
  }

  test("normal execution followed by profile triggers physical planning but not logical planning") {
    // ensure label exists
    graph.withTx(tx => tx.createNode(Label.label("Person")))

    val cacheListener = new LoggingTracer()

    val query = "MATCH (n:Person) RETURN n"
    val profileQuery = s"PROFILE $query"

    graph.withTx(tx => tx.execute(query).resultAsString())
    graph.withTx(tx => tx.execute(profileQuery).resultAsString())

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // query
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey($query,$empty_parameters,false,5)",
      s"String: cacheCompile: CacheKey($query,$empty_parameters,false,5)",
      // profileQuery
      s"AST:    cacheHit", // no logical planning
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER PROFILE $query,$empty_parameters,false,5)",
      s"String: cacheCompile: CacheKey(CYPHER PROFILE $query,$empty_parameters,false,5)" // physical planning
    ))
  }

  test("profile followed by normal execution triggers physical planning but not logical planning") {
    // ensure label exists
    graph.withTx(tx => tx.createNode(Label.label("Person")))

    val cacheListener = new LoggingTracer()

    val query = "MATCH (n:Person) RETURN n"
    val profileQuery = s"PROFILE $query"

    graph.withTx(tx => tx.execute(profileQuery).resultAsString())
    graph.withTx(tx => tx.execute(query).resultAsString())

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // profileQuery
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER PROFILE $query,$empty_parameters,false,5)",
      s"String: cacheCompile: CacheKey(CYPHER PROFILE $query,$empty_parameters,false,5)",
      // query
      s"AST:    cacheHit", // no logical planning
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey($query,$empty_parameters,false,5)",
      s"String: cacheCompile: CacheKey($query,$empty_parameters,false,5)" // physical planning
    ))
  }

  test(
    "Different String but same AST should hit AstExecutableQueryCache and ExecutionPlanCache but miss ExecutionEngineQueryCache"
  ) {
    // ensure label exists
    graph.withTx(tx => tx.createNode(Label.label("Person")))

    val cacheListener = new LoggingTracer()

    val query1 = "MATCH (n:Person) RETURN n"
    val query2 = "MATCH (n:`Person`) RETURN n"

    graph.withTx(tx => tx.execute(query1).resultAsString())
    graph.withTx(tx => tx.execute(query2).resultAsString())

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // query1
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey($query1,$empty_parameters,false,5)",
      s"String: cacheCompile: CacheKey($query1,$empty_parameters,false,5)",
      // query2
      s"AST:    cacheHit", // Same AST, we should hit the cache,
      executionPlanCacheKeyHit, // same plan should hit the cache
      s"String: cacheMiss: CacheKey($query2,$empty_parameters,false,5)", // Different string, we should miss the cache
      s"String: cacheCompile: CacheKey($query2,$empty_parameters,false,5)"
    ))
  }

  test("repeating query with same parameters should hit the caches") {
    val cacheListener = new LoggingTracer()

    val query = "RETURN $n"
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(42))

    graph.withTx(tx => {
      tx.execute(query, params1.asJava).resultAsString()
    })
    graph.withTx(tx => {
      tx.execute(query, params1.asJava).resultAsString()
    })
    graph.withTx(tx => {
      tx.execute(query, params1.asJava).resultAsString()
    })

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // first
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      s"String: cacheCompile: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      // second
      s"String: cacheHit: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      // third
      s"AST:    cacheHit",
      executionPlanCacheKeyMiss,
      s"String: cacheCompileWithExpressionCodeGen: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)", // String cache JIT compiles on the first hit
      s"String: cacheHit: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)"
    ))
  }

  test("repeating query with replan=force should not hit the caches") {
    val cacheListener = new LoggingTracer()

    val query = "RETURN 42"

    graph.withTx(tx => {
      tx.execute(query).resultAsString()
    })
    graph.withTx(tx => {
      tx.execute(s"CYPHER replan=force $query").resultAsString()
    })
    graph.withTx(tx => {
      tx.execute(s"CYPHER replan=force $query").resultAsString()
    })

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // 1st run
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey($query,$empty_parameters,false,5)",
      s"String: cacheCompile: CacheKey($query,$empty_parameters,false,5)",
      // 2nd run
      s"AST:    cacheHit",
      s"AST:    cacheCompileWithExpressionCodeGen", // replan=force calls into a method for immediate recompilation, even though recompilation is doing the same steps in the AST cache, but the tracer calls are unaware of that.
      executionPlanCacheKeyMiss, // we will miss here since we need to have reached the recompilation limit
      s"String: cacheHit: CacheKey($query,$empty_parameters,false,5)",
      s"String: cacheCompileWithExpressionCodeGen: CacheKey($query,$empty_parameters,false,5)",
      // 3rd run
      s"AST:    cacheHit",
      s"AST:    cacheCompileWithExpressionCodeGen",
      executionPlanCacheKeyHit, // since we get the same plan we will have a hit here
      s"String: cacheHit: CacheKey($query,$empty_parameters,false,5)",
      s"String: cacheCompileWithExpressionCodeGen: CacheKey($query,$empty_parameters,false,5)"
    ))
  }

  test("repeating query with same parameter types but different values should hit the caches") {
    val cacheListener = new LoggingTracer()

    val query = "RETURN $n"
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(12))
    val params2: Map[String, AnyRef] = Map("n" -> Long.box(42))
    val params3: Map[String, AnyRef] = Map("n" -> Long.box(1337))
    graph.withTx(tx => tx.execute(query, params1.asJava).resultAsString())
    graph.withTx(tx => tx.execute(query, params2.asJava).resultAsString())
    graph.withTx(tx => tx.execute(query, params3.asJava).resultAsString())

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // params1
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      s"String: cacheCompile: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      // params2
      s"String: cacheHit: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      // params3
      s"AST:    cacheHit",
      executionPlanCacheKeyMiss, // recompilation limit reached
      s"String: cacheCompileWithExpressionCodeGen: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)", // String cache JIT compiles on the first hit
      s"String: cacheHit: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)"
    ))
  }

  test("repeating query with different parameters types should hit the execution plan cache") {
    restartWithConfig(
      databaseConfig() + (GraphDatabaseInternalSettings.cypher_size_hint_parameters -> java.lang.Boolean.TRUE)
    )
    val cacheListener = new LoggingTracer()

    val query = "RETURN $n"
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(42))
    val params2: Map[String, AnyRef] = Map("n" -> "nope")

    graph.withTx(tx => tx.execute(query, params1.asJava).resultAsString())
    graph.withTx(tx => tx.execute(query, params2.asJava).resultAsString())

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // params1
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      s"String: cacheCompile: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      // params2
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyHit,
      s"String: cacheMiss: CacheKey($query,Map(n -> ParameterTypeInfo(String,ApproximateSize(10))),false,5)",
      s"String: cacheCompile: CacheKey($query,Map(n -> ParameterTypeInfo(String,ApproximateSize(10))),false,5)"
    ))
  }

  test(
    "repeating query with same parameters types but different string lengths should hit the execution plan cache, when configured to use size hint"
  ) {
    restartWithConfig(
      databaseConfig() + (GraphDatabaseInternalSettings.cypher_size_hint_parameters -> java.lang.Boolean.TRUE)
    )

    val cacheListener = new LoggingTracer()

    val query = "RETURN $n"
    val params1: Map[String, AnyRef] = Map("n" -> "a")
    val params2: Map[String, AnyRef] = Map("n" -> "a".repeat(1001))

    graph.withTx(tx => tx.execute(query, params1.asJava).resultAsString())
    graph.withTx(tx => tx.execute(query, params2.asJava).resultAsString())

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // params1
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey($query,Map(n -> ParameterTypeInfo(String,ExactSize(1))),false,5)",
      s"String: cacheCompile: CacheKey($query,Map(n -> ParameterTypeInfo(String,ExactSize(1))),false,5)",
      // params2
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyHit,
      s"String: cacheMiss: CacheKey($query,Map(n -> ParameterTypeInfo(String,ApproximateSize(10000))),false,5)",
      s"String: cacheCompile: CacheKey($query,Map(n -> ParameterTypeInfo(String,ApproximateSize(10000))),false,5)"
    ))
  }

  test(
    "repeating query with same parameters types but different string lengths should hit the execution plan cache, when configured not to use size hint"
  ) {
    restartWithConfig(
      databaseConfig() + (GraphDatabaseInternalSettings.cypher_size_hint_parameters -> java.lang.Boolean.FALSE)
    )
    val cacheListener = new LoggingTracer()

    val query = "RETURN $n"
    val params1: Map[String, AnyRef] = Map("n" -> "")
    val params2: Map[String, AnyRef] = Map("n" -> "a".repeat(1001))

    graph.withTx(tx => tx.execute(query, params1.asJava).resultAsString())
    graph.withTx(tx => tx.execute(query, params2.asJava).resultAsString())

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // params1
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey($query,Map(n -> ParameterTypeInfo(String,UnknownSize)),false,5)",
      s"String: cacheCompile: CacheKey($query,Map(n -> ParameterTypeInfo(String,UnknownSize)),false,5)",
      // params2
      s"String: cacheHit: CacheKey($query,Map(n -> ParameterTypeInfo(String,UnknownSize)),false,5)"
    ))
  }

  test(
    "repeating query with same parameters types but different list lengths should hit the execution plan cache, when configured to use size hint"
  ) {
    restartWithConfig(
      databaseConfig() + (GraphDatabaseInternalSettings.cypher_size_hint_parameters -> java.lang.Boolean.TRUE)
    )

    val cacheListener = new LoggingTracer()

    val query = "RETURN $n"
    val params1: Map[String, AnyRef] = Map("n" -> List(42).asJava)
    val params2: Map[String, AnyRef] = Map("n" -> List.fill(1001)(42).asJava)

    graph.withTx(tx => tx.execute(query, params1.asJava).resultAsString())
    graph.withTx(tx => tx.execute(query, params2.asJava).resultAsString())

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // params1
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey($query,Map(n -> ParameterTypeInfo(List<Any>,ExactSize(1))),false,5)",
      s"String: cacheCompile: CacheKey($query,Map(n -> ParameterTypeInfo(List<Any>,ExactSize(1))),false,5)",
      // params2
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyHit,
      s"String: cacheMiss: CacheKey($query,Map(n -> ParameterTypeInfo(List<Any>,ApproximateSize(10000))),false,5)",
      s"String: cacheCompile: CacheKey($query,Map(n -> ParameterTypeInfo(List<Any>,ApproximateSize(10000))),false,5)"
    ))
  }

  test(
    "repeating query with same parameters types but different list lengths should hit the caches, when configured to not use size hint"
  ) {
    restartWithConfig(
      databaseConfig() + (GraphDatabaseInternalSettings.cypher_size_hint_parameters -> java.lang.Boolean.FALSE)
    )

    val cacheListener = new LoggingTracer()

    val query = "RETURN $n"
    val params1: Map[String, AnyRef] = Map("n" -> List(42).asJava)
    val params2: Map[String, AnyRef] = Map("n" -> List.fill(1001)(42).asJava)

    graph.withTx(tx => tx.execute(query, params1.asJava).resultAsString())
    graph.withTx(tx => tx.execute(query, params2.asJava).resultAsString())

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // params1
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey($query,Map(n -> ParameterTypeInfo(List<Any>,UnknownSize)),false,5)",
      s"String: cacheCompile: CacheKey($query,Map(n -> ParameterTypeInfo(List<Any>,UnknownSize)),false,5)",
      // params2
      s"String: cacheHit: CacheKey($query,Map(n -> ParameterTypeInfo(List<Any>,UnknownSize)),false,5)"
    ))
  }

  test("Query with missing parameters should not be cached") {
    val cacheListener = new LoggingTracer()

    val query = "RETURN $n,$m"
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(42))

    for (_ <- 0 to 1) {
      try {
        graph.withTx(tx => tx.execute(query, params1.asJava).resultAsString())
      } catch {
        case qee: QueryExecutionException => qee.getMessage should equal("Expected parameter(s): m")
      }
    }

    // The query is not even run by the AST cache
    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // 1st run
      s"String: cacheMiss: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      s"String: cacheCompile: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      // 2nd run
      s"String: cacheMiss: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      s"String: cacheCompile: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)"
    ))
  }

  test("EXPLAIN query with missing parameters should not be cached - using Notifications API") {
    val cacheListener = new LoggingTracer()

    val actualQuery = "RETURN $n,$m"
    val executedQuery = "EXPLAIN " + actualQuery
    val params: Map[String, AnyRef] = Map("n" -> Long.box(42))

    val notifications = graph.withTx(tx => { tx.execute(executedQuery, params.asJava).getNotifications })
    graph.withTx(tx => { tx.execute(executedQuery, params.asJava).getNotifications })

    var acc = 0
    notifications.asScala.foreach(n => {
      n.getDescription should equal(
        "Did not supply query with enough parameters. The produced query plan will not be cached and is not executable without EXPLAIN. (Missing parameters: m)"
      )
      acc = acc + 1
    })
    acc should be(1)

    // The query is not even run by the AST cache
    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // 1st run
      s"String: cacheMiss: CacheKey($actualQuery,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      s"String: cacheCompile: CacheKey($actualQuery,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      // 2nd run
      s"String: cacheMiss: CacheKey($actualQuery,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      s"String: cacheCompile: CacheKey($actualQuery,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)"
    ))
  }

  test("EXPLAIN query with missing parameters should not be cached - using GqlStatusObject API") {
    val cacheListener = new LoggingTracer()

    val actualQuery = "RETURN $n,$m"
    val executedQuery = "EXPLAIN " + actualQuery
    val params: Map[String, AnyRef] = Map("n" -> Long.box(42))

    val gqlStatusObjects = graph.withTx(tx => { tx.execute(executedQuery, params.asJava).getGqlStatusObjects })
    graph.withTx(tx => { tx.execute(executedQuery, params.asJava).getGqlStatusObjects })

    gqlStatusObjects.asScala.size should be(2)
    gqlStatusObjects.asScala.map(_.gqlStatus) shouldBe List(
      STATUS_01N60.getStatusString,
      STATUS_00001.getStatusString
    )

    // The query is not even run by the AST cache
    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // 1st run
      s"String: cacheMiss: CacheKey($actualQuery,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      s"String: cacheCompile: CacheKey($actualQuery,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      // 2nd run
      s"String: cacheMiss: CacheKey($actualQuery,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      s"String: cacheCompile: CacheKey($actualQuery,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)"
    ))
  }

  test("EXPLAIN Query with enough parameters should be cached") {
    val cacheListener = new LoggingTracer()

    val actualQuery = "RETURN $n,$m"
    val executedQuery = "EXPLAIN " + actualQuery
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(42), "m" -> Long.box(21))

    for (_ <- 0 to 2) graph.withTx(tx => tx.execute(executedQuery, params1.asJava).resultAsString())

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // 1st run
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey($actualQuery,Map(m -> ParameterTypeInfo(Integer,UnknownSize), n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      s"String: cacheCompile: CacheKey($actualQuery,Map(m -> ParameterTypeInfo(Integer,UnknownSize), n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      // 2nd run
      s"String: cacheHit: CacheKey($actualQuery,Map(m -> ParameterTypeInfo(Integer,UnknownSize), n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      // 3rd run
      s"AST:    cacheHit",
      executionPlanCacheKeyMiss,
      s"String: cacheCompileWithExpressionCodeGen: CacheKey($actualQuery,Map(m -> ParameterTypeInfo(Integer,UnknownSize), n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)", // String cache JIT compiles on the first hit
      s"String: cacheHit: CacheKey($actualQuery,Map(m -> ParameterTypeInfo(Integer,UnknownSize), n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)"
    ))
  }

  test("Different expressionEngine in query should not use same executableQuery, but the same LogicalPlan") {
    val cacheListener = new LoggingTracer()

    graph.withTx { tx =>
      tx.execute("CYPHER expressionEngine=interpreted RETURN 42 AS a").resultAsString()
      tx.execute("CYPHER expressionEngine=compiled RETURN 42 AS a").resultAsString()
    }

    cacheListener.expectTrace(List(
      "String: cacheFlushDetected",
      "AST: cacheFlushDetected",
      // 1st run
      "AST: cacheMiss",
      "AST: cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER expressionEngine=interpreted RETURN 42 AS a,Map(),false,5)",
      s"String: cacheCompile: CacheKey(CYPHER expressionEngine=interpreted RETURN 42 AS a,Map(),false,5)",
      // 2nd run
      "AST: cacheHit",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER expressionEngine=compiled RETURN 42 AS a,Map(),false,5)",
      s"String: cacheCompile: CacheKey(CYPHER expressionEngine=compiled RETURN 42 AS a,Map(),false,5)"
    ))
  }

  test("Different operatorEngine in query should not use same executableQuery, but the same LogicalPlan") {
    val cacheListener = new LoggingTracer()

    graph.withTx { tx =>
      tx.execute("CYPHER operatorEngine=interpreted RETURN 42 AS a").resultAsString()
      tx.execute("CYPHER operatorEngine=compiled RETURN 42 AS a").resultAsString()
      tx.execute("CYPHER RETURN 42 AS a").resultAsString()
    }

    cacheListener.expectTrace(List(
      "String: cacheFlushDetected",
      "AST: cacheFlushDetected",
      // 1st run
      "AST: cacheMiss",
      "AST: cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER operatorEngine=interpreted RETURN 42 AS a,Map(),false,5)",
      s"String: cacheCompile: CacheKey(CYPHER operatorEngine=interpreted RETURN 42 AS a,Map(),false,5)",
      // 2nd run
      "AST: cacheHit",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER operatorEngine=compiled RETURN 42 AS a,Map(),false,5)",
      s"String: cacheCompile: CacheKey(CYPHER operatorEngine=compiled RETURN 42 AS a,Map(),false,5)",
      // 3rd run
      "AST: cacheHit",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(RETURN 42 AS a,Map(),false,5)",
      s"String: cacheCompile: CacheKey(RETURN 42 AS a,Map(),false,5)"
    ))
  }

  test("Different parallelRuntimeConfig should use different executableQuery and logicalPlan") {
    val cacheListener = new LoggingTracer()

    graph.withTx { tx =>
      tx.execute("CYPHER runtime=parallel MATCH (n:L) RETURN n ORDER BY n").resultAsString()
      tx.execute(
        "CYPHER runtime=parallel parallelRuntimeConfig=leverageOrder MATCH (n:L) RETURN n ORDER BY n"
      ).resultAsString()
    }

    cacheListener.expectTrace(List(
      "String: cacheFlushDetected",
      "AST:    cacheFlushDetected",
      // 1st run
      "AST:    cacheMiss",
      "AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER runtime=parallel MATCH (n:L) RETURN n ORDER BY n,Map(),false,5)",
      s"String: cacheCompile: CacheKey(CYPHER runtime=parallel MATCH (n:L) RETURN n ORDER BY n,Map(),false,5)",
      // 2nd run
      "AST:    cacheMiss",
      "AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER runtime=parallel parallelRuntimeConfig=leverageorder MATCH (n:L) RETURN n ORDER BY n,Map(),false,5)",
      s"String: cacheCompile: CacheKey(CYPHER runtime=parallel parallelRuntimeConfig=leverageorder MATCH (n:L) RETURN n ORDER BY n,Map(),false,5)"
    ))
  }

  test("Different runtime in query should not use same plan") {
    val cacheListener = new LoggingTracer()

    graph.withTx { tx =>
      tx.execute("CYPHER runtime=interpreted RETURN 42 AS a").resultAsString()
      tx.execute("CYPHER runtime=slotted RETURN 42 AS a").resultAsString()
    }

    cacheListener.expectTrace(List(
      "String: cacheFlushDetected",
      "AST:    cacheFlushDetected",
      // 1st run
      "AST:    cacheMiss",
      "AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER runtime=interpreted RETURN 42 AS a,Map(),false,5)",
      s"String: cacheCompile: CacheKey(CYPHER runtime=interpreted RETURN 42 AS a,Map(),false,5)",
      // 2nd run
      "AST:    cacheFlushDetected", // Different runtimes actually use different compilers (thus different AST caches), but they write to the same monitor
      "AST:    cacheMiss",
      "AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER runtime=slotted RETURN 42 AS a,Map(),false,5)",
      s"String: cacheCompile: CacheKey(CYPHER runtime=slotted RETURN 42 AS a,Map(),false,5)"
    ))
  }

  test("should cache plans when same parameter appears multiple times") {
    val cacheListener = new LoggingTracer()

    val query = "RETURN $n + $n"
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(42))

    for (_ <- 0 to 2) graph.withTx(tx => tx.execute(query, params1.asJava).resultAsString())

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // 1st run
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      s"String: cacheCompile: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      // 2nd run
      s"String: cacheHit: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      // 3rd run
      s"AST:    cacheHit",
      executionPlanCacheKeyMiss, // JIT compilation forces us to miss here
      s"String: cacheCompileWithExpressionCodeGen: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)", // String cache JIT compiles on the first hit
      s"String: cacheHit: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)"
    ))
  }

  test("No compilation with expression code generation on first attempt") {
    val cacheListener = new LoggingTracer()

    val query = "RETURN $n + 3 < 6"
    val params: Map[String, AnyRef] = Map("n" -> Long.box(42))

    graph.withTx(tx => tx.execute(query, params.asJava).resultAsString())

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      s"String: cacheCompile: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)"
    ))
  }

  test("One and only one compilation with expression code generation after several attempts. LogicalPlan is reused.") {
    val cacheListener = new LoggingTracer()

    val query = "RETURN $n + 3 < 6"
    val params: Map[String, AnyRef] = Map("n" -> Long.box(42))

    graph.withTx(tx => {
      tx.execute(s"CYPHER $query", params.asJava).resultAsString()
      tx.execute(s"CYPHER $query", params.asJava).resultAsString()
      tx.execute(s"CYPHER $query", params.asJava).resultAsString()
      tx.execute(s"CYPHER $query", params.asJava).resultAsString()
      tx.execute(s"CYPHER $query", params.asJava).resultAsString()
    })

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // 1st run
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      s"String: cacheCompile: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      // 2nd run
      s"String: cacheHit: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      // 3rd run
      s"AST:    cacheHit",
      executionPlanCacheKeyMiss,
      s"String: cacheCompileWithExpressionCodeGen: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      s"String: cacheHit: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      // 4th run
      s"String: cacheHit: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      // 5th run
      s"String: cacheHit: CacheKey($query,Map(n -> ParameterTypeInfo(Integer,UnknownSize)),false,5)"
    ))
  }

  test("Changes in transaction state result in cache miss") {
    val cacheListener = new LoggingTracer()

    val query = "RETURN 1"
    val createNodeQuery = "CREATE ()"

    graph.withTx { tx =>
      tx.execute(query).resultAsString()
      tx.execute(createNodeQuery).resultAsString()
      tx.execute(query).resultAsString()
      tx.execute(query).resultAsString()
    }

    cacheListener.expectTrace(List(
      // RETURN 1
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey($query,Map(),false,5)",
      s"String: cacheCompile: CacheKey($query,Map(),false,5)",
      // CREATE ()
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey($createNodeQuery,Map(),false,5)",
      s"String: cacheCompile: CacheKey($createNodeQuery,Map(),false,5)",
      // RETURN 1
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyHit,
      s"String: cacheMiss: CacheKey($query,Map(),true,5)",
      s"String: cacheCompile: CacheKey($query,Map(),true,5)",
      // RETURN 1
      s"String: cacheHit: CacheKey($query,Map(),true,5)"
    ))
  }

  test("auto-parameterized query should not redo physical planning") {
    // ensure label exists
    graph.withTx(tx => tx.createNode(Label.label("Person")))

    val cacheListener = new LoggingTracer()

    graph.withTx(tx => tx.execute("RETURN 42 AS n").next().get("n") should equal(42))
    graph.withTx(tx => tx.execute("RETURN 43 AS n").next().get("n") should equal(43))

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // 1st run
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(RETURN 42 AS n,$empty_parameters,false,5)",
      s"String: cacheCompile: CacheKey(RETURN 42 AS n,$empty_parameters,false,5)",
      // 2nd run
      s"AST:    cacheHit", // no logical planning
      executionPlanCacheKeyHit,
      s"String: cacheMiss: CacheKey(RETURN 43 AS n,$empty_parameters,false,5)",
      s"String: cacheCompile: CacheKey(RETURN 43 AS n,$empty_parameters,false,5)"
    ))
  }

  test("prepend auto-parameterized query with CYPHER should not redo logical/physical planning") {
    // ensure label exists
    graph.withTx(tx => tx.createNode(Label.label("Person")))

    val cacheListener = new LoggingTracer()

    graph.withTx(tx => tx.execute("RETURN 42 AS n").next().get("n") should equal(42))
    graph.withTx(tx => tx.execute("CYPHER RETURN 43 AS n").next().get("n") should equal(43))

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // 1st run
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(RETURN 42 AS n,$empty_parameters,false,5)",
      s"String: cacheCompile: CacheKey(RETURN 42 AS n,$empty_parameters,false,5)",
      // 2nd run
      s"AST:    cacheHit", // no logical planning
      executionPlanCacheKeyHit,
      s"String: cacheMiss: CacheKey(RETURN 43 AS n,$empty_parameters,false,5)",
      s"String: cacheCompile: CacheKey(RETURN 43 AS n,$empty_parameters,false,5)"
    ))
  }

  test("query containing parameters and literals should not redo physical planning") {
    val cacheListener = new LoggingTracer()

    graph.withTx(tx => tx.execute("RETURN 42 + $p AS n", java.util.Map.of("p", 2)).next().get("n") should equal(44))
    graph.withTx(tx => tx.execute("RETURN 43 + $p AS n", java.util.Map.of("p", 4)).next().get("n") should equal(47))

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // 1st run
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      "String: cacheMiss: CacheKey(RETURN 42 + $p AS n,Map(p -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      "String: cacheCompile: CacheKey(RETURN 42 + $p AS n,Map(p -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      // 2nd run
      s"AST:    cacheHit", // no logical planning
      executionPlanCacheKeyHit,
      "String: cacheMiss: CacheKey(RETURN 43 + $p AS n,Map(p -> ParameterTypeInfo(Integer,UnknownSize)),false,5)",
      "String: cacheCompile: CacheKey(RETURN 43 + $p AS n,Map(p -> ParameterTypeInfo(Integer,UnknownSize)),false,5)"
    ))
  }

  test("calling db.clearQueryCaches() should clear everything") {
    // ensure label exists
    graph.withTx(tx => tx.createNode(Label.label("Person")))

    val cacheListener = new LoggingTracer()

    val query = "MATCH (n:Person) RETURN n"
    val clearCacheQuery = "CALL db.clearQueryCaches()"
    graph.withTx(tx => tx.execute(query).resultAsString())
    graph.withTx(tx => tx.execute(query).resultAsString())
    graph.withTx(tx => tx.execute(query).resultAsString())
    graph.withTx(tx => tx.execute(query).resultAsString())
    graph.withTx(tx => tx.execute(clearCacheQuery).resultAsString())
    graph.withTx(tx => tx.execute(query).resultAsString())

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // 1st run
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey($query,$empty_parameters,false,5)",
      s"String: cacheCompile: CacheKey($query,$empty_parameters,false,5)",
      // 2nd run
      s"String: cacheHit: CacheKey($query,$empty_parameters,false,5)",
      // 3rd run
      s"AST:    cacheHit", // no logical planning
      executionPlanCacheKeyMiss,
      s"String: cacheCompileWithExpressionCodeGen: CacheKey($query,$empty_parameters,false,5)", // physical planning
      s"String: cacheHit: CacheKey($query,$empty_parameters,false,5)",
      // 4th run now everything is cached
      s"String: cacheHit: CacheKey($query,$empty_parameters,false,5)",
      // CALL db.clearQueryCaches()
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey($clearCacheQuery,$empty_parameters,false,5)",
      s"String: cacheCompile: CacheKey($clearCacheQuery,$empty_parameters,false,5)",
      s"AST:    cacheFlushDetected",
      executionPlanCacheKeyFlush,
      s"String: cacheFlushDetected",
      // 4th run
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey($query,$empty_parameters,false,5)",
      s"String: cacheCompile: CacheKey($query,$empty_parameters,false,5)"
    ))
  }

  test("Cardinality change should change cache-key") {
    val q = "EXPLAIN MATCH (n) RETURN n"
    var resBefore: Result = null
    var resAfter: Result = null

    graph.withTx(tx => {
      resBefore = tx.execute(q)
      resBefore.resultAsString()
    })

    graph.withTx(tx => {
      for (_ <- 0 to 10000) tx.createNode()
    })

    graph.withTx(tx => {
      resAfter = tx.execute(q)
      resAfter.resultAsString()
    })

    val estimatedRowsBefore = resBefore.getExecutionPlanDescription.getArguments.get("EstimatedRows")
    val estimatedRowsAfter = resAfter.getExecutionPlanDescription.getArguments.get("EstimatedRows")
    estimatedRowsBefore should not be estimatedRowsAfter
  }

  // TODO Add similar test case with ALTER DATABASE SET DEFAULT LANGAUGE
  test("cypher version option") {
    val cacheListener = new LoggingTracer()

    graph.withTx(tx => tx.execute("RETURN 42 AS n").next().get("n") should equal(42))
    graph.withTx(tx => tx.execute("RETURN 42 AS `n`").next().get("n") should equal(42))
    graph.withTx(tx => tx.execute("CYPHER 5 RETURN 42 AS n").next().get("n") should equal(42))
    graph.withTx(tx => tx.execute("CYPHER 25 RETURN 42 AS n").next().get("n") should equal(42))

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // RETURN 42 AS n
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(RETURN 42 AS n,$empty_parameters,false,5)",
      s"String: cacheCompile: CacheKey(RETURN 42 AS n,$empty_parameters,false,5)",
      // RETURN 42 AS `n`
      s"AST:    cacheHit",
      executionPlanCacheKeyHit,
      s"String: cacheMiss: CacheKey(RETURN 42 AS `n`,$empty_parameters,false,5)",
      s"String: cacheCompile: CacheKey(RETURN 42 AS `n`,$empty_parameters,false,5)",
      // CYPHER 5 RETURN 42 AS n
      s"String: cacheHit: CacheKey(RETURN 42 AS n,$empty_parameters,false,5)",
      // CYPHER 25 RETURN 42 AS n
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(RETURN 42 AS n,$empty_parameters,false,25)",
      s"String: cacheCompile: CacheKey(RETURN 42 AS n,$empty_parameters,false,25)"
    ))
  }

  def executionPlanCacheKeyHit: String
  def executionPlanCacheKeyMiss: String
  def executionPlanCacheKeyFlush: String

  private class LoggingTracer(
    traceAstLogicalPlanCache: Boolean = true,
    traceExecutionEngineQueryCache: Boolean = true,
    traceExecutionPlanCache: Boolean = true
  ) {

    private class LoggingCacheTracer[Key](name: String, logKey: Boolean) extends CacheTracer[Key] {
      override def cacheHit(key: Key, metaData: String): Unit = log += s"$name: cacheHit" + keySuffix(key)
      override def cacheMiss(key: Key, metaData: String): Unit = log += s"$name: cacheMiss" + keySuffix(key)

      override def compute(key: Key, codeGenSize: Long, metaData: String): Unit =
        log += s"$name: cacheCompile" + keySuffix(key)

      override def computeWithExpressionCodeGen(key: Key, codeGenSize: Long, metaData: String): Unit =
        log += s"$name: cacheCompileWithExpressionCodeGen" + keySuffix(key)

      override def cacheStale(
        key: Key,
        secondsSincePlan: Int,
        metaData: String,
        maybeReason: Option[String]
      ): Unit = log += s"$name: cacheStale" + keySuffix(key)
      override def cacheFlush(sizeBeforeFlush: Long): Unit = log += s"$name: cacheFlushDetected"

      private def keySuffix(key: Key): String = if (logKey) s": $key" else ""
    }

    if (traceAstLogicalPlanCache) {
      CypherQueryCaches.LogicalPlanCache.addMonitorListener(
        kernelMonitors,
        new LoggingCacheTracer[CypherQueryCaches.LogicalPlanCache.Key]("AST", logKey = false)
      )
    }

    if (traceExecutionEngineQueryCache) {
      CypherQueryCaches.ExecutableQueryCache.addMonitorListener(
        kernelMonitors,
        new LoggingCacheTracer[CypherQueryCaches.ExecutableQueryCache.Key]("String", logKey = true)
      )
    }

    if (traceExecutionPlanCache) {
      CypherQueryCaches.ExecutionPlanCache.addMonitorListener(
        kernelMonitors,
        new LoggingCacheTracer[CypherQueryCaches.ExecutionPlanCache.Key]("ExecutionPlanCacheKey", logKey = false)
      )
    }

    private val log: mutable.Builder[String, List[String]] = List.newBuilder

    private def trace: Seq[String] = log.result()

    def clear(): Unit = {
      log.clear()
    }

    def expectTrace(expected: List[String]): Unit = {
      val actual = trace.map(str => str.replaceAll("\\s+", " "))
      val expectedFormatted = expected.filterNot(_.isEmpty).map(str => str.replaceAll("\\s+", " "))
      withClue(s"Actual:\n${actual.mkString("\n")}\n\nExpected:\n${expectedFormatted.mkString("\n")}\n") {
        actual should equal(expectedFormatted)
      }
    }
  }
}

class DefaultQueryCachingTest extends QueryCachingTest() {
  override def executionPlanCacheKeyHit: String = "ExecutionPlanCacheKey: cacheHit"

  override def executionPlanCacheKeyMiss: String = "ExecutionPlanCacheKey: cacheMiss"

  override def executionPlanCacheKeyFlush: String = "ExecutionPlanCacheKey: cacheFlushDetected"
}

class NoExecutablePlanQueryCachingTest extends QueryCachingTest(0) {
  override def executionPlanCacheKeyHit: String = ""

  override def executionPlanCacheKeyMiss: String = ""

  override def executionPlanCacheKeyFlush: String = ""
}
