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
package org.neo4j.cypher

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.CacheTracer
import org.neo4j.cypher.internal.ExecutionEngineQueryCacheMonitor
import org.neo4j.cypher.internal.ExecutionPlanCacheKey
import org.neo4j.cypher.internal.ExecutionPlanCacheTracer
import org.neo4j.cypher.internal.QueryCache.CacheKey
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.config.Setting
import org.scalatest.prop.TableDrivenPropertyChecks

import java.time.Duration
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable

abstract class QueryCachingTest(executionPlanCacheSize: Int = GraphDatabaseInternalSettings.query_execution_plan_cache_size.defaultValue()) extends CypherFunSuite with GraphDatabaseTestSupport with TableDrivenPropertyChecks {

  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(
    // String cache JIT compiles on the first hit
    GraphDatabaseInternalSettings.cypher_expression_recompilation_limit -> Integer.valueOf(2),
    GraphDatabaseInternalSettings.cypher_enable_runtime_monitors -> java.lang.Boolean.TRUE,
    GraphDatabaseInternalSettings.query_execution_plan_cache_size -> Integer.valueOf(executionPlanCacheSize),
    GraphDatabaseSettings.cypher_min_replan_interval -> Duration.ofSeconds(0)
  )

  private val currentVersion = "4.4"
  private val previousMinor = "4.3"
  private val previousMajor = "3.5"
  private val empty_parameters = "Map()"

  test("AstLogicalPlanCache re-uses cached plan across different execution modes") {
    // ensure label exists
    graph.withTx( tx => tx.createNode(Label.label("Person")) )

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
        graph.withTx( tx => {
          tx.kernelTransaction().schemaRead().schemaStateFlush()
        } )

        graph.withTx( tx => tx.execute(firstQuery).resultAsString() )
        // run first query twice in order to reach recompilation limit - otherwise we will get the plan from the query string cache
        graph.withTx( tx => tx.execute(firstQuery).resultAsString() )
        graph.withTx( tx => tx.execute(secondQuery).resultAsString() )

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
    graph.withTx( tx => tx.createNode(Label.label("Person")) )

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
        graph.withTx( tx => {
          tx.kernelTransaction().schemaRead().schemaStateFlush()
        } )

        graph.withTx( tx => tx.execute(firstQuery).resultAsString() )
        graph.withTx( tx => tx.execute(secondQuery).resultAsString() )
        graph.withTx( tx => tx.execute(thirdQuery).resultAsString() )

        cacheListener.expectTrace(List(
          s"String: cacheFlushDetected",
          // firstQuery
          s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)",
          s"String: cacheCompile: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)",
          // secondQuery
          s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)",
          // thirdQuery
          s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)",
          s"String: cacheCompileWithExpressionCodeGen: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)", // String cache JIT compiles on the second hit
        ))
    }
  }

  test("normal execution followed by profile triggers physical planning but not logical planning") {
    // ensure label exists
    graph.withTx( tx => tx.createNode(Label.label("Person")) )

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
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)",
      // profileQuery
      s"AST:    cacheHit", // no logical planning
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion PROFILE $query,$empty_parameters,false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion PROFILE $query,$empty_parameters,false)", // physical planning
    ))
  }

  test("profile followed by normal execution triggers physical planning but not logical planning") {
    // ensure label exists
    graph.withTx( tx => tx.createNode(Label.label("Person")) )

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
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion PROFILE $query,$empty_parameters,false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion PROFILE $query,$empty_parameters,false)",
      // query
      s"AST:    cacheHit", // no logical planning
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)", // physical planning
    ))
  }

  test("Different String but same AST should hit AstExecutableQueryCache and ExecutionPlanCache but miss ExecutionEngineQueryCache") {
    // ensure label exists
    graph.withTx( tx => tx.createNode(Label.label("Person")) )

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
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query1,$empty_parameters,false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $query1,$empty_parameters,false)",
      // query2
      s"AST:    cacheHit", // Same AST, we should hit the cache,
      executionPlanCacheKeyHit,//same plan should hit the cache
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query2,$empty_parameters,false)", // Different string, we should miss the cache
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $query2,$empty_parameters,false)",
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
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      // second
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      // third
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"AST:    cacheHit",
      executionPlanCacheKeyMiss,
      s"String: cacheCompileWithExpressionCodeGen: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)", // String cache JIT compiles on the first hit
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
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)",
      // 2nd run
      s"AST:    cacheMiss",
      s"AST:    cacheCompileWithExpressionCodeGen", // replan=force calls into a method for immediate recompilation, even though recompilation is doing the same steps in the AST cache, but the tracer calls are unaware of that.
      executionPlanCacheKeyMiss,// we will miss here since we need to have reached the recompilation limit
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)",
      s"String: cacheCompileWithExpressionCodeGen: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)",
      // 3rd run
      s"AST:    cacheMiss",
      s"AST:    cacheCompileWithExpressionCodeGen",
      executionPlanCacheKeyHit,//since we get the same plan we will have a hit here
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)",
      s"String: cacheCompileWithExpressionCodeGen: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)",
    ))
  }

  test("repeating query with same parameter types but different values should hit the caches") {
    val cacheListener = new LoggingTracer()

    val query = "RETURN $n"
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(12))
    val params2: Map[String, AnyRef] = Map("n" -> Long.box(42))
    val params3: Map[String, AnyRef] = Map("n" -> Long.box(1337))
    graph.withTx( tx => tx.execute(query, params1.asJava).resultAsString() )
    graph.withTx( tx => tx.execute(query, params2.asJava).resultAsString() )
    graph.withTx( tx => tx.execute(query, params3.asJava).resultAsString() )

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // params1
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      // params2
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      // params3
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"AST:    cacheHit",
      executionPlanCacheKeyMiss, // recompilation limit reached
      s"String: cacheCompileWithExpressionCodeGen: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",  // String cache JIT compiles on the first hit
    ))
  }

  test("repeating query with different parameters types should not hit the caches") {
    val cacheListener = new LoggingTracer()

    val query = "RETURN $n"
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(42))
    val params2: Map[String, AnyRef] = Map("n" -> "nope")

    graph.withTx( tx => tx.execute(query, params1.asJava).resultAsString() )
    graph.withTx( tx => tx.execute(query, params2.asJava).resultAsString() )

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // params1
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      // params2
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.UTF8StringValue),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.UTF8StringValue),false)",
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
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      // 2nd run
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
    ))
  }

  test("EXPLAIN Query with missing parameters should not be cached") {
    val cacheListener = new LoggingTracer()

    val actualQuery = "RETURN $n,$m"
    val executedQuery = "EXPLAIN " + actualQuery
    val params: Map[String, AnyRef] = Map("n" -> Long.box(42))

    val notifications = graph.withTx( tx => { tx.execute(executedQuery, params.asJava).getNotifications } )
    graph.withTx( tx => { tx.execute(executedQuery, params.asJava).getNotifications } )

    var acc = 0
    notifications.asScala.foreach(n => {
      n.getDescription should equal(
        "Did not supply query with enough parameters. The produced query plan will not be cached and is not executable without EXPLAIN. (Missing parameters: m)"
      )
      acc = acc + 1
    })
    acc should be (1)

    // The query is not even run by the AST cache
    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // 1st run
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $actualQuery,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $actualQuery,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      // 2nd run
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $actualQuery,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $actualQuery,Map(n -> class org.neo4j.values.storable.LongValue),false)",
    ))
  }

  test("EXPLAIN Query with enough parameters should be cached") {
    val cacheListener = new LoggingTracer()

    val actualQuery = "RETURN $n,$m"
    val executedQuery = "EXPLAIN " + actualQuery
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(42), "m" -> Long.box(21))

    for (_ <- 0 to 2) graph.withTx( tx => tx.execute(executedQuery, params1.asJava).resultAsString() )

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // 1st run
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $actualQuery,Map(m -> class org.neo4j.values.storable.LongValue, n -> class org.neo4j.values.storable.LongValue),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $actualQuery,Map(m -> class org.neo4j.values.storable.LongValue, n -> class org.neo4j.values.storable.LongValue),false)",
      // 2nd run
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $actualQuery,Map(m -> class org.neo4j.values.storable.LongValue, n -> class org.neo4j.values.storable.LongValue),false)",
      // 3rd run
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $actualQuery,Map(m -> class org.neo4j.values.storable.LongValue, n -> class org.neo4j.values.storable.LongValue),false)",
      s"AST:    cacheHit",
      executionPlanCacheKeyMiss,
      s"String: cacheCompileWithExpressionCodeGen: CacheKey(CYPHER $currentVersion $actualQuery,Map(m -> class org.neo4j.values.storable.LongValue, n -> class org.neo4j.values.storable.LongValue),false)", // String cache JIT compiles on the first hit
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
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion expressionEngine=interpreted RETURN 42 AS a,Map(),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion expressionEngine=interpreted RETURN 42 AS a,Map(),false)",
      // 2nd run
      "AST: cacheHit",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion expressionEngine=compiled RETURN 42 AS a,Map(),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion expressionEngine=compiled RETURN 42 AS a,Map(),false)",
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
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion operatorEngine=interpreted RETURN 42 AS a,Map(),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion operatorEngine=interpreted RETURN 42 AS a,Map(),false)",
      // 2nd run
      "AST: cacheHit",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion operatorEngine=compiled RETURN 42 AS a,Map(),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion operatorEngine=compiled RETURN 42 AS a,Map(),false)",
      // 3rd run
      "AST: cacheHit",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion RETURN 42 AS a,Map(),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion RETURN 42 AS a,Map(),false)",
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
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion runtime=interpreted RETURN 42 AS a,Map(),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion runtime=interpreted RETURN 42 AS a,Map(),false)",
      // 2nd run
      "AST:    cacheFlushDetected", // Different runtimes actually use different compilers (thus different AST caches), but they write to the same monitor
      "AST:    cacheMiss",
      "AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion runtime=slotted RETURN 42 AS a,Map(),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion runtime=slotted RETURN 42 AS a,Map(),false)",
    ))
  }

  test("should cache plans when same parameter appears multiple times") {
    val cacheListener = new LoggingTracer()

    val query = "RETURN $n + $n"
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(42))

    for (_ <- 0 to 2) graph.withTx( tx => tx.execute(query, params1.asJava).resultAsString() )

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // 1st run
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      // 2nd run
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      // 3rd run
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"AST:    cacheHit",
      executionPlanCacheKeyMiss,//JIT compilation forces us to miss here
      s"String: cacheCompileWithExpressionCodeGen: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)", // String cache JIT compiles on the first hit
    ))
  }

  test("No compilation with expression code generation on first attempt") {
    val cacheListener = new LoggingTracer()

    val query = "RETURN $n + 3 < 6"
    val params: Map[String, AnyRef] = Map("n" -> Long.box(42))

    graph.withTx( tx => tx.execute(query, params.asJava).resultAsString() )

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
    ))
  }

  test("One and only one compilation with expression code generation after several attempts. LogicalPlan is reused.") {
    val cacheListener = new LoggingTracer()

    val query = "RETURN $n + 3 < 6"
    val params: Map[String, AnyRef] = Map("n" -> Long.box(42))

    graph.withTx( tx => {
      tx.execute(s"CYPHER $query", params.asJava).resultAsString()
      tx.execute(s"CYPHER $query", params.asJava).resultAsString()
      tx.execute(s"CYPHER $query", params.asJava).resultAsString()
      tx.execute(s"CYPHER $query", params.asJava).resultAsString()
      tx.execute(s"CYPHER $query", params.asJava).resultAsString()
    } )

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // 1st run
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      // 2nd run
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      // 3rd run
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"AST:    cacheHit",
      executionPlanCacheKeyMiss,
      s"String: cacheCompileWithExpressionCodeGen: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      // 4th run
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      // 5th run
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,Map(n -> class org.neo4j.values.storable.LongValue),false)"))
  }

  test("Different cypher version results in cache miss") {
    val cacheListener = new LoggingTracer()

    val query = "RETURN 1"

    graph.withTx( tx => {
      tx.execute(s"CYPHER $query").resultAsString()
      tx.execute(s"CYPHER $previousMinor $query").resultAsString()
      tx.execute(s"CYPHER $previousMajor $query").resultAsString()
      tx.execute(s"CYPHER $currentVersion $query").resultAsString()
      tx.execute(s"CYPHER $query").resultAsString()
      tx.execute(s"CYPHER $previousMinor $query").resultAsString()
      tx.execute(s"CYPHER $previousMajor $query").resultAsString()
      tx.execute(s"CYPHER $currentVersion $query").resultAsString()
      tx.execute(s"CYPHER $query").resultAsString()
      tx.execute(s"CYPHER $previousMinor $query").resultAsString()
      tx.execute(s"CYPHER $previousMajor $query").resultAsString()
      tx.execute(s"CYPHER $currentVersion $query").resultAsString()
      tx.execute(s"CYPHER $query").resultAsString()
      tx.execute(s"CYPHER $previousMinor $query").resultAsString()
      tx.execute(s"CYPHER $previousMajor $query").resultAsString()
      tx.execute(s"CYPHER $currentVersion $query").resultAsString()
    } )

    cacheListener.expectTrace(List(

      // Round 1

      // CYPHER $query
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query,Map(),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $query,Map(),false)",
      // CYPHER $previousMinor $query
      s"AST:    cacheFlushDetected", // Different cypher versions actually use different compilers (thus different AST caches), but they write to the same monitor
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER $previousMinor $query,Map(),false)",
      s"String: cacheCompile: CacheKey(CYPHER $previousMinor $query,Map(),false)",
      // CYPHER $previousMajor $query
      s"AST:    cacheFlushDetected", // Different cypher versions actually use different compilers (thus different AST caches), but they write to the same monitor
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER $previousMajor $query,Map(),false)",
      s"String: cacheCompile: CacheKey(CYPHER $previousMajor $query,Map(),false)",
      // CYPHER $currentVersion $query
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,Map(),false)",

      // Round 2

      // CYPHER $query
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,Map(),false)",
      s"AST: cacheHit",
      executionPlanCacheKeyMiss,
      s"String: cacheCompileWithExpressionCodeGen: CacheKey(CYPHER $currentVersion RETURN 1,Map(),false)",
      // CYPHER $previousMinor $query
      s"String: cacheHit: CacheKey(CYPHER $previousMinor $query,Map(),false)",
      // CYPHER $previousMajor $query
      s"String: cacheHit: CacheKey(CYPHER $previousMajor $query,Map(),false)",
      // CYPHER $currentVersion $query
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,Map(),false)",

      // Round 3

      // CYPHER $query
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,Map(),false)",
      // CYPHER $previousMinor $query
      s"String: cacheHit: CacheKey(CYPHER $previousMinor $query,Map(),false)",
      s"AST: cacheHit",
      executionPlanCacheKeyMiss,
      s"String: cacheCompileWithExpressionCodeGen: CacheKey(CYPHER $previousMinor RETURN 1,Map(),false)",
      // CYPHER $previousMajor $query
      s"String: cacheHit: CacheKey(CYPHER $previousMajor $query,Map(),false)",
      s"AST: cacheHit",
      executionPlanCacheKeyMiss,
      s"String: cacheCompileWithExpressionCodeGen: CacheKey(CYPHER $previousMajor RETURN 1,Map(),false)",
      // CYPHER $currentVersion $query
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,Map(),false)",

      // Round 4 - Now everything is fully cached

      // CYPHER $query
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,Map(),false)",
      // CYPHER $previousMinor $query
      s"String: cacheHit: CacheKey(CYPHER $previousMinor $query,Map(),false)",
      // CYPHER $previousMajor $query
      s"String: cacheHit: CacheKey(CYPHER $previousMajor $query,Map(),false)",
      // CYPHER $currentVersion $query
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,Map(),false)",
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
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query,Map(),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $query,Map(),false)",
      // CREATE ()
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $createNodeQuery,Map(),false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $createNodeQuery,Map(),false)",
      // RETURN 1
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyHit,
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query,Map(),true)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $query,Map(),true)",
      // RETURN 1
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,Map(),true)",
    ))
  }

  test("auto-parameterized query should not redo physical planning") {
    // ensure label exists
    graph.withTx( tx => tx.createNode(Label.label("Person")) )

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
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion RETURN 42 AS n,$empty_parameters,false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion RETURN 42 AS n,$empty_parameters,false)",
      // 2nd run
      s"AST:    cacheHit", // no logical planning
      executionPlanCacheKeyHit,
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion RETURN 43 AS n,$empty_parameters,false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion RETURN 43 AS n,$empty_parameters,false)",
    ))
  }

  test("prepend auto-parameterized query with CYPHER should not redo logical/physical planning") {
    // ensure label exists
    graph.withTx( tx => tx.createNode(Label.label("Person")) )

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
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion RETURN 42 AS n,$empty_parameters,false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion RETURN 42 AS n,$empty_parameters,false)",
      // 2nd run
      s"AST:    cacheHit", // no logical planning
      executionPlanCacheKeyHit,
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion RETURN 43 AS n,$empty_parameters,false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion RETURN 43 AS n,$empty_parameters,false)",
    ))
  }

  test("calling db.clearQueryCaches() should clear everything") {
    // ensure label exists
    graph.withTx( tx => tx.createNode(Label.label("Person")) )

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
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)",
      // 2nd run
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)",
      // 3rd run
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)",
      s"AST:    cacheHit", // no logical planning
      executionPlanCacheKeyMiss,
      s"String: cacheCompileWithExpressionCodeGen: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)", // physical planning
      // 4th run now everything is cached
      s"String: cacheHit: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)",
      // CALL db.clearQueryCaches()
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $clearCacheQuery,$empty_parameters,false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $clearCacheQuery,$empty_parameters,false)",
      s"AST:    cacheFlushDetected",
      s"String: cacheFlushDetected",
      // 4th run
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      executionPlanCacheKeyMiss,
      s"String: cacheMiss: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)",
      s"String: cacheCompile: CacheKey(CYPHER $currentVersion $query,$empty_parameters,false)",
    ))
  }

  test("Cardinality change should change cache-key") {
    val q = "EXPLAIN MATCH (n) RETURN n"
    var resBefore: Result = null
    var resAfter: Result = null

    graph.withTx( tx => {
      resBefore = tx.execute(q)
      resBefore.resultAsString()
    })

    graph.withTx( tx => {
      for (_ <- 0 to 10000) tx.createNode()
    })

    graph.withTx( tx => {
      resAfter = tx.execute(q)
      resAfter.resultAsString()
    })

    val estimatedRowsBefore = resBefore.getExecutionPlanDescription.getArguments.get("EstimatedRows")
    val estimatedRowsAfter = resAfter.getExecutionPlanDescription.getArguments.get("EstimatedRows")
    estimatedRowsBefore should not be estimatedRowsAfter
  }

  def executionPlanCacheKeyHit: String
  def executionPlanCacheKeyMiss: String

  private class LoggingTracer(traceAstLogicalPlanCache: Boolean = true, traceExecutionEngineQueryCache: Boolean = true, traceExecutionPlanCache: Boolean = true)
    extends CacheTracer[CacheKey[Statement]] with ExecutionEngineQueryCacheMonitor with ExecutionPlanCacheTracer {
    kernelMonitors.addMonitorListener(this)

    private val log: mutable.Builder[String, List[String]] = List.newBuilder

    private def trace: Seq[String] = log.result()

    def clear(): Unit = {
      log.clear()
    }

    def expectTrace(expected: List[String]): Unit = {
      val actual = trace.map(str => str.replaceAll("\\s+", " "))
      val expectedFormatted = expected.filterNot(_.isEmpty).map(str => str.replaceAll("\\s+", " "))
      actual should equal(expectedFormatted)
    }

    override def queryCacheHit(queryKey: CacheKey[Statement], metaData: String): Unit = {
      if (traceAstLogicalPlanCache) log += "AST: cacheHit"
    }

    override def queryCacheMiss(queryKey: CacheKey[Statement], metaData: String): Unit = {
      if (traceAstLogicalPlanCache) log += "AST: cacheMiss"
    }

    override def queryCompile(queryKey: CacheKey[Statement], metaData: String): Unit = {
      if (traceAstLogicalPlanCache) log += "AST: cacheCompile"
    }

    override def queryCompileWithExpressionCodeGen(queryKey: CacheKey[Statement], metaData: String): Unit = {
      if (traceAstLogicalPlanCache) log += "AST: cacheCompileWithExpressionCodeGen"
    }

    override def queryCacheStale(queryKey: CacheKey[Statement],
                                 secondsSincePlan: Int,
                                 metaData: String,
                                 maybeReason: Option[String]): Unit = {
      if (traceAstLogicalPlanCache) log += "AST: cacheStale"
    }

    override def queryCacheFlush(sizeOfCacheBeforeFlush: Long): Unit = {
      if (traceAstLogicalPlanCache) log += "AST: cacheFlushDetected"
    }

    override def cacheFlushDetected(sizeBeforeFlush: Long): Unit = {
      if (traceExecutionEngineQueryCache) log += "String: cacheFlushDetected"
    }

    override def cacheHit(key: CacheKey[String]): Unit = {
      if (traceExecutionEngineQueryCache) log += s"String: cacheHit: $key"
    }

    override def cacheMiss(key: CacheKey[String]): Unit = {
      if (traceExecutionEngineQueryCache) log += s"String: cacheMiss: $key"
    }

    override def cacheDiscard(key: CacheKey[String], ignored: String, secondsSinceReplan: Int, maybeReason: Option[String]): Unit = {
      if (traceExecutionEngineQueryCache) log += s"String: cacheDiscard: $key"
    }

    override def cacheCompile(key: CacheKey[String]): Unit = {
      if (traceExecutionEngineQueryCache) log += s"String: cacheCompile: $key"
    }

    override def cacheCompileWithExpressionCodeGen(key: CacheKey[String]): Unit = {
      if (traceExecutionEngineQueryCache) log += s"String: cacheCompileWithExpressionCodeGen: $key"
    }

    override def cacheHit(key: ExecutionPlanCacheKey): Unit = {
      if (traceExecutionPlanCache) log += "ExecutionPlanCacheKey: cacheHit"
    }

    override def cacheMiss(key: ExecutionPlanCacheKey): Unit = {
      if (traceExecutionPlanCache) log += "ExecutionPlanCacheKey: cacheMiss"
    }
  }
}

class DefaultQueryCachingTest extends QueryCachingTest() {
  override def executionPlanCacheKeyHit: String = "ExecutionPlanCacheKey: cacheHit"

  override def executionPlanCacheKeyMiss: String = "ExecutionPlanCacheKey: cacheMiss"
}

class NoExecutablePlanQueryCachingTest extends QueryCachingTest(0) {
  override def executionPlanCacheKeyHit: String = ""

  override def executionPlanCacheKeyMiss: String = ""
}