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
import org.neo4j.cypher.internal.CacheTracer
import org.neo4j.cypher.internal.ExecutionEngineQueryCacheMonitor
import org.neo4j.cypher.internal.QueryCache.ParameterTypeMap
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.helpers.collection.Pair
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable

class QueryCachingTest extends CypherFunSuite with GraphDatabaseTestSupport with TableDrivenPropertyChecks {

  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(
    // String cache JIT compiles on the first hit
    GraphDatabaseInternalSettings.cypher_expression_recompilation_limit -> Integer.valueOf(1)
  )

  private val currentVersion = "4.4"
  private val previousMinor = "4.3"
  private val previousMajor = "3.5"
  private val empty_parameters = "Map()"

  test("AstLogicalPlanCache re-uses cached plan across different execution modes") {
    // ensure label exists
    graph.withTx( tx => tx.createNode(Label.label("Person")) )

    val cacheListener = new LoggingTracer(traceExecutionEngineQueryCache = false)

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

    val cacheListener = new LoggingTracer(traceAstLogicalPlanCache = false)

    val query = "MATCH (n:Person) RETURN n"
    val explainQuery = s"EXPLAIN $query"

    val modeCombinations = Table(
      ("firstQuery", "secondQuery"),
      (query, query),
      (query, explainQuery),

      (explainQuery, query),
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
        graph.withTx( tx => tx.execute(secondQuery).resultAsString() )

        cacheListener.expectTrace(List(
          s"String: cacheFlushDetected",
          // firstQuery
          s"String: cacheMiss: (CYPHER $currentVersion $query, $empty_parameters)",
          s"String: cacheCompile: (CYPHER $currentVersion $query, $empty_parameters)",
          // secondQuery
          s"String: cacheHit: (CYPHER $currentVersion $query, $empty_parameters)",
          s"String: cacheCompileWithExpressionCodeGen: (CYPHER $currentVersion $query, $empty_parameters)", // String cache JIT compiles on the first hit
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
      s"String: cacheMiss: (CYPHER $currentVersion $query, $empty_parameters)",
      s"String: cacheCompile: (CYPHER $currentVersion $query, $empty_parameters)",
      // profileQuery
      s"AST:    cacheHit", // no logical planning
      s"String: cacheMiss: (CYPHER $currentVersion PROFILE $query, $empty_parameters)",
      s"String: cacheCompile: (CYPHER $currentVersion PROFILE $query, $empty_parameters)", // physical planning
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
      s"String: cacheMiss: (CYPHER $currentVersion PROFILE $query, $empty_parameters)",
      s"String: cacheCompile: (CYPHER $currentVersion PROFILE $query, $empty_parameters)",
      // query
      s"AST:    cacheHit", // no logical planning
      s"String: cacheMiss: (CYPHER $currentVersion $query, $empty_parameters)",
      s"String: cacheCompile: (CYPHER $currentVersion $query, $empty_parameters)", // physical planning
    ))
  }

  test("Different String but same AST should hit AstExecutableQueryCache but miss ExecutionEngineQueryCache") {
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
      s"String: cacheMiss: (CYPHER $currentVersion $query1, $empty_parameters)",
      s"String: cacheCompile: (CYPHER $currentVersion $query1, $empty_parameters)",
      // query2
      s"AST:    cacheHit", // Same AST, we should hit the cache
      s"String: cacheMiss: (CYPHER $currentVersion $query2, $empty_parameters)", // Different string, we should miss the cache
      s"String: cacheCompile: (CYPHER $currentVersion $query2, $empty_parameters)",
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

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // params1
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      s"String: cacheMiss: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"String: cacheCompile: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      // params2
      s"String: cacheHit: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"AST:    cacheHit",
      s"String: cacheCompileWithExpressionCodeGen: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))", // String cache JIT compiles on the first hit
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
      s"String: cacheMiss: (CYPHER $currentVersion $query, $empty_parameters)",
      s"String: cacheCompile: (CYPHER $currentVersion $query, $empty_parameters)",
      // 2nd run
      s"AST:    cacheMiss",
      s"AST:    cacheCompileWithExpressionCodeGen", // replan=force calls into a method for immediate recompilation, even though recompilation is doing the same steps in the AST cache, but the tracer calls are unaware of that.
      s"String: cacheMiss: (CYPHER $currentVersion $query, $empty_parameters)",
      s"String: cacheCompileWithExpressionCodeGen: (CYPHER $currentVersion $query, $empty_parameters)",
      // 3rd run
      s"AST:    cacheMiss",
      s"AST:    cacheCompileWithExpressionCodeGen",
      s"String: cacheMiss: (CYPHER $currentVersion $query, $empty_parameters)",
      s"String: cacheCompileWithExpressionCodeGen: (CYPHER $currentVersion $query, $empty_parameters)",
    ))
  }

  test("repeating query with same parameter types but different values should hit the caches") {
    val cacheListener = new LoggingTracer()

    val query = "RETURN $n"
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(12))
    val params2: Map[String, AnyRef] = Map("n" -> Long.box(42))
    graph.withTx( tx => tx.execute(query, params1.asJava).resultAsString() )
    graph.withTx( tx => tx.execute(query, params2.asJava).resultAsString() )

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // params1
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      s"String: cacheMiss: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"String: cacheCompile: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      // params2
      s"String: cacheHit: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"AST:    cacheHit",
      s"String: cacheCompileWithExpressionCodeGen: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",  // String cache JIT compiles on the first hit
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
      s"String: cacheMiss: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"String: cacheCompile: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      // params2
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      s"String: cacheMiss: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.UTF8StringValue))",
      s"String: cacheCompile: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.UTF8StringValue))",
    ))
  }

  test("Query with missing parameters should not be cached") {
    val cacheListener = new LoggingTracer()

    val query = "RETURN $n, $m"
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
      s"String: cacheMiss: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"String: cacheCompile: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      // 2nd run
      s"String: cacheMiss: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"String: cacheCompile: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
    ))
  }

  test("EXPLAIN Query with missing parameters should not be cached") {
    val cacheListener = new LoggingTracer()

    val actualQuery = "RETURN $n, $m"
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
      s"String: cacheMiss: (CYPHER $currentVersion $actualQuery, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"String: cacheCompile: (CYPHER $currentVersion $actualQuery, Map(n -> class org.neo4j.values.storable.LongValue))",
      // 2nd run
      s"String: cacheMiss: (CYPHER $currentVersion $actualQuery, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"String: cacheCompile: (CYPHER $currentVersion $actualQuery, Map(n -> class org.neo4j.values.storable.LongValue))",
    ))
  }

  test("EXPLAIN Query with enough parameters should be cached") {
    val cacheListener = new LoggingTracer()

    val actualQuery = "RETURN $n, $m"
    val executedQuery = "EXPLAIN " + actualQuery
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(42), "m" -> Long.box(21))

    for (_ <- 0 to 1) graph.withTx( tx => tx.execute(executedQuery, params1.asJava).resultAsString() )

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // 1st run
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      s"String: cacheMiss: (CYPHER $currentVersion $actualQuery, Map(m -> class org.neo4j.values.storable.LongValue, n -> class org.neo4j.values.storable.LongValue))",
      s"String: cacheCompile: (CYPHER $currentVersion $actualQuery, Map(m -> class org.neo4j.values.storable.LongValue, n -> class org.neo4j.values.storable.LongValue))",
      // 2nd run
      s"String: cacheHit: (CYPHER $currentVersion $actualQuery, Map(m -> class org.neo4j.values.storable.LongValue, n -> class org.neo4j.values.storable.LongValue))",
      s"AST:    cacheHit",
      s"String: cacheCompileWithExpressionCodeGen: (CYPHER $currentVersion $actualQuery, Map(m -> class org.neo4j.values.storable.LongValue, n -> class org.neo4j.values.storable.LongValue))", // String cache JIT compiles on the first hit
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
      s"String: cacheMiss: (CYPHER $currentVersion expressionEngine=interpreted RETURN 42 AS a, Map())",
      s"String: cacheCompile: (CYPHER $currentVersion expressionEngine=interpreted RETURN 42 AS a, Map())",
      // 2nd run
      "AST: cacheHit",
      s"String: cacheMiss: (CYPHER $currentVersion expressionEngine=compiled RETURN 42 AS a, Map())",
      s"String: cacheCompile: (CYPHER $currentVersion expressionEngine=compiled RETURN 42 AS a, Map())",
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
      s"String: cacheMiss: (CYPHER $currentVersion operatorEngine=interpreted RETURN 42 AS a, Map())",
      s"String: cacheCompile: (CYPHER $currentVersion operatorEngine=interpreted RETURN 42 AS a, Map())",
      // 2nd run
      "AST: cacheHit",
      s"String: cacheMiss: (CYPHER $currentVersion operatorEngine=compiled RETURN 42 AS a, Map())",
      s"String: cacheCompile: (CYPHER $currentVersion operatorEngine=compiled RETURN 42 AS a, Map())",
      // 3rd run
      "AST: cacheHit",
      s"String: cacheMiss: (CYPHER $currentVersion RETURN 42 AS a, Map())",
      s"String: cacheCompile: (CYPHER $currentVersion RETURN 42 AS a, Map())",
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
      s"String: cacheMiss: (CYPHER $currentVersion runtime=interpreted RETURN 42 AS a, Map())",
      s"String: cacheCompile: (CYPHER $currentVersion runtime=interpreted RETURN 42 AS a, Map())",
      // 2nd run
      "AST:    cacheFlushDetected", // Different runtimes actually use different compilers (thus different AST caches), but they write to the same monitor
      "AST:    cacheMiss",
      "AST:    cacheCompile",
      s"String: cacheMiss: (CYPHER $currentVersion runtime=slotted RETURN 42 AS a, Map())",
      s"String: cacheCompile: (CYPHER $currentVersion runtime=slotted RETURN 42 AS a, Map())",
    ))
  }

  test("should cache plans when same parameter appears multiple times") {
    val cacheListener = new LoggingTracer()

    val query = "RETURN $n + $n"
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(42))

    for (_ <- 0 to 1) graph.withTx( tx => tx.execute(query, params1.asJava).resultAsString() )

    cacheListener.expectTrace(List(
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      // 1st run
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      s"String: cacheMiss: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"String: cacheCompile: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      // 2nd run
      s"String: cacheHit: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"AST:    cacheHit",
      s"String: cacheCompileWithExpressionCodeGen: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))", // String cache JIT compiles on the first hit
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
      s"String: cacheMiss: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"String: cacheCompile: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
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
      s"String: cacheMiss: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"String: cacheCompile: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      // 2nd run
      s"String: cacheHit: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"AST:    cacheHit",
      s"String: cacheCompileWithExpressionCodeGen: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      // 3rd run
      s"String: cacheHit: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      // 4th run
      s"String: cacheHit: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      // 5th run
      s"String: cacheHit: (CYPHER $currentVersion $query, Map(n -> class org.neo4j.values.storable.LongValue))"))
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
    } )

    cacheListener.expectTrace(List(

      // Round 1

      // CYPHER $query
      s"String: cacheFlushDetected",
      s"AST:    cacheFlushDetected",
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      s"String: cacheMiss: (CYPHER $currentVersion $query, Map())",
      s"String: cacheCompile: (CYPHER $currentVersion $query, Map())",
      // CYPHER $previousMinor $query
      s"AST:    cacheFlushDetected", // Different cypher versions actually use different compilers (thus different AST caches), but they write to the same monitor
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      s"String: cacheMiss: (CYPHER $previousMinor $query, Map())",
      s"String: cacheCompile: (CYPHER $previousMinor $query, Map())",
      // CYPHER $previousMajor $query
      s"AST:    cacheFlushDetected", // Different cypher versions actually use different compilers (thus different AST caches), but they write to the same monitor
      s"AST:    cacheMiss",
      s"AST:    cacheCompile",
      s"String: cacheMiss: (CYPHER $previousMajor $query, Map())",
      s"String: cacheCompile: (CYPHER $previousMajor $query, Map())",
      // CYPHER $currentVersion $query
      s"String: cacheHit: (CYPHER $currentVersion $query, Map())",
      s"AST: cacheHit",
      s"String: cacheCompileWithExpressionCodeGen: (CYPHER $currentVersion RETURN 1, Map())",

      // Round 2

      // CYPHER $query
      s"String: cacheHit: (CYPHER $currentVersion $query, Map())",
      // CYPHER $previousMinor $query
      s"String: cacheHit: (CYPHER $previousMinor $query, Map())",
      s"AST: cacheHit",
      s"String: cacheCompileWithExpressionCodeGen: (CYPHER $previousMinor RETURN 1, Map())",
      // CYPHER $previousMajor $query
      s"String: cacheHit: (CYPHER $previousMajor $query, Map())",
      s"AST: cacheHit",
      s"String: cacheCompileWithExpressionCodeGen: (CYPHER $previousMajor RETURN 1, Map())",
      // CYPHER $currentVersion $query
      s"String: cacheHit: (CYPHER $currentVersion $query, Map())",

      // Round 3 - Now everything is fully cached

      // CYPHER $query
      s"String: cacheHit: (CYPHER $currentVersion $query, Map())",
      // CYPHER $previousMinor $query
      s"String: cacheHit: (CYPHER $previousMinor $query, Map())",
      // CYPHER $previousMajor $query
      s"String: cacheHit: (CYPHER $previousMajor $query, Map())",
      // CYPHER $currentVersion $query
      s"String: cacheHit: (CYPHER $currentVersion $query, Map())",
    ))
  }

  private class LoggingTracer(traceAstLogicalPlanCache: Boolean = true, traceExecutionEngineQueryCache: Boolean = true)
    extends CacheTracer[Pair[Pair[String, Statement], ParameterTypeMap]] with ExecutionEngineQueryCacheMonitor {
    kernelMonitors.addMonitorListener(this)

    private val log: mutable.Builder[String, List[String]] = List.newBuilder

    private def trace: Seq[String] = log.result()

    def clear(): Unit = {
      log.clear()
    }

    def expectTrace(expected: List[String]): Unit = {
      val actual = trace.map(str => str.replaceAll("\\s+", " "))
      val expectedFormatted = expected.map(str => str.replaceAll("\\s+", " "))
      actual should equal(expectedFormatted)
    }

    override def queryCacheHit(queryKey: Pair[Pair[String, Statement], ParameterTypeMap],
                               metaData: String): Unit = {
      if (traceAstLogicalPlanCache) log += "AST: cacheHit"
    }

    override def queryCacheMiss(queryKey: Pair[Pair[String, Statement], ParameterTypeMap], metaData: String): Unit = {
      if (traceAstLogicalPlanCache) log += "AST: cacheMiss"
    }

    override def queryCompile(queryKey: Pair[Pair[String, Statement], ParameterTypeMap], metaData: String): Unit = {
      if (traceAstLogicalPlanCache) log += "AST: cacheCompile"
    }

    override def queryCompileWithExpressionCodeGen(queryKey: Pair[Pair[String, Statement], ParameterTypeMap], metaData: String): Unit = {
      if (traceAstLogicalPlanCache) log += "AST: cacheCompileWithExpressionCodeGen"
    }

    override def queryCacheStale(queryKey: Pair[Pair[String, Statement], ParameterTypeMap],
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

    override def cacheHit(key: Pair[String, ParameterTypeMap]): Unit = {
      if (traceExecutionEngineQueryCache) log += s"String: cacheHit: $key"
    }

    override def cacheMiss(key: Pair[String, ParameterTypeMap]): Unit = {
      if (traceExecutionEngineQueryCache) log += s"String: cacheMiss: $key"
    }

    override def cacheDiscard(key: Pair[String, ParameterTypeMap], ignored: String, secondsSinceReplan: Int, maybeReason: Option[String]): Unit = {
      if (traceExecutionEngineQueryCache) log += s"String: cacheDiscard: $key"
    }

    override def cacheCompile(key: Pair[String, ParameterTypeMap]): Unit = {
      if (traceExecutionEngineQueryCache) log += s"String: cacheCompile: $key"
    }

    override def cacheCompileWithExpressionCodeGen(key: Pair[String, ParameterTypeMap]): Unit = {
      if (traceExecutionEngineQueryCache) log += s"String: cacheCompileWithExpressionCodeGen: $key"
    }
  }
}
