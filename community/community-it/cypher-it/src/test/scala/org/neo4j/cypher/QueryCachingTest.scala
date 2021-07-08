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
import org.neo4j.configuration.GraphDatabaseInternalSettings.CypherExpressionEngine.ONLY_WHEN_HOT
import org.neo4j.cypher.internal.CacheTracer
import org.neo4j.cypher.internal.ExecutionEngineQueryCacheMonitor
import org.neo4j.cypher.internal.QueryCache.CacheKey
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.config.Setting
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable

class QueryCachingTest extends CypherFunSuite with GraphDatabaseTestSupport with TableDrivenPropertyChecks {

  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(GraphDatabaseInternalSettings.cypher_expression_engine -> ONLY_WHEN_HOT,
                                                               GraphDatabaseInternalSettings.cypher_expression_recompilation_limit -> Integer.valueOf(1))

  private val empty_parameters = "Map()"

  test("re-uses cached plan across different execution modes") {
    // ensure label exists
    graph.withTx( tx => tx.createNode(Label.label("Person")) )

    val cacheListener = new LoggingExecutionEngineQueryCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

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

        val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
        val expected = List(
          s"cacheFlushDetected",
          s"cacheMiss: CacheKey(CYPHER 4.1 $query,$empty_parameters,false)",
          s"cacheCompile: CacheKey(CYPHER 4.1 $query,$empty_parameters,false)",
          s"cacheHit: CacheKey(CYPHER 4.1 $query,$empty_parameters,false)",
          s"cacheCompileWithExpressionCodeGen: CacheKey(CYPHER 4.1 $query,$empty_parameters,false)",
        )

        actual should equal(expected)
    }
  }

  test("repeating query with same parameters should hit the cache") {
    val cacheListener = new LoggingExecutionEngineQueryCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

    val query = "RETURN $n"
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(42))

    graph.withTx(tx => {
      tx.execute(query, params1.asJava).resultAsString()
    })
    graph.withTx(tx => {
      tx.execute(query, params1.asJava).resultAsString()
    })

    val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
    val expected = List(
      s"cacheFlushDetected",
      s"cacheMiss: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"cacheCompile: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"cacheHit: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"cacheCompileWithExpressionCodeGen: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
    )

    actual should equal(expected)
  }

  test("repeating query with replan=force should not hit the cache") {
    val cacheListener = new LoggingExecutionEngineQueryCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

    val query = "RETURN 42"

    graph.withTx(tx => {
      tx.execute(query).resultAsString()
    })
    graph.withTx(tx => {
      tx.execute(s"CYPHER replan=force $query").resultAsString()
    })

    val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
    val expected = List(
      s"cacheFlushDetected",
      s"cacheMiss: CacheKey(CYPHER 4.1 $query,$empty_parameters,false)",
      s"cacheCompile: CacheKey(CYPHER 4.1 $query,$empty_parameters,false)",
      s"cacheMiss: CacheKey(CYPHER 4.1 $query,$empty_parameters,false)",
      s"cacheCompileWithExpressionCodeGen: CacheKey(CYPHER 4.1 $query,$empty_parameters,false)",
    )
    actual should equal(expected)
  }

  test("repeating query with same parameter types but different values should hit the cache") {
    val cacheListener = new LoggingExecutionEngineQueryCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

    val query = "RETURN $n"
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(12))
    val params2: Map[String, AnyRef] = Map("n" -> Long.box(42))
    graph.withTx( tx => tx.execute(query, params1.asJava).resultAsString() )
    graph.withTx( tx => tx.execute(query, params2.asJava).resultAsString() )

    val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
    val expected = List(
      s"cacheFlushDetected",
      s"cacheMiss: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"cacheCompile: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"cacheHit: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"cacheCompileWithExpressionCodeGen: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
    )

    actual should equal(expected)
  }

  test("repeating query with different parameters types should not hit the cache") {
    val cacheListener = new LoggingExecutionEngineQueryCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

    val query = "RETURN $n"
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(42))
    val params2: Map[String, AnyRef] = Map("n" -> "nope")

    graph.withTx( tx => tx.execute(query, params1.asJava).resultAsString() )
    graph.withTx( tx => tx.execute(query, params2.asJava).resultAsString() )

    val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
    val expected = List(
      s"cacheFlushDetected",
      s"cacheMiss: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"cacheCompile: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"cacheMiss: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.UTF8StringValue),false)",
      s"cacheCompile: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.UTF8StringValue),false)",
    )

    actual should equal(expected)
  }

  test("Query with missing parameters should not be cached") {
    val cacheListener = new LoggingExecutionEngineQueryCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

    val query = "RETURN $n, $m"
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(42))

    try {
      graph.withTx( tx => tx.execute(query, params1.asJava).resultAsString() )
    } catch {
      case qee: QueryExecutionException => qee.getMessage should equal("Expected parameter(s): m")
    }

    val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
    val expected = List(
      s"cacheFlushDetected",
      s"cacheMiss: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"cacheCompile: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
    )

    actual should equal(expected)
  }

  test("EXPLAIN Query with missing parameters should not be cached") {
    val cacheListener = new LoggingExecutionEngineQueryCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

    val actualQuery = "RETURN $n, $m"
    val executedQuery = "EXPLAIN " + actualQuery
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(42))

    val notifications = graph.withTx( tx => { tx.execute(executedQuery, params1.asJava).getNotifications } )

    var acc = 0
    notifications.asScala.foreach(n => {
      n.getDescription should equal(
        "Did not supply query with enough parameters. The produced query plan will not be cached and is not executable without EXPLAIN. (Missing parameters: m)"
      )
      acc = acc + 1
    })
    acc should be (1)

    val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
    val expected = List(
      s"cacheFlushDetected",
      s"cacheMiss: CacheKey(CYPHER 4.1 $actualQuery,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"cacheCompile: CacheKey(CYPHER 4.1 $actualQuery,Map(n -> class org.neo4j.values.storable.LongValue),false)",
    )

    actual should equal(expected)
  }

  test("EXPLAIN Query with enough parameters should be cached") {
    val cacheListener = new LoggingExecutionEngineQueryCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

    val actualQuery = "RETURN $n, $m"
    val executedQuery = "EXPLAIN " + actualQuery
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(42), "m" -> Long.box(21))

    graph.withTx( tx => tx.execute(executedQuery, params1.asJava).resultAsString() )

    val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
    val expected = List(
      s"cacheFlushDetected",
      s"cacheMiss: CacheKey(CYPHER 4.1 $actualQuery,Map(m -> class org.neo4j.values.storable.LongValue, n -> class org.neo4j.values.storable.LongValue),false)",
      s"cacheCompile: CacheKey(CYPHER 4.1 $actualQuery,Map(m -> class org.neo4j.values.storable.LongValue, n -> class org.neo4j.values.storable.LongValue),false)",
    )

    actual should equal(expected)
  }

  test("Different expressionEngine in query should not use same plan") {
    val cacheListener = new LoggingExecutionEngineQueryCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

    graph.withTx { tx =>
      tx.execute("CYPHER expressionEngine=interpreted RETURN 42 AS a").resultAsString()
      tx.execute("CYPHER expressionEngine=compiled RETURN 42 AS a").resultAsString()
    }

    val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
    val expected = List(
      s"cacheFlushDetected",
      "cacheMiss: CacheKey(CYPHER 4.1 expressionEngine=interpreted RETURN 42 AS a,Map(),false)",
      "cacheCompile: CacheKey(CYPHER 4.1 expressionEngine=interpreted RETURN 42 AS a,Map(),false)",
      "cacheMiss: CacheKey(CYPHER 4.1 expressionEngine=compiled RETURN 42 AS a,Map(),false)",
      "cacheCompile: CacheKey(CYPHER 4.1 expressionEngine=compiled RETURN 42 AS a,Map(),false)",
    )

    actual should equal(expected)
  }

  test("Different operatorEngine in query should not use same plan") {
    val cacheListener = new LoggingExecutionEngineQueryCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

    graph.withTx { tx =>
      tx.execute("CYPHER operatorEngine=interpreted RETURN 42 AS a").resultAsString()
      tx.execute("CYPHER operatorEngine=compiled RETURN 42 AS a").resultAsString()
    }

    val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
    val expected = List(
      s"cacheFlushDetected",
      "cacheMiss: CacheKey(CYPHER 4.1 operatorEngine=interpreted RETURN 42 AS a,Map(),false)",
      "cacheCompile: CacheKey(CYPHER 4.1 operatorEngine=interpreted RETURN 42 AS a,Map(),false)",
      "cacheMiss: CacheKey(CYPHER 4.1 RETURN 42 AS a,Map(),false)",
      "cacheCompile: CacheKey(CYPHER 4.1 RETURN 42 AS a,Map(),false)",
    )

    actual should equal(expected)
  }

  test("Different runtime in query should not use same plan") {
    val cacheListener = new LoggingExecutionEngineQueryCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

    graph.withTx { tx =>
      tx.execute("CYPHER runtime=interpreted RETURN 42 AS a").resultAsString()
      tx.execute("CYPHER runtime=slotted RETURN 42 AS a").resultAsString()
    }

    val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
    val expected = List(
      s"cacheFlushDetected",
      "cacheMiss: CacheKey(CYPHER 4.1 runtime=interpreted RETURN 42 AS a,Map(),false)",
      "cacheCompile: CacheKey(CYPHER 4.1 runtime=interpreted RETURN 42 AS a,Map(),false)",
      "cacheMiss: CacheKey(CYPHER 4.1 runtime=slotted RETURN 42 AS a,Map(),false)",
      "cacheCompile: CacheKey(CYPHER 4.1 runtime=slotted RETURN 42 AS a,Map(),false)",
    )

    actual should equal(expected)
  }

  test("should cache plans when same parameter appears multiple times") {
    val cacheListener = new LoggingExecutionEngineQueryCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

    val query = "RETURN $n + $n"
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(42))

    graph.withTx( tx => tx.execute(query, params1.asJava).resultAsString() )

    val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
    val expected = List(
      s"cacheFlushDetected",
      s"cacheMiss: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"cacheCompile: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
    )

    actual should equal(expected)
  }

  test("No compilation with expression code generation on first attempt") {
    val cacheListener = new LoggingExecutionEngineQueryCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

    val query = "RETURN $n + 3 < 6"
    val params: Map[String, AnyRef] = Map("n" -> Long.box(42))

    graph.withTx( tx => tx.execute(query, params.asJava).resultAsString() )

    val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
    val expected = List(
      s"cacheFlushDetected",
      s"cacheMiss: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"cacheCompile: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
    )

    actual should equal(expected)
  }

  test("One and only one compilation with expression code generation after several attempts") {
    val cacheListener = new LoggingExecutionEngineQueryCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

    val query = "RETURN $n + 3 < 6"
    val params: Map[String, AnyRef] = Map("n" -> Long.box(42))

    graph.withTx( tx => {
      tx.execute(s"CYPHER $query", params.asJava).resultAsString()
      tx.execute(s"CYPHER $query", params.asJava).resultAsString()
      tx.execute(s"CYPHER $query", params.asJava).resultAsString()
      tx.execute(s"CYPHER $query", params.asJava).resultAsString()
      tx.execute(s"CYPHER $query", params.asJava).resultAsString()
    } )

    val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
    val expected = List(
      s"cacheFlushDetected",
      s"cacheMiss: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"cacheCompile: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"cacheHit: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"cacheCompileWithExpressionCodeGen: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"cacheHit: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"cacheHit: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)",
      s"cacheHit: CacheKey(CYPHER 4.1 $query,Map(n -> class org.neo4j.values.storable.LongValue),false)")

    actual should equal(expected)
  }

  test("Changes in transaction state result in cache miss") {
    val cacheListener = new LoggingExecutionEngineQueryCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

    val query = "RETURN 1"
    val createNodeQuery = "CREATE ()"

    graph.withTx { tx =>
      tx.execute(query).resultAsString()
      tx.execute(createNodeQuery).resultAsString()
      tx.execute(query).resultAsString()
      tx.execute(query).resultAsString()
    }

    val currentVersion = "4.1"
    val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
    val expected = List(
      // RETURN 1
      s"cacheFlushDetected",
      s"cacheMiss: CacheKey(CYPHER $currentVersion $query,Map(),false)",
      s"cacheCompile: CacheKey(CYPHER $currentVersion $query,Map(),false)",
      // CREATE ()
      s"cacheMiss: CacheKey(CYPHER $currentVersion $createNodeQuery,Map(),false)",
      s"cacheCompile: CacheKey(CYPHER $currentVersion $createNodeQuery,Map(),false)",
      // RETURN 1
      s"cacheMiss: CacheKey(CYPHER $currentVersion $query,Map(),true)",
      s"cacheCompile: CacheKey(CYPHER $currentVersion $query,Map(),true)",
      // RETURN 1
      s"cacheHit: CacheKey(CYPHER $currentVersion $query,Map(),true)",
      s"cacheCompileWithExpressionCodeGen: CacheKey(CYPHER $currentVersion $query,Map(),true)"
    )

    actual should equal(expected)
  }

  private class LogginAstLogicalPlanCacheTracer extends CacheTracer[CacheKey[Statement]] {
    private val log: mutable.Builder[String, List[String]] = List.newBuilder

    def trace: Seq[String] = log.result()

    def clear(): Unit = {
      log.clear()
    }

    override def queryCacheHit(queryKey: CacheKey[Statement], metaData: String): Unit = {
      log += "cacheHit"
    }

    override def queryCacheMiss(queryKey: CacheKey[Statement], metaData: String): Unit = {
      log += "cacheMiss"
    }

    override def queryCompile(queryKey: CacheKey[Statement], metaData: String): Unit = {
      log += "cacheCompile"
    }

    override def queryCompileWithExpressionCodeGen(queryKey: CacheKey[Statement], metaData: String): Unit = {
      log += "cacheCompileWithExpressionCodeGen"
    }

    override def queryCacheStale(queryKey: CacheKey[Statement],
                                 secondsSincePlan: Int,
                                 metaData:  String,
                                 maybeReason:  Option[String]): Unit = {
      log += s"cacheStale"
    }

    override def queryCacheFlush(sizeOfCacheBeforeFlush: Long): Unit = {
      log += s"cacheFlushDetected"
    }
}

  private class LoggingExecutionEngineQueryCacheListener extends ExecutionEngineQueryCacheMonitor {
    private val log: mutable.Builder[String, List[String]] = List.newBuilder

    def trace: Seq[String] = log.result()

    def clear(): Unit = {
      log.clear()
    }

    override def cacheFlushDetected(sizeBeforeFlush: Long): Unit = {
      log += s"cacheFlushDetected"
    }

    override def cacheHit(key: CacheKey[String]): Unit = {
      log += s"cacheHit: $key"
    }

    override def cacheMiss(key: CacheKey[String]): Unit = {
      log += s"cacheMiss: $key"
    }

    override def cacheDiscard(key: CacheKey[String], ignored: String, secondsSinceReplan: Int, maybeReason: Option[String]): Unit = {
      log += s"cacheDiscard: $key"
    }

    override def cacheCompile(key: CacheKey[String]): Unit = {
      log += s"cacheCompile: $key"
    }

    override def cacheCompileWithExpressionCodeGen(key: CacheKey[String]): Unit = {
      log += s"cacheCompileWithExpressionCodeGen: $key"
    }
  }
}
