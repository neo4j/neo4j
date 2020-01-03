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
package org.neo4j.cypher

import org.neo4j.cypher.internal.QueryCache.ParameterTypeMap
import org.neo4j.cypher.internal.StringCacheMonitor
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.helpers.collection.Pair
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.collection.JavaConversions._
import scala.collection.{Map, mutable}

class QueryCachingTest extends CypherFunSuite with GraphDatabaseTestSupport with TableDrivenPropertyChecks {

  override def databaseConfig(): Map[Setting[_], String] = Map(GraphDatabaseSettings.cypher_expression_engine -> "ONLY_WHEN_HOT",
                                                               GraphDatabaseSettings.cypher_expression_recompilation_limit -> "1")

  test("re-uses cached plan across different execution modes") {
    // ensure label exists
    graph.inTx { graph.createNode(Label.label("Person")) }

    val cacheListener = new LoggingStringCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

    val query = "MATCH (n:Person) RETURN n"
    val profileQuery = s"PROFILE $query"
    val explainQuery = s"EXPLAIN $query"
    val empty_parameters = "Map()"

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

        graph.inTx {
          val statement = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge]).getKernelTransactionBoundToThisThread(true)
          statement.schemaRead().schemaStateFlush()
        }

        graph.execute(firstQuery).resultAsString()
        graph.execute(secondQuery).resultAsString()

        val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
        val expected = List(
          s"cacheFlushDetected",
          s"cacheMiss: (CYPHER 3.5 $query, $empty_parameters)",
          s"cacheHit: (CYPHER 3.5 $query, $empty_parameters)",
          s"cacheRecompile: (CYPHER 3.5 $query, $empty_parameters)",
          s"cacheHit: (CYPHER 3.5 $query, $empty_parameters)")

        actual should equal(expected)
    }
  }

  test("repeating query with same parameters should hit the cache") {

    val cacheListener = new LoggingStringCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

    val query = "RETURN $n"
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(42))

    graph.execute(query, params1).resultAsString()
    graph.execute(query, params1).resultAsString()

    val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
    val expected = List(
      s"cacheFlushDetected",
      s"cacheMiss: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"cacheHit: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"cacheRecompile: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"cacheHit: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.LongValue))")

    actual should equal(expected)
  }

  test("repeating query with same parameter types but different values should hit the cache") {

    val cacheListener = new LoggingStringCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

    val query = "RETURN $n"
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(12))
    val params2: Map[String, AnyRef] = Map("n" -> Long.box(42))
    graph.execute(query, params1).resultAsString()
    graph.execute(query, params2).resultAsString()

    val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
    val expected = List(
      s"cacheFlushDetected",
      s"cacheMiss: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"cacheHit: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"cacheRecompile: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"cacheHit: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.LongValue))")

    actual should equal(expected)
  }

  test("repeating query with different parameters types should not hit the cache") {

    val cacheListener = new LoggingStringCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

    val query = "RETURN $n"
    val params1: Map[String, AnyRef] = Map("n" -> Long.box(42))
    val params2: Map[String, AnyRef] = Map("n" -> "nope")

    graph.execute(query, params1).resultAsString()
    graph.execute(query, params2).resultAsString()

    val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
    val expected = List(
      s"cacheFlushDetected",
      s"cacheMiss: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"cacheHit: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"cacheMiss: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.StringWrappingStringValue))",
      s"cacheHit: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.StringWrappingStringValue))")

    actual should equal(expected)
  }

  test("No recompilation on first attempt") {

    val cacheListener = new LoggingStringCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

    val query = "RETURN $n + 3 < 6"
    val params: Map[String, AnyRef] = Map("n" -> Long.box(42))

    graph.execute(query, params).resultAsString()

    val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
    val expected = List(
      s"cacheFlushDetected",
      s"cacheMiss: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"cacheHit: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.LongValue))"
    )

    actual should equal(expected)
  }

  test("One and only one recompilation after several attempts") {

    val cacheListener = new LoggingStringCacheListener
    kernelMonitors.addMonitorListener(cacheListener)

    val query = "RETURN $n + 3 < 6"
    val params: Map[String, AnyRef] = Map("n" -> Long.box(42))

    graph.execute(s"CYPHER $query", params).resultAsString()
    graph.execute(s"CYPHER $query", params).resultAsString()
    graph.execute(s"CYPHER $query", params).resultAsString()
    graph.execute(s"CYPHER $query", params).resultAsString()
    graph.execute(s"CYPHER $query", params).resultAsString()

    val actual = cacheListener.trace.map(str => str.replaceAll("\\s+", " "))
    val expected = List(
      s"cacheFlushDetected",
      s"cacheMiss: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"cacheHit: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"cacheRecompile: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"cacheHit: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"cacheHit: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"cacheHit: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.LongValue))",
      s"cacheHit: (CYPHER 3.5 $query, Map(n -> class org.neo4j.values.storable.LongValue))")

    actual should equal(expected)
  }

  private class LoggingStringCacheListener extends StringCacheMonitor {
    private var log: mutable.Builder[String, List[String]] = List.newBuilder

    def trace = log.result()

    def clear(): Unit = {
      log.clear()
    }

    override def cacheFlushDetected(sizeBeforeFlush: Long): Unit = {
      log += s"cacheFlushDetected"
    }

    override def cacheHit(key: Pair[String, ParameterTypeMap]): Unit = {
      log += s"cacheHit: $key"
    }

    override def cacheMiss(key: Pair[String, ParameterTypeMap]): Unit = {
      log += s"cacheMiss: $key"
    }

    override def cacheDiscard(key: Pair[String, ParameterTypeMap], ignored: String, secondsSinceReplan: Int): Unit = {
      log += s"cacheDiscard: $key"
    }

    override def cacheRecompile(key: Pair[String, ParameterTypeMap]): Unit = {
      log += s"cacheRecompile: $key"
    }
  }
}
