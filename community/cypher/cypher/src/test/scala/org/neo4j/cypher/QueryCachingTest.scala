/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.DynamicLabel
import org.neo4j.kernel.api.Statement
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.collection.mutable

class QueryCachingTest extends CypherFunSuite with GraphDatabaseTestSupport with TableDrivenPropertyChecks {

  test("re-uses cached plan across different execution modes") {
    // ensure label exists
    graph.inTx { graph.createNode(DynamicLabel.label("Person")) }

    val cacheListener = new LoggingStringCacheListener
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
        graph.inTx { statement.readOperations().schemaStateFlush() }

        graph.execute(firstQuery).resultAsString()
        graph.execute(secondQuery).resultAsString()

        val actual = cacheListener.trace
        val expected = List(
          s"cacheFlushDetected",
          s"cacheMiss: CYPHER 2.3 $query",
          s"cacheHit: CYPHER 2.3 $query",
          s"cacheHit: CYPHER 2.3 $query")

        actual should equal(expected)
    }
  }

  private class LoggingStringCacheListener extends StringCacheMonitor {
    private var log: mutable.Builder[String, List[String]] = List.newBuilder

    def trace = log.result()

    def clear(): Unit = {
      log.clear()
    }

    override def cacheFlushDetected(justBeforeKey: Statement): Unit = {
      log += s"cacheFlushDetected"
    }

    override def cacheHit(key: String): Unit = {
      log += s"cacheHit: $key"
    }

    override def cacheMiss(key: String): Unit = {
      log += s"cacheMiss: $key"
    }

    override def cacheDiscard(key: String): Unit = {
      log += s"cacheDiscard: $key"
    }
  }
}
