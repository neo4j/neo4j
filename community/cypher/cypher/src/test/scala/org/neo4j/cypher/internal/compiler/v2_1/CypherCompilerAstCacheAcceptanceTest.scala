/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.ast.Statement
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.ExecutionPlan
import org.neo4j.cypher.GraphDatabaseTestSupport

class CypherCompilerAstCacheAcceptanceTest extends CypherFunSuite with GraphDatabaseTestSupport {

  case class CacheCounts(hits: Int = 0, misses: Int = 0, flushes: Int = 0)

  class CacheCounter(var counts: CacheCounts = CacheCounts()) extends AstCacheMonitor {
    def cacheHit(key: Statement) {
      counts = counts.copy(hits = counts.hits + 1)
    }

    def cacheMiss(key: Statement) {
      counts = counts.copy(misses = counts.misses + 1)
    }

    def cacheFlushDetected(justBeforeKey: CacheAccessor[Statement, ExecutionPlan]) {
      counts = counts.copy(flushes = counts.flushes + 1)
    }
  }

  test("should monitor cache misses") {
    val compiler = newCurrentCompiler.ronjaCompiler2_1
    val counter = new CacheCounter()
    compiler.monitors.addMonitorListener(counter)

    graph.inTx { compiler.planQuery("return 42", planContext) }

    counter.counts should equal(CacheCounts(hits = 0, misses = 1, flushes = 1))
  }

  test("should monitor cache hits") {
    val compiler = newCurrentCompiler.ronjaCompiler2_1
    val counter = new CacheCounter()
    compiler.monitors.addMonitorListener(counter)

    graph.inTx { compiler.planQuery("return 42", planContext) }
    graph.inTx { compiler.planQuery("return 42", planContext) }

    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1))
  }

  test("should monitor cache flushes") {
    val compiler = newCurrentCompiler.ronjaCompiler2_1
    val counter = new CacheCounter()
    compiler.monitors.addMonitorListener(counter)

    graph.inTx { compiler.planQuery("return 42", planContext) }
    graph.createConstraint("Person", "id")
    graph.inTx { compiler.planQuery("return 42", planContext) }

    counter.counts should equal(CacheCounts(hits = 0, misses = 2, flushes = 2))
  }
}
