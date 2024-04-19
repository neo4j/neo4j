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
package org.neo4j.cypher.internal.runtime.spec.tests.stress

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

abstract class RelationshipTypeReadConcurrencyStressTestBase[CONTEXT <: RuntimeContext](
                                                                                         edition: Edition[CONTEXT],
                                                                                         runtime: CypherRuntime[CONTEXT]
                                                                                       ) extends ConcurrencyStressTestBase[CONTEXT](edition, runtime) {

  test("should not throw when reading type of relationship that has been deleted") {
    val SIZE_HINT = 10000
    // given
    val rels = given {
      val (_, rels) = circleGraph(nNodes = SIZE_HINT, relType = "R", outDegree = 1)
      rels.map(_.getId)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("rT")
      .projection("type(r) as rT")
      .eager()
      .relationshipTypeScan("()-[r:R]->()")
      .build()

    val deletingThreads = 4
    val latch = new CountDownLatch(deletingThreads)

    val plan = buildPlan(logicalQuery, runtime)
    concurrentDelete(rels, deletingThreads, latch)
    // NOTE: Currently we ignore if anything goes wrong with the delete transactions
    val deadline = TEST_TIMEOUT.fromNow
    while (!latch.await(1, TimeUnit.MILLISECONDS)) {
      if (deadline.isOverdue()) {
        fail(s"Test execution timeout after $TEST_TIMEOUT")
      }
      consume(execute(plan))
    }
  }
}