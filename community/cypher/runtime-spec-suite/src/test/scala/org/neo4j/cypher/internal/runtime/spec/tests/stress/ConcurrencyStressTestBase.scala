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
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

abstract class ConcurrencyStressTestBase[CONTEXT <: RuntimeContext](
                                                                     edition: Edition[CONTEXT],
                                                                     runtime: CypherRuntime[CONTEXT],
                                                                   ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  protected val TEST_TIMEOUT: FiniteDuration = Duration(5, TimeUnit.MINUTES)


  /**
   * Expects query result columns to all be Node/Relationship IDs
   * @param rels all Relationship IDs in the graph, which will be deleted in batches during query execution
   */
  protected def executeWithConcurrentDeletes(rels: Seq[Long], logicalQuery: LogicalQuery): Unit = {
    val deletingThreads = 4
    val latch = new CountDownLatch(deletingThreads)

    val notNullPredicate: AnyValue => Boolean = v => Values.longValue(-1) != v
    val notNullColumnPredicates: Array[(String, AnyValue => Boolean)] = logicalQuery.resultColumns.map(column => (column, notNullPredicate))

    val plan = buildPlan(logicalQuery, runtime)
    concurrentDelete(rels, deletingThreads, latch)
    while (!latch.await(1, TimeUnit.MILLISECONDS)) {
      val runtimeResult: RecordingRuntimeResult = execute(plan)
      // then
      runtimeResult should beColumns("nId", "rId", "mId").withRows(disallowValues(notNullColumnPredicates))
    }
  }

  protected def concurrentDelete(ids: Seq[Long], threadCount: Int, latch: CountDownLatch): Future[immutable.IndexedSeq[Unit]] = {
    val threadIds = ids.grouped(ids.size / threadCount).toSeq
    Future.sequence((0 until threadCount).map(idsOffset => Future {
      val tx = newTx()
      val idsToDelete = threadIds(idsOffset)
      val idCount = idsToDelete.size
      var i = 0
      while (i < idCount) {
        tx.kernelTransaction().dataWrite().relationshipDelete(idsToDelete(i))
        i += 1
      }
      tx.commit()
      latch.countDown()
    }))
  }
}

object ConcurrencyStressTestBase {
  val SIZE_HINT = 10000
}