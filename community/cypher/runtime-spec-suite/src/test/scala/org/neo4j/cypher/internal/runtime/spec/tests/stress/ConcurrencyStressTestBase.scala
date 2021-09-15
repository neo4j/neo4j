/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
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

abstract class ConcurrencyStressTestBase[CONTEXT <: RuntimeContext](
                                                                     edition: Edition[CONTEXT],
                                                                     runtime: CypherRuntime[CONTEXT],
                                                                   ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

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

  private def concurrentDelete(ids: Seq[Long], threadCount: Int, latch: CountDownLatch): Future[immutable.IndexedSeq[Unit]] = {
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