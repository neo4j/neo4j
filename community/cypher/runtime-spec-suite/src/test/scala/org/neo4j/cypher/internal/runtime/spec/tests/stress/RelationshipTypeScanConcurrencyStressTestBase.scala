/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.tests.stress

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

abstract class RelationshipTypeScanConcurrencyStressTestBase[CONTEXT <: RuntimeContext](
                                                                                         edition: Edition[CONTEXT],
                                                                                         runtime: CypherRuntime[CONTEXT],
                                                                                         sizeHint: Int
                                                                                       ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should not return relationships with null end nodes from directed relationship type") {
    scanWithConcurrentDeletes(directed = true)
  }

  test("should not return relationships with null end nodes from undirected relationship type") {
    scanWithConcurrentDeletes(directed = false)
  }

  private def scanWithConcurrentDeletes(directed: Boolean): Unit = {
    // given
    val sizeHint = 10000
    val rels = given {
      val (_, rels) = circleGraph(nNodes = sizeHint, relType = "R", outDegree = 1)
      rels.map(_.getId)
    }

    // when
    val pattern =  s"(n)-[r:R]-${if (directed) ">" else ""}(m)"
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("nId", "rId", "mId")
      .projection("id(n) AS nId", "id(r) AS rId", "id(m) AS mId")
      .relationshipTypeScan(pattern)
      .build()

    val deletingThreads = 4
    val latch = new CountDownLatch(deletingThreads)

    val plan = buildPlan(logicalQuery, runtime)
    concurrentDelete(rels, deletingThreads, latch)
    while (!latch.await(1, TimeUnit.MILLISECONDS)) {
      val runtimeResult: RecordingRuntimeResult = execute(plan)
      // then
      val notNullPredicate: AnyValue => Boolean = v => Values.longValue(-1) != v
      runtimeResult should beColumns("nId", "rId", "mId").withRows(disallowValues(Seq(("nId", notNullPredicate), ("rId", notNullPredicate), ("mId", notNullPredicate))))
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
