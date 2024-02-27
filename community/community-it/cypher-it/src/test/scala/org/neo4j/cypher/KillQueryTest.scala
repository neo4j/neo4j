/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.graphdb.TransactionTerminatedException
import org.neo4j.graphdb.TransientTransactionFailureException
import org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration
import org.neo4j.kernel.impl.query.QuerySubscriber.DO_NOTHING_SUBSCRIBER
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.impl.query.TransactionalContextFactory
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

class KillQueryTest extends ExecutionEngineFunSuite {
  /*
  This test creates threads that run a Cypher query over and over again for some time.
  Concurrently, another thread tries to terminate all running queries. This should not lead to weird behaviour - only
  well known and expected exceptions should be produced.
   */
  val NODE_COUNT = 1000
  val THREAD_COUNT: Int = Runtime.getRuntime.availableProcessors() * 2
  val SECONDS_TO_RUN = 5

  test("run queries and kill them left and right") {

    // given
    givenTx {
      (1 to NODE_COUNT) foreach { x =>
        createLabeledNode(Map("x" -> x, "name" -> ("apa" + x)), "Label")
      }
    }

    // when
    val contextFactory = Neo4jTransactionalContextFactory.create(graph)
    val engine = ExecutionEngineHelper.createEngine(graph)

    val query = "MATCH (n:Label) WHERE n.x > 12 RETURN n.name"

    val continue = new AtomicBoolean(true)
    val exceptionsThrown = new ConcurrentLinkedQueue[Throwable]()

    val queryRunners =
      for (i <- 0 until THREAD_COUNT)
        yield new QueryRunner(continue, contextFactory, query, engine, exceptionsThrown.add)

    val queryKiller = new QueryKiller(continue, queryRunners, exceptionsThrown.add)

    val threads: Seq[Thread] = queryRunners.map(new Thread(_)) :+ new Thread(queryKiller)
    threads.foreach(_.start())
    threads.foreach(_.join())

    // then
    val e = collectExceptions(exceptionsThrown)
    if (e != null) {
      throw e
    }
  }

  private def collectExceptions(exceptionsThrown: ConcurrentLinkedQueue[Throwable]): Throwable = {
    val exceptions = exceptionsThrown.iterator()
    var e: Throwable = null
    while (exceptions.hasNext) {
      if (e == null) e = exceptions.next()
      else e.addSuppressed(exceptions.next())
    }
    e
  }

  class QueryKiller(continue: AtomicBoolean, runners: IndexedSeq[QueryRunner], exLogger: Throwable => Unit)
      extends Runnable {

    override def run(): Unit =
      try {
        val start = System.currentTimeMillis()
        var i = 0
        while ((System.currentTimeMillis() - start) < SECONDS_TO_RUN * 1000 && continue.get()) {
          val transactionalContext = runners(i).tc
          if (transactionalContext != null) {
            try {
              transactionalContext.terminate()
            } catch {
              case _: TransactionTerminatedException =>
              case e: Throwable =>
                exLogger(e)
                continue.set(false)
            }
          }
          i = (i + 1) % runners.size
        }
      } finally {
        continue.set(false)
      }
  }

  class QueryRunner(
    continue: AtomicBoolean,
    contextFactory: TransactionalContextFactory,
    query: String,
    engine: ExecutionEngine,
    exLogger: Throwable => Unit
  ) extends Runnable {

    @volatile var tc: TransactionalContext = _

    override def run(): Unit = {
      while (continue.get()) {
        val tx = graph.beginTransaction(Type.IMPLICIT, AUTH_DISABLED)
        try {
          val transactionalContext: TransactionalContext =
            contextFactory.newContext(tx, query, EMPTY_MAP, QueryExecutionConfiguration.DEFAULT_CONFIG)
          tc = transactionalContext
          val result = engine.execute(
            query,
            EMPTY_MAP,
            transactionalContext,
            profile = false,
            prePopulate = false,
            DO_NOTHING_SUBSCRIBER
          )
          result.request(Long.MaxValue)
          result.await()
        } catch {
          // These are the acceptable exceptions
          case _: TransactionTerminatedException       =>
          case _: TransientTransactionFailureException =>

          case e: Throwable =>
            continue.set(false)
            exLogger(e)
        } finally {
          tc.close()
          tx.close()
        }
      }
    }
  }

}
