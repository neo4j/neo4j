/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.neo4j.cypher.internal.{CommunityCompatibilityFactory, ExecutionEngine}
import org.neo4j.graphdb.{TransactionTerminatedException, TransientTransactionFailureException}
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.api.security.SecurityContext.AUTH_DISABLED
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo
import org.neo4j.kernel.impl.query.{Neo4jTransactionalContextFactory, TransactionalContext, TransactionalContextFactory}
import org.neo4j.logging.NullLogProvider

class KillQueryTest extends ExecutionEngineFunSuite {
  /*
  This test creates 10 threads that run a Cypher query over and over again for 10 seconds.
  Concurrently, another thread tries to terminate all running queries. This should not lead to weird behaviour - only
  well known and expected exceptions should be produced.
   */
  val emptyMap = new util.HashMap[String, AnyRef]
  val NODE_COUNT = 1000
  val THREAD_COUNT = Runtime.getRuntime().availableProcessors() * 2
  val SECONDS_TO_RUN = 5

  test("run queries and kill them left and right") {
    val locker = new PropertyContainerLocker()
    val contextFactory = Neo4jTransactionalContextFactory.create(graph, locker)

    (1 to NODE_COUNT) foreach { x =>
      createLabeledNode(Map("x" -> x, "name" -> ("apa" + x)), "Label")
    }

    val logProvider = NullLogProvider.getInstance()
    val compatibilityFactory = new CommunityCompatibilityFactory(graph, kernelAPI, kernelMonitors, logProvider)
    val engine = new ExecutionEngine(graph, logProvider, compatibilityFactory)

    val query = "MATCH (n:Label) WHERE n.x > 12 RETURN n.name"

    val continue = new AtomicBoolean(true)
    var exceptionsThrown = List.empty[Throwable]

    val tcs = new ArrayBlockingQueue[TransactionalContext](1000)
    val queryRunner = createQueryRunner(continue, contextFactory, query, tcs, engine, e => exceptionsThrown = exceptionsThrown :+ e)


    val queryKiller = createQueryKiller(continue, tcs, e => exceptionsThrown = exceptionsThrown :+ e)

    val threads: Seq[Thread] = (0 until THREAD_COUNT map (x => new Thread(queryRunner))) :+ new Thread(queryKiller)

    threads.foreach(_.start())
    threads.foreach(_.join())
    exceptionsThrown.foreach(throw _)
  }

  private val connectionInfo = new ClientConnectionInfo {
    override def asConnectionDetails(): String = ???

    override def protocol(): String = ???
  }

  private def createQueryKiller(continue: AtomicBoolean, tcs: ArrayBlockingQueue[TransactionalContext], exLogger: Throwable => Unit) = {
    new Runnable {
      override def run(): Unit =
        try {
          val start = System.currentTimeMillis()
          while ((System.currentTimeMillis() - start) < SECONDS_TO_RUN * 1000 && continue.get()) {
            val transactionalContext = tcs.poll()
            if (transactionalContext != null)
              try {
                transactionalContext.terminate()
              } catch {
                case e: Throwable =>
                  exLogger(e)
                  continue.set(false)
              }
          }
        } finally {
          continue.set(false)
        }
    }
  }

  private def createQueryRunner(continue: AtomicBoolean, contextFactory: TransactionalContextFactory, query: String, tcs: ArrayBlockingQueue[TransactionalContext], engine: ExecutionEngine, exLogger: Throwable => Unit) = {
    new Runnable {
      def run() {
        while (continue.get()) {
          val tx = graph.beginTransaction(Type.`implicit`, AUTH_DISABLED)
          try {
            val transactionalContext: TransactionalContext = contextFactory.newContext(connectionInfo, tx, query, emptyMap)
            tcs.put(transactionalContext)
            val result = engine.execute(query, Map.empty[String, AnyRef], transactionalContext)
            result.resultAsString()
            tx.success()
          }
          catch {
            // These are the acceptable exceptions
            case _: TransactionTerminatedException =>
            case _: TransientTransactionFailureException =>

            case e: Throwable =>
              tx.close()
              continue.set(false)
              exLogger(e)
          }
        }
      }
    }
  }

}
