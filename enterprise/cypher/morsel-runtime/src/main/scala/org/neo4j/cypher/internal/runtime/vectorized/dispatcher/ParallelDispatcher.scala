/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.vectorized.dispatcher

import java.util.concurrent.Executor
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.{concurrent, function}

import org.neo4j.concurrent.BinaryLatch
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.util.v3_4.{InternalException, TaskCloser}
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConverters._

class ParallelDispatcher(morselSize: Int, workers: Int, executor: Executor) extends Dispatcher {

  def execute[E <: Exception](operators: Pipeline,
                              queryContext: QueryContext,
                              params: MapValue,
                              taskCloser: TaskCloser)(visitor: QueryResultVisitor[E]): Unit = {
    val leaf = getLeaf(operators)
    val iteration = new Iteration(None)
    val query = new Query()
    val startMessage = StartLeafLoop(iteration)
    val state = QueryState(params, visitor)
    val action = createAction(query, startMessage, leaf, queryContext, state)
    executor.execute(action)
    query.blockUntilQueryFinishes()
    val failure = query.failure
    if (failure != null) {
        taskCloser.close(success = false)
        throw failure
    }
    taskCloser.close(success = true)
  }

  private def createAction(query: Query,
                           incoming: Message,
                           pipeline: Pipeline,
                           q: QueryContext,
                           state: QueryState): Runnable = {
    // We remember that the loop has started even before the task has been scheduled
    query.startLoop(incoming.iterationState)
    new Runnable {
      override def run(): Unit = try {
        val queryContext = q.createNewQueryContext()
        var message = incoming
        var continuation: Continuation = null
        while (continuation == null || !continuation.isInstanceOf[EndOfLoop]) {
          continuation = execute(query, pipeline, message, queryContext, state)
          message = ContinueLoopWith(continuation)
        }

        // Once we have exhausted this loop, we check if we just closed the last loop.
        val loopsLeft = query.endLoop(message.iterationState)
        val weJustClosedTheLastLoop = loopsLeft == 0
        if (weJustClosedTheLastLoop) {
          query.eagerReceiver match {
            case None =>
              // We where the last pipeline! Cool! Let's signal the query that we are done here.
              query.releaseBlockedThreads()

            case Some(eagerConsumingPipeline) =>
              query.eagerReceiver = None
              val startEager = StartLoopWithEagerData(query.eagerData.asScala.toArray, incoming.iterationState)
              executor.execute(createAction(query, startEager, eagerConsumingPipeline, queryContext, state))
          }

        }
      } catch {
        case e: Exception =>
          query.markFailure(e)
          query.releaseBlockedThreads()
          throw e
      }
    }
  }

  private def execute(query: Query, pipeline: Pipeline, message: Message, queryContext: QueryContext, state: QueryState) = {
    val data = Morsel.create(pipeline.slots, morselSize)
    val continuation = pipeline.operate(message, data, queryContext, state)

    pipeline.parent match {
      case Some(mother) if mother.dependency.isInstanceOf[Eager] && query.eagerReceiver.contains(mother) =>
        query.eagerData.add(data)

      case Some(mother) if mother.dependency.isInstanceOf[Eager] && query.eagerReceiver.isEmpty =>
        query.eagerReceiver = Some(mother)
        query.eagerData.add(data)

      case Some(mother) if mother.dependency.isInstanceOf[Eager] =>
        throw new InternalException("This is not the same eager receiver as I want to use")

      case Some(mother) if mother.dependency.isInstanceOf[Lazy] =>
        val nextStep = StartLoopWithSingleMorsel(data, message.iterationState)
        executor.execute(createAction(query, nextStep, mother, queryContext, state))

      case _ =>
    }
    continuation
  }

  private def getLeaf(pipeline: Pipeline): Pipeline = {
    var leafOp = pipeline
    while (leafOp.dependency != NoDependencies) {
      leafOp = leafOp.dependency.pipeline
    }

    leafOp
  }

  class Query() {
    private val loopCount = new concurrent.ConcurrentHashMap[Iteration, AtomicInteger]()
    private val error = new AtomicReference[Throwable]()
    private val latch = new BinaryLatch
    var eagerReceiver: Option[Pipeline] = None
    lazy val eagerData = new java.util.concurrent.ConcurrentLinkedQueue[Morsel]()

    def startLoop(iteration: Iteration): Unit = {
      loopCount.computeIfAbsent(iteration, createAtomicInteger).incrementAndGet()
    }

    def endLoop(iteration: Iteration): Int = {
      val i = loopCount.get(iteration).decrementAndGet()
      if(i==0)
        loopCount.remove(iteration)
      i
    }

    def failure: Throwable = error.get()
    def markFailure(t: Throwable): Unit = error.compareAndSet(null, t)

    def blockUntilQueryFinishes(): Unit = latch.await()

    def releaseBlockedThreads(): Unit = latch.release()

    private val createAtomicInteger = new function.Function[Iteration, AtomicInteger] {
      override def apply(t: Iteration) = new AtomicInteger(0)
    }

  }
}
