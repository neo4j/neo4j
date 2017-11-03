package org.neo4j.cypher.internal.runtime.vectorized


import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.graphdb.Result
import org.neo4j.values.virtual.MapValue

object ForkJoinPoolExecutor {
  lazy val forkJoinPool = new java.util.concurrent.ForkJoinPool(Runtime.getRuntime.availableProcessors() * 2)
  private val MORSEL_SIZE = 10000

  private val loopCounter = new java.util.concurrent.ConcurrentHashMap[Iteration, AtomicInteger]()
  private val queryWaitCounter = new java.util.concurrent.ConcurrentHashMap[Iteration, QueryBlocker]()


  def execute[E <: Exception](operators: Pipeline,
                              queryContext: QueryContext,
                              params: MapValue)(visitor: Result.ResultVisitor[E]): Unit = {
    val leaf = getLeaf(operators)
    val iteration = new Iteration(None)
    loopCounter.put(iteration, new AtomicInteger(0))
    forkJoinPool.submit(exec(StartLeafLoop(iteration), leaf, queryContext, QueryState(params, visitor)))
    val blocker = new QueryBlocker
    queryWaitCounter.put(iteration, blocker)
    blocker.block()
  }

  private def exec(incoming: Message,
                   pipeline: Pipeline,
                   queryContext: QueryContext,
                   state: QueryState): Runnable = {
    // We remember that the loop has started even before the task has been scheduled
    loopCounter.get(incoming.iterationState).incrementAndGet()
    new Runnable {
      override def run(): Unit = {
        println(s"${Thread.currentThread()} executing $pipeline with $incoming")
        var message = incoming
        var continuation: Continuation = null
        while (continuation == null || !continuation.isInstanceOf[EndOfLoop]) {
          val data = Morsel.create(pipeline.slotInformation, MORSEL_SIZE)
          continuation = pipeline.operate(message, data, queryContext, state)

          pipeline.parent match {
            case Some(mother) if mother.dependency.isInstanceOf[Eager] =>
              ???

            case Some(mother) if mother.dependency.isInstanceOf[Lazy] =>
              val nextStep = StartLoopWithSingleMorsel(data, message.iterationState)
              forkJoinPool.execute(exec(nextStep, mother, queryContext, state))

            case _ =>
          }

          message = ContinueLoopWith(continuation)
        }
        val loopsLeft = loopCounter.get(message.iterationState).decrementAndGet()
        if (loopsLeft == 0) {
          // We where the last ones! Cool! Let's signal the query that we are done here.
          queryWaitCounter.get(incoming.iterationState).unblock()
        }

        println(s"${Thread.currentThread()} finished $pipeline")
      }
    }
  }

  private def getLeaf(pipeline: Pipeline): Pipeline = {
    var leafOp = pipeline
    while (leafOp.dependency != NoDependencies) {
      leafOp = leafOp.dependency.pipeline
    }

    leafOp
  }
}

class QueryBlocker {
  private val queue = new ArrayBlockingQueue[Unit](1)

  /**
    * Will block this thread until someone does an unblock on this object
    */
  def block(): Unit = {
    queue.clear()
    queue.take()
  }

  /**
    * If another thread is waiting for this object, calling this method will allow the other thread to continue
    */
  def unblock(): Unit = {
    queue.put({})
  }
}