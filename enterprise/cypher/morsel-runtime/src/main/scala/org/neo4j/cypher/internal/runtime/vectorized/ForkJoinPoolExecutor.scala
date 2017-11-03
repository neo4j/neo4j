package org.neo4j.cypher.internal.runtime.vectorized


import java.util.concurrent.atomic.AtomicInteger
import java.util.{concurrent, function}

import org.neo4j.concurrent.BinaryLatch
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConverters._

object ForkJoinPoolExecutor {
  lazy val forkJoinPool = new java.util.concurrent.ForkJoinPool(Runtime.getRuntime.availableProcessors() * 2)
  private val MORSEL_SIZE = 10000

  def execute[E <: Exception](operators: Pipeline,
                              queryContext: QueryContext,
                              params: MapValue)(visitor: QueryResultVisitor[E]): Unit = {
    val leaf = getLeaf(operators)
    val iteration = new Iteration(None)
    val query = new Query()
    val startMessage = StartLeafLoop(iteration)
    val state = QueryState(params, visitor)
    forkJoinPool.submit(createRunnable(query, startMessage, leaf, queryContext, state))
    query.blockUntilQueryFinishes()
  }

  private def createRunnable(query: Query,
                             incoming: Message,
                             pipeline: Pipeline,
                             q: QueryContext,
                             state: QueryState): Runnable = {
    // We remember that the loop has started even before the task has been scheduled
    query.startLoop(incoming.iterationState)
    new Runnable {
      override def run(): Unit = {
        val queryContext = q.createNewQueryContext()
//        println(s"${Thread.currentThread()} executing $pipeline with $incoming")
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
          pipeline.parent.map(_.dependency) match {
            case None =>
              // We where the last pipeline! Cool! Let's signal the query that we are done here.
              query.releaseBlockedThreads()

            case Some(_: Eager) =>
              val startEager = StartLoopWithEagerData(query.eagerData.asScala.toSeq, incoming.iterationState)
              forkJoinPool.execute(createRunnable(query, startEager, pipeline.parent.get, queryContext, state))

            case Some(_: Lazy) =>
            // Nothing to do - we have been scheduling work all along
          }

        }

//        println(s"${Thread.currentThread()} finished $pipeline")
      }
    }
  }

  private def execute(query: Query, pipeline: Pipeline, message: Message, queryContext: QueryContext, state: QueryState) = {
    val data = Morsel.create(pipeline.slotInformation, MORSEL_SIZE)
    val continuation = pipeline.operate(message, data, queryContext, state)

    pipeline.parent match {
      case Some(mother) if mother.dependency.isInstanceOf[Eager] && query.eagerReceiver.contains(mother) =>
        query.eagerData.add(data)

      case Some(mother) if mother.dependency.isInstanceOf[Eager] && query.eagerReceiver.isEmpty =>
        query.eagerReceiver = Some(mother)
        query.eagerData.add(data)

      case Some(mother) if mother.dependency.isInstanceOf[Eager] =>
        throw new InternalException("This is not the same eager receiver as I want to us")

      case Some(mother) if mother.dependency.isInstanceOf[Lazy] =>
        val nextStep = StartLoopWithSingleMorsel(data, message.iterationState)
        forkJoinPool.execute(createRunnable(query, nextStep, mother, queryContext, state))

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
}

class Query() {
  private val loopCount = new concurrent.ConcurrentHashMap[Iteration, AtomicInteger]()
  private val latch = new BinaryLatch
  lazy val eagerData = new java.util.concurrent.ConcurrentLinkedQueue[Morsel]()
  var eagerReceiver: Option[Pipeline] = None

  def startLoop(iteration: Iteration): Unit = {
    loopCount.computeIfAbsent(iteration, createAtomicInteger).incrementAndGet()
  }

  def endLoop(iteration: Iteration): Int = {
    val i = loopCount.get(iteration).decrementAndGet()
    if(i==0)
      loopCount.remove(iteration)
    i
  }

  def blockUntilQueryFinishes(): Unit = latch.await()

  def releaseBlockedThreads(): Unit = latch.release()

  private val createAtomicInteger = new function.Function[Iteration, AtomicInteger] {
    override def apply(t: Iteration) = new AtomicInteger(0)
  }

}