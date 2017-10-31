/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.vectorized.dispatcher

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

import org.neo4j.cypher.internal.runtime.vectorized._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class Conductor(minions: Array[Minion], MORSEL_SIZE: Int, queryQueue: ConcurrentLinkedQueue[Query]) extends Runnable {

  // Here we keep track of which query each minion is currently assigned to.
  private val _alive = new AtomicBoolean(true)

  // Used to know when an iteration has finished all tasks
  private val loopRefCounting = mutable.HashMap[Iteration, Int]()
  private val eagerAccumulation = mutable.HashMap[Iteration, ArrayBuffer[Morsel]]()
  private var currentlyRunningQueries: Set[Query] = Set.empty

  type TaskQueue = mutable.Queue[(Minion, Task)]

  def shutdown(): Unit = {
    minions.foreach {
      minion =>
        minion.input.clear()
        minion.input.add(Task(null, null, ShutdownWorkers, null))
    }
    _alive.set(false)
    currentlyRunningQueries.foreach(_.finished())
  }

  override def run(): Unit = {
    while (_alive.get()) {
      try {
        val workQueue = new TaskQueue()
        // For each round, do this:
        // 1. Check if any queries are waiting to be executed.
        //    If there are, we'll queue up work for each query waiting.
        enqueueWaitingQueries(workQueue)

        // 2. Go through each minion and check their output buffers, consuming finished tasks from them,
        //    and queueing up follow up tasks.
        emptyOutputBuffersAndProduceFollowUpWork(minions, workQueue)

        // 3. Figure out which minions should work on which queries, and schedule work to them.
        scheduleWorkOnMinions(minions, workQueue)
      } catch {

        // Someone probably left us a message
        case _: InterruptedException =>

        // Uh-oh... An uncaught exception is not good. Let's kill everything.
        case e: Exception =>
          e.printStackTrace()
          shutdown()
          throw e

      }
    }
  }

  private def emptyOutputBuffersAndProduceFollowUpWork(minions: Array[Minion], workQueue: TaskQueue): Unit = {
    minions foreach {
      minion =>
        if(minion.crashed != null) {
          println("found dead minion. let's shut everything down.")
          shutdown()
        }

        while (!minion.output.isEmpty) {
          val resultObject: ResultObject = minion.output.poll()
          createContinuationTask(minion, resultObject, workQueue)
          scheduleMoreWorkOnMorsel(resultObject, workQueue)
        }
    }
  }

  val r = new Random()

  private def startLoop(iteration: Iteration): Unit = {
    val current = loopRefCounting.getOrElse(iteration, 0)
    loopRefCounting.put(iteration, current + 1)
  }

  private def endLoop(iteration: Iteration): Int = {
    val current = loopRefCounting(iteration) - 1
    loopRefCounting.put(iteration, current)
    current
  }

  private def scheduleWorkOnMinions(minions: Array[Minion], workQueue: TaskQueue): Unit = {
    workQueue.foreach { case (wantedMinion, task) =>
      val minion = if (wantedMinion != null)
        wantedMinion
      else
        minions(r.nextInt(minions.length))
      minion.input.add(task)
    }
  }

  private def enqueueWaitingQueries(workQueue: TaskQueue): Unit = {
    while (!queryQueue.isEmpty) {
      val query = queryQueue.poll()
      currentlyRunningQueries = currentlyRunningQueries + query
      var current = query.pipeline

      while (current.dependency != NoDependencies) {
        current = current.dependency.pipeline
      }

      val morsel = Morsel.create(current.slotInformation, MORSEL_SIZE)
      val iteration = new Iteration(None)
      startLoop(iteration)
      workQueue.enqueue((null, Task(current, query, InitIteration(iteration), morsel)))
    }
  }

  private def createContinuationTask(minion: Minion, result: ResultObject, workQueue: TaskQueue): Unit = {
    result.next match {
      case _: Continue =>
        val morsel = Morsel.create(result.pipeline.slotInformation, MORSEL_SIZE)
        val task = result.createFollowContinuationTask(morsel)
        val runOn = task.message match {
          case ContinueWith(x: Continue) if x.needsSameThread => minion
          case _ => null
        }
        workQueue.enqueue((runOn, task))

      case _: EndOfLoop =>
        val iteration = result.next.iteration
        val current = endLoop(iteration)

        if (current > 1) {
          loopRefCounting(iteration) = current - 1
        } else { // This was the last work item, and we can now schedule the eager work, if we are on an eager pipeline
          result.pipeline.parent match {
            case Some(parentPipe: Pipeline) if parentPipe.dependency.isInstanceOf[Eager] =>
              val data = Morsel.create(parentPipe.slotInformation, MORSEL_SIZE)
              startLoop(result.next.iteration)
              val eagerData: Seq[Morsel] = eagerAccumulation.remove(iteration).get
              val message = WorkWithEagerData(eagerData, iteration)
              workQueue.enqueue((null, Task(parentPipe, result.query, message, data)))
            case None =>
              loopRefCounting.remove(iteration)
              result.query.finished()
              currentlyRunningQueries = currentlyRunningQueries - result.query
            case _ =>
          }
        }
    }
  }

  private def scheduleMoreWorkOnMorsel(result: ResultObject, workQueue: TaskQueue): Unit = {
    val iteration = result.next.iteration
    result.pipeline.parent match {
      case Some(daddy: Pipeline) if daddy.dependency.isInstanceOf[Lazy] =>
        // If we are feeding a lazy pipeline, we can just schedule this morsel for the next stage
        val data = Morsel.create(daddy.slotInformation, MORSEL_SIZE)
        val message = WorkWithLazyData(result.morsel, iteration)
        startLoop(iteration)
        workQueue.enqueue((null, Task(daddy, result.query, message, data)))

      case Some(daddy: Pipeline) if daddy.dependency.isInstanceOf[Eager] =>
        // When we are feeding an eager pipeline, we'll just accumulate here, and schedule work when we finish
        // the last work item on this pipeline
        val acc = eagerAccumulation.getOrElseUpdate(iteration, new ArrayBuffer[Morsel]())
        acc += result.morsel

      case None =>
      // no more work here
    }
  }

}
