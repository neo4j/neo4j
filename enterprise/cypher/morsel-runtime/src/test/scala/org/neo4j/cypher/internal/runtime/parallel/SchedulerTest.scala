/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.parallel

import java.util
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

abstract class SchedulerTest extends CypherFunSuite {

  private val tracer = SchedulerTracer.NoSchedulerTracer

  def newScheduler(maxConcurrency: Int): Scheduler

  test("execute simple task") {

    val s = newScheduler( 1 )

    val testThread = Thread.currentThread().getId
    val taskThreadId = new AtomicLong(testThread)

    val sb = new StringBuilder
    val queryExecution = s.execute(NoopTask(() => {
      sb ++= "great success"
      taskThreadId.set(Thread.currentThread().getId)
    }), tracer)

    queryExecution.await()

    sb.result() should equal("great success")
    if (s.isMultiThreaded)
      taskThreadId.get() should not equal testThread
  }

  test("execute 1000 simple tasks, spread over 4 threads") {
    val concurrency = 4
    val s = newScheduler(concurrency)

    val map = new ConcurrentHashMap[Int, Long]()
    val futures =
      for ( i <- 0 until 1000 ) yield
        s.execute(NoopTask(() => {
          map.put(i, Thread.currentThread().getId)
        }), tracer)

    futures.foreach(f => f.await())

    if (s.isMultiThreaded) {
      val countsPerThread = map.toSeq.groupBy(kv => kv._2).mapValues(_.size)
      countsPerThread.size() should equal(concurrency)
    }
  }

  test("execute downstream tasks") {

    val s = newScheduler(2)

    val result: mutable.Set[String] =
      java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap[String, java.lang.Boolean])

    val queryExecution = s.execute(
      SubTasker(List(
        NoopTask(() => result += "once"),
        NoopTask(() => result += "upon"),
        NoopTask(() => result += "a"),
        NoopTask(() => result += "time")
      )), tracer)

    queryExecution.await()
    result should equal(Set("once", "upon", "a", "time"))
  }

  test("execute reduce-like task tree") {

    val s = newScheduler(64)

    val aggregator = SumAggregator()

    val tasks = SubTasker(List(
      PushToEager(List(1,10,100), aggregator),
      PushToEager(List(1000,10000,100000), aggregator)))

    val queryExecution = s.execute(tasks, tracer)

    queryExecution.await()
    aggregator.sum.get() should be(111111)
  }

  // HELPER TASKS

  case class SumAggregator() extends Task {

    val buffer = new ConcurrentLinkedQueue[Integer]
    val sum = new AtomicInteger()

    override def executeWorkUnit(): Seq[Task] = {
      var value = buffer.poll()
      while (value != null) {
        sum.addAndGet(value)
        value = buffer.poll()
      }
      Nil
    }

    override def canContinue: Boolean = buffer.nonEmpty
  }

  case class PushToEager(subResults: Seq[Int], eager: SumAggregator) extends Task {

    private val resultSequence = subResults.iterator

    override def executeWorkUnit(): Seq[Task] = {
      if (resultSequence.hasNext)
        eager.buffer.add(resultSequence.next())

      if (canContinue) Nil
      else List(eager)
    }

    override def canContinue: Boolean = resultSequence.hasNext
  }

  case class SubTasker(subtasks: Seq[Task]) extends Task {

    private val taskSequence = subtasks.iterator

    override def executeWorkUnit(): Seq[Task] =
      if (taskSequence.hasNext) List(taskSequence.next())
      else Nil

    override def canContinue: Boolean = taskSequence.nonEmpty
  }

  case class NoopTask(f: () => Any) extends Task {
    override def executeWorkUnit(): Seq[Task] = {
      f()
      Nil
    }

    override def canContinue: Boolean = false
  }
}
